use crate::events::{
    EncryptionSnapshot, EncryptionStatus, FrontendEvent, SegmentSnapshot, SegmentStatus, TaskId,
    TaskLog, TaskSnapshot, TaskStatus,
};
use crate::hls::{run_hls_download, HlsDownloadConfig, ResumeState};
use crate::websocket::WebSocketState;
use anyhow::{anyhow, Result};
use base64::{engine::general_purpose, Engine as _};
use chrono::Utc;
use parking_lot::RwLock;
use rand::seq::SliceRandom;
use rand::thread_rng;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tauri::{async_runtime::JoinHandle, AppHandle, Manager};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

const MAX_LOGS: usize = 100;

// Representative desktop user agents sourced from the open-source dataset at
// https://github.com/intoli/user-agents (captured 2024-03).
const UA_POOL: &[&str] = &[
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.128 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
];

#[derive(Clone)]
pub struct DownloadManager {
    inner: Arc<ManagerInner>,
}

struct ManagerInner {
    tasks: RwLock<HashMap<TaskId, ManagedTask>>,
    websocket: WebSocketState,
    app_handle: AppHandle,
    state_path: PathBuf,
}

struct ManagedTask {
    snapshot: TaskSnapshot,
    config: HlsDownloadConfig,
    cancel_token: CancellationToken,
    handle: Option<JoinHandle<()>>,
    started_at: Option<Instant>,
    bytes_at_start: u64,
}

fn random_user_agent() -> String {
    let mut rng = thread_rng();
    UA_POOL
        .choose(&mut rng)
        .copied()
        .unwrap_or(UA_POOL[0])
        .to_string()
}

#[derive(Debug, Deserialize)]
pub struct NewTaskRequest {
    pub playlist_url: String,
    pub preferred_quality: Option<String>,
    pub save_dir: String,
    pub page_url: String,
    pub custom_headers: Option<String>,
    pub cookies: Option<String>,
    pub manual_key: Option<String>,
    pub manual_iv: Option<String>,
    pub title: Option<String>,
    pub max_concurrency: u8,
}

impl DownloadManager {
    pub fn new(app_handle: AppHandle, websocket: WebSocketState) -> Self {
        let state_path = {
            let app_data_dir = app_handle
                .path()
                .app_data_dir()
                .unwrap_or_else(|_| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
            if let Err(err) = std::fs::create_dir_all(&app_data_dir) {
                warn!(
                    "failed to prepare app data directory {}: {err:?}",
                    app_data_dir.display()
                );
            }
            app_data_dir.join("tasks-state.json")
        };

        let manager = Self {
            inner: Arc::new(ManagerInner {
                tasks: RwLock::new(HashMap::new()),
                websocket,
                app_handle,
                state_path,
            }),
        };

        if let Err(err) = manager.load_persisted_tasks() {
            warn!("failed to restore task state: {err:?}");
        }

        manager
    }

    pub fn is_task_paused(&self, task_id: TaskId) -> bool {
        let guard = self.inner.tasks.read();
        guard
            .get(&task_id)
            .map(|task| task.snapshot.status == TaskStatus::Paused)
            .unwrap_or(false)
    }

    fn load_persisted_tasks(&self) -> Result<()> {
        let path = &self.inner.state_path;
        if !path.exists() {
            return Ok(());
        }

        let data = fs::read_to_string(path)?;
        if data.trim().is_empty() {
            return Ok(());
        }

        let persisted: PersistedState = serde_json::from_str(&data)?;
        let mut guard = self.inner.tasks.write();
        guard.clear();

        for persisted_task in persisted.tasks {
            let mut snapshot = persisted_task.snapshot;
            if matches!(
                snapshot.status,
                TaskStatus::Downloading | TaskStatus::Queued
            ) {
                snapshot.status = TaskStatus::Paused;
                snapshot.last_event = Some("应用重启，任务已暂停".into());
                snapshot.speed_bps = 0.0;
                snapshot.eta_seconds = None;
            } else {
                snapshot.speed_bps = 0.0;
            }
            if snapshot.logs.len() > MAX_LOGS {
                let keep = snapshot.logs.len().saturating_sub(MAX_LOGS);
                snapshot.logs = snapshot.logs.split_off(keep);
            }

            let mut config = persisted_task.config.into_runtime()?;
            if config.resume.is_none() {
                config.resume = resume_state_from_snapshot(&snapshot);
            }
            let task_id = config.task_id;
            let bytes_at_start = snapshot.downloaded_bytes;
            guard.insert(
                task_id,
                ManagedTask {
                    snapshot,
                    config,
                    cancel_token: CancellationToken::new(),
                    handle: None,
                    started_at: None,
                    bytes_at_start,
                },
            );
        }
        drop(guard);
        self.persist_state();
        Ok(())
    }

    fn persist_state(&self) {
        if let Err(err) = self.save_state_to_disk() {
            warn!("failed to persist task state: {err:?}");
        }
    }

    fn save_state_to_disk(&self) -> Result<()> {
        let guard = self.inner.tasks.read();
        let tasks: Vec<PersistedTask> = guard
            .values()
            .map(|task| PersistedTask::from_managed(task))
            .collect();
        drop(guard);

        let json = serde_json::to_string_pretty(&PersistedState { tasks })?;
        let tmp_path = self.inner.state_path.with_extension("tmp");
        fs::write(&tmp_path, json)?;
        fs::rename(tmp_path, &self.inner.state_path)?;
        Ok(())
    }

    pub async fn create_task(&self, request: NewTaskRequest) -> Result<TaskId> {
        if request.playlist_url.trim().is_empty() {
            return Err(anyhow!("请提供有效的 m3u8 链接"));
        }
        if request.page_url.trim().is_empty() {
            return Err(anyhow!("请提供视频页面地址，它会作为首个 m3u8 请求的 Referer"));
        }

        let playlist_url = url::Url::parse(request.playlist_url.trim())?;
        let page_url = url::Url::parse(request.page_url.trim())
            .map_err(|_| anyhow!("页面地址格式无效，请输入完整的 http/https 链接"))?;
        let save_dir = self.prep_save_dir(&request.save_dir).await?;
        let title = request
            .title
            .as_deref()
            .unwrap_or_else(|| playlist_url.path().rsplit('/').next().unwrap_or("HLS 任务"))
            .trim();
        let title = if title.is_empty() {
            format!("任务 {}", Uuid::new_v4().simple())
        } else {
            title.to_string()
        };

        let manual_key = decode_optional_bytes(request.manual_key.as_deref())?;
        let manual_iv = decode_optional_bytes(request.manual_iv.as_deref())?;

        let id = Uuid::new_v4();
        let snapshot = TaskSnapshot {
            id: id.to_string(),
            title: title.clone(),
            playlist_url: playlist_url.to_string(),
            playlist_type: "unknown".into(),
            quality: request.preferred_quality.clone(),
            resolution: None,
            audio: None,
            save_dir: save_dir.display().to_string(),
            total_segments: 0,
            downloaded_segments: 0,
            downloaded_bytes: 0,
            progress: 0.0,
            speed_bps: 0.0,
            status: TaskStatus::Queued,
            eta_seconds: None,
            created_at: Utc::now().to_rfc3339(),
            last_event: Some("等待解析 m3u8 清单".into()),
            average_segment_size_bytes: None,
            retries: 0,
            encryption: None,
            segments: Vec::new(),
            logs: Vec::new(),
            output_path: None,
        };

        let config = HlsDownloadConfig {
            task_id: id,
            title,
            playlist_url: playlist_url.to_string(),
            save_dir,
            preferred_quality: request.preferred_quality,
            referer: Some(page_url.to_string()),
            custom_headers: request.custom_headers,
            cookies: request.cookies,
            manual_key,
            manual_iv,
            concurrency: request.max_concurrency.clamp(1, 16),
            user_agent: random_user_agent(),
            resume: None,
        };

        {
            let mut guard = self.inner.tasks.write();
            guard.insert(
                id,
                ManagedTask {
                    snapshot: snapshot.clone(),
                    config: config.clone(),
                    cancel_token: CancellationToken::new(),
                    handle: None,
                    started_at: None,
                    bytes_at_start: 0,
                },
            );
        }

        self.inner
            .websocket
            .send(FrontendEvent::TaskCreated { task: snapshot })
            .ok();
        info!(task_id = %id, "task created");

        self.persist_state();

        self.spawn_download(id, config);
        Ok(id)
    }

    pub async fn pause_task(&self, task_id: TaskId) -> Result<()> {
        enum PauseAction {
            Noop,
            Update {
                handle: Option<JoinHandle<()>>,
                cancel_token: CancellationToken,
            },
        }

        let action = {
            let mut guard = self.inner.tasks.write();
            let Some(task) = guard.get_mut(&task_id) else {
                return Err(anyhow!("task not found"));
            };
            if matches!(
                task.snapshot.status,
                TaskStatus::Paused | TaskStatus::Completed | TaskStatus::Error
            ) {
                PauseAction::Noop
            } else {
                task.snapshot.status = TaskStatus::Paused;
                task.snapshot.last_event = Some("任务已暂停".into());
                task.snapshot.speed_bps = 0.0;
                task.snapshot.eta_seconds = None;
                for segment in task.snapshot.segments.iter_mut() {
                    if segment.status != SegmentStatus::Completed {
                        segment.status = SegmentStatus::Pending;
                        segment.speed_bps = None;
                        segment.size_bytes = None;
                        segment.last_updated = None;
                    }
                }
                let prev_token = task.cancel_token.clone();
                task.cancel_token = CancellationToken::new();
                let handle = task.handle.take();
                task.started_at = None;
                task.bytes_at_start = task.snapshot.downloaded_bytes;
                task.config.resume = resume_state_from_snapshot(&task.snapshot);
                self.broadcast_task(task_id, &task.snapshot);
                PauseAction::Update {
                    handle,
                    cancel_token: prev_token,
                }
            }
        };

        if let PauseAction::Update {
            handle,
            cancel_token,
        } = action
        {
            cancel_token.cancel();
            if let Some(handle) = handle {
                handle.abort();
                let _ = handle.await;
            }
            self.push_log(task_id, "任务已暂停，可稍后继续").await.ok();
            info!(task_id = %task_id, "task paused");
            self.persist_state();
        }

        Ok(())
    }

    pub async fn resume_task(&self, task_id: TaskId) -> Result<()> {
        let config = {
            let mut guard = self.inner.tasks.write();
            let task = guard
                .get_mut(&task_id)
                .ok_or_else(|| anyhow!("task not found"))?;
            task.cancel_token = CancellationToken::new();
            task.snapshot.status = TaskStatus::Queued;
            task.snapshot.last_event = Some("准备恢复下载".into());
            task.started_at = None;
            task.bytes_at_start = task.snapshot.downloaded_bytes;
            self.broadcast_task(task_id, &task.snapshot);
            task.config.clone()
        };
        self.spawn_download(task_id, config);
        info!(task_id = %task_id, "task resume requested");
        self.persist_state();
        Ok(())
    }

    pub async fn restart_task(&self, task_id: TaskId) -> Result<()> {
        let status = {
            let guard = self.inner.tasks.read();
            let task = guard
                .get(&task_id)
                .ok_or_else(|| anyhow!("task not found"))?;
            task.snapshot.status.clone()
        };

        let (output_hint, completed_segments, has_remaining, mut base_config) = {
            let mut guard = self.inner.tasks.write();
            let task = guard
                .get_mut(&task_id)
                .ok_or_else(|| anyhow!("task not found"))?;
            task.cancel_token.cancel();
            if let Some(handle) = task.handle.take() {
                handle.abort();
            }
            let completed: Vec<u64> = task
                .snapshot
                .segments
                .iter()
                .filter(|seg| seg.status == SegmentStatus::Completed)
                .map(|seg| seg.seq)
                .collect();
            let has_remaining = task
                .snapshot
                .segments
                .iter()
                .any(|seg| seg.status != SegmentStatus::Completed);
            let output_path = task.snapshot.output_path.clone();
            let config_clone = task.config.clone();
            (output_path, completed, has_remaining, config_clone)
        };

        if has_remaining && !completed_segments.is_empty() {
            if let Some(path) = output_hint.clone() {
                if tokio::fs::metadata(Path::new(&path)).await.is_ok() {
                    let resume_state = ResumeState {
                        completed: completed_segments.clone(),
                        output_path: PathBuf::from(path.clone()),
                    };
                    let completed_set: HashSet<u64> =
                        resume_state.completed.iter().copied().collect();
                    {
                        let mut guard = self.inner.tasks.write();
                        let task = guard
                            .get_mut(&task_id)
                            .ok_or_else(|| anyhow!("task not found"))?;
                        task.cancel_token = CancellationToken::new();
                        task.handle = None;
                        task.started_at = None;
                        let mut downloaded_segments = 0u64;
                        let mut downloaded_bytes = 0u64;
                        for segment in task.snapshot.segments.iter_mut() {
                            if completed_set.contains(&segment.seq) {
                                segment.status = SegmentStatus::Completed;
                                if let Some(bytes) = segment.size_bytes {
                                    downloaded_bytes = downloaded_bytes.saturating_add(bytes);
                                }
                                downloaded_segments += 1;
                            } else {
                                segment.status = SegmentStatus::Pending;
                                segment.speed_bps = None;
                                segment.size_bytes = None;
                                segment.last_updated = None;
                            }
                        }
                        task.snapshot.downloaded_segments = downloaded_segments;
                        task.snapshot.downloaded_bytes = downloaded_bytes;
                        if task.snapshot.total_segments > 0 {
                            task.snapshot.progress = (downloaded_segments as f32
                                / task.snapshot.total_segments as f32)
                                * 100.0;
                        } else {
                            task.snapshot.progress = 0.0;
                        }
                        task.snapshot.status = TaskStatus::Queued;
                        task.snapshot.last_event = Some("准备恢复下载".into());
                        task.snapshot.speed_bps = 0.0;
                        task.snapshot.eta_seconds = None;
                        task.snapshot.retries = 0;
                        if task.snapshot.output_path.is_none() {
                            task.snapshot.output_path = Some(path.clone());
                        }
                        task.bytes_at_start = task.snapshot.downloaded_bytes;
                        task.snapshot.logs.truncate(MAX_LOGS);
                        self.broadcast_task(task_id, &task.snapshot);
                    }
                    base_config.resume = Some(resume_state);
                    self.persist_state();
                    self.push_log(task_id, "继续下载剩余分片").await.ok();
                    self.spawn_download(task_id, base_config);
                    info!(task_id = %task_id, "task restart resumed from checkpoint");
                    return Ok(());
                }
            }
        }

        let mut restart_message = "任务已重置，准备重新下载".to_string();

        if status == TaskStatus::Completed {
            if let Some(ref path) = output_hint {
                match tokio::fs::metadata(Path::new(path)).await {
                    Ok(_) => {
                        self.push_log(
                            task_id,
                            format!("任务已完成，输出文件仍存在，无需重启 ({path})"),
                        )
                        .await
                        .ok();
                        return Ok(());
                    }
                    Err(err) => {
                        if err.kind() == ErrorKind::NotFound {
                            restart_message = "任务已完成但输出文件缺失，准备重新下载".to_string();
                        } else {
                            warn!("failed to stat output {path}: {err:?}");
                            restart_message = format!("检查输出文件时出错 ({err}), 将重新下载");
                        }
                    }
                }
            } else {
                restart_message = "任务已完成但缺少输出文件，准备重新下载".into();
            }
        }

        let (config, output_path) = self.reset_task_state(task_id, &restart_message)?;

        if let Some(path) = output_path {
            if !path.is_empty() {
                match tokio::fs::remove_file(Path::new(&path)).await {
                    Ok(_) => {}
                    Err(err) if err.kind() == ErrorKind::NotFound => {}
                    Err(err) => warn!("failed to remove previous output {path}: {err:?}"),
                }
            }
        }

        self.persist_state();

        self.push_log(task_id, restart_message).await.ok();
        self.spawn_download(task_id, config);
        info!(task_id = %task_id, "task restart requested");
        Ok(())
    }

    pub async fn retry_segment(&self, task_id: TaskId, seq: u64) -> Result<()> {
        {
            let mut guard = self.inner.tasks.write();
            let task = guard
                .get_mut(&task_id)
                .ok_or_else(|| anyhow!("task not found"))?;
            if let Some(segment) = task.snapshot.segments.iter_mut().find(|seg| seg.seq == seq) {
                segment.status = SegmentStatus::Retrying;
                segment.speed_bps = None;
                segment.size_bytes = None;
                segment.retries += 1;
            }
            self.broadcast_task(task_id, &task.snapshot);
        }
        self.resume_task(task_id).await
    }

    pub async fn remove_task(&self, task_id: TaskId) -> Result<()> {
        tracing::info!("[REMOVE_TASK] 开始删除任务, task_id: {}", task_id);
        let removed = {
            let mut guard = self.inner.tasks.write();
            tracing::info!("[REMOVE_TASK] 获取锁成功, 当前任务数: {}", guard.len());
            if let Some(mut task) = guard.remove(&task_id) {
                tracing::info!("[REMOVE_TASK] 任务已从tasks中移除, task_id: {}", task_id);
                task.cancel_token.cancel();
                if let Some(handle) = task.handle.take() {
                    tracing::info!("[REMOVE_TASK] 中止下载handle, task_id: {}", task_id);
                    handle.abort();
                }
                true
            } else {
                tracing::warn!("[REMOVE_TASK] 任务不存在于tasks中, task_id: {}", task_id);
                false
            }
        };

        if removed {
            tracing::info!(
                "[REMOVE_TASK] 准备发送TaskRemoved事件, task_id: {}",
                task_id
            );
            let event = FrontendEvent::TaskRemoved {
                task_id: task_id.to_string(),
            };
            tracing::info!("[REMOVE_TASK] TaskRemoved事件内容: {:?}", event);
            match self.inner.websocket.send(event) {
                Ok(_) => {
                    tracing::info!(
                        "[REMOVE_TASK] TaskRemoved事件发送成功, task_id: {}",
                        task_id
                    );
                }
                Err(e) => {
                    tracing::error!(
                        "[REMOVE_TASK] TaskRemoved事件发送失败, task_id: {}, error: {:?}",
                        task_id,
                        e
                    );
                }
            }
            tracing::info!("[REMOVE_TASK] 准备持久化状态");
            self.persist_state();
            tracing::info!("[REMOVE_TASK] 任务删除完成, task_id: {}", task_id);
        }
        Ok(())
    }

    pub async fn snapshot(&self) -> Result<Vec<TaskSnapshot>> {
        let mut guard = self.inner.tasks.write();
        let mut needs_persist = false;
        for (id, task) in guard.iter_mut() {
            if task.snapshot.status == TaskStatus::Downloading {
                let finished = task
                    .handle
                    .as_ref()
                    .map(|h| h.inner().is_finished())
                    .unwrap_or(true);
                if finished {
                    task.snapshot.status = TaskStatus::Error;
                    task.snapshot
                        .last_event
                        .get_or_insert_with(|| "任务已中断，请重试".into());
                    task.snapshot.speed_bps = 0.0;
                    task.snapshot.eta_seconds = None;
                    warn!(task_id = %id, "task marked errored during snapshot due to finished handle");
                    needs_persist = true;
                }
            }
        }
        let snapshots = guard
            .values()
            .map(|task| task.snapshot.clone())
            .collect::<Vec<_>>();
        drop(guard);
        if needs_persist {
            self.persist_state();
        }
        Ok(snapshots)
    }

    pub async fn mark_playlist_resolved(
        &self,
        task_id: TaskId,
        playlist_type: String,
        total_segments: usize,
        start_seq: u64,
        meta: PlaylistMeta,
    ) -> Result<()> {
        let snapshot = {
            let mut guard = self.inner.tasks.write();
            let task = guard
                .get_mut(&task_id)
                .ok_or_else(|| anyhow!("task not found"))?;
            task.snapshot.playlist_type = playlist_type;
            task.snapshot.total_segments = total_segments as u64;
            task.snapshot.quality = task.snapshot.quality.clone().or(meta.quality.clone());
            task.snapshot.resolution = meta.resolution;
            task.snapshot.audio = meta.audio;
            task.snapshot.eta_seconds = None;
            task.snapshot.encryption = meta.key_uri.map(|uri| EncryptionSnapshot {
                method: meta.method,
                key_uri: Some(uri),
                status: EncryptionStatus::Unknown,
                iv: None,
                last_error: None,
            });
            let existing = task.snapshot.segments.clone();
            let previous_bytes = task.snapshot.downloaded_bytes;
            let mut downloaded_segments = 0u64;
            let mut downloaded_bytes = 0u64;
            let mut existing_by_seq: HashMap<u64, SegmentSnapshot> =
                existing.into_iter().map(|seg| (seg.seq, seg)).collect();
            task.snapshot.segments = (0..total_segments as u64)
                .map(|offset| {
                    let seq = start_seq + offset;
                    if let Some(mut existing_seg) = existing_by_seq.remove(&seq) {
                        existing_seg.seq = seq;
                        if existing_seg.status == SegmentStatus::Completed {
                            downloaded_segments += 1;
                            if let Some(bytes) = existing_seg.size_bytes {
                                downloaded_bytes = downloaded_bytes.saturating_add(bytes);
                            }
                            return existing_seg;
                        }
                        existing_seg.status = SegmentStatus::Pending;
                        existing_seg.speed_bps = None;
                        existing_seg.size_bytes = None;
                        existing_seg.retries = 0;
                        existing_seg.last_updated = None;
                        return existing_seg;
                    }
                    SegmentSnapshot {
                        seq,
                        status: SegmentStatus::Pending,
                        speed_bps: None,
                        size_bytes: None,
                        retries: 0,
                        last_updated: None,
                    }
                })
                .collect();
            task.snapshot.downloaded_segments = downloaded_segments;
            task.snapshot.downloaded_bytes = if downloaded_bytes > 0 {
                downloaded_bytes
            } else {
                previous_bytes
            };
            if total_segments > 0 {
                task.snapshot.progress =
                    (downloaded_segments as f32 / total_segments as f32) * 100.0;
            } else {
                task.snapshot.progress = 0.0;
            }
            task.snapshot.clone()
        };
        self.inner
            .websocket
            .send(FrontendEvent::TaskUpdated { task: snapshot })
            .ok();
        self.persist_state();
        Ok(())
    }

    pub async fn update_encryption_status(
        &self,
        task_id: TaskId,
        snapshot: EncryptionSnapshot,
    ) -> Result<()> {
        {
            let mut guard = self.inner.tasks.write();
            if let Some(task) = guard.get_mut(&task_id) {
                task.snapshot.encryption = Some(snapshot.clone());
            }
        }
        self.inner
            .websocket
            .send(FrontendEvent::KeyStatus {
                task_id: task_id.to_string(),
                encryption: snapshot,
            })
            .ok();
        Ok(())
    }

    pub async fn update_segment(
        &self,
        task_id: TaskId,
        seq: u64,
        status: SegmentStatus,
        speed: Option<f64>,
        size: Option<u64>,
    ) -> Result<()> {
        let segment = {
            let mut guard = self.inner.tasks.write();
            let task = guard
                .get_mut(&task_id)
                .ok_or_else(|| anyhow!("task not found"))?;
            if let Some(segment) = task.snapshot.segments.iter_mut().find(|seg| seg.seq == seq) {
                segment.status = status.clone();
                segment.speed_bps = speed;
                segment.size_bytes = size;
                segment.last_updated = Some(Utc::now().to_rfc3339());
                segment.clone()
            } else {
                return Ok(());
            }
        };

        self.inner
            .websocket
            .send(FrontendEvent::SegmentUpdated {
                task_id: task_id.to_string(),
                segment,
            })
            .ok();
        Ok(())
    }

    pub async fn mark_segment_retrying(&self, task_id: TaskId, seq: u64) -> Result<()> {
        let segment = {
            let mut guard = self.inner.tasks.write();
            let task = guard
                .get_mut(&task_id)
                .ok_or_else(|| anyhow!("task not found"))?;
            if let Some(segment) = task.snapshot.segments.iter_mut().find(|seg| seg.seq == seq) {
                segment.status = SegmentStatus::Retrying;
                segment.speed_bps = None;
                segment.size_bytes = None;
                segment.retries = segment.retries.saturating_add(1);
                segment.last_updated = Some(Utc::now().to_rfc3339());
                segment.clone()
            } else {
                return Ok(());
            }
        };

        self.inner
            .websocket
            .send(FrontendEvent::SegmentUpdated {
                task_id: task_id.to_string(),
                segment,
            })
            .ok();
        Ok(())
    }

    pub async fn mark_segment_complete(
        &self,
        task_id: TaskId,
        seq: u64,
        bytes: u64,
        speed_bps: f64,
    ) -> Result<()> {
        let snapshot = {
            let mut guard = self.inner.tasks.write();
            let task = guard
                .get_mut(&task_id)
                .ok_or_else(|| anyhow!("task not found"))?;
            task.snapshot.downloaded_segments += 1;
            task.snapshot.downloaded_bytes = task.snapshot.downloaded_bytes.saturating_add(bytes);
            if task.snapshot.total_segments > 0 {
                task.snapshot.progress = (task.snapshot.downloaded_segments as f32
                    / task.snapshot.total_segments as f32)
                    * 100.0;
            }
            let new_avg = match task.snapshot.average_segment_size_bytes {
                Some(prev) => {
                    ((prev * (task.snapshot.downloaded_segments - 1)) + bytes)
                        / task.snapshot.downloaded_segments
                }
                None => bytes,
            };
            task.snapshot.average_segment_size_bytes = Some(new_avg);

            let remaining_segments = task
                .snapshot
                .total_segments
                .saturating_sub(task.snapshot.downloaded_segments);
            let overall_speed = task
                .started_at
                .map(|started| {
                    let elapsed = started.elapsed().as_secs_f64();
                    let bytes_since_start = task
                        .snapshot
                        .downloaded_bytes
                        .saturating_sub(task.bytes_at_start);
                    if elapsed > 0.0 && bytes_since_start > 0 {
                        Some(bytes_since_start as f64 / elapsed)
                    } else {
                        None
                    }
                })
                .flatten();
            task.snapshot.eta_seconds = if remaining_segments == 0 {
                Some(0)
            } else if new_avg > 0 {
                let remaining_bytes = new_avg.saturating_mul(remaining_segments);
                let denominator = overall_speed.unwrap_or(speed_bps);
                if denominator > 1.0 {
                    let eta = (remaining_bytes as f64 / denominator).ceil() as u64;
                    Some(eta)
                } else {
                    None
                }
            } else {
                None
            };
            task.snapshot.speed_bps = overall_speed.unwrap_or(speed_bps);
            if let Some(segment) = task.snapshot.segments.iter_mut().find(|seg| seg.seq == seq) {
                segment.status = SegmentStatus::Completed;
                segment.size_bytes = Some(bytes);
                segment.speed_bps = Some(speed_bps);
                segment.last_updated = Some(Utc::now().to_rfc3339());
            }
            task.snapshot.clone()
        };
        self.inner
            .websocket
            .send(FrontendEvent::TaskUpdated { task: snapshot })
            .ok();
        Ok(())
    }

    pub async fn push_log(&self, task_id: TaskId, message: impl Into<String>) -> Result<()> {
        let log = TaskLog {
            timestamp: Utc::now().to_rfc3339(),
            message: message.into(),
        };
        {
            let mut guard = self.inner.tasks.write();
            if let Some(task) = guard.get_mut(&task_id) {
                let mut logs: VecDeque<TaskLog> = task.snapshot.logs.clone().into();
                logs.push_back(log.clone());
                while logs.len() > MAX_LOGS {
                    logs.pop_front();
                }
                task.snapshot.logs = logs.into();
                task.snapshot.last_event = Some(log.message.clone());
            }
        }
        self.inner
            .websocket
            .send(FrontendEvent::LogAppended {
                task_id: task_id.to_string(),
                log,
            })
            .ok();
        Ok(())
    }

    pub async fn mark_task_completed(&self, task_id: TaskId, output: PathBuf) -> Result<()> {
        let snapshot = {
            let mut guard = self.inner.tasks.write();
            let task = guard
                .get_mut(&task_id)
                .ok_or_else(|| anyhow!("task not found"))?;
            task.snapshot.status = TaskStatus::Completed;
            task.snapshot.output_path = Some(output.display().to_string());
            task.snapshot.progress = 100.0;
            task.snapshot.speed_bps = 0.0;
            task.snapshot.eta_seconds = Some(0);
            task.snapshot.last_event = Some("任务已完成".into());
            task.started_at = None;
            task.bytes_at_start = task.snapshot.downloaded_bytes;
            task.snapshot.clone()
        };
        self.inner
            .websocket
            .send(FrontendEvent::TaskCompleted {
                task_id: task_id.to_string(),
                task: snapshot,
            })
            .ok();
        info!(task_id = %task_id, "task finished successfully");
        self.persist_state();
        Ok(())
    }

    pub async fn mark_task_error(&self, task_id: TaskId, message: String) -> Result<()> {
        let reason = if message.trim().is_empty() {
            "未知错误".to_string()
        } else {
            message
        };

        self.push_log(task_id, format!("任务异常: {reason}"))
            .await
            .ok();
        {
            let mut guard = self.inner.tasks.write();
            if let Some(task) = guard.get_mut(&task_id) {
                task.snapshot.status = TaskStatus::Error;
                task.snapshot.last_event = Some(reason.clone());
                task.started_at = None;
                task.bytes_at_start = task.snapshot.downloaded_bytes;
            }
        }
        self.inner
            .websocket
            .send(FrontendEvent::TaskError {
                task_id: task_id.to_string(),
                message: reason.clone(),
            })
            .ok();
        error!(task_id = %task_id, reason = %reason, "task entered error state");
        self.persist_state();
        Ok(())
    }

    fn spawn_download(&self, task_id: TaskId, mut config: HlsDownloadConfig) {
        let manager = self.clone();
        let (cancel, snapshot_resume) = {
            let mut guard = self.inner.tasks.write();
            if let Some(task) = guard.get_mut(&task_id) {
                if let Some(handle) = task.handle.take() {
                    handle.abort();
                }
                task.cancel_token.cancel();
                task.cancel_token = CancellationToken::new();
                (
                    task.cancel_token.clone(),
                    resume_state_from_snapshot(&task.snapshot),
                )
            } else {
                (CancellationToken::new(), None)
            }
        };

        if config.resume.is_none() {
            config.resume = snapshot_resume;
        } else {
            config.resume = config.resume.take().and_then(|mut state| {
                state.completed.sort_unstable();
                state.completed.dedup();
                if state.completed.is_empty() {
                    None
                } else {
                    Some(state)
                }
            });
        }

        let cancel_for_run = cancel.clone();
        let run_config = config.clone();
        let handle = tauri::async_runtime::spawn(async move {
            if let Err(err) =
                run_hls_download(manager.clone(), run_config, cancel_for_run.clone()).await
            {
                if cancel_for_run.is_cancelled() {
                    info!("task {task_id} cancelled");
                } else {
                    manager
                        .mark_task_error(task_id, format!("{err:#}"))
                        .await
                        .unwrap_or_else(|e| warn!("failed to mark error: {e:?}"));
                }
            }
        });

        let mut guard = self.inner.tasks.write();
        if let Some(task) = guard.get_mut(&task_id) {
            task.handle = Some(handle);
            task.config = config.clone();
            task.snapshot.status = TaskStatus::Downloading;
            task.snapshot.last_event = Some("开始下载分片".into());
            task.started_at = Some(Instant::now());
            task.bytes_at_start = task.snapshot.downloaded_bytes;
            task.snapshot.speed_bps = 0.0;
            self.broadcast_task(task_id, &task.snapshot);
        }
        drop(guard);
        info!(task_id = %task_id, "spawned download task");
        self.persist_state();
    }

    pub async fn ensure_output_path(&self, task_id: TaskId, path: &Path) -> Result<()> {
        let updated = {
            let mut guard = self.inner.tasks.write();
            let task = guard
                .get_mut(&task_id)
                .ok_or_else(|| anyhow!("task not found"))?;
            let new_path = path.display().to_string();
            if task.snapshot.output_path.as_ref() != Some(&new_path) {
                task.snapshot.output_path = Some(new_path);
                true
            } else {
                false
            }
        };
        if updated {
            self.persist_state();
        }
        Ok(())
    }

    fn reset_task_state(
        &self,
        task_id: TaskId,
        last_event: &str,
    ) -> Result<(HlsDownloadConfig, Option<String>)> {
        let mut guard = self.inner.tasks.write();
        let task = guard
            .get_mut(&task_id)
            .ok_or_else(|| anyhow!("task not found"))?;

        task.cancel_token.cancel();
        if let Some(handle) = task.handle.take() {
            handle.abort();
        }

        let mut config = task.config.clone();
        let output_path = task.snapshot.output_path.clone();

        task.cancel_token = CancellationToken::new();
        task.snapshot.status = TaskStatus::Queued;
        task.snapshot.last_event = Some(last_event.to_string());
        task.snapshot.total_segments = 0;
        task.snapshot.downloaded_segments = 0;
        task.snapshot.downloaded_bytes = 0;
        task.snapshot.progress = 0.0;
        task.snapshot.speed_bps = 0.0;
        task.snapshot.eta_seconds = None;
        task.snapshot.average_segment_size_bytes = None;
        task.snapshot.retries = 0;
        task.snapshot.playlist_type = "unknown".into();
        task.snapshot.segments.clear();
        task.snapshot.encryption = None;
        task.snapshot.output_path = None;
        task.started_at = None;
        task.bytes_at_start = 0;

        self.broadcast_task(task_id, &task.snapshot);

        config.resume = None;

        Ok((config, output_path))
    }

    fn broadcast_task(&self, task_id: TaskId, snapshot: &TaskSnapshot) {
        if let Err(err) = self.inner.websocket.send(FrontendEvent::TaskUpdated {
            task: snapshot.clone(),
        }) {
            warn!("broadcast error for {task_id}: {err:?}");
        }
    }

    fn prep_default_dir(&self) -> PathBuf {
        self.inner
            .app_handle
            .path()
            .app_data_dir()
            .unwrap_or_else(|_| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
    }

    async fn prep_save_dir(&self, custom: &str) -> Result<PathBuf> {
        let base = if custom.trim().is_empty() {
            self.prep_default_dir()
        } else {
            Path::new(custom.trim()).to_path_buf()
        };
        tokio::fs::create_dir_all(&base).await?;
        Ok(base)
    }
}

#[derive(Serialize, Deserialize)]
struct PersistedState {
    tasks: Vec<PersistedTask>,
}

#[derive(Serialize, Deserialize)]
struct PersistedTask {
    snapshot: TaskSnapshot,
    config: PersistedConfig,
}

impl PersistedTask {
    fn from_managed(managed: &ManagedTask) -> Self {
        let mut snapshot = managed.snapshot.clone();
        if snapshot.logs.len() > MAX_LOGS {
            let keep = snapshot.logs.len().saturating_sub(MAX_LOGS);
            snapshot.logs = snapshot.logs.split_off(keep);
        }
        let mut runtime_config = managed.config.clone();
        if runtime_config.resume.is_none() {
            runtime_config.resume = resume_state_from_snapshot(&snapshot);
        }
        Self {
            snapshot,
            config: PersistedConfig::from_runtime(&runtime_config),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct PersistedConfig {
    task_id: String,
    title: String,
    playlist_url: String,
    save_dir: String,
    preferred_quality: Option<String>,
    referer: Option<String>,
    custom_headers: Option<String>,
    cookies: Option<String>,
    manual_key: Option<String>,
    manual_iv: Option<String>,
    concurrency: u8,
    user_agent: String,
    #[serde(default)]
    resume: Option<PersistedResumeState>,
}

#[derive(Serialize, Deserialize)]
struct PersistedResumeState {
    completed: Vec<u64>,
    output_path: String,
}

impl PersistedConfig {
    fn from_runtime(config: &HlsDownloadConfig) -> Self {
        Self {
            task_id: config.task_id.to_string(),
            title: config.title.clone(),
            playlist_url: config.playlist_url.clone(),
            save_dir: config.save_dir.display().to_string(),
            preferred_quality: config.preferred_quality.clone(),
            referer: config.referer.clone(),
            custom_headers: config.custom_headers.clone(),
            cookies: config.cookies.clone(),
            manual_key: config
                .manual_key
                .as_ref()
                .map(|bytes| general_purpose::STANDARD.encode(bytes)),
            manual_iv: config
                .manual_iv
                .as_ref()
                .map(|bytes| general_purpose::STANDARD.encode(bytes)),
            concurrency: config.concurrency,
            user_agent: config.user_agent.clone(),
            resume: config.resume.as_ref().map(|state| PersistedResumeState {
                completed: state.completed.clone(),
                output_path: state.output_path.display().to_string(),
            }),
        }
    }

    fn into_runtime(self) -> Result<HlsDownloadConfig> {
        let task_id = uuid::Uuid::parse_str(&self.task_id)?;
        let manual_key = match self.manual_key {
            Some(value) => Some(
                general_purpose::STANDARD
                    .decode(value)
                    .map_err(|e| anyhow!("无效的 KEY 编码: {e}"))?,
            ),
            None => None,
        };
        let manual_iv = match self.manual_iv {
            Some(value) => Some(
                general_purpose::STANDARD
                    .decode(value)
                    .map_err(|e| anyhow!("无效的 IV 编码: {e}"))?,
            ),
            None => None,
        };

        let resume = self.resume.map(|state| ResumeState {
            completed: state.completed,
            output_path: PathBuf::from(state.output_path),
        });

        Ok(HlsDownloadConfig {
            task_id,
            title: self.title,
            playlist_url: self.playlist_url,
            save_dir: PathBuf::from(self.save_dir),
            preferred_quality: self.preferred_quality,
            referer: self.referer,
            custom_headers: self.custom_headers,
            cookies: self.cookies,
            manual_key,
            manual_iv,
            concurrency: self.concurrency,
            user_agent: self.user_agent,
            resume,
        })
    }
}

fn resume_state_from_snapshot(snapshot: &TaskSnapshot) -> Option<ResumeState> {
    if snapshot.total_segments == 0 {
        return None;
    }
    let path = snapshot.output_path.as_ref()?;
    if path.trim().is_empty() {
        return None;
    }

    let mut completed: Vec<u64> = snapshot
        .segments
        .iter()
        .filter(|seg| seg.status == SegmentStatus::Completed)
        .map(|seg| seg.seq)
        .collect();

    if completed.is_empty() || completed.len() as u64 == snapshot.total_segments {
        return None;
    }

    completed.sort_unstable();
    completed.dedup();

    Some(ResumeState {
        completed,
        output_path: PathBuf::from(path),
    })
}

fn decode_optional_bytes(value: Option<&str>) -> Result<Option<Vec<u8>>> {
    match value {
        None => Ok(None),
        Some(input) => {
            let trimmed = input.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            if let Ok(bytes) = hex::decode(trimmed.trim_start_matches("0x")) {
                return Ok(Some(bytes));
            }
            if let Ok(bytes) = general_purpose::STANDARD.decode(trimmed) {
                return Ok(Some(bytes));
            }
            Err(anyhow!("无法解析密钥/IV，请使用 HEX 或 Base64 编码"))
        }
    }
}

#[derive(Clone)]
pub struct PlaylistMeta {
    pub method: Option<String>,
    pub key_uri: Option<String>,
    pub quality: Option<String>,
    pub resolution: Option<String>,
    pub audio: Option<String>,
}
