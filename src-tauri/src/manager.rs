use crate::events::{
    EncryptionSnapshot, EncryptionStatus, FrontendEvent, SegmentSnapshot, SegmentStatus, TaskId,
    TaskLog, TaskSnapshot, TaskStatus,
};
use crate::hls::{run_hls_download, HlsDownloadConfig};
use crate::websocket::WebSocketState;
use anyhow::{anyhow, Result};
use base64::{engine::general_purpose, Engine as _};
use chrono::Utc;
use parking_lot::RwLock;
use serde::Deserialize;
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tauri::{async_runtime::JoinHandle, AppHandle, Manager};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

const MAX_LOGS: usize = 100;

#[derive(Clone)]
pub struct DownloadManager {
    inner: Arc<ManagerInner>,
}

struct ManagerInner {
    tasks: RwLock<HashMap<TaskId, ManagedTask>>,
    websocket: WebSocketState,
    app_handle: AppHandle,
}

struct ManagedTask {
    snapshot: TaskSnapshot,
    config: HlsDownloadConfig,
    cancel_token: CancellationToken,
    handle: Option<JoinHandle<()>>,
}

#[derive(Debug, Deserialize)]
pub struct NewTaskRequest {
    pub playlist_url: String,
    pub preferred_quality: Option<String>,
    pub save_dir: String,
    pub referer: Option<String>,
    pub custom_headers: Option<String>,
    pub cookies: Option<String>,
    pub manual_key: Option<String>,
    pub manual_iv: Option<String>,
    pub title: Option<String>,
    pub max_concurrency: u8,
}

impl DownloadManager {
    pub fn new(app_handle: AppHandle, websocket: WebSocketState) -> Self {
        Self {
            inner: Arc::new(ManagerInner {
                tasks: RwLock::new(HashMap::new()),
                websocket,
                app_handle,
            }),
        }
    }

    pub async fn create_task(&self, request: NewTaskRequest) -> Result<TaskId> {
        if request.playlist_url.trim().is_empty() {
            return Err(anyhow!("请提供有效的 m3u8 链接"));
        }

        let playlist_url = url::Url::parse(request.playlist_url.trim())?;
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
            referer: request.referer,
            custom_headers: request.custom_headers,
            cookies: request.cookies,
            manual_key,
            manual_iv,
            concurrency: request.max_concurrency.clamp(1, 16),
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
                },
            );
        }

        self.inner
            .websocket
            .send(FrontendEvent::TaskCreated { task: snapshot })
            .ok();

        self.spawn_download(id, config);
        Ok(id)
    }

    pub async fn pause_task(&self, task_id: TaskId) -> Result<()> {
        {
            let mut guard = self.inner.tasks.write();
            let Some(task) = guard.get_mut(&task_id) else {
                return Err(anyhow!("task not found"));
            };
            task.snapshot.status = TaskStatus::Paused;
            task.snapshot.last_event = Some("任务已暂停".into());
            task.cancel_token.cancel();
            if let Some(handle) = task.handle.take() {
                handle.abort();
            }
            self.broadcast_task(task_id, &task.snapshot);
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
            self.broadcast_task(task_id, &task.snapshot);
            task.config.clone()
        };
        self.spawn_download(task_id, config);
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
        {
            let mut guard = self.inner.tasks.write();
            if let Some(mut task) = guard.remove(&task_id) {
                task.cancel_token.cancel();
                if let Some(handle) = task.handle.take() {
                    handle.abort();
                }
            }
        }
        Ok(())
    }

    pub async fn snapshot(&self) -> Result<Vec<TaskSnapshot>> {
        let guard = self.inner.tasks.read();
        Ok(guard.values().map(|task| task.snapshot.clone()).collect())
    }

    pub async fn mark_playlist_resolved(
        &self,
        task_id: TaskId,
        playlist_type: String,
        total_segments: usize,
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
            task.snapshot.encryption = meta.key_uri.map(|uri| EncryptionSnapshot {
                method: meta.method,
                key_uri: Some(uri),
                status: EncryptionStatus::Unknown,
                iv: None,
                last_error: None,
            });
            task.snapshot.segments = (0..total_segments as u64)
                .map(|seq| SegmentSnapshot {
                    seq,
                    status: SegmentStatus::Pending,
                    speed_bps: None,
                    size_bytes: None,
                    retries: 0,
                    last_updated: None,
                })
                .collect();
            task.snapshot.clone()
        };
        self.inner
            .websocket
            .send(FrontendEvent::TaskUpdated { task: snapshot })
            .ok();
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
            if task.snapshot.total_segments > 0 {
                task.snapshot.progress = (task.snapshot.downloaded_segments as f32
                    / task.snapshot.total_segments as f32)
                    * 100.0;
            }
            task.snapshot.speed_bps = speed_bps;
            task.snapshot.average_segment_size_bytes =
                Some(match task.snapshot.average_segment_size_bytes {
                    Some(prev) => {
                        ((prev * (task.snapshot.downloaded_segments - 1)) + bytes)
                            / task.snapshot.downloaded_segments
                    }
                    None => bytes,
                });
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
            task.snapshot.last_event = Some("任务已完成".into());
            task.snapshot.clone()
        };
        self.inner
            .websocket
            .send(FrontendEvent::TaskCompleted {
                task_id: task_id.to_string(),
                task: snapshot,
            })
            .ok();
        Ok(())
    }

    pub async fn mark_task_error(&self, task_id: TaskId, message: String) -> Result<()> {
        self.push_log(task_id, format!("任务异常: {message}"))
            .await
            .ok();
        {
            let mut guard = self.inner.tasks.write();
            if let Some(task) = guard.get_mut(&task_id) {
                task.snapshot.status = TaskStatus::Error;
                task.snapshot.last_event = Some(message.clone());
            }
        }
        self.inner
            .websocket
            .send(FrontendEvent::TaskError {
                task_id: task_id.to_string(),
                message,
            })
            .ok();
        Ok(())
    }

    fn spawn_download(&self, task_id: TaskId, config: HlsDownloadConfig) {
        let manager = self.clone();
        let cancel = {
            let mut guard = self.inner.tasks.write();
            if let Some(task) = guard.get_mut(&task_id) {
                if let Some(handle) = task.handle.take() {
                    handle.abort();
                }
                task.cancel_token.cancel();
                task.cancel_token = CancellationToken::new();
                task.cancel_token.clone()
            } else {
                CancellationToken::new()
            }
        };

        let handle = tauri::async_runtime::spawn(async move {
            if let Err(err) =
                run_hls_download(manager.clone(), config.clone(), cancel.clone()).await
            {
                if cancel.is_cancelled() {
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
            task.snapshot.status = TaskStatus::Downloading;
            task.snapshot.last_event = Some("开始下载分片".into());
            self.broadcast_task(task_id, &task.snapshot);
        }
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
