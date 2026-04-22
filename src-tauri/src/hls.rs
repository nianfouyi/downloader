use crate::events::{EncryptionSnapshot, EncryptionStatus, SegmentStatus, TaskId};
use crate::manager::{DownloadManager, PlaylistMeta};
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use cipher::block_padding::NoPadding;
use cipher::{BlockDecryptMut, KeyIvInit};
use futures::stream::{self, StreamExt};
use hex::encode as hex_encode;
use m3u8_rs::{
    KeyMethod, MasterPlaylist, MediaPlaylist, MediaPlaylistType, Playlist, VariantStream,
};
use reqwest::header::{
    HeaderMap, HeaderName, HeaderValue, ACCEPT, ACCEPT_ENCODING, COOKIE, REFERER, USER_AGENT,
};
use reqwest::{Client, StatusCode};
use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use url::Url;

type Aes128CbcDec = cbc::Decryptor<aes::Aes128>;

const DEFAULT_USER_AGENT: &str =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chromium/123.0.0.0 Safari/537.36";
const RETRY_BACKOFF_BASE_MS: u64 = 1_000;
const RETRY_BACKOFF_MAX_MS: u64 = 15_000;
const MAX_HTTP_ERROR_BODY_CHARS: usize = 240;

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct HlsDownloadConfig {
    pub task_id: uuid::Uuid,
    pub title: String,
    pub playlist_url: String,
    pub save_dir: PathBuf,
    pub preferred_quality: Option<String>,
    pub referer: Option<String>,
    pub custom_headers: Option<String>,
    pub cookies: Option<String>,
    pub manual_key: Option<Vec<u8>>,
    pub manual_iv: Option<Vec<u8>>,
    pub concurrency: u8,
    pub user_agent: String,
    pub resume: Option<ResumeState>,
}

#[derive(Clone, Debug)]
pub struct ResumeState {
    pub completed: Vec<u64>,
    pub output_path: PathBuf,
}

#[derive(Clone)]
struct SegmentEntry {
    seq: u64,
    uri: Url,
    cipher: Option<CipherSpec>,
}

#[derive(Clone)]
enum CipherSpec {
    Manual {
        key: Arc<Vec<u8>>,
        iv_override: Option<[u8; 16]>,
    },
    Remote {
        key: Arc<Vec<u8>>,
        iv_override: Option<[u8; 16]>,
    },
}

struct SegmentDownload {
    seq: u64,
    bytes: Vec<u8>,
    size_bytes: u64,
    speed_bps: f64,
}

struct SegmentError {
    seq: u64,
    error: anyhow::Error,
}

impl SegmentError {
    fn new(seq: u64, error: anyhow::Error) -> Self {
        Self { seq, error }
    }
}

pub async fn run_hls_download(
    manager: DownloadManager,
    mut config: HlsDownloadConfig,
    cancel: CancellationToken,
) -> Result<()> {
    let client = build_client(&config)?;
    let playlist_url =
        Url::parse(&config.playlist_url).context("无法解析 m3u8 链接，请确认地址是否有效")?;

    manager
        .push_log(
            config.task_id,
            format!("开始解析 m3u8 清单: {}", playlist_url),
        )
        .await?;

    let playlist_bytes = fetch_bytes(&client, &playlist_url, &cancel)
        .await
        .with_context(|| "请求 m3u8 清单失败")?;

    let playlist = parse_playlist(&playlist_bytes)?;

    let (media_playlist, variant_meta, media_url) = match playlist {
        Playlist::MasterPlaylist(master) => {
            handle_master_playlist(&client, &manager, &config, master, playlist_url, &cancel)
                .await?
        }
        Playlist::MediaPlaylist(media) => (media, VariantMeta::default(), playlist_url),
    };

    if media_playlist.segments.is_empty() {
        return Err(anyhow!("解析到的媒体清单为空，未找到可下载的分片"));
    }

    let mut resume_state = config.resume.clone();
    let temp_output_path = prepare_output(&config, "ts").await?;
    let final_output_path = prepare_output(&config, "mp4").await?;

    let (output_path, mut writer, has_existing_bytes) = if let Some(mut resume) = resume_state.clone()
    {
        let resume_output_path =
            reconcile_resume_output_path(&resume.output_path, &temp_output_path).await?;
        resume.output_path = resume_output_path.clone();
        match OpenOptions::new()
            .write(true)
            .append(true)
            .open(&resume.output_path)
            .await
        {
            Ok(file) => {
                resume_state = Some(resume.clone());
                let has_existing = match file.metadata().await {
                    Ok(meta) => meta.len() > 0,
                    Err(err) => {
                        warn!(
                            "failed to inspect existing output {}: {err:?}",
                            resume.output_path.display()
                        );
                        false
                    }
                };
                (resume.output_path.clone(), file, has_existing)
            }
            Err(err) => {
                warn!("resume output missing or inaccessible: {err:?}, 将重新下载");
                resume_state = None;
                config.resume = None;
                let file = File::create(&temp_output_path)
                    .await
                    .with_context(|| format!("无法创建输出文件 {}", temp_output_path.display()))?;
                (temp_output_path.clone(), file, false)
            }
        }
    } else {
        let file = File::create(&temp_output_path)
            .await
            .with_context(|| format!("无法创建输出文件 {}", temp_output_path.display()))?;
        (temp_output_path.clone(), file, false)
    };

    let should_write_map = !has_existing_bytes;

    manager
        .ensure_output_path(config.task_id, &output_path)
        .await
        .ok();

    config.resume = resume_state.clone();

    if let Some(resume) = resume_state.as_ref() {
        manager
            .push_log(
                config.task_id,
                format!(
                    "检测到 {} 个分片已完成，继续下载剩余部分",
                    resume.completed.len()
                ),
            )
            .await
            .ok();
    }

    let playlist_type = playlist_type_label(&media_playlist);

    let resolved_meta = build_playlist_meta(&config, &variant_meta, &media_playlist);
    let playlist_start_seq = media_playlist.media_sequence;

    manager
        .mark_playlist_resolved(
            config.task_id,
            playlist_type,
            media_playlist.segments.len(),
            playlist_start_seq,
            resolved_meta,
        )
        .await?;

    if should_write_map {
        if let Some(map) = media_playlist
            .segments
            .iter()
            .find_map(|segment| segment.map.as_ref())
        {
            let map_url = resolve_url(&media_url, &map.uri)?;
            manager
                .push_log(
                    config.task_id,
                    format!("下载初始化片头: {}", map_url.as_str()),
                )
                .await?;
            let bytes = fetch_bytes(&client, &map_url, &cancel)
                .await
                .with_context(|| "下载初始化片头失败")?;
            writer
                .write_all(&bytes)
                .await
                .context("写入初始化片头失败")?;
        }
    }

    let segments = build_segments(
        &manager,
        &client,
        &config,
        &media_playlist,
        &media_url,
        resume_state.as_ref(),
        &cancel,
    )
    .await?;

    if segments.is_empty() {
        let completed_output =
            finalize_output_path(&output_path, &final_output_path).await?;
        manager
            .push_log(config.task_id, "检测到所有分片已完成，跳过下载")
            .await
            .ok();
        manager
            .mark_task_completed(config.task_id, completed_output)
            .await?;
        return Ok(());
    }

    if cancel.is_cancelled() {
        manager
            .push_log(config.task_id, "任务已取消，可在稍后恢复下载")
            .await
            .ok();
        return Ok(());
    }

    manager
        .push_log(
            config.task_id,
            format!("共解析到 {} 个分片，开始下载", segments.len()),
        )
        .await?;

    download_segments(&manager, &client, &config, segments, &mut writer, &cancel).await?;

    if cancel.is_cancelled() {
        info!(task_id = %config.task_id, "download cancelled before completion");
        return Ok(());
    }

    writer.flush().await.ok();

    let completed_output = finalize_output_path(&output_path, &final_output_path).await?;

    manager
        .push_log(
            config.task_id,
            format!("下载完成，输出文件 {}", completed_output.display()),
        )
        .await?;
    manager
        .mark_task_completed(config.task_id, completed_output)
        .await?;
    Ok(())
}

async fn download_segments(
    manager: &DownloadManager,
    client: &Client,
    config: &HlsDownloadConfig,
    segments: Vec<SegmentEntry>,
    writer: &mut File,
    cancel: &CancellationToken,
) -> Result<()> {
    let concurrency = config.concurrency.clamp(1, 16) as usize;
    if segments.is_empty() {
        return Ok(());
    }
    let start_seq = segments.first().map(|seg| seg.seq).unwrap_or_default();
    let mut pending = BTreeMap::<u64, SegmentDownload>::new();
    let mut expected_seq = start_seq;
    let manager_clone = manager.clone();

    let mut stream = stream::iter(segments.into_iter().map(|segment| {
        let client = client.clone();
        let manager = manager_clone.clone();
        let cancel = cancel.clone();
        async move {
            download_segment_with_retry(&manager, &client, config.task_id, segment, cancel.clone())
                .await
        }
    }))
    .buffer_unordered(concurrency);

    let mut cancelled = false;

    while let Some(result) = stream.next().await {
        if manager.is_task_paused(config.task_id) {
            cancel.cancel();
            cancelled = true;
        }

        match result {
            Ok(Some(download)) => {
                pending.insert(download.seq, download);
                while let Some(ready) = pending.remove(&expected_seq) {
                    if cancel.is_cancelled() || manager.is_task_paused(config.task_id) {
                        cancelled = true;
                        break;
                    }
                    writer
                        .write_all(&ready.bytes)
                        .await
                        .with_context(|| format!("写入分片 {} 失败", ready.seq))?;
                    manager
                        .mark_segment_complete(
                            config.task_id,
                            ready.seq,
                            ready.size_bytes,
                            ready.speed_bps,
                        )
                        .await?;
                    expected_seq += 1;
                }
            }
            Ok(None) => {
                cancelled = true;
                break;
            }
            Err(err) => {
                manager
                    .update_segment(config.task_id, err.seq, SegmentStatus::Failed, None, None)
                    .await
                    .ok();
                manager
                    .push_log(
                        config.task_id,
                        format!("分片 {} 下载失败: {}", err.seq, err.error),
                    )
                    .await
                    .ok();
                return Err(err.error);
            }
        }

        if cancelled || cancel.is_cancelled() {
            break;
        }
    }

    if cancelled || cancel.is_cancelled() {
        let message = if manager.is_task_paused(config.task_id) {
            "任务已暂停，可在稍后恢复下载"
        } else {
            "任务已取消，可在稍后恢复下载"
        };
        manager.push_log(config.task_id, message).await.ok();
        return Ok(());
    }

    Ok(())
}

fn backoff_delay(attempt: u32) -> Duration {
    let factor = 1u64 << attempt.min(12);
    let ms = (RETRY_BACKOFF_BASE_MS * factor).min(RETRY_BACKOFF_MAX_MS);
    Duration::from_millis(ms)
}

async fn download_segment_with_retry(
    manager: &DownloadManager,
    client: &Client,
    task_id: TaskId,
    entry: SegmentEntry,
    cancel: CancellationToken,
) -> Result<Option<SegmentDownload>, SegmentError> {
    if cancel.is_cancelled() {
        return Ok(None);
    }

    if manager.is_task_paused(task_id) {
        cancel.cancel();
        return Ok(None);
    }

    let mut attempt: u32 = 0;
    loop {
        if cancel.is_cancelled() {
            return Ok(None);
        }

        if manager.is_task_paused(task_id) {
            cancel.cancel();
            return Ok(None);
        }

        if attempt > 0 {
            manager
                .mark_segment_retrying(task_id, entry.seq)
                .await
                .map_err(|err| SegmentError::new(entry.seq, err.into()))?;
        }

        let result = tokio::select! {
            _ = cancel.cancelled() => Ok(None),
            res = download_segment_once(
                manager,
                client,
                task_id,
                entry.clone(),
                cancel.clone(),
            ) => res,
        };

        match result {
            Ok(result) => {
                match result {
                    Some(download) => {
                        if attempt > 0 {
                            manager
                                .push_log(task_id, format!("分片 {} 重试成功", entry.seq))
                                .await
                                .map_err(|err| SegmentError::new(entry.seq, err.into()))?;
                        }
                        // reset attempt counter on success for good measure
                        return Ok(Some(download));
                    }
                    None => return Ok(None),
                }
            }
            Err(err) => {
                let mut fatal = false;
                if let Some(req_err) = err.error.downcast_ref::<reqwest::Error>() {
                    if let Some(status) = req_err.status() {
                        if status.is_client_error()
                            && status != StatusCode::REQUEST_TIMEOUT
                            && status != StatusCode::TOO_MANY_REQUESTS
                        {
                            fatal = true;
                        }
                    }
                }
                if fatal {
                    return Err(err);
                }
                let error_message = (&err.error).to_string();
                let delay = backoff_delay(attempt);
                let delay_secs = delay.as_secs_f64();
                let retry_index = attempt + 1;
                manager
                    .push_log(
                        task_id,
                        format!(
                            "分片 {} 下载失败（第 {} 次）：{}，将在 {:.1} 秒后重试",
                            entry.seq, retry_index, error_message, delay_secs
                        ),
                    )
                    .await
                    .map_err(|err| SegmentError::new(entry.seq, err.into()))?;

                sleep(delay).await;
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

async fn download_segment_once(
    manager: &DownloadManager,
    client: &Client,
    task_id: TaskId,
    entry: SegmentEntry,
    cancel: CancellationToken,
) -> Result<Option<SegmentDownload>, SegmentError> {
    if cancel.is_cancelled() {
        return Ok(None);
    }

    if manager.is_task_paused(task_id) {
        cancel.cancel();
        return Ok(None);
    }

    manager
        .update_segment(task_id, entry.seq, SegmentStatus::Downloading, None, None)
        .await
        .map_err(|err| SegmentError::new(entry.seq, err.into()))?;

    let start = Instant::now();

    let response = client
        .get(entry.uri.clone())
        .send()
        .await
        .map_err(|err| SegmentError::new(entry.seq, anyhow!(err)))?;
    let response = response
        .error_for_status()
        .map_err(|err| SegmentError::new(entry.seq, anyhow!(err)))?;

    if cancel.is_cancelled() {
        return Ok(None);
    }

    if manager.is_task_paused(task_id) {
        cancel.cancel();
        return Ok(None);
    }

    let mut bytes = response
        .bytes()
        .await
        .map_err(|err| SegmentError::new(entry.seq, anyhow!(err)))?
        .to_vec();

    if let Some(cipher) = entry.cipher.clone() {
        let (key_arc, iv) = match cipher {
            CipherSpec::Manual { key, iv_override } => {
                if key.len() != 16 {
                    return Err(SegmentError::new(
                        entry.seq,
                        anyhow!("手动提供的 KEY 必须为 16 字节"),
                    ));
                }
                let iv = iv_override.unwrap_or_else(|| generate_iv(entry.seq));
                (key, iv)
            }
            CipherSpec::Remote { key, iv_override } => {
                let iv = iv_override.unwrap_or_else(|| generate_iv(entry.seq));
                (key, iv)
            }
        };
        let key_slice: &[u8] = key_arc.as_ref();
        decrypt_segment(&mut bytes, key_slice, &iv)
            .map_err(|err| SegmentError::new(entry.seq, err))?;
    }

    if cancel.is_cancelled() {
        return Ok(None);
    }

    if manager.is_task_paused(task_id) {
        cancel.cancel();
        return Ok(None);
    }

    let elapsed = start.elapsed().as_secs_f64().max(0.001);
    let size_bytes = bytes.len() as u64;
    let speed_bps = size_bytes as f64 / elapsed;

    if cancel.is_cancelled() {
        return Ok(None);
    }

    Ok(Some(SegmentDownload {
        seq: entry.seq,
        bytes,
        size_bytes,
        speed_bps,
    }))
}

fn decrypt_segment(data: &mut Vec<u8>, key: &[u8], iv: &[u8; 16]) -> Result<()> {
    let cipher =
        Aes128CbcDec::new_from_slices(key, iv).map_err(|err| anyhow!("初始化解密器失败: {err}"))?;
    cipher
        .decrypt_padded_mut::<NoPadding>(data)
        .map_err(|_| anyhow!("分片数据解密失败，密钥或 IV 可能不正确"))?;
    Ok(())
}

async fn build_segments(
    manager: &DownloadManager,
    client: &Client,
    config: &HlsDownloadConfig,
    media_playlist: &MediaPlaylist,
    media_url: &Url,
    resume: Option<&ResumeState>,
    cancel: &CancellationToken,
) -> Result<Vec<SegmentEntry>> {
    let mut entries = Vec::with_capacity(media_playlist.segments.len());
    let start_seq = media_playlist.media_sequence;
    let mut key_cache = KeyCache::default();

    let manual_cipher = if let Some(ref manual_key) = config.manual_key {
        if manual_key.len() != 16 {
            return Err(anyhow!("自定义 KEY 需为 16 字节的 AES-128 密钥"));
        }

        let iv = match &config.manual_iv {
            Some(bytes) if bytes.len() == 16 => {
                let mut arr = [0u8; 16];
                arr.copy_from_slice(bytes);
                Some(arr)
            }
            Some(_) => {
                return Err(anyhow!("自定义 IV 必须为 16 字节"));
            }
            None => None,
        };

        manager
            .update_encryption_status(
                config.task_id,
                EncryptionSnapshot {
                    method: Some("manual".into()),
                    key_uri: None,
                    status: EncryptionStatus::Ready,
                    iv: iv.map(hex_encode),
                    last_error: None,
                },
            )
            .await?;

        Some(CipherSpec::Manual {
            key: Arc::new(manual_key.clone()),
            iv_override: iv,
        })
    } else {
        None
    };

    let mut active_cipher = manual_cipher.clone();

    for (idx, segment) in media_playlist.segments.iter().enumerate() {
        if cancel.is_cancelled() {
            break;
        }

        if manual_cipher.is_none() {
            if let Some(ref key_tag) = segment.key {
                match &key_tag.method {
                    KeyMethod::None => {
                        active_cipher = None;
                    }
                    KeyMethod::AES128 => {
                        let uri = key_tag
                            .uri
                            .as_ref()
                            .ok_or_else(|| anyhow!("密钥声明缺少 URI (EXT-X-KEY)"))?;
                        let key_url = resolve_url(media_url, uri)?;
                        let method_name = key_tag.method.to_string();
                        let key_bytes = key_cache
                            .get_or_fetch(
                                &manager,
                                config.task_id,
                                &method_name,
                                key_tag.iv.as_deref(),
                                &key_url,
                                client,
                                cancel,
                            )
                            .await?;
                        let iv = match key_tag.iv.as_deref() {
                            Some(raw) => Some(parse_iv(raw)?),
                            None => None,
                        };
                        active_cipher = Some(CipherSpec::Remote {
                            key: key_bytes,
                            iv_override: iv,
                        });
                    }
                    KeyMethod::SampleAES => {
                        return Err(anyhow!("暂不支持 SAMPLE-AES 加密的 HLS 流"));
                    }
                    KeyMethod::Other(name) => {
                        return Err(anyhow!("暂不支持的加密方式: {}", name));
                    }
                }
            }
        }

        let seq = start_seq + idx as u64;
        let uri = resolve_url(media_url, &segment.uri)?;
        entries.push(SegmentEntry {
            seq,
            uri,
            cipher: active_cipher.clone(),
        });
    }

    if let Some(resume) = resume {
        let completed: HashSet<u64> = resume.completed.iter().copied().collect();
        entries.retain(|entry| !completed.contains(&entry.seq));
    }

    Ok(entries)
}

#[derive(Default, Clone)]
struct KeyCache {
    entries: Vec<CachedKey>,
}

#[derive(Clone)]
struct CachedKey {
    uri: String,
    key: Arc<Vec<u8>>,
}

impl KeyCache {
    async fn get_or_fetch(
        &mut self,
        manager: &DownloadManager,
        task_id: TaskId,
        method: &str,
        iv: Option<&str>,
        url: &Url,
        client: &Client,
        cancel: &CancellationToken,
    ) -> Result<Arc<Vec<u8>>> {
        if let Some(entry) = self.entries.iter().find(|entry| entry.uri == url.as_str()) {
            return Ok(entry.key.clone());
        }

        manager
            .update_encryption_status(
                task_id,
                EncryptionSnapshot {
                    method: Some(method.into()),
                    key_uri: Some(url.to_string()),
                    status: EncryptionStatus::Fetching,
                    iv: iv.map(|v| v.to_string()),
                    last_error: None,
                },
            )
            .await?;

        let bytes = match fetch_bytes(client, url, cancel).await {
            Ok(data) => data,
            Err(err) => {
                let message = err.to_string();
                manager
                    .update_encryption_status(
                        task_id,
                        EncryptionSnapshot {
                            method: Some(method.into()),
                            key_uri: Some(url.to_string()),
                            status: EncryptionStatus::Failed,
                            iv: iv.map(|v| v.to_string()),
                            last_error: Some(message.clone()),
                        },
                    )
                    .await
                    .ok();
                return Err(err.context(format!("获取密钥失败: {}", url)));
            }
        };
        if bytes.len() != 16 {
            manager
                .update_encryption_status(
                    task_id,
                    EncryptionSnapshot {
                        method: Some(method.into()),
                        key_uri: Some(url.to_string()),
                        status: EncryptionStatus::Failed,
                        iv: iv.map(|v| v.to_string()),
                        last_error: Some(format!(
                            "密钥长度不正确 ({} 字节)，期望 16 字节",
                            bytes.len()
                        )),
                    },
                )
                .await
                .ok();
            return Err(anyhow!(
                "密钥长度不正确，期望 16 字节，实际 {} 字节",
                bytes.len()
            ));
        }

        let key_bytes = Arc::new(bytes.to_vec());

        manager
            .update_encryption_status(
                task_id,
                EncryptionSnapshot {
                    method: Some(method.into()),
                    key_uri: Some(url.to_string()),
                    status: EncryptionStatus::Ready,
                    iv: iv.map(|v| v.to_string()),
                    last_error: None,
                },
            )
            .await
            .ok();

        self.entries.push(CachedKey {
            uri: url.to_string(),
            key: key_bytes.clone(),
        });

        Ok(key_bytes)
    }
}

fn playlist_type_label(media: &MediaPlaylist) -> String {
    if let Some(pt) = &media.playlist_type {
        match pt {
            MediaPlaylistType::Vod => "vod".into(),
            MediaPlaylistType::Event => "event".into(),
            MediaPlaylistType::Other(other) => other.clone(),
        }
    } else if media.end_list {
        "vod".into()
    } else {
        "live".into()
    }
}

fn build_playlist_meta(
    config: &HlsDownloadConfig,
    variant_meta: &VariantMeta,
    media: &MediaPlaylist,
) -> PlaylistMeta {
    let derived_key = media.segments.iter().find_map(|segment| {
        let key = segment.key.as_ref()?;
        match &key.method {
            KeyMethod::None => None,
            other => Some((other.to_string(), key.uri.clone())),
        }
    });

    let method_field = if config.manual_key.is_some() {
        Some("manual".into())
    } else {
        derived_key
            .as_ref()
            .map(|(method, _)| method.clone())
            .or_else(|| variant_meta.method.clone())
    };

    let key_uri_field = if config.manual_key.is_some() {
        None
    } else {
        derived_key
            .as_ref()
            .and_then(|(_, uri)| uri.clone())
            .or_else(|| variant_meta.key_uri.clone())
    };

    PlaylistMeta {
        method: method_field,
        key_uri: key_uri_field,
        quality: config
            .preferred_quality
            .clone()
            .or_else(|| variant_meta.quality.clone()),
        resolution: variant_meta.resolution.clone(),
        audio: variant_meta.audio.clone(),
    }
}

#[derive(Default, Clone)]
struct VariantMeta {
    quality: Option<String>,
    resolution: Option<String>,
    audio: Option<String>,
    method: Option<String>,
    key_uri: Option<String>,
}

async fn handle_master_playlist(
    client: &Client,
    manager: &DownloadManager,
    config: &HlsDownloadConfig,
    master: MasterPlaylist,
    playlist_url: Url,
    cancel: &CancellationToken,
) -> Result<(MediaPlaylist, VariantMeta, Url)> {
    let variant = pick_variant(&master, config.preferred_quality.as_deref())
        .ok_or_else(|| anyhow!("未能在主清单中找到合适的码率/清晰度分流"))?;

    let resolution_label = variant
        .resolution
        .map(|res| format!("{}x{}", res.width, res.height));
    let quality_label = resolution_label
        .clone()
        .unwrap_or_else(|| format!("{} kbps", variant.bandwidth / 1000));

    manager
        .push_log(
            config.task_id,
            format!("选择分流: {} -> {}", quality_label, variant.uri),
        )
        .await?;

    let variant_url = resolve_url(&playlist_url, &variant.uri)?;
    let bytes = fetch_bytes(client, &variant_url, cancel)
        .await
        .with_context(|| format!("请求媒体清单失败: {}", variant_url))?;
    let playlist = parse_playlist(&bytes)?;
    let media = match playlist {
        Playlist::MediaPlaylist(media) => media,
        Playlist::MasterPlaylist(_) => {
            return Err(anyhow!("主清单嵌套主清单暂不支持"));
        }
    };

    let meta = VariantMeta {
        quality: Some(quality_label),
        resolution: resolution_label,
        audio: variant.audio.clone(),
        method: None,
        key_uri: None,
    };

    Ok((media, meta, variant_url))
}

fn pick_variant<'a>(
    master: &'a MasterPlaylist,
    preferred: Option<&str>,
) -> Option<&'a VariantStream> {
    if let Some(target) = preferred {
        let target_lower = target.trim().to_ascii_lowercase();
        if let Some(found) = master.variants.iter().find(|variant| {
            if let Some(res) = variant.resolution {
                let res_text = format!("{}x{}", res.width, res.height).to_ascii_lowercase();
                if res_text == target_lower {
                    return true;
                }
                let res_short = format!("{}p", res.height).to_ascii_lowercase();
                if res_short == target_lower {
                    return true;
                }
            }
            if variant.uri.to_ascii_lowercase().contains(&target_lower) {
                return true;
            }
            let bandwidth_kbps = format!("{}", variant.bandwidth / 1000);
            bandwidth_kbps == target_lower
        }) {
            return Some(found);
        }
    }

    master
        .variants
        .iter()
        .max_by_key(|variant| variant.bandwidth)
}

fn parse_playlist(bytes: &Bytes) -> Result<Playlist> {
    let content = std::str::from_utf8(bytes).context("m3u8 内容不是合法的 UTF-8 编码")?;
    let playlist = m3u8_rs::parse_playlist_res(content.as_bytes())
        .map_err(|_| anyhow!("无法解析 m3u8 清单，格式可能不受支持"))?;
    Ok(playlist)
}

async fn fetch_bytes(client: &Client, url: &Url, cancel: &CancellationToken) -> Result<Bytes> {
    let mut attempt: u32 = 0;
    loop {
        if cancel.is_cancelled() {
            return Err(anyhow!("任务已取消"));
        }

        match client.get(url.clone()).send().await {
            Ok(response) => {
                let status = response.status();
                if status.is_success() {
                    match response.bytes().await {
                        Ok(bytes) => {
                            return Ok(bytes);
                        }
                        Err(err) => {
                            warn!(
                                "读取响应体失败 (尝试 {}，状态 {})：{err:?}",
                                attempt + 1,
                                status
                            );
                        }
                    }
                } else if status.is_client_error()
                    && status != StatusCode::REQUEST_TIMEOUT
                    && status != StatusCode::TOO_MANY_REQUESTS
                {
                    let text = response.text().await.unwrap_or_default();
                    return Err(anyhow!(format_http_error(url, status, &text)));
                } else {
                    warn!(
                        "请求 {} 返回状态 {} (尝试 {})，将重试",
                        url,
                        status,
                        attempt + 1
                    );
                }
            }
            Err(err) => {
                warn!("请求发送失败 (尝试 {}): {err:?}", attempt + 1);
            }
        }

        let delay = backoff_delay(attempt);
        sleep(delay).await;
        attempt = attempt.saturating_add(1);
    }
}

fn resolve_url(base: &Url, value: &str) -> Result<Url> {
    base.join(value)
        .with_context(|| format!("无法解析相对路径: {}", value))
}

fn build_client(config: &HlsDownloadConfig) -> Result<Client> {
    let playlist_url = Url::parse(&config.playlist_url).ok();
    let ua_str = config.user_agent.trim();
    let ua = if ua_str.is_empty() {
        DEFAULT_USER_AGENT
    } else {
        ua_str
    };
    let ua_header =
        HeaderValue::from_str(ua).unwrap_or_else(|_| HeaderValue::from_static(DEFAULT_USER_AGENT));

    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, ua_header);
    headers.insert(
        ACCEPT,
        HeaderValue::from_static(
            "application/x-mpegURL, application/vnd.apple.mpegurl, application/octet-stream;q=0.9, */*;q=0.8",
        ),
    );
    headers.insert(
        ACCEPT_ENCODING,
        HeaderValue::from_static("gzip, deflate, br"),
    );
    headers.insert(
        HeaderName::from_static("accept-language"),
        HeaderValue::from_static("zh-CN,zh;q=0.9,en;q=0.8"),
    );
    headers.insert(
        HeaderName::from_static("cache-control"),
        HeaderValue::from_static("no-cache"),
    );
    headers.insert(
        HeaderName::from_static("pragma"),
        HeaderValue::from_static("no-cache"),
    );

    if let Some(referer) = config.referer.as_ref().and_then(|r| {
        if r.trim().is_empty() {
            None
        } else {
            HeaderValue::from_str(r.trim()).ok()
        }
    }) {
        headers.insert(REFERER, referer);
    }
    if let Some(cookies) = config.cookies.as_ref().and_then(|c| {
        if c.trim().is_empty() {
            None
        } else {
            HeaderValue::from_str(c.trim()).ok()
        }
    }) {
        headers.insert(COOKIE, cookies);
    }

    let origin_header = HeaderName::from_static("origin");
    if let Some(custom) = config.custom_headers.as_ref() {
        for line in custom.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            if let Some((name, value)) = trimmed.split_once(':') {
                if let (Ok(name), Ok(value)) = (
                    HeaderName::from_bytes(name.trim().as_bytes()),
                    HeaderValue::from_str(value.trim()),
                ) {
                    headers.insert(name, value);
                }
            }
        }
    }

    if !headers.contains_key(&origin_header) {
        if let Some(origin) = derive_origin_value(config.referer.as_deref(), playlist_url.as_ref())
        {
            if let Ok(value) = HeaderValue::from_str(&origin) {
                headers.insert(origin_header.clone(), value);
            }
        }
    }

    let sec_fetch_mode = HeaderName::from_static("sec-fetch-mode");
    if !headers.contains_key(&sec_fetch_mode) {
        headers.insert(sec_fetch_mode, HeaderValue::from_static("cors"));
    }
    let sec_fetch_dest = HeaderName::from_static("sec-fetch-dest");
    if !headers.contains_key(&sec_fetch_dest) {
        headers.insert(sec_fetch_dest, HeaderValue::from_static("empty"));
    }
    let sec_fetch_site = HeaderName::from_static("sec-fetch-site");
    if !headers.contains_key(&sec_fetch_site) {
        if let Some(value) = derive_sec_fetch_site(config.referer.as_deref(), playlist_url.as_ref())
        {
            headers.insert(sec_fetch_site, HeaderValue::from_static(value));
        }
    }

    Client::builder()
        .user_agent(ua)
        .default_headers(headers)
        .cookie_store(true)
        .redirect(reqwest::redirect::Policy::limited(10))
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|err| err.into())
}

fn derive_origin_value(referer: Option<&str>, fallback_url: Option<&Url>) -> Option<String> {
    referer
        .and_then(parse_origin_from_input)
        .or_else(|| fallback_url.and_then(origin_from_url))
}

fn parse_origin_from_input(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let url = Url::parse(trimmed).ok()?;
    origin_from_url(&url)
}

fn origin_from_url(url: &Url) -> Option<String> {
    if !matches!(url.scheme(), "http" | "https") {
        return None;
    }
    let host = url.host_str()?;
    let mut origin = format!("{}://{}", url.scheme(), host);
    if let Some(port) = url.port() {
        let is_default_port =
            (url.scheme() == "http" && port == 80) || (url.scheme() == "https" && port == 443);
        if !is_default_port {
            origin.push(':');
            origin.push_str(&port.to_string());
        }
    }
    Some(origin)
}

fn derive_sec_fetch_site(referer: Option<&str>, request_url: Option<&Url>) -> Option<&'static str> {
    let referer = referer.and_then(|value| Url::parse(value.trim()).ok())?;
    let request_url = request_url?;
    let is_same_origin = referer.scheme() == request_url.scheme()
        && referer.host_str() == request_url.host_str()
        && referer.port_or_known_default() == request_url.port_or_known_default();
    if is_same_origin {
        Some("same-origin")
    } else {
        Some("cross-site")
    }
}

fn format_http_error(url: &Url, status: StatusCode, body: &str) -> String {
    if is_cloudflare_block(status, body) {
        let summary = summarize_response_body(body, 160);
        return format!(
            "请求 {} 返回 {}，目标站点触发了 Cloudflare 风控页面。当前版本会自动补齐 Referer/Origin、常见浏览器 Header，并保留服务端下发的 Cookie；如果仍然被拦截，请先在浏览器中打开对应视频页面，再把该 m3u8 请求的 Cookie、Origin 或完整请求头粘贴到任务表单中重试。响应摘要: {}",
            url, status, summary
        );
    }

    format!(
        "请求 {} 返回错误状态 {}: {}",
        url,
        status,
        summarize_response_body(body, MAX_HTTP_ERROR_BODY_CHARS)
    )
}

fn is_cloudflare_block(status: StatusCode, body: &str) -> bool {
    if status != StatusCode::FORBIDDEN {
        return false;
    }

    let lower = body.to_ascii_lowercase();
    lower.contains("cloudflare")
        && (lower.contains("attention required")
            || lower.contains("you have been blocked")
            || lower.contains("cf-error-details")
            || lower.contains("please enable cookies"))
}

fn summarize_response_body(body: &str, max_chars: usize) -> String {
    let collapsed = body.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.is_empty() {
        return "<empty body>".into();
    }

    let mut result = String::new();
    for ch in collapsed.chars().take(max_chars) {
        result.push(ch);
    }
    if collapsed.chars().count() > max_chars {
        result.push_str("...");
    }
    result
}

async fn reconcile_resume_output_path(current: &Path, desired: &Path) -> Result<PathBuf> {
    if current == desired {
        return Ok(desired.to_path_buf());
    }

    let current_exists = fs::metadata(current).await.is_ok();
    let desired_exists = fs::metadata(desired).await.is_ok();

    match (current_exists, desired_exists) {
        (true, false) => {
            fs::rename(current, desired).await.with_context(|| {
                format!(
                    "无法将临时输出文件从 {} 重命名为 {}",
                    current.display(),
                    desired.display()
                )
            })?;
            Ok(desired.to_path_buf())
        }
        (false, true) => Ok(desired.to_path_buf()),
        (true, true) => Ok(current.to_path_buf()),
        (false, false) => Ok(desired.to_path_buf()),
    }
}

async fn finalize_output_path(temp_path: &Path, final_path: &Path) -> Result<PathBuf> {
    if temp_path == final_path {
        return Ok(final_path.to_path_buf());
    }

    if fs::metadata(final_path).await.is_ok() {
        fs::remove_file(final_path).await.with_context(|| {
            format!("无法覆盖已存在的输出文件 {}", final_path.display())
        })?;
    }

    fs::rename(temp_path, final_path).await.with_context(|| {
        format!(
            "无法将临时输出文件从 {} 重命名为 {}",
            temp_path.display(),
            final_path.display()
        )
    })?;

    Ok(final_path.to_path_buf())
}

async fn prepare_output(config: &HlsDownloadConfig, extension: &str) -> Result<PathBuf> {
    fs::create_dir_all(&config.save_dir)
        .await
        .with_context(|| format!("无法创建目录 {}", config.save_dir.display()))?;
    let sanitized = sanitize_title(&config.title);
    Ok(config.save_dir.join(format!("{sanitized}.{extension}")))
}

fn sanitize_title(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    for ch in input.chars() {
        if matches!(ch, '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|') {
            result.push('_');
        } else {
            result.push(ch);
        }
    }
    result.trim().trim_end_matches('.').to_string()
}

fn parse_iv(value: &str) -> Result<[u8; 16]> {
    let trimmed = value.trim_start_matches("0x").trim_start_matches("0X");
    let bytes = hex::decode(trimmed).with_context(|| "IV 解析失败，需为十六进制字符串")?;
    if bytes.len() != 16 {
        return Err(anyhow!("IV 长度必须为 16 字节"));
    }
    let mut arr = [0u8; 16];
    arr.copy_from_slice(&bytes);
    Ok(arr)
}

fn generate_iv(seq: u64) -> [u8; 16] {
    let mut iv = [0u8; 16];
    iv[8..].copy_from_slice(&seq.to_be_bytes());
    iv
}

#[cfg(test)]
mod tests {
    use super::{
        derive_origin_value, derive_sec_fetch_site, format_http_error, is_cloudflare_block,
        origin_from_url, summarize_response_body,
    };
    use reqwest::StatusCode;
    use url::Url;

    #[test]
    fn uses_referer_origin_before_playlist_origin() {
        let playlist_url = Url::parse("https://cdn.example.com/video/master.m3u8").unwrap();
        let origin = derive_origin_value(
            Some("https://www.example.com/watch?id=123"),
            Some(&playlist_url),
        );

        assert_eq!(origin.as_deref(), Some("https://www.example.com"));
    }

    #[test]
    fn derives_origin_from_playlist_when_referer_missing() {
        let playlist_url = Url::parse("https://media.example.com:8443/live/720p.m3u8").unwrap();
        let origin = origin_from_url(&playlist_url);

        assert_eq!(origin.as_deref(), Some("https://media.example.com:8443"));
    }

    #[test]
    fn derives_sec_fetch_site_from_referer_and_request() {
        let request = Url::parse("https://cdn.example.com/video/master.m3u8").unwrap();

        assert_eq!(
            derive_sec_fetch_site(Some("https://cdn.example.com/player"), Some(&request)),
            Some("same-origin")
        );
        assert_eq!(
            derive_sec_fetch_site(Some("https://www.example.com/watch"), Some(&request)),
            Some("cross-site")
        );
    }

    #[test]
    fn detects_cloudflare_block_pages() {
        let body = "<html><title>Attention Required! | Cloudflare</title><div id=\"cf-error-details\">Please enable cookies.</div></html>";

        assert!(is_cloudflare_block(StatusCode::FORBIDDEN, body));
    }

    #[test]
    fn formats_cloudflare_error_with_actionable_message() {
        let url = Url::parse("https://surrit.com/video.m3u8").unwrap();
        let message = format_http_error(
            &url,
            StatusCode::FORBIDDEN,
            "<html><title>Attention Required! | Cloudflare</title></html>",
        );

        assert!(message.contains("Cloudflare"));
        assert!(message.contains("Cookie"));
        assert!(message.contains("Origin"));
    }

    #[test]
    fn trims_and_shortens_error_body() {
        let summary = summarize_response_body(" a   b \n c ", 5);

        assert_eq!(summary, "a b c");
    }
}
