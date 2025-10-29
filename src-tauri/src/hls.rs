use crate::events::{EncryptionSnapshot, EncryptionStatus, SegmentStatus};
use crate::manager::{DownloadManager, PlaylistMeta};
use anyhow::Result;
use hex::encode as hex_encode;
use std::path::PathBuf;
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

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
}

pub async fn run_hls_download(
    manager: DownloadManager,
    config: HlsDownloadConfig,
    cancel: CancellationToken,
) -> Result<()> {
    manager
        .push_log(
            config.task_id,
            format!(
                "开始解析 m3u8 清单（模拟），源地址：{}",
                config.playlist_url
            ),
        )
        .await?;

    let total_segments = 24usize;

    manager
        .mark_playlist_resolved(
            config.task_id,
            "vod".into(),
            total_segments,
            PlaylistMeta {
                method: config.manual_key.as_ref().map(|_| "manual".into()),
                key_uri: None,
                quality: config.preferred_quality.clone(),
                resolution: None,
                audio: None,
            },
        )
        .await?;

    if let Some(key) = config.manual_key.as_ref() {
        manager
            .update_encryption_status(
                config.task_id,
                EncryptionSnapshot {
                    method: Some("manual".into()),
                    key_uri: None,
                    status: EncryptionStatus::Ready,
                    iv: config
                        .manual_iv
                        .as_ref()
                        .map(hex_encode)
                        .or_else(|| Some(hex_encode(&generate_iv(0)))),
                    last_error: if key.is_empty() {
                        Some("提供的 KEY 为空，模拟跳过解密".into())
                    } else {
                        None
                    },
                },
            )
            .await?;
    }

    fs::create_dir_all(&config.save_dir).await?;
    let output_path = config.save_dir.join(format!("{}.ts", config.title));
    let mut writer = File::create(&output_path).await?;

    let delay = Duration::from_millis(200.max(800 / config.concurrency as u64));
    for seq in 0..total_segments {
        if cancel.is_cancelled() {
            manager
                .push_log(config.task_id, "任务已取消，可从暂停处继续")
                .await?;
            return Ok(());
        }

        manager
            .update_segment(
                config.task_id,
                seq as u64,
                SegmentStatus::Downloading,
                None,
                None,
            )
            .await?;

        sleep(delay).await;

        let simulated_bytes = 1_600_000u64 + (seq as u64 % 4) * 180_000;
        writer.write_all(b"\0").await?;

        let speed_bps = simulated_bytes as f64 / delay.as_secs_f64().max(0.001);
        manager
            .mark_segment_complete(config.task_id, seq as u64, simulated_bytes, speed_bps)
            .await?;
    }

    writer.flush().await?;
    manager
        .push_log(
            config.task_id,
            format!("模拟下载完成，输出文件 {}", output_path.display()),
        )
        .await?;
    manager
        .mark_task_completed(config.task_id, output_path)
        .await?;
    Ok(())
}

fn generate_iv(seq: u64) -> Vec<u8> {
    let mut iv = vec![0u8; 16];
    iv[8..].copy_from_slice(&seq.to_be_bytes());
    iv
}
