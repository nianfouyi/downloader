use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use serde::Deserialize;
use std::net::SocketAddr;
use tracing::info;

use crate::manager::{DownloadManager, NewTaskRequest};

#[derive(Deserialize)]
pub struct DownloadPayload {
    /// m3u8 播放列表地址
    pub url: String,
    /// 视频页面地址（Referer）
    pub page_url: String,
    /// 本地保存目录
    pub save_dir: String,
    /// 任务标题
    pub title: Option<String>,
    /// 下载线程数，默认 4
    pub threads: Option<u8>,
}

pub fn spawn_http_server(manager: DownloadManager) {
    tauri::async_runtime::spawn(async move {
        let app = Router::new()
            .route("/download", post(handle_download))
            .with_state(manager)
            .layer(
                tower_http::cors::CorsLayer::new()
                    .allow_origin(tower_http::cors::Any)
                    .allow_methods(tower_http::cors::Any)
                    .allow_headers(tower_http::cors::Any),
            );

        let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
        info!("HTTP API server listening on http://{}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await.expect("无法绑定 127.0.0.1:8080");
        axum::serve(listener, app).await.expect("HTTP server 异常退出");
    });
}

async fn handle_download(
    State(manager): State<DownloadManager>,
    Json(payload): Json<DownloadPayload>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let request = NewTaskRequest {
        playlist_url: payload.url,
        save_dir: payload.save_dir,
        page_url: payload.page_url,
        title: payload.title,
        max_concurrency: payload.threads.unwrap_or(4),
        preferred_quality: None,
        custom_headers: None,
        cookies: None,
        manual_key: None,
        manual_iv: None,
    };

    match manager.create_task(request).await {
        Ok(id) => Ok(Json(serde_json::json!({
            "success": true,
            "task_id": id.to_string(),
        }))),
        Err(err) => Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "success": false,
                "error": err.to_string(),
            })),
        )),
    }
}
