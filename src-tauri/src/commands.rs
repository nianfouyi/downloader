use crate::events::TaskId;
use crate::manager::{DownloadManager, NewTaskRequest};
use crate::websocket::WebSocketState;
use serde::Deserialize;
use tauri::State;

#[derive(Deserialize)]
pub struct CreateTaskPayload {
    pub playlist_url: String,
    pub quality: Option<String>,
    pub save_dir: String,
    pub referer: Option<String>,
    pub headers: Option<String>,
    pub cookies: Option<String>,
    pub manual_key: Option<String>,
    pub manual_iv: Option<String>,
    pub title: Option<String>,
    pub max_threads: Option<u8>,
}

#[tauri::command]
pub async fn create_task(
    manager: State<'_, DownloadManager>,
    payload: CreateTaskPayload,
) -> Result<String, String> {
    let request = NewTaskRequest {
        playlist_url: payload.playlist_url,
        preferred_quality: payload.quality,
        save_dir: payload.save_dir,
        referer: payload.referer,
        custom_headers: payload.headers,
        cookies: payload.cookies,
        manual_key: payload.manual_key,
        manual_iv: payload.manual_iv,
        title: payload.title,
        max_concurrency: payload.max_threads.unwrap_or(6),
    };
    manager
        .create_task(request)
        .await
        .map(|id| id.to_string())
        .map_err(|err| err.to_string())
}

#[tauri::command]
pub async fn pause_task(
    manager: State<'_, DownloadManager>,
    task_id: String,
) -> Result<(), String> {
    manager
        .pause_task(parse_id(task_id)?)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn resume_task(
    manager: State<'_, DownloadManager>,
    task_id: String,
) -> Result<(), String> {
    manager
        .resume_task(parse_id(task_id)?)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn retry_segment(
    manager: State<'_, DownloadManager>,
    task_id: String,
    segment_seq: u64,
) -> Result<(), String> {
    manager
        .retry_segment(parse_id(task_id)?, segment_seq)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn remove_task(
    manager: State<'_, DownloadManager>,
    task_id: String,
) -> Result<(), String> {
    manager
        .remove_task(parse_id(task_id)?)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn list_tasks(
    manager: State<'_, DownloadManager>,
) -> Result<Vec<crate::events::TaskSnapshot>, String> {
    manager.snapshot().await.map_err(|e| e.to_string())
}

#[tauri::command]
pub fn websocket_endpoint(state: State<'_, WebSocketState>) -> Result<String, String> {
    Ok(format!("ws://127.0.0.1:{}", state.port()))
}

fn parse_id(id: String) -> Result<TaskId, String> {
    id.parse::<uuid::Uuid>()
        .map_err(|_| "invalid task id".to_string())
}
