use crate::events::TaskId;
use crate::logging::{self, LogFileInfo, LogManager, LoggingConfig};
use crate::manager::{DownloadManager, NewTaskRequest};
use crate::websocket::WebSocketState;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tauri::AppHandle;
use tauri::State;
use tauri_plugin_dialog::DialogExt;

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
pub async fn restart_task(
    manager: State<'_, DownloadManager>,
    task_id: String,
) -> Result<(), String> {
    manager
        .restart_task(parse_id(task_id)?)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn pause_tasks(
    manager: State<'_, DownloadManager>,
    task_ids: Vec<String>,
) -> Result<(), String> {
    if task_ids.is_empty() {
        return Ok(());
    }
    let mut errors = Vec::new();
    for raw_id in task_ids {
        match parse_id(raw_id) {
            Ok(id) => {
                if let Err(err) = manager.pause_task(id).await {
                    errors.push(err.to_string());
                }
            }
            Err(err) => errors.push(err),
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

#[tauri::command]
pub async fn restart_tasks(
    manager: State<'_, DownloadManager>,
    task_ids: Vec<String>,
) -> Result<(), String> {
    if task_ids.is_empty() {
        return Ok(());
    }

    let mut errors = Vec::new();
    for raw_id in task_ids {
        match parse_id(raw_id) {
            Ok(id) => {
                if let Err(err) = manager.restart_task(id).await {
                    errors.push(err.to_string());
                }
            }
            Err(err) => errors.push(err),
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
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
pub async fn remove_tasks(
    manager: State<'_, DownloadManager>,
    task_ids: Vec<String>,
) -> Result<(), String> {
    tracing::info!("[REMOVE_TASKS] 收到删除任务请求, task_ids: {:?}", task_ids);
    if task_ids.is_empty() {
        tracing::warn!("[REMOVE_TASKS] task_ids为空,直接返回");
        return Ok(());
    }

    let mut errors = Vec::new();
    for raw_id in task_ids {
        tracing::info!("[REMOVE_TASKS] 处理任务ID: {}", raw_id);
        match parse_id(raw_id.clone()) {
            Ok(id) => {
                tracing::info!("[REMOVE_TASKS] 调用manager.remove_task, id: {}", id);
                if let Err(err) = manager.remove_task(id).await {
                    tracing::error!("[REMOVE_TASKS] 删除任务失败, id: {}, error: {:?}", id, err);
                    errors.push(err.to_string());
                } else {
                    tracing::info!("[REMOVE_TASKS] 删除任务成功, id: {}", id);
                }
            }
            Err(err) => {
                tracing::error!("[REMOVE_TASKS] 解析任务ID失败, raw_id: {}, error: {}", raw_id, err);
                errors.push(err);
            }
        }
    }

    if errors.is_empty() {
        tracing::info!("[REMOVE_TASKS] 所有任务删除成功");
        Ok(())
    } else {
        tracing::error!("[REMOVE_TASKS] 部分任务删除失败: {}", errors.join("; "));
        Err(errors.join("; "))
    }
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

#[derive(Serialize)]
pub struct LoggingConfigResponse {
    pub max_file_size_bytes: u64,
    pub max_total_size_bytes: u64,
}

#[derive(Deserialize)]
pub struct LoggingConfigRequest {
    pub max_file_size_bytes: u64,
    pub max_total_size_bytes: u64,
}

#[tauri::command]
pub fn get_logging_config(
    log_manager: State<'_, LogManager>,
) -> Result<LoggingConfigResponse, String> {
    let config = log_manager.config();
    Ok(LoggingConfigResponse {
        max_file_size_bytes: config.max_file_size_bytes,
        max_total_size_bytes: config.max_total_size_bytes,
    })
}

#[tauri::command]
pub fn update_logging_config(
    log_manager: State<'_, LogManager>,
    config: LoggingConfigRequest,
) -> Result<(), String> {
    let new_config = LoggingConfig {
        max_file_size_bytes: config.max_file_size_bytes,
        max_total_size_bytes: config.max_total_size_bytes,
    };
    log_manager
        .update_config(new_config)
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub fn list_log_files(log_manager: State<'_, LogManager>) -> Result<Vec<LogFileInfo>, String> {
    logging::list_log_files_for_ui(&log_manager).map_err(|e| e.to_string())
}

#[tauri::command]
pub fn read_log_file(
    log_manager: State<'_, LogManager>,
    file_name: String,
    limit: Option<u64>,
) -> Result<String, String> {
    log_manager
        .read_log(&file_name, limit)
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub fn log_directory_path(log_manager: State<'_, LogManager>) -> Result<String, String> {
    Ok(log_manager.log_directory().display().to_string())
}

#[derive(Deserialize)]
pub struct DirectoryDialogPayload {
    pub default_path: Option<String>,
}

#[tauri::command]
pub async fn select_save_directory(
    app: AppHandle,
    payload: DirectoryDialogPayload,
) -> Result<Option<String>, String> {
    let mut builder = app.dialog().file().set_title("选择保存目录");
    if let Some(default) = payload.default_path.as_ref() {
        let candidate = PathBuf::from(default);
        if candidate.is_dir() {
            builder = builder.set_directory(candidate);
        } else if let Some(parent) = candidate.parent() {
            builder = builder.set_directory(parent.to_path_buf());
        }
    }

    let result = builder.blocking_pick_folder();
    match result {
        Some(path) => match path.into_path() {
            Ok(p) => Ok(Some(p.display().to_string())),
            Err(err) => Err(err.to_string()),
        },
        None => Ok(None),
    }
}
