#![allow(clippy::too_many_arguments)]

mod commands;
mod events;
mod hls;
mod http_api;
mod logging;
mod manager;
mod websocket;

use crate::logging::LogManager;
use crate::manager::DownloadManager;
use crate::websocket::WebSocketState;
use tauri::Manager;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_dialog::init())
        .manage(WebSocketState::default())
        .setup(|app| -> Result<(), Box<dyn std::error::Error>> {
            let app_handle = app.handle();
            let log_manager = LogManager::setup(&app_handle)?;
            app.manage(log_manager.clone());

            let ws_state = app.state::<WebSocketState>().inner().clone();
            let manager = DownloadManager::new(app_handle.clone(), ws_state.clone());
            app.manage(manager.clone());
            ws_state.spawn_listener(app_handle.clone(), manager.clone());
            http_api::spawn_http_server(manager);

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::create_task,
            commands::pause_task,
            commands::pause_tasks,
            commands::restart_task,
            commands::restart_tasks,
            commands::resume_task,
            commands::retry_segment,
            commands::remove_task,
            commands::remove_tasks,
            commands::list_tasks,
            commands::websocket_endpoint,
            commands::get_logging_config,
            commands::update_logging_config,
            commands::list_log_files,
            commands::read_log_file,
            commands::log_directory_path,
            commands::select_save_directory
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
