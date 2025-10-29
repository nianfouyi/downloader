#![allow(clippy::too_many_arguments)]

mod commands;
mod events;
mod hls;
mod manager;
mod websocket;

use crate::manager::DownloadManager;
use crate::websocket::WebSocketState;
use tauri::Manager;
use tracing_subscriber::{fmt, EnvFilter};

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    setup_tracing();

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .manage(WebSocketState::default())
        .setup(|app| {
            let app_handle = app.handle();
            let ws_state = app.state::<WebSocketState>().inner().clone();
            let manager = DownloadManager::new(app_handle.clone(), ws_state.clone());
            app.manage(manager.clone());
            ws_state.spawn_listener(app_handle.clone(), manager);

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::create_task,
            commands::pause_task,
            commands::resume_task,
            commands::retry_segment,
            commands::remove_task,
            commands::list_tasks,
            commands::websocket_endpoint
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

fn setup_tracing() {
    let _ = fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive("downloader=info".parse().unwrap()),
        )
        .with_target(false)
        .try_init();
}
