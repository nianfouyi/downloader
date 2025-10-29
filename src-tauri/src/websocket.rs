use crate::events::{FrontendEvent, TaskSnapshot};
use crate::manager::DownloadManager;
use anyhow::Result;
use futures::{SinkExt, StreamExt};
use parking_lot::Mutex;
use serde_json::json;
use std::net::SocketAddr;
use std::sync::Arc;
use tauri::{AppHandle, Emitter};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info};

const DEFAULT_PORT: u16 = 9573;

#[derive(Clone)]
pub struct WebSocketState {
    inner: Arc<WebSocketInner>,
}

struct WebSocketInner {
    port: u16,
    broadcaster: broadcast::Sender<FrontendEvent>,
    started: Mutex<bool>,
}

impl Default for WebSocketState {
    fn default() -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self {
            inner: Arc::new(WebSocketInner {
                port: DEFAULT_PORT,
                broadcaster: tx,
                started: Mutex::new(false),
            }),
        }
    }
}

impl WebSocketState {
    pub fn spawn_listener(&self, app: AppHandle, manager: DownloadManager) {
        let mut started = self.inner.started.lock();
        if *started {
            return;
        }
        *started = true;
        drop(started);

        let state = self.clone();
        tauri::async_runtime::spawn(async move {
            if let Err(err) = state.run_server(manager.clone()).await {
                error!("WebSocket server error: {err:#}");
            }
        });
        info!(
            "WebSocket server listening on ws://127.0.0.1:{}",
            self.inner.port
        );
        app.emit("ws-ready", json!({ "port": self.inner.port }))
            .ok();
    }

    async fn run_server(&self, manager: DownloadManager) -> Result<()> {
        let addr = SocketAddr::from(([127, 0, 0, 1], self.inner.port));
        let listener = TcpListener::bind(addr).await?;
        loop {
            let (stream, _) = listener.accept().await?;
            let state = self.clone();
            let manager = manager.clone();
            tauri::async_runtime::spawn(async move {
                if let Err(err) = handle_connection(stream, state, manager).await {
                    error!("WebSocket connection error: {err:#}");
                }
            });
        }
    }

    pub fn send(
        &self,
        event: FrontendEvent,
    ) -> Result<(), broadcast::error::SendError<FrontendEvent>> {
        self.inner.broadcaster.send(event).map(|_| ())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<FrontendEvent> {
        self.inner.broadcaster.subscribe()
    }

    pub fn port(&self) -> u16 {
        self.inner.port
    }
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    state: WebSocketState,
    manager: DownloadManager,
) -> Result<()> {
    let ws_stream = tokio_tungstenite::accept_async(stream).await?;
    let (mut sender, mut receiver) = ws_stream.split();

    let snapshot: Vec<TaskSnapshot> = manager.snapshot().await.unwrap_or_default();
    sender
        .send(Message::Text(serde_json::to_string(
            &FrontendEvent::Snapshot { tasks: snapshot },
        )?))
        .await?;

    let mut rx = state.subscribe();

    loop {
        tokio::select! {
            maybe_msg = rx.recv() => {
                match maybe_msg {
                    Ok(event) => {
                        let payload = serde_json::to_string(&event)?;
                        if sender.send(Message::Text(payload)).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        sender.send(Message::Text(serde_json::to_string(&FrontendEvent::TaskError {
                            task_id: "broadcast".into(),
                            message: format!("消息丢失 {skipped} 条，请重新连接"),
                        })?)).await.ok();
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
        }
    }

    Ok(())
}
