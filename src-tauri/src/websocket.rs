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
use tracing::{debug, error, info};

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
        let app_for_server = app.clone();
        tauri::async_runtime::spawn(async move {
            if let Err(err) = state.run_server(app_for_server, manager.clone()).await {
                *state.inner.started.lock() = false;
                error!("WebSocket server error: {err:#}");
            }
        });
    }

    async fn run_server(&self, app: AppHandle, manager: DownloadManager) -> Result<()> {
        let addr = SocketAddr::from(([127, 0, 0, 1], self.inner.port));
        let listener = TcpListener::bind(addr).await?;
        info!("WebSocket server listening on ws://{addr}");
        app.emit("ws-ready", json!({ "port": self.inner.port }))?;
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
        let subscriber_count = self.inner.broadcaster.receiver_count();
        if subscriber_count == 0 {
            debug!("WebSocket 暂无订阅者，跳过实时事件广播");
            return Ok(());
        }

        if self.inner.broadcaster.send(event).is_err() {
            // A client can disconnect between receiver_count() and send(). This
            // is normal and must not be reported as a task/download failure.
            debug!("WebSocket 客户端在事件广播前断开");
        }
        Ok(())
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
    tracing::info!("[WS_CONNECTION] 新WebSocket连接建立");
    let ws_stream = tokio_tungstenite::accept_async(stream).await?;
    let (mut sender, mut receiver) = ws_stream.split();
    let mut rx = state.subscribe();

    let snapshot: Vec<TaskSnapshot> = manager.snapshot().await.unwrap_or_default();
    tracing::info!(
        "[WS_CONNECTION] 发送初始snapshot, 任务数: {}",
        snapshot.len()
    );
    sender
        .send(Message::Text(serde_json::to_string(
            &FrontendEvent::Snapshot { tasks: snapshot },
        )?))
        .await?;

    tracing::info!("[WS_CONNECTION] 开始监听事件");

    loop {
        tokio::select! {
            maybe_msg = rx.recv() => {
                match maybe_msg {
                    Ok(event) => {
                        tracing::debug!("[WS_CONNECTION] 收到广播事件: {:?}", event);
                        let payload = serde_json::to_string(&event)?;
                        tracing::debug!("[WS_CONNECTION] 准备发送事件到客户端");
                        if sender.send(Message::Text(payload)).await.is_err() {
                            tracing::warn!("[WS_CONNECTION] 发送事件到客户端失败,连接断开");
                            break;
                        }
                        tracing::debug!("[WS_CONNECTION] 事件已发送到客户端");
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!("[WS_CONNECTION] 消息丢失 {} 条", skipped);
                        sender.send(Message::Text(serde_json::to_string(&FrontendEvent::TaskError {
                            task_id: "broadcast".into(),
                            message: format!("消息丢失 {skipped} 条，请重新连接"),
                        })?)).await.ok();
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::info!("[WS_CONNECTION] 广播通道关闭");
                        break;
                    }
                }
            }
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => {
                        tracing::info!("[WS_CONNECTION] 客户端关闭连接");
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    tracing::info!("[WS_CONNECTION] WebSocket连接结束");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::WebSocketState;
    use crate::events::FrontendEvent;

    #[test]
    fn sending_without_subscribers_is_not_an_error() {
        let state = WebSocketState::default();

        assert!(state
            .send(FrontendEvent::TaskRemoved {
                task_id: "task-1".into(),
            })
            .is_ok());
    }

    #[test]
    fn subscribed_clients_receive_events() {
        let state = WebSocketState::default();
        let mut receiver = state.subscribe();

        state
            .send(FrontendEvent::TaskRemoved {
                task_id: "task-2".into(),
            })
            .unwrap();

        assert!(matches!(
            receiver.try_recv(),
            Ok(FrontendEvent::TaskRemoved { task_id }) if task_id == "task-2"
        ));
    }
}
