use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};
use uuid::Uuid;

type WsSink = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<TcpStream>,
    Message,
>;

struct ClientState {
    sink: WsSink,
    subscriptions: Vec<Value>,
}

/// LocalWsPublisher — protocol-compatible with the Python version.
///
/// On client connect: sends `{"client_id": "<hex>"}`.
/// Client subscribes via: `{"action":"subscribe","channels":[...]}`.
/// Messages are matched against subscriptions by `_data_type`, `venue`, `sym`.
pub struct LocalWsPublisher {
    port: u16,
    clients: Arc<RwLock<HashMap<String, Arc<Mutex<ClientState>>>>>,
}

impl LocalWsPublisher {
    pub fn new(port: u16) -> Self {
        Self {
            port,
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Start the WebSocket server. This spawns a background task and returns immediately.
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = format!("127.0.0.1:{}", self.port);
        let listener = TcpListener::bind(&addr).await?;
        info!("LocalWsPublisher running on port {}", self.port);

        let clients = Arc::clone(&self.clients);
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        let clients = Arc::clone(&clients);
                        tokio::spawn(handle_connection(stream, addr, clients));
                    }
                    Err(e) => {
                        error!("Failed to accept connection: {}", e);
                    }
                }
            }
        });

        Ok(())
    }

    /// Broadcast a message to all clients whose subscriptions match.
    pub async fn send(&self, message: &Value) {
        let clients = self.clients.read().await;
        let mut dead_clients = Vec::new();

        for (client_id, client_state) in clients.iter() {
            let mut state = client_state.lock().await;
            let matches = state
                .subscriptions
                .iter()
                .any(|sub| matches_filter(message, sub));

            if matches {
                let msg_str = serde_json::to_string(message).unwrap_or_default();
                if let Err(e) = state.sink.send(Message::Text(msg_str.into())).await {
                    warn!("Failed to send to client {}: {}", client_id, e);
                    dead_clients.push(client_id.clone());
                }
            }
        }

        // Remove dead clients
        if !dead_clients.is_empty() {
            drop(clients);
            let mut clients = self.clients.write().await;
            for id in dead_clients {
                info!("Local websocket ClientID = {} disconnected", id);
                clients.remove(&id);
            }
        }
    }
}

/// Check if a message matches a subscription filter.
/// Keys checked: `_data_type`, `venue`, `sym`. Wildcard `"*"` matches anything.
fn matches_filter(message: &Value, sub: &Value) -> bool {
    for key in &["_data_type", "venue", "sym"] {
        let sub_val = sub
            .get(key)
            .and_then(|v| v.as_str())
            .unwrap_or("*");
        if sub_val == "*" {
            continue;
        }
        let msg_val = message.get(key).and_then(|v| v.as_str()).unwrap_or("");
        if sub_val != msg_val {
            return false;
        }
    }
    true
}

async fn handle_connection(
    stream: TcpStream,
    addr: SocketAddr,
    clients: Arc<RwLock<HashMap<String, Arc<Mutex<ClientState>>>>>,
) {
    let ws_stream = match tokio_tungstenite::accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            error!("WebSocket handshake failed for {}: {}", addr, e);
            return;
        }
    };

    let client_id = Uuid::new_v4().simple().to_string();
    let (mut sink, mut stream_rx) = ws_stream.split();

    // Send client_id to the newly connected client
    let welcome = json!({"client_id": client_id});
    if let Err(e) = sink
        .send(Message::Text(serde_json::to_string(&welcome).unwrap().into()))
        .await
    {
        error!("Failed to send client_id to {}: {}", addr, e);
        return;
    }

    info!("Local websocket ClientID = {} connected.", client_id);

    let state = Arc::new(Mutex::new(ClientState {
        sink,
        subscriptions: Vec::new(),
    }));

    {
        let mut map = clients.write().await;
        map.insert(client_id.clone(), Arc::clone(&state));
    }

    // Read loop: process subscribe/unsubscribe messages
    while let Some(msg_result) = stream_rx.next().await {
        match msg_result {
            Ok(Message::Text(text)) => {
                if let Ok(data) = serde_json::from_str::<Value>(&text) {
                    process_client_message(&client_id, &data, &state).await;
                }
            }
            Ok(Message::Close(_)) => break,
            Err(_) => break,
            _ => {}
        }
    }

    // Client disconnected
    {
        let mut map = clients.write().await;
        map.remove(&client_id);
    }
    info!("Local websocket ClientID = {} disconnected", client_id);
}

async fn process_client_message(
    client_id: &str,
    data: &Value,
    state: &Arc<Mutex<ClientState>>,
) {
    let action = data.get("action").and_then(|v| v.as_str()).unwrap_or("");

    match action {
        "subscribe" => {
            if let Some(channels) = data.get("channels").and_then(|v| v.as_array()) {
                let mut s = state.lock().await;
                s.subscriptions.extend(channels.iter().cloned());
                info!(
                    "Client {} subscribed to {} channel(s)",
                    client_id,
                    channels.len()
                );
            }
        }
        "unsubscribe" => {
            if let Some(channels) = data.get("channels").and_then(|v| v.as_array()) {
                let mut s = state.lock().await;
                for unsub in channels {
                    s.subscriptions.retain(|sub| !matches_filter(sub, unsub));
                }
            }
        }
        _ => {
            warn!("Unknown action from client {}: {}", client_id, action);
        }
    }
}
