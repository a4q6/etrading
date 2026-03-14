use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use futures_util::{SinkExt, StreamExt};
use ordered_float::OrderedFloat;
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

use etr_common::datamodel::{self, venue, MarketBook, MarketTrade, Rate};
use etr_common::tp_logger::TpLogger;
use etr_publisher::LocalWsPublisher;

const WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";
const HANDLER_CLASS: &str = "OkxSwapSocketClient";
const BOOK_THROTTLE_MS: i64 = 100;
const HEARTBEAT_INTERVAL_SECS: u64 = 25;

/// OKX USDT Perpetual Swap WebSocket feed handler.
///
/// Channels:
/// - `books5` (top-5 snapshot, always full) → MarketBook + Rate
/// - `trades` → MarketTrade
///
/// Ping/Pong: sends plain-text "ping" every 25s; server replies with plain-text "pong".
/// instId format: "BTC-USDT-SWAP" — internal key: "BTCUSDTSWAP" (hyphens removed).
pub struct OkxSwapSocketClient {
    /// OKX instId strings, e.g. ["BTC-USDT-SWAP"]
    inst_ids: Vec<String>,
    reconnect_attempts: Option<u32>,
    publisher: Option<Arc<LocalWsPublisher>>,
    tp_loggers: HashMap<String, TpLogger>,
    market_books: HashMap<String, MarketBook>,
    rates: HashMap<String, Rate>,
    last_emit_book: HashMap<String, DateTime<Utc>>,
    /// instId → internal key mapping, e.g. "BTC-USDT-SWAP" → "BTCUSDTSWAP"
    inst_to_key: HashMap<String, String>,
    running: bool,
}

impl OkxSwapSocketClient {
    pub fn new(
        inst_ids: Vec<String>,
        reconnect_attempts: Option<u32>,
        publisher: Option<Arc<LocalWsPublisher>>,
        rtp_dir: &Path,
    ) -> Self {
        let mut tp_loggers = HashMap::new();
        let mut market_books = HashMap::new();
        let mut rates = HashMap::new();
        let mut last_emit_book = HashMap::new();
        let mut inst_to_key = HashMap::new();

        for inst_id in &inst_ids {
            let key = inst_id.replace('-', "");
            inst_to_key.insert(inst_id.clone(), key.clone());

            let logger_name = format!("TP-{}-{}", HANDLER_CLASS, key);
            tp_loggers.insert(key.clone(), TpLogger::new(&logger_name, rtp_dir));

            market_books.insert(key.clone(), MarketBook::new(&key, venue::OKX, "swap"));
            rates.insert(key.clone(), Rate::new(&key, venue::OKX, "swap"));
            last_emit_book.insert(
                key.clone(),
                DateTime::from_timestamp(0, 0).unwrap_or_else(Utc::now),
            );
        }

        Self {
            inst_ids,
            reconnect_attempts,
            publisher,
            tp_loggers,
            market_books,
            rates,
            last_emit_book,
            inst_to_key,
            running: true,
        }
    }

    pub async fn start(&mut self) {
        let mut attempts: u32 = 0;

        while self.running {
            if let Some(max) = self.reconnect_attempts {
                if attempts >= max {
                    error!("Reached max connection attempts, stop listening.");
                    break;
                }
            }

            match self.connect().await {
                Ok(()) => {
                    attempts = 0;
                }
                Err(e) => {
                    attempts += 1;
                    error!("Connection Error (#Attempts={}): {}", attempts, e);
                    let sleep_sec = 10.0 * (attempts as f64).ln().max(1.0);
                    info!("Wait {:.2} seconds to reconnect...", sleep_sec);
                    tokio::time::sleep(Duration::from_secs_f64(sleep_sec)).await;
                }
            }
        }
    }

    async fn connect(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Connecting to {}", WS_URL);
        let (ws_stream, _) = tokio_tungstenite::connect_async(WS_URL).await?;
        info!("Start subscribing: {}", WS_URL);

        let (write, mut read) = ws_stream.split();
        let write = Arc::new(Mutex::new(write));

        // Subscribe to books5 and trades for all instIds
        {
            let mut args = Vec::new();
            for inst_id in &self.inst_ids {
                args.push(json!({"channel": "books5", "instId": inst_id}));
                args.push(json!({"channel": "trades", "instId": inst_id}));
            }
            let sub_msg = json!({"op": "subscribe", "args": args});
            let mut w = write.lock().await;
            w.send(Message::Text(sub_msg.to_string().into())).await?;
            info!("Subscribed: {:?}", self.inst_ids);
        }

        // Heartbeat task — OKX requires plain-text "ping" every 25s
        let write_hb = Arc::clone(&write);
        let hb_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(HEARTBEAT_INTERVAL_SECS)).await;
                let mut w = write_hb.lock().await;
                if let Err(e) = w.send(Message::Text("ping".into())).await {
                    warn!("Failed to send heartbeat ping: {}", e);
                    break;
                }
            }
        });

        let result = loop {
            if !self.running {
                break Ok(());
            }

            match tokio::time::timeout(Duration::from_secs(120), read.next()).await {
                Ok(Some(Ok(Message::Text(text)))) => {
                    // OKX pong is plain-text
                    if text.trim() == "pong" {
                        continue;
                    }
                    match serde_json::from_str::<Value>(&text) {
                        Ok(msg) => self.process_message(&msg).await,
                        Err(e) => warn!("Failed to parse JSON: {}", e),
                    }
                }
                Ok(Some(Ok(Message::Close(_)))) => {
                    info!("Websocket closed OK");
                    break Ok(());
                }
                Ok(Some(Ok(Message::Ping(data)))) => {
                    let mut w = write.lock().await;
                    let _ = w.send(Message::Pong(data)).await;
                }
                Ok(Some(Err(e))) => {
                    error!("Websocket closed ERR: {}", e);
                    break Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
                }
                Ok(None) => {
                    info!("WebSocket stream ended");
                    break Ok(());
                }
                Err(_) => {
                    warn!("WebSocket read timeout (120s), reconnecting...");
                    break Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "WebSocket read timeout",
                    )) as Box<dyn std::error::Error + Send + Sync>);
                }
                _ => {}
            }
        };

        hb_handle.abort();
        info!("Close websocket");
        result
    }

    async fn process_message(&mut self, msg: &Value) {
        // Handle subscription confirmation / error events
        if let Some(event) = msg.get("event").and_then(|v| v.as_str()) {
            match event {
                "subscribe" => {
                    info!("Subscription confirmed: {:?}", msg.get("arg"));
                }
                "error" => {
                    error!("OKX error: {}", msg);
                }
                _ => {}
            }
            return;
        }

        let channel = msg
            .get("arg")
            .and_then(|v| v.get("channel"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let inst_id = msg
            .get("arg")
            .and_then(|v| v.get("instId"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let key = match self.inst_to_key.get(inst_id) {
            Some(k) => k.clone(),
            None => return,
        };

        let data_arr = match msg.get("data").and_then(|v| v.as_array()) {
            Some(arr) => arr.clone(),
            None => return,
        };

        match channel {
            "books5" => {
                for item in &data_arr {
                    self.handle_books5(&key, item).await;
                }
            }
            "trades" => {
                for item in &data_arr {
                    self.handle_trade(&key, inst_id, item).await;
                }
            }
            _ => {}
        }
    }

    /// Handle `books5` — always a full top-5 snapshot.
    async fn handle_books5(&mut self, key: &str, item: &Value) {
        let now = Utc::now();
        let mkt_ts = item
            .get("ts")
            .and_then(|v| v.as_str())
            .and_then(|v| v.parse::<i64>().ok())
            .map(datamodel::from_millis)
            .unwrap_or_else(Utc::now);

        let book = match self.market_books.get_mut(key) {
            Some(b) => b,
            None => return,
        };
        book.timestamp = now;
        book.market_created_timestamp = mkt_ts;
        book.misc = "whole".to_string();

        // Replace entire book (books5 is always a full snapshot)
        book.bids.clear();
        if let Some(bids) = item.get("bids").and_then(|v| v.as_array()) {
            for b in bids {
                if let Some(arr) = b.as_array() {
                    if arr.len() >= 2 {
                        let price = arr[0].as_str().and_then(|v| v.parse::<f64>().ok());
                        let size = arr[1].as_str().and_then(|v| v.parse::<f64>().ok());
                        if let (Some(p), Some(s)) = (price, size) {
                            if s > 0.0 {
                                book.bids.insert(OrderedFloat(p), s);
                            }
                        }
                    }
                }
            }
        }
        book.asks.clear();
        if let Some(asks) = item.get("asks").and_then(|v| v.as_array()) {
            for a in asks {
                if let Some(arr) = a.as_array() {
                    if arr.len() >= 2 {
                        let price = arr[0].as_str().and_then(|v| v.parse::<f64>().ok());
                        let size = arr[1].as_str().and_then(|v| v.parse::<f64>().ok());
                        if let (Some(p), Some(s)) = (price, size) {
                            if s > 0.0 {
                                book.asks.insert(OrderedFloat(p), s);
                            }
                        }
                    }
                }
            }
        }

        let dict = book.to_dict();
        let json_str = serde_json::to_string(&dict).unwrap_or_default();

        // Always publish to LocalWsPublisher
        if let Some(ref publisher) = self.publisher {
            let pub_clone = Arc::clone(publisher);
            let dict_clone = dict.clone();
            tokio::spawn(async move {
                pub_clone.send(&dict_clone).await;
            });
        }

        // Throttle TP log to 100ms
        let last = self.last_emit_book.get(key).copied().unwrap_or_else(|| {
            DateTime::from_timestamp(0, 0).unwrap_or_else(Utc::now)
        });
        if now - last >= TimeDelta::milliseconds(BOOK_THROTTLE_MS) {
            if let Some(logger) = self.tp_loggers.get(key) {
                logger.info(json_str);
            }
            self.last_emit_book.insert(key.to_string(), now);
        }

        // Emit Rate if mid_price changed
        let new_rate = book.to_rate();
        let cur_rate = self.rates.get(key);
        let should_emit = cur_rate
            .map(|r| r.misc.as_deref() == Some("null") || r.mid_price != new_rate.mid_price)
            .unwrap_or(true);

        if should_emit {
            let rate_dict = new_rate.to_dict();
            let rate_json = serde_json::to_string(&rate_dict).unwrap_or_default();

            if let Some(ref publisher) = self.publisher {
                let pub_clone = Arc::clone(publisher);
                let dict_clone = rate_dict.clone();
                tokio::spawn(async move {
                    pub_clone.send(&dict_clone).await;
                });
            }
            if let Some(logger) = self.tp_loggers.get(key) {
                logger.info(rate_json);
            }
            self.rates.insert(key.to_string(), new_rate);
        }
    }

    /// Handle `trades` channel.
    async fn handle_trade(&mut self, key: &str, _inst_id: &str, item: &Value) {
        let now = Utc::now();
        let mkt_ts = item
            .get("ts")
            .and_then(|v| v.as_str())
            .and_then(|v| v.parse::<i64>().ok())
            .map(datamodel::from_millis)
            .unwrap_or_else(Utc::now);

        let side_str = item.get("side").and_then(|v| v.as_str()).unwrap_or("");
        let side = if side_str == "buy" { 1 } else { -1 };

        let price = item
            .get("px")
            .and_then(|v| v.as_str())
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0);
        let amount = item
            .get("sz")
            .and_then(|v| v.as_str())
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0);
        let trade_id = item
            .get("tradeId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let mut trade = MarketTrade::new(key, venue::OKX, "swap", side, price, amount, &trade_id, mkt_ts);
        trade.timestamp = now;

        let dict = trade.to_dict();
        let json_str = serde_json::to_string(&dict).unwrap_or_default();

        if let Some(ref publisher) = self.publisher {
            let pub_clone = Arc::clone(publisher);
            let dict_clone = dict.clone();
            tokio::spawn(async move {
                pub_clone.send(&dict_clone).await;
            });
        }
        if let Some(logger) = self.tp_loggers.get(key) {
            logger.info(json_str);
        }
    }

    pub fn stop(&mut self) {
        self.running = false;
    }
}
