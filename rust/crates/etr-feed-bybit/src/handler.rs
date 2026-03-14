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

const WS_URL_LINEAR: &str = "wss://stream.bybit.com/v5/public/linear";
const HANDLER_CLASS: &str = "BybitLinearSocketClient";
const BOOK_DEPTH: u32 = 50;
const BOOK_THROTTLE_MS: i64 = 100;
const HEARTBEAT_INTERVAL_SECS: u64 = 20;

/// Bybit USDT Perpetual (linear) WebSocket feed handler.
///
/// Channels:
/// - `orderbook.50.{sym}`: snapshot + delta → MarketBook + Rate
/// - `publicTrade.{sym}`: → MarketTrade
///
/// Heartbeat: sends `{"op": "ping"}` every 20s; server replies with pong.
/// Symbol format: "BTCUSDT" (no separator) — same as internal key.
pub struct BybitLinearSocketClient {
    symbols: Vec<String>,
    reconnect_attempts: Option<u32>,
    publisher: Option<Arc<LocalWsPublisher>>,
    tp_loggers: HashMap<String, TpLogger>,
    market_books: HashMap<String, MarketBook>,
    rates: HashMap<String, Rate>,
    last_emit_book: HashMap<String, DateTime<Utc>>,
    running: bool,
}

impl BybitLinearSocketClient {
    pub fn new(
        symbols: Vec<String>,
        reconnect_attempts: Option<u32>,
        publisher: Option<Arc<LocalWsPublisher>>,
        rtp_dir: &Path,
    ) -> Self {
        let mut tp_loggers = HashMap::new();
        let mut market_books = HashMap::new();
        let mut rates = HashMap::new();
        let mut last_emit_book = HashMap::new();

        for sym in &symbols {
            let sym_upper = sym.to_uppercase().replace('-', "").replace('_', "");

            let logger_name = format!("TP-{}-{}", HANDLER_CLASS, sym_upper);
            tp_loggers.insert(sym_upper.clone(), TpLogger::new(&logger_name, rtp_dir));

            market_books.insert(sym_upper.clone(), MarketBook::new(&sym_upper, venue::BYBIT_LINEAR, "linear"));
            rates.insert(sym_upper.clone(), Rate::new(&sym_upper, venue::BYBIT_LINEAR, "linear"));
            last_emit_book.insert(
                sym_upper.clone(),
                DateTime::from_timestamp(0, 0).unwrap_or_else(Utc::now),
            );
        }

        // Normalize symbols to uppercase
        let symbols = symbols
            .into_iter()
            .map(|s| s.to_uppercase().replace('-', "").replace('_', ""))
            .collect();

        Self {
            symbols,
            reconnect_attempts,
            publisher,
            tp_loggers,
            market_books,
            rates,
            last_emit_book,
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
        info!("Connecting to {}", WS_URL_LINEAR);
        let (ws_stream, _) = tokio_tungstenite::connect_async(WS_URL_LINEAR).await?;
        info!("Start subscribing: {}", WS_URL_LINEAR);

        let (write, mut read) = ws_stream.split();
        let write = Arc::new(Mutex::new(write));

        // Subscribe orderbook + publicTrade for all symbols in one request
        {
            let mut args: Vec<String> = Vec::new();
            for sym in &self.symbols {
                args.push(format!("orderbook.{}.{}", BOOK_DEPTH, sym));
                args.push(format!("publicTrade.{}", sym));
            }
            let sub_msg = json!({"op": "subscribe", "args": args});
            let mut w = write.lock().await;
            w.send(Message::Text(sub_msg.to_string().into())).await?;
            info!("Subscribed: {:?}", args);
        }

        // Heartbeat task — Bybit disconnects after ~20s without ping
        let write_hb = Arc::clone(&write);
        let hb_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(HEARTBEAT_INTERVAL_SECS)).await;
                let mut w = write_hb.lock().await;
                if let Err(e) = w.send(Message::Text(json!({"op": "ping"}).to_string().into())).await {
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
        // op responses (pong, subscribe, ping)
        if let Some(op) = msg.get("op").and_then(|v| v.as_str()) {
            match op {
                "subscribe" => {
                    if msg.get("success").and_then(|v| v.as_bool()).unwrap_or(false) {
                        info!("[{}] Subscription confirmed (conn_id={:?})", HANDLER_CLASS, msg.get("conn_id"));
                    } else {
                        error!("[{}] Subscription REJECTED: {:?} | {}", HANDLER_CLASS, msg.get("ret_msg"), msg);
                    }
                }
                "pong" | "ping" => {}
                _ => {}
            }
            return;
        }

        // Server-side messages without a topic
        if msg.get("ret_msg").is_some() && msg.get("topic").is_none() {
            return;
        }

        let topic = match msg.get("topic").and_then(|v| v.as_str()) {
            Some(t) => t,
            None => {
                warn!("[{}] Message without topic: {}", HANDLER_CLASS, msg);
                return;
            }
        };

        if topic.contains("orderbook") {
            let sym = topic.split('.').last().unwrap_or("").to_string();
            let msg_type = msg.get("type").and_then(|v| v.as_str()).unwrap_or("");
            match msg_type {
                "snapshot" => self.handle_book_snapshot(&sym, msg).await,
                "delta" => self.handle_book_delta(&sym, msg).await,
                _ => warn!("[{}] Unknown orderbook msg_type={:?} for {}", HANDLER_CLASS, msg_type, sym),
            }
        } else if topic.contains("publicTrade") {
            let sym = topic.split('.').last().unwrap_or("").to_string();
            self.handle_trade(&sym, msg).await;
        } else {
            warn!("[{}] Unhandled topic: {}", HANDLER_CLASS, topic);
        }
    }

    /// Handle orderbook snapshot — replace entire book.
    async fn handle_book_snapshot(&mut self, sym: &str, msg: &Value) {
        if !self.market_books.contains_key(sym) {
            return;
        }
        let now = Utc::now();
        let mkt_ts = msg
            .get("ts")
            .and_then(|v| v.as_i64())
            .map(datamodel::from_millis)
            .unwrap_or(now);

        let data = match msg.get("data") {
            Some(d) => d,
            None => return,
        };

        let book = self.market_books.get_mut(sym).unwrap();
        book.timestamp = now;
        book.market_created_timestamp = mkt_ts;
        book.misc = "whole".to_string();

        // "b": [["price_str", "qty_str"], ...]
        book.bids.clear();
        if let Some(bids) = data.get("b").and_then(|v| v.as_array()) {
            for b in bids {
                if let Some(arr) = b.as_array() {
                    if arr.len() >= 2 {
                        let p = arr[0].as_str().and_then(|v| v.parse::<f64>().ok());
                        let s = arr[1].as_str().and_then(|v| v.parse::<f64>().ok());
                        if let (Some(price), Some(size)) = (p, s) {
                            if size > 0.0 {
                                book.bids.insert(OrderedFloat(price), size);
                            }
                        }
                    }
                }
            }
        }
        book.asks.clear();
        if let Some(asks) = data.get("a").and_then(|v| v.as_array()) {
            for a in asks {
                if let Some(arr) = a.as_array() {
                    if arr.len() >= 2 {
                        let p = arr[0].as_str().and_then(|v| v.parse::<f64>().ok());
                        let s = arr[1].as_str().and_then(|v| v.parse::<f64>().ok());
                        if let (Some(price), Some(size)) = (p, s) {
                            if size > 0.0 {
                                book.asks.insert(OrderedFloat(price), size);
                            }
                        }
                    }
                }
            }
        }

        let dict = book.to_dict();
        let json_str = serde_json::to_string(&dict).unwrap_or_default();

        if let Some(ref publisher) = self.publisher {
            let pub_clone = Arc::clone(publisher);
            let dict_clone = dict.clone();
            tokio::spawn(async move { pub_clone.send(&dict_clone).await; });
        }
        if let Some(logger) = self.tp_loggers.get(sym) {
            logger.info(json_str);
        }
        self.last_emit_book.insert(sym.to_string(), now);

        self.emit_rate_from_book(sym).await;
    }

    /// Handle orderbook delta — apply incremental diff.
    async fn handle_book_delta(&mut self, sym: &str, msg: &Value) {
        {
            let book = match self.market_books.get(sym) {
                Some(b) => b,
                None => return,
            };
            if book.misc == "null" {
                // Snapshot not yet received, discard diff
                return;
            }
        }

        let now = Utc::now();
        let mkt_ts = msg
            .get("ts")
            .and_then(|v| v.as_i64())
            .map(datamodel::from_millis)
            .unwrap_or(now);

        let data = match msg.get("data") {
            Some(d) => d,
            None => return,
        };

        let book = self.market_books.get_mut(sym).unwrap();

        if let Some(bids) = data.get("b").and_then(|v| v.as_array()) {
            for b in bids {
                if let Some(arr) = b.as_array() {
                    if arr.len() >= 2 {
                        let p = arr[0].as_str().and_then(|v| v.parse::<f64>().ok());
                        let s = arr[1].as_str().and_then(|v| v.parse::<f64>().ok());
                        if let (Some(price), Some(size)) = (p, s) {
                            if size == 0.0 {
                                book.bids.remove(&OrderedFloat(price));
                            } else {
                                book.bids.insert(OrderedFloat(price), size);
                            }
                        }
                    }
                }
            }
        }
        if let Some(asks) = data.get("a").and_then(|v| v.as_array()) {
            for a in asks {
                if let Some(arr) = a.as_array() {
                    if arr.len() >= 2 {
                        let p = arr[0].as_str().and_then(|v| v.parse::<f64>().ok());
                        let s = arr[1].as_str().and_then(|v| v.parse::<f64>().ok());
                        if let (Some(price), Some(size)) = (p, s) {
                            if size == 0.0 {
                                book.asks.remove(&OrderedFloat(price));
                            } else {
                                book.asks.insert(OrderedFloat(price), size);
                            }
                        }
                    }
                }
            }
        }

        book.timestamp = now;
        book.market_created_timestamp = mkt_ts;
        book.misc = "diff".to_string();

        let dict = book.to_dict();

        if let Some(ref publisher) = self.publisher {
            let pub_clone = Arc::clone(publisher);
            let dict_clone = dict.clone();
            tokio::spawn(async move { pub_clone.send(&dict_clone).await; });
        }

        // Throttle TP log to 100ms
        let last = self.last_emit_book.get(sym).copied().unwrap_or_else(|| {
            DateTime::from_timestamp(0, 0).unwrap_or_else(Utc::now)
        });
        if now - last >= TimeDelta::milliseconds(BOOK_THROTTLE_MS) {
            let json_str = serde_json::to_string(&dict).unwrap_or_default();
            if let Some(logger) = self.tp_loggers.get(sym) {
                logger.info(json_str);
            }
            self.last_emit_book.insert(sym.to_string(), now);
        }

        self.emit_rate_from_book(sym).await;
    }

    /// Handle `publicTrade` channel.
    async fn handle_trade(&mut self, sym: &str, msg: &Value) {
        if !self.tp_loggers.contains_key(sym) {
            return;
        }
        let now = Utc::now();
        let trades = match msg.get("data").and_then(|v| v.as_array()) {
            Some(t) => t.clone(),
            None => return,
        };

        for t in &trades {
            let mkt_ts = t
                .get("T")
                .and_then(|v| v.as_i64())
                .map(datamodel::from_millis)
                .unwrap_or(now);

            let side_str = t.get("S").and_then(|v| v.as_str()).unwrap_or("");
            let side = if side_str == "Buy" { 1 } else { -1 };

            let price = t
                .get("p")
                .and_then(|v| v.as_str())
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or(0.0);
            let amount = t
                .get("v")
                .and_then(|v| v.as_str())
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or(0.0);
            let trade_id = t
                .get("i")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let is_block = t.get("BT").and_then(|v| v.as_bool()).unwrap_or(false);

            let mut mt = MarketTrade::new(
                sym,
                venue::BYBIT_LINEAR,
                "linear",
                side,
                price,
                amount,
                &trade_id,
                mkt_ts,
            );
            mt.timestamp = now;
            if is_block {
                mt.misc = Some("block_trade".to_string());
            }

            let dict = mt.to_dict();
            let json_str = serde_json::to_string(&dict).unwrap_or_default();

            if let Some(ref publisher) = self.publisher {
                let pub_clone = Arc::clone(publisher);
                let dict_clone = dict.clone();
                tokio::spawn(async move { pub_clone.send(&dict_clone).await; });
            }
            if let Some(logger) = self.tp_loggers.get(sym) {
                logger.info(json_str);
            }
        }
    }

    async fn emit_rate_from_book(&mut self, sym: &str) {
        let new_rate = match self.market_books.get(sym) {
            Some(b) => b.to_rate(),
            None => return,
        };

        let should_emit = self
            .rates
            .get(sym)
            .map(|r| r.misc.as_deref() == Some("null") || r.mid_price != new_rate.mid_price)
            .unwrap_or(true);

        if should_emit {
            let rate_dict = new_rate.to_dict();
            let rate_json = serde_json::to_string(&rate_dict).unwrap_or_default();

            if let Some(ref publisher) = self.publisher {
                let pub_clone = Arc::clone(publisher);
                let dict_clone = rate_dict.clone();
                tokio::spawn(async move { pub_clone.send(&dict_clone).await; });
            }
            if let Some(logger) = self.tp_loggers.get(sym) {
                logger.info(rate_json);
            }
            self.rates.insert(sym.to_string(), new_rate);
        }
    }

    pub fn stop(&mut self) {
        self.running = false;
    }
}
