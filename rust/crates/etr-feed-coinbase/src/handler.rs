use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use futures_util::{SinkExt, StreamExt};
use ordered_float::OrderedFloat;
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::{
    protocol::WebSocketConfig,
    Message,
};
use tracing::{error, info, warn};

use etr_common::datamodel::{venue, MarketBook, MarketTrade, Rate};
use etr_common::tp_logger::TpLogger;
use etr_publisher::LocalWsPublisher;

const WS_URL: &str = "wss://advanced-trade-ws.coinbase.com";
const HANDLER_CLASS: &str = "CoinbaseSocketClient";
const BOOK_THROTTLE_MS: i64 = 100;

/// Coinbase Advanced Trade WebSocket feed handler.
///
/// Channels:
/// - `level2`: snapshot + incremental diff → MarketBook + Rate
/// - `market_trades`: → MarketTrade
/// - `heartbeats`: keep-alive (no data emitted)
///
/// product_id format: "BTC-USD" — internal key: "BTCUSD" (hyphens removed).
pub struct CoinbaseSocketClient {
    /// Coinbase product_id strings, e.g. ["BTC-USD"]
    product_ids: Vec<String>,
    reconnect_attempts: Option<u32>,
    publisher: Option<Arc<LocalWsPublisher>>,
    tp_loggers: HashMap<String, TpLogger>,
    market_books: HashMap<String, MarketBook>,
    rates: HashMap<String, Rate>,
    last_emit_book: HashMap<String, DateTime<Utc>>,
    /// product_id → internal key, e.g. "BTC-USD" → "BTCUSD"
    prod_to_key: HashMap<String, String>,
    running: bool,
}

impl CoinbaseSocketClient {
    pub fn new(
        product_ids: Vec<String>,
        reconnect_attempts: Option<u32>,
        publisher: Option<Arc<LocalWsPublisher>>,
        rtp_dir: &Path,
    ) -> Self {
        let mut tp_loggers = HashMap::new();
        let mut market_books = HashMap::new();
        let mut rates = HashMap::new();
        let mut last_emit_book = HashMap::new();
        let mut prod_to_key = HashMap::new();

        for prod_id in &product_ids {
            let key = prod_id.replace('-', "");
            prod_to_key.insert(prod_id.clone(), key.clone());

            let logger_name = format!("TP-{}-{}", HANDLER_CLASS, key);
            tp_loggers.insert(key.clone(), TpLogger::new(&logger_name, rtp_dir));

            market_books.insert(key.clone(), MarketBook::new(&key, venue::COINBASE, "advanced-trade"));
            rates.insert(key.clone(), Rate::new(&key, venue::COINBASE, "advanced-trade"));
            last_emit_book.insert(
                key.clone(),
                DateTime::from_timestamp(0, 0).unwrap_or_else(Utc::now),
            );
        }

        Self {
            product_ids,
            reconnect_attempts,
            publisher,
            tp_loggers,
            market_books,
            rates,
            last_emit_book,
            prod_to_key,
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

        // Coinbase level2 snapshots can be very large — disable frame size limit
        let config = WebSocketConfig {
            max_message_size: None,
            max_frame_size: None,
            ..Default::default()
        };
        let (ws_stream, _) =
            tokio_tungstenite::connect_async_with_config(WS_URL, Some(config), false).await?;
        info!("Start subscribing: {}", WS_URL);

        let (write, mut read) = ws_stream.split();
        let write = Arc::new(Mutex::new(write));

        // Subscribe to level2, market_trades, heartbeats (sequential with small delay)
        {
            let sub_level2 = json!({
                "type": "subscribe",
                "product_ids": self.product_ids,
                "channel": "level2",
            });
            let sub_trades = json!({
                "type": "subscribe",
                "product_ids": self.product_ids,
                "channel": "market_trades",
            });
            let sub_hb = json!({
                "type": "subscribe",
                "product_ids": self.product_ids,
                "channel": "heartbeats",
            });

            let mut w = write.lock().await;
            w.send(Message::Text(sub_level2.to_string().into())).await?;
            drop(w);
            tokio::time::sleep(Duration::from_millis(500)).await;

            let mut w = write.lock().await;
            w.send(Message::Text(sub_trades.to_string().into())).await?;
            drop(w);
            tokio::time::sleep(Duration::from_millis(500)).await;

            let mut w = write.lock().await;
            w.send(Message::Text(sub_hb.to_string().into())).await?;
            drop(w);

            info!("Subscribed level2+market_trades+heartbeats: {:?}", self.product_ids);
        }

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

        info!("Close websocket");
        result
    }

    async fn process_message(&mut self, msg: &Value) {
        let channel = match msg.get("channel").and_then(|v| v.as_str()) {
            Some(c) => c,
            None => return,
        };

        match channel {
            "heartbeats" => {}
            "subscriptions" => {
                info!("Subscription confirmed: {}", msg);
            }
            "l2_data" => {
                let events = match msg.get("events").and_then(|v| v.as_array()) {
                    Some(e) => e.clone(),
                    None => return,
                };
                for event in &events {
                    match event.get("type").and_then(|v| v.as_str()) {
                        Some("snapshot") => self.handle_l2_snapshot(event).await,
                        Some("update") => self.handle_l2_update(event).await,
                        _ => {}
                    }
                }
            }
            "market_trades" => {
                let events = match msg.get("events").and_then(|v| v.as_array()) {
                    Some(e) => e.clone(),
                    None => return,
                };
                for event in &events {
                    self.handle_trades(event).await;
                }
            }
            _ => {}
        }
    }

    /// Handle `l2_data` snapshot — replace the entire order book.
    async fn handle_l2_snapshot(&mut self, event: &Value) {
        let now = Utc::now();
        let product_id = match event.get("product_id").and_then(|v| v.as_str()) {
            Some(p) => p,
            None => return,
        };
        let key = match self.prod_to_key.get(product_id) {
            Some(k) => k.clone(),
            None => return,
        };

        let updates = match event.get("updates").and_then(|v| v.as_array()) {
            Some(u) => u.clone(),
            None => return,
        };

        let book = match self.market_books.get_mut(&key) {
            Some(b) => b,
            None => return,
        };
        book.bids.clear();
        book.asks.clear();

        let mut mkt_ts = now;
        for upd in &updates {
            let price = match upd.get("price_level").and_then(|v| v.as_str()).and_then(|v| v.parse::<f64>().ok()) {
                Some(p) => p,
                None => continue,
            };
            let qty = match upd.get("new_quantity").and_then(|v| v.as_str()).and_then(|v| v.parse::<f64>().ok()) {
                Some(q) => q,
                None => continue,
            };
            let side = upd.get("side").and_then(|v| v.as_str()).unwrap_or("");
            if let Some(ts_str) = upd.get("event_time").and_then(|v| v.as_str()) {
                mkt_ts = parse_iso8601(ts_str).unwrap_or(mkt_ts);
            }
            if qty > 0.0 {
                match side {
                    "bid" => { book.bids.insert(OrderedFloat(price), qty); }
                    _ => { book.asks.insert(OrderedFloat(price), qty); } // "offer"
                }
            }
        }

        book.timestamp = now;
        book.market_created_timestamp = mkt_ts;
        book.misc = "whole".to_string();

        let dict = book.to_dict();
        let json_str = serde_json::to_string(&dict).unwrap_or_default();

        if let Some(ref publisher) = self.publisher {
            let pub_clone = Arc::clone(publisher);
            let dict_clone = dict.clone();
            tokio::spawn(async move { pub_clone.send(&dict_clone).await; });
        }
        if let Some(logger) = self.tp_loggers.get(&key) {
            logger.info(json_str);
        }
        self.last_emit_book.insert(key.clone(), now);

        self.emit_rate_from_book(&key, now).await;
    }

    /// Handle `l2_data` update — apply incremental diff.
    async fn handle_l2_update(&mut self, event: &Value) {
        let now = Utc::now();
        let product_id = match event.get("product_id").and_then(|v| v.as_str()) {
            Some(p) => p,
            None => return,
        };
        let key = match self.prod_to_key.get(product_id) {
            Some(k) => k.clone(),
            None => return,
        };

        {
            let book = match self.market_books.get(&key) {
                Some(b) => b,
                None => return,
            };
            if book.misc == "null" {
                // Snapshot not yet received
                return;
            }
        }

        let updates = match event.get("updates").and_then(|v| v.as_array()) {
            Some(u) => u.clone(),
            None => return,
        };

        let mut mkt_ts = now;
        {
            let book = self.market_books.get_mut(&key).unwrap();
            for upd in &updates {
                let price = match upd.get("price_level").and_then(|v| v.as_str()).and_then(|v| v.parse::<f64>().ok()) {
                    Some(p) => p,
                    None => continue,
                };
                let qty = match upd.get("new_quantity").and_then(|v| v.as_str()).and_then(|v| v.parse::<f64>().ok()) {
                    Some(q) => q,
                    None => continue,
                };
                let side = upd.get("side").and_then(|v| v.as_str()).unwrap_or("");
                if let Some(ts_str) = upd.get("event_time").and_then(|v| v.as_str()) {
                    mkt_ts = parse_iso8601(ts_str).unwrap_or(mkt_ts);
                }
                match side {
                    "bid" => {
                        if qty == 0.0 {
                            book.bids.remove(&OrderedFloat(price));
                        } else {
                            book.bids.insert(OrderedFloat(price), qty);
                        }
                    }
                    _ => { // "offer"
                        if qty == 0.0 {
                            book.asks.remove(&OrderedFloat(price));
                        } else {
                            book.asks.insert(OrderedFloat(price), qty);
                        }
                    }
                }
            }
            book.timestamp = now;
            book.market_created_timestamp = mkt_ts;
            book.misc = "diff".to_string();
        }

        let dict = {
            let book = self.market_books.get(&key).unwrap();
            book.to_dict()
        };

        if let Some(ref publisher) = self.publisher {
            let pub_clone = Arc::clone(publisher);
            let dict_clone = dict.clone();
            tokio::spawn(async move { pub_clone.send(&dict_clone).await; });
        }

        // Throttle TP log to 100ms
        let last = self.last_emit_book.get(&key).copied().unwrap_or_else(|| {
            DateTime::from_timestamp(0, 0).unwrap_or_else(Utc::now)
        });
        if now - last >= TimeDelta::milliseconds(BOOK_THROTTLE_MS) {
            let json_str = serde_json::to_string(&dict).unwrap_or_default();
            if let Some(logger) = self.tp_loggers.get(&key) {
                logger.info(json_str);
            }
            self.last_emit_book.insert(key.clone(), now);
        }

        self.emit_rate_from_book(&key, now).await;
    }

    /// Handle `market_trades` events.
    async fn handle_trades(&mut self, event: &Value) {
        let now = Utc::now();
        let trades = match event.get("trades").and_then(|v| v.as_array()) {
            Some(t) => t.clone(),
            None => return,
        };

        for trade in &trades {
            let product_id = match trade.get("product_id").and_then(|v| v.as_str()) {
                Some(p) => p,
                None => continue,
            };
            let key = match self.prod_to_key.get(product_id) {
                Some(k) => k.clone(),
                None => continue,
            };

            let mkt_ts = trade
                .get("time")
                .and_then(|v| v.as_str())
                .and_then(|s| parse_iso8601(s))
                .unwrap_or(now);

            let side_str = trade.get("side").and_then(|v| v.as_str()).unwrap_or("");
            let side = if side_str.to_uppercase() == "BUY" { 1 } else { -1 };

            let price = trade
                .get("price")
                .and_then(|v| v.as_str())
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or(0.0);
            let amount = trade
                .get("size")
                .and_then(|v| v.as_str())
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or(0.0);
            let trade_id = trade
                .get("trade_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let mut mt = MarketTrade::new(
                &key,
                venue::COINBASE,
                "advanced-trade",
                side,
                price,
                amount,
                &trade_id,
                mkt_ts,
            );
            mt.timestamp = now;

            let dict = mt.to_dict();
            let json_str = serde_json::to_string(&dict).unwrap_or_default();

            if let Some(ref publisher) = self.publisher {
                let pub_clone = Arc::clone(publisher);
                let dict_clone = dict.clone();
                tokio::spawn(async move { pub_clone.send(&dict_clone).await; });
            }
            if let Some(logger) = self.tp_loggers.get(&key) {
                logger.info(json_str);
            }
        }
    }

    async fn emit_rate_from_book(&mut self, key: &str, now: DateTime<Utc>) {
        let new_rate = match self.market_books.get(key) {
            Some(b) => b.to_rate(),
            None => return,
        };

        let should_emit = self
            .rates
            .get(key)
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
            if let Some(logger) = self.tp_loggers.get(key) {
                logger.info(rate_json);
            }
            self.rates.insert(key.to_string(), new_rate);
        }
        let _ = now;
    }

    pub fn stop(&mut self) {
        self.running = false;
    }
}

fn parse_iso8601(s: &str) -> Option<DateTime<Utc>> {
    // "2019-08-14T20:42:27.265Z" or "2019-08-14T20:42:27.265123456Z"
    let s = s.replace('Z', "+00:00");
    DateTime::parse_from_rfc3339(&s)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}
