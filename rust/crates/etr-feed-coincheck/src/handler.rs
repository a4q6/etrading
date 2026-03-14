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

use etr_common::datamodel::{venue, MarketBook, MarketTrade, Rate};
use etr_common::tp_logger::TpLogger;
use etr_publisher::LocalWsPublisher;

const WS_URL: &str = "wss://ws-api.coincheck.com/";
const HANDLER_CLASS: &str = "CoincheckSocketClient";
const BOOK_THROTTLE_MS: i64 = 100;

/// Coincheck WebSocket feed handler.
///
/// Subscribe format: `{"type": "subscribe", "channel": "{pair}-orderbook" | "{pair}-trades"}`
///
/// ### OrderBook message (incremental diff):
/// ```json
/// ["btc_jpy", {"last_update_at": "1620000000", "bids": [["p","s"],...], "asks": [...]}]
/// ```
/// size == "0" → remove price level. First message acts as initial snapshot.
/// Emitted only after misc transitions from "null" (triggered by first trade).
///
/// ### Trade message:
/// ```json
/// [["ts_sec","trade_id","btc_jpy","price","amount","?","?","buy"|"sell","order_id1","order_id2"], ...]
/// ```
/// Each inner array has 8 elements. After each trade the book is pruned:
/// - buy → remove asks below trade price
/// - sell → remove bids above trade price
pub struct CoincheckSocketClient {
    /// ccypair in Coincheck format, e.g. ["btc_jpy"]
    ccy_pairs: Vec<String>,
    reconnect_attempts: Option<u32>,
    publisher: Option<Arc<LocalWsPublisher>>,
    /// key: Coincheck pair string (e.g. "btc_jpy")
    tp_loggers: HashMap<String, TpLogger>,
    market_books: HashMap<String, MarketBook>,
    rates: HashMap<String, Rate>,
    last_emit_book: HashMap<String, DateTime<Utc>>,
    running: bool,
}

impl CoincheckSocketClient {
    pub fn new(
        ccy_pairs: Vec<String>,
        reconnect_attempts: Option<u32>,
        publisher: Option<Arc<LocalWsPublisher>>,
        rtp_dir: &Path,
    ) -> Self {
        let mut tp_loggers = HashMap::new();
        let mut market_books = HashMap::new();
        let mut rates = HashMap::new();
        let mut last_emit_book = HashMap::new();

        for pair in &ccy_pairs {
            let sym = pair.replace('_', "").to_uppercase();

            let logger_name = format!("TP-{}-{}", HANDLER_CLASS, sym);
            tp_loggers.insert(pair.clone(), TpLogger::new(&logger_name, rtp_dir));

            market_books.insert(pair.clone(), MarketBook::new(&sym, venue::COINCHECK, "websocket"));
            rates.insert(pair.clone(), Rate::new(&sym, venue::COINCHECK, "websocket"));
            last_emit_book.insert(
                pair.clone(),
                DateTime::from_timestamp(0, 0).unwrap_or_else(Utc::now),
            );
        }

        Self {
            ccy_pairs,
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
        info!("Connecting to {}", WS_URL);
        let (ws_stream, _) = tokio_tungstenite::connect_async(WS_URL).await?;
        info!("Start subscribing: {}", WS_URL);

        let (write, mut read) = ws_stream.split();
        let write = Arc::new(Mutex::new(write));

        // Subscribe to orderbook and trades for each pair
        {
            let mut w = write.lock().await;
            for pair in &self.ccy_pairs {
                for channel_type in &["orderbook", "trades"] {
                    let channel = format!("{}-{}", pair, channel_type);
                    let sub = json!({"type": "subscribe", "channel": channel});
                    w.send(Message::Text(sub.to_string().into())).await?;
                    info!("Subscribed: {}", channel);
                }
            }
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
        let arr = match msg.as_array() {
            Some(a) => a,
            None => return,
        };

        if arr.is_empty() {
            return;
        }

        // OrderBook: ["btc_jpy", {"last_update_at": ..., "bids": [...], "asks": [...]}]
        if arr.len() == 2 {
            if let (Some(pair), Some(obj)) = (arr[0].as_str(), arr[1].as_object()) {
                if obj.contains_key("last_update_at") {
                    self.handle_orderbook(pair, &arr[1]).await;
                    return;
                }
            }
        }

        // Trade: [[ts, id, pair, price, amount, ?, ?, side, oid1, oid2], ...]
        if let Some(first) = arr.first() {
            if let Some(inner) = first.as_array() {
                if inner.len() == 8 {
                    self.handle_trades(arr).await;
                    return;
                }
            }
        }
    }

    /// Handle `{pair}-orderbook` — incremental diff.
    /// First message acts as initial snapshot. size=="0" removes the level.
    /// Not emitted until book has been initialized (misc != "null").
    async fn handle_orderbook(&mut self, pair: &str, data: &Value) {
        let now = Utc::now();

        let mkt_ts = data
            .get("last_update_at")
            .and_then(|v| v.as_str())
            .and_then(|v| v.parse::<i64>().ok())
            .and_then(|secs| DateTime::from_timestamp(secs, 0))
            .unwrap_or(now);

        let book = match self.market_books.get_mut(pair) {
            Some(b) => b,
            None => return,
        };

        // Apply diff: size "0" → remove, else → upsert
        if let Some(bids) = data.get("bids").and_then(|v| v.as_array()) {
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
        if let Some(asks) = data.get("asks").and_then(|v| v.as_array()) {
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

        // Only emit after book is initialized by first trade (misc != "null")
        if book.misc == "null" {
            return;
        }

        let dict = book.to_dict();

        if let Some(ref publisher) = self.publisher {
            let pub_clone = Arc::clone(publisher);
            let dict_clone = dict.clone();
            tokio::spawn(async move { pub_clone.send(&dict_clone).await; });
        }

        // Throttle TP log to 100ms
        let last = self.last_emit_book.get(pair).copied().unwrap_or_else(|| {
            DateTime::from_timestamp(0, 0).unwrap_or_else(Utc::now)
        });
        if now - last >= TimeDelta::milliseconds(BOOK_THROTTLE_MS) {
            let json_str = serde_json::to_string(&dict).unwrap_or_default();
            if let Some(logger) = self.tp_loggers.get(pair) {
                logger.info(json_str);
            }
            self.last_emit_book.insert(pair.to_string(), now);
        }

        // Emit Rate if mid_price changed
        let new_rate = book.to_rate();
        let should_emit = self
            .rates
            .get(pair)
            .map(|r| r.mid_price != new_rate.mid_price)
            .unwrap_or(true);

        if should_emit {
            let rate_dict = new_rate.to_dict();
            let rate_json = serde_json::to_string(&rate_dict).unwrap_or_default();
            if let Some(ref publisher) = self.publisher {
                let pub_clone = Arc::clone(publisher);
                let dict_clone = rate_dict.clone();
                tokio::spawn(async move { pub_clone.send(&dict_clone).await; });
            }
            if let Some(logger) = self.tp_loggers.get(pair) {
                logger.info(rate_json);
            }
            self.rates.insert(pair.to_string(), new_rate);
        }
    }

    /// Handle `{pair}-trades`.
    ///
    /// Inner array (len=8): [ts_sec, trade_id, pair, price, amount, ?, ?, side, order_id1, order_id2]
    /// After each trade, prune the book to keep it consistent:
    /// - buy  → remove ask levels with price < trade_price
    /// - sell → remove bid levels with price > trade_price
    async fn handle_trades(&mut self, arr: &[Value]) {
        let now = Utc::now();

        for item in arr {
            let t = match item.as_array() {
                Some(a) if a.len() == 8 => a,
                _ => continue,
            };

            let ts_sec = t[0].as_str().and_then(|v| v.parse::<i64>().ok()).unwrap_or(0);
            let mkt_ts = DateTime::from_timestamp(ts_sec, 0).unwrap_or(now);

            let trade_id = t[1].as_str().unwrap_or("").to_string();
            let pair = t[2].as_str().unwrap_or("").to_string();
            let sym = pair.replace('_', "").to_uppercase();

            let price = t[3].as_str().and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
            let amount = t[4].as_str().and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
            let side_str = t[7].as_str().unwrap_or("");
            let side = if side_str == "buy" { 1 } else { -1 };
            let order_id1 = t[6].as_str().unwrap_or("").to_string();
            let order_id2 = t[7].as_str().unwrap_or("").to_string();

            let mut mt = MarketTrade::new(
                &sym,
                venue::COINCHECK,
                "websocket",
                side,
                price,
                amount,
                &trade_id,
                mkt_ts,
            );
            mt.timestamp = now;
            mt.order_ids = Some(format!("{}_{}", order_id1, order_id2));

            let dict = mt.to_dict();
            let json_str = serde_json::to_string(&dict).unwrap_or_default();

            if let Some(ref publisher) = self.publisher {
                let pub_clone = Arc::clone(publisher);
                let dict_clone = dict.clone();
                tokio::spawn(async move { pub_clone.send(&dict_clone).await; });
            }
            if let Some(logger) = self.tp_loggers.get(&pair) {
                logger.info(json_str);
            }

            // Prune book based on executed price and initialize if needed
            if let Some(book) = self.market_books.get_mut(&pair) {
                if book.market_created_timestamp <= mkt_ts {
                    if side > 0 {
                        // buy: remove asks below trade price (they should have been consumed)
                        book.asks.retain(|p, _| p.into_inner() >= price);
                    } else {
                        // sell: remove bids above trade price
                        book.bids.retain(|p, _| p.into_inner() <= price);
                    }
                    // Mark book as initialized (enables orderbook emission)
                    book.misc = "trade_init".to_string();
                }
            }
        }
    }

    pub fn stop(&mut self) {
        self.running = false;
    }
}
