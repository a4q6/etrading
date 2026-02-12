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
use uuid::Uuid;

use etr_common::datamodel::{self, venue, MarketBook, MarketTrade, Rate};
use etr_common::tp_logger::TpLogger;
use etr_publisher::LocalWsPublisher;

const WS_URL: &str = "wss://www.bitmex.com/realtime";
const HANDLER_CLASS: &str = "BitmexSocketClient";

/// BitMex WebSocket feed handler — direct translation of Python BitmexSocketClient.
///
/// Handles:
/// - `trade:{pair}` → MarketTrade
/// - `orderBookL2_25:{pair}` → MarketBook (ID-based management)
/// - Rate derived from MarketBook
pub struct BitmexSocketClient {
    #[allow(dead_code)]
    ccy_pairs: Vec<String>,
    reconnect_attempts: Option<u32>,
    publisher: Option<Arc<LocalWsPublisher>>,
    tp_loggers: HashMap<String, TpLogger>,
    market_books: HashMap<String, MarketBook>,
    rates: HashMap<String, Rate>,
    last_emit_market_book: HashMap<String, DateTime<Utc>>,
    /// ID-based price mapping: (sym, side) → {id → price}
    id2price: HashMap<(String, String), HashMap<i64, f64>>,
    subscriptions: Vec<String>,
    running: bool,
}

impl BitmexSocketClient {
    pub fn new(
        ccy_pairs: Vec<String>,
        reconnect_attempts: Option<u32>,
        publisher: Option<Arc<LocalWsPublisher>>,
        rtp_dir: &Path,
    ) -> Self {
        let mut tp_loggers = HashMap::new();
        let mut market_books = HashMap::new();
        let mut rates = HashMap::new();
        let mut last_emit = HashMap::new();
        let mut id2price = HashMap::new();
        let mut subscriptions = Vec::new();

        for pair in &ccy_pairs {
            let sym = pair.replace("_", "").to_uppercase();

            let logger_name = format!("TP-{}-{}", HANDLER_CLASS, sym);
            tp_loggers.insert(sym.clone(), TpLogger::new(&logger_name, rtp_dir));

            market_books.insert(
                sym.clone(),
                MarketBook::new(&sym, venue::BITMEX, ""),
            );
            rates.insert(sym.clone(), Rate::new(&sym, venue::BITMEX, ""));

            last_emit.insert(
                sym.clone(),
                DateTime::from_timestamp(0, 0).unwrap_or_else(Utc::now),
            );

            // ID maps for Buy and Sell sides
            id2price.insert((sym.clone(), "Buy".to_string()), HashMap::new());
            id2price.insert((sym.clone(), "Sell".to_string()), HashMap::new());

            subscriptions.push(format!("trade:{}", pair));
            subscriptions.push(format!("orderBookL2_25:{}", pair));
        }

        Self {
            ccy_pairs,
            reconnect_attempts,
            publisher,
            tp_loggers,
            market_books,
            rates,
            last_emit_market_book: last_emit,
            id2price,
            subscriptions,
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
        let (ws_stream, _) = tokio_tungstenite::connect_async(WS_URL).await?;
        info!("Start subscribing: {}", WS_URL);

        let (mut write, mut read) = ws_stream.split();

        // Send subscribe messages
        for sub in &self.subscriptions {
            let payload = json!({"op": "subscribe", "args": [sub]});
            write
                .send(Message::Text(payload.to_string().into()))
                .await?;
            info!("Send request for {}", sub);
        }

        // Wrap write half for ping loop
        let write = Arc::new(Mutex::new(write));
        let write_ping = Arc::clone(&write);

        // Ping loop: send "ping" text every 60s (BitMex uses text ping, not WS ping)
        let ping_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                let mut w = write_ping.lock().await;
                if let Err(e) = w.send(Message::Text("ping".into())).await {
                    error!("Ping loop error: {}", e);
                    break;
                }
                info!("Sent ping");
            }
        });

        // Message read loop
        let result = loop {
            if !self.running {
                break Ok(());
            }

            match read.next().await {
                Some(Ok(Message::Text(text))) => {
                    let text_str: &str = &text;
                    if text_str == "pong" {
                        info!("Received pong");
                        continue;
                    }
                    match serde_json::from_str::<Value>(text_str) {
                        Ok(message) => {
                            self.process_message(&message).await;
                        }
                        Err(e) => {
                            warn!("Failed to parse JSON message: {}", e);
                        }
                    }
                }
                Some(Ok(Message::Close(_))) => {
                    info!("Websocket closed OK");
                    break Ok(());
                }
                Some(Ok(Message::Ping(data))) => {
                    let mut w = write.lock().await;
                    let _ = w.send(Message::Pong(data)).await;
                }
                Some(Err(e)) => {
                    error!("Websocket closed ERR: {}", e);
                    break Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
                }
                None => {
                    info!("WebSocket stream ended");
                    break Ok(());
                }
                _ => {}
            }
        };

        ping_handle.abort();
        info!("Close websocket");
        result
    }

    async fn process_message(&mut self, message: &Value) {
        let table = message.get("table").and_then(|v| v.as_str());

        match table {
            Some("trade") => {
                self.handle_trade(message).await;
            }
            Some("orderBookL2_25") => {
                self.handle_order_book(message).await;
                self.derive_rate().await;
            }
            _ => {
                // info/subscription/error messages — skip silently
            }
        }
    }

    /// Handle `trade` table → MarketTrade
    async fn handle_trade(&mut self, message: &Value) {
        let data_arr = match message.get("data").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return,
        };

        for one_trade in data_arr {
            let sym = one_trade
                .get("symbol")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            // Parse timestamp
            let ts_str = one_trade
                .get("timestamp")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let market_ts = datamodel::parse_exec_date(ts_str);

            // Side: "Buy" → 1, "Sell" → -1
            let side_str = one_trade
                .get("side")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let side = match side_str {
                "Buy" => 1,
                "Sell" => -1,
                _ => 0,
            };

            let price = one_trade
                .get("price")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let amount = one_trade
                .get("homeNotional")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let trade_id = one_trade
                .get("trdMatchID")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let tick_direction = one_trade
                .get("tickDirection")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let mut data = MarketTrade::new(
                &sym,
                venue::BITMEX,
                "",
                side,
                price,
                amount,
                &trade_id,
                market_ts,
            );
            data.order_ids = Some(String::new());
            data.misc = Some(tick_direction);

            let dict = data.to_dict();
            let json_str = serde_json::to_string(&dict).unwrap_or_default();

            // Publish
            if let Some(ref publisher) = self.publisher {
                let pub_clone = Arc::clone(publisher);
                let dict_clone = dict.clone();
                tokio::spawn(async move {
                    pub_clone.send(&dict_clone).await;
                });
            }

            // TP log
            if let Some(logger) = self.tp_loggers.get(&sym) {
                logger.info(json_str);
            }
        }
    }

    /// Handle `orderBookL2_25` table → MarketBook (ID-based management)
    async fn handle_order_book(&mut self, message: &Value) {
        let action = match message.get("action").and_then(|v| v.as_str()) {
            Some(a) => a.to_string(),
            None => return,
        };

        let data_arr = match message.get("data").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return,
        };

        // Extract unique symbols from data
        let mut syms: Vec<String> = Vec::new();
        for item in data_arr {
            if let Some(sym) = item.get("symbol").and_then(|v| v.as_str()) {
                if !syms.contains(&sym.to_string()) {
                    syms.push(sym.to_string());
                }
            }
        }

        match action.as_str() {
            "partial" | "insert" => {
                if action == "partial" {
                    // Reset book and id maps for all syms
                    for sym in &syms {
                        if let Some(book) = self.market_books.get_mut(sym.as_str()) {
                            book.bids.clear();
                            book.asks.clear();
                        }
                        if let Some(map) = self.id2price.get_mut(&(sym.clone(), "Buy".to_string())) {
                            map.clear();
                        }
                        if let Some(map) = self.id2price.get_mut(&(sym.clone(), "Sell".to_string())) {
                            map.clear();
                        }
                    }
                }

                for item in data_arr {
                    let sym = match item.get("symbol").and_then(|v| v.as_str()) {
                        Some(s) => s.to_string(),
                        None => continue,
                    };
                    let side = match item.get("side").and_then(|v| v.as_str()) {
                        Some(s) => s.to_string(),
                        None => continue,
                    };
                    let id = item.get("id").and_then(|v| v.as_i64()).unwrap_or(0);
                    let price = item.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    let size = item.get("size").and_then(|v| v.as_f64()).unwrap_or(0.0);

                    // Only process if partial (which resets) or book is already initialized
                    let is_initialized = action == "partial"
                        || self
                            .market_books
                            .get(&sym)
                            .map(|b| b.misc != "null")
                            .unwrap_or(false);

                    if is_initialized {
                        // Store id → price mapping
                        if let Some(map) = self.id2price.get_mut(&(sym.clone(), side.clone())) {
                            map.insert(id, price);
                        }
                        // Update book
                        if let Some(book) = self.market_books.get_mut(&sym) {
                            match side.as_str() {
                                "Buy" => {
                                    book.bids.insert(OrderedFloat(price), size);
                                }
                                "Sell" => {
                                    book.asks.insert(OrderedFloat(price), size);
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }

            "update" => {
                for item in data_arr {
                    let sym = match item.get("symbol").and_then(|v| v.as_str()) {
                        Some(s) => s.to_string(),
                        None => continue,
                    };
                    let side = match item.get("side").and_then(|v| v.as_str()) {
                        Some(s) => s.to_string(),
                        None => continue,
                    };
                    let id = item.get("id").and_then(|v| v.as_i64()).unwrap_or(0);
                    let size = item.get("size").and_then(|v| v.as_f64()).unwrap_or(0.0);

                    let is_initialized = self
                        .market_books
                        .get(&sym)
                        .map(|b| b.misc != "null")
                        .unwrap_or(false);

                    if is_initialized {
                        // Look up price from id
                        let price = self
                            .id2price
                            .get(&(sym.clone(), side.clone()))
                            .and_then(|map| map.get(&id))
                            .copied();

                        if let Some(price) = price {
                            if let Some(book) = self.market_books.get_mut(&sym) {
                                match side.as_str() {
                                    "Buy" => {
                                        book.bids.insert(OrderedFloat(price), size);
                                    }
                                    "Sell" => {
                                        book.asks.insert(OrderedFloat(price), size);
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }

            "delete" => {
                for item in data_arr {
                    let sym = match item.get("symbol").and_then(|v| v.as_str()) {
                        Some(s) => s.to_string(),
                        None => continue,
                    };
                    let side = match item.get("side").and_then(|v| v.as_str()) {
                        Some(s) => s.to_string(),
                        None => continue,
                    };
                    let id = item.get("id").and_then(|v| v.as_i64()).unwrap_or(0);

                    let is_initialized = self
                        .market_books
                        .get(&sym)
                        .map(|b| b.misc != "null")
                        .unwrap_or(false);

                    if is_initialized {
                        // Look up price and remove from book and id map
                        let price = self
                            .id2price
                            .get_mut(&(sym.clone(), side.clone()))
                            .and_then(|map| map.remove(&id));

                        if let Some(price) = price {
                            if let Some(book) = self.market_books.get_mut(&sym) {
                                let key = OrderedFloat(price);
                                match side.as_str() {
                                    "Buy" => {
                                        book.bids.remove(&key);
                                    }
                                    "Sell" => {
                                        book.asks.remove(&key);
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }

            _ => {}
        }

        // Update timestamps and misc for all affected syms
        for sym in &syms {
            if let Some(book) = self.market_books.get_mut(sym.as_str()) {
                book.timestamp = Utc::now();
                book.universal_id = Uuid::new_v4().simple().to_string();
                book.misc = action.clone();
            }
        }

        // Distribute MarketBook
        for sym in &syms {
            if let Some(book) = self.market_books.get(sym.as_str()) {
                let dict = book.to_dict();
                let json_str = serde_json::to_string(&dict).unwrap_or_default();

                // Publish
                if let Some(ref publisher) = self.publisher {
                    let pub_clone = Arc::clone(publisher);
                    let dict_clone = dict.clone();
                    tokio::spawn(async move {
                        pub_clone.send(&dict_clone).await;
                    });
                }

                // TP log with 250ms throttling
                let now = book.timestamp;
                let last = self
                    .last_emit_market_book
                    .get(sym.as_str())
                    .copied()
                    .unwrap_or(DateTime::from_timestamp(0, 0).unwrap_or_else(Utc::now));

                if last + TimeDelta::milliseconds(250) < now {
                    if let Some(logger) = self.tp_loggers.get(sym.as_str()) {
                        logger.info(json_str);
                    }
                    self.last_emit_market_book
                        .insert(sym.clone(), now);
                }
            }
        }
    }

    /// Derive Rate from MarketBook (emit when mid_price changes)
    async fn derive_rate(&mut self) {
        let syms: Vec<String> = self.market_books.keys().cloned().collect();

        for sym in syms {
            let is_initialized = self
                .market_books
                .get(&sym)
                .map(|b| b.misc != "null")
                .unwrap_or(false);

            if !is_initialized {
                continue;
            }

            if let Some(book) = self.market_books.get(&sym) {
                let new_rate = book.to_rate();
                let prev_mid = self
                    .rates
                    .get(&sym)
                    .map(|r| r.mid_price)
                    .unwrap_or(f64::NAN);

                if new_rate.mid_price != prev_mid || prev_mid.is_nan() {
                    let dict = new_rate.to_dict();
                    let json_str = serde_json::to_string(&dict).unwrap_or_default();

                    // Publish
                    if let Some(ref publisher) = self.publisher {
                        let pub_clone = Arc::clone(publisher);
                        let dict_clone = dict.clone();
                        tokio::spawn(async move {
                            pub_clone.send(&dict_clone).await;
                        });
                    }

                    // TP log
                    if let Some(logger) = self.tp_loggers.get(&sym) {
                        logger.info(json_str);
                    }

                    self.rates.insert(sym.clone(), new_rate);
                }
            }
        }
    }

    pub fn stop(&mut self) {
        self.running = false;
    }
}
