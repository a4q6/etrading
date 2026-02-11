use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};
use uuid::Uuid;

use etr_common::datamodel::{self, venue, MarketBook, MarketTrade, Rate};
use etr_common::tp_logger::TpLogger;
use etr_publisher::LocalWsPublisher;

const WS_URL: &str = "wss://stream.bitbank.cc/socket.io/?EIO=4&transport=websocket";
const HANDLER_CLASS: &str = "BitBankSocketClient";

/// BitBank WebSocket feed handler — Socket.IO v4 protocol.
///
/// Handles:
/// - `transactions_{pair}` → MarketTrade
/// - `depth_whole_{pair}` → MarketBook (whole snapshot)
/// - `depth_diff_{pair}` → MarketBook (incremental diff)
/// - `circuit_break_info_{pair}` → CircuitBreaker (direct TP output)
///
/// MarketBook uses sequence-id based diff buffering to ensure correct ordering.
pub struct BitBankSocketClient {
    #[allow(dead_code)]
    ccy_pairs: Vec<String>,
    reconnect_attempts: Option<u32>,
    publisher: Option<Arc<LocalWsPublisher>>,
    tp_loggers: HashMap<String, TpLogger>,
    market_books: HashMap<String, MarketBook>,
    rates: HashMap<String, Rate>,
    diff_message_buffer: HashMap<String, BTreeMap<i64, Value>>,
    last_emit_market_book: HashMap<String, DateTime<Utc>>,
    channels: Vec<String>,
    running: bool,
}

impl BitBankSocketClient {
    pub fn new(
        ccy_pairs: Vec<String>,
        reconnect_attempts: Option<u32>,
        publisher: Option<Arc<LocalWsPublisher>>,
        rtp_dir: &Path,
    ) -> Self {
        let mut tp_loggers = HashMap::new();
        let mut market_books = HashMap::new();
        let mut rates = HashMap::new();
        let mut diff_buffer = HashMap::new();
        let mut last_emit = HashMap::new();
        let mut channels = Vec::new();

        for pair in &ccy_pairs {
            let sym = pair.replace("_", "").to_uppercase();

            // TP logger: TP-BitBankSocketClient-BTCJPY
            let logger_name = format!("TP-{}-{}", HANDLER_CLASS, sym);
            tp_loggers.insert(pair.clone(), TpLogger::new(&logger_name, rtp_dir));

            // Initialize MarketBook with misc=["null", 0] equivalent
            let mut book = MarketBook::new(&sym, venue::BITBANK, "");
            book.misc = json!(["null", 0]).to_string();
            market_books.insert(pair.clone(), book);

            rates.insert(pair.clone(), Rate::new(&sym, venue::BITBANK, ""));

            diff_buffer.insert(pair.clone(), BTreeMap::new());

            last_emit.insert(
                pair.clone(),
                DateTime::from_timestamp(0, 0).unwrap_or_else(Utc::now),
            );

            channels.push(format!("transactions_{}", pair));
            channels.push(format!("depth_whole_{}", pair));
            channels.push(format!("depth_diff_{}", pair));
            channels.push(format!("circuit_break_info_{}", pair));
        }

        Self {
            ccy_pairs,
            reconnect_attempts,
            publisher,
            tp_loggers,
            market_books,
            rates,
            diff_message_buffer: diff_buffer,
            last_emit_market_book: last_emit,
            channels,
            running: true,
        }
    }

    /// Main entry point — connects and reconnects with exponential backoff.
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

        let (write, mut read) = ws_stream.split();
        let write = Arc::new(Mutex::new(write));

        // Socket.IO handshake + message loop
        let result = loop {
            if !self.running {
                break Ok(());
            }

            match read.next().await {
                Some(Ok(Message::Text(text))) => {
                    let text: &str = &text;
                    if text.starts_with("0") {
                        // Handshake: server sends "0{...}"
                        info!("Handshake: {}", text);
                        let mut w = write.lock().await;
                        w.send(Message::Text("40".into())).await?;
                    } else if text == "2" {
                        // Ping from server → respond with pong
                        info!("Response to ping(2) -> pong(3)");
                        let mut w = write.lock().await;
                        w.send(Message::Text("3".into())).await?;
                    } else if text.starts_with("40") {
                        // Connection established
                        info!("Connection established: {}", text);
                        // Subscribe to all channels
                        let mut w = write.lock().await;
                        for channel in &self.channels {
                            let subscribe_msg =
                                format!(r#"42["join-room","{}"]"#, channel);
                            w.send(Message::Text(subscribe_msg.into())).await?;
                            info!("Send request for '{}'", channel);
                        }
                    } else if text.starts_with("42") {
                        self.process_message(text).await;
                    } else {
                        info!("Uncategorized message: {}", text);
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

        info!("Close websocket");
        result
    }

    async fn process_message(&mut self, raw_msg: &str) {
        // Parse Socket.IO message: 42["message", {...}]
        let payload_str = &raw_msg[2..];
        let payload: Value = match serde_json::from_str(payload_str) {
            Ok(v) => v,
            Err(e) => {
                warn!("Failed to parse: {}", e);
                return;
            }
        };

        // Expect ["message", {body}]
        let arr = match payload.as_array() {
            Some(a) if a.len() == 2 && a[0].as_str() == Some("message") => a,
            _ => return,
        };

        let body = &arr[1];

        let room_name = match body.get("room_name").and_then(|v| v.as_str()) {
            Some(r) => r,
            None => return,
        };

        // Extract ccypair: last two segments joined by "_"
        let parts: Vec<&str> = room_name.split('_').collect();
        let ccypair = if parts.len() >= 2 {
            format!("{}_{}", parts[parts.len() - 2], parts[parts.len() - 1])
        } else {
            room_name.to_string()
        };

        if room_name.starts_with("transaction") {
            self.handle_transactions(&ccypair, body).await;
        } else if room_name.starts_with("depth_whole") {
            self.handle_depth_whole(&ccypair, body).await;
        } else if room_name.starts_with("depth_diff") {
            self.handle_depth_diff(&ccypair, body).await;
        } else if room_name.starts_with("circuit_break_info") {
            self.handle_circuit_breaker(&ccypair, body).await;
        }

        // Derive Rate from depth messages
        if room_name.starts_with("depth") {
            self.derive_rate(&ccypair).await;
        }
    }

    /// Handle `transactions_{pair}` → produce MarketTrade messages
    async fn handle_transactions(&mut self, ccypair: &str, body: &Value) {
        let sym = ccypair.replace("_", "").to_uppercase();

        let transactions = match body
            .get("message")
            .and_then(|v| v.get("data"))
            .and_then(|v| v.get("transactions"))
            .and_then(|v| v.as_array())
        {
            Some(arr) => arr,
            None => return,
        };

        for trs in transactions {
            let executed_at = trs
                .get("executed_at")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let market_ts = datamodel::from_millis(executed_at);

            let side_str = trs.get("side").and_then(|v| v.as_str()).unwrap_or("");
            let side = if side_str == "buy" { 1 } else { -1 };

            let price = trs
                .get("price")
                .and_then(|v| v.as_str())
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or(0.0);
            let amount = trs
                .get("amount")
                .and_then(|v| v.as_str())
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or(0.0);
            let trade_id = trs
                .get("transaction_id")
                .map(|v| v.to_string())
                .unwrap_or_default();

            let data = MarketTrade::new(
                &sym,
                venue::BITBANK,
                "",
                side,
                price,
                amount,
                &trade_id,
                market_ts,
            );

            let dict = data.to_dict();
            let json_str = serde_json::to_string(&dict).unwrap_or_default();

            if let Some(ref publisher) = self.publisher {
                let pub_clone = Arc::clone(publisher);
                let dict_clone = dict.clone();
                tokio::spawn(async move {
                    pub_clone.send(&dict_clone).await;
                });
            }

            if let Some(logger) = self.tp_loggers.get(ccypair) {
                logger.info(json_str);
            }
        }
    }

    /// Handle `depth_whole_{pair}` → replace entire MarketBook, replay buffered diffs
    async fn handle_depth_whole(&mut self, ccypair: &str, body: &Value) {
        let book_msg = match body
            .get("message")
            .and_then(|v| v.get("data"))
        {
            Some(d) => d,
            None => return,
        };

        let sequence_id = match book_msg
            .get("sequenceId")
            .and_then(|v| v.as_str())
            .and_then(|v| v.parse::<i64>().ok())
        {
            Some(s) => s,
            None => return,
        };

        let market_ts_millis = book_msg
            .get("timestamp")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        if let Some(book) = self.market_books.get_mut(ccypair) {
            let sym = ccypair.replace("_", "").to_uppercase();
            book.sym = sym;
            book.timestamp = Utc::now();
            book.market_created_timestamp = datamodel::from_millis(market_ts_millis);
            book.universal_id = Uuid::new_v4().simple().to_string();

            // Set bids/asks from whole snapshot
            if let Some(bids_arr) = book_msg.get("bids").and_then(|v| v.as_array()) {
                book.set_bids_from_str_array(bids_arr);
            }
            if let Some(asks_arr) = book_msg.get("asks").and_then(|v| v.as_array()) {
                book.set_asks_from_str_array(asks_arr);
            }
            book.misc = json!(["whole", sequence_id]).to_string();

            // Discard older diffs, replay newer ones
            if let Some(buffer) = self.diff_message_buffer.get_mut(ccypair) {
                // Keep only diffs with s_id > sequence_id
                *buffer = buffer.split_off(&(sequence_id + 1));

                // Replay remaining diffs
                let diffs: Vec<(i64, Value)> = buffer.iter().map(|(k, v)| (*k, v.clone())).collect();
                for (s_id, msg) in diffs {
                    let t = msg.get("t").and_then(|v| v.as_i64()).unwrap_or(0);
                    book.market_created_timestamp = datamodel::from_millis(t);

                    // Apply ask diffs
                    if let Some(asks) = msg.get("a").and_then(|v| v.as_array()) {
                        for diff in asks {
                            if let Some(arr) = diff.as_array() {
                                if arr.len() >= 2 {
                                    let price = arr[0].as_str().and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
                                    let amt = arr[1].as_str().and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
                                    if amt == 0.0 {
                                        book.apply_ask_diff(price, 0.0);
                                    } else {
                                        book.apply_ask_diff(price, amt);
                                    }
                                }
                            }
                        }
                    }

                    // Apply bid diffs
                    if let Some(bids) = msg.get("b").and_then(|v| v.as_array()) {
                        for diff in bids {
                            if let Some(arr) = diff.as_array() {
                                if arr.len() >= 2 {
                                    let price = arr[0].as_str().and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
                                    let amt = arr[1].as_str().and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
                                    if amt == 0.0 {
                                        book.apply_bid_diff(price, 0.0);
                                    } else {
                                        book.apply_bid_diff(price, amt);
                                    }
                                }
                            }
                        }
                    }

                    book.misc = json!(["whole", s_id]).to_string();
                }
            }
        }

        // Output
        if let Some(book) = self.market_books.get(ccypair) {
            let dict = book.to_dict();
            let json_str = serde_json::to_string(&dict).unwrap_or_default();

            if let Some(ref publisher) = self.publisher {
                let pub_clone = Arc::clone(publisher);
                let dict_clone = dict.clone();
                tokio::spawn(async move {
                    pub_clone.send(&dict_clone).await;
                });
            }

            if let Some(logger) = self.tp_loggers.get(ccypair) {
                logger.info(json_str);
            }
        }
    }

    /// Handle `depth_diff_{pair}` → apply diff to MarketBook with sequence ordering
    async fn handle_depth_diff(&mut self, ccypair: &str, body: &Value) {
        let msg = match body
            .get("message")
            .and_then(|v| v.get("data"))
        {
            Some(d) => d,
            None => return,
        };

        let sequence_id = match msg
            .get("s")
            .and_then(|v| v.as_str())
            .and_then(|v| v.parse::<i64>().ok())
        {
            Some(s) => s,
            None => return,
        };

        // Buffer the diff
        if let Some(buffer) = self.diff_message_buffer.get_mut(ccypair) {
            buffer.insert(sequence_id, msg.clone());
        }

        // Check if MarketBook is initialized
        let is_initialized = self
            .market_books
            .get(ccypair)
            .map(|b| !b.misc.contains("\"null\""))
            .unwrap_or(false);

        if !is_initialized {
            return;
        }

        // Get current misc last element (last sequence_id)
        let cur_seq = self
            .market_books
            .get(ccypair)
            .and_then(|b| {
                serde_json::from_str::<Value>(&b.misc)
                    .ok()
                    .and_then(|v| v.as_array().and_then(|a| a.last().and_then(|x| x.as_i64())))
            })
            .unwrap_or(0);

        if let Some(book) = self.market_books.get_mut(ccypair) {
            book.universal_id = Uuid::new_v4().simple().to_string();

            if cur_seq < sequence_id {
                book.timestamp = Utc::now();
                let t = msg.get("t").and_then(|v| v.as_i64()).unwrap_or(0);
                book.market_created_timestamp = datamodel::from_millis(t);

                // Apply ask diffs
                if let Some(asks) = msg.get("a").and_then(|v| v.as_array()) {
                    for diff in asks {
                        if let Some(arr) = diff.as_array() {
                            if arr.len() >= 2 {
                                let price = arr[0].as_str().and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
                                let amt = arr[1].as_str().and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
                                if amt == 0.0 {
                                    book.apply_ask_diff(price, 0.0);
                                } else {
                                    book.apply_ask_diff(price, amt);
                                }
                            }
                        }
                    }
                }

                // Apply bid diffs
                if let Some(bids) = msg.get("b").and_then(|v| v.as_array()) {
                    for diff in bids {
                        if let Some(arr) = diff.as_array() {
                            if arr.len() >= 2 {
                                let price = arr[0].as_str().and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
                                let amt = arr[1].as_str().and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
                                if amt == 0.0 {
                                    book.apply_bid_diff(price, 0.0);
                                } else {
                                    book.apply_bid_diff(price, amt);
                                }
                            }
                        }
                    }
                }

                book.misc = json!(["diff", sequence_id]).to_string();
            }
        }

        // Output
        if let Some(book) = self.market_books.get(ccypair) {
            let dict = book.to_dict();
            let json_str = serde_json::to_string(&dict).unwrap_or_default();

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
                .get(ccypair)
                .copied()
                .unwrap_or(DateTime::from_timestamp(0, 0).unwrap_or_else(Utc::now));

            if last + TimeDelta::milliseconds(250) < now {
                if let Some(logger) = self.tp_loggers.get(ccypair) {
                    logger.info(json_str);
                }
                self.last_emit_market_book.insert(ccypair.to_string(), now);
            }
        }
    }

    /// Handle `circuit_break_info_{pair}` → direct TP output
    async fn handle_circuit_breaker(&mut self, ccypair: &str, body: &Value) {
        let msg = match body
            .get("message")
            .and_then(|v| v.get("data"))
        {
            Some(d) => d,
            None => return,
        };

        let sym = ccypair.replace("_", "").to_uppercase();
        let ts_millis = msg.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0);
        let market_ts = datamodel::from_millis(ts_millis);

        let mut cb_msg = msg.clone();
        if let Some(obj) = cb_msg.as_object_mut() {
            obj.insert("_data_type".to_string(), json!("CircuitBreaker"));
            obj.insert(
                "received_timestamp".to_string(),
                json!(datamodel::format_timestamp(&Utc::now())),
            );
            obj.insert(
                "timestamp".to_string(),
                json!(datamodel::format_timestamp(&market_ts)),
            );
            obj.insert("sym".to_string(), json!(sym));
            obj.insert("venue".to_string(), json!(venue::BITBANK));
        }

        let json_str = serde_json::to_string(&cb_msg).unwrap_or_default();

        if let Some(logger) = self.tp_loggers.get(ccypair) {
            logger.info(json_str);
        }
    }

    /// Derive Rate from MarketBook (for depth channels)
    async fn derive_rate(&mut self, ccypair: &str) {
        let is_initialized = self
            .market_books
            .get(ccypair)
            .map(|b| !b.misc.contains("\"null\""))
            .unwrap_or(false);

        if !is_initialized {
            return;
        }

        if let Some(book) = self.market_books.get(ccypair) {
            let new_rate = book.to_rate();
            let prev_mid = self
                .rates
                .get(ccypair)
                .map(|r| r.mid_price)
                .unwrap_or(f64::NAN);

            if new_rate.mid_price != prev_mid || prev_mid.is_nan() {
                let dict = new_rate.to_dict();
                let json_str = serde_json::to_string(&dict).unwrap_or_default();

                if let Some(ref publisher) = self.publisher {
                    let pub_clone = Arc::clone(publisher);
                    let dict_clone = dict.clone();
                    tokio::spawn(async move {
                        pub_clone.send(&dict_clone).await;
                    });
                }

                if let Some(logger) = self.tp_loggers.get(ccypair) {
                    logger.info(json_str);
                }

                self.rates.insert(ccypair.to_string(), new_rate);
            }
        }
    }

    pub fn stop(&mut self) {
        self.running = false;
    }
}
