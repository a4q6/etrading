use std::collections::HashMap;
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

const WS_URL: &str = "wss://ws.lightstream.bitflyer.com/json-rpc";
const HANDLER_CLASS: &str = "BitFlyerSocketClient";

/// BitFlyer WebSocket feed handler — direct translation of Python BitFlyerSocketClient.
///
/// Handles:
/// - `lightning_executions_{pair}` → MarketTrade
/// - `lightning_board_snapshot_{pair}` → MarketBook (whole)
/// - `lightning_board_{pair}` → MarketBook (diff) + Rate derived
pub struct BitFlyerSocketClient {
    #[allow(dead_code)]
    ccy_pairs: Vec<String>,
    reconnect_attempts: Option<u32>,
    publisher: Option<Arc<LocalWsPublisher>>,
    tp_loggers: HashMap<String, TpLogger>,
    market_books: HashMap<String, MarketBook>,
    rates: HashMap<String, Rate>,
    last_emit_market_book: HashMap<String, DateTime<Utc>>,
    channels: Vec<String>,
    running: bool,
}

impl BitFlyerSocketClient {
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
        let mut channels = Vec::new();

        for pair in &ccy_pairs {
            let sym = pair.replace("_", "").to_uppercase();

            // TP logger: TP-BitFlyerSocketClient-FXBTCJPY
            let logger_name = format!("TP-{}-{}", HANDLER_CLASS, sym);
            tp_loggers.insert(pair.clone(), TpLogger::new(&logger_name, rtp_dir));

            // Initialize MarketBook and Rate
            market_books.insert(
                pair.clone(),
                MarketBook::new(&sym, venue::BITFLYER, "json-rpc"),
            );
            rates.insert(pair.clone(), Rate::new(&sym, venue::BITFLYER, "json-rpc"));

            // Epoch for throttle comparison
            last_emit.insert(
                pair.clone(),
                DateTime::from_timestamp(0, 0).unwrap_or_else(Utc::now),
            );

            // Subscribe channels
            channels.push(format!("lightning_board_snapshot_{}", pair));
            channels.push(format!("lightning_executions_{}", pair));
            channels.push(format!("lightning_board_{}", pair));
        }

        Self {
            ccy_pairs,
            reconnect_attempts,
            publisher,
            tp_loggers,
            market_books: market_books,
            rates,
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
                    // Normal close, reset attempts
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

        // Send subscribe messages for each channel (JSON-RPC)
        for channel in &self.channels {
            let subscribe_msg = json!({
                "method": "subscribe",
                "params": {"channel": channel}
            });
            write
                .send(Message::Text(subscribe_msg.to_string().into()))
                .await?;
            info!("Send request for '{}'", channel);
        }

        // Wrap write half for heartbeat task
        let write = Arc::new(Mutex::new(write));
        let _write_hb = Arc::clone(&write);

        // Heartbeat task (60s interval)
        let heartbeat_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                info!("(heartbeat) BitFlyer WebSocket is alive");
                // We just log — the read loop will detect if the connection died
            }
        });

        // Message read loop
        let result = loop {
            if !self.running {
                break Ok(());
            }

            match read.next().await {
                Some(Ok(Message::Text(text))) => {
                    match serde_json::from_str::<Value>(&text) {
                        Ok(message) => {
                            if !message.get("params").is_some() {
                                info!("no `params` key in message. pass this message.");
                                info!("{}", message);
                            } else {
                                self.process_message(&message).await;
                            }
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

        heartbeat_handle.abort();
        info!("Close websocket");
        result
    }

    async fn process_message(&mut self, message: &Value) {
        let params = match message.get("params") {
            Some(p) => p,
            None => return,
        };
        let channel = match params.get("channel").and_then(|v| v.as_str()) {
            Some(c) => c,
            None => {
                info!("Unknown message type: {}", message);
                return;
            }
        };

        if channel.contains("executions") {
            self.handle_executions(channel, params).await;
        } else if channel.contains("board_snapshot") {
            self.handle_board_snapshot(channel, params).await;
        } else if channel.contains("board") {
            self.handle_board_diff(channel, params).await;
        }

        // Rate derivation for any "board" channel (including board_snapshot)
        if channel.contains("board") {
            self.derive_rate(channel).await;
        }
    }

    /// Handle `lightning_executions_{pair}` → produce MarketTrade messages
    async fn handle_executions(&mut self, channel: &str, params: &Value) {
        let ccypair = channel.split("executions_").last().unwrap_or("");
        let sym = ccypair.replace("_", "").to_uppercase();

        let trades = match params.get("message").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return,
        };

        for one_trade in trades {
            // Parse exec_date
            let exec_date_str = one_trade
                .get("exec_date")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let market_ts = datamodel::parse_exec_date(exec_date_str);

            // Parse side: "BUY" → 1, "SELL" → -1
            let side_str = one_trade
                .get("side")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let side = match side_str {
                "BUY" => 1,
                "SELL" => -1,
                _ => 0,
            };

            let price = one_trade
                .get("price")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let amount = one_trade
                .get("size")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let trade_id = one_trade
                .get("id")
                .map(|v| v.to_string())
                .unwrap_or_default();

            let buy_id = one_trade
                .get("buy_child_order_acceptance_id")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let sell_id = one_trade
                .get("sell_child_order_acceptance_id")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            let mut data = MarketTrade::new(
                &sym,
                venue::BITFLYER,
                "json-rpc",
                side,
                price,
                amount,
                &trade_id,
                market_ts,
            );
            data.order_ids = Some(format!("{}_{}", buy_id, sell_id));

            let dict = data.to_dict();
            let json_str = serde_json::to_string(&dict).unwrap_or_default();

            // Publish (fire-and-forget via spawned task)
            if let Some(ref publisher) = self.publisher {
                let pub_clone = Arc::clone(publisher);
                let dict_clone = dict.clone();
                tokio::spawn(async move {
                    pub_clone.send(&dict_clone).await;
                });
            }

            // TP log
            if let Some(logger) = self.tp_loggers.get(ccypair) {
                logger.info(json_str);
            }
        }
    }

    /// Handle `lightning_board_snapshot_{pair}` → replace entire MarketBook
    async fn handle_board_snapshot(&mut self, channel: &str, params: &Value) {
        let ccypair = channel.split("board_snapshot_").last().unwrap_or("");

        let body = match params.get("message") {
            Some(b) => b,
            None => return,
        };

        let book = match self.market_books.get(ccypair) {
            Some(b) => b,
            None => return,
        };

        // Clone and update (Python deepcopy equivalent)
        let mut cur_book = book.clone();
        cur_book.timestamp = Utc::now();
        cur_book.universal_id = Uuid::new_v4().simple().to_string();
        cur_book.misc = "whole".to_string();

        // Set bids from snapshot
        if let Some(bids_arr) = body.get("bids").and_then(|v| v.as_array()) {
            cur_book.set_bids_from_json(bids_arr);
        }

        // Set asks from snapshot
        if let Some(asks_arr) = body.get("asks").and_then(|v| v.as_array()) {
            cur_book.set_asks_from_json(asks_arr);
        }

        let dict = cur_book.to_dict();
        let json_str = serde_json::to_string(&dict).unwrap_or_default();

        self.market_books.insert(ccypair.to_string(), cur_book);

        // Publish
        if let Some(ref publisher) = self.publisher {
            let pub_clone = Arc::clone(publisher);
            let dict_clone = dict.clone();
            tokio::spawn(async move {
                pub_clone.send(&dict_clone).await;
            });
        }

        // TP log (unconditional for snapshots)
        if let Some(logger) = self.tp_loggers.get(ccypair) {
            logger.info(json_str);
        }
    }

    /// Handle `lightning_board_{pair}` → apply diff to MarketBook
    async fn handle_board_diff(&mut self, channel: &str, params: &Value) {
        let ccypair = channel.split("board_").last().unwrap_or("");

        // Skip if no snapshot received yet (misc == "null")
        let is_initialized = self
            .market_books
            .get(ccypair)
            .map(|b| b.misc != "null")
            .unwrap_or(false);

        if !is_initialized {
            return;
        }

        let body = match params.get("message") {
            Some(b) => b,
            None => return,
        };

        // Apply diff to market book
        if let Some(book) = self.market_books.get_mut(ccypair) {
            book.timestamp = Utc::now();
            book.universal_id = Uuid::new_v4().simple().to_string();
            book.misc = "diff".to_string();

            // Apply ask diffs
            if let Some(asks) = body.get("asks").and_then(|v| v.as_array()) {
                for diff in asks {
                    let price = diff.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    let size = diff.get("size").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    book.apply_ask_diff(price, size);
                }
            }

            // Apply bid diffs
            if let Some(bids) = body.get("bids").and_then(|v| v.as_array()) {
                for diff in bids {
                    let price = diff.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    let size = diff.get("size").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    book.apply_bid_diff(price, size);
                }
            }
        }

        // Generate output
        if let Some(book) = self.market_books.get(ccypair) {
            let dict = book.to_dict();
            let json_str = serde_json::to_string(&dict).unwrap_or_default();

            // Publish (all diffs)
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

    /// Derive Rate from MarketBook (for any "board" channel)
    async fn derive_rate(&mut self, channel: &str) {
        // Extract ccypair from channel
        let ccypair = if channel.contains("board_snapshot_") {
            channel.split("board_snapshot_").last().unwrap_or("")
        } else {
            channel.split("board_").last().unwrap_or("")
        };

        let is_initialized = self
            .market_books
            .get(ccypair)
            .map(|b| b.misc != "null")
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

            // Only emit if mid_price changed
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
                if let Some(logger) = self.tp_loggers.get(ccypair) {
                    logger.info(json_str);
                }

                self.rates.insert(ccypair.to_string(), new_rate);
            }
        }
    }

    /// Signal graceful shutdown
    pub fn stop(&mut self) {
        self.running = false;
    }
}
