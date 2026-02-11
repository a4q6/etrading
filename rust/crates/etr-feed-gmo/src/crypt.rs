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

const WS_URL: &str = "wss://api.coin.z.com/ws/public/v1";
const HANDLER_CLASS: &str = "GmoCryptSocketClient";

/// GMO Crypto WebSocket feed handler — plain JSON protocol.
///
/// Handles:
/// - `trades` → MarketTrade (TAKER_ONLY)
/// - `orderbooks` → MarketBook (full snapshot) + Rate derived
///
/// MarketBook is always a full snapshot (no diff management needed).
pub struct GmoCryptSocketClient {
    #[allow(dead_code)]
    ccy_pairs: Vec<String>,
    reconnect_attempts: Option<u32>,
    publisher: Option<Arc<LocalWsPublisher>>,
    tp_loggers: HashMap<String, TpLogger>,
    market_books: HashMap<String, MarketBook>,
    rates: HashMap<String, Rate>,
    last_emit_market_book: HashMap<String, DateTime<Utc>>,
    heartbeat_memo: DateTime<Utc>,
    running: bool,
}

impl GmoCryptSocketClient {
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

        for pair in &ccy_pairs {
            let sym = pair.replace("_", "").to_uppercase();

            let logger_name = format!("TP-{}-{}", HANDLER_CLASS, sym);
            tp_loggers.insert(pair.clone(), TpLogger::new(&logger_name, rtp_dir));

            market_books.insert(
                pair.clone(),
                MarketBook::new(&sym, venue::GMO, "websocket"),
            );
            rates.insert(pair.clone(), Rate::new(&sym, venue::GMO, "websocket"));

            last_emit.insert(
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
            last_emit_market_book: last_emit,
            heartbeat_memo: Utc::now(),
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

        // Subscribe to channels with 1.5s delay between each
        for sym in &self.ccy_pairs.clone() {
            for channel in &["orderbooks", "trades"] {
                let mut subscribe_msg = json!({
                    "command": "subscribe",
                    "channel": channel,
                    "symbol": sym,
                });
                if *channel == "trades" {
                    subscribe_msg
                        .as_object_mut()
                        .unwrap()
                        .insert("option".to_string(), json!("TAKER_ONLY"));
                }
                let msg_str = serde_json::to_string(&subscribe_msg).unwrap_or_default();
                write
                    .send(Message::Text(msg_str.into()))
                    .await?;
                info!("Send request for '{}'", subscribe_msg);
                tokio::time::sleep(Duration::from_millis(1500)).await;
            }
        }

        let write = Arc::new(Mutex::new(write));

        // Heartbeat task (60s interval)
        let heartbeat_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                info!("(heartbeat) GMO Crypt WebSocket is alive");
            }
        });

        let result = loop {
            if !self.running {
                break Ok(());
            }

            match read.next().await {
                Some(Ok(Message::Text(text))) => {
                    match serde_json::from_str::<Value>(&text) {
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

        heartbeat_handle.abort();
        info!("Close websocket");
        result
    }

    async fn process_message(&mut self, message: &Value) {
        let symbol = match message.get("symbol").and_then(|v| v.as_str()) {
            Some(s) => s,
            None => {
                warn!("Unknown message type: {}", message);
                return;
            }
        };
        let channel = match message.get("channel").and_then(|v| v.as_str()) {
            Some(c) => c,
            None => {
                warn!("Unknown message type: {}", message);
                return;
            }
        };

        let ccypair = symbol.to_string();

        match channel {
            "trades" => self.handle_trades(&ccypair, message).await,
            "orderbooks" => self.handle_orderbooks(&ccypair, message).await,
            _ => {}
        }
    }

    /// Handle `trades` → MarketTrade
    async fn handle_trades(&mut self, ccypair: &str, message: &Value) {
        let sym = ccypair.replace("_", "");

        let ts_str = message
            .get("timestamp")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let market_ts = datamodel::parse_gmo_timestamp(ts_str);

        let side_str = message
            .get("side")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let side = match side_str {
            "BUY" => 1,
            "SELL" => -1,
            _ => 0,
        };

        let price = message
            .get("price")
            .and_then(|v| v.as_str())
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0);
        let size = message
            .get("size")
            .and_then(|v| v.as_str())
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0);

        let mut data = MarketTrade::new(
            &sym,
            venue::GMO,
            "websocket",
            side,
            price,
            size,
            "",
            market_ts,
        );
        data.category = "websocket".to_string();

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

    /// Handle `orderbooks` → MarketBook (full snapshot) + Rate
    async fn handle_orderbooks(&mut self, ccypair: &str, message: &Value) {
        let ts_str = message
            .get("timestamp")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let market_ts = datamodel::parse_gmo_timestamp(ts_str);

        if let Some(book) = self.market_books.get_mut(ccypair) {
            book.timestamp = Utc::now();
            book.market_created_timestamp = market_ts;

            if let Some(bids_arr) = message.get("bids").and_then(|v| v.as_array()) {
                book.set_bids_from_json_str(bids_arr);
            }
            if let Some(asks_arr) = message.get("asks").and_then(|v| v.as_array()) {
                book.set_asks_from_json_str(asks_arr);
            }

            book.universal_id = Uuid::new_v4().simple().to_string();
            book.misc = "whole".to_string();
        }

        // Output MarketBook
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

            // TP with 250ms throttle
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

            // Derive Rate
            let new_rate = book.to_rate();
            let prev_mid = self
                .rates
                .get(ccypair)
                .map(|r| r.mid_price)
                .unwrap_or(f64::NAN);

            if new_rate.mid_price != prev_mid || prev_mid.is_nan() {
                let rate_dict = new_rate.to_dict();
                let rate_json = serde_json::to_string(&rate_dict).unwrap_or_default();

                if let Some(ref publisher) = self.publisher {
                    let pub_clone = Arc::clone(publisher);
                    let rate_dict_clone = rate_dict.clone();
                    tokio::spawn(async move {
                        pub_clone.send(&rate_dict_clone).await;
                    });
                }

                if let Some(logger) = self.tp_loggers.get(ccypair) {
                    logger.info(rate_json);
                }

                self.rates.insert(ccypair.to_string(), new_rate);
            }

            // Heartbeat log (60s)
            if self.heartbeat_memo + TimeDelta::seconds(60) < now {
                self.heartbeat_memo = now;
                let last_update: String = self
                    .market_books
                    .iter()
                    .map(|(_, b)| {
                        format!(
                            "{}\t{}",
                            b.sym,
                            datamodel::format_timestamp(&b.timestamp)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                info!("heartbeat:\n{}", last_update);
            }
        }
    }

    pub fn stop(&mut self) {
        self.running = false;
    }
}
