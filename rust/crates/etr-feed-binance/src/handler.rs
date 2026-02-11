use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use futures_util::StreamExt;
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

use etr_common::datamodel::{self, venue, MarketTrade, Rate};
use etr_common::tp_logger::TpLogger;
use etr_publisher::LocalWsPublisher;

const WS_BASE: &str = "wss://stream.binance.com:9443/stream?streams=";
const HANDLER_CLASS: &str = "BinanceSocketClient";

/// Binance WebSocket feed handler — combined streams via URL params.
///
/// Handles:
/// - `aggTrade` → MarketTrade (m=true → SELL, m=false → BUY)
/// - `bookTicker` → Rate (best bid/ask direct)
///
/// No MarketBook, no throttling. Simple and efficient.
pub struct BinanceSocketClient {
    #[allow(dead_code)]
    ccy_pairs: Vec<String>,
    reconnect_attempts: Option<u32>,
    publisher: Option<Arc<LocalWsPublisher>>,
    tp_loggers: HashMap<String, TpLogger>,
    rates: HashMap<String, Rate>,
    channels: Vec<String>,
    running: bool,
}

impl BinanceSocketClient {
    pub fn new(
        ccy_pairs: Vec<String>,
        reconnect_attempts: Option<u32>,
        publisher: Option<Arc<LocalWsPublisher>>,
        rtp_dir: &Path,
    ) -> Self {
        let mut tp_loggers = HashMap::new();
        let mut rates = HashMap::new();
        let mut channels = Vec::new();

        for pair in &ccy_pairs {
            let sym = pair.replace("_", "").to_uppercase();

            let logger_name = format!("TP-{}-{}", HANDLER_CLASS, sym);
            tp_loggers.insert(sym.clone(), TpLogger::new(&logger_name, rtp_dir));

            let mut rate = Rate::new(&sym, venue::BINANCE, "spot");
            rate.misc = Some("null".to_string());
            rates.insert(sym.clone(), rate);

            let lower = pair.to_lowercase();
            channels.push(format!("{}@aggTrade", lower));
            channels.push(format!("{}@bookTicker", lower));
        }

        Self {
            ccy_pairs,
            reconnect_attempts,
            publisher,
            tp_loggers,
            rates,
            channels,
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
        let streams = self.channels.join("/");
        let url = format!("{}{}", WS_BASE, streams);
        info!("Connecting to {}", url);

        let (ws_stream, _) = tokio_tungstenite::connect_async(&url).await?;
        info!("Start subscribing: {}", url);

        let (_write, mut read) = ws_stream.split();

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
                Some(Ok(Message::Ping(_data))) => {
                    // Binance sends pings, tungstenite auto-responds
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

    async fn process_message(&mut self, message: &Value) {
        let stream = match message.get("stream").and_then(|v| v.as_str()) {
            Some(s) => s,
            None => return,
        };

        if stream.contains("bookTicker") {
            self.handle_book_ticker(message).await;
        } else if stream.contains("aggTrade") {
            self.handle_agg_trade(message).await;
        }
    }

    /// Handle `bookTicker` → Rate
    async fn handle_book_ticker(&mut self, message: &Value) {
        let data = match message.get("data") {
            Some(d) => d,
            None => return,
        };

        let sym = data
            .get("s")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_uppercase()
            .replace("_", "");

        let best_bid = data
            .get("b")
            .and_then(|v| v.as_str())
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0);
        let best_ask = data
            .get("a")
            .and_then(|v| v.as_str())
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0);

        let update_id = data
            .get("u")
            .map(|v| v.to_string())
            .unwrap_or_default();

        // Only emit if changed or first time
        let should_emit = if let Some(rate) = self.rates.get(&sym) {
            best_ask != rate.best_ask
                || best_bid != rate.best_bid
                || rate.misc.as_deref() == Some("null")
        } else {
            true
        };

        if should_emit {
            if let Some(rate) = self.rates.get_mut(&sym) {
                rate.timestamp = Utc::now();
                rate.universal_id = update_id;
                rate.best_bid = best_bid;
                rate.best_ask = best_ask;
                rate.mid_price = (best_bid + best_ask) / 2.0;
                rate.misc = Some("spot".to_string());
            }

            if let Some(rate) = self.rates.get(&sym) {
                let dict = rate.to_dict();
                let json_str = serde_json::to_string(&dict).unwrap_or_default();

                if let Some(ref publisher) = self.publisher {
                    let pub_clone = Arc::clone(publisher);
                    let dict_clone = dict.clone();
                    tokio::spawn(async move {
                        pub_clone.send(&dict_clone).await;
                    });
                }

                if let Some(logger) = self.tp_loggers.get(&sym) {
                    logger.info(json_str);
                }
            }
        }
    }

    /// Handle `aggTrade` → MarketTrade
    async fn handle_agg_trade(&mut self, message: &Value) {
        let data = match message.get("data") {
            Some(d) => d,
            None => return,
        };

        let sym = data
            .get("s")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_uppercase()
            .replace("_", "");

        let trade_time = data
            .get("T")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let market_ts = datamodel::from_millis(trade_time);

        // m=true → buyer is market maker → taker is seller → side=-1
        let is_buyer_maker = data
            .get("m")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let side = if is_buyer_maker { -1 } else { 1 };

        let price = data
            .get("p")
            .and_then(|v| v.as_str())
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0);
        let amount = data
            .get("q")
            .and_then(|v| v.as_str())
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0);
        let trade_id = data
            .get("a")
            .map(|v| v.to_string())
            .unwrap_or_default();

        let first_id = data
            .get("f")
            .map(|v| v.to_string())
            .unwrap_or_default();
        let last_id = data
            .get("l")
            .map(|v| v.to_string())
            .unwrap_or_default();

        let mut trade_data = MarketTrade::new(
            &sym,
            venue::BINANCE,
            "spot",
            side,
            price,
            amount,
            &trade_id,
            market_ts,
        );
        trade_data.order_ids = Some(format!("{}_{}", first_id, last_id));
        trade_data.misc = Some("spot".to_string());

        let dict = trade_data.to_dict();
        let json_str = serde_json::to_string(&dict).unwrap_or_default();

        if let Some(ref publisher) = self.publisher {
            let pub_clone = Arc::clone(publisher);
            let dict_clone = dict.clone();
            tokio::spawn(async move {
                pub_clone.send(&dict_clone).await;
            });
        }

        if let Some(logger) = self.tp_loggers.get(&sym) {
            logger.info(json_str);
        }
    }

    pub fn stop(&mut self) {
        self.running = false;
    }
}
