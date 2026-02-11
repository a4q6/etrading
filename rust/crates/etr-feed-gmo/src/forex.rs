use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Datelike, TimeDelta, Timelike, Utc};
use chrono_tz::US::Eastern;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

use etr_common::datamodel::{self, venue, Rate};
use etr_common::tp_logger::TpLogger;
use etr_publisher::LocalWsPublisher;

const WS_URL: &str = "wss://forex-api.coin.z.com/ws/public/v1";
const HANDLER_CLASS: &str = "GmoForexSocketClient";

/// GMO Forex WebSocket feed handler — plain JSON protocol.
///
/// Handles:
/// - `ticker` → Rate (bid/ask direct)
///
/// No MarketBook — Rate only. Includes weekend sleep_offset logic.
pub struct GmoForexSocketClient {
    #[allow(dead_code)]
    ccy_pairs: Vec<String>,
    reconnect_attempts: Option<u32>,
    publisher: Option<Arc<LocalWsPublisher>>,
    tp_loggers: HashMap<String, TpLogger>,
    rates: HashMap<String, Rate>,
    heartbeat_memo: DateTime<Utc>,
    running: bool,
}

impl GmoForexSocketClient {
    pub fn new(
        ccy_pairs: Vec<String>,
        reconnect_attempts: Option<u32>,
        publisher: Option<Arc<LocalWsPublisher>>,
        rtp_dir: &Path,
    ) -> Self {
        let mut tp_loggers = HashMap::new();
        let mut rates = HashMap::new();

        for pair in &ccy_pairs {
            let sym = pair.replace("_", "").to_uppercase();

            let logger_name = format!("TP-{}-{}", HANDLER_CLASS, sym);
            tp_loggers.insert(pair.clone(), TpLogger::new(&logger_name, rtp_dir));

            rates.insert(pair.clone(), Rate::new(&sym, venue::GMO, "websocket"));
        }

        Self {
            ccy_pairs,
            reconnect_attempts,
            publisher,
            tp_loggers,
            rates,
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
                    let base_sleep = 10.0 * (attempts as f64).ln().max(1.0);
                    let weekend_offset = Self::sleep_offset();
                    let sleep_sec = base_sleep + weekend_offset;
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

        // Subscribe to ticker channels with 2s delay
        for sym in &self.ccy_pairs.clone() {
            let subscribe_msg = json!({
                "command": "subscribe",
                "channel": "ticker",
                "symbol": sym,
            });
            let msg_str = serde_json::to_string(&subscribe_msg).unwrap_or_default();
            write
                .send(Message::Text(msg_str.into()))
                .await?;
            info!("Send request for '{}'", subscribe_msg);
            tokio::time::sleep(Duration::from_millis(2000)).await;
        }

        let write = Arc::new(Mutex::new(write));

        // Heartbeat task
        let heartbeat_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                info!("(heartbeat) GMO Forex WebSocket is alive");
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
        // Check for required fields: symbol, bid, ask, timestamp, status
        let symbol = match message.get("symbol").and_then(|v| v.as_str()) {
            Some(s) => s,
            None => return,
        };
        let bid_str = match message.get("bid").and_then(|v| v.as_str()) {
            Some(b) => b,
            None => return,
        };
        let ask_str = match message.get("ask").and_then(|v| v.as_str()) {
            Some(a) => a,
            None => return,
        };
        let ts_str = match message.get("timestamp").and_then(|v| v.as_str()) {
            Some(t) => t,
            None => return,
        };
        let status = match message.get("status").and_then(|v| v.as_str()) {
            Some(s) => s,
            None => return,
        };

        let ccypair = symbol.to_string();
        let sym = ccypair.replace("_", "");
        let best_bid = bid_str.parse::<f64>().unwrap_or(0.0);
        let best_ask = ask_str.parse::<f64>().unwrap_or(0.0);
        let mid_price = ((best_bid + best_ask) / 2.0 * 10_000_000.0).round() / 10_000_000.0;
        let market_ts = datamodel::parse_gmo_timestamp(ts_str);

        let now = Utc::now();
        let mut data = Rate::new(&sym, venue::GMO, "websocket");
        data.timestamp = now;
        data.market_created_timestamp = market_ts;
        data.best_bid = best_bid;
        data.best_ask = best_ask;
        data.mid_price = mid_price;
        data.misc = Some(status.to_string());

        let dict = data.to_dict();
        let json_str = serde_json::to_string(&dict).unwrap_or_default();

        // Always publish
        if let Some(ref publisher) = self.publisher {
            let pub_clone = Arc::clone(publisher);
            let dict_clone = dict.clone();
            tokio::spawn(async move {
                pub_clone.send(&dict_clone).await;
            });
        }

        // TP with 250ms throttle
        let last_ts = self
            .rates
            .get(&ccypair)
            .map(|r| r.timestamp)
            .unwrap_or(DateTime::from_timestamp(0, 0).unwrap_or_else(Utc::now));

        if last_ts + TimeDelta::milliseconds(250) < now {
            self.rates.insert(ccypair.clone(), data);
            if let Some(logger) = self.tp_loggers.get(&ccypair) {
                logger.info(json_str);
            }
        }

        // Heartbeat log (60s)
        if self.heartbeat_memo + TimeDelta::seconds(60) < now {
            self.heartbeat_memo = now;
            let last_update: String = self
                .rates
                .iter()
                .map(|(_, r)| {
                    format!(
                        "{}\t{}",
                        r.sym,
                        datamodel::format_timestamp(&r.timestamp)
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            info!("heartbeat:\n{}", last_update);
        }
    }

    /// Calculate weekend sleep offset.
    /// NY time: Friday 18:00 to Sunday 18:00 (EST) → return seconds until Sunday 18:00.
    pub fn sleep_offset() -> f64 {
        let now = Utc::now().with_timezone(&Eastern);
        let weekday = now.weekday().num_days_from_monday(); // Mon=0, Sun=6

        let is_weekend = (weekday == 4 && now.hour() >= 18) // Friday 18:00+
            || weekday == 5 // Saturday
            || (weekday == 6 && now.hour() < 18); // Sunday before 18:00

        if is_weekend {
            // Calculate seconds until next Sunday 18:00 EST
            let days_until_sunday = match weekday {
                4 => 2, // Friday → Sunday
                5 => 1, // Saturday → Sunday
                6 => 0, // Sunday (already)
                _ => 0,
            };

            let target = now
                .date_naive()
                .checked_add_days(chrono::Days::new(days_until_sunday))
                .unwrap()
                .and_hms_opt(18, 0, 0)
                .unwrap()
                .and_local_timezone(Eastern)
                .unwrap();

            let duration = target.signed_duration_since(now);
            duration.num_seconds().max(0) as f64
        } else {
            0.0
        }
    }

    pub fn stop(&mut self) {
        self.running = false;
    }
}
