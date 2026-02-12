use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

use etr_common::datamodel::{self, format_timestamp, venue, MarketTrade};
use etr_common::tp_logger::TpLogger;
use etr_publisher::LocalWsPublisher;

const WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
const REST_URL: &str = "https://api.hyperliquid.xyz/info";
const HANDLER_CLASS: &str = "HyperliquidSocketClient";

/// HyperLiquid WebSocket feed handler — direct translation of Python HyperliquidSocketClient.
///
/// Handles:
/// - `trades` → MarketTrade
/// - `candle` (1m) → Candle (OHLC with delayed emission)
///
/// Features:
/// - Dynamic symbol mapping via REST API
/// - Daily symbol refresh at UTC 00:00
/// - OHLC cache with write loop
pub struct HyperliquidSocketClient {
    ccy_pairs: Vec<String>,
    reconnect_attempts: Option<u32>,
    publisher: Option<Arc<LocalWsPublisher>>,
    limit_candle_symbols: bool,
    rtp_dir: std::path::PathBuf,

    // Populated after initialize_attributes()
    tp_loggers: HashMap<String, TpLogger>,
    /// exchange symbol → ccypair name (e.g. "@1" → "HFUNUSDC")
    symbol_to_pair: HashMap<String, String>,
    /// ccypair → category ("perp" or "spot")
    pair_to_category: HashMap<String, String>,
    channels: Vec<Value>,
    running: bool,
}

#[derive(Clone)]
struct OhlcEntry {
    data: Value,
    emitted: bool,
}

impl HyperliquidSocketClient {
    pub fn new(
        ccy_pairs: Vec<String>,
        reconnect_attempts: Option<u32>,
        publisher: Option<Arc<LocalWsPublisher>>,
        limit_candle_symbols: bool,
        rtp_dir: &Path,
    ) -> Self {
        Self {
            ccy_pairs,
            reconnect_attempts,
            publisher,
            limit_candle_symbols,
            rtp_dir: rtp_dir.to_path_buf(),
            tp_loggers: HashMap::new(),
            symbol_to_pair: HashMap::new(),
            pair_to_category: HashMap::new(),
            channels: Vec::new(),
            running: true,
        }
    }

    /// Fetch symbol mapping from REST API.
    /// Returns: Vec<(exchange_name, pair_name, category, is_delisted)>
    async fn fetch_symbols(&self) -> Vec<(String, String, String, bool)> {
        let client = reqwest::Client::new();
        info!("Try fetching symbol list");

        let perp_resp = client
            .post(REST_URL)
            .json(&json!({"type": "metaAndAssetCtxs"}))
            .header("Content-Type", "application/json")
            .send()
            .await;

        let spot_resp = client
            .post(REST_URL)
            .json(&json!({"type": "spotMeta"}))
            .header("Content-Type", "application/json")
            .send()
            .await;

        let perp_data = match perp_resp {
            Ok(r) => r.json::<Value>().await.unwrap_or(Value::Null),
            Err(e) => {
                error!("Failed to fetch perp meta: {}", e);
                return vec![];
            }
        };

        let spot_data = match spot_resp {
            Ok(r) => r.json::<Value>().await.unwrap_or(Value::Null),
            Err(e) => {
                error!("Failed to fetch spot meta: {}", e);
                return vec![];
            }
        };

        let mut result = Vec::new();

        // Parse perp universe
        if let Some(universe) = perp_data
            .get(0)
            .and_then(|v| v.get("universe"))
            .and_then(|v| v.as_array())
        {
            for item in universe {
                let name = item.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let is_delisted = item
                    .get("isDelisted")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                result.push((
                    name.to_string(),
                    name.to_string(),
                    "perp".to_string(),
                    is_delisted,
                ));
            }
        }

        // Parse spot universe — need token index to build pair names
        let tokens = spot_data.get("tokens").and_then(|v| v.as_array());
        if let (Some(universe), Some(tokens)) = (
            spot_data.get("universe").and_then(|v| v.as_array()),
            tokens,
        ) {
            let token_names: HashMap<i64, String> = tokens
                .iter()
                .filter_map(|t| {
                    let idx = t.get("index").and_then(|v| v.as_i64())?;
                    let name = t.get("name").and_then(|v| v.as_str())?;
                    Some((idx, name.to_string()))
                })
                .collect();

            for item in universe {
                let name = item.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let token_indices = item.get("tokens").and_then(|v| v.as_array());
                if let Some(indices) = token_indices {
                    let token0 = indices
                        .first()
                        .and_then(|v| v.as_i64())
                        .and_then(|i| token_names.get(&i))
                        .cloned()
                        .unwrap_or_default();
                    let token1 = indices
                        .get(1)
                        .and_then(|v| v.as_i64())
                        .and_then(|i| token_names.get(&i))
                        .cloned()
                        .unwrap_or_default();
                    let pair_name = format!("{}{}", token0, token1);
                    result.push((
                        name.to_string(),
                        pair_name,
                        "spot".to_string(),
                        false,
                    ));
                }
            }
        }

        if !result.is_empty() {
            info!(
                "Successfully retrieved symbol list ({} symbols)",
                result.len()
            );
        }

        result
    }

    /// Initialize attributes from fetched symbol data.
    async fn initialize_attributes(&mut self) {
        let symbols = self.fetch_symbols().await;
        if symbols.is_empty() {
            error!("Failed to fetch symbols, cannot initialize");
            return;
        }

        self.symbol_to_pair.clear();
        self.pair_to_category.clear();
        self.channels.clear();
        self.tp_loggers.clear();

        for (name, pair_name, category, is_delisted) in &symbols {
            if !is_delisted {
                self.symbol_to_pair
                    .insert(name.clone(), pair_name.clone());
            }
            self.pair_to_category
                .insert(pair_name.clone(), category.clone());
        }

        for (symbol, ccy_pair) in &self.symbol_to_pair.clone() {
            // Candles: all symbols or only ccy_pairs based on limit_candle_symbols
            if !self.limit_candle_symbols || self.ccy_pairs.contains(ccy_pair) {
                self.channels.push(json!({
                    "type": "candle",
                    "coin": symbol,
                    "interval": "1m"
                }));
            }

            // MarketTrades: only for requested ccy_pairs
            if self.ccy_pairs.contains(ccy_pair) {
                self.channels.push(json!({
                    "type": "trades",
                    "coin": symbol
                }));
            }

            // TP logger
            if !self.tp_loggers.contains_key(ccy_pair.as_str()) {
                let sym = ccy_pair.replace("_", "").to_uppercase();
                let logger_name = format!("TP-{}-{}", HANDLER_CLASS, sym);
                self.tp_loggers
                    .insert(ccy_pair.clone(), TpLogger::new(&logger_name, &self.rtp_dir));
            }
        }

        info!(
            "Initialized: {} channels, {} TP loggers",
            self.channels.len(),
            self.tp_loggers.len()
        );
    }

    /// Background loop: refresh symbol list daily at UTC 00:00.
    async fn symbol_refresh_loop(
        symbol_to_pair: Arc<Mutex<HashMap<String, String>>>,
        pair_to_category: Arc<Mutex<HashMap<String, String>>>,
        running: Arc<Mutex<bool>>,
    ) {
        let client = reqwest::Client::new();
        loop {
            {
                let r = running.lock().await;
                if !*r {
                    break;
                }
            }

            // Wait until next UTC 00:00
            let now = Utc::now();
            let tomorrow = (now + chrono::Duration::days(1))
                .date_naive()
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc();
            let wait = (tomorrow - now).num_seconds().max(1) as u64;
            info!(
                "Symbol refresh: next at {} (in {} seconds)",
                tomorrow.format("%Y-%m-%d %H:%M"),
                wait
            );
            tokio::time::sleep(Duration::from_secs(wait)).await;

            // Fetch and update
            info!("Refreshing symbol list...");
            let perp_resp = client
                .post(REST_URL)
                .json(&json!({"type": "metaAndAssetCtxs"}))
                .header("Content-Type", "application/json")
                .send()
                .await;

            if let Ok(resp) = perp_resp {
                if let Ok(perp_data) = resp.json::<Value>().await {
                    if let Some(universe) = perp_data
                        .get(0)
                        .and_then(|v| v.get("universe"))
                        .and_then(|v| v.as_array())
                    {
                        let mut s2p = symbol_to_pair.lock().await;
                        let mut p2c = pair_to_category.lock().await;
                        for item in universe {
                            let name =
                                item.get("name").and_then(|v| v.as_str()).unwrap_or("");
                            let is_delisted = item
                                .get("isDelisted")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);
                            if !is_delisted {
                                s2p.insert(name.to_string(), name.to_string());
                                p2c.insert(name.to_string(), "perp".to_string());
                            }
                        }
                        info!("Symbol list refreshed ({} symbols)", s2p.len());
                    }
                }
            }
        }
    }

    pub async fn start(&mut self) {
        self.initialize_attributes().await;

        // Shared state for background loops
        let ohlc_cache: Arc<Mutex<HashMap<String, OhlcEntry>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let symbol_to_pair = Arc::new(Mutex::new(self.symbol_to_pair.clone()));
        let pair_to_category = Arc::new(Mutex::new(self.pair_to_category.clone()));
        let running = Arc::new(Mutex::new(true));

        // Start symbol refresh loop
        let s2p_clone = Arc::clone(&symbol_to_pair);
        let p2c_clone = Arc::clone(&pair_to_category);
        let running_clone = Arc::clone(&running);
        tokio::spawn(async move {
            Self::symbol_refresh_loop(s2p_clone, p2c_clone, running_clone).await;
        });

        // Start OHLC write loop (check every 60s for un-emitted closed candles)
        let ohlc_for_loop = Arc::clone(&ohlc_cache);
        let pub_for_loop = self.publisher.clone();
        let running_for_loop = Arc::clone(&running);
        tokio::spawn(async move {
            loop {
                {
                    let r = running_for_loop.lock().await;
                    if !*r {
                        break;
                    }
                }

                let now_str = format_timestamp(&Utc::now());
                let mut cache = ohlc_for_loop.lock().await;

                for (ccypair, entry) in cache.iter_mut() {
                    let close_time = entry
                        .data
                        .get("close_time")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");

                    if close_time < now_str.as_str() && !entry.emitted {
                        entry.emitted = true;
                        let mut data = entry.data.clone();
                        data["timestamp"] = json!(now_str);

                        if let Some(ref pub_ref) = pub_for_loop {
                            let pub_clone = Arc::clone(pub_ref);
                            let data_clone = data.clone();
                            tokio::spawn(async move {
                                pub_clone.send(&data_clone).await;
                            });
                        }

                        info!("OHLC write loop emitted candle for {}", ccypair);
                    }
                }

                drop(cache);

                // Sleep until next minute boundary
                let now = Utc::now();
                let secs_to_next = 60 - (now.timestamp() % 60);
                tokio::time::sleep(Duration::from_secs(secs_to_next as u64)).await;
            }
        });

        let mut attempts: u32 = 0;

        while self.running {
            if let Some(max) = self.reconnect_attempts {
                if attempts >= max {
                    error!("Reached max connection attempts, stop listening.");
                    break;
                }
            }

            // Sync shared symbol maps back
            {
                let s2p = symbol_to_pair.lock().await;
                self.symbol_to_pair = s2p.clone();
                let p2c = pair_to_category.lock().await;
                self.pair_to_category = p2c.clone();
            }

            match self.connect_ws(&ohlc_cache).await {
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

        // Signal background loops to stop
        let mut r = running.lock().await;
        *r = false;
    }

    async fn connect_ws(
        &mut self,
        ohlc_cache: &Arc<Mutex<HashMap<String, OhlcEntry>>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (ws_stream, _) = tokio_tungstenite::connect_async(WS_URL).await?;
        info!("Start subscribing: {}", WS_URL);

        let (write, mut read) = ws_stream.split();

        let write = Arc::new(Mutex::new(write));

        // Heartbeat task
        let heartbeat_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                info!("(heartbeat) Hyperliquid WebSocket is alive");
            }
        });

        // Send subscribe messages
        {
            let mut w = write.lock().await;
            for channel in &self.channels {
                let subscribe_msg = json!({
                    "method": "subscribe",
                    "subscription": channel
                });
                info!("Send request of {}", subscribe_msg);
                w.send(Message::Text(subscribe_msg.to_string().into()))
                    .await?;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        // Message read loop
        let result = loop {
            if !self.running {
                break Ok(());
            }

            match read.next().await {
                Some(Ok(Message::Text(text))) => {
                    match serde_json::from_str::<Value>(&text) {
                        Ok(message) => {
                            if message.get("channel").is_none() {
                                info!("{}", message);
                            } else {
                                self.process_message(&message, ohlc_cache).await;
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

    async fn process_message(
        &mut self,
        message: &Value,
        ohlc_cache: &Arc<Mutex<HashMap<String, OhlcEntry>>>,
    ) {
        let channel = match message.get("channel").and_then(|v| v.as_str()) {
            Some(c) => c,
            None => return,
        };

        match channel {
            "trades" => self.handle_trades(message).await,
            "candle" => self.handle_candle(message, ohlc_cache).await,
            "subscriptionResponse" => {
                info!("{}", message);
            }
            _ => {
                info!("Unknown channel message: {}", message);
            }
        }
    }

    /// Handle `trades` channel → MarketTrade
    async fn handle_trades(&mut self, message: &Value) {
        let data_arr = match message.get("data").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return,
        };

        for one_trade in data_arr {
            let symbol = one_trade
                .get("coin")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            let ccypair = match self.symbol_to_pair.get(symbol) {
                Some(p) => p.clone(),
                None => continue,
            };

            let category = self
                .pair_to_category
                .get(&ccypair)
                .cloned()
                .unwrap_or_default();

            // Parse time (millisecond epoch)
            let time_ms = one_trade
                .get("time")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let market_ts = datamodel::from_millis(time_ms);

            // Side: "B" → 1, "A" → -1
            let side_str = one_trade
                .get("side")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let side = match side_str {
                "B" => 1,
                "A" => -1,
                _ => 0,
            };

            let price = one_trade
                .get("px")
                .and_then(|v| v.as_str())
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or(0.0);
            let amount = one_trade
                .get("sz")
                .and_then(|v| v.as_str())
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or(0.0);
            let trade_id = one_trade
                .get("tid")
                .map(|v| v.to_string().trim_matches('"').to_string())
                .unwrap_or_default();

            // users array → order_ids
            let users = one_trade
                .get("users")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str())
                        .collect::<Vec<_>>()
                        .join("_")
                })
                .unwrap_or_default();

            let hash = one_trade
                .get("hash")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let mut data = MarketTrade::new(
                &ccypair,
                venue::HYPERLIQUID,
                &category,
                side,
                price,
                amount,
                &trade_id,
                market_ts,
            );
            data.order_ids = Some(users);
            data.misc = Some(hash);

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
            if let Some(logger) = self.tp_loggers.get(&ccypair) {
                logger.info(json_str);
            }
        }
    }

    /// Handle `candle` channel → Candle (OHLC with delayed emission)
    async fn handle_candle(
        &mut self,
        message: &Value,
        ohlc_cache: &Arc<Mutex<HashMap<String, OhlcEntry>>>,
    ) {
        let data = match message.get("data") {
            Some(d) => d,
            None => return,
        };

        let symbol = data.get("s").and_then(|v| v.as_str()).unwrap_or("");
        let ccypair = match self.symbol_to_pair.get(symbol) {
            Some(p) => p.clone(),
            None => return,
        };

        let category = self
            .pair_to_category
            .get(&ccypair)
            .cloned()
            .unwrap_or_default();

        // Parse candle fields
        let open_time_ms = data.get("t").and_then(|v| v.as_i64()).unwrap_or(0);
        let close_time_ms = data.get("T").and_then(|v| v.as_i64()).unwrap_or(0);
        let open_time = datamodel::from_millis(open_time_ms);
        let close_time = datamodel::from_millis(close_time_ms + 1);

        let cur_msg = json!({
            "timestamp": format_timestamp(&Utc::now()),
            "open_time": format_timestamp(&open_time),
            "close_time": format_timestamp(&close_time),
            "sym": ccypair,
            "venue": venue::HYPERLIQUID,
            "category": category,
            "open": data.get("o").and_then(|v| v.as_str()).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0),
            "high": data.get("h").and_then(|v| v.as_str()).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0),
            "low": data.get("l").and_then(|v| v.as_str()).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0),
            "close": data.get("c").and_then(|v| v.as_str()).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0),
            "volume": data.get("v").and_then(|v| v.as_str()).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0),
            "counts": data.get("n").and_then(|v| v.as_i64()).unwrap_or(0),
            "_data_type": "Candle",
        });

        let cur_open_time = cur_msg["open_time"].as_str().unwrap_or("").to_string();

        let mut cache = ohlc_cache.lock().await;

        let should_emit_prev = cache
            .get(&ccypair)
            .map(|prev| {
                let prev_open = prev
                    .data
                    .get("open_time")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                prev_open < cur_open_time.as_str() && !prev.emitted
            })
            .unwrap_or(false);

        let is_new_period = cache
            .get(&ccypair)
            .map(|prev| {
                let prev_open = prev
                    .data
                    .get("open_time")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                prev_open < cur_open_time.as_str()
            })
            .unwrap_or(true);

        if should_emit_prev {
            let prev_entry = cache.get(&ccypair).unwrap().clone();
            let mut prev_data = prev_entry.data.clone();
            prev_data["timestamp"] = json!(format_timestamp(&Utc::now()));

            // Publish
            if let Some(ref publisher) = self.publisher {
                let pub_clone = Arc::clone(publisher);
                let data_clone = prev_data.clone();
                tokio::spawn(async move {
                    pub_clone.send(&data_clone).await;
                });
            }

            // TP log
            if let Some(logger) = self.tp_loggers.get(&ccypair) {
                logger.info(serde_json::to_string(&prev_data).unwrap_or_default());
            }
        }

        if is_new_period {
            cache.insert(
                ccypair,
                OhlcEntry {
                    data: cur_msg,
                    emitted: false,
                },
            );
        } else {
            // Same candle period — update in place
            cache.insert(
                ccypair,
                OhlcEntry {
                    data: cur_msg,
                    emitted: false,
                },
            );
        }
    }

    pub fn stop(&mut self) {
        self.running = false;
    }
}
