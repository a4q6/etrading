use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use chrono::{DateTime, Datelike, FixedOffset, NaiveTime, TimeZone, Utc};
use serde_json::{json, Value};
use tracing::{error, info};

use etr_common::datamodel::{format_timestamp, venue};
use etr_common::tp_logger::TpLogger;

const ENDPOINT: &str = "https://api.bitflyer.com/v1/getfundingrate";
const HANDLER_CLASS: &str = "BitFlyerFundingRate";

/// BitFlyer FundingRate REST poller — direct translation of Python BitFlyerFundingRate.
///
/// Polls funding rate at JST 6:00, 14:00, 22:00 (update schedule).
/// Produces `_data_type: "FundingRate"` messages.
pub struct BitFlyerFundingRate {
    ccy_pairs: Vec<String>,
    tp_loggers: HashMap<String, TpLogger>,
    latest_data: HashMap<String, Option<String>>, // latest next_funding_rate_settledate per pair
    running: bool,
}

impl BitFlyerFundingRate {
    pub fn new(ccy_pairs: Vec<String>, rtp_dir: &Path) -> Self {
        let mut tp_loggers = HashMap::new();
        let mut latest_data = HashMap::new();

        for pair in &ccy_pairs {
            let sym = pair.replace("_", "").to_uppercase();
            let logger_name = format!("TP-{}-{}", HANDLER_CLASS, sym);
            tp_loggers.insert(pair.clone(), TpLogger::new(&logger_name, rtp_dir));
            latest_data.insert(pair.clone(), None);
        }

        Self {
            ccy_pairs,
            tp_loggers,
            latest_data,
            running: true,
        }
    }

    /// Calculate the next update time (JST 6:00, 14:00, or 22:00).
    fn get_next_update_time(now: DateTime<Utc>) -> DateTime<Utc> {
        let jst = FixedOffset::east_opt(9 * 3600).expect("valid offset");
        let now_jst = now.with_timezone(&jst);

        let update_hours = [6, 14, 22];
        for &hour in &update_hours {
            let next_time = jst
                .with_ymd_and_hms(
                    now_jst.year(),
                    now_jst.month(),
                    now_jst.day(),
                    hour,
                    0,
                    0,
                )
                .unwrap();
            if now_jst < next_time {
                return next_time.with_timezone(&Utc);
            }
        }

        // Next day 6am JST
        let tomorrow = now_jst.date_naive() + chrono::Duration::days(1);
        let next_time = jst
            .from_local_datetime(
                &tomorrow.and_time(NaiveTime::from_hms_opt(6, 0, 0).unwrap()),
            )
            .unwrap();
        next_time.with_timezone(&Utc)
    }

    /// Fetch funding rate for a single currency pair.
    async fn get_funding_rate(&self, ccy_pair: &str) -> Option<Value> {
        let client = reqwest::Client::new();
        info!("Send request funding rate of '{}'", ccy_pair);

        match client
            .get(ENDPOINT)
            .query(&[("product_code", ccy_pair)])
            .send()
            .await
        {
            Ok(resp) => {
                if resp.status().is_success() {
                    match resp.json::<Value>().await {
                        Ok(mut data) => {
                            let sym = ccy_pair.replace("_", "");
                            let now = Utc::now();
                            data["timestamp"] = json!(format_timestamp(&now));
                            data["sym"] = json!(sym);
                            data["venue"] = json!(venue::BITFLYER);
                            data["category"] = json!("REST");
                            data["_data_type"] = json!("FundingRate");
                            Some(data)
                        }
                        Err(e) => {
                            error!("Failed to parse funding rate response: {}", e);
                            None
                        }
                    }
                } else {
                    info!("Error: HTTP {}", resp.status());
                    None
                }
            }
            Err(e) => {
                error!(
                    "Exception occurred while fetching funding rate: {}",
                    e
                );
                None
            }
        }
    }

    /// Main entry point — polls funding rate on schedule.
    pub async fn start(&mut self) {
        while self.running {
            // Try 1min loop until all funding rates updated
            let mut updated_flag: HashMap<String, bool> = self
                .ccy_pairs
                .iter()
                .map(|p| (p.clone(), false))
                .collect();

            loop {
                for pair in &self.ccy_pairs.clone() {
                    if updated_flag.get(pair) == Some(&true) {
                        continue;
                    }

                    if let Some(data) = self.get_funding_rate(pair).await {
                        let new_settle = data
                            .get("next_funding_rate_settledate")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());

                        let prev_settle = self.latest_data.get(pair).cloned().flatten();

                        if new_settle != prev_settle {
                            updated_flag.insert(pair.clone(), true);
                            self.latest_data.insert(pair.clone(), new_settle);

                            let json_str =
                                serde_json::to_string(&data).unwrap_or_default();
                            if let Some(logger) = self.tp_loggers.get(pair) {
                                logger.info(json_str);
                            }
                        }
                    }
                }

                if updated_flag.values().all(|v| *v) {
                    break;
                } else {
                    info!(
                        "Failed to retrieve new data, wait 1min to retry: {:?}",
                        updated_flag
                    );
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
            }

            let now = Utc::now();
            let next_time = Self::get_next_update_time(now);
            let wait_sec = (next_time - now).num_seconds() + 5;
            info!(
                "Next check scheduled at {} (in {} seconds)",
                next_time.format("%Y-%m-%d %H:%M"),
                wait_sec
            );
            tokio::time::sleep(Duration::from_secs(wait_sec as u64)).await;
        }
    }

    pub fn stop(&mut self) {
        self.running = false;
    }
}
