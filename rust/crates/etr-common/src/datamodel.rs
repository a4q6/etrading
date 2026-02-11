use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDateTime, Utc};
use ordered_float::OrderedFloat;
use serde_json::{json, Value};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Venue constants (matching Python VENUE dataclass)
// ---------------------------------------------------------------------------
pub mod venue {
    pub const BITFLYER: &str = "bitflyer";
    pub const BITMEX: &str = "bitmex";
    pub const BINANCE: &str = "binance";
    pub const BITBANK: &str = "bitbank";
    pub const GMO: &str = "gmo";
    pub const HYPERLIQUID: &str = "hyperliquid";
    pub const COINCHECK: &str = "coincheck";
}

// ---------------------------------------------------------------------------
// Helper: format DateTime<Utc> as Python-compatible ISO8601
// Python outputs: 2025-06-15T12:30:45.123456+00:00
// ---------------------------------------------------------------------------
pub fn format_timestamp(dt: &DateTime<Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string()
}

/// Parse BitFlyer exec_date: "2025-06-15T12:30:45.1234567Z"
/// Takes first 23 chars → "2025-06-15T12:30:45.123" and parses.
pub fn parse_exec_date(s: &str) -> DateTime<Utc> {
    let truncated = if s.len() >= 23 { &s[..23] } else { s };
    NaiveDateTime::parse_from_str(truncated, "%Y-%m-%dT%H:%M:%S%.f")
        .map(|ndt| ndt.and_utc())
        .unwrap_or_else(|_| Utc::now())
}

/// Parse GMO/Binance timestamp: "2025-04-30T13:09:43.601Z"
/// Takes first 23 chars → "2025-04-30 13:09:43.601" and parses as UTC.
pub fn parse_gmo_timestamp(s: &str) -> DateTime<Utc> {
    let truncated = if s.len() >= 23 { &s[..23] } else { s };
    let cleaned = truncated.replace('T', " ").replace('Z', "");
    NaiveDateTime::parse_from_str(&cleaned, "%Y-%m-%d %H:%M:%S%.f")
        .map(|ndt| ndt.and_utc())
        .unwrap_or_else(|_| Utc::now())
}

/// Convert millisecond epoch to DateTime<Utc>
pub fn from_millis(millis: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(millis).unwrap_or_else(Utc::now)
}

// ---------------------------------------------------------------------------
// Rate
// ---------------------------------------------------------------------------
#[derive(Debug, Clone)]
pub struct Rate {
    pub timestamp: DateTime<Utc>,
    pub market_created_timestamp: DateTime<Utc>,
    pub sym: String,
    pub venue: String,
    pub category: String,
    pub best_bid: f64,
    pub best_ask: f64,
    pub mid_price: f64,
    pub misc: Option<String>,
    pub universal_id: String,
}

impl Rate {
    pub fn new(sym: &str, venue: &str, category: &str) -> Self {
        Self {
            timestamp: Utc::now(),
            market_created_timestamp: Utc::now(),
            sym: sym.to_string(),
            venue: venue.to_string(),
            category: category.to_string(),
            best_bid: f64::NAN,
            best_ask: f64::NAN,
            mid_price: f64::NAN,
            misc: None,
            universal_id: Uuid::new_v4().simple().to_string(),
        }
    }

    /// Convert to JSON dict matching Python Rate.to_dict()
    pub fn to_dict(&self) -> Value {
        let mid = (self.best_ask + self.best_bid) / 2.0;
        json!({
            "_data_type": "Rate",
            "timestamp": format_timestamp(&self.timestamp),
            "market_created_timestamp": format_timestamp(&self.market_created_timestamp),
            "sym": self.sym,
            "venue": self.venue,
            "category": self.category,
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "mid_price": mid,
            "misc": self.misc.as_deref().unwrap_or("None"),
            "universal_id": self.universal_id,
        })
    }
}

// ---------------------------------------------------------------------------
// MarketTrade
// ---------------------------------------------------------------------------
#[derive(Debug, Clone)]
pub struct MarketTrade {
    pub timestamp: DateTime<Utc>,
    pub market_created_timestamp: DateTime<Utc>,
    pub sym: String,
    pub venue: String,
    pub category: String,
    pub side: i32,
    pub price: f64,
    pub amount: f64,
    pub trade_id: String,
    pub order_ids: Option<String>,
    pub misc: Option<String>,
    pub universal_id: String,
}

impl MarketTrade {
    pub fn new(
        sym: &str,
        venue: &str,
        category: &str,
        side: i32,
        price: f64,
        amount: f64,
        trade_id: &str,
        market_created_timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            timestamp: Utc::now(),
            market_created_timestamp,
            sym: sym.to_string(),
            venue: venue.to_string(),
            category: category.to_string(),
            side,
            price,
            amount,
            trade_id: trade_id.to_string(),
            order_ids: None,
            misc: None,
            universal_id: Uuid::new_v4().simple().to_string(),
        }
    }

    /// Convert to JSON dict matching Python MarketTrade.to_dict()
    pub fn to_dict(&self) -> Value {
        json!({
            "_data_type": "MarketTrade",
            "timestamp": format_timestamp(&self.timestamp),
            "market_created_timestamp": format_timestamp(&self.market_created_timestamp),
            "sym": self.sym,
            "venue": self.venue,
            "category": self.category,
            "side": self.side,
            "price": self.price,
            "amount": self.amount,
            "trade_id": self.trade_id,
            "order_ids": self.order_ids.as_deref().unwrap_or("None"),
            "misc": self.misc.as_deref().unwrap_or("None"),
            "universal_id": self.universal_id,
        })
    }
}

// ---------------------------------------------------------------------------
// MarketBook
// ---------------------------------------------------------------------------
#[derive(Debug, Clone)]
pub struct MarketBook {
    pub timestamp: DateTime<Utc>,
    pub market_created_timestamp: DateTime<Utc>,
    pub sym: String,
    pub venue: String,
    pub category: String,
    pub bids: BTreeMap<OrderedFloat<f64>, f64>,
    pub asks: BTreeMap<OrderedFloat<f64>, f64>,
    pub universal_id: String,
    pub misc: String,
}

impl MarketBook {
    pub fn new(sym: &str, venue: &str, category: &str) -> Self {
        Self {
            timestamp: Utc::now(),
            market_created_timestamp: Utc::now(),
            sym: sym.to_string(),
            venue: venue.to_string(),
            category: category.to_string(),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            universal_id: Uuid::new_v4().simple().to_string(),
            misc: "null".to_string(),
        }
    }

    /// Best bid = highest bid price (last key in BTreeMap)
    pub fn best_bid(&self) -> f64 {
        self.bids
            .keys()
            .next_back()
            .map(|k| k.into_inner())
            .unwrap_or(f64::NAN)
    }

    /// Best ask = lowest ask price (first key in BTreeMap)
    pub fn best_ask(&self) -> f64 {
        self.asks
            .keys()
            .next()
            .map(|k| k.into_inner())
            .unwrap_or(f64::NAN)
    }

    pub fn mid_price(&self) -> f64 {
        (self.best_ask() + self.best_bid()) / 2.0
    }

    /// Convert to JSON dict matching Python MarketBook.to_dict(level=20)
    pub fn to_dict(&self) -> Value {
        self.to_dict_level(20)
    }

    pub fn to_dict_level(&self, level: usize) -> Value {
        // bids: top `level` entries in descending order (highest prices first)
        // Python: list(data["bids"].items())[-level:][::-1]
        let bids_vec: Vec<Value> = self
            .bids
            .iter()
            .rev()
            .take(level)
            .map(|(p, s)| json!([p.into_inner(), *s]))
            .collect();

        // asks: top `level` entries in ascending order (lowest prices first)
        // Python: list(data["asks"].items())[:level]
        let asks_vec: Vec<Value> = self
            .asks
            .iter()
            .take(level)
            .map(|(p, s)| json!([p.into_inner(), *s]))
            .collect();

        json!({
            "_data_type": "MarketBook",
            "timestamp": format_timestamp(&self.timestamp),
            "market_created_timestamp": format_timestamp(&self.market_created_timestamp),
            "sym": self.sym,
            "venue": self.venue,
            "category": self.category,
            "bids": bids_vec,
            "asks": asks_vec,
            "universal_id": self.universal_id,
            "misc": self.misc,
        })
    }

    /// Convert MarketBook → Rate (matching Python MarketBook.to_rate())
    pub fn to_rate(&self) -> Rate {
        Rate {
            timestamp: self.timestamp,
            market_created_timestamp: self.market_created_timestamp,
            sym: self.sym.clone(),
            venue: self.venue.clone(),
            category: self.category.clone(),
            best_bid: self.best_bid(),
            best_ask: self.best_ask(),
            mid_price: self.mid_price(),
            misc: Some(self.universal_id.clone()),
            universal_id: Uuid::new_v4().simple().to_string(),
        }
    }

    /// Apply a board diff: size == 0 → remove, else → upsert
    pub fn apply_ask_diff(&mut self, price: f64, size: f64) {
        let key = OrderedFloat(price);
        if size == 0.0 {
            self.asks.remove(&key);
        } else {
            self.asks.insert(key, size);
        }
    }

    pub fn apply_bid_diff(&mut self, price: f64, size: f64) {
        let key = OrderedFloat(price);
        if size == 0.0 {
            self.bids.remove(&key);
        } else {
            self.bids.insert(key, size);
        }
    }

    /// Replace all bids from a snapshot
    pub fn set_bids_from_snapshot(&mut self, entries: &[(&str, &str)]) {
        self.bids.clear();
        for (price_str, size_str) in entries {
            if let (Ok(p), Ok(s)) = (price_str.parse::<f64>(), size_str.parse::<f64>()) {
                self.bids.insert(OrderedFloat(p), s);
            }
        }
    }

    /// Replace all asks from a snapshot
    pub fn set_asks_from_snapshot(&mut self, entries: &[(&str, &str)]) {
        self.asks.clear();
        for (price_str, size_str) in entries {
            if let (Ok(p), Ok(s)) = (price_str.parse::<f64>(), size_str.parse::<f64>()) {
                self.asks.insert(OrderedFloat(p), s);
            }
        }
    }

    /// Set bids from JSON array of {"price": f64, "size": f64}
    pub fn set_bids_from_json(&mut self, entries: &[Value]) {
        self.bids.clear();
        for entry in entries {
            if let (Some(p), Some(s)) = (
                entry.get("price").and_then(|v| v.as_f64()),
                entry.get("size").and_then(|v| v.as_f64()),
            ) {
                self.bids.insert(OrderedFloat(p), s);
            }
        }
    }

    /// Set asks from JSON array of {"price": f64, "size": f64}
    pub fn set_asks_from_json(&mut self, entries: &[Value]) {
        self.asks.clear();
        for entry in entries {
            if let (Some(p), Some(s)) = (
                entry.get("price").and_then(|v| v.as_f64()),
                entry.get("size").and_then(|v| v.as_f64()),
            ) {
                self.asks.insert(OrderedFloat(p), s);
            }
        }
    }

    /// Set bids from JSON array of ["price_str", "size_str"] (BitBank format)
    pub fn set_bids_from_str_array(&mut self, entries: &[Value]) {
        self.bids.clear();
        for entry in entries {
            if let Some(arr) = entry.as_array() {
                if arr.len() >= 2 {
                    if let (Some(p), Some(s)) = (
                        arr[0].as_str().and_then(|v| v.parse::<f64>().ok()),
                        arr[1].as_str().and_then(|v| v.parse::<f64>().ok()),
                    ) {
                        self.bids.insert(OrderedFloat(p), s);
                    }
                }
            }
        }
    }

    /// Set asks from JSON array of ["price_str", "size_str"] (BitBank format)
    pub fn set_asks_from_str_array(&mut self, entries: &[Value]) {
        self.asks.clear();
        for entry in entries {
            if let Some(arr) = entry.as_array() {
                if arr.len() >= 2 {
                    if let (Some(p), Some(s)) = (
                        arr[0].as_str().and_then(|v| v.parse::<f64>().ok()),
                        arr[1].as_str().and_then(|v| v.parse::<f64>().ok()),
                    ) {
                        self.asks.insert(OrderedFloat(p), s);
                    }
                }
            }
        }
    }

    /// Set bids from JSON array of {"price": "str", "size": "str"} (GMO format)
    pub fn set_bids_from_json_str(&mut self, entries: &[Value]) {
        self.bids.clear();
        for entry in entries {
            if let (Some(p), Some(s)) = (
                entry.get("price").and_then(|v| v.as_str()).and_then(|v| v.parse::<f64>().ok()),
                entry.get("size").and_then(|v| v.as_str()).and_then(|v| v.parse::<f64>().ok()),
            ) {
                self.bids.insert(OrderedFloat(p), s);
            }
        }
    }

    /// Set asks from JSON array of {"price": "str", "size": "str"} (GMO format)
    pub fn set_asks_from_json_str(&mut self, entries: &[Value]) {
        self.asks.clear();
        for entry in entries {
            if let (Some(p), Some(s)) = (
                entry.get("price").and_then(|v| v.as_str()).and_then(|v| v.parse::<f64>().ok()),
                entry.get("size").and_then(|v| v.as_str()).and_then(|v| v.parse::<f64>().ok()),
            ) {
                self.asks.insert(OrderedFloat(p), s);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_exec_date() {
        let dt = parse_exec_date("2025-06-15T12:30:45.1234567Z");
        assert_eq!(dt.format("%Y-%m-%d %H:%M:%S").to_string(), "2025-06-15 12:30:45");
    }

    #[test]
    fn test_market_book_best_prices() {
        let mut book = MarketBook::new("FXBTCJPY", venue::BITFLYER, "json-rpc");
        book.bids.insert(OrderedFloat(100.0), 1.0);
        book.bids.insert(OrderedFloat(101.0), 2.0);
        book.bids.insert(OrderedFloat(99.0), 3.0);
        book.asks.insert(OrderedFloat(102.0), 1.0);
        book.asks.insert(OrderedFloat(103.0), 2.0);

        assert_eq!(book.best_bid(), 101.0);
        assert_eq!(book.best_ask(), 102.0);
        assert_eq!(book.mid_price(), 101.5);
    }

    #[test]
    fn test_market_book_to_dict_ordering() {
        let mut book = MarketBook::new("FXBTCJPY", venue::BITFLYER, "json-rpc");
        // Add bids (should be returned in descending order)
        for i in 1..=25 {
            book.bids.insert(OrderedFloat(i as f64), 1.0);
        }
        // Add asks (should be returned in ascending order)
        for i in 26..=50 {
            book.asks.insert(OrderedFloat(i as f64), 1.0);
        }

        let dict = book.to_dict();
        let bids = dict["bids"].as_array().unwrap();
        let asks = dict["asks"].as_array().unwrap();

        assert_eq!(bids.len(), 20);
        assert_eq!(asks.len(), 20);

        // Bids: descending (25, 24, 23, ...)
        assert_eq!(bids[0][0].as_f64().unwrap(), 25.0);
        assert_eq!(bids[19][0].as_f64().unwrap(), 6.0);

        // Asks: ascending (26, 27, 28, ...)
        assert_eq!(asks[0][0].as_f64().unwrap(), 26.0);
        assert_eq!(asks[19][0].as_f64().unwrap(), 45.0);
    }

    #[test]
    fn test_apply_diff() {
        let mut book = MarketBook::new("FXBTCJPY", venue::BITFLYER, "json-rpc");
        book.apply_bid_diff(100.0, 5.0);
        assert_eq!(book.bids[&OrderedFloat(100.0)], 5.0);

        // Update
        book.apply_bid_diff(100.0, 10.0);
        assert_eq!(book.bids[&OrderedFloat(100.0)], 10.0);

        // Remove (size == 0)
        book.apply_bid_diff(100.0, 0.0);
        assert!(!book.bids.contains_key(&OrderedFloat(100.0)));
    }

    #[test]
    fn test_timestamp_format() {
        use chrono::TimeZone;
        let dt = Utc.with_ymd_and_hms(2025, 6, 15, 12, 30, 45).unwrap();
        let s = format_timestamp(&dt);
        assert_eq!(s, "2025-06-15T12:30:45.000000+00:00");
    }
}
