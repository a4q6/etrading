use std::collections::HashMap;

/// Mapping from FeedHandler class name to the list of `_data_type` values it produces.
pub fn table_list() -> HashMap<&'static str, Vec<&'static str>> {
    HashMap::from([
        (
            "BitmexSocketClient",
            vec!["Rate", "MarketBook", "MarketTrade"],
        ),
        (
            "BitBankSocketClient",
            vec!["Rate", "MarketBook", "MarketTrade", "CircuitBreaker"],
        ),
        (
            "BitFlyerSocketClient",
            vec!["Rate", "MarketBook", "MarketTrade"],
        ),
        ("GmoForexSocketClient", vec!["Rate"]),
        (
            "GmoCryptSocketClient",
            vec!["Rate", "MarketBook", "MarketTrade"],
        ),
        (
            "CoincheckSocketClient",
            vec!["MarketTrade", "Rate", "MarketBook"],
        ),
        ("BitFlyerFundingRate", vec!["FundingRate"]),
        ("BinanceSocketClient", vec!["Rate", "MarketTrade"]),
        ("BinanceRestEoption", vec!["ImpliedVolatility"]),
        (
            "CoincheckPrivate",
            vec!["Order", "Trade"],
        ),
        (
            "BitBankPrivate",
            vec!["Order", "Trade", "PositionUpdate"],
        ),
        (
            "HyperliquidSocketClient",
            vec!["MarketTrade", "Candle"],
        ),
    ])
}

/// Mapping from FeedHandler class name to venue string.
pub fn venue_map() -> HashMap<&'static str, &'static str> {
    HashMap::from([
        ("BitmexSocketClient", "bitmex"),
        ("BitBankSocketClient", "bitbank"),
        ("BitFlyerSocketClient", "bitflyer"),
        ("GmoForexSocketClient", "gmo"),
        ("GmoCryptSocketClient", "gmo"),
        ("CoincheckSocketClient", "coincheck"),
        ("BitFlyerFundingRate", "bitflyer"),
        ("BinanceSocketClient", "binance"),
        ("BinanceRestEoption", "binance"),
        ("BitBankPrivate", "bitbank"),
        ("CoincheckPrivate", "coincheck"),
        ("HyperliquidSocketClient", "hyperliquid"),
    ])
}

/// Handlers that have multi-TP files (numbered suffix).
pub fn mtp_list() -> Vec<&'static str> {
    vec!["BitBankPrivate", "CoincheckPrivate"]
}
