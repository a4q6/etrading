use std::path::Path;
use std::sync::Arc;

use chrono::{NaiveDate, Utc};
use clap::Parser;
use tracing::{error, info};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt::format::{self, FormatEvent, FormatFields};
use tracing_subscriber::fmt::{self as fmt_mod, FmtContext};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{self, EnvFilter};

use etr_common::config::Config;
use etr_feed_binance::BinanceSocketClient;
use etr_feed_bitbank::BitBankSocketClient;
use etr_feed_bitflyer::BitFlyerSocketClient;
use etr_feed_gmo::{GmoCryptSocketClient, GmoForexSocketClient};
use etr_publisher::LocalWsPublisher;

/// etr-feed: Rust-based exchange feed handler
#[derive(Parser, Debug)]
#[command(name = "etr-feed", about = "Exchange feed handler (Rust)")]
struct Args {
    /// BitFlyer currency pairs (comma-separated, e.g. "BTC_JPY,FX_BTC_JPY")
    #[arg(long = "bitflyer-pairs", value_delimiter = ',')]
    bitflyer_pairs: Option<Vec<String>>,

    /// BitBank currency pairs (comma-separated, e.g. "btc_jpy,eth_jpy")
    #[arg(long = "bitbank-pairs", value_delimiter = ',')]
    bitbank_pairs: Option<Vec<String>>,

    /// GMO Crypto currency pairs (comma-separated, e.g. "BTC_JPY,ETH_JPY,BTC,ETH")
    #[arg(long = "gmo-crypt-pairs", value_delimiter = ',')]
    gmo_crypt_pairs: Option<Vec<String>>,

    /// GMO Forex currency pairs (comma-separated, e.g. "USD_JPY,EUR_USD")
    #[arg(long = "gmo-forex-pairs", value_delimiter = ',')]
    gmo_forex_pairs: Option<Vec<String>>,

    /// Binance currency pairs (comma-separated, e.g. "BTCUSDT,ETHUSDT")
    #[arg(long = "binance-pairs", value_delimiter = ',')]
    binance_pairs: Option<Vec<String>>,

    /// WebSocket publisher port (set 0 to disable publisher)
    #[arg(long, default_value_t = 8765)]
    port: u16,

    /// Disable the WebSocket publisher (TP logs only)
    #[arg(long)]
    no_publisher: bool,

    /// Maximum reconnection attempts (unlimited if not set)
    #[arg(long)]
    max_reconnects: Option<u32>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let config = Config::load();

    // Initialize tracing: console + file (daily rotation)
    let _guard = init_tracing(&config);

    // Clean up process logs older than 14 days
    cleanup_old_logs(&config.log_dir, "etr-feed.log.", 14);

    info!("etr-feed starting");
    if let Some(ref pairs) = args.bitflyer_pairs {
        info!("BitFlyer pairs: {:?}", pairs);
    }
    if let Some(ref pairs) = args.bitbank_pairs {
        info!("BitBank pairs: {:?}", pairs);
    }
    if let Some(ref pairs) = args.gmo_crypt_pairs {
        info!("GMO Crypt pairs: {:?}", pairs);
    }
    if let Some(ref pairs) = args.gmo_forex_pairs {
        info!("GMO Forex pairs: {:?}", pairs);
    }
    if let Some(ref pairs) = args.binance_pairs {
        info!("Binance pairs: {:?}", pairs);
    }
    info!("RTP dir: {:?}", config.rtp_dir);
    let pub_status = if args.no_publisher {
        "disabled".to_string()
    } else {
        format!("port {}", args.port)
    };
    info!("Publisher: {}", pub_status);

    // Start publisher (unless disabled)
    let publisher = if !args.no_publisher && args.port > 0 {
        let pub_instance = Arc::new(LocalWsPublisher::new(args.port));
        if let Err(e) = pub_instance.start().await {
            error!("Failed to start publisher: {}", e);
            return;
        }
        Some(pub_instance)
    } else {
        None
    };

    // Spawn all configured feed handlers
    let mut handles = Vec::new();

    if let Some(pairs) = args.bitflyer_pairs {
        let mut handler = BitFlyerSocketClient::new(
            pairs,
            args.max_reconnects,
            publisher.clone(),
            &config.rtp_dir,
        );
        handles.push(tokio::spawn(async move {
            handler.start().await;
            info!("BitFlyer feed handler stopped");
        }));
    }

    if let Some(pairs) = args.bitbank_pairs {
        let mut handler = BitBankSocketClient::new(
            pairs,
            args.max_reconnects,
            publisher.clone(),
            &config.rtp_dir,
        );
        handles.push(tokio::spawn(async move {
            handler.start().await;
            info!("BitBank feed handler stopped");
        }));
    }

    if let Some(pairs) = args.gmo_crypt_pairs {
        let mut handler = GmoCryptSocketClient::new(
            pairs,
            args.max_reconnects,
            publisher.clone(),
            &config.rtp_dir,
        );
        handles.push(tokio::spawn(async move {
            handler.start().await;
            info!("GMO Crypt feed handler stopped");
        }));
    }

    if let Some(pairs) = args.gmo_forex_pairs {
        let mut handler = GmoForexSocketClient::new(
            pairs,
            args.max_reconnects,
            publisher.clone(),
            &config.rtp_dir,
        );
        handles.push(tokio::spawn(async move {
            handler.start().await;
            info!("GMO Forex feed handler stopped");
        }));
    }

    if let Some(pairs) = args.binance_pairs {
        let mut handler = BinanceSocketClient::new(
            pairs,
            args.max_reconnects,
            publisher.clone(),
            &config.rtp_dir,
        );
        handles.push(tokio::spawn(async move {
            handler.start().await;
            info!("Binance feed handler stopped");
        }));
    }

    if handles.is_empty() {
        error!("No feed handlers configured. Use --bitflyer-pairs, --bitbank-pairs, --gmo-crypt-pairs, --gmo-forex-pairs, or --binance-pairs to specify at least one.");
        return;
    }

    // Graceful shutdown on SIGINT/SIGTERM
    let shutdown = tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for ctrl+c");
        info!("Shutdown signal received");
    });

    tokio::select! {
        _ = futures_wait_all(handles) => {
            info!("All feed handlers stopped");
        }
        _ = shutdown => {
            info!("Shutting down gracefully...");
        }
    }

    info!("etr-feed stopped");
}

/// Wait for all spawned tasks to complete.
async fn futures_wait_all(handles: Vec<tokio::task::JoinHandle<()>>) {
    for handle in handles {
        let _ = handle.await;
    }
}

/// Delete log files older than `retain_days` in the given directory.
fn cleanup_old_logs(log_dir: &Path, prefix: &str, retain_days: i64) {
    let cutoff = Utc::now().date_naive() - chrono::Duration::days(retain_days);

    let entries = match std::fs::read_dir(log_dir) {
        Ok(e) => e,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        let fname = match entry.file_name().into_string() {
            Ok(s) => s,
            Err(_) => continue,
        };

        if !fname.starts_with(prefix) {
            continue;
        }
        let date_part = match fname.rsplit('.').next() {
            Some(d) if d.len() == 10 => d,
            _ => continue,
        };
        if let Ok(file_date) = NaiveDate::parse_from_str(date_part, "%Y-%m-%d") {
            if file_date < cutoff {
                let path = entry.path();
                if std::fs::remove_file(&path).is_ok() {
                    info!(file = %path.display(), "Deleted old process log");
                }
            }
        }
    }
}

/// Pipe-delimited log formatter.
/// Console: `2025-02-11 12:30:45.123|INFO|message key=value ...`
/// File:    `2025-02-11 12:30:45.123|INFO|module:line|message key=value ...`
#[derive(Clone)]
struct PipeFormatter {
    include_location: bool,
}

impl<S, N> FormatEvent<S, N> for PipeFormatter
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        let now = Utc::now();
        let meta = event.metadata();
        let level = *meta.level();
        write!(writer, "{}|{}|", now.format("%Y-%m-%d %H:%M:%S%.3f"), level)?;
        if self.include_location {
            let file = meta.file().unwrap_or("?");
            // Strip path prefix, keep only filename
            let file_short = file.rsplit('/').next().unwrap_or(file);
            let line = meta.line().unwrap_or(0);
            write!(writer, "{}:{}|", file_short, line)?;
        }
        ctx.field_format().format_fields(writer.by_ref(), event)?;
        writeln!(writer)
    }
}

/// Initialize tracing with both console and file output.
/// File logs are written to `{LOG_DIR}/etr-feed.log` with UTC daily rotation (14 days kept).
/// Returns a guard that must be held for the lifetime of the program to ensure logs are flushed.
fn init_tracing(config: &Config) -> WorkerGuard {
    std::fs::create_dir_all(&config.log_dir).expect("Failed to create log directory");

    let file_appender = tracing_appender::rolling::daily(&config.log_dir, "etr-feed.log");
    let (file_writer, guard) = tracing_appender::non_blocking(file_appender);

    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(
            fmt_mod::layer()
                .event_format(PipeFormatter { include_location: false })
                .with_ansi(false)
                .with_writer(std::io::stdout),
        )
        .with(
            fmt_mod::layer()
                .event_format(PipeFormatter { include_location: true })
                .with_ansi(false)
                .with_writer(file_writer),
        )
        .init();

    guard
}
