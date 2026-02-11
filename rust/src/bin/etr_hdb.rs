use std::io;
use std::path::{Path, PathBuf};

use chrono::{NaiveDate, Utc};
use clap::Parser;
use tracing::info;
use tracing_subscriber::fmt::format::{self, FormatEvent, FormatFields};
use tracing_subscriber::fmt::FmtContext;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use etr_common::config::Config;
use etr_hdb_dumper::HdbDumper;

#[derive(Parser)]
#[command(name = "etr-hdb", about = "Rust HDB Dumper - Convert TP logs to Parquet")]
struct Cli {
    /// Process a single TP log file
    #[arg(long)]
    file: Option<PathBuf>,

    /// Number of days to process (default: 10)
    #[arg(long, default_value = "10")]
    n_days: i64,

    /// Override HDB output directory
    #[arg(long)]
    hdb_dir: Option<PathBuf>,

    /// Override TP log directory
    #[arg(long)]
    tp_dir: Option<PathBuf>,

    /// Send Discord notification after processing
    #[arg(long)]
    notification: bool,

    /// Run as a loop (daily trigger)
    #[arg(long)]
    r#loop: bool,

    /// Trigger time in UTC for loop mode (HH:MM)
    #[arg(long, default_value = "01:00")]
    trigger_time: String,

    /// Batch size for Parquet row groups (default: 100000)
    #[arg(long, default_value = "100000")]
    batch_size: usize,

    /// Force reprocessing even if output already exists
    #[arg(long)]
    force: bool,
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
            let file_short = file.rsplit('/').next().unwrap_or(file);
            let line = meta.line().unwrap_or(0);
            write!(writer, "{}:{}|", file_short, line)?;
        }
        ctx.field_format().format_fields(writer.by_ref(), event)?;
        writeln!(writer)
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

        // Match rotated files like "hdb_dumper.log.2025-06-15"
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let config = Config::load();

    let log_dir = &config.log_dir;
    std::fs::create_dir_all(log_dir)?;
    let file_appender = tracing_appender::rolling::daily(log_dir, "hdb_dumper.log");

    tracing_subscriber::registry()
        .with(filter)
        .with(
            fmt::layer()
                .event_format(PipeFormatter { include_location: false })
                .with_ansi(false)
                .with_writer(io::stdout),
        )
        .with(
            fmt::layer()
                .event_format(PipeFormatter { include_location: true })
                .with_ansi(false)
                .with_writer(file_appender),
        )
        .init();

    // Clean up process logs older than 14 days
    cleanup_old_logs(log_dir, "hdb_dumper.log.", 14);

    let cli = Cli::parse();

    let mut dumper = HdbDumper::from_config(&config);

    if let Some(ref hdb_dir) = cli.hdb_dir {
        dumper.hdb_dir = hdb_dir.clone();
    }
    if let Some(ref tp_dir) = cli.tp_dir {
        dumper.tp_dir = tp_dir.clone();
    }
    dumper.batch_size = cli.batch_size;

    info!(
        hdb_dir = %dumper.hdb_dir.display(),
        tp_dir = %dumper.tp_dir.display(),
        batch_size = dumper.batch_size,
        "HDB Dumper started"
    );

    if let Some(ref file) = cli.file {
        let skip = !cli.force;
        dumper.dump_to_hdb(file, skip)?;
    } else if cli.r#loop {
        dumper
            .start_hdb_loop(cli.n_days, &cli.trigger_time, cli.notification)
            .await?;
    } else {
        let skip = !cli.force;
        dumper.dump_all(cli.n_days, skip)?;
    }

    Ok(())
}
