use std::path::PathBuf;

use clap::Parser;
use tracing::info;
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Logging setup
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let config = Config::load();

    let log_dir = &config.log_dir;
    std::fs::create_dir_all(log_dir)?;
    let file_appender = tracing_appender::rolling::daily(log_dir, "hdb_dumper.log");

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_target(false))
        .with(
            fmt::layer()
                .with_target(false)
                .with_ansi(false)
                .with_writer(file_appender),
        )
        .init();

    let cli = Cli::parse();

    let mut dumper = HdbDumper::from_config(&config);

    // Override paths if specified
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
        // Single file mode
        let skip = !cli.force;
        dumper.dump_to_hdb(file, skip)?;
    } else if cli.r#loop {
        // Loop mode
        dumper
            .start_hdb_loop(cli.n_days, &cli.trigger_time, cli.notification)
            .await?;
    } else {
        // One-shot dump_all
        let skip = !cli.force;
        dumper.dump_all(cli.n_days, skip)?;
    }

    Ok(())
}
