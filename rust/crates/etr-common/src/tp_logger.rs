use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use chrono::{NaiveDate, Utc};
use tokio::sync::mpsc;
use tracing::{error, info};

/// TP (Tick Print) logger that writes messages to daily-rotated log files.
///
/// Format: `YYYY-MM-DD HH:MM:SS.mmm||{json}\n`
/// File naming: `TP-{handler_class}-{SYMBOL}.log` (active)
///              `TP-{handler_class}-{SYMBOL}.log.YYYY-MM-DD` (rotated)
///
/// Compatible with Python AsyncBufferedLogger and HdbDumper.
pub struct TpLogger {
    tx: mpsc::UnboundedSender<String>,
}

impl TpLogger {
    /// Create a new TpLogger. Spawns a background tokio task for async writes.
    ///
    /// - `logger_name`: e.g. "TP-BitFlyerSocketClient-FXBTCJPY"
    /// - `log_dir`: directory to store log files
    pub fn new(logger_name: &str, log_dir: &Path) -> Self {
        // Ensure log directory exists
        if let Err(e) = fs::create_dir_all(log_dir) {
            error!("Failed to create TP log directory {:?}: {}", log_dir, e);
        }

        let (tx, rx) = mpsc::unbounded_channel::<String>();
        let writer = TpLogWriter::new(logger_name.to_string(), log_dir.to_path_buf());

        // Spawn background writer task
        tokio::spawn(async move {
            writer.run(rx).await;
        });

        Self { tx }
    }

    /// Send a log message (non-blocking). The message will be timestamped and
    /// written to the log file by the background task.
    pub fn info(&self, msg: String) {
        if self.tx.send(msg).is_err() {
            error!("TpLogger channel closed, message dropped");
        }
    }
}

struct TpLogWriter {
    logger_name: String,
    log_dir: PathBuf,
    current_file: Option<File>,
    current_date: Option<NaiveDate>,
    backup_count: usize,
}

impl TpLogWriter {
    fn new(logger_name: String, log_dir: PathBuf) -> Self {
        Self {
            logger_name,
            log_dir,
            current_file: None,
            current_date: None,
            backup_count: 14,
        }
    }

    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<String>) {
        while let Some(msg) = rx.recv().await {
            let now = Utc::now();
            let today = now.date_naive();

            // Check if we need to rotate
            if self.current_date != Some(today) {
                self.rotate(today);
            }

            // Format: YYYY-MM-DD HH:MM:SS.mmm||{json}
            let ts = now.format("%Y-%m-%d %H:%M:%S%.3f");
            let line = format!("{}||{}\n", ts, msg);

            if let Some(ref mut file) = self.current_file {
                if let Err(e) = file.write_all(line.as_bytes()) {
                    error!("Failed to write TP log: {}", e);
                }
            }
        }

        info!("TpLogWriter for {} shutting down", self.logger_name);
    }

    fn active_path(&self) -> PathBuf {
        self.log_dir.join(format!("{}.log", self.logger_name))
    }

    fn rotated_path(&self, date: &NaiveDate) -> PathBuf {
        self.log_dir
            .join(format!("{}.log.{}", self.logger_name, date.format("%Y-%m-%d")))
    }

    fn rotate(&mut self, today: NaiveDate) {
        // Close current file
        self.current_file = None;

        let active = self.active_path();

        // If active file exists, rename it to the previous date
        if active.exists() {
            if let Some(prev_date) = self.current_date {
                let rotated = self.rotated_path(&prev_date);
                if let Err(e) = fs::rename(&active, &rotated) {
                    error!("Failed to rotate TP log: {}", e);
                }
            }
        }

        // Clean up old rotated files (keep backup_count)
        self.cleanup_old_files();

        // Open new active file
        match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&active)
        {
            Ok(file) => {
                self.current_file = Some(file);
                self.current_date = Some(today);
                info!(
                    "TpLogger opened: {}",
                    active.display()
                );
            }
            Err(e) => {
                error!("Failed to open TP log file {:?}: {}", active, e);
            }
        }
    }

    fn cleanup_old_files(&self) {
        let pattern = format!("{}.log.", self.logger_name);
        let dir = &self.log_dir;

        if let Ok(entries) = fs::read_dir(dir) {
            let mut rotated_files: Vec<PathBuf> = entries
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .map(|n| n.starts_with(&pattern))
                        .unwrap_or(false)
                })
                .collect();

            // Sort by name (which includes date, so lexicographic = chronological)
            rotated_files.sort();

            // Remove oldest files if we exceed backup_count
            while rotated_files.len() > self.backup_count {
                if let Some(old_file) = rotated_files.first() {
                    info!("Removing old TP log: {}", old_file.display());
                    let _ = fs::remove_file(old_file);
                    rotated_files.remove(0);
                }
            }
        }
    }
}
