use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{NaiveDate, Utc};
use glob::glob;
use regex::Regex;
use tracing::{info, warn};

use crate::discord::send_discord_webhook;
use crate::parquet_writer::BatchParquetWriter;
use crate::schema;
use crate::tp_reader::{self, TpReader};

const DEFAULT_BATCH_SIZE: usize = 100_000;

pub struct HdbDumper {
    pub hdb_dir: PathBuf,
    pub tp_dir: PathBuf,
    pub batch_size: usize,
    pub discord_url: Option<String>,
}

/// Information parsed from a TP log filename.
struct LogFileInfo {
    logger_name: String,
    raw_logger_name: String,
    sym: String,
    date: String,
}

impl HdbDumper {
    pub fn new(hdb_dir: PathBuf, tp_dir: PathBuf, discord_url: Option<String>) -> Self {
        Self {
            hdb_dir,
            tp_dir,
            batch_size: DEFAULT_BATCH_SIZE,
            discord_url,
        }
    }

    /// Create from etr-common Config.
    pub fn from_config(config: &etr_common::config::Config) -> Self {
        Self::new(
            config.hdb_dir.clone(),
            config.tp_dir.clone(),
            config.discord_url_all.clone(),
        )
    }

    /// Parse TP log filename into its components.
    fn parse_log_filename(log_file: &Path) -> Option<LogFileInfo> {
        let fname = log_file.file_name()?.to_str()?;
        // Format: TP-{LoggerName}-{Symbol}.log.{YYYY-MM-DD}
        let parts: Vec<&str> = fname.splitn(3, '-').collect();
        if parts.len() < 3 {
            warn!(fname = fname, "Cannot parse TP filename");
            return None;
        }

        let logger_name = parts[1].to_string();
        // Strip trailing digits to get base handler name
        let re = Regex::new(r"\d+").unwrap();
        let raw_logger_name = re.replace_all(&logger_name, "").to_string();

        // "BTCJPY.log.2025-06-15" -> sym="BTCJPY", date="2025-06-15"
        let rest = parts[2];
        let sym = rest
            .split(".log")
            .next()
            .unwrap_or("")
            .replace('_', "")
            .to_uppercase();
        let date = fname.rsplit('.').next().unwrap_or("").to_string();

        Some(LogFileInfo {
            logger_name,
            raw_logger_name,
            sym,
            date,
        })
    }

    /// Build the output Parquet path (matches Python's build_path).
    fn build_path(&self, table: &str, date: &str, venue: &str, sym: &str) -> PathBuf {
        let ymd = date; // already in YYYY-MM-DD format
        let fname = format!("{table}_{venue}_{sym}_{date}.parquet");
        self.hdb_dir
            .join(table)
            .join(venue)
            .join(ymd)
            .join(sym)
            .join(fname)
    }

    /// Dump a single TP log file to Parquet HDB.
    pub fn dump_to_hdb(
        &self,
        log_file: &Path,
        skip_if_exists: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let info = Self::parse_log_filename(log_file)
            .ok_or_else(|| format!("Cannot parse filename: {}", log_file.display()))?;

        let table_list = schema::table_list();
        let venue_map = schema::venue_map();
        let mtp_list = schema::mtp_list();

        let tables = table_list
            .get(info.raw_logger_name.as_str())
            .ok_or_else(|| {
                format!(
                    "Unknown handler '{}' (raw: '{}')",
                    info.logger_name, info.raw_logger_name
                )
            })?;

        let venue = venue_map
            .get(info.raw_logger_name.as_str())
            .ok_or_else(|| format!("Unknown venue for '{}'", info.raw_logger_name))?;

        info!(
            file = %log_file.display(),
            handler = %info.raw_logger_name,
            venue = venue,
            sym = %info.sym,
            date = %info.date,
            "Start extraction"
        );

        // Check if all output files already exist
        if skip_if_exists {
            let all_exist = tables
                .iter()
                .all(|t| self.build_path(t, &info.date, venue, &info.sym).exists());
            if all_exist {
                info!(
                    file = %log_file.display(),
                    "All output files exist, skipping"
                );
                return Ok(());
            }
        }

        // Collect TP files to read (handle multi-TP)
        let mut tp_files: Vec<PathBuf> = vec![log_file.to_path_buf()];
        if mtp_list.contains(&info.raw_logger_name.as_str()) {
            for i in 0..10 {
                let candidate_name = log_file
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .replace(&info.logger_name, &format!("{}{}", info.raw_logger_name, i));
                let candidate = log_file.with_file_name(&candidate_name);
                if candidate.exists() && candidate != log_file {
                    info!(file = %candidate.display(), "Found MTP file");
                    tp_files.push(candidate);
                }
            }
        }

        // Create writers for each table
        let mut writers: HashMap<String, BatchParquetWriter> = HashMap::new();
        for table in tables {
            let path = self.build_path(table, &info.date, venue, &info.sym);
            writers.insert(
                table.to_string(),
                BatchParquetWriter::new(path, self.batch_size),
            );
        }

        // Stream through all TP files
        for tp_file in &tp_files {
            let reader = TpReader::open(tp_file)?;
            for (data_type, record) in reader {
                if let Some(writer) = writers.get_mut(&data_type) {
                    writer.push(record)?;
                }
                // Records for unknown data types are silently skipped
            }
        }

        // Finalize all writers
        for (table, writer) in writers {
            let total = writer.finish()?;
            if total == 0 {
                info!(table = %table, "No records found");
            } else {
                info!(table = %table, rows = total, "Saved");
            }
        }

        Ok(())
    }

    /// List TP log files matching `*.log.*` pattern.
    /// Returns Vec of (path, logger_name, date_str).
    pub fn list_log_files(&self) -> Vec<(PathBuf, String, String)> {
        let pattern = format!("{}/*.log.*", self.tp_dir.display());
        let mut files: Vec<(PathBuf, String, String)> = Vec::new();

        for entry in glob(&pattern).unwrap_or_else(|_| panic!("Invalid glob pattern")) {
            if let Ok(path) = entry {
                if let Some(info) = Self::parse_log_filename(&path) {
                    files.push((path, info.raw_logger_name, info.date));
                }
            }
        }

        // Sort by date
        files.sort_by(|a, b| a.2.cmp(&b.2));
        files
    }

    /// Roll active log files that contain data from a previous day.
    /// Renames `*.log` to `*.log.YYYY-MM-DD`.
    pub fn roll_old_log_files(&self) {
        let pattern = format!("{}/*.log", self.tp_dir.display());
        let today = Utc::now().date_naive();

        for entry in glob(&pattern).unwrap_or_else(|_| panic!("Invalid glob pattern")) {
            let path = match entry {
                Ok(p) => p,
                Err(_) => continue,
            };

            // Read first timestamp from the file
            if let Some(ts_str) = tp_reader::read_first_timestamp(&path) {
                if let Ok(file_date) = NaiveDate::parse_from_str(&ts_str[..10], "%Y-%m-%d") {
                    if file_date < today {
                        let new_name =
                            format!("{}.{}", path.display(), file_date.format("%Y-%m-%d"));
                        match fs::rename(&path, &new_name) {
                            Ok(_) => {
                                info!(
                                    from = %path.display(),
                                    to = %new_name,
                                    "Rotated log file"
                                );
                            }
                            Err(e) => {
                                warn!(
                                    file = %path.display(),
                                    error = %e,
                                    "Failed to rotate log file"
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    /// Process all TP log files from the last `n_days`.
    pub fn dump_all(
        &self,
        n_days: i64,
        skip_if_exists: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.roll_old_log_files();

        let files = self.list_log_files();
        let threshold = Utc::now().date_naive() - chrono::Duration::days(n_days);

        for (path, _handler, date_str) in &files {
            if let Ok(file_date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                if file_date >= threshold {
                    if let Err(e) = self.dump_to_hdb(path, skip_if_exists) {
                        warn!(
                            file = %path.display(),
                            error = %e,
                            "Failed to process TP file"
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Run the HDB dump loop: execute dump_all at a fixed time daily.
    pub async fn start_hdb_loop(
        &self,
        n_days: i64,
        trigger_time_utc: &str,
        notification: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let trigger_hour: u32 = trigger_time_utc[..2].parse().unwrap_or(1);
        let trigger_minute: u32 = trigger_time_utc[3..5].parse().unwrap_or(0);

        loop {
            // Run dump
            info!("Starting HDB dump cycle");
            if let Err(e) = self.dump_all(n_days, true) {
                warn!(error = %e, "dump_all failed");
            }

            // Notification
            if notification {
                if let Some(ref url) = self.discord_url {
                    let msg = format!(
                        "EoD Process Finished! (Rust HDB Dumper)\nProcessed files from last {} days",
                        n_days
                    );
                    if let Err(e) = send_discord_webhook(&msg, "EoD-HDB", url).await {
                        warn!(error = %e, "Discord notification failed");
                    }
                }
            }

            // Calculate sleep until next trigger
            let now = Utc::now();
            let today = now.date_naive();
            let trigger_time =
                today.and_hms_opt(trigger_hour, trigger_minute, 0).unwrap();
            let next_run = if now.naive_utc() < trigger_time {
                trigger_time
            } else {
                trigger_time + chrono::Duration::days(1)
            };

            let sleep_secs = (next_run - now.naive_utc()).num_seconds().max(0) as u64;
            info!(
                next_run = %next_run,
                sleep_minutes = sleep_secs / 60,
                "Waiting for next cycle"
            );
            tokio::time::sleep(std::time::Duration::from_secs(sleep_secs)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_log_filename() {
        let path = Path::new("/data/rtp/TP-GmoCryptSocketClient-BTCJPY.log.2025-06-15");
        let info = HdbDumper::parse_log_filename(path).unwrap();
        assert_eq!(info.logger_name, "GmoCryptSocketClient");
        assert_eq!(info.raw_logger_name, "GmoCryptSocketClient");
        assert_eq!(info.sym, "BTCJPY");
        assert_eq!(info.date, "2025-06-15");
    }

    #[test]
    fn test_parse_log_filename_mtp() {
        let path = Path::new("/data/rtp/TP-BitBankPrivate0-ALL.log.2025-06-27");
        let info = HdbDumper::parse_log_filename(path).unwrap();
        assert_eq!(info.logger_name, "BitBankPrivate0");
        assert_eq!(info.raw_logger_name, "BitBankPrivate");
        assert_eq!(info.sym, "ALL");
        assert_eq!(info.date, "2025-06-27");
    }

    #[test]
    fn test_build_path() {
        let dumper = HdbDumper::new(
            PathBuf::from("/data/rhdb"),
            PathBuf::from("/data/rtp"),
            None,
        );
        let path = dumper.build_path("MarketTrade", "2025-06-15", "gmo", "BTCJPY");
        assert_eq!(
            path,
            PathBuf::from(
                "/data/rhdb/MarketTrade/gmo/2025-06-15/BTCJPY/MarketTrade_gmo_BTCJPY_2025-06-15.parquet"
            )
        );
    }

    #[test]
    fn test_dump_to_hdb_integration() {
        use std::io::Write;

        let tmp = tempfile::tempdir().unwrap();
        let tp_dir = tmp.path().join("rtp");
        let hdb_dir = tmp.path().join("hdb");
        fs::create_dir_all(&tp_dir).unwrap();

        // Create a TP log file
        let log_path = tp_dir.join("TP-GmoCryptSocketClient-BTCJPY.log.2025-06-15");
        let mut f = fs::File::create(&log_path).unwrap();
        for i in 0..5 {
            writeln!(
                f,
                "2025-06-15 12:30:{:02}.000||{{\"_data_type\":\"Rate\",\"timestamp\":\"2025-06-15T12:30:{:02}.000000+00:00\",\"sym\":\"BTCJPY\",\"venue\":\"gmo\",\"best_bid\":{}.0,\"best_ask\":{}.5}}",
                i, i, 100 + i, 100 + i
            ).unwrap();
        }
        for i in 0..3 {
            writeln!(
                f,
                "2025-06-15 12:31:{:02}.000||{{\"_data_type\":\"MarketTrade\",\"timestamp\":\"2025-06-15T12:31:{:02}.000000+00:00\",\"sym\":\"BTCJPY\",\"venue\":\"gmo\",\"side\":1,\"price\":{}.0,\"amount\":0.1}}",
                i, i, 200 + i
            ).unwrap();
        }
        f.flush().unwrap();

        let dumper = HdbDumper::new(hdb_dir.clone(), tp_dir, None);
        dumper.dump_to_hdb(&log_path, false).unwrap();

        // Verify output files exist
        let rate_path = dumper.build_path("Rate", "2025-06-15", "gmo", "BTCJPY");
        let trade_path = dumper.build_path("MarketTrade", "2025-06-15", "gmo", "BTCJPY");
        assert!(rate_path.exists(), "Rate parquet should exist");
        assert!(trade_path.exists(), "MarketTrade parquet should exist");
    }
}
