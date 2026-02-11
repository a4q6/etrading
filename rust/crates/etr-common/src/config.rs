use std::path::PathBuf;

/// Application configuration loaded from environment variables (.env file).
pub struct Config {
    pub rtp_dir: PathBuf,
    pub log_dir: PathBuf,
    pub tp_dir: PathBuf,
    pub hdb_dir: PathBuf,
    pub discord_url_all: Option<String>,
}

impl Config {
    /// Load config from .env file and environment variables.
    /// Falls back to sensible defaults if variables are not set.
    pub fn load() -> Self {
        // Try loading .env from current dir or parent dirs
        let _ = dotenvy::dotenv();

        let rtp_dir = std::env::var("RTP_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./data/rtp"));

        let log_dir = std::env::var("LOG_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./logs"));

        let tp_dir = std::env::var("TP_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./data/tp"));

        let hdb_dir = std::env::var("HDB_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./data/rhdb"));

        let discord_url_all = std::env::var("DISCORD_URL_ALL").ok();

        Config {
            rtp_dir,
            log_dir,
            tp_dir,
            hdb_dir,
            discord_url_all,
        }
    }
}
