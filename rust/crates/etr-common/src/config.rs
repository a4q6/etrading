use std::path::PathBuf;

/// Application configuration loaded from environment variables (.env file).
pub struct Config {
    pub rtp_dir: PathBuf,
    pub log_dir: PathBuf,
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

        Config { rtp_dir, log_dir }
    }
}
