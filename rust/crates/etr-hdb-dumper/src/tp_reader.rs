use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use tracing::warn;

/// Streaming reader for TP (Tick Print) log files.
///
/// TP format: `YYYY-MM-DD HH:MM:SS.mmm||{json}\n`
/// Each line is parsed lazily as an iterator item.
pub struct TpReader {
    reader: BufReader<File>,
    line_buf: String,
    line_number: usize,
}

impl TpReader {
    /// Open a TP log file for streaming.
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file = File::open(path)?;
        Ok(Self {
            reader: BufReader::with_capacity(256 * 1024, file),
            line_buf: String::with_capacity(4096),
            line_number: 0,
        })
    }
}

impl Iterator for TpReader {
    /// Returns (data_type, json_value) for each valid line.
    type Item = (String, serde_json::Value);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.line_buf.clear();
            match self.reader.read_line(&mut self.line_buf) {
                Ok(0) => return None, // EOF
                Ok(_) => {
                    self.line_number += 1;
                    let line = self.line_buf.trim();
                    if line.is_empty() {
                        continue;
                    }

                    // Split on "||" separator
                    let json_part = match line.split_once("||") {
                        Some((_, json)) => json,
                        None => {
                            warn!(line = self.line_number, "Missing '||' separator, skipping");
                            continue;
                        }
                    };

                    // Parse JSON
                    let value: serde_json::Value = match serde_json::from_str(json_part) {
                        Ok(v) => v,
                        Err(e) => {
                            warn!(
                                line = self.line_number,
                                error = %e,
                                "Failed to parse JSON, skipping"
                            );
                            continue;
                        }
                    };

                    // Extract _data_type
                    let data_type = match value.get("_data_type").and_then(|v| v.as_str()) {
                        Some(dt) => dt.to_string(),
                        None => {
                            warn!(
                                line = self.line_number,
                                "Missing _data_type field, skipping"
                            );
                            continue;
                        }
                    };

                    return Some((data_type, value));
                }
                Err(e) => {
                    warn!(
                        line = self.line_number,
                        error = %e,
                        "Read error, skipping line"
                    );
                    continue;
                }
            }
        }
    }
}

/// Extract the first timestamp from a TP log file (for log rotation).
pub fn read_first_timestamp<P: AsRef<Path>>(path: P) -> Option<String> {
    let file = File::open(path).ok()?;
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let line = line.ok()?;
        let line = line.trim().to_string();
        if line.len() > 10 {
            if let Some((ts_part, _)) = line.split_once("||") {
                return Some(ts_part.to_string());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn create_temp_tp_file(content: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn test_tp_reader_basic() {
        let content = r#"2025-06-15 12:30:45.123||{"_data_type":"Rate","sym":"BTCJPY","venue":"gmo","best_bid":100.0}
2025-06-15 12:30:46.456||{"_data_type":"MarketTrade","sym":"BTCJPY","venue":"gmo","price":100.5}
"#;
        let f = create_temp_tp_file(content);
        let reader = TpReader::open(f.path()).unwrap();
        let records: Vec<_> = reader.collect();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0, "Rate");
        assert_eq!(records[1].0, "MarketTrade");
    }

    #[test]
    fn test_tp_reader_skips_invalid() {
        let content = r#"2025-06-15 12:30:45.123||{"_data_type":"Rate","sym":"BTC"}
bad line without separator
2025-06-15 12:30:46.456||not valid json
2025-06-15 12:30:47.789||{"no_data_type":"oops"}
2025-06-15 12:30:48.000||{"_data_type":"MarketTrade","sym":"BTC"}
"#;
        let f = create_temp_tp_file(content);
        let reader = TpReader::open(f.path()).unwrap();
        let records: Vec<_> = reader.collect();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn test_read_first_timestamp() {
        let content = "2025-06-15 12:30:45.123||{\"_data_type\":\"Rate\"}\n";
        let f = create_temp_tp_file(content);
        let ts = read_first_timestamp(f.path()).unwrap();
        assert_eq!(ts, "2025-06-15 12:30:45.123");
    }
}
