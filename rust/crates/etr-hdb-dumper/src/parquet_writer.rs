use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tracing::{debug, info};

/// Batch-oriented Parquet writer that accumulates JSON records in memory
/// and flushes to disk when the buffer reaches `batch_size`.
pub struct BatchParquetWriter {
    path: PathBuf,
    schema: Option<Arc<Schema>>,
    writer: Option<ArrowWriter<File>>,
    buffer: Vec<serde_json::Value>,
    batch_size: usize,
    total_rows: usize,
}

impl BatchParquetWriter {
    pub fn new(path: PathBuf, batch_size: usize) -> Self {
        Self {
            path,
            schema: None,
            writer: None,
            buffer: Vec::with_capacity(batch_size),
            batch_size,
            total_rows: 0,
        }
    }

    /// Push a single JSON record into the buffer.
    /// Automatically flushes when buffer reaches batch_size.
    pub fn push(&mut self, mut record: serde_json::Value) -> Result<(), Box<dyn std::error::Error>> {
        // Remove _data_type field (matches Python behavior)
        if let Some(obj) = record.as_object_mut() {
            obj.remove("_data_type");
        }
        self.buffer.push(record);

        if self.buffer.len() >= self.batch_size {
            self.flush()?;
        }
        Ok(())
    }

    /// Flush the current buffer to a Parquet row group.
    pub fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        // Infer schema from first record if not yet set
        if self.schema.is_none() {
            let schema = infer_schema(&self.buffer[0])?;
            self.schema = Some(Arc::new(schema));
        }
        let schema = self.schema.as_ref().unwrap().clone();

        // Initialize writer on first flush
        if self.writer.is_none() {
            if let Some(parent) = self.path.parent() {
                fs::create_dir_all(parent)?;
            }
            let file = File::create(&self.path)?;
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();
            let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
            self.writer = Some(writer);
        }

        // Build RecordBatch from buffer
        let batch = build_record_batch(&schema, &self.buffer)?;
        let num_rows = batch.num_rows();

        self.writer.as_mut().unwrap().write(&batch)?;
        self.total_rows += num_rows;
        debug!(
            rows = num_rows,
            total = self.total_rows,
            "Flushed batch to parquet"
        );

        self.buffer.clear();
        Ok(())
    }

    /// Finalize the Parquet file (flush remaining + close).
    /// Returns the total number of rows written.
    pub fn finish(mut self) -> Result<usize, Box<dyn std::error::Error>> {
        self.flush()?;
        if let Some(writer) = self.writer.take() {
            writer.close()?;
            info!(
                path = %self.path.display(),
                rows = self.total_rows,
                "Parquet file written"
            );
        }
        Ok(self.total_rows)
    }

    pub fn total_rows(&self) -> usize {
        self.total_rows
    }
}

/// Infer Arrow schema from a single JSON record.
/// - Columns containing "time" or "timestamp" → Timestamp(Microsecond, UTC)
/// - Number values → Float64 or Int64
/// - Everything else → Utf8
fn infer_schema(record: &serde_json::Value) -> Result<Schema, Box<dyn std::error::Error>> {
    let obj = record
        .as_object()
        .ok_or("Expected JSON object for schema inference")?;

    // Use BTreeMap to maintain stable column order
    let mut field_map: BTreeMap<&str, DataType> = BTreeMap::new();

    for (key, value) in obj {
        if key == "_data_type" {
            continue;
        }
        let dtype = if key.contains("time") || key.contains("timestamp") {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        } else {
            match value {
                serde_json::Value::Number(n) => {
                    if n.is_i64() {
                        DataType::Int64
                    } else {
                        DataType::Float64
                    }
                }
                serde_json::Value::Bool(_) => DataType::Boolean,
                _ => DataType::Utf8,
            }
        };
        field_map.insert(key.as_str(), dtype);
    }

    let fields: Vec<Field> = field_map
        .into_iter()
        .map(|(name, dtype)| Field::new(name, dtype, true))
        .collect();

    Ok(Schema::new(fields))
}

/// Build a RecordBatch from a list of JSON values using the given schema.
fn build_record_batch(
    schema: &Arc<Schema>,
    records: &[serde_json::Value],
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let num_rows = records.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let name = field.name().as_str();
        let array: ArrayRef = match field.data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let mut builder = TimestampMicrosecondBuilder::with_capacity(num_rows);
                for record in records {
                    match record.get(name).and_then(|v| v.as_str()) {
                        Some(s) => {
                            if let Some(us) = parse_timestamp_to_micros(s) {
                                builder.append_value(us);
                            } else {
                                builder.append_null();
                            }
                        }
                        None => builder.append_null(),
                    }
                }
                let arr = builder.finish().with_timezone("UTC");
                Arc::new(arr)
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for record in records {
                    match record.get(name) {
                        Some(serde_json::Value::Number(n)) => {
                            builder.append_value(n.as_f64().unwrap_or(f64::NAN));
                        }
                        Some(serde_json::Value::String(s)) => {
                            if let Ok(v) = s.parse::<f64>() {
                                builder.append_value(v);
                            } else {
                                builder.append_null();
                            }
                        }
                        Some(serde_json::Value::Null) | None => builder.append_null(),
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(num_rows);
                for record in records {
                    match record.get(name) {
                        Some(serde_json::Value::Number(n)) => {
                            if let Some(i) = n.as_i64() {
                                builder.append_value(i);
                            } else if let Some(f) = n.as_f64() {
                                builder.append_value(f as i64);
                            } else {
                                builder.append_null();
                            }
                        }
                        Some(serde_json::Value::String(s)) => {
                            if let Ok(v) = s.parse::<i64>() {
                                builder.append_value(v);
                            } else {
                                builder.append_null();
                            }
                        }
                        Some(serde_json::Value::Null) | None => builder.append_null(),
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(num_rows);
                for record in records {
                    match record.get(name) {
                        Some(serde_json::Value::Bool(b)) => builder.append_value(*b),
                        Some(serde_json::Value::Null) | None => builder.append_null(),
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Utf8 | _ => {
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
                for record in records {
                    match record.get(name) {
                        Some(serde_json::Value::String(s)) => builder.append_value(s),
                        Some(serde_json::Value::Null) | None => builder.append_null(),
                        Some(v) => builder.append_value(v.to_string()),
                    }
                }
                Arc::new(builder.finish())
            }
        };
        columns.push(array);
    }

    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    Ok(batch)
}

/// Parse various timestamp string formats to microseconds since epoch.
fn parse_timestamp_to_micros(s: &str) -> Option<i64> {
    use chrono::{DateTime, NaiveDateTime};

    // Try ISO 8601 with timezone
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp_micros());
    }

    // Try ISO 8601 variants (Python format: "2025-06-15T12:30:45.123456+00:00")
    if let Ok(dt) = DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f%:z") {
        return Some(dt.timestamp_micros());
    }

    // Try without timezone (assume UTC)
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
        return Some(dt.and_utc().timestamp_micros());
    }

    // Try "YYYY-MM-DD HH:MM:SS.fff" format
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        return Some(dt.and_utc().timestamp_micros());
    }

    // Try millisecond epoch
    if let Ok(ms) = s.parse::<i64>() {
        return Some(ms * 1000);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_timestamp_rfc3339() {
        let ts = parse_timestamp_to_micros("2025-06-15T12:30:45.123456+00:00");
        assert!(ts.is_some());
    }

    #[test]
    fn test_parse_timestamp_naive() {
        let ts = parse_timestamp_to_micros("2025-06-15T12:30:45.123456");
        assert!(ts.is_some());
    }

    #[test]
    fn test_infer_schema() {
        let record = serde_json::json!({
            "timestamp": "2025-06-15T12:30:45.123456+00:00",
            "sym": "BTCJPY",
            "price": 100.5,
            "amount": 10,
            "venue": "gmo"
        });
        let schema = infer_schema(&record).unwrap();
        assert_eq!(schema.fields().len(), 5);

        // timestamp should be Timestamp type
        let ts_field = schema.field_with_name("timestamp").unwrap();
        assert!(matches!(
            ts_field.data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, _)
        ));

        // price should be Float64
        let price_field = schema.field_with_name("price").unwrap();
        assert_eq!(price_field.data_type(), &DataType::Float64);
    }

    #[test]
    fn test_batch_writer_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");

        let mut writer = BatchParquetWriter::new(path.clone(), 2);

        writer
            .push(serde_json::json!({
                "_data_type": "Rate",
                "timestamp": "2025-06-15T12:30:45.123456+00:00",
                "sym": "BTCJPY",
                "price": 100.5
            }))
            .unwrap();

        writer
            .push(serde_json::json!({
                "_data_type": "Rate",
                "timestamp": "2025-06-15T12:30:46.000000+00:00",
                "sym": "BTCJPY",
                "price": 101.0
            }))
            .unwrap();

        writer
            .push(serde_json::json!({
                "_data_type": "Rate",
                "timestamp": "2025-06-15T12:30:47.000000+00:00",
                "sym": "BTCJPY",
                "price": 101.5
            }))
            .unwrap();

        let total = writer.finish().unwrap();
        assert_eq!(total, 3);
        assert!(path.exists());
    }
}
