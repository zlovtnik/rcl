use crate::config::{validate_table_identifier, PostgresConfig};
use crate::errors::{ProcessingError, TransportError, ValidationError};
use crate::health::{ComponentStatus, HealthRegistry};
use crate::metrics::Metrics;
use crate::types::Operation;
use backoff::future::retry;
use backoff::ExponentialBackoff;
use chrono::Utc;
use csv::Writer as CsvWriter;

use serde_json::Value;
use sqlx::postgres::{PgConnectOptions, PgConnection, PgPoolOptions, PgSslMode};
use sqlx::types::Json;
use sqlx::{PgPool, QueryBuilder};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

#[derive(Clone, Debug)]
pub struct RecordMeta {
    pub topic: String,
    pub partition: i64,
    pub offset: i64,
    pub ingest_ts: i64,
    pub operation: Operation,
    pub payload: Value,
}

impl RecordMeta {
    pub fn extract(val: &Value) -> Self {
        let topic = val
            .get("_meta_topic")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let partition = val
            .get("_meta_partition")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let offset = val
            .get("_meta_offset")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let ingest_ts = val
            .get("_meta_ingest_ts")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let operation = val
            .get("operation_type")
            .and_then(|v| v.as_str())
            .and_then(|s| Operation::try_from(s).ok())
            .unwrap_or(Operation::Create); // default, but should be present

        let mut payload = val.clone();
        if let Value::Object(ref mut map) = payload {
            map.remove("_meta_topic");
            map.remove("_meta_partition");
            map.remove("_meta_offset");
            map.remove("_meta_ingest_ts");
        }

        Self {
            topic,
            partition,
            offset,
            ingest_ts,
            operation,
            payload,
        }
    }
}

#[derive(Clone)]
pub struct Writer {
    pool: PgPool,
    cfg: Arc<PostgresConfig>,
    metrics: Metrics,
    health: Arc<HealthRegistry>,
}

impl Writer {
    pub async fn new(
        cfg: Arc<PostgresConfig>,
        metrics: Metrics,
        health: Arc<HealthRegistry>,
    ) -> Result<Self, ProcessingError> {
        let mut url = cfg.url.clone();
        if let Some(root_cert) = &cfg.ssl_root_cert {
            if !url.contains("sslrootcert=") {
                let separator = if url.contains('?') { "&" } else { "?" };
                // Encode spaces in the cert path for URL safety
                let encoded_cert = root_cert.replace(' ', "%20");
                url.push_str(&format!("{}sslrootcert={}", separator, encoded_cert));
            }
        }

        let mut options = PgConnectOptions::from_str(&url).map_err(|e| {
            ProcessingError::from(ValidationError::new(format!("invalid pg url: {}", e)))
        })?;

        if let Some(mode) = &cfg.ssl_mode {
            let valid_modes = [
                "disable",
                "allow",
                "prefer",
                "require",
                "verify-ca",
                "verify-full",
            ];
            let lower_mode = mode.to_lowercase();
            if !valid_modes.contains(&lower_mode.as_str()) {
                warn!("invalid ssl_mode '{}', expected one of: disable, allow, prefer, require, verify-ca, verify-full; defaulting to 'prefer'", mode);
            }
            let ssl_mode = match lower_mode.as_str() {
                "disable" => PgSslMode::Disable,
                "allow" => PgSslMode::Allow,
                "prefer" => PgSslMode::Prefer,
                "require" => PgSslMode::Require,
                "verify-ca" => PgSslMode::VerifyCa,
                "verify-full" => PgSslMode::VerifyFull,
                _ => PgSslMode::Prefer,
            };
            options = options.ssl_mode(ssl_mode);
        }

        let max_connections = cfg.pool.as_ref().map(|p| p.max_connections).unwrap_or(10);
        let acquire_timeout = cfg
            .pool
            .as_ref()
            .map(|p| Duration::from_millis(p.acquire_timeout_ms))
            .unwrap_or(Duration::from_secs(5));

        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(acquire_timeout)
            .connect_with(options)
            .await
            .map_err(|e| {
                let _ = health.set_postgres_status(ComponentStatus::Unhealthy);
                ProcessingError::from(TransportError::new("pg connect", e))
            })?;

        let _ = health.set_postgres_status(ComponentStatus::Healthy);

        Ok(Self {
            pool,
            cfg,
            metrics,
            health,
        })
    }

    pub async fn write(
        &self,
        value: &Value,
        table: &str,
        pipeline_name: &str,
    ) -> Result<(), ProcessingError> {
        self.write_batch(std::slice::from_ref(value), table, pipeline_name)
            .await
    }

    #[tracing::instrument(skip(self, values, table), fields(batch_size = values.len()))]
    pub async fn write_batch(
        &self,
        values: &[Value],
        table: &str,
        pipeline_name: &str,
    ) -> Result<(), ProcessingError> {
        self.metrics.batch_size.observe(values.len() as f64);
        let timer = self.metrics.write_latency_seconds.start_timer();

        // Log operations for debugging
        for val in values {
            let meta = RecordMeta::extract(val);
            tracing::trace!(operation = %meta.operation, "processing operation");
        }

        let op = || async {
            let res = if self.cfg.copy_enabled {
                let mut conn = self
                    .pool
                    .acquire()
                    .await
                    .map_err(|e| ProcessingError::from(TransportError::new("pg acquire", e)))?;

                match copy_into(&mut conn, table, values).await {
                    Ok(_) => Ok(()),
                    Err(err) => {
                        warn!(table = %table, error = %err, "copy failed, falling back to insert");
                        insert_batch(&self.pool, table, values).await
                    }
                }
            } else {
                insert_batch(&self.pool, table, values).await
            };

            res.map_err(|e| {
                if e.is_retryable() {
                    backoff::Error::transient(e)
                } else {
                    backoff::Error::permanent(e)
                }
            })
        };

        let backoff = ExponentialBackoff::default();
        let res = retry(backoff, op).await;

        timer.observe_duration();

        match &res {
            Ok(_) => {
                let _ = self.health.set_postgres_status(ComponentStatus::Healthy);
                let _ = self.health.update_pipeline_success(pipeline_name);
            }
            Err(e) => {
                if let Err(err) = self
                    .health
                    .update_pipeline_error(pipeline_name, e.to_string())
                {
                    warn!("Failed to update pipeline error status: {}", err);
                }
                if !e.is_retryable() {
                    let _ = self.health.set_postgres_status(ComponentStatus::Unhealthy);
                }
            }
        }

        res
    }
}

async fn copy_into(
    conn: &mut PgConnection,
    table: &str,
    values: &[Value],
) -> Result<(), ProcessingError> {
    // Validate table name to prevent SQL injection
    validate_table_identifier(table)
        .map_err(|e| ProcessingError::Validation(ValidationError::new(e.to_string())))?;

    // Escape any double quotes in table name and wrap in quotes for safe SQL identifier
    let quoted_table = format!("\"{}\"", table.replace('"', "\"\""));
    let copy_sql = format!(
        "COPY {} (payload, ingest_system_time, _meta_topic, _meta_partition, _meta_offset, _meta_ingest_ts) FROM STDIN WITH (FORMAT csv)",
        quoted_table
    );
    let mut writer = conn
        .copy_in_raw(&copy_sql)
        .await
        .map_err(|e| ProcessingError::from(TransportError::new("copy start", e)))?;

    for val in values {
        let meta = RecordMeta::extract(val);

        let json = serde_json::to_string(&meta.payload)
            .map_err(|e| ProcessingError::from(TransportError::new("json encode", e)))?;
        let escaped_json = json.replace('"', "\"\"");
        let mut wtr = CsvWriter::from_writer(vec![]);
        wtr.write_record(&[
            &escaped_json,
            &Utc::now().timestamp_millis().to_string(),
            &meta.topic,
            &meta.partition.to_string(),
            &meta.offset.to_string(),
            &meta.ingest_ts.to_string(),
        ])
        .map_err(|e| ProcessingError::Validation(ValidationError::new(format!("csv write: {}", e))))?;
        let csv_data = wtr
            .into_inner()
            .map_err(|e| ProcessingError::Validation(ValidationError::new(format!("csv flush: {}", e))))?;
        let line = String::from_utf8(csv_data)
            .map_err(|e| ProcessingError::Validation(ValidationError::new(format!("utf8 decode: {}", e))))?
            + "\n";
        writer
            .send(line.into_bytes())
            .await
            .map_err(|e| ProcessingError::from(TransportError::new("copy send", e)))?;
    }

    writer
        .finish()
        .await
        .map_err(|e| ProcessingError::from(TransportError::new("copy finish", e)))?;
    Ok(())
}

async fn insert_batch(pool: &PgPool, table: &str, values: &[Value]) -> Result<(), ProcessingError> {
    if values.is_empty() {
        return Ok(());
    }

    // Validate table name to prevent SQL injection
    validate_table_identifier(table)
        .map_err(|e| ProcessingError::Validation(ValidationError::new(e.to_string())))?;

    // Escape any double quotes in table name and wrap in quotes for safe SQL identifier
    let quoted_table = format!("\"{}\"", table.replace('"', "\"\""));
    let mut builder = QueryBuilder::new(format!(
        "INSERT INTO {} (payload, ingest_system_time, _meta_topic, _meta_partition, _meta_offset, _meta_ingest_ts) VALUES ",
        quoted_table
    ));

    builder.push_values(values, |mut b, val| {
        let meta = RecordMeta::extract(val);

        b.push_bind(Json(meta.payload));
        b.push_bind(Utc::now());
        b.push_bind(meta.topic);
        b.push_bind(meta.partition);
        b.push_bind(meta.offset);
        b.push_bind(meta.ingest_ts);
    });

    let query = builder.build();
    query
        .execute(pool)
        .await
        .map_err(|e| ProcessingError::from(TransportError::new("insert", e)))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_validation_valid_names() {
        // Valid single table names
        assert!(validate_table_identifier("users").is_ok());
        assert!(validate_table_identifier("user_data").is_ok());
        assert!(validate_table_identifier("_private_table").is_ok());
        assert!(validate_table_identifier("table123").is_ok());

        // Valid schema.table names
        assert!(validate_table_identifier("public.users").is_ok());
        assert!(validate_table_identifier("staging.orders").is_ok());
        assert!(validate_table_identifier("schema_name.table_name").is_ok());

        // Edge cases that should be valid
        assert!(validate_table_identifier("a.b").is_ok()); // single character parts
        assert!(validate_table_identifier("_._").is_ok()); // underscores
    }

    #[test]
    fn test_table_validation_invalid_names() {
        // Empty string
        assert!(validate_table_identifier("").is_err());

        // Invalid characters
        assert!(validate_table_identifier("user-data").is_err()); // dash not allowed
        assert!(validate_table_identifier("user data").is_err()); // space not allowed
        assert!(validate_table_identifier("user;data").is_err()); // semicolon injection attempt
        assert!(validate_table_identifier("user--data").is_err()); // SQL comment attempt
        assert!(validate_table_identifier("user' OR '1'='1").is_err()); // SQL injection attempt

        // Invalid starting characters
        assert!(validate_table_identifier("123table").is_err()); // starts with number
        assert!(validate_table_identifier("-table").is_err()); // starts with dash

        // Too many dots
        assert!(validate_table_identifier("a.b.c").is_err()); // more than one dot

        // Empty parts
        assert!(validate_table_identifier(".table").is_err()); // empty schema
        assert!(validate_table_identifier("schema.").is_err()); // empty table

        // Names too long (over 63 characters)
        let long_name = "a".repeat(64);
        assert!(validate_table_identifier(&long_name).is_err());
    }

    #[test]
    fn test_copy_into_validation_rejection() {
        // This test verifies that copy_into rejects invalid table names
        // We can't fully test the async function without a database connection,
        // but we can test that validation happens early by checking the error type

        // Create a mock connection that would fail anyway, but validation should happen first
        // Since we can't easily create a PgConnection without a database, we'll test the validation
        // separately. The integration test below would need a test database.

        // Test that validation function works as expected
        assert!(validate_table_identifier("users; DROP TABLE users;--").is_err());
        assert!(validate_table_identifier("users' OR '1'='1").is_err());
    }

    #[test]
    fn test_insert_batch_validation_rejection() {
        // Similar to copy_into test - validation should prevent SQL injection

        assert!(validate_table_identifier("users; DROP TABLE users;--").is_err());
        assert!(validate_table_identifier("users' OR '1'='1").is_err());
    }
}
