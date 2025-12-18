use crate::config::{PipelineConfig, PostgresConfig};
use crate::errors::{ProcessingError, TransportError, ValidationError};
use crate::metrics::Metrics;
use chrono::Utc;
use serde_json::Value;
use sqlx::postgres::{PgConnection, PgPoolOptions};
use sqlx::types::Json;
use sqlx::{PgPool, QueryBuilder};
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

#[derive(Clone)]
pub struct Writer {
    pool: PgPool,
    cfg: Arc<PostgresConfig>,
    metrics: Metrics,
}

impl Writer {
    pub async fn new(cfg: Arc<PostgresConfig>, metrics: Metrics) -> Result<Self, ProcessingError> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .acquire_timeout(Duration::from_secs(5))
            .connect(&cfg.url)
            .await
            .map_err(|e| ProcessingError::from(TransportError::new("pg connect", e)))?;

        Ok(Self { pool, cfg, metrics })
    }

    pub async fn write(
        &self,
        value: &Value,
        pipeline: &PipelineConfig,
    ) -> Result<(), ProcessingError> {
        self.write_batch(std::slice::from_ref(value), pipeline)
            .await
    }

    pub async fn write_batch(
        &self,
        values: &[Value],
        pipeline: &PipelineConfig,
    ) -> Result<(), ProcessingError> {
        let timer = self.metrics.write_latency_seconds.start_timer();
        let res = if self.cfg.copy_enabled {
            let mut conn = self
                .pool
                .acquire()
                .await
                .map_err(|e| ProcessingError::from(TransportError::new("pg acquire", e)))?;

            match copy_into(&mut conn, pipeline, values).await {
                Ok(_) => Ok(()),
                Err(err) => {
                    warn!(table = %pipeline.staging_table, error = %err, "copy failed, falling back to insert");
                    insert_batch(&self.pool, pipeline, values).await
                }
            }
        } else {
            insert_batch(&self.pool, pipeline, values).await
        };
        timer.observe_duration();
        res
    }
}

fn validate_table_identifier(table: &str) -> Result<&str, ProcessingError> {
    fn valid_part(part: &str) -> bool {
        if part.is_empty() || part.len() > 63 {
            return false;
        }

        let mut chars = part.chars();
        let first = match chars.next() {
            Some(c) => c,
            None => return false,
        };

        if !(first == '_' || first.is_ascii_alphabetic()) {
            return false;
        }

        chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
    }

    if table.is_empty() {
        return Err(ProcessingError::from(ValidationError::new(
            "staging_table must not be empty",
        )));
    }

    let mut parts = table.split('.');
    let first = parts.next().unwrap_or("");
    let second = parts.next();

    if parts.next().is_some() {
        return Err(ProcessingError::from(ValidationError::new(
            "staging_table may contain at most one '.' separator",
        )));
    }

    if !valid_part(first) {
        return Err(ProcessingError::from(ValidationError::new(
            "staging_table must start with a letter or underscore, contain only alphanumerics/underscores, and be <= 63 characters",
        )));
    }

    if let Some(schema_or_table) = second {
        if !valid_part(schema_or_table) {
            return Err(ProcessingError::from(ValidationError::new(
                "staging_table schema/table parts must start with a letter or underscore, contain only alphanumerics/underscores, and be <= 63 characters",
            )));
        }
    }

    Ok(table)
}

async fn copy_into(
    conn: &mut PgConnection,
    pipeline: &PipelineConfig,
    values: &[Value],
) -> Result<(), ProcessingError> {
    let staging_table = validate_table_identifier(&pipeline.staging_table)?;
    let copy_sql = format!(
        "COPY {} (payload, ingest_system_time) FROM STDIN WITH (FORMAT csv)",
        staging_table
    );
    let mut writer = conn
        .copy_in_raw(&copy_sql)
        .await
        .map_err(|e| ProcessingError::from(TransportError::new("copy start", e)))?;

    for val in values {
        let json = serde_json::to_string(val)
            .map_err(|e| ProcessingError::from(TransportError::new("json encode", e)))?;
        let escaped_json = json.replace('"', "\"\"");
        let line = format!("\"{}\",{}\n", escaped_json, Utc::now());
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

async fn insert_batch(
    pool: &PgPool,
    pipeline: &PipelineConfig,
    values: &[Value],
) -> Result<(), ProcessingError> {
    if values.is_empty() {
        return Ok(());
    }

    let staging_table = validate_table_identifier(&pipeline.staging_table)?;

    let mut builder = QueryBuilder::new(format!(
        "INSERT INTO {} (payload, ingest_system_time) VALUES ",
        staging_table
    ));

    builder.push_values(values, |mut b, val| {
        b.push_bind(Json(val));
        b.push_bind(Utc::now());
    });

    let query = builder.build();
    query
        .execute(pool)
        .await
        .map_err(|e| ProcessingError::from(TransportError::new("insert", e)))?;
    Ok(())
}
