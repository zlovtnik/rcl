use crate::eip::{BackpressureConfig, DlqConfig, PipelineConfig};
use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::env;
use std::fs;
use std::path::Path;
use tracing::Level;
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    pub fn as_tracing_level(&self) -> Level {
        match self {
            LogLevel::Debug => Level::DEBUG,
            LogLevel::Info => Level::INFO,
            LogLevel::Warn => Level::WARN,
            LogLevel::Error => Level::ERROR,
        }
    }
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            LogLevel::Debug => "debug",
            LogLevel::Info => "info",
            LogLevel::Warn => "warn",
            LogLevel::Error => "error",
        };

        f.write_str(s)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ServiceConfig {
    pub log_level: LogLevel,
    pub metrics_port: u16,
    pub otlp_endpoint: Option<String>,
    #[serde(default = "default_health_check_timeout_ms")]
    pub health_check_timeout_ms: u64,
    #[serde(default = "default_shutdown_timeout")]
    pub shutdown_timeout: String,
}

fn default_health_check_timeout_ms() -> u64 {
    5000
}

fn default_shutdown_timeout() -> String {
    "30s".to_string()
}

impl ServiceConfig {
    pub fn shutdown_timeout_duration(&self) -> std::time::Duration {
        parse_duration(&self.shutdown_timeout).unwrap_or(std::time::Duration::from_secs(30))
    }
}

fn parse_duration(s: &str) -> Result<std::time::Duration, std::num::ParseIntError> {
    if s.ends_with('s') {
        let secs: u64 = s.trim_end_matches('s').parse()?;
        Ok(std::time::Duration::from_secs(secs))
    } else {
        // fallback to seconds if no suffix
        let secs: u64 = s.parse()?;
        Ok(std::time::Duration::from_secs(secs))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KafkaSecurityConfig {
    pub tls: bool,
    pub sasl_enabled: bool,
    pub sasl_mechanism: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
    pub ssl_ca_location: Option<String>,
    pub ssl_certificate_location: Option<String>,
    pub ssl_key_location: Option<String>,
    pub ssl_key_password: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KafkaFetchConfig {
    pub max_bytes: i32,
    pub max_wait_ms: i32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KafkaConfig {
    pub brokers: String,
    pub group_id: String,
    #[serde(default)]
    pub security: Option<KafkaSecurityConfig>,
    #[serde(default)]
    pub fetch: Option<KafkaFetchConfig>,
    pub session_timeout_ms: u32,
    pub max_inflight_messages: usize,
    pub producer_retries: i32,
    pub dlq_message_timeout_ms: u32,
    pub compression: String,
    #[serde(default = "default_dlq_readiness_timeout_secs")]
    pub dlq_readiness_timeout_secs: u64,
    #[serde(default = "default_dlq_readiness_backoff_secs")]
    pub dlq_readiness_backoff_secs: u64,
    #[serde(default = "default_staleness_threshold_seconds")]
    pub staleness_threshold_seconds: u64,
}

fn default_dlq_readiness_timeout_secs() -> u64 {
    30
}

fn default_dlq_readiness_backoff_secs() -> u64 {
    1
}

fn default_staleness_threshold_seconds() -> u64 {
    60
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PostgresPoolConfig {
    pub max_connections: u32,
    pub acquire_timeout_ms: u64,
}

impl PostgresPoolConfig {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.max_connections > 0,
            "max_connections must be greater than 0"
        );
        ensure!(
            self.acquire_timeout_ms > 0,
            "acquire_timeout_ms must be greater than 0"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PostgresConfig {
    pub url: String,
    pub ssl_mode: Option<String>,
    pub ssl_root_cert: Option<String>,
    #[serde(default)]
    pub pool: Option<PostgresPoolConfig>,
    pub copy_enabled: bool,
    pub copy_batch_rows: usize,
    pub insert_batch_rows: usize,
}

impl PostgresConfig {
    pub fn validate(&self) -> Result<()> {
        if let Some(ref pool) = self.pool {
            pool.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    pub service: ServiceConfig,
    pub kafka: KafkaConfig,
    pub postgres: PostgresConfig,
    pub pipelines: Vec<PipelineConfig>,
}

impl Config {
    /// Load configuration from the path pointed to by the `RCL_CONFIG_PATH` env var.
    pub fn from_env() -> Result<Self> {
        let path = env::var("RCL_CONFIG_PATH")
            .context("RCL_CONFIG_PATH env var is required to load configuration")?;

        Self::from_file(path)
    }

    /// Load configuration from a JSON file, applying environment overrides where present.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let raw = fs::read_to_string(path_ref)
            .with_context(|| format!("failed to read config file at {}", path_ref.display()))?;

        let mut cfg: Self = serde_json::from_str(&raw)
            .with_context(|| format!("failed to parse config file at {}", path_ref.display()))?;

        if let Ok(brokers) = env::var("RCL_KAFKA_BROKERS") {
            if !brokers.trim().is_empty() {
                cfg.kafka.brokers = brokers;
            }
        }

        if let Ok(group_id) = env::var("RCL_KAFKA_GROUP_ID") {
            if !group_id.trim().is_empty() {
                cfg.kafka.group_id = group_id;
            }
        }

        if let Ok(pg_url) = env::var("RCL_POSTGRES_URL") {
            if !pg_url.trim().is_empty() {
                cfg.postgres.url = pg_url;
            }
        }

        // Kafka SASL secrets
        if let Ok(sasl_user) = env::var("RCL_KAFKA_SASL_USERNAME") {
            if let Some(sec) = &mut cfg.kafka.security {
                sec.sasl_username = Some(sasl_user);
            }
        }
        if let Ok(sasl_pass) = env::var("RCL_KAFKA_SASL_PASSWORD") {
            if let Some(sec) = &mut cfg.kafka.security {
                sec.sasl_password = Some(sasl_pass);
            }
        }

        cfg.validate().with_context(|| {
            format!(
                "configuration loaded from {} is invalid",
                path_ref.display()
            )
        })?;

        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        self.postgres.validate()?;

        ensure!(
            self.service.metrics_port > 0,
            "service.metrics_port must be greater than zero"
        );

        ensure!(
            self.service.health_check_timeout_ms > 0,
            "health_check_timeout_ms must be greater than 0"
        );

        ensure!(
            !self.kafka.brokers.trim().is_empty(),
            "kafka.brokers is required (set via config file or RCL_KAFKA_BROKERS env)"
        );
        ensure!(
            !self.kafka.group_id.trim().is_empty(),
            "kafka.group_id is required (set via config file or RCL_KAFKA_GROUP_ID env)"
        );
        ensure!(
            self.kafka.session_timeout_ms > 0,
            "kafka.session_timeout_ms must be > 0"
        );
        ensure!(
            self.kafka.max_inflight_messages > 0,
            "kafka.max_inflight_messages must be > 0"
        );
        ensure!(
            self.kafka.producer_retries >= 0,
            "kafka.producer_retries must be >= 0"
        );
        ensure!(
            self.kafka.dlq_message_timeout_ms > 0,
            "kafka.dlq_message_timeout_ms must be > 0"
        );
        ensure!(
            !self.kafka.compression.trim().is_empty(),
            "kafka.compression is required (e.g. lz4, snappy, gzip)"
        );

        if let Some(fetch) = &self.kafka.fetch {
            ensure!(fetch.max_bytes > 0, "kafka.fetch.max_bytes must be > 0");
            ensure!(fetch.max_wait_ms > 0, "kafka.fetch.max_wait_ms must be > 0");
        }

        if self.pipelines.is_empty() {
            bail!("at least one pipeline configuration is required");
        }

        let mut pipeline_names = HashSet::new();
        let mut topics = HashSet::new();

        for pipeline in &self.pipelines {
            ensure!(
                !pipeline.name.trim().is_empty(),
                "pipeline.name is required"
            );
            if !pipeline_names.insert(&pipeline.name) {
                bail!("duplicate pipeline name: {}", pipeline.name);
            }

            ensure!(
                !pipeline.topic.trim().is_empty(),
                "pipeline.topic is required"
            );
            if !topics.insert(&pipeline.topic) {
                bail!("duplicate topic across pipelines: {}", pipeline.topic);
            }

            validate_table_identifier(&pipeline.staging_table).with_context(|| {
                format!("invalid staging_table for pipeline '{}'", pipeline.name)
            })?;

            if let Some(dlq) = &pipeline.dlq {
                ensure!(
                    !dlq.topic.trim().is_empty(),
                    "pipeline.dlq.topic is required"
                );
                ensure!(dlq.max_retries > 0, "pipeline.dlq.max_retries must be > 0");
                ensure!(
                    dlq.max_payload_bytes > 0,
                    "pipeline.dlq.max_payload_bytes must be > 0"
                );
            }
            ensure!(
                pipeline.backpressure.channel_capacity > 0,
                "pipeline.backpressure.channel_capacity must be > 0"
            );

            if self.postgres.copy_batch_rows > pipeline.backpressure.channel_capacity {
                eprintln!(
                    "WARN: pipeline '{}': postgres.copy_batch_rows ({}) > backpressure.channel_capacity ({}). This may cause pipeline stalls.",
                    pipeline.name,
                    self.postgres.copy_batch_rows,
                    pipeline.backpressure.channel_capacity
                );
            }

            ensure!(
                !pipeline.required_fields.is_empty(),
                "pipeline.required_fields must not be empty"
            );
            ensure!(
                pipeline
                    .required_fields
                    .iter()
                    .all(|f| !f.trim().is_empty()),
                "pipeline.required_fields entries cannot be empty"
            );
        }

        Ok(())
    }

    /// Example config used for local development and as a scaffold default.
    pub fn example() -> Self {
        Self {
            service: ServiceConfig {
                log_level: LogLevel::Info,
                metrics_port: 9090,
                otlp_endpoint: None,
                health_check_timeout_ms: 5000,
                shutdown_timeout: "30s".to_string(),
            },
            kafka: KafkaConfig {
                brokers: "localhost:9092".to_string(),
                group_id: "rcloader-consumer".to_string(),
                security: None,
                fetch: Some(KafkaFetchConfig {
                    max_bytes: 5_242_880,
                    max_wait_ms: 500,
                }),
                session_timeout_ms: 45_000,
                max_inflight_messages: 500,
                producer_retries: 5,
                dlq_message_timeout_ms: 15_000,
                compression: "lz4".to_string(),
                dlq_readiness_timeout_secs: 30,
                dlq_readiness_backoff_secs: 1,
                staleness_threshold_seconds: 300,
            },
            postgres: PostgresConfig {
                url: "postgres://rcl:rcl@localhost:5432/warehouse".to_string(),
                ssl_mode: None,
                ssl_root_cert: None,
                pool: Some(PostgresPoolConfig {
                    max_connections: 10,
                    acquire_timeout_ms: 5000,
                }),
                copy_enabled: true,
                copy_batch_rows: 5_000,
                insert_batch_rows: 500,
            },
            pipelines: vec![PipelineConfig {
                name: "orders-cdc".to_string(),
                topic: "cdc.orders".to_string(),
                required_fields: vec![
                    "order_id".to_string(),
                    "op_ts".to_string(),
                    "operation_type".to_string(),
                ],
                debezium_envelope: true,
                staging_table: "stg_orders".to_string(),
                dlq: Some(DlqConfig {
                    topic: "dlq.orders".to_string(),
                    max_retries: 3,
                    max_payload_bytes: DlqConfig::DEFAULT_MAX_PAYLOAD_BYTES,
                }),
                backpressure: BackpressureConfig {
                    channel_capacity: 20_000,
                },
                stages: vec![],
            }],
        }
    }
}

pub fn validate_table_identifier(table: &str) -> Result<&str> {
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
        bail!("staging_table must not be empty");
    }

    let mut parts = table.split('.');
    let first = parts.next().unwrap_or("");
    let second = parts.next();

    if parts.next().is_some() {
        bail!("staging_table may contain at most one '.' separator");
    }

    if !valid_part(first) {
        bail!("staging_table must start with a letter or underscore, contain only alphanumerics/underscores, and be <= 63 characters");
    }

    if let Some(schema_or_table) = second {
        if !valid_part(schema_or_table) {
            bail!("staging_table schema/table parts must start with a letter or underscore, contain only alphanumerics/underscores, and be <= 63 characters");
        }
    }

    Ok(table)
}
