#![allow(clippy::collapsible_if)]
use crate::eip::PipelineConfig;
use crate::retry::RetryConfig;
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::env;
use std::fs;
use std::path::Path;
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl std::fmt::Display for LogLevel {
    /// Formats a `LogLevel` as its lowercase textual representation.
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::LogLevel;
    /// assert_eq!(format!("{}", LogLevel::Debug), "debug");
    /// assert_eq!(format!("{}", LogLevel::Info), "info");
    /// assert_eq!(format!("{}", LogLevel::Warn), "warn");
    /// assert_eq!(format!("{}", LogLevel::Error), "error");
    /// ```
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
    #[serde(default)]
    pub memory: MemoryConfig,
}

fn default_health_check_timeout_ms() -> u64 {
    5000
}

fn default_shutdown_timeout() -> String {
    "30s".to_string()
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MemoryConfig {
    #[serde(default = "default_max_memory_mb")]
    pub max_memory_mb: u64,
    #[serde(default = "default_memory_check_interval_ms")]
    pub memory_check_interval_ms: u64,
}

fn default_max_memory_mb() -> u64 {
    1024 // 1GB default
}

fn default_memory_check_interval_ms() -> u64 {
    1000 // 1 second
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            max_memory_mb: default_max_memory_mb(),
            memory_check_interval_ms: default_memory_check_interval_ms(),
        }
    }
}

fn default_copy_format() -> String {
    "csv".to_string()
}

fn default_copy_buffer_size() -> usize {
    65536 // 64KB
}

impl ServiceConfig {
    pub fn shutdown_timeout_duration(&self) -> std::time::Duration {
        parse_duration(&self.shutdown_timeout).unwrap_or(std::time::Duration::from_secs(30))
    }
}

/// Parse a duration string expressed in seconds.
///
/// Accepts a string with an optional trailing `s` suffix; interprets the numeric portion as seconds.
///
/// # Returns
///
/// A `Duration` representing the parsed number of seconds.
///
/// # Errors
///
/// Returns a `ParseIntError` if the numeric portion of the input cannot be parsed as an unsigned integer.
///
/// # Examples
///
/// ```
/// let d = parse_duration("45s").unwrap();
/// assert_eq!(d.as_secs(), 45);
///
/// let d2 = parse_duration("10").unwrap();
/// assert_eq!(d2.as_secs(), 10);
/// ```
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration_with_s() {
        let d = parse_duration("45s").unwrap();
        assert_eq!(d.as_secs(), 45);
    }

    #[test]
    fn test_parse_duration_plain_number() {
        let d = parse_duration("10").unwrap();
        assert_eq!(d.as_secs(), 10);
    }

    #[test]
    fn test_parse_duration_invalid() {
        assert!(parse_duration("1m").is_err());
    }

    #[test]
    fn test_service_shutdown_timeout_duration() {
        let svc = ServiceConfig {
            log_level: LogLevel::Info,
            metrics_port: 9000,
            otlp_endpoint: None,
            health_check_timeout_ms: 5000,
            shutdown_timeout: "60s".to_string(),
            memory: MemoryConfig::default(),
        };
        let dur = svc.shutdown_timeout_duration();
        assert_eq!(dur.as_secs(), 60);
    }

    #[test]
    fn test_loglevel_display() {
        assert_eq!(LogLevel::Debug.to_string(), "debug");
        assert_eq!(LogLevel::Info.to_string(), "info");
        assert_eq!(LogLevel::Warn.to_string(), "warn");
        assert_eq!(LogLevel::Error.to_string(), "error");
    }

    #[test]
    fn test_loglevel_equality() {
        assert_eq!(LogLevel::Info, LogLevel::Info);
        assert_ne!(LogLevel::Info, LogLevel::Debug);
    }

    #[test]
    fn test_service_config_with_otlp_endpoint() {
        let svc = ServiceConfig {
            log_level: LogLevel::Debug,
            metrics_port: 8080,
            otlp_endpoint: Some("http://localhost:4317/".to_string()),
            health_check_timeout_ms: 3000,
            shutdown_timeout: "30s".to_string(),
            memory: MemoryConfig::default(),
        };
        assert_eq!(svc.metrics_port, 8080);
        assert!(svc.otlp_endpoint.is_some());
    }

    #[test]
    fn test_postgres_pool_config_validation_valid() {
        let cfg = PostgresPoolConfig {
            max_connections: 10,
            acquire_timeout_ms: 5000,
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_postgres_pool_config_validation_zero_connections() {
        let cfg = PostgresPoolConfig {
            max_connections: 0,
            acquire_timeout_ms: 5000,
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_postgres_pool_config_validation_zero_timeout() {
        let cfg = PostgresPoolConfig {
            max_connections: 10,
            acquire_timeout_ms: 0,
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_postgres_config_validation_with_pool() {
        let cfg = PostgresConfig {
            url: "postgres://localhost/db".to_string(),
            ssl_mode: None,
            ssl_root_cert: None,
            pool: Some(PostgresPoolConfig {
                max_connections: 10,
                acquire_timeout_ms: 5000,
            }),
            copy_enabled: true,
            copy_batch_rows: 1000,
            insert_batch_rows: 100,
            enable_offset_tracking: false,
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_postgres_config_validation_invalid_pool() {
        let cfg = PostgresConfig {
            url: "postgres://localhost/db".to_string(),
            ssl_mode: None,
            ssl_root_cert: None,
            pool: Some(PostgresPoolConfig {
                max_connections: 0,
                acquire_timeout_ms: 10000,
            }),
            copy_enabled: true,
            copy_batch_rows: 1000,
            insert_batch_rows: 100,
            enable_offset_tracking: false,
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_validate_table_identifier_simple() {
        assert!(validate_table_identifier("users").is_ok());
        assert!(validate_table_identifier("orders").is_ok());
        assert!(validate_table_identifier("_temp").is_ok());
    }

    /// Verifies that schema-qualified table identifiers with valid schema and table parts are accepted.
    ///
    /// This test asserts that identifiers like `"public.users"`, `"staging.orders"`, and `"schema_1.table_1"` pass validation.
    #[test]
    fn test_validate_table_identifier_with_schema() {
        assert!(validate_table_identifier("public.users").is_ok());
        assert!(validate_table_identifier("staging.orders").is_ok());
        assert!(validate_table_identifier("schema_1.table_1").is_ok());
    }

    #[test]
    fn test_validate_table_identifier_single_char() {
        assert!(validate_table_identifier("a").is_ok());
        assert!(validate_table_identifier("_").is_ok());
        assert!(validate_table_identifier("a.b").is_ok());
    }

    #[test]
    fn test_validate_table_identifier_empty() {
        assert!(validate_table_identifier("").is_err());
    }

    #[test]
    fn test_validate_table_identifier_invalid_start() {
        assert!(validate_table_identifier("1users").is_err());
        assert!(validate_table_identifier("-table").is_err());
    }

    #[test]
    fn test_validate_table_identifier_invalid_chars() {
        assert!(validate_table_identifier("user-data").is_err());
        assert!(validate_table_identifier("user data").is_err());
        assert!(validate_table_identifier("user;data").is_err());
        assert!(validate_table_identifier("user'data").is_err());
    }

    #[test]
    fn test_validate_table_identifier_too_many_dots() {
        assert!(validate_table_identifier("a.b.c").is_err());
    }

    #[test]
    fn test_validate_table_identifier_empty_parts() {
        assert!(validate_table_identifier(".table").is_err());
        assert!(validate_table_identifier("schema.").is_err());
    }

    #[test]
    fn test_validate_table_identifier_too_long() {
        let long = "a".repeat(64);
        assert!(validate_table_identifier(&long).is_err());

        let long_with_schema = format!("a.{}", "b".repeat(64));
        assert!(validate_table_identifier(&long_with_schema).is_err());
    }

    #[test]
    fn test_validate_table_identifier_sql_injection_attempts() {
        assert!(validate_table_identifier("users; DROP TABLE users;--").is_err());
        assert!(validate_table_identifier("users' OR '1'='1").is_err());
        assert!(validate_table_identifier("users/**/").is_err());
    }

    #[test]
    fn test_config_validation_empty_pipelines() {
        let mut cfg = create_valid_config();
        cfg.pipelines.clear();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validation_empty_pipeline_name() {
        let mut cfg = create_valid_config();
        cfg.pipelines[0].name = "".to_string();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validation_duplicate_pipeline_names() {
        let mut cfg = create_valid_config();
        cfg.pipelines.push(cfg.pipelines[0].clone());
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validation_empty_pipeline_topic() {
        let mut cfg = create_valid_config();
        cfg.pipelines[0].topic = "".to_string();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validation_duplicate_topics() {
        let mut cfg = create_valid_config();
        let mut pipeline2 = cfg.pipelines[0].clone();
        pipeline2.name = "pipeline2".to_string();
        cfg.pipelines.push(pipeline2);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validation_invalid_staging_table() {
        let mut cfg = create_valid_config();
        cfg.pipelines[0].staging_table = "123invalid".to_string();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validation_zero_metrics_port() {
        let mut cfg = create_valid_config();
        cfg.service.metrics_port = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validation_zero_health_check_timeout() {
        let mut cfg = create_valid_config();
        cfg.service.health_check_timeout_ms = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validation_empty_kafka_brokers() {
        let mut cfg = create_valid_config();
        cfg.kafka.brokers = "".to_string();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validation_empty_kafka_group_id() {
        let mut cfg = create_valid_config();
        cfg.kafka.group_id = "".to_string();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validation_zero_session_timeout() {
        let mut cfg = create_valid_config();
        cfg.kafka.session_timeout_ms = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validation_zero_inflight_messages() {
        let mut cfg = create_valid_config();
        cfg.kafka.max_inflight_messages = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validation_negative_producer_retries() {
        let mut cfg = create_valid_config();
        cfg.kafka.producer_retries = -1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validation_zero_dlq_message_timeout() {
        let mut cfg = create_valid_config();
        cfg.kafka.dlq_message_timeout_ms = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validation_empty_compression() {
        let mut cfg = create_valid_config();
        cfg.kafka.compression = "".to_string();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validation_invalid_fetch_config() {
        let mut cfg = create_valid_config();
        cfg.kafka.fetch = Some(KafkaFetchConfig {
            max_bytes: 0,
            max_wait_ms: 100,
        });
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validation_dlq_empty_topic() {
        let mut cfg = create_valid_config();
        cfg.pipelines[0].dlq = Some(crate::eip::DlqConfig {
            topic: "".to_string(),
            max_retries: 3,
            max_payload_bytes: 1_000_000,
        });
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validation_dlq_zero_retries() {
        let mut cfg = create_valid_config();
        cfg.pipelines[0].dlq = Some(crate::eip::DlqConfig {
            topic: "dlq".to_string(),
            max_retries: 0,
            max_payload_bytes: 1_000_000,
        });
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validation_dlq_zero_payload_bytes() {
        let mut cfg = create_valid_config();
        cfg.pipelines[0].dlq = Some(crate::eip::DlqConfig {
            topic: "dlq".to_string(),
            max_retries: 3,
            max_payload_bytes: 0,
        });
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validation_zero_channel_capacity() {
        let mut cfg = create_valid_config();
        cfg.pipelines[0].backpressure.channel_capacity = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validation_empty_required_fields() {
        let mut cfg = create_valid_config();
        cfg.pipelines[0].required_fields.clear();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validation_blank_required_field() {
        let mut cfg = create_valid_config();
        cfg.pipelines[0].required_fields = vec!["   ".to_string()];
        assert!(cfg.validate().is_err());
    }

    // Helper to create a valid config for validation tests
    /// Creates a pre-filled, valid Config configured for tests.
    ///
    /// The returned Config is populated with sensible defaults for ServiceConfig, KafkaConfig, PostgresConfig (including a PostgresPoolConfig), and a single PipelineConfig named "test-pipeline"; it is intended to pass `Config::validate`.
    ///
    /// # Examples
    ///
    /// ```
    /// let cfg = create_valid_config();
    /// assert!(cfg.validate().is_ok());
    /// assert_eq!(cfg.pipelines.len(), 1);
    /// ```
    fn create_valid_config() -> Config {
        Config {
            service: ServiceConfig {
                log_level: LogLevel::Info,
                metrics_port: 9090,
                otlp_endpoint: None,
                health_check_timeout_ms: 5000,
                shutdown_timeout: "30s".to_string(),
                memory: MemoryConfig::default(),
            },
            kafka: KafkaConfig {
                brokers: "localhost:9092".to_string(),
                group_id: "test-group".to_string(),
                security: None,
                fetch: None,
                session_timeout_ms: 45000,
                max_inflight_messages: 500,
                producer_retries: 3,
                dlq_message_timeout_ms: 15000,
                compression: "lz4".to_string(),
                dlq_readiness_timeout_secs: 30,
                dlq_readiness_backoff_secs: 1,
                staleness_threshold_seconds: 300,
            },
            postgres: PostgresConfig {
                url: "postgres://localhost/test".to_string(),
                ssl_mode: None,
                ssl_root_cert: None,
                pool: Some(PostgresPoolConfig {
                    max_connections: 10,
                    acquire_timeout_ms: 5000,
                }),
                copy_enabled: true,
                copy_batch_rows: 5000,
                insert_batch_rows: 500,
                enable_offset_tracking: false,
            },
            retry: RetryConfig::default(),
            pipelines: vec![PipelineConfig {
                name: "test-pipeline".to_string(),
                topic: "test-topic".to_string(),
                debezium_envelope: false,
                staging_table: "test_table".to_string(),
                dlq: None,
                stages: vec![],
                required_fields: vec!["id".to_string()],
                backpressure: crate::eip::BackpressureConfig {
                    channel_capacity: 20000,
                },
                batching: Default::default(),
                circuit_breaker: Default::default(),
                worker_threads: 1,
            }],
        }
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
    #[serde(default)]
    pub per_pipeline_pools: bool,
    pub copy_enabled: bool,
    #[serde(default = "default_copy_format")]
    pub copy_format: String,
    #[serde(default = "default_copy_buffer_size")]
    pub copy_buffer_size: usize,
    pub copy_batch_rows: usize,
    pub insert_batch_rows: usize,
    /// Enable offset tracking for exactly-once semantics (default: false).
    ///
    /// When enabled, the application persists Kafka partition offsets to the database alongside
    /// data writes, enabling recovery from the last successfully processed offset on restart.
    /// This provides exactly-once delivery guarantees by tracking progress per (pipeline, topic, partition).
    ///
    /// **When to enable:**
    /// - Require exactly-once semantics and cannot tolerate message replay on crash/restart
    /// - Using long-running pipelines with potential recovery scenarios
    /// - Need to preserve offset state across service restarts
    ///
    /// **Performance implications:**
    /// - Minimal CPU overhead (single SQL insert/update per batch)
    /// - Database disk I/O: Additional writes to offset_tracker table (1 row per partition per flush)
    /// - Memory: Negligible (offset_tracker maintains minimal state per pipeline)
    /// - Network: Single extra request per flush (batched with data writes in same transaction)
    ///
    /// **Operational notes:**
    /// - Requires offset_tracker table in Postgres database (created during init)
    /// - Offsets persisted atomically with data writes (same transaction)
    /// - If disabled: offsets tracked only in Kafka group state (at-least-once semantics)
    /// - Query `SELECT * FROM offset_tracker` for debugging or monitoring offset state
    #[serde(default)]
    pub enable_offset_tracking: bool,
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
    #[serde(default)]
    pub retry: RetryConfig,
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

        // Validate retry configuration
        self.retry
            .validate()
            .map_err(|e| anyhow::anyhow!(e))
            .context("retry config invalid")?;

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
        bail!(
            "staging_table must start with a letter or underscore, contain only alphanumerics/underscores, and be <= 63 characters"
        );
    }

    if let Some(schema_or_table) = second {
        if !valid_part(schema_or_table) {
            bail!(
                "staging_table schema/table parts must start with a letter or underscore, contain only alphanumerics/underscores, and be <= 63 characters"
            );
        }
    }

    Ok(table)
}
