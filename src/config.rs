use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::{env, fs, path::Path};
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
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KafkaSecurityConfig {
    pub tls: bool,
    pub sasl_enabled: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KafkaConfig {
    pub brokers: String,
    pub group_id: String,
    #[serde(default)]
    pub security: Option<KafkaSecurityConfig>,
    pub session_timeout_ms: u32,
    pub max_inflight_messages: usize,
    pub producer_retries: i32,
    pub dlq_message_timeout_ms: u32,
    pub compression: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DlqConfig {
    pub topic: String,
    pub max_retries: u16,
    #[serde(default = "DlqConfig::default_max_payload_bytes")]
    pub max_payload_bytes: usize,
}

impl DlqConfig {
    pub const DEFAULT_MAX_PAYLOAD_BYTES: usize = 1_000_000;

    pub fn default_max_payload_bytes() -> usize {
        Self::DEFAULT_MAX_PAYLOAD_BYTES
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BackpressureConfig {
    pub channel_capacity: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PipelineConfig {
    pub name: String,
    pub topic: String,
    pub required_fields: Vec<String>,
    pub debezium_envelope: bool,
    pub staging_table: String,
    pub dlq: Option<DlqConfig>,
    pub backpressure: BackpressureConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PostgresConfig {
    pub url: String,
    pub copy_enabled: bool,
    pub copy_batch_rows: usize,
    pub insert_batch_rows: usize,
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

        cfg.validate().with_context(|| {
            format!(
                "configuration loaded from {} is invalid",
                path_ref.display()
            )
        })?;

        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            self.service.metrics_port > 0,
            "service.metrics_port must be greater than zero"
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
        ensure!(self.kafka.producer_retries >= 0, "kafka.producer_retries must be >= 0");
        ensure!(
            self.kafka.dlq_message_timeout_ms > 0,
            "kafka.dlq_message_timeout_ms must be > 0"
        );
        ensure!(
            !self.kafka.compression.trim().is_empty(),
            "kafka.compression is required (e.g. lz4, snappy, gzip)"
        );

        if self.pipelines.is_empty() {
            bail!("at least one pipeline configuration is required");
        }

        for pipeline in &self.pipelines {
            ensure!(
                !pipeline.name.trim().is_empty(),
                "pipeline.name is required"
            );
            ensure!(
                !pipeline.topic.trim().is_empty(),
                "pipeline.topic is required"
            );
            if let Some(dlq) = &pipeline.dlq {
                ensure!(!dlq.topic.trim().is_empty(), "pipeline.dlq.topic is required");
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
            },
            kafka: KafkaConfig {
                brokers: "localhost:9092".to_string(),
                group_id: "rcloader-consumer".to_string(),
                security: None,
                session_timeout_ms: 45_000,
                max_inflight_messages: 500,
                producer_retries: 5,
                dlq_message_timeout_ms: 15_000,
                compression: "lz4".to_string(),
            },
            postgres: PostgresConfig {
                url: "postgres://rcl:rcl@localhost:5432/warehouse".to_string(),
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
            }],
        }
    }
}
