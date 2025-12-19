use crate::config::ServiceConfig;
use opentelemetry::global;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
#[cfg(test)]
use serial_test::serial;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing_subscriber::{EnvFilter, Registry, fmt, layer::SubscriberExt, util::SubscriberInitExt};

// Phase 5.2: Structured logging support
/// Unique identifier for batch lifecycle tracking
#[allow(dead_code)]
pub struct BatchId(pub String);

impl BatchId {
    /// Generate a new batch ID based on timestamp and counter
    #[allow(dead_code)]
    pub fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let counter = COUNTER.fetch_add(1, Ordering::SeqCst);
        BatchId(format!("{}-{}", timestamp, counter))
    }

    /// Generate from pipeline name and timestamp for easier debugging
    #[allow(dead_code)]
    pub fn for_pipeline(pipeline_name: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let counter = COUNTER.fetch_add(1, Ordering::SeqCst);
        BatchId(format!("{}-{}-{}", pipeline_name, timestamp, counter))
    }
}

impl std::fmt::Display for BatchId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Correlation ID for tracing messages through entire pipeline
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct CorrelationId(pub String);

impl CorrelationId {
    /// Create from topic, partition, and offset
    #[allow(dead_code)]
    pub fn from_kafka(topic: &str, partition: i32, offset: i64) -> Self {
        CorrelationId(format!("{}:{}:{}", topic, partition, offset))
    }

    /// Create from batch and message index
    #[allow(dead_code)]
    pub fn from_batch(batch_id: &str, msg_index: usize) -> Self {
        CorrelationId(format!("batch:{}:msg:{}", batch_id, msg_index))
    }
}

impl std::fmt::Display for CorrelationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Logger context with sampling support for high-frequency logs
#[allow(dead_code)]
pub struct LogSampler {
    sample_rate: u64, // 1 in N messages
    counter: Arc<AtomicU64>,
}

impl LogSampler {
    /// Create sampler for 1-in-N messages (e.g., 100 = log 1%)
    #[allow(dead_code)]
    pub fn new(sample_rate: u64) -> Self {
        LogSampler {
            sample_rate,
            counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Check if this message should be logged
    #[allow(dead_code)]
    pub fn should_log(&self) -> bool {
        let count = self.counter.fetch_add(1, Ordering::Relaxed);
        count % self.sample_rate == 0
    }

    /// Reset counter for new phase
    #[allow(dead_code)]
    pub fn reset(&self) {
        self.counter.store(0, Ordering::Relaxed);
    }
}

// Helper macros for common structured logging patterns
#[macro_export]
macro_rules! log_batch_flush {
    ($pipeline_name:expr, $batch_id:expr, $reason:expr, $msg_count:expr) => {
        tracing::info!(
            pipeline_name = %$pipeline_name,
            batch_id = %$batch_id,
            reason = %$reason,
            message_count = $msg_count,
            "batch flushed"
        )
    };
}

#[macro_export]
macro_rules! log_slow_write {
    ($pipeline_name:expr, $batch_id:expr, $duration_ms:expr) => {
        tracing::warn!(
            pipeline_name = %$pipeline_name,
            batch_id = %$batch_id,
            duration_ms = $duration_ms,
            "slow batch write (>1000ms)"
        )
    };
}

#[macro_export]
macro_rules! log_with_correlation {
    ($correlation_id:expr, $pipeline_name:expr, $msg:expr) => {
        tracing::info!(
            correlation_id = %$correlation_id,
            pipeline_name = %$pipeline_name,
            $msg
        )
    };
}

pub struct TracingGuard {
    has_otlp: bool,
}

impl Drop for TracingGuard {
    fn drop(&mut self) {
        if self.has_otlp {
            opentelemetry::global::shutdown_tracer_provider();
        }
    }
}

/// Initializes global tracing, logging, and optional OTLP exporting based on the given configuration.
///
/// This sets the global W3C trace context propagator, configures a logging filter (from the
/// environment if present, otherwise from `cfg.log_level`), and installs a JSON-formatted
/// tracing/logging layer. If `cfg.otlp_endpoint` is set, an OTLP exporter is created and attached,
/// and the resulting tracer is shut down when the returned `TracingGuard` is dropped.
///
/// # Parameters
///
/// - `cfg`: configuration whose `log_level` determines the default logging filter and whose
///   optional `otlp_endpoint` enables OTLP exporting when present.
///
/// # Returns
///
/// A `TracingGuard` whose `_has_otlp` field is `true` if an OTLP exporter was configured, `false` otherwise.
///
/// # Examples
///
/// ```
/// // Construct a ServiceConfig with no OTLP endpoint (pseudo-code; adapt to your config type)
/// let cfg = ServiceConfig { log_level: "info".into(), otlp_endpoint: None };
/// let guard = init(&cfg).expect("failed to initialize tracing");
/// assert!(!guard._has_otlp);
/// ```
pub fn init(
    cfg: &ServiceConfig,
) -> Result<TracingGuard, Box<dyn std::error::Error + Send + Sync + 'static>> {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let default_directive = cfg.log_level.to_string();
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_directive));

    let fmt_layer = fmt::layer().with_target(false).json();

    let registry = Registry::default().with(filter).with(fmt_layer);

    let has_otlp = if let Some(endpoint) = &cfg.otlp_endpoint {
        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .tonic()
                    .with_endpoint(endpoint),
            )
            .install_batch(opentelemetry_sdk::runtime::Tokio)?;

        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
        registry.with(telemetry).try_init()?;
        true
    } else {
        registry.try_init()?;
        false
    };

    Ok(TracingGuard { has_otlp })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LogLevel;
    use crate::config::ServiceConfig;

    #[serial]
    #[test]
    fn test_init_no_otlp() {
        let cfg = ServiceConfig {
            log_level: LogLevel::Info,
            metrics_port: 9000,
            otlp_endpoint: None,
            health_check_timeout_ms: 5000,
            shutdown_timeout: "30s".to_string(),
        };

        let guard = init(&cfg).expect("init should succeed");
        // Verify that the guard was created successfully without OTLP
        // The has_otlp field is private and used only in Drop for cleanup
        drop(guard); // Verify drop completes without panic
    }
}
