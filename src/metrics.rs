use crate::health::{ComponentStatus, HealthRegistry};
use anyhow::Result;
use axum::{Router, routing::get};
use prometheus::{
    Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::error;

/// Circuit breaker state encodings for metrics reporting.
#[allow(dead_code)]
mod circuit_breaker_states {
    /// Circuit breaker is Closed (normal operation, requests allowed).
    pub const STATE_CLOSED: i64 = 0;
    /// Circuit breaker is Open (fast-fail, requests rejected without attempting downstream).
    pub const STATE_OPEN: i64 = 1;
    /// Circuit breaker is Half-Open (testing recovery with limited trial requests).
    pub const STATE_HALF_OPEN: i64 = 2;
}

#[derive(Clone)]
pub struct Metrics {
    pub messages_total: IntCounter,
    pub decode_failures: IntCounter,
    pub processing_failures: IntCounter,
    pub dlq_total: IntCounter,
    pub lag_ms: IntGaugeVec,
    pub write_latency_seconds: Histogram,
    pub batch_size: Histogram,
    pub last_poll_timestamp: IntGauge,
    // Batch-specific metrics
    pub batch_messages_total: IntCounter,
    pub batch_flush_total: IntCounterVec,
    pub batch_bytes_total: IntCounter,
    pub batch_latency_seconds: Histogram,
    pub current_batch_size_limit: IntGauge,
    // Circuit breaker metrics
    pub circuit_breaker_state: IntGaugeVec,
    #[allow(dead_code)]
    pub circuit_breaker_opens_total: IntCounterVec,
    #[allow(dead_code)]
    pub circuit_breaker_closed_total: IntCounterVec,
    // Retry metrics
    pub retry_attempts: Histogram,
    pub retry_success_after_n_attempts: IntCounterVec,
    // Phase 5.1: Enhanced per-pipeline metrics
    #[allow(dead_code)]
    pub batch_size_per_pipeline: HistogramVec,
    #[allow(dead_code)]
    pub batch_flush_reason_per_pipeline: IntCounterVec,
    pub channel_depth_per_pipeline: IntGaugeVec,
    #[allow(dead_code)]
    pub write_throughput_bytes_per_pipeline: IntCounterVec,
    #[allow(dead_code)]
    pub copy_vs_insert_ratio_per_pipeline: IntCounterVec,
    #[allow(dead_code)]
    pub retry_attempts_per_pipeline: HistogramVec,
    #[allow(dead_code)]
    pub circuit_breaker_state_per_pipeline: IntGaugeVec,
    #[allow(dead_code)]
    pub inflight_batches_per_pipeline: IntGaugeVec,
    // Throughput metrics
    pub write_throughput_records_per_second_per_pipeline: IntGaugeVec,
    #[allow(dead_code)]
    pub memory_usage_bytes: IntGauge,
    #[allow(dead_code)]
    pub memory_usage_per_pipeline: IntGaugeVec,
    // Multi-tenancy metrics (Phase 9.2)
    #[allow(dead_code)]
    pub tenant_messages_total: IntCounterVec,
    #[allow(dead_code)]
    pub tenant_bytes_total: IntCounterVec,
    #[allow(dead_code)]
    pub tenant_processing_failures: IntCounterVec,
    #[allow(dead_code)]
    pub tenant_dlq_total: IntCounterVec,
    #[allow(dead_code)]
    pub tenant_write_latency_seconds: HistogramVec,
    #[allow(dead_code)]
    pub tenant_rate_limit_rejections: IntCounterVec,
    #[allow(dead_code)]
    pub tenant_rate_limit_tokens_available: IntGaugeVec,
}

impl Metrics {
    /// Create and register all Prometheus metrics and return an initialized `Metrics` instance.
    ///
    /// This function constructs each metric used by the metrics subsystem, registers them with
    /// the provided `Registry`, and returns a `Metrics` struct containing the registered handles.
    /// Registration errors from Prometheus primitives are propagated to the caller.
    ///
    /// # Errors
    ///
    /// Returns any error produced while creating or registering metrics with the given `Registry`.
    ///
    /// # Examples
    ///
    /// ```
    /// use prometheus::Registry;
    /// // Assume `Metrics` is in scope from the same crate
    /// let registry = Registry::new();
    /// let metrics = Metrics::register(&registry).unwrap();
    /// // Newly created counters start at zero
    /// assert_eq!(metrics.messages_total.get(), 0);
    /// ```
    pub fn register(registry: &Registry) -> Result<Self> {
        let messages_total =
            IntCounter::with_opts(Opts::new("messages_total", "Messages consumed"))?;
        let decode_failures =
            IntCounter::with_opts(Opts::new("decode_failures", "Decode/validation failures"))?;
        let processing_failures = IntCounter::with_opts(Opts::new(
            "processing_failures",
            "Post-write/commit failures",
        ))?;
        let dlq_total = IntCounter::with_opts(Opts::new("dlq_total", "Messages sent to DLQ"))?;
        let lag_ms = IntGaugeVec::new(
            Opts::new("lag_ms", "Consumer lag approximation in ms"),
            &["topic", "partition"],
        )?;
        let write_latency_seconds = Histogram::with_opts(HistogramOpts::new(
            "write_latency_seconds",
            "Latency of storage writes",
        ))?;
        let batch_size = Histogram::with_opts(
            HistogramOpts::new("batch_size", "Number of records per batch").buckets(vec![
                1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0,
            ]),
        )?;
        let last_poll_timestamp = IntGauge::with_opts(Opts::new(
            "last_poll_timestamp",
            "Timestamp of last successful poll in milliseconds since epoch",
        ))?;

        // Batch-specific metrics
        let batch_messages_total = IntCounter::with_opts(Opts::new(
            "batch_messages_total",
            "Total messages processed through batcher",
        ))?;
        let batch_flush_total = IntCounterVec::new(
            Opts::new("batch_flush_total", "Total batch flushes by reason"),
            &["reason"],
        )?;
        let batch_bytes_total = IntCounter::with_opts(Opts::new(
            "batch_bytes_total",
            "Total bytes processed through batcher",
        ))?;
        // Tuned buckets for batch latency (seconds): fine-grained ms up to several seconds
        let batch_latency_opts = HistogramOpts::new(
            "batch_latency_seconds",
            "Time from first message to batch flush",
        )
        .buckets(vec![
            0.001, // 1ms
            0.002, // 2ms
            0.005, // 5ms
            0.01,  // 10ms
            0.02,  // 20ms
            0.05,  // 50ms
            0.1,   // 100ms
            0.2,   // 200ms
            0.5,   // 500ms
            1.0,   // 1s
            2.0,   // 2s
            5.0,   // 5s
            10.0,  // 10s
        ]);

        let batch_latency_seconds = Histogram::with_opts(batch_latency_opts)?;

        let current_batch_size_limit = IntGauge::with_opts(Opts::new(
            "current_batch_size_limit",
            "Current adaptive batch size limit",
        ))?;

        // Register all metrics directly - they're already created above
        registry.register(Box::new(messages_total.clone()))?;
        registry.register(Box::new(decode_failures.clone()))?;
        registry.register(Box::new(processing_failures.clone()))?;
        registry.register(Box::new(dlq_total.clone()))?;
        registry.register(Box::new(lag_ms.clone()))?;
        registry.register(Box::new(write_latency_seconds.clone()))?;
        registry.register(Box::new(batch_size.clone()))?;
        registry.register(Box::new(last_poll_timestamp.clone()))?;
        registry.register(Box::new(batch_messages_total.clone()))?;
        registry.register(Box::new(batch_flush_total.clone()))?;
        registry.register(Box::new(batch_bytes_total.clone()))?;
        registry.register(Box::new(batch_latency_seconds.clone()))?;
        registry.register(Box::new(current_batch_size_limit.clone()))?;

        // Circuit breaker metrics
        let circuit_breaker_state = IntGaugeVec::new(
            Opts::new(
                "circuit_breaker_state",
                "Circuit breaker state (0=Closed, 1=Open, 2=HalfOpen)",
            ),
            &["pipeline"],
        )?;
        let circuit_breaker_opens_total = IntCounterVec::new(
            Opts::new("circuit_breaker_opens_total", "Total circuit breaker opens"),
            &["pipeline"],
        )?;
        let circuit_breaker_closed_total = IntCounterVec::new(
            Opts::new(
                "circuit_breaker_closed_total",
                "Total circuit breaker close events (transitions from Open to Closed)",
            ),
            &["pipeline"],
        )?;

        registry.register(Box::new(circuit_breaker_state.clone()))?;
        registry.register(Box::new(circuit_breaker_opens_total.clone()))?;
        registry.register(Box::new(circuit_breaker_closed_total.clone()))?;

        // Retry metrics
        let retry_attempts = Histogram::with_opts(
            HistogramOpts::new(
                "retry_attempts",
                "Number of attempts per write (including retries)",
            )
            .buckets(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]),
        )?;
        let retry_success_after_n_attempts = IntCounterVec::new(
            Opts::new(
                "retry_success_after_n_attempts",
                "Successful writes after N retry attempts",
            ),
            &["attempts"],
        )?;

        registry.register(Box::new(retry_attempts.clone()))?;
        registry.register(Box::new(retry_success_after_n_attempts.clone()))?;

        // Phase 5.1: Enhanced per-pipeline metrics
        let batch_size_per_pipeline = HistogramVec::new(
            HistogramOpts::new(
                "batch_size_per_pipeline",
                "Number of records per batch per pipeline",
            )
            .buckets(vec![
                1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0,
            ]),
            &["pipeline"],
        )?;
        let batch_flush_reason_per_pipeline = IntCounterVec::new(
            Opts::new(
                "batch_flush_reason_per_pipeline",
                "Batch flush count per pipeline per reason (time/size/bytes/shutdown)",
            ),
            &["pipeline", "reason"],
        )?;
        let channel_depth_per_pipeline = IntGaugeVec::new(
            Opts::new(
                "channel_depth_per_pipeline",
                "Number of messages pending in pipeline channel",
            ),
            &["pipeline"],
        )?;
        let write_throughput_bytes_per_pipeline = IntCounterVec::new(
            Opts::new(
                "write_throughput_bytes_per_pipeline",
                "Total bytes written to database per pipeline",
            ),
            &["pipeline"],
        )?;
        let copy_vs_insert_ratio_per_pipeline = IntCounterVec::new(
            Opts::new(
                "copy_vs_insert_ratio_per_pipeline",
                "Count of COPY vs INSERT operations per pipeline per method",
            ),
            &["pipeline", "method"],
        )?;
        let retry_attempts_per_pipeline = HistogramVec::new(
            HistogramOpts::new(
                "retry_attempts_per_pipeline",
                "Number of retry attempts per write per pipeline",
            )
            .buckets(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]),
            &["pipeline"],
        )?;
        let circuit_breaker_state_per_pipeline = IntGaugeVec::new(
            Opts::new(
                "circuit_breaker_state_per_pipeline",
                "Circuit breaker state per pipeline (0=Closed, 1=Open, 2=HalfOpen)",
            ),
            &["pipeline"],
        )?;
        let inflight_batches_per_pipeline = IntGaugeVec::new(
            Opts::new(
                "inflight_batches_per_pipeline",
                "Number of batches currently being written per pipeline",
            ),
            &["pipeline"],
        )?;

        let write_throughput_records_per_second_per_pipeline = IntGaugeVec::new(
            Opts::new(
                "write_throughput_records_per_second_per_pipeline",
                "Current write throughput in records per second per pipeline",
            ),
            &["pipeline"],
        )?;

        registry.register(Box::new(batch_size_per_pipeline.clone()))?;
        registry.register(Box::new(batch_flush_reason_per_pipeline.clone()))?;
        registry.register(Box::new(channel_depth_per_pipeline.clone()))?;
        registry.register(Box::new(write_throughput_bytes_per_pipeline.clone()))?;
        registry.register(Box::new(copy_vs_insert_ratio_per_pipeline.clone()))?;
        registry.register(Box::new(retry_attempts_per_pipeline.clone()))?;
        registry.register(Box::new(circuit_breaker_state_per_pipeline.clone()))?;
        registry.register(Box::new(inflight_batches_per_pipeline.clone()))?;
        registry.register(Box::new(write_throughput_records_per_second_per_pipeline.clone()))?;

        // Memory management metrics
        let memory_usage_bytes = IntGauge::new(
            "memory_usage_bytes",
            "Current process memory usage in bytes",
        )?;
        let memory_usage_per_pipeline = IntGaugeVec::new(
            Opts::new(
                "memory_usage_per_pipeline",
                "Estimated memory usage per pipeline in bytes",
            ),
            &["pipeline"],
        )?;

        registry.register(Box::new(memory_usage_bytes.clone()))?;
        registry.register(Box::new(memory_usage_per_pipeline.clone()))?;

        // Multi-tenancy metrics (Phase 9.2)
        let tenant_messages_total = IntCounterVec::new(
            Opts::new("tenant_messages_total", "Messages processed per tenant"),
            &["tenant_id"],
        )?;
        let tenant_bytes_total = IntCounterVec::new(
            Opts::new("tenant_bytes_total", "Bytes processed per tenant"),
            &["tenant_id"],
        )?;
        let tenant_processing_failures = IntCounterVec::new(
            Opts::new(
                "tenant_processing_failures",
                "Processing failures per tenant",
            ),
            &["tenant_id"],
        )?;
        let tenant_dlq_total = IntCounterVec::new(
            Opts::new("tenant_dlq_total", "Messages sent to DLQ per tenant"),
            &["tenant_id"],
        )?;
        let tenant_write_latency_seconds = HistogramVec::new(
            HistogramOpts::new(
                "tenant_write_latency_seconds",
                "Write latency per tenant in seconds",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0,
            ]),
            &["tenant_id"],
        )?;
        let tenant_rate_limit_rejections = IntCounterVec::new(
            Opts::new(
                "tenant_rate_limit_rejections",
                "Rate limit rejections per tenant",
            ),
            &["tenant_id"],
        )?;
        let tenant_rate_limit_tokens_available = IntGaugeVec::new(
            Opts::new(
                "tenant_rate_limit_tokens_available",
                "Available rate limit tokens per tenant",
            ),
            &["tenant_id"],
        )?;

        registry.register(Box::new(tenant_messages_total.clone()))?;
        registry.register(Box::new(tenant_bytes_total.clone()))?;
        registry.register(Box::new(tenant_processing_failures.clone()))?;
        registry.register(Box::new(tenant_dlq_total.clone()))?;
        registry.register(Box::new(tenant_write_latency_seconds.clone()))?;
        registry.register(Box::new(tenant_rate_limit_rejections.clone()))?;
        registry.register(Box::new(tenant_rate_limit_tokens_available.clone()))?;

        Ok(Self {
            messages_total,
            decode_failures,
            processing_failures,
            dlq_total,
            lag_ms,
            write_latency_seconds,
            batch_size,
            last_poll_timestamp,
            batch_messages_total,
            batch_flush_total,
            batch_bytes_total,
            batch_latency_seconds,
            current_batch_size_limit,
            circuit_breaker_state,
            circuit_breaker_opens_total,
            circuit_breaker_closed_total,
            retry_attempts,
            retry_success_after_n_attempts,
            batch_size_per_pipeline,
            batch_flush_reason_per_pipeline,
            channel_depth_per_pipeline,
            write_throughput_bytes_per_pipeline,
            copy_vs_insert_ratio_per_pipeline,
            retry_attempts_per_pipeline,
            circuit_breaker_state_per_pipeline,
            inflight_batches_per_pipeline,
            write_throughput_records_per_second_per_pipeline,
            memory_usage_bytes,
            memory_usage_per_pipeline,
            tenant_messages_total,
            tenant_bytes_total,
            tenant_processing_failures,
            tenant_dlq_total,
            tenant_write_latency_seconds,
            tenant_rate_limit_rejections,
            tenant_rate_limit_tokens_available,
        })
    }
}

fn metrics_handler(registry: Registry) -> Result<String, String> {
    let encoder = TextEncoder::new();
    let metric_families = registry.gather();
    let mut buffer = Vec::new();
    encoder
        .encode(&metric_families, &mut buffer)
        .map_err(|e| e.to_string())?;
    String::from_utf8(buffer).map_err(|e| e.to_string())
}

/// Starts an HTTP server that exposes Prometheus metrics and health endpoints on the given port.
///
/// The server binds to 0.0.0.0 and provides:
/// - `GET /metrics`: Prometheus text format metrics (`text/plain; version=0.0.4`) or 500 on error.
/// - `GET /health`: returns the literal `"ok"`.
/// - `GET /ready`: returns the service health as JSON; responds 200 for `Healthy` or `Degraded`, 503 for `Unhealthy`, or 500 on serialization error.
///
/// Errors encountered while binding or serving are logged but do not panic the caller.
///
/// # Parameters
///
/// - `registry`: Prometheus registry whose metrics will be served at `/metrics`.
/// - `health`: shared health registry used to produce the `/ready` response.
/// - `port`: TCP port to bind the exporter to (server listens on 0.0.0.0:port).
///
/// # Returns
///
/// A `JoinHandle<()>` for the spawned Tokio task running the HTTP exporter.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use prometheus::Registry;
///
/// // Create a registry and a health registry (types shown for illustration).
/// let registry = Registry::new();
/// let health = Arc::new(crate::health::HealthRegistry::new());
///
/// // Spawn exporter on port 9100.
/// let _handle = crate::metrics::spawn_exporter(registry, health, 9100);
/// ```
pub fn spawn_exporter(
    registry: Registry,
    health: Arc<HealthRegistry>,
    port: u16,
) -> JoinHandle<()> {
    let registry_clone = registry.clone();
    let health_clone = health.clone();
    let app = Router::new()
        .route(
            "/metrics",
            get(move || {
                let registry = registry_clone.clone();
                async move {
                    match metrics_handler(registry) {
                        Ok(body) => axum::response::Response::builder()
                            .status(200)
                            .header("Content-Type", "text/plain; version=0.0.4")
                            .body(body)
                            .unwrap(),
                        Err(err) => axum::response::Response::builder()
                            .status(500)
                            .body(err)
                            .unwrap(),
                    }
                }
            }),
        )
        .route("/health", get(|| async { "ok" }))
        .route(
            "/ready",
            get(move || {
                let health = health_clone.clone();
                async move {
                    let status = health.get_status();
                    let code = match status.status {
                        ComponentStatus::Healthy => 200,
                        ComponentStatus::Degraded => 200,
                        ComponentStatus::Unhealthy => 503,
                    };
                    match serde_json::to_string(&status) {
                        Ok(body) => axum::response::Response::builder()
                            .status(code)
                            .header("Content-Type", "application/json")
                            .body(body)
                            .unwrap(),
                        Err(err) => axum::response::Response::builder()
                            .status(500)
                            .body(format!("Failed to serialize status: {}", err))
                            .unwrap(),
                    }
                }
            }),
        );

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tokio::spawn(async move {
        match TcpListener::bind(addr).await {
            Ok(listener) => {
                if let Err(err) = axum::serve(listener, app.into_make_service()).await {
                    error!("metrics server error: {}", err);
                }
            }
            Err(err) => error!("metrics listener bind error: {}", err),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_register_creates_all_metrics() {
        let registry = Registry::new();
        let metrics = Metrics::register(&registry).expect("should register metrics");

        // Check that all metrics were created
        assert_eq!(metrics.messages_total.get(), 0);
        assert_eq!(metrics.decode_failures.get(), 0);
        assert_eq!(metrics.processing_failures.get(), 0);
        assert_eq!(metrics.dlq_total.get(), 0);
        assert_eq!(metrics.batch_messages_total.get(), 0);
        assert_eq!(metrics.batch_bytes_total.get(), 0);
    }

    #[test]
    fn test_metrics_increment_counters() {
        let registry = Registry::new();
        let metrics = Metrics::register(&registry).expect("should register metrics");

        // Test message counter
        assert_eq!(metrics.messages_total.get(), 0);
        metrics.messages_total.inc();
        assert_eq!(metrics.messages_total.get(), 1);
        metrics.messages_total.inc();
        assert_eq!(metrics.messages_total.get(), 2);
    }

    #[test]
    fn test_metrics_increment_decode_failures() {
        let registry = Registry::new();
        let metrics = Metrics::register(&registry).expect("should register metrics");

        assert_eq!(metrics.decode_failures.get(), 0);
        metrics.decode_failures.inc();
        assert_eq!(metrics.decode_failures.get(), 1);
    }

    #[test]
    fn test_metrics_increment_processing_failures() {
        let registry = Registry::new();
        let metrics = Metrics::register(&registry).expect("should register metrics");

        assert_eq!(metrics.processing_failures.get(), 0);
        metrics.processing_failures.inc();
        assert_eq!(metrics.processing_failures.get(), 1);
    }

    #[test]
    fn test_metrics_increment_dlq_total() {
        let registry = Registry::new();
        let metrics = Metrics::register(&registry).expect("should register metrics");

        assert_eq!(metrics.dlq_total.get(), 0);
        metrics.dlq_total.inc();
        assert_eq!(metrics.dlq_total.get(), 1);
    }

    #[test]
    fn test_metrics_batch_counters() {
        let registry = Registry::new();
        let metrics = Metrics::register(&registry).expect("should register metrics");

        assert_eq!(metrics.batch_messages_total.get(), 0);
        metrics.batch_messages_total.inc();
        assert_eq!(metrics.batch_messages_total.get(), 1);

        assert_eq!(metrics.batch_bytes_total.get(), 0);
        metrics.batch_bytes_total.inc_by(1024);
        assert_eq!(metrics.batch_bytes_total.get(), 1024);
    }

    #[test]
    fn test_metrics_lag_gauge_with_labels() {
        let registry = Registry::new();
        let metrics = Metrics::register(&registry).expect("should register metrics");

        metrics.lag_ms.with_label_values(&["topic1", "0"]).set(100);
        metrics.lag_ms.with_label_values(&["topic1", "1"]).set(200);

        let v0 = metrics.lag_ms.with_label_values(&["topic1", "0"]).get();
        let v1 = metrics.lag_ms.with_label_values(&["topic1", "1"]).get();

        assert_eq!(v0, 100);
        assert_eq!(v1, 200);
    }

    #[test]
    fn test_metrics_last_poll_timestamp() {
        let registry = Registry::new();
        let metrics = Metrics::register(&registry).expect("should register metrics");

        assert_eq!(metrics.last_poll_timestamp.get(), 0);
        metrics.last_poll_timestamp.set(1234567890);
        assert_eq!(metrics.last_poll_timestamp.get(), 1234567890);
    }

    #[test]
    fn test_metrics_handler_generates_output() {
        let registry = Registry::new();
        let _metrics = Metrics::register(&registry).expect("should register metrics");

        let output = metrics_handler(registry).expect("should generate metrics output");
        assert!(!output.is_empty());
        assert!(output.contains("messages_total"));
    }

    #[test]
    fn test_metrics_clone() {
        let registry = Registry::new();
        let metrics1 = Metrics::register(&registry).expect("should register metrics");
        let metrics2 = metrics1.clone();

        metrics1.messages_total.inc();
        // Both should reference same counters
        assert_eq!(metrics2.messages_total.get(), 1);
    }

    #[test]
    fn test_metrics_write_latency_histogram() {
        let registry = Registry::new();
        let metrics = Metrics::register(&registry).expect("should register metrics");

        // Observe some values
        metrics.write_latency_seconds.observe(0.1);
        metrics.write_latency_seconds.observe(0.5);
        metrics.write_latency_seconds.observe(1.0);

        // Verify observations were recorded by checking histogram data directly
        // Histograms in prometheus have internal sample_sum and sample_count
        let metric_families = registry.gather();

        for mf in &metric_families {
            if mf.get_name() == "write_latency_seconds" {
                for metric in mf.get_metric() {
                    if metric.has_histogram() {
                        let histogram = metric.get_histogram();
                        // Check sample count: should be 3
                        assert_eq!(
                            histogram.get_sample_count(),
                            3u64,
                            "expected 3 sample count, got {}",
                            histogram.get_sample_count()
                        );
                        // Check sample sum: should be 0.1 + 0.5 + 1.0 = 1.6
                        assert!(
                            (histogram.get_sample_sum() - 1.6).abs() < 0.001,
                            "expected sum 1.6, got {}",
                            histogram.get_sample_sum()
                        );
                        return; // Found and verified
                    }
                }
            }
        }
        panic!("write_latency_seconds histogram metric not found");
    }

    #[test]
    fn test_metrics_batch_size_histogram() {
        let registry = Registry::new();
        let metrics = Metrics::register(&registry).expect("should register metrics");

        // Observe some batch sizes
        metrics.batch_size.observe(10.0);
        metrics.batch_size.observe(100.0);
        metrics.batch_size.observe(500.0);

        // Verify observations were recorded by checking histogram data directly
        let metric_families = registry.gather();

        for mf in &metric_families {
            if mf.get_name() == "batch_size" {
                for metric in mf.get_metric() {
                    if metric.has_histogram() {
                        let histogram = metric.get_histogram();
                        // Check sample count: should be 3
                        assert_eq!(
                            histogram.get_sample_count(),
                            3u64,
                            "expected 3 sample count, got {}",
                            histogram.get_sample_count()
                        );
                        // Check sample sum: should be 10 + 100 + 500 = 610
                        assert!(
                            (histogram.get_sample_sum() - 610.0).abs() < 0.001,
                            "expected sum 610.0, got {}",
                            histogram.get_sample_sum()
                        );
                        return; // Found and verified
                    }
                }
            }
        }
        panic!("batch_size histogram metric not found");
    }

    #[test]
    fn test_metrics_batch_flush_counter() {
        let registry = Registry::new();
        let metrics = Metrics::register(&registry).expect("should register metrics");

        metrics
            .batch_flush_total
            .with_label_values(&["timeout"])
            .inc();
        metrics
            .batch_flush_total
            .with_label_values(&["manual"])
            .inc();
        metrics
            .batch_flush_total
            .with_label_values(&["shutdown"])
            .inc();

        // Verify counters incremented
        assert_eq!(
            metrics
                .batch_flush_total
                .with_label_values(&["timeout"])
                .get(),
            1
        );
        assert_eq!(
            metrics
                .batch_flush_total
                .with_label_values(&["manual"])
                .get(),
            1
        );
        assert_eq!(
            metrics
                .batch_flush_total
                .with_label_values(&["shutdown"])
                .get(),
            1
        );
    }

    #[test]
    fn test_metrics_batch_latency_histogram() {
        let registry = Registry::new();
        let metrics = Metrics::register(&registry).expect("should register metrics");

        metrics.batch_latency_seconds.observe(0.001);
        metrics.batch_latency_seconds.observe(0.1);
        metrics.batch_latency_seconds.observe(1.0);

        // Verify observations were recorded by checking histogram data directly
        let metric_families = registry.gather();

        for mf in &metric_families {
            if mf.get_name() == "batch_latency_seconds" {
                for metric in mf.get_metric() {
                    if metric.has_histogram() {
                        let histogram = metric.get_histogram();
                        // Check sample count: should be 3
                        assert_eq!(
                            histogram.get_sample_count(),
                            3u64,
                            "expected 3 sample count, got {}",
                            histogram.get_sample_count()
                        );
                        // Check sample sum: should be 0.001 + 0.1 + 1.0 = 1.101
                        assert!(
                            (histogram.get_sample_sum() - 1.101).abs() < 0.001,
                            "expected sum 1.101, got {}",
                            histogram.get_sample_sum()
                        );
                        return; // Found and verified
                    }
                }
            }
        }
        panic!("batch_latency_seconds histogram metric not found");
    }

    #[test]
    fn test_metrics_handler_with_empty_registry() {
        let registry = Registry::new();
        let output = metrics_handler(registry).expect("should handle empty registry");
        assert_eq!(output, "");
    }

    #[test]
    fn test_metrics_handler_with_populated_registry() {
        let registry = Registry::new();
        let metrics = Metrics::register(&registry).expect("should register metrics");

        // Add some data
        metrics.messages_total.inc_by(42);
        metrics.decode_failures.inc_by(5);
        metrics.processing_failures.inc_by(3);
        metrics.dlq_total.inc_by(2);

        let output = metrics_handler(registry).expect("should generate output");

        // Verify output contains expected metric names
        assert!(output.contains("messages_total"));
        assert!(output.contains("decode_failures"));
        assert!(output.contains("processing_failures"));
        assert!(output.contains("dlq_total"));
    }

    #[test]
    fn test_metrics_increment_by() {
        let registry = Registry::new();
        let metrics = Metrics::register(&registry).expect("should register metrics");

        assert_eq!(metrics.messages_total.get(), 0);
        metrics.messages_total.inc_by(10);
        assert_eq!(metrics.messages_total.get(), 10);
        metrics.messages_total.inc_by(5);
        assert_eq!(metrics.messages_total.get(), 15);
    }

    #[test]
    fn test_metrics_processing_failures_increment() {
        let registry = Registry::new();
        let metrics = Metrics::register(&registry).expect("should register metrics");

        for _ in 0..10 {
            metrics.processing_failures.inc();
        }
        assert_eq!(metrics.processing_failures.get(), 10);
    }

    #[test]
    fn test_metrics_dlq_total_multiple_increments() {
        let registry = Registry::new();
        let metrics = Metrics::register(&registry).expect("should register metrics");

        metrics.dlq_total.inc();
        metrics.dlq_total.inc();
        metrics.dlq_total.inc();

        assert_eq!(metrics.dlq_total.get(), 3);
    }

    #[test]
    fn test_metrics_lag_multiple_topics_and_partitions() {
        let registry = Registry::new();
        let metrics = Metrics::register(&registry).expect("should register metrics");

        // Simulate lag for multiple topics and partitions
        for topic in &["orders", "payments", "users"] {
            for partition in 0..3 {
                metrics
                    .lag_ms
                    .with_label_values(&[topic, &partition.to_string()])
                    .set(100 * (partition as i64 + 1));
            }
        }

        // Verify some values
        assert_eq!(
            metrics.lag_ms.with_label_values(&["orders", "0"]).get(),
            100
        );
        assert_eq!(
            metrics.lag_ms.with_label_values(&["payments", "1"]).get(),
            200
        );
        assert_eq!(metrics.lag_ms.with_label_values(&["users", "2"]).get(), 300);
    }

    #[test]
    fn test_metrics_batch_bytes_accumulation() {
        let registry = Registry::new();
        let metrics = Metrics::register(&registry).expect("should register metrics");

        assert_eq!(metrics.batch_bytes_total.get(), 0);

        // Simulate accumulating batch bytes
        metrics.batch_bytes_total.inc_by(1024);
        assert_eq!(metrics.batch_bytes_total.get(), 1024);

        metrics.batch_bytes_total.inc_by(2048);
        assert_eq!(metrics.batch_bytes_total.get(), 3072);
    }

    /// Verifies that the `batch_flush_total` counter records increments for multiple labeled flush reasons.
    ///
    /// # Examples
    ///
    /// ```
    /// let registry = prometheus::Registry::new();
    /// let metrics = crate::metrics::Metrics::register(&registry).unwrap();
    /// metrics.batch_flush_total.with_label_values(&["timeout"]).inc_by(3.0);
    /// assert_eq!(metrics.batch_flush_total.with_label_values(&["timeout"]).get(), 3);
    /// ```
    #[test]
    fn test_metrics_multiple_flush_reasons() {
        let registry = Registry::new();
        let metrics = Metrics::register(&registry).expect("should register metrics");

        let flush_reasons = vec!["timeout", "manual", "shutdown", "full"];

        for reason in flush_reasons {
            for _ in 0..3 {
                metrics.batch_flush_total.with_label_values(&[reason]).inc();
            }
        }

        // Verify all counters
        assert_eq!(
            metrics
                .batch_flush_total
                .with_label_values(&["timeout"])
                .get(),
            3
        );
        assert_eq!(
            metrics
                .batch_flush_total
                .with_label_values(&["manual"])
                .get(),
            3
        );
        assert_eq!(
            metrics
                .batch_flush_total
                .with_label_values(&["shutdown"])
                .get(),
            3
        );
        assert_eq!(
            metrics.batch_flush_total.with_label_values(&["full"]).get(),
            3
        );
    }

    #[test]
    fn test_metrics_large_values() {
        let registry = Registry::new();
        let metrics = Metrics::register(&registry).expect("should register metrics");

        // Test with large counter values
        metrics.messages_total.inc_by(u64::MAX / 2);
        assert_eq!(metrics.messages_total.get(), u64::MAX / 2);

        // Test with large gauge values
        metrics.last_poll_timestamp.set(i64::MAX);
        assert_eq!(metrics.last_poll_timestamp.get(), i64::MAX);
    }

    #[test]
    fn test_metrics_single_registration() {
        let registry = Registry::new();
        // Register metrics once
        let metrics = Metrics::register(&registry).expect("registration should succeed");

        // Verify the metrics work as expected
        metrics.messages_total.inc_by(5);
        assert_eq!(metrics.messages_total.get(), 5);
    }

    #[test]
    fn test_metrics_latency_histogram_ranges() {
        let registry = Registry::new();
        let metrics = Metrics::register(&registry).expect("should register metrics");

        // Test observing values across histogram buckets
        let latencies = vec![0.001, 0.01, 0.1, 0.5, 1.0, 5.0, 10.0];
        let expected_sum: f64 = latencies.iter().sum();
        for latency in latencies {
            metrics.write_latency_seconds.observe(latency);
        }

        // Verify observations across histogram buckets by checking histogram data directly
        let metric_families = registry.gather();

        for mf in &metric_families {
            if mf.get_name() == "write_latency_seconds" {
                for metric in mf.get_metric() {
                    if metric.has_histogram() {
                        let histogram = metric.get_histogram();
                        // Check sample count: should be 7
                        assert_eq!(
                            histogram.get_sample_count(),
                            7u64,
                            "expected 7 sample count, got {}",
                            histogram.get_sample_count()
                        );
                        // Check sample sum across the range
                        assert!(
                            (histogram.get_sample_sum() - expected_sum).abs() < 0.0001,
                            "expected sum {}, got {}",
                            expected_sum,
                            histogram.get_sample_sum()
                        );
                        return; // Found and verified
                    }
                }
            }
        }
        panic!("write_latency_seconds histogram metric not found");
    }

    #[test]
    fn test_circuit_breaker_state_gauge() {
        let registry = Registry::new();
        let metrics = Metrics::register(&registry).expect("should register metrics");

        // Test setting circuit breaker states for different pipelines
        metrics
            .circuit_breaker_state
            .with_label_values(&["pipeline1"])
            .set(circuit_breaker_states::STATE_CLOSED);
        metrics
            .circuit_breaker_state
            .with_label_values(&["pipeline2"])
            .set(circuit_breaker_states::STATE_OPEN);
        metrics
            .circuit_breaker_state
            .with_label_values(&["pipeline3"])
            .set(circuit_breaker_states::STATE_HALF_OPEN);

        // Verify we can read the gauge values by recreating the labels
        // The IntGaugeVec stores state values internally; we verify by examining the gathered metrics
        let metric_families = registry.gather();
        let mut found_count = 0;
        for mf in &metric_families {
            if mf.get_name() == "circuit_breaker_state" {
                found_count = mf.get_metric().len();
            }
        }
        // Should have at least 3 label combinations
        assert!(found_count >= 3, "expected at least 3 state gauge metrics");
    }

    #[test]
    fn test_circuit_breaker_opens_counter() {
        let registry = Registry::new();
        let metrics = Metrics::register(&registry).expect("should register metrics");

        // Increment circuit breaker open events for different pipelines
        metrics
            .circuit_breaker_opens_total
            .with_label_values(&["pipeline1"])
            .inc();
        metrics
            .circuit_breaker_opens_total
            .with_label_values(&["pipeline1"])
            .inc();
        metrics
            .circuit_breaker_opens_total
            .with_label_values(&["pipeline2"])
            .inc();

        // Verify the metrics are registered and callable
        assert_eq!(
            metrics
                .circuit_breaker_opens_total
                .with_label_values(&["pipeline1"])
                .get(),
            2
        );
        assert_eq!(
            metrics
                .circuit_breaker_opens_total
                .with_label_values(&["pipeline2"])
                .get(),
            1
        );
    }

    #[test]
    fn test_circuit_breaker_closed_counter() {
        let registry = Registry::new();
        let metrics = Metrics::register(&registry).expect("should register metrics");

        // Increment circuit breaker closed events (transitions from Open to Closed)
        metrics
            .circuit_breaker_closed_total
            .with_label_values(&["pipeline1"])
            .inc();
        metrics
            .circuit_breaker_closed_total
            .with_label_values(&["pipeline1"])
            .inc();
        metrics
            .circuit_breaker_closed_total
            .with_label_values(&["pipeline1"])
            .inc();
        metrics
            .circuit_breaker_closed_total
            .with_label_values(&["pipeline2"])
            .inc_by(2);

        // Verify counter values using direct metric access
        assert_eq!(
            metrics
                .circuit_breaker_closed_total
                .with_label_values(&["pipeline1"])
                .get(),
            3
        );
        assert_eq!(
            metrics
                .circuit_breaker_closed_total
                .with_label_values(&["pipeline2"])
                .get(),
            2
        );
    }

    #[test]
    fn test_retry_attempts_histogram() {
        let registry = Registry::new();
        let metrics = Metrics::register(&registry).expect("should register metrics");

        // Record retry attempts across different scenarios
        let attempt_counts = vec![1.0, 2.0, 3.0, 1.0, 4.0, 2.0];
        let expected_sum: f64 = attempt_counts.iter().sum();
        for attempts in attempt_counts {
            metrics.retry_attempts.observe(attempts);
        }

        // Verify histogram observations via registry
        let metric_families = registry.gather();
        for mf in &metric_families {
            if mf.get_name() == "retry_attempts" {
                for metric in mf.get_metric() {
                    if metric.has_histogram() {
                        let histogram = metric.get_histogram();
                        assert_eq!(
                            histogram.get_sample_count(),
                            6u64,
                            "expected 6 attempt observations"
                        );
                        assert!(
                            (histogram.get_sample_sum() - expected_sum).abs() < 0.0001,
                            "expected sum {}, got {}",
                            expected_sum,
                            histogram.get_sample_sum()
                        );
                        return;
                    }
                }
            }
        }
        panic!("retry_attempts histogram not found");
    }

    #[test]
    fn test_retry_success_after_n_attempts_counter() {
        let registry = Registry::new();
        let metrics = Metrics::register(&registry).expect("should register metrics");

        // Record successful retries after various attempt counts
        metrics
            .retry_success_after_n_attempts
            .with_label_values(&["1"])
            .inc_by(10); // 10 successful writes on first attempt
        metrics
            .retry_success_after_n_attempts
            .with_label_values(&["2"])
            .inc_by(5); // 5 successful writes after 1 retry
        metrics
            .retry_success_after_n_attempts
            .with_label_values(&["3"])
            .inc_by(2); // 2 successful writes after 2 retries
        metrics
            .retry_success_after_n_attempts
            .with_label_values(&["5"])
            .inc(); // 1 successful write after 4 retries

        // Verify counter values directly
        assert_eq!(
            metrics
                .retry_success_after_n_attempts
                .with_label_values(&["1"])
                .get(),
            10
        );
        assert_eq!(
            metrics
                .retry_success_after_n_attempts
                .with_label_values(&["2"])
                .get(),
            5
        );
        assert_eq!(
            metrics
                .retry_success_after_n_attempts
                .with_label_values(&["3"])
                .get(),
            2
        );
        assert_eq!(
            metrics
                .retry_success_after_n_attempts
                .with_label_values(&["5"])
                .get(),
            1
        );
    }
}
