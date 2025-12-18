use crate::health::{ComponentStatus, HealthRegistry};
use anyhow::Result;
use axum::{routing::get, Router};
use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntGauge, IntGaugeVec, Opts, Registry,
    TextEncoder,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::error;

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
    registry: Registry,
}

impl Metrics {
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

        // Register all metrics directly - they're already created above
        registry.register(Box::new(messages_total.clone()))?;
        registry.register(Box::new(decode_failures.clone()))?;
        registry.register(Box::new(processing_failures.clone()))?;
        registry.register(Box::new(dlq_total.clone()))?;
        registry.register(Box::new(lag_ms.clone()))?;
        registry.register(Box::new(write_latency_seconds.clone()))?;
        registry.register(Box::new(batch_size.clone()))?;
        registry.register(Box::new(last_poll_timestamp.clone()))?;

        Ok(Self {
            messages_total,
            decode_failures,
            processing_failures,
            dlq_total,
            lag_ms,
            write_latency_seconds,
            batch_size,
            last_poll_timestamp,
            registry: registry.clone(),
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
