use anyhow::Result;
use axum::{routing::get, Router};
use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntGaugeVec, Opts, Registry, TextEncoder,
};
use std::net::SocketAddr;
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

        // Register all metrics directly - they're already created above
        registry.register(Box::new(messages_total.clone()))?;
        registry.register(Box::new(decode_failures.clone()))?;
        registry.register(Box::new(processing_failures.clone()))?;
        registry.register(Box::new(dlq_total.clone()))?;
        registry.register(Box::new(lag_ms.clone()))?;
        registry.register(Box::new(write_latency_seconds.clone()))?;

        Ok(Self {
            messages_total,
            decode_failures,
            processing_failures,
            dlq_total,
            lag_ms,
            write_latency_seconds,
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

pub fn spawn_exporter(registry: Registry, port: u16) -> JoinHandle<()> {
    let registry_clone = registry.clone();
    let app = Router::new().route(
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
