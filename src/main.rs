mod config;
mod consumer;
mod decoder;
mod dlq;
mod errors;
mod logging;
mod metrics;
mod types;
mod writer;

use crate::config::Config;
use crate::metrics::Metrics;
use crate::writer::Writer;
use anyhow::Result;
use prometheus::Registry;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = Config::from_env()?;
    logging::init(&cfg.service.log_level)
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;

    let registry = Registry::new();
    let metrics = Metrics::register(&registry)?;
    metrics::spawn_exporter(registry.clone(), cfg.service.metrics_port);

    let cfg = Arc::new(cfg);
    let writer_cfg = Arc::new(cfg.postgres.clone());
    let writer = Arc::new(Writer::new(writer_cfg, metrics.clone()).await?);
    consumer::run(cfg, writer, metrics).await?;
    Ok(())
}
