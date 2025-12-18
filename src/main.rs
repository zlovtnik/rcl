mod batcher;
mod config;
mod consumer;
mod decoder;
mod dlq;
mod eip;
mod errors;
mod health;
mod integration_tests;
mod load_test;
mod logging;
mod metrics;
mod shutdown;
mod stages;
mod types;
mod writer;

use crate::config::Config;
use crate::health::HealthRegistry;
use crate::metrics::Metrics;
use crate::writer::Writer;
use anyhow::Result;
use clap::{Parser, Subcommand};
use prometheus::Registry;
use std::sync::Arc;
use tracing::{error, info};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Validate configuration and exit
    #[arg(long)]
    validate_config: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the ingestion service (default)
    Run,
    /// Replay messages from a specific offset range
    Replay {
        #[arg(short, long)]
        topic: String,
        #[arg(short, long)]
        partition: i32,
        #[arg(long)]
        start_offset: i64,
        #[arg(long)]
        end_offset: i64,
    },
    /// Inspect or drain DLQ
    Dlq {
        #[arg(short, long)]
        topic: String,
        #[command(subcommand)]
        action: DlqAction,
    },
    /// Run load test
    LoadTest {
        #[arg(short, long, default_value_t = 1000)]
        rate: u32,
        #[arg(short, long, default_value_t = 60)]
        duration_sec: u64,
    },
}

#[derive(Subcommand)]
enum DlqAction {
    Inspect {
        #[arg(short, long, default_value_t = 10)]
        limit: usize,
    },
    Drain {
        #[arg(short, long)]
        output: Option<String>,
        #[arg(long)]
        requeue: bool,
    },
}

/// Application entry point that parses CLI arguments, loads configuration, initializes logging, and dispatches the selected subcommand (`Run`, `Replay`, `Dlq`, or `LoadTest`).
///
/// The function exits early if `--validate-config` is provided after printing a validation message.
///
/// # Examples
///
/// ```no_run
/// // Start the service with default behavior
/// // cargo run --bin my_service
/// ```
#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();
    let cfg = Config::from_env()?;

    if cli.validate_config {
        println!("Configuration is valid");
        return Ok(());
    }

    let _guard = logging::init(&cfg.service).map_err(|e| anyhow::anyhow!(e.to_string()))?;

    match cli.command.unwrap_or(Commands::Run) {
        Commands::Run => run_service(cfg).await,
        Commands::Replay {
            topic,
            partition,
            start_offset,
            end_offset,
        } => {
            let cfg = Arc::new(cfg);
            let registry = Registry::new();
            let metrics = Metrics::register(&registry)?;
            let writer_cfg = Arc::new(cfg.postgres.clone());
            let health = Arc::new(HealthRegistry::new(std::time::Duration::from_millis(
                cfg.service.health_check_timeout_ms,
            )));
            let writer: Arc<Writer> =
                Arc::new(Writer::new(writer_cfg, metrics.clone(), health).await?);

            consumer::replay(
                cfg,
                writer,
                metrics,
                topic,
                partition,
                start_offset,
                end_offset,
            )
            .await
        }
        Commands::Dlq { topic, action } => match action {
            DlqAction::Inspect { limit } => dlq::inspect(&cfg.kafka, &topic, limit).await,
            DlqAction::Drain { output, requeue } => {
                dlq::drain(&cfg.kafka, &topic, output, requeue).await
            }
        },
        Commands::LoadTest { rate, duration_sec } => load_test::run(cfg, rate, duration_sec).await,
    }
}

/// Starts the ingestion service: initializes health and metrics, constructs the writer and consumer, and coordinates graceful shutdown.
///
/// On completion returns success when the consumer finishes within the configured shutdown timeout; otherwise returns an error.
///
/// # Errors
///
/// Returns an error if initialization fails, if the consumer task fails to join, or if shutdown times out.
///
/// # Examples
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let cfg = /* build or load Config */ unimplemented!();
/// run_service(cfg).await?;
/// # Ok(()) }
/// ```
async fn run_service(cfg: Config) -> Result<()> {
    let health = Arc::new(HealthRegistry::new(std::time::Duration::from_millis(
        cfg.service.health_check_timeout_ms,
    )));
    let registry = Registry::new();
    let metrics = Metrics::register(&registry)?;
    metrics::spawn_exporter(registry.clone(), health.clone(), cfg.service.metrics_port);

    let cfg = Arc::new(cfg);
    let shutdown_timeout = cfg.service.shutdown_timeout_duration();
    let writer_cfg = Arc::new(cfg.postgres.clone());
    let writer: Arc<Writer> =
        Arc::new(Writer::new(writer_cfg, metrics.clone(), health.clone()).await?);

    let (coordinator, shutdown_rx) = shutdown::ShutdownCoordinator::new();

    let consumer_handle = tokio::spawn(consumer::run(
        cfg.clone(),
        writer,
        metrics,
        shutdown_rx,
        health.clone(),
    ));

    coordinator.wait_for_signal().await;

    info!(
        "Waiting for consumer to shutdown (timeout: {})",
        cfg.service.shutdown_timeout
    );
    match tokio::time::timeout(shutdown_timeout, consumer_handle).await {
        Ok(res) => match res {
            Ok(inner_res) => inner_res,
            Err(e) => Err(anyhow::anyhow!("Consumer task join error: {}", e)),
        },
        Err(_) => {
            error!("Shutdown timed out, forcing exit");
            Err(anyhow::anyhow!("Shutdown timed out"))
        }
    }
}