use crate::config::Config;
use anyhow::Result;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::{Duration, Instant};
use tokio::time;
use tokio::time::MissedTickBehavior;

pub async fn run(cfg: Config, rate: u32, duration_sec: u64) -> Result<()> {
    if rate == 0 {
        return Err(anyhow::anyhow!("rate must be > 0"));
    }

    let producer: FutureProducer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", &cfg.kafka.brokers)
        .set("message.timeout.ms", "5000")
        .create()?;

    let topic = cfg
        .pipelines
        .first()
        .ok_or_else(|| anyhow::anyhow!("No pipelines configured"))?
        .topic
        .as_str();
    println!(
        "Starting load test on topic {} at {} msg/s for {}s",
        topic, rate, duration_sec
    );

    let start = Instant::now();
    let interval = Duration::from_micros(1_000_000 / rate as u64);
    let mut interval_timer = time::interval(interval);
    interval_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut count = 0;

    while start.elapsed().as_secs() < duration_sec {
        interval_timer.tick().await;

        let payload = serde_json::json!({
            "payload": {
                "op": "c",
                "after": {
                    "id": count,
                    "data": "synthetic-data",
                    "ts": chrono::Utc::now().to_rfc3339()
                }
            }
        })
        .to_string();

        let key = count.to_string();
        let record = FutureRecord::to(topic).payload(&payload).key(&key);

        if let Err((e, _)) = producer.send(record, Duration::from_secs(5)).await {
            eprintln!("Failed to send: {}", e);
        }

        count += 1;
        if count % rate == 0 {
            println!("Sent {} messages", count);
        }
    }

    println!("Load test complete. Sent {} messages.", count);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    /// Helper function to create a minimal test configuration with placeholder values
    fn create_test_config(pipelines: Vec<crate::eip::PipelineConfig>) -> Config {
        Config {
            service: crate::config::ServiceConfig {
                log_level: crate::config::LogLevel::Info,
                metrics_port: 9090,
                otlp_endpoint: None,
                health_check_timeout_ms: 5000,
                shutdown_timeout: "30s".to_string(),
            },
            kafka: crate::config::KafkaConfig {
                brokers: "localhost:9092".to_string(),
                group_id: "g".to_string(),
                security: None,
                fetch: None,
                session_timeout_ms: 30000,
                max_inflight_messages: 1,
                producer_retries: 1,
                dlq_message_timeout_ms: 5000,
                compression: "none".to_string(),
                dlq_readiness_timeout_secs: 30,
                dlq_readiness_backoff_secs: 1,
                staleness_threshold_seconds: 60,
            },
            postgres: crate::config::PostgresConfig {
                url: "postgres://user:pass@localhost/db".to_string(),
                ssl_mode: None,
                ssl_root_cert: None,
                pool: None,
                copy_enabled: false,
                copy_batch_rows: 1000,
                insert_batch_rows: 100,
                enable_offset_tracking: false,
            },
            retry: crate::retry::RetryConfig::default(),
            pipelines,
        }
    }

    #[tokio::test]
    async fn test_run_rate_zero_errors() {
        let cfg = create_test_config(vec![]);
        let res = run(cfg, 0, 1).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_run_no_pipelines_errors() {
        let cfg = create_test_config(vec![]);
        let res = run(cfg, 1000, 1).await;
        assert!(res.is_err());
    }
}
