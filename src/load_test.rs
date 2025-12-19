use crate::config::Config;
use anyhow::Result;
use memory_stats::memory_stats;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::{Duration, Instant};
use sysinfo::{CpuRefreshKind, RefreshKind, System};
use tokio::time;
use tokio::time::MissedTickBehavior;

pub async fn run(cfg: Config, rate: u32, duration_sec: u64, producers: usize) -> Result<()> {
    if rate == 0 {
        return Err(anyhow::anyhow!("rate must be > 0"));
    }
    if producers == 0 {
        return Err(anyhow::anyhow!("producers must be > 0"));
    }

    let topic = cfg
        .pipelines
        .first()
        .ok_or_else(|| anyhow::anyhow!("No pipelines configured"))?
        .topic
        .as_str();

    let per_producer_rate = rate / producers as u32;
    if per_producer_rate == 0 {
        return Err(anyhow::anyhow!(
            "rate per producer must be > 0, increase rate or decrease producers"
        ));
    }

    println!(
        "Starting load test on topic {} with {} producers at total {} msg/s ({} msg/s per producer) for {}s",
        topic, producers, rate, per_producer_rate, duration_sec
    );

    let mut handles = vec![];

    for i in 0..producers {
        let cfg = cfg.clone();
        let handle = tokio::spawn(async move {
            run_single_producer(cfg, per_producer_rate, duration_sec, i).await
        });
        handles.push(handle);
    }

    let mut total_sent = 0;
    let mut total_latencies = vec![];

    for handle in handles {
        let (sent, latencies) = handle.await??;
        total_sent += sent;
        total_latencies.extend(latencies);
    }

    let total_time = duration_sec as f64;
    let total_throughput = total_sent as f64 / total_time;
    let avg_latency = if !total_latencies.is_empty() {
        total_latencies.iter().sum::<Duration>() / total_latencies.len() as u32
    } else {
        Duration::from_nanos(0)
    };

    println!("Load test complete.");
    println!("Total messages sent: {}", total_sent);
    println!("Average throughput: {:.0} msg/s", total_throughput);
    println!("Average latency: {:.2}ms", avg_latency.as_millis());

    Ok(())
}

async fn run_single_producer(
    cfg: Config,
    rate: u32,
    duration_sec: u64,
    producer_id: usize,
) -> Result<(u64, Vec<Duration>)> {
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

    let start = Instant::now();
    let interval = Duration::from_micros(1_000_000 / rate as u64);
    let mut interval_timer = time::interval(interval);
    interval_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut count = 0;
    let mut latencies = Vec::new();
    let mut last_report = start;
    let mut last_count = 0;

    // Initialize system monitor
    let mut sys = System::new_with_specifics(
        RefreshKind::new().with_cpu(CpuRefreshKind::new().with_cpu_usage()),
    );

    while start.elapsed().as_secs() < duration_sec {
        interval_timer.tick().await;

        let payload = serde_json::json!({
            "payload": {
                "op": "c",
                "after": {
                    "id": count,
                    "producer_id": producer_id,
                    "data": "synthetic-data",
                    "ts": chrono::Utc::now().to_rfc3339()
                }
            }
        })
        .to_string();

        let key = format!("{}-{}", producer_id, count);
        let send_start = Instant::now();
        let record = FutureRecord::to(topic).payload(&payload).key(&key);

        match producer.send(record, Duration::from_secs(5)).await {
            Ok(_) => {
                let latency = send_start.elapsed();
                latencies.push(latency);
            }
            Err((e, _)) => {
                eprintln!("Producer {}: Failed to send: {}", producer_id, e);
            }
        }

        count += 1;

        // Report stats every 5 seconds per producer
        if last_report.elapsed().as_secs() >= 5 {
            let elapsed = last_report.elapsed().as_secs_f64();
            let msgs_per_sec = (count - last_count) as f64 / elapsed;
            let avg_latency = if !latencies.is_empty() {
                latencies.iter().sum::<Duration>() / latencies.len() as u32
            } else {
                Duration::from_nanos(0)
            };

            // Memory usage
            let memory_usage = if let Some(usage) = memory_stats() {
                usage.physical_mem / 1024 / 1024 // MB
            } else {
                0
            };

            // CPU usage
            sys.refresh_cpu();
            let cpu_usage =
                sys.cpus().iter().map(|cpu| cpu.cpu_usage()).sum::<f32>() / sys.cpus().len() as f32;

            println!(
                "Producer {}: {:.0} msg/s, avg latency: {:.2}ms, memory: {}MB, CPU: {:.1}%, sent: {}",
                producer_id,
                msgs_per_sec,
                avg_latency.as_millis(),
                memory_usage,
                cpu_usage,
                count
            );

            latencies.clear();
            last_report = Instant::now();
            last_count = count;
        }
    }

    Ok((count, latencies))
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
                memory: crate::config::MemoryConfig::default(),
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
        let res = run(cfg, 0, 1, 1).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_run_no_pipelines_errors() {
        let cfg = create_test_config(vec![]);
        let res = run(cfg, 1000, 1, 1).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_run_producers_zero_errors() {
        let cfg = create_test_config(vec![crate::eip::PipelineConfig {
            name: "test".to_string(),
            topic: "test".to_string(),
            debezium_envelope: false,
            staging_table: "test".to_string(),
            required_fields: vec![],
            backpressure: crate::eip::BackpressureConfig::default(),
            batching: crate::eip::BatchingConfig::default(),
            circuit_breaker: crate::circuit_breaker::CircuitBreakerConfig::default(),
            worker_threads: 1,
            dlq: None,
            stages: vec![],
        }]);
        let res = run(cfg, 1000, 1, 0).await;
        assert!(res.is_err());
    }
}
