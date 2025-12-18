use crate::config::Config;
use anyhow::Result;
use chrono::Utc;
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
            "op": "c",
            "after": {
                "id": count,
                "data": "synthetic-data",
                "ts": chrono::Utc::now().to_rfc3339()
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
