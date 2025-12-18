use crate::config::KafkaConfig;
use crate::eip::DlqConfig;
use crate::errors::{ProcessingError, PublicErrorReason};
use crate::types::MessageContext;
use anyhow::Result;
use chrono::Utc;
use futures::StreamExt;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fs::File;
use std::io::Write;
use std::time::{Duration, Instant};
use tracing::warn;

const MAX_DLQ_PREVIEW_BYTES: usize = 4_096;

async fn wait_for_kafka_readiness(
    consumer: &StreamConsumer,
    readiness_timeout_secs: u64,
    readiness_backoff_secs: u64,
) -> Result<()> {
    let readiness_timeout = Duration::from_secs(readiness_timeout_secs);
    let readiness_backoff = Duration::from_secs(readiness_backoff_secs);
    let start = Instant::now();
    loop {
        match consumer.fetch_metadata(None, Duration::from_millis(100)) {
            Ok(_) => break,
            Err(e) => {
                if start.elapsed() > readiness_timeout {
                    return Err(anyhow::anyhow!(
                        "Kafka readiness check timed out after {}s: {}",
                        readiness_timeout.as_secs(),
                        e
                    ));
                }
                tokio::time::sleep(readiness_backoff).await;
            }
        }
    }
    Ok(())
}

pub fn build_producer(cfg: &KafkaConfig) -> Result<FutureProducer> {
    let producer: FutureProducer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", &cfg.brokers)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("retries", cfg.producer_retries.to_string())
        .set("compression.type", &cfg.compression)
        .set("message.timeout.ms", cfg.dlq_message_timeout_ms.to_string())
        .create()?;
    Ok(producer)
}

fn build_dlq_consumer(cfg: &KafkaConfig, group_id: String) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", &cfg.brokers)
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()?;
    Ok(consumer)
}

#[derive(Serialize)]
pub struct DlqPayload<'a> {
    pub context: &'a MessageContext,
    pub reason: PublicErrorReason,
    pub pipeline: &'a str,
    pub payload: Cow<'a, str>,
    pub original_size: usize,
    pub truncated: bool,
}

pub async fn publish(
    producer: &FutureProducer,
    pipeline_name: &str,
    dlq_cfg: &DlqConfig,
    ctx: &MessageContext,
    reason: &ProcessingError,
    raw_payload: &str,
) -> Result<()> {
    let correlation = ctx.correlation_id();
    let public_reason = reason.public_reason();
    let timestamp = Utc::now().to_rfc3339();
    let retry_count = "0";
    let original_size = raw_payload.len();
    let (payload, truncated) =
        sanitize_payload(raw_payload, original_size, dlq_cfg.max_payload_bytes);
    let body = serde_json::to_string(&DlqPayload {
        context: ctx,
        reason: public_reason.clone(),
        pipeline: pipeline_name,
        payload,
        original_size,
        truncated,
    })?;

    let headers = OwnedHeaders::new()
        .insert(Header {
            key: "error_type",
            value: Some(public_reason.code.as_bytes()),
        })
        .insert(Header {
            key: "timestamp",
            value: Some(timestamp.as_bytes()),
        })
        .insert(Header {
            key: "retry_count",
            value: Some(retry_count.as_bytes()),
        })
        .insert(Header {
            key: "original_topic",
            value: Some(ctx.topic.as_bytes()),
        });

    let record = FutureRecord::to(&dlq_cfg.topic)
        .payload(&body)
        .key(&correlation)
        .headers(headers);

    producer
        .send(record, Duration::from_secs(5))
        .await
        .map_err(|(e, _)| anyhow::anyhow!("dlq send failed: {e}"))?;
    Ok(())
}

fn sanitize_payload<'a>(
    raw_payload: &'a str,
    original_size: usize,
    max_payload_bytes: usize,
) -> (Cow<'a, str>, bool) {
    if original_size <= max_payload_bytes {
        return (Cow::Borrowed(raw_payload), false);
    }

    let preview_limit = max_payload_bytes.min(MAX_DLQ_PREVIEW_BYTES);
    let preview = truncate_utf8(raw_payload, preview_limit);
    let payload = format!("{}...<payload omitted - too large>", preview);

    (Cow::Owned(payload), true)
}

fn truncate_utf8(input: &str, max_bytes: usize) -> &str {
    if input.is_empty() || max_bytes == 0 {
        return "";
    }

    let mut end = 0;
    for (idx, ch) in input.char_indices() {
        let next = idx + ch.len_utf8();
        if next > max_bytes {
            break;
        }
        end = next;
    }

    &input[..end]
}

#[derive(Deserialize)]
struct OwnedDlqPayload {
    context: MessageContext,
    #[allow(dead_code)]
    reason: OwnedPublicErrorReason,
    #[allow(dead_code)]
    pipeline: String,
    payload: String,
    #[allow(dead_code)]
    original_size: usize,
    truncated: bool,
}

#[derive(Deserialize)]
struct OwnedPublicErrorReason {
    #[allow(dead_code)]
    code: String,
    #[allow(dead_code)]
    message: String,
}

pub async fn drain(
    cfg: &KafkaConfig,
    topic: &str,
    output_file: Option<String>,
    requeue: bool,
) -> Result<()> {
    let consumer = build_dlq_consumer(cfg, format!("rcl-dlq-drain-{}", uuid::Uuid::new_v4()))?;

    consumer.subscribe(&[topic])?;

    let producer = if requeue {
        Some(build_producer(cfg)?)
    } else {
        None
    };

    let mut file = if let Some(path) = output_file {
        Some(File::create(path)?)
    } else {
        None
    };

    println!("Draining DLQ topic: {}", topic);

    wait_for_kafka_readiness(
        &consumer,
        cfg.dlq_readiness_timeout_secs,
        cfg.dlq_readiness_backoff_secs,
    )
    .await?;

    let mut count = 0;
    let mut stream = consumer.stream();

    loop {
        match tokio::time::timeout(Duration::from_secs(2), stream.next()).await {
            Ok(Some(Ok(msg))) => {
                if let Some(payload_str) = msg.payload_view::<str>().and_then(|r| r.ok()) {
                    match serde_json::from_str::<OwnedDlqPayload>(payload_str) {
                        Ok(dlq_msg) => {
                            if let Some(p) = &producer {
                                if dlq_msg.truncated {
                                    eprintln!(
                                        "Skipping requeue for truncated message at offset {}",
                                        msg.offset()
                                    );
                                    if let Some(f) = &mut file {
                                        writeln!(f, "{}", payload_str)?;
                                    }
                                } else {
                                    if let Some(f) = &mut file {
                                        writeln!(f, "{}", payload_str)?;
                                    }
                                    let key = dlq_msg.context.correlation_id();
                                    let record = FutureRecord::to(&dlq_msg.context.topic)
                                        .payload(&dlq_msg.payload)
                                        .key(&key);

                                    match p.send(record, Duration::from_secs(5)).await {
                                        Ok(_) => {
                                            println!(
                                                "Requeued message from offset {} to {}",
                                                msg.offset(),
                                                dlq_msg.context.topic
                                            );
                                        }
                                        Err((e, _)) => {
                                            eprintln!(
                                                "Failed to requeue message at offset {}: {}",
                                                msg.offset(),
                                                e
                                            );
                                        }
                                    }
                                    println!(
                                        "Requeued message from offset {} to {}",
                                        msg.offset(),
                                        dlq_msg.context.topic
                                    );
                                }
                            } else if let Some(f) = &mut file {
                                writeln!(f, "{}", payload_str)?;
                            }
                        }
                        Err(e) => {
                            eprintln!(
                                "Failed to parse DLQ message at offset {}: {}",
                                msg.offset(),
                                e
                            );
                        }
                    }
                } else {
                    warn!(
                        topic = %msg.topic(),
                        partition = msg.partition(),
                        offset = msg.offset(),
                        payload_len = msg.payload().map(|p| p.len()).unwrap_or(0),
                        "Skipping DLQ message with invalid UTF-8 payload"
                    );
                }

                // Commit offset to mark as processed (drained)
                let mut tpl = rdkafka::TopicPartitionList::new();
                tpl.add_partition_offset(
                    topic,
                    msg.partition(),
                    rdkafka::Offset::Offset(msg.offset() + 1),
                )?;
                consumer.commit(&tpl, rdkafka::consumer::CommitMode::Async)?;

                count += 1;
            }
            Ok(Some(Err(e))) => {
                eprintln!("Error receiving message: {}", e);
                break;
            }
            Ok(None) => break,
            Err(_) => {
                println!("Drain complete (timeout). Processed {} messages.", count);
                break;
            }
        }
    }

    Ok(())
}

/// Prints up to `limit` messages from the configured DLQ `topic`.
///
/// Subscribes a temporary consumer, waits for Kafka readiness, then receives and prints each message's UTF-8 payload or a notice when the payload is invalid UTF-8. The operation stops after `limit` messages, on a receive error, or on a receive timeout.
///
/// `cfg` is the Kafka configuration, `topic` is the DLQ topic name to inspect, and `limit` is the maximum number of messages to print.
///
/// # Examples
///
/// ```no_run
/// // Inspect up to 10 messages from the "my-dlq" topic.
/// // Requires an async runtime (tokio) and a valid KafkaConfig.
/// # async fn example(cfg: &crate::kafka::KafkaConfig) -> anyhow::Result<()> {
/// crate::dlq::inspect(cfg, "my-dlq", 10).await?;
/// # Ok(())
/// # }
/// ```
pub async fn inspect(cfg: &KafkaConfig, topic: &str, limit: usize) -> Result<()> {
    let consumer = build_dlq_consumer(cfg, format!("rcl-dlq-inspector-{}", uuid::Uuid::new_v4()))?;

    consumer.subscribe(&[topic])?;

    println!("Inspecting DLQ topic: {}", topic);
    let mut count = 0;

    wait_for_kafka_readiness(
        &consumer,
        cfg.dlq_readiness_timeout_secs,
        cfg.dlq_readiness_backoff_secs,
    )
    .await?;

    while count < limit {
        match tokio::time::timeout(Duration::from_secs(5), consumer.recv()).await {
            Ok(Ok(m)) => {
                if let Some(payload) = m.payload_view::<str>() {
                    match payload {
                        Ok(s) => println!("Offset {}: {}", m.offset(), s),
                        Err(e) => println!("Offset {}: [Invalid UTF-8] {:?}", m.offset(), e),
                    }
                }
                count += 1;
            }
            Ok(Err(e)) => {
                eprintln!("Error receiving message: {}", e);
                break;
            }
            Err(_) => {
                println!("Timeout waiting for messages.");
                break;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::MessageContext;

    #[test]
    fn test_truncate_utf8_empty_and_zero() {
        assert_eq!(truncate_utf8("", 10), "");
        assert_eq!(truncate_utf8("hello", 0), "");
    }

    #[test]
    fn test_truncate_utf8_multibyte() {
        // 'é' is two bytes in UTF-8; ensure we don't cut in the middle
        let s = "aébc"; // bytes: a (1) é (2) b (1) c (1)
                        // limit 2 bytes should only include 'a'
        assert_eq!(truncate_utf8(s, 2), "a");
        // limit 3 bytes should include 'a' and 'é'
        assert_eq!(truncate_utf8(s, 3), "aé");
    }

    #[test]
    fn test_sanitize_payload_small() {
        let raw = "small payload";
        let (payload, truncated) = sanitize_payload(raw, raw.len(), 100);
        assert!(!truncated);
        assert!(matches!(payload, std::borrow::Cow::Borrowed(_)));
    }

    #[test]
    fn test_sanitize_payload_large() {
        let raw = "x".repeat(10_000);
        let (payload, truncated) = sanitize_payload(&raw, raw.len(), 1024);
        assert!(truncated);
        let s = payload.into_owned();
        assert!(s.contains("<payload omitted - too large>"));
    }

    #[test]
    fn test_dlq_payload_serialize() {
        let ctx = MessageContext::new("topic-a".to_string(), 1, 2, 3);
        let reason = crate::errors::ProcessingError::Validation(
            crate::errors::ValidationError::new("bad".to_string()),
        );
        let raw = "ok";
        let dlq_cfg = crate::eip::DlqConfig {
            topic: "dlq".to_string(),
            max_payload_bytes: 1024,
            max_retries: 3,
        };

        let (payload, truncated) = sanitize_payload(raw, raw.len(), dlq_cfg.max_payload_bytes);
        let body = serde_json::to_string(&DlqPayload {
            context: &ctx,
            reason: reason.public_reason(),
            pipeline: "p1",
            payload,
            original_size: raw.len(),
            truncated,
        })
        .unwrap();

        // Should be valid JSON and contain expected fields
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v.get("pipeline").and_then(|p| p.as_str()), Some("p1"));
        assert_eq!(v.get("payload").and_then(|p| p.as_str()), Some("ok"));
    }

    #[test]
    fn test_truncate_utf8_emoji_and_boundaries() {
        // emoji are 4 bytes; ensure truncation doesn't cut them
        let s = "a🙂b"; // a (1) 🙂 (4) b (1)
                        // limit 2 bytes -> only 'a'
        assert_eq!(truncate_utf8(s, 2), "a");
        // limit 5 bytes -> include emoji
        assert_eq!(truncate_utf8(s, 5), "a🙂");
    }

    #[test]
    fn test_sanitize_payload_zero_max() {
        let raw = "non empty";
        let (payload, truncated) = sanitize_payload(raw, raw.len(), 0);
        // with max 0 we should get an owned truncated placeholder
        assert!(truncated);
        let s = payload.into_owned();
        assert!(s.contains("payload omitted"));
    }

    #[test]
    fn test_sanitize_payload_within_limit() {
        let raw = "test payload";
        let original_size = raw.len();
        let (payload, truncated) = sanitize_payload(raw, original_size, 1000);

        assert!(!truncated);
        assert_eq!(payload.as_ref(), raw);
    }

    #[test]
    fn test_sanitize_payload_exceeds_limit() {
        let raw = "this is a very long payload that exceeds the max size";
        let original_size = raw.len();
        let max = 20;

        let (payload, truncated) = sanitize_payload(raw, original_size, max);
        assert!(truncated);
        let s = payload.into_owned();
        assert!(s.contains("payload omitted"));
    }

    #[test]
    fn test_truncate_utf8_empty() {
        assert_eq!(truncate_utf8("", 100), "");
    }

    #[test]
    fn test_truncate_utf8_zero_max() {
        assert_eq!(truncate_utf8("hello", 0), "");
    }

    #[test]
    fn test_truncate_utf8_exact_fit() {
        let s = "hello";
        assert_eq!(truncate_utf8(s, 5), "hello");
    }

    #[test]
    fn test_truncate_utf8_mid_char() {
        // Even with enough bytes for one multi-byte char, it truncates properly
        let s = "a🙂";
        assert_eq!(truncate_utf8(s, 2), "a"); // 2 bytes -> can't fit emoji (4 bytes)
    }

    #[test]
    fn test_dlq_payload_serialization() {
        let ctx = MessageContext::new("topic".to_string(), 0, 42, 123);
        let reason = crate::errors::PublicErrorReason {
            code: "ERR001".to_string(),
            message: "test error".to_string(),
        };

        let payload_str = DlqPayload {
            context: &ctx,
            reason,
            pipeline: "p1",
            payload: std::borrow::Cow::Borrowed("test"),
            original_size: 4,
            truncated: false,
        };

        let json = serde_json::to_string(&payload_str).unwrap();
        assert!(json.contains("topic"));
        assert!(json.contains("ERR001"));
        assert!(json.contains("test"));
    }

    #[test]
    fn test_dlq_payload_truncated_flag() {
        let ctx = MessageContext::new("topic".to_string(), 0, 42, 123);
        let reason = crate::errors::PublicErrorReason {
            code: "ERR001".to_string(),
            message: "test error".to_string(),
        };

        let payload_str = DlqPayload {
            context: &ctx,
            reason,
            pipeline: "p1",
            payload: std::borrow::Cow::Borrowed("test"),
            original_size: 1000,
            truncated: true,
        };

        let json = serde_json::to_string(&payload_str).unwrap();
        assert!(json.contains("\"truncated\":true"));
    }

    #[test]
    fn test_max_dlq_preview_bytes_constant() {
        assert_eq!(MAX_DLQ_PREVIEW_BYTES, 4_096);
    }

    #[test]
    fn test_truncate_utf8_multibyte_chars() {
        // Test with various multibyte characters
        let s = "a€b"; // € is 3 bytes
        assert_eq!(truncate_utf8(s, 2), "a");
        assert_eq!(truncate_utf8(s, 4), "a€");
    }
}