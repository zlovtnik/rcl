use crate::config::{DlqConfig, KafkaConfig};
use crate::errors::{ProcessingError, PublicErrorReason};
use crate::types::MessageContext;
use anyhow::Result;
use chrono::Utc;
use rdkafka::message::{Header, OwnedHeaders};

use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Serialize;
use std::borrow::Cow;
use std::time::Duration;

const MAX_DLQ_PREVIEW_BYTES: usize = 4_096;

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
    let original_size = raw_payload.as_bytes().len();
    let (payload, truncated) =
        sanitize_payload(raw_payload, original_size, dlq_cfg.max_payload_bytes);
    let body = serde_json::to_string(&DlqPayload {
        context: ctx,
        reason: public_reason,
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
