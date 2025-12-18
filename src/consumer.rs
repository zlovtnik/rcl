use crate::config::{Config, KafkaConfig, PipelineConfig};
use crate::decoder::decode_and_validate;
use crate::dlq;
use crate::errors::ProcessingError;
use crate::metrics::Metrics;
use crate::types::MessageContext;
use crate::writer::Writer;
use anyhow::Result;
use futures::StreamExt;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::OwnedMessage;
use rdkafka::Message;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info, warn};

pub async fn run(cfg: Arc<Config>, writer: Arc<Writer>, metrics: Metrics) -> Result<()> {
    let pipelines_by_topic: HashMap<String, PipelineConfig> = cfg
        .pipelines
        .iter()
        .map(|p| (p.topic.clone(), p.clone()))
        .collect();
    let capacity = cfg
        .pipelines
        .iter()
        .map(|p| p.backpressure.channel_capacity)
        .max()
        .unwrap_or(1024);

    let consumer = Arc::new(build_consumer(&cfg.kafka)?);
    consumer.subscribe(
        &cfg.pipelines
            .iter()
            .map(|p| p.topic.as_str())
            .collect::<Vec<_>>(),
    )?;

    let dlq_enabled = cfg.pipelines.iter().any(|p| p.dlq.is_some());
    let producer = if dlq_enabled {
        Some(dlq::build_producer(&cfg.kafka)?)
    } else {
        None
    };
    let (tx, rx) = mpsc::channel::<(MessageContext, OwnedMessage)>(capacity);

    let fetch_loop = {
        let consumer = consumer.clone();
        let metrics = metrics.clone();
        tokio::spawn(async move {
            let mut stream = consumer.stream();
            while let Some(message) = stream.next().await {
                match message {
                    Ok(msg) => {
                        let ctx = MessageContext {
                            topic: msg.topic().to_string(),
                            partition: msg.partition(),
                            offset: msg.offset(),
                        };
                        metrics.messages_total.inc();
                        if let Some(ts) = msg.timestamp().to_millis() {
                            if let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH) {
                                let lag = now.as_millis() as i64 - ts;
                                metrics
                                    .lag_ms
                                    .with_label_values(&[&ctx.topic, &ctx.partition.to_string()])
                                    .set(lag);
                            }
                        }
                        if tx.send((ctx, msg.detach())).await.is_err() {
                            warn!("channel closed; stopping fetch loop");
                            break;
                        }
                    }
                    Err(err) => {
                        metrics.processing_failures.inc();
                        warn!(error = %err, "consumer poll error");
                    }
                }
            }
        })
    };

    let processing_loop = {
        let metrics = metrics.clone();
        let pipelines_by_topic = pipelines_by_topic.clone();
        let producer = producer.clone();
        let consumer = consumer.clone();
        let writer = writer.clone();
        tokio::spawn(async move {
            let mut stream = ReceiverStream::new(rx);
            while let Some((ctx, msg)) = stream.next().await {
                let pipeline = match pipelines_by_topic.get(&ctx.topic) {
                    Some(p) => p,
                    None => {
                        warn!(context = %ctx.correlation_id(), "no pipeline for topic");
                        continue;
                    }
                };

                let payload = match msg.payload_view::<str>() {
                    Some(Ok(p)) => p.to_string(),
                    Some(Err(e)) => {
                        metrics.decode_failures.inc();
                        error!(context = %ctx.correlation_id(), error = %e, "utf8 decode failed");
                        continue;
                    }
                    None => {
                        metrics.decode_failures.inc();
                        warn!(context = %ctx.correlation_id(), "empty payload");
                        continue;
                    }
                };

                match handle_message(&payload, pipeline, &writer).await {
                    Ok(_) => {
                        if let Err(err) = commit_offset(&consumer, &ctx) {
                            metrics.processing_failures.inc();
                            warn!(context = %ctx.correlation_id(), error = %err, "offset commit failed");
                        }
                    }
                    Err(reason) => {
                        metrics.decode_failures.inc();
                        let public_reason = reason.public_reason();
                        if let (Some(producer), Some(dlq_cfg)) =
                            (producer.as_ref(), pipeline.dlq.as_ref())
                        {
                            if let Err(err) = dlq::publish(
                                producer,
                                &pipeline.name,
                                dlq_cfg,
                                &ctx,
                                &reason,
                                &payload,
                            )
                            .await
                            {
                                error!(
                                    context = %ctx.correlation_id(),
                                    error = %err,
                                    "dlq publish failed"
                                );
                            } else {
                                metrics.dlq_total.inc();
                                warn!(
                                    context = %ctx.correlation_id(),
                                    reason_code = %public_reason.code,
                                    reason_message = %public_reason.message,
                                    "sent to dlq"
                                );
                            }
                        } else {
                            warn!(
                                context = %ctx.correlation_id(),
                                reason_code = %public_reason.code,
                                reason_message = %public_reason.message,
                                "dlq disabled; message skipped"
                            );
                        }
                    }
                }
            }
        })
    };

    tokio::try_join!(fetch_loop, processing_loop)?;
    info!("consumer loops stopped");
    Ok(())
}

fn build_consumer(cfg: &KafkaConfig) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", &cfg.brokers)
        .set("group.id", &cfg.group_id)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", cfg.session_timeout_ms.to_string())
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set(
            "max.in.flight.requests.per.connection",
            cfg.max_inflight_messages.to_string(),
        )
        .create()?;
    Ok(consumer)
}

async fn handle_message(
    payload: &str,
    pipeline: &PipelineConfig,
    writer: &Writer,
) -> Result<(), ProcessingError> {
    let decoded = decode_and_validate(payload.as_bytes(), pipeline)?;
    writer.write(&decoded, pipeline).await?;
    Ok(())
}

fn commit_offset(
    consumer: &StreamConsumer,
    ctx: &MessageContext,
) -> Result<(), rdkafka::error::KafkaError> {
    let mut tpl = rdkafka::TopicPartitionList::new();
    tpl.add_partition_offset(
        &ctx.topic,
        ctx.partition,
        rdkafka::Offset::Offset(ctx.offset + 1),
    )?;
    consumer.commit(&tpl, CommitMode::Async)
}
