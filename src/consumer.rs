use crate::batcher::{Batcher, BatcherConfig, FlushReason};
use crate::circuit_breaker::CircuitBreaker;
use crate::config::{Config, KafkaConfig};
use crate::decoder::decode_and_validate;
use crate::dlq;
use crate::eip::{MessageMetadata, Pipeline, StageContext};
use crate::errors::{ProcessingError, ValidationError};
use crate::health::{ComponentStatus, HealthRegistry};
use crate::metrics::Metrics;
use crate::offset_tracker::OffsetTracker;
use crate::shutdown::ShutdownCoordinator;
use crate::types::MessageContext;
use crate::writer::Writer;
use anyhow::Result;
use futures::StreamExt;
use rdkafka::Message;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Headers;
use rdkafka::message::OwnedMessage;
use rdkafka::producer::FutureProducer;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, mpsc};
use tokio::time::{Duration, interval};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info, warn};

use rdkafka::Offset;
use rdkafka::TopicPartitionList;
use rdkafka::message::BorrowedHeaders;

/// Extracts the retry count from Kafka message headers.
///
/// Iterates through message headers to find the "retry_count" header, converts its value
/// from bytes to UTF-8, parses it to u32, and returns `Some(count)` on success or `None`
/// if the header is absent, malformed, or unparseable.
fn extract_retry_count(headers: Option<&BorrowedHeaders>) -> Option<u32> {
    headers.and_then(|hdrs| {
        (0..hdrs.count()).find_map(|i| {
            let header = hdrs.get(i);
            if header.key == "retry_count" {
                let value = header.value?;
                let s = std::str::from_utf8(value).ok()?;
                s.parse::<u32>().ok()
            } else {
                None
            }
        })
    })
}

/// Holds per-pipeline message channels and metadata
#[derive(Clone)]
struct PipelineChannels {
    /// Sender for routing messages to this pipeline
    tx: mpsc::Sender<(MessageContext, OwnedMessage)>,
}

/// Manages a set of per-pipeline channels
struct PipelineChannelRegistry {
    /// Map from topic name to pipeline channels
    channels_by_topic: HashMap<String, PipelineChannels>,
}

impl PipelineChannelRegistry {
    /// Gets the sender for the given topic
    fn get_sender(&self, topic: &str) -> Option<mpsc::Sender<(MessageContext, OwnedMessage)>> {
        self.channels_by_topic.get(topic).map(|pc| pc.tx.clone())
    }
}

async fn run_fetch_loop(
    consumer: Arc<StreamConsumer>,
    channel_registry: Arc<parking_lot::Mutex<PipelineChannelRegistry>>,
    topic_to_pipeline: HashMap<String, String>,
    metrics: Metrics,
    health: Arc<HealthRegistry>,
    last_poll: Arc<AtomicU64>,
    memory_config: crate::config::MemoryConfig,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let mut stream = consumer.stream();
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("fetch loop shutting down");
                break;
            }
            message = tokio::time::timeout(Duration::from_secs(30), stream.next()) => {
                // Check memory usage before processing messages
                let should_pause = if let Some(usage) = memory_stats::memory_stats() {
                    let memory_bytes = usage.physical_mem;
                    let max_bytes = memory_config.max_memory_mb * 1024 * 1024;
                    memory_bytes > max_bytes.try_into().unwrap()
                } else {
                    false
                };

                if should_pause {
                    warn!("Memory usage high, pausing Kafka consumption");
                    // Sleep for a short time before checking again
                    tokio::time::sleep(Duration::from_millis(memory_config.memory_check_interval_ms)).await;
                    continue;
                }

                match message {
                    Ok(Some(Ok(msg))) => {
                        last_poll.store(
                            SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as u64,
                            Ordering::Relaxed,
                        );
                        let _ = health.set_kafka_status(ComponentStatus::Healthy);

                        let retry_count = extract_retry_count(msg.headers());

                        let ctx = MessageContext {
                            topic: msg.topic().to_string(),
                            partition: msg.partition(),
                            offset: msg.offset(),
                            timestamp: msg.timestamp().to_millis().unwrap_or(0),
                            retry_count,
                        };
                        metrics.messages_total.inc();
                        #[allow(clippy::collapsible_if)]
                        if let Some(ts) = msg.timestamp().to_millis() {
                            if let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH) {
                                let lag = now.as_millis() as i64 - ts;
                                metrics
                                    .lag_ms
                                    .with_label_values(&[&ctx.topic, &ctx.partition.to_string()])
                                    .set(lag);
                            }
                        }

                        // Route message to the correct pipeline channel
                        let tx = {
                            let registry = channel_registry.lock();
                            registry.get_sender(&ctx.topic)
                        };

                        if let Some(tx) = tx {
                            let topic_for_log = ctx.topic.clone();
                            let ctx_clone = ctx.clone();
                            if tx.send((ctx, msg.detach())).await.is_err() {
                                warn!("pipeline channel closed for topic {}", topic_for_log);
                            } else {
                                // Increment channel depth metric on successful send
                                if let Some(pipeline_name) = topic_to_pipeline.get(&ctx_clone.topic) {
                                    let gauge = metrics.channel_depth_per_pipeline.with_label_values(&[pipeline_name]);
                                    gauge.inc();
                                }
                            }
                        } else {
                            warn!("no pipeline channel for topic {}", ctx.topic);
                        }
                    }
                    Ok(Some(Err(err))) => {
                        last_poll.store(
                            SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as u64,
                            Ordering::Relaxed,
                        );
                        let _ = health.set_kafka_status(ComponentStatus::Unhealthy);
                        metrics.processing_failures.inc();
                        warn!(error = %err, "consumer poll error");
                    }
                    Ok(None) => break,
                    Err(_) => {
                        // timeout, empty poll
                        last_poll.store(
                            SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as u64,
                            Ordering::Relaxed,
                        );
                    }
                }
            }
        }
    }
}

async fn run_heartbeat_task(
    last_poll: Arc<AtomicU64>,
    health: Arc<HealthRegistry>,
    metrics: Metrics,
    staleness_threshold_seconds: u64,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let mut interval = interval(Duration::from_secs(30));
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("heartbeat task shutting down");
                break;
            }
            _ = interval.tick() => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                let last = last_poll.load(Ordering::Relaxed);
                metrics.last_poll_timestamp.set(last as i64);
                if now - last > staleness_threshold_seconds * 1000 {
                    let _ = health.set_kafka_status(ComponentStatus::Unhealthy);
                } else {
                    let _ = health.set_kafka_status(ComponentStatus::Healthy);
                }
            }
        }
    }
}

async fn run_circuit_breaker_metrics_task(
    circuit_breakers_by_pipeline: HashMap<String, Arc<CircuitBreaker>>,
    metrics: Metrics,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let mut interval = interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("circuit breaker metrics task shutting down");
                break;
            }
            _ = interval.tick() => {
                for (pipeline_name, cb) in &circuit_breakers_by_pipeline {
                    let state = cb.get_state();
                    let state_value = match state {
                        crate::circuit_breaker::CircuitBreakerState::Closed => 0,
                        crate::circuit_breaker::CircuitBreakerState::Open => 1,
                        crate::circuit_breaker::CircuitBreakerState::HalfOpen => 2,
                    };
                    metrics
                        .circuit_breaker_state
                        .with_label_values(&[pipeline_name])
                        .set(state_value);
                }
            }
        }
    }
}

async fn run_memory_monitor_task(
    metrics: Metrics,
    memory_config: crate::config::MemoryConfig,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let mut interval = interval(Duration::from_millis(memory_config.memory_check_interval_ms));
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("memory monitor task shutting down");
                break;
            }
            _ = interval.tick() => {
                if let Some(usage) = memory_stats::memory_stats() {
                    let memory_bytes = usage.physical_mem as i64;
                    metrics.memory_usage_bytes.set(memory_bytes);

                    // Check if memory usage exceeds limit
                    let max_bytes = (memory_config.max_memory_mb * 1024 * 1024) as i64;
                    if memory_bytes > max_bytes {
                        warn!(
                            memory_used_mb = memory_bytes / 1024 / 1024,
                            memory_limit_mb = memory_config.max_memory_mb,
                            "Memory usage exceeds configured limit"
                        );
                    }
                }
            }
        }
    }
}

/// Processes incoming Kafka messages from the given receiver for a specific pipeline.
///
/// Receives messages for a single pipeline from `rx`, routes each message through the pipeline
/// configuration, decodes the message payload as UTF-8, and enqueues produced messages
/// into the pipeline's batcher. Decode or processing failures are handled and the original message
/// offset is committed or skipped as appropriate.
///
/// The loop runs until the receiver is closed; when finished, it logs that processing has stopped.
async fn run_pipeline_processing_loop(
    pipeline: Pipeline,
    rx: mpsc::Receiver<(MessageContext, OwnedMessage)>,
    context: Arc<PipelineProcessingContext>,
) {
    let mut stream = ReceiverStream::new(rx);

    while let Some((ctx, msg)) = stream.next().await {
        // Decrement channel depth metric on message receipt
        let gauge = context.metrics.channel_depth_per_pipeline.with_label_values(&[&pipeline.name]);
        gauge.dec();

        // Check circuit breaker before processing
        if context.circuit_breaker.try_execute().is_err() {
            // Extract payload for DLQ publishing
            let payload = match msg.payload_view::<str>() {
                Some(Ok(p)) => p.to_string(),
                Some(Err(e)) => {
                    warn!(
                        context = %ctx.correlation_id(),
                        pipeline_name = %pipeline.name,
                        error = %e,
                        "circuit breaker open and failed to extract payload for DLQ"
                    );
                    // Commit offset since we can't process or DLQ this message
                    context.commit_offset(&ctx).await;
                    continue;
                }
                None => {
                    warn!(
                        context = %ctx.correlation_id(),
                        pipeline_name = %pipeline.name,
                        "circuit breaker open and message has no payload for DLQ"
                    );
                    // Commit offset since we can't process or DLQ this message
                    context.commit_offset(&ctx).await;
                    continue;
                }
            };

            // Create circuit breaker error and send to DLQ
            let circuit_breaker_error = ProcessingError::Validation(ValidationError::new(
                "circuit breaker is open - message sent to DLQ for preservation".to_string(),
            ));

            context
                .handle_processing_error(&pipeline, &ctx, &circuit_breaker_error, &payload)
                .await;
            continue;
        }

        let payload = match msg.payload_view::<str>() {
            Some(Ok(p)) => p.to_string(),
            Some(Err(e)) => {
                context
                    .handle_decode_error(&pipeline, &ctx, format!("utf8 decode failed: {}", e))
                    .await;
                context.circuit_breaker.record_failure();
                continue;
            }
            None => {
                context
                    .handle_decode_error(&pipeline, &ctx, "empty payload".to_string())
                    .await;
                context.circuit_breaker.record_failure();
                continue;
            }
        };

        match context.process_message(&payload, &pipeline, &ctx).await {
            Ok(_) => {
                // Message added to batcher, offset will be committed after batch write
                context.circuit_breaker.record_success();
            }
            Err(reason) => {
                context
                    .handle_processing_error(&pipeline, &ctx, &reason, &payload)
                    .await;
                context.circuit_breaker.record_failure();
            }
        }
    }

    info!(pipeline_name = %pipeline.name, "pipeline processing loop finished");
}

/// Shared context for pipeline processing
struct PipelineProcessingContext {
    consumer: Arc<StreamConsumer>,
    batcher: Arc<tokio::sync::Mutex<Batcher>>,
    metrics: Metrics,
    health: Arc<HealthRegistry>,
    producer: Option<FutureProducer>,
    circuit_breaker: Arc<CircuitBreaker>,
}

impl PipelineProcessingContext {
    /// Enqueues a processed message and its context into the batcher for the specified pipeline and table.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the message was accepted into the batcher, `Err(ProcessingError)` if adding failed.
    async fn add_to_batcher(
        &self,
        pipeline_name: &str,
        table: &str,
        message: Value,
        context: &MessageContext,
    ) -> Result<(), ProcessingError> {
        info!(context = %context.correlation_id(), "adding to batcher");
        let mut batcher_guard = self.batcher.lock().await;
        batcher_guard
            .add_message(pipeline_name, table, message, context.clone())
            .await
    }

    /// Commit the consumer offset for the provided message context.
    async fn commit_offset(&self, ctx: &MessageContext) {
        let mut tpl = TopicPartitionList::new();
        if let Err(e) =
            tpl.add_partition_offset(&ctx.topic, ctx.partition, Offset::Offset(ctx.offset + 1))
        {
            warn!("Failed to add partition offset to list: {}", e);
            return;
        }
        if let Err(e) = self.consumer.commit(&tpl, CommitMode::Async) {
            self.metrics.processing_failures.inc();
            warn!(context = %ctx.correlation_id(), error = %e, "offset commit failed");
        } else {
            info!(context = %ctx.correlation_id(), "committed offset for failed message");
        }
    }

    /// Handle a message decoding failure for a pipeline and finalize the message offset.
    async fn handle_decode_error(
        &self,
        pipeline: &Pipeline,
        ctx: &MessageContext,
        error_msg: String,
    ) {
        if let Err(err) = self
            .health
            .update_pipeline_error(&pipeline.name, error_msg.clone())
        {
            warn!("Failed to update pipeline error status: {}", err);
        }
        self.metrics.decode_failures.inc();
        warn!(context = %ctx.correlation_id(), error = %error_msg, "decode error");
        self.commit_offset(ctx).await;
    }

    /// Process a single message payload through the given pipeline
    async fn process_message(
        &self,
        payload: &str,
        pipeline: &Pipeline,
        ctx: &MessageContext,
    ) -> Result<(), ProcessingError> {
        info!(context = %ctx.correlation_id(), topic = %ctx.topic, partition = %ctx.partition, offset = %ctx.offset, "processing message");
        let mut decoded = decode_and_validate(payload.as_bytes(), &pipeline.config)?;

        if let serde_json::Value::Object(ref mut map) = decoded {
            map.insert(
                "_meta_topic".to_string(),
                serde_json::Value::String(ctx.topic.clone()),
            );
            map.insert(
                "_meta_partition".to_string(),
                serde_json::Value::Number(ctx.partition.into()),
            );
            map.insert(
                "_meta_offset".to_string(),
                serde_json::Value::Number(ctx.offset.into()),
            );
            map.insert(
                "_meta_ingest_ts".to_string(),
                serde_json::Value::Number(ctx.timestamp.into()),
            );
        }

        // Execute EIP pipeline
        let stage_ctx = StageContext {
            correlation_id: ctx.correlation_id().to_string(),
            pipeline_name: pipeline.name.clone(),
            message_metadata: MessageMetadata::from_kafka(
                ctx.topic.clone(),
                ctx.partition,
                ctx.offset,
                Some(ctx.timestamp),
            ),
        };

        info!(context = %ctx.correlation_id(), pipeline = %pipeline.name, stages = pipeline.stages.len(), "executing pipeline stages");
        let processed_messages = pipeline.execute(&stage_ctx, decoded).await?;
        info!(context = %ctx.correlation_id(), messages_after_stages = processed_messages.len(), "pipeline execution completed");

        // Batch processed messages
        for msg in processed_messages {
            // Check for routing metadata
            let candidate = msg
                .get("_destination_table")
                .and_then(|v| v.as_str())
                .unwrap_or(&pipeline.config.staging_table);

            let table = crate::config::validate_table_identifier(candidate)
                .map_err(|e| ProcessingError::Validation(ValidationError::new(e.to_string())))?;

            info!(context = %ctx.correlation_id(), table = %table, "adding to batcher");
            self.add_to_batcher(&pipeline.name, table, msg.clone(), ctx)
                .await?;
        }

        Ok(())
    }

    /// Handle a processing error for a message
    async fn handle_processing_error(
        &self,
        pipeline: &Pipeline,
        ctx: &MessageContext,
        reason: &ProcessingError,
        payload: &str,
    ) {
        if let Err(err) = self
            .health
            .update_pipeline_error(&pipeline.name, reason.to_string())
        {
            warn!("Failed to update pipeline error status: {}", err);
        }

        match reason {
            ProcessingError::Transport(_) => self.metrics.processing_failures.inc(),
            _ => self.metrics.decode_failures.inc(),
        }

        let public_reason = reason.public_reason();
        if let Some(producer) = self.producer.as_ref() {
            if let Some(dlq_cfg) = pipeline.config.dlq.as_ref() {
                if let Err(err) = dlq::publish(
                    producer,
                    &pipeline.name,
                    dlq_cfg,
                    ctx,
                    reason,
                    payload,
                    ctx.retry_count,
                )
                .await
                {
                    error!(
                        context = %ctx.correlation_id(),
                        error = %err,
                        "dlq publish failed"
                    );
                } else {
                    self.metrics.dlq_total.inc();
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
        } else {
            warn!(
                context = %ctx.correlation_id(),
                reason_code = %public_reason.code,
                reason_message = %public_reason.message,
                "dlq disabled; message skipped"
            );
        }
        self.commit_offset(ctx).await;
    }
}

/// Process maps of committed offsets and commit them to Kafka, updating metrics on failures.
///
/// This function consumes `HashMap<(String, i32), i64>` values from `rx`, builds a topic/partition
/// list from each map, and asks the provided `consumer` to commit those offsets. On commit
/// errors it increments the `processing_failures` metric and logs a warning; on success it logs
/// the number of committed partitions.
///
/// # Parameters
///
/// - `rx`: receiver that yields maps from `(topic, partition)` to `offset` to be committed.
/// - `consumer`: Kafka consumer used to perform the offset commits.
/// - `metrics`: metrics collector whose `processing_failures` counter is incremented when commits fail.
///
/// # Examples
///
/// ```no_run
/// use std::collections::HashMap;
/// use std::sync::Arc;
/// use rdkafka::consumer::StreamConsumer;
/// use tokio::sync::mpsc;
///
/// # async fn example(
/// #     consumer: Arc<StreamConsumer>,
/// #     metrics: crate::metrics::Metrics,
/// # ) {
/// let (tx, rx) = mpsc::unbounded_channel::<HashMap<(String, i32), i64>>();
///
/// // Start the background handler (consume commits until the sender is dropped)
/// tokio::spawn(run_committed_offsets_handler(rx, consumer.clone(), metrics.clone()));
///
/// // Send a map of committed offsets
/// let mut m = HashMap::new();
/// m.insert(("my-topic".to_string(), 0), 123_i64);
/// tx.send(m).unwrap();
/// # }
/// ```
async fn run_committed_offsets_handler(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<HashMap<(String, i32), i64>>,
    consumer: Arc<StreamConsumer>,
    metrics: Metrics,
) {
    while let Some(committed_offsets) = rx.recv().await {
        let mut tpl = TopicPartitionList::new();
        for ((topic, partition), offset) in committed_offsets {
            if let Err(e) = tpl.add_partition_offset(&topic, partition, Offset::Offset(offset)) {
                warn!("Failed to add partition offset to list: {}", e);
                continue;
            }
        }

        if let Err(e) = consumer.commit(&tpl, CommitMode::Async) {
            metrics.processing_failures.inc();
            warn!(error = %e, "offset commit failed");
        } else {
            info!("Committed offsets for {} partitions", tpl.count());
        }
    }
}

/// Replays messages from a Kafka topic partition through the configured pipeline and writer without batching.
///
/// Processes messages in the inclusive range [start_offset, end_offset] from the given topic and partition,
/// decoding each payload as UTF-8 and running it through the pipeline's processing logic. Stops and returns an error
/// on Kafka errors, invalid UTF-8 payloads, or processing failures; returns `Ok(())` after successfully processing
/// up to `end_offset`.
///
/// # Parameters
///
/// - `cfg`: application configuration containing pipeline definitions and Kafka settings.
/// - `writer`: writer used by pipelines during replay.
/// - `_metrics`: metrics collector (unused for replay but kept for API compatibility).
/// - `topic`: Kafka topic to replay.
/// - `partition`: partition index to replay.
/// - `start_offset`: starting offset (inclusive).
/// - `end_offset`: ending offset (inclusive).
///
/// # Returns
///
/// `Ok(())` if replay completes successfully; `Err` if a Kafka error, payload decoding error, or pipeline processing error occurs.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// # // assume Config, Writer and Metrics are constructed appropriately in real usage
/// # let cfg: Arc<Config> = Arc::new(Config::default());
/// # let writer: Arc<Writer> = Arc::new(Writer::new());
/// # let metrics = Metrics::default();
/// let _ = rcl::consumer::replay(cfg, writer, metrics, "my-topic".to_string(), 0, 0, 100).await;
/// ```
pub async fn replay(
    cfg: Arc<Config>,
    _writer: Arc<Writer>,
    _metrics: Metrics,
    topic: String,
    partition: i32,
    start_offset: i64,
    end_offset: i64,
) -> Result<()> {
    let pipelines_by_topic: HashMap<String, Pipeline> = cfg
        .pipelines
        .iter()
        .map(|p| {
            let pipeline = Pipeline::from_config(p)?;
            Ok((p.topic.clone(), pipeline))
        })
        .collect::<Result<HashMap<_, _>>>()?;

    let pipeline = pipelines_by_topic
        .get(&topic)
        .ok_or_else(|| anyhow::anyhow!("No pipeline found for topic {}", topic))?
        .clone();

    let consumer: StreamConsumer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", &cfg.kafka.brokers)
        .set("group.id", format!("rcl-replay-{}", uuid::Uuid::new_v4()))
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic, partition, Offset::Offset(start_offset))?;
    consumer.assign(&tpl)?;

    info!(
        "Replaying {}:{} from {} to {}",
        topic, partition, start_offset, end_offset
    );

    let mut stream = consumer.stream();

    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                if msg.offset() > end_offset {
                    info!("Reached end offset {}", end_offset);
                    break;
                }

                let retry_count = extract_retry_count(msg.headers());

                let ctx = MessageContext {
                    topic: msg.topic().to_string(),
                    partition: msg.partition(),
                    offset: msg.offset(),
                    timestamp: msg.timestamp().to_millis().unwrap_or(0),
                    retry_count,
                };

                match msg.payload() {
                    Some(payload) => match std::str::from_utf8(payload) {
                        Ok(payload_str) => {
                            // Decode and validate
                            match decode_and_validate(payload_str.as_bytes(), &pipeline.config) {
                                Ok(mut decoded) => {
                                    // Inject metadata
                                    if let serde_json::Value::Object(ref mut map) = decoded {
                                        map.insert(
                                            "_meta_topic".to_string(),
                                            serde_json::Value::String(ctx.topic.clone()),
                                        );
                                        map.insert(
                                            "_meta_partition".to_string(),
                                            serde_json::Value::Number(ctx.partition.into()),
                                        );
                                        map.insert(
                                            "_meta_offset".to_string(),
                                            serde_json::Value::Number(ctx.offset.into()),
                                        );
                                        map.insert(
                                            "_meta_ingest_ts".to_string(),
                                            serde_json::Value::Number(ctx.timestamp.into()),
                                        );
                                    }

                                    // Execute pipeline
                                    let stage_ctx = StageContext {
                                        correlation_id: ctx.correlation_id().to_string(),
                                        pipeline_name: pipeline.name.clone(),
                                        message_metadata: MessageMetadata::from_kafka(
                                            ctx.topic.clone(),
                                            ctx.partition,
                                            ctx.offset,
                                            Some(ctx.timestamp),
                                        ),
                                    };

                                    match pipeline.execute(&stage_ctx, decoded).await {
                                        Ok(_processed_messages) => {
                                            info!(context = %ctx.correlation_id(), "replayed successfully");
                                        }
                                        Err(e) => {
                                            error!(context = %ctx.correlation_id(), error = %e, "processing failed during replay; stopping");
                                            return Err(e.into());
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!(context = %ctx.correlation_id(), error = %e, "decode failed");
                                    return Err(e.into());
                                }
                            }
                        }
                        Err(e) => {
                            error!(context = %ctx.correlation_id(), error = %e, "invalid UTF-8 payload");
                            return Err(e.into());
                        }
                    },
                    None => {
                        warn!(context = %ctx.correlation_id(), "missing payload");
                    }
                }
            }
            Err(e) => {
                error!("Kafka error: {}", e);
                return Err(e.into());
            }
        }
    }

    Ok(())
}

async fn sync_offsets(
    cfg: &KafkaConfig,
    offset_tracker: &OffsetTracker,
    pipelines: &[crate::eip::PipelineConfig],
) -> Result<()> {
    info!("Syncing offsets from DB to Kafka...");
    let consumer = build_consumer(cfg)?;

    let mut tpl = TopicPartitionList::new();
    let mut count = 0;

    for pipeline in pipelines {
        let db_offsets = offset_tracker
            .read_topic_offsets(&pipeline.name, &pipeline.topic)
            .await?;
        for (partition, offset) in db_offsets {
            // We commit offset + 1 because Kafka expects the *next* offset to fetch
            tpl.add_partition_offset(&pipeline.topic, partition, Offset::Offset(offset + 1))?;
            count += 1;
        }
    }

    if count > 0 {
        info!("Found {} offsets to sync", count);
        // We must assign to commit
        consumer.assign(&tpl)?;
        consumer.commit(&tpl, CommitMode::Sync)?;
        info!("Offsets synced successfully");
    } else {
        info!("No offsets found in DB to sync");
    }

    Ok(())
}

/// Starts the consumer runtime: builds and subscribes the Kafka consumer, constructs pipelines and batcher,
/// spawns the fetch, processing, heartbeat, batcher flush, and committed-offsets handler tasks, and awaits their completion.
///
/// This function wires together metrics, health checks, optional DLQ producer, and a committed-offsets channel,
/// then runs the long-lived background tasks that consume, decode, execute pipelines, batch processed messages,
/// and commit offsets. It does not return until the spawned tasks exit or an error occurs during startup or task execution.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use tokio::sync::broadcast;
///
/// #[tokio::main]
/// async fn example() -> anyhow::Result<()> {
///     // Construct `cfg`, `writer`, `metrics`, and `health` according to your application.
///     let cfg = Arc::new(/* Config */ unimplemented!());
///     let writer = Arc::new(/* Writer */ unimplemented!());
///     let metrics = /* Metrics */ unimplemented!();
///     let health = Arc::new(/* HealthRegistry */ unimplemented!());
///     let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
///
///     // Start the consumer runtime (will run until shutdown or error).
///     run(cfg, writer, metrics, shutdown_rx, health).await?;
///     Ok(())
/// }
/// ```
pub async fn run(
    cfg: Arc<Config>,
    writer: Arc<Writer>,
    metrics: Metrics,
    shutdown_rx: broadcast::Receiver<()>,
    health: Arc<HealthRegistry>,
) -> Result<()> {
    let pipelines_by_topic: HashMap<String, Pipeline> = cfg
        .pipelines
        .iter()
        .map(|p| {
            let pipeline = Pipeline::from_config(p)?;
            Ok((p.topic.clone(), pipeline))
        })
        .collect::<Result<HashMap<_, _>>>()?;

    for p in &cfg.pipelines {
        health.register_pipeline(&p.name);
    }

    if cfg.postgres.enable_offset_tracking {
        let pool = writer.pool();
        let tracker = OffsetTracker::new(pool.clone());
        tracker.init().await?;
        sync_offsets(&cfg.kafka, &tracker, &cfg.pipelines).await?;
    }

    let shutdown_rx_heartbeat = shutdown_rx.resubscribe();
    let shutdown_rx_fetch = shutdown_rx.resubscribe();
    let shutdown_rx_metrics = shutdown_rx.resubscribe();
    let shutdown_rx_memory = shutdown_rx.resubscribe();

    let consumer = Arc::new(build_consumer(&cfg.kafka)?);
    let last_poll = Arc::new(AtomicU64::new(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
    ));

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

    // Create per-pipeline channels and batchers
    let mut channels_by_topic = HashMap::new();
    let mut topic_to_pipeline = HashMap::new();
    let mut processing_tasks = Vec::new();
    let mut circuit_breakers_by_pipeline = HashMap::new();

    let shutdown_coordinator = ShutdownCoordinator::default();

    for pipeline_cfg in &cfg.pipelines {
        let pipeline_name = pipeline_cfg.name.clone();
        let topic = pipeline_cfg.topic.clone();
        let capacity = pipeline_cfg.backpressure.channel_capacity;

        // Create channel for this pipeline
        let (tx, rx) = mpsc::channel::<(MessageContext, OwnedMessage)>(capacity);
        channels_by_topic.insert(topic.clone(), PipelineChannels { tx });
        topic_to_pipeline.insert(topic.clone(), pipeline_name.clone());

        // Initialize channel depth metric to 0
        metrics.channel_depth_per_pipeline.with_label_values(&[&pipeline_name]).set(0);

        // Create batcher for this pipeline
        let batcher_config = BatcherConfig::from_pipeline_config(
            pipeline_cfg,
            &cfg.postgres,
            cfg.service.shutdown_timeout_duration(),
        );

        let (committed_offsets_tx, committed_offsets_rx) =
            tokio::sync::mpsc::unbounded_channel::<HashMap<(String, i32), i64>>();

        let batcher = Arc::new(tokio::sync::Mutex::new(Batcher::new(
            batcher_config,
            writer.clone(),
            metrics.clone(),
            &shutdown_coordinator,
            committed_offsets_tx,
        )));

        // Get the pipeline from pipelines_by_topic
        let pipeline = pipelines_by_topic
            .get(&topic)
            .expect("pipeline should exist")
            .clone();

        // Create circuit breaker for this pipeline
        let circuit_breaker = Arc::new(CircuitBreaker::new(
            pipeline_name.clone(),
            pipeline_cfg.circuit_breaker.clone(),
        ));
        circuit_breakers_by_pipeline.insert(pipeline_name.clone(), circuit_breaker.clone());

        // Create processing context
        let processing_context = Arc::new(PipelineProcessingContext {
            consumer: consumer.clone(),
            batcher: batcher.clone(),
            metrics: metrics.clone(),
            health: health.clone(),
            producer: producer.clone(),
            circuit_breaker: circuit_breaker.clone(),
        });

        // Spawn batcher background flush task for this pipeline
        {
            let batcher_for_flush = batcher.clone();
            let pipeline_name_clone = pipeline_name.clone();
            let mut shutdown_rx_flush = shutdown_rx.resubscribe();

            let flush_task = tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(1000));
                loop {
                    tokio::select! {
                        _ = shutdown_rx_flush.recv() => {
                            info!(pipeline_name = %pipeline_name_clone, "batcher flush task received shutdown signal");
                            let mut batcher_guard = batcher_for_flush.lock().await;
                            if let Err(e) = batcher_guard.flush_all_buffers(FlushReason::Shutdown).await {
                                error!(pipeline_name = %pipeline_name_clone, "batcher shutdown flush failed: {}", e);
                            }
                            break;
                        }
                        _ = interval.tick() => {
                            let mut batcher_guard = batcher_for_flush.lock().await;
                            if let Err(e) = batcher_guard.flush_pending_buffers().await {
                                error!(pipeline_name = %pipeline_name_clone, "batcher periodic flush failed: {}", e);
                            }
                        }
                    }
                }
            });
            processing_tasks.push(flush_task);
        }

        // Spawn committed offsets handler for this pipeline
        {
            let committed_offsets_task = tokio::spawn(run_committed_offsets_handler(
                committed_offsets_rx,
                consumer.clone(),
                metrics.clone(),
            ));
            processing_tasks.push(committed_offsets_task);
        }

        // Spawn processing loop for this pipeline
        {
            let processing_task = tokio::spawn(run_pipeline_processing_loop(
                pipeline,
                rx,
                processing_context,
            ));
            processing_tasks.push(processing_task);
        }
    }

    // Create channel registry for fetch loop
    let channel_registry = Arc::new(parking_lot::Mutex::new(PipelineChannelRegistry {
        channels_by_topic,
    }));

    let fetch_loop = tokio::spawn(run_fetch_loop(
        consumer.clone(),
        channel_registry,
        topic_to_pipeline,
        metrics.clone(),
        health.clone(),
        last_poll.clone(),
        cfg.service.memory.clone(),
        shutdown_rx_fetch,
    ));

    let heartbeat_task = tokio::spawn(run_heartbeat_task(
        last_poll,
        health.clone(),
        metrics.clone(),
        cfg.kafka.staleness_threshold_seconds,
        shutdown_rx_heartbeat,
    ));

    // Spawn circuit breaker metrics update task
    let circuit_breaker_metrics_task = tokio::spawn(run_circuit_breaker_metrics_task(
        circuit_breakers_by_pipeline,
        metrics.clone(),
        shutdown_rx_metrics,
    ));

    // Spawn memory monitor task
    let memory_monitor_task = tokio::spawn(run_memory_monitor_task(
        metrics.clone(),
        cfg.service.memory.clone(),
        shutdown_rx_memory,
    ));

    // Wait for fetch and heartbeat to complete, then join all processing tasks
    let _ = tokio::join!(fetch_loop, heartbeat_task, circuit_breaker_metrics_task, memory_monitor_task);

    for task in processing_tasks {
        let _ = task.await;
    }

    info!("consumer loops stopped");
    Ok(())
}

/// Creates a Kafka StreamConsumer configured from the provided `KafkaConfig`.
///
/// The resulting consumer reflects fetch limits, session timeout, max in-flight requests,
/// and optional security settings (TLS and/or SASL) present in `cfg`. Auto commit and
/// automatic offset storage are disabled on the returned consumer.
///
/// # Returns
///
/// `StreamConsumer` configured according to `cfg`, or an error if the underlying client cannot be created.
///
/// # Examples
///
/// ```no_run
/// let cfg = KafkaConfig { /* populate brokers, group_id, session_timeout_ms, ... */ };
/// let consumer = build_consumer(&cfg).expect("failed to build consumer");
/// ```
fn build_consumer(cfg: &KafkaConfig) -> Result<StreamConsumer> {
    let mut client_config = rdkafka::config::ClientConfig::new();
    client_config
        .set("bootstrap.servers", &cfg.brokers)
        .set("group.id", &cfg.group_id)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", cfg.session_timeout_ms.to_string())
        .set("socket.timeout.ms", "30000") // 30 second timeout for socket operations
        .set("connections.max.idle.ms", "540000") // 9 minutes
        .set("request.timeout.ms", "30000") // 30 second timeout for broker requests
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .set(
            "max.in.flight.requests.per.connection",
            cfg.max_inflight_messages.to_string(),
        );

    if let Some(fetch) = &cfg.fetch {
        client_config.set("fetch.message.max.bytes", fetch.max_bytes.to_string());
        client_config.set("fetch.wait.max.ms", fetch.max_wait_ms.to_string());
    }

    if let Some(sec) = &cfg.security {
        let protocol = if sec.sasl_enabled && sec.tls {
            Some("SASL_SSL")
        } else if sec.sasl_enabled {
            Some("SASL_PLAINTEXT")
        } else if sec.tls {
            Some("SSL")
        } else {
            None
        };
        if let Some(p) = protocol {
            client_config.set("security.protocol", p);
        }

        if sec.tls {
            if let Some(ca) = &sec.ssl_ca_location {
                client_config.set("ssl.ca.location", ca);
            }
            if let Some(cert) = &sec.ssl_certificate_location {
                client_config.set("ssl.certificate.location", cert);
            }
            if let Some(key) = &sec.ssl_key_location {
                client_config.set("ssl.key.location", key);
            }
            if let Some(pass) = &sec.ssl_key_password {
                client_config.set("ssl.key.password", pass);
            }
        }

        if sec.sasl_enabled {
            if let Some(mech) = &sec.sasl_mechanism {
                client_config.set("sasl.mechanism", mech);
            }
            if let Some(user) = &sec.sasl_username {
                client_config.set("sasl.username", user);
            }
            if let Some(pass) = &sec.sasl_password {
                client_config.set("sasl.password", pass);
            }
        }
    }

    let consumer: StreamConsumer = client_config.create()?;
    Ok(consumer)
}

/// Commit the consumer offset for the given message context.
///
/// This commits offset `ctx.offset + 1` for `ctx.topic` and `ctx.partition` to the provided consumer.
///
/// # Returns
///
/// `Ok(())` if the commit was successfully initiated, `Err(KafkaError)` otherwise.
///
/// # Examples
///
/// ```no_run
/// // commit_offset(&consumer, &ctx)?;
/// let _ = commit_offset(&consumer, &ctx);
/// ```
#[allow(dead_code)]
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
