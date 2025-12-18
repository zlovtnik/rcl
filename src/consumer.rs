use crate::batcher::{Batcher, BatcherConfig};
use crate::config::{Config, KafkaConfig};
use crate::decoder::decode_and_validate;
use crate::dlq;
use crate::eip::{MessageMetadata, Pipeline, StageContext};
use crate::errors::{ProcessingError, ValidationError};
use crate::health::{ComponentStatus, HealthRegistry};
use crate::metrics::Metrics;
use crate::shutdown::ShutdownCoordinator;
use crate::types::MessageContext;
use crate::writer::Writer;
use anyhow::Result;
use futures::StreamExt;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::OwnedMessage;
use rdkafka::producer::FutureProducer;
use rdkafka::Message;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, mpsc};
use tokio::time::{interval, Duration};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info, warn};

use rdkafka::Offset;
use rdkafka::TopicPartitionList;

struct ConsumerContext {
    consumer: Arc<StreamConsumer>,
    batcher: Arc<tokio::sync::Mutex<Batcher>>,
    metrics: Metrics,
    health: Arc<HealthRegistry>,
    pipelines_by_topic: HashMap<String, Pipeline>,
    producer: Option<FutureProducer>,
}

impl ConsumerContext {
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
        let mut batcher = self.batcher.lock().await;
        batcher
            .add_message(pipeline_name, table, message, context.clone())
            .await
    }

    /// Commit the consumer offset for the provided message context.
    ///
    /// This will attempt to commit the next offset (message offset + 1) for the topic and partition
    /// described by `ctx`. On a commit failure the `processing_failures` metric is incremented and a
    /// warning is logged; on success an informational log is emitted.
    ///
    /// # Examples
    ///
    /// ```
    /// # use tokio::runtime::Runtime;
    /// # async fn _example(consumer_ctx: &crate::consumer::ConsumerContext, ctx: crate::consumer::MessageContext) {
    /// consumer_ctx.commit_offset(&ctx).await;
    /// # }
    /// # let _ = Runtime::new().unwrap();
    /// ```
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
    ///
    /// This updates the pipeline error status in the health registry with `error_msg`, increments the `decode_failures` metric, logs a warning containing the message correlation id and error text, and commits the message offset from `ctx`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example(ctx: &ConsumerContext, pipeline: &Pipeline, msg_ctx: &MessageContext) {
    /// ctx.handle_decode_error(pipeline, msg_ctx, "invalid payload".to_string()).await;
    /// # }
    /// ```
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
}

impl ConsumerContext {
    /// Process a single message payload through the given pipeline, enrich each resulting message with Kafka metadata, and enqueue the produced messages into the batcher.
    ///
    /// The payload is decoded and validated according to the pipeline configuration. If the decoded value is a JSON object, `_meta_topic`, `_meta_partition`, `_meta_offset`, and `_meta_ingest_ts` fields are injected from the provided message context. The pipeline is executed with a stage context derived from the message context; each produced message is routed to a destination table (from `_destination_table` or the pipeline staging table), validated, and added to the batcher.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, `Err(ProcessingError)` if decoding/validation, pipeline execution, routing validation, or batching fails.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn example(ctx: &crate::consumer::ConsumerContext, pipeline: &crate::pipeline::Pipeline) -> Result<(), crate::consumer::ProcessingError> {
    /// let payload = r#"{"name":"alice"}"#;
    /// ctx.process_message(payload, pipeline, &crate::consumer::MessageContext::default()).await?;
    /// # Ok(()) }
    /// ```
    async fn process_message(
        &self,
        payload: &str,
        pipeline: &Pipeline,
        ctx: &MessageContext,
    ) -> Result<(), ProcessingError> {
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

        let processed_messages = pipeline.execute(&stage_ctx, decoded).await?;

        // Batch processed messages
        for msg in processed_messages {
            // Check for routing metadata
            let candidate = msg
                .get("_destination_table")
                .and_then(|v| v.as_str())
                .unwrap_or(&pipeline.config.staging_table);

            let table = crate::config::validate_table_identifier(candidate)
                .map_err(|e| ProcessingError::Validation(ValidationError::new(e.to_string())))?;

            self.add_to_batcher(&pipeline.name, table, msg.clone(), ctx)
                .await?;
        }

        Ok(())
    }

    /// Handle a processing error for a message by updating pipeline health, incrementing metrics,
    /// attempting to publish the failed message to the pipeline's DLQ (if configured), and committing
    /// the message offset so the consumer can advance.
    ///
    /// The method:
    /// - records the pipeline error in the health registry (logs a warning if that update fails),
    /// - increments the appropriate failure metric (`processing_failures` for transport errors,
    ///   `decode_failures` otherwise),
    /// - if a producer and DLQ configuration are available, attempts to publish the original payload
    ///   and error details to the DLQ and increments `dlq_total` on success (logs on failure),
    /// - if no DLQ is available, logs that the message was skipped,
    /// - commits the message offset for the provided context.
    ///
    /// # Parameters
    ///
    /// - `pipeline`: the pipeline whose processing failed; used for health updates and DLQ routing.
    /// - `ctx`: message context containing correlation id and metadata used for logging and commits.
    /// - `reason`: the processing error that occurred; its public reason is used for logs and DLQ.
    /// - `payload`: the original message payload as a string, forwarded to the DLQ when publishing.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example(
    /// #     consumer_ctx: &crate::consumer::ConsumerContext,
    /// #     pipeline: &crate::pipeline::Pipeline,
    /// #     msg_ctx: &crate::consumer::MessageContext,
    /// #     err: &crate::consumer::ProcessingError,
    /// # ) {
    /// consumer_ctx
    ///     .handle_processing_error(pipeline, msg_ctx, err, "original payload")
    ///     .await;
    /// # }
    /// ```
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
        if let (Some(producer), Some(dlq_cfg)) =
            (self.producer.as_ref(), pipeline.config.dlq.as_ref())
        {
            if let Err(err) =
                dlq::publish(producer, &pipeline.name, dlq_cfg, ctx, reason, payload).await
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
        self.commit_offset(ctx).await;
    }
}

async fn run_fetch_loop(
    consumer: Arc<StreamConsumer>,
    tx: mpsc::Sender<(MessageContext, OwnedMessage)>,
    metrics: Metrics,
    health: Arc<HealthRegistry>,
    last_poll: Arc<AtomicU64>,
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
                match message {
                    Ok(Some(Ok(msg))) => {
                        last_poll.store(
                            SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64,
                            Ordering::Relaxed,
                        );
                        let _ = health.set_kafka_status(ComponentStatus::Healthy);
                        let ctx = MessageContext {
                            topic: msg.topic().to_string(),
                            partition: msg.partition(),
                            offset: msg.offset(),
                            timestamp: msg.timestamp().to_millis().unwrap_or(0),
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
                    Ok(Some(Err(err))) => {
                        last_poll.store(
                            SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
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
                                .unwrap()
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
                    .unwrap()
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

/// Processes incoming Kafka messages from the given receiver, routes each message to the pipeline
/// configured for its topic, decodes the message payload as UTF-8, and enqueues produced messages
/// into the batching pipeline. Decode or processing failures are handled and the original message
/// offset is committed or skipped as appropriate.
///
/// The loop runs until the receiver is closed; when finished, it logs that processing has stopped.
///
/// # Examples
///
/// ```no_run
/// use rdkafka::consumer::StreamConsumer;
/// use std::sync::Arc;
/// use tokio::sync::mpsc;
///
/// // Assume `context` is a prepared ConsumerContext and `consumer` is an Arc<StreamConsumer>.
/// // Create a channel and start the processing loop task.
/// let (tx, rx) = mpsc::channel(128);
/// let context = /* ConsumerContext prepared elsewhere */;
/// let consumer = /* Arc<StreamConsumer> prepared elsewhere */;
///
/// // spawn the processing loop (example; uses tokio runtime)
/// tokio::spawn(async move {
///     run_processing_loop(rx, context, consumer).await;
/// });
/// ```
async fn run_processing_loop(
    rx: mpsc::Receiver<(MessageContext, OwnedMessage)>,
    context: ConsumerContext,
    _consumer: Arc<StreamConsumer>,
) {
    let mut stream = ReceiverStream::new(rx);

    while let Some((ctx, msg)) = stream.next().await {
        let pipeline = match context.pipelines_by_topic.get(&ctx.topic) {
            Some(p) => p,
            None => {
                warn!(context = %ctx.correlation_id(), "no pipeline for topic");
                context.commit_offset(&ctx).await;
                continue;
            }
        };

        let payload = match msg.payload_view::<str>() {
            Some(Ok(p)) => p.to_string(),
            Some(Err(e)) => {
                context
                    .handle_decode_error(pipeline, &ctx, format!("utf8 decode failed: {}", e))
                    .await;
                continue;
            }
            None => {
                context
                    .handle_decode_error(pipeline, &ctx, "empty payload".to_string())
                    .await;
                continue;
            }
        };

        match context.process_message(&payload, pipeline, &ctx).await {
            Ok(_) => {
                // Message added to batcher, offset will be committed after batch write
            }
            Err(reason) => {
                context
                    .handle_processing_error(pipeline, &ctx, &reason, &payload)
                    .await;
            }
        }
    }

    info!("processing loop finished");
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
    writer: Arc<Writer>,
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

    // For replay, we'll use the writer directly since batching isn't needed
    let (dummy_tx, _) = tokio::sync::mpsc::unbounded_channel::<HashMap<(String, i32), i64>>();
    let context = ConsumerContext {
        consumer: Arc::new(consumer),
        batcher: Arc::new(tokio::sync::Mutex::new(Batcher::new(
            BatcherConfig::default(),
            writer.clone(),
            _metrics.clone(),
            &ShutdownCoordinator::default(),
            dummy_tx,
        ))), // Not used in replay
        metrics: _metrics,
        health: Arc::new(HealthRegistry::new(Duration::from_secs(30))), // Not used in replay
        pipelines_by_topic,
        producer: None, // DLQ not used in replay
    };

    let mut stream = context.consumer.stream();

    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                if msg.offset() > end_offset {
                    info!("Reached end offset {}", end_offset);
                    break;
                }

                let ctx = MessageContext {
                    topic: msg.topic().to_string(),
                    partition: msg.partition(),
                    offset: msg.offset(),
                    timestamp: msg.timestamp().to_millis().unwrap_or(0),
                };

                match msg.payload() {
                    Some(payload) => match std::str::from_utf8(payload) {
                        Ok(payload_str) => {
                            match context.process_message(payload_str, &pipeline, &ctx).await {
                                Ok(_) => {
                                    info!(context = %ctx.correlation_id(), "replayed successfully");
                                }
                                Err(e) => {
                                    error!(context = %ctx.correlation_id(), error = %e, "processing failed during replay; stopping");
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

    let shutdown_rx_heartbeat = shutdown_rx.resubscribe();
    let shutdown_rx_fetch = shutdown_rx.resubscribe();
    let _shutdown_rx_batcher = shutdown_rx.resubscribe();

    let capacity = cfg
        .pipelines
        .iter()
        .map(|p| p.backpressure.channel_capacity)
        .max()
        .unwrap_or(1024);

    let consumer = Arc::new(build_consumer(&cfg.kafka)?);
    let last_poll = Arc::new(AtomicU64::new(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
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

    // Create committed offsets channel
    let (committed_offsets_tx, committed_offsets_rx) =
        tokio::sync::mpsc::unbounded_channel::<HashMap<(String, i32), i64>>();

    // Create batcher
    let shutdown_coordinator = ShutdownCoordinator::default();
    let batcher_config = BatcherConfig::from_pipeline_config(
        &cfg.pipelines[0], // Use first pipeline for config, could be made per-pipeline later
        &cfg.postgres,
        cfg.service.shutdown_timeout_duration(),
    );
    let batcher = Arc::new(tokio::sync::Mutex::new(Batcher::new(
        batcher_config,
        writer.clone(),
        metrics.clone(),
        &shutdown_coordinator,
        committed_offsets_tx,
    )));

    let (tx, rx) = mpsc::channel::<(MessageContext, OwnedMessage)>(capacity);

    let context = ConsumerContext {
        consumer: consumer.clone(),
        batcher: batcher.clone(),
        metrics: metrics.clone(),
        health: health.clone(),
        pipelines_by_topic,
        producer,
    };

    let fetch_loop = tokio::spawn(run_fetch_loop(
        consumer.clone(),
        tx,
        metrics.clone(),
        health.clone(),
        last_poll.clone(),
        shutdown_rx_fetch,
    ));

    let heartbeat_task = tokio::spawn(run_heartbeat_task(
        last_poll,
        health.clone(),
        metrics.clone(),
        cfg.kafka.staleness_threshold_seconds,
        shutdown_rx_heartbeat,
    ));

    let processing_loop = tokio::spawn(run_processing_loop(rx, context, consumer.clone()));

    // Spawn batcher background flush task
    let batcher_task = tokio::spawn(async move {
        let mut batcher_guard = batcher.lock().await;
        if let Err(e) = batcher_guard.run_background_flush().await {
            error!("batcher background flush failed: {}", e);
        }
    });

    // Spawn committed offsets handler task
    let committed_offsets_task = tokio::spawn(run_committed_offsets_handler(
        committed_offsets_rx,
        consumer.clone(),
        metrics.clone(),
    ));

    tokio::try_join!(
        fetch_loop,
        processing_loop,
        heartbeat_task,
        batcher_task,
        committed_offsets_task
    )?;
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
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
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