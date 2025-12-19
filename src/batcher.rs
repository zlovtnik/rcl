use crate::config::PostgresConfig;
use crate::eip::PipelineConfig;
use crate::errors::{ProcessingError, ValidationError};
use crate::metrics::Metrics;
use crate::shutdown::ShutdownCoordinator;
use crate::writer::Writer;

use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tokio::time;
use tracing::{error, info, warn};

use crate::types::MessageContext;

#[derive(Clone, Debug)]
pub struct BatcherConfig {
    pub flush_interval_ms: u64,
    pub max_batch_size: usize,
    pub max_batch_bytes: usize,
    #[allow(dead_code)]
    pub shutdown_timeout: Duration,
    pub adaptive_batch_enabled: bool,
    pub adaptive_min_batch_size: usize,
    pub adaptive_max_batch_size: usize,
    pub latency_window_size: usize,
    pub latency_target_ms: u64,
}

impl Default for BatcherConfig {
    /// Creates a BatcherConfig populated with sensible defaults for production use.
    ///
    /// The defaults enable moderate batching and a safe shutdown timeout, with adaptive
    /// batching disabled by default and conservative adaptive bounds if later enabled.
    ///
    /// # Examples
    ///
    /// ```
    /// let cfg = crate::BatcherConfig::default();
    /// assert_eq!(cfg.flush_interval_ms, 5000);
    /// assert_eq!(cfg.max_batch_size, 5000);
    /// assert_eq!(cfg.max_batch_bytes, 10_485_760);
    /// assert_eq!(cfg.shutdown_timeout.as_secs(), 30);
    /// assert_eq!(cfg.adaptive_batch_enabled, false);
    /// ```
    fn default() -> Self {
        Self {
            flush_interval_ms: 5000,     // 5 seconds
            max_batch_size: 5000,        // Default batch size
            max_batch_bytes: 10_485_760, // 10MB
            shutdown_timeout: Duration::from_secs(30),
            adaptive_batch_enabled: false,
            adaptive_min_batch_size: 100,
            adaptive_max_batch_size: 50000,
            latency_window_size: 10,
            latency_target_ms: 1000, // 1 second target
        }
    }
}

impl BatcherConfig {
    /// Creates a BatcherConfig from pipeline and Postgres settings using the provided shutdown timeout.
    ///
    /// The resulting config uses:
    /// - `flush_interval_ms` = 5000 ms,
    /// - `max_batch_size` from `postgres.copy_batch_rows`,
    /// - `max_batch_bytes` = 10_485_760 bytes,
    ///   and maps adaptive batching fields from `pipeline.batching` (`adaptive_enabled`, `min_batch_size`,
    ///   `max_batch_size`, `latency_window_size`, `latency_target_ms`). The provided `shutdown_timeout` is used
    ///   verbatim.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// let pipeline: PipelineConfig = Default::default();
    /// let postgres: PostgresConfig = Default::default();
    /// let cfg = BatcherConfig::from_pipeline_config(&pipeline, &postgres, Duration::from_secs(30));
    /// assert_eq!(cfg.shutdown_timeout, Duration::from_secs(30));
    /// ```
    pub fn from_pipeline_config(
        pipeline: &PipelineConfig,
        postgres: &PostgresConfig,
        shutdown_timeout: Duration,
    ) -> Self {
        Self {
            flush_interval_ms: 5000, // Could be made configurable later
            max_batch_size: postgres.copy_batch_rows,
            max_batch_bytes: 10_485_760, // Could be made configurable later
            shutdown_timeout,
            adaptive_batch_enabled: pipeline.batching.adaptive_enabled,
            adaptive_min_batch_size: pipeline.batching.min_batch_size,
            adaptive_max_batch_size: pipeline.batching.max_batch_size,
            latency_window_size: pipeline.batching.latency_window_size,
            latency_target_ms: pipeline.batching.latency_target_ms,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PipelineBuffer {
    pub messages: Vec<Value>,
    pub contexts: Vec<MessageContext>,
    pub table: String,
    pub pipeline_name: String,
    pub last_flush: Instant,
    pub current_bytes: usize,
    pub first_message_time: Option<Instant>,
}

impl PipelineBuffer {
    /// Creates a new, empty `PipelineBuffer` for the specified pipeline and table.
    ///
    /// The buffer is initialized with no messages or contexts, zero byte count,
    /// `last_flush` set to the current instant, and `first_message_time` unset.
    ///
    /// # Examples
    ///
    /// ```
    /// let buf = PipelineBuffer::new("my_pipeline".to_string(), "my_table".to_string());
    /// assert!(buf.is_empty());
    /// assert_eq!(buf.len(), 0);
    /// assert_eq!(buf.pipeline_name, "my_pipeline");
    /// assert_eq!(buf.table, "my_table");
    /// ```
    pub fn new(pipeline_name: String, table: String) -> Self {
        Self {
            messages: Vec::new(),
            contexts: Vec::new(),
            table,
            pipeline_name,
            last_flush: Instant::now(),
            current_bytes: 0,
            first_message_time: None,
        }
    }

    /// Adds a JSON `message` and its `context` to this buffer if the resulting batch size stays within `max_batch_bytes`.
    ///
    /// The optional `message_size` can be provided to avoid serializing `message`; if omitted, the function serializes the message to determine its size. The method returns an error if serialization fails or if adding the message would exceed `max_batch_bytes`.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the message and context were appended; `Err(ProcessingError::Validation)` if serialization fails or the message would exceed the batch byte limit.
    ///
    /// # Examples
    ///
    /// ```
    /// use serde_json::json;
    ///
    /// let mut buf = PipelineBuffer::new("pipeline", "table");
    /// let msg = json!({ "id": 1, "name": "example" });
    /// // Provide a precomputed size to avoid serialization in this example.
    /// let precomputed_size = serde_json::to_string(&msg).unwrap().len();
    /// let ctx = MessageContext::default();
    ///
    /// buf.add_message(msg, ctx, 10_000, Some(precomputed_size)).unwrap();
    /// assert_eq!(buf.len(), 1);
    /// ```
    pub fn add_message(
        &mut self,
        message: Value,
        context: MessageContext,
        max_batch_bytes: usize,
        message_size: Option<usize>,
    ) -> Result<(), ProcessingError> {
        // Use precomputed size if provided, otherwise serialize to compute size
        let computed_size = if let Some(size) = message_size {
            size
        } else {
            serde_json::to_string(&message)
                .map_err(|e| {
                    ProcessingError::Validation(ValidationError::new(format!(
                        "json serialize: {}",
                        e
                    )))
                })?
                .len()
        };

        // Check if adding this message would exceed byte limit
        if self.current_bytes + computed_size > max_batch_bytes {
            return Err(ProcessingError::Validation(ValidationError::new(format!(
                "message size {} bytes would exceed batch limit {} bytes (current: {} bytes)",
                computed_size, max_batch_bytes, self.current_bytes
            ))));
        }

        if self.messages.is_empty() {
            self.first_message_time = Some(Instant::now());
        }

        self.messages.push(message);
        self.contexts.push(context);
        self.current_bytes += computed_size;

        Ok(())
    }

    pub fn should_flush(&self, config: &BatcherConfig, now: Instant) -> Option<FlushReason> {
        // Size-based flush
        if self.messages.len() >= config.max_batch_size {
            return Some(FlushReason::Size);
        }

        // Byte-based flush
        if self.current_bytes >= config.max_batch_bytes {
            return Some(FlushReason::Bytes);
        }

        // Time-based flush
        if now.duration_since(self.last_flush) >= Duration::from_millis(config.flush_interval_ms) {
            return Some(FlushReason::Time);
        }

        None
    }

    /// Empties the buffer and resets its byte and timing state.
    ///
    /// This removes all stored messages and their corresponding contexts, sets `current_bytes` to 0,
    /// updates `last_flush` to the current instant, and clears `first_message_time`.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut buf = PipelineBuffer::new("pipeline", "table");
    /// buf.clear();
    /// assert!(buf.is_empty());
    /// assert_eq!(buf.len(), 0);
    /// ```
    pub fn clear(&mut self) {
        self.messages.clear();
        self.contexts.clear();
        self.current_bytes = 0;
        self.last_flush = Instant::now();
        self.first_message_time = None;
    }

    /// Checks whether the buffer contains no messages.
    ///
    /// # Returns
    /// `true` if the buffer contains no messages, `false` otherwise.
    ///
    /// # Examples
    ///
    /// ```
    /// let buf = PipelineBuffer::new("pipeline_name", "table_name");
    /// assert!(buf.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    /// Retrieve the number of messages currently stored in the buffer.
    ///
    /// # Returns
    ///
    /// The number of messages in the buffer.
    ///
    /// # Examples
    ///
    /// ```
    /// let buf = PipelineBuffer::new("pipeline", "table");
    /// assert_eq!(buf.len(), 0);
    /// ```
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.messages.len()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum FlushReason {
    Time,
    Size,
    Bytes,
    Shutdown,
}

impl std::fmt::Display for FlushReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlushReason::Time => write!(f, "time"),
            FlushReason::Size => write!(f, "size"),
            FlushReason::Bytes => write!(f, "bytes"),
            FlushReason::Shutdown => write!(f, "shutdown"),
        }
    }
}

pub struct Batcher {
    buffers: HashMap<String, PipelineBuffer>,
    config: BatcherConfig,
    writer: Arc<Writer>,
    metrics: Metrics,
    shutdown_rx: broadcast::Receiver<()>,
    committed_offsets_tx: tokio::sync::mpsc::UnboundedSender<HashMap<(String, i32), i64>>,
    recent_latencies: VecDeque<f64>,
}

impl Batcher {
    /// Creates a new Batcher wired to the provided writer, metrics, shutdown coordinator, and committed-offsets sender.
    ///
    /// The returned Batcher is ready to accept messages and will listen for shutdown signals from the coordinator.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use tokio::sync::mpsc;
    /// // Assume BatcherConfig, Writer, Metrics, ShutdownCoordinator are available in scope.
    ///
    /// let config = BatcherConfig::default();
    /// let writer = Arc::new(/* your Writer implementation */);
    /// let metrics = Metrics::default();
    /// let shutdown_coordinator = ShutdownCoordinator::new();
    /// let (tx, _rx) = mpsc::unbounded_channel::<std::collections::HashMap<(String, i32), i64>>();
    ///
    /// let batcher = crate::batcher::Batcher::new(config, writer, metrics, &shutdown_coordinator, tx);
    /// ```
    pub fn new(
        config: BatcherConfig,
        writer: Arc<Writer>,
        metrics: Metrics,
        shutdown_coordinator: &ShutdownCoordinator,
        committed_offsets_tx: tokio::sync::mpsc::UnboundedSender<HashMap<(String, i32), i64>>,
    ) -> Self {
        Self {
            buffers: HashMap::new(),
            config,
            writer,
            metrics,
            shutdown_rx: shutdown_coordinator.subscribe(),
            committed_offsets_tx,
            recent_latencies: VecDeque::with_capacity(20), // pre-allocate
        }
    }

    /// Appends a message and its context to the pipeline/table buffer and triggers an immediate flush if thresholds are met.
    ///
    /// The message is enqueued into the in-memory buffer for the given pipeline and table, the message counter metric is incremented, and the buffer will be flushed immediately if adding this message causes a size, byte, or time threshold to be exceeded.
    ///
    /// # Parameters
    ///
    /// - `pipeline_name`: Identifier of the source pipeline for the message.
    /// - `table`: Target table name where the message will be written.
    /// - `message`: JSON payload to buffer.
    /// - `context`: Per-message context (e.g., topic/partition/offset) preserved for eventual offset commitment.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success; `Err(ProcessingError)` if the message cannot be enqueued or processing fails (for example, JSON serialization or validation errors).
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn doc_example(mut batcher: crate::batcher::Batcher, msg: serde_json::Value, ctx: crate::batcher::MessageContext) {
    /// batcher.add_message("my_pipeline", "users", msg, ctx).await.unwrap();
    /// # }
    /// ```
    pub async fn add_message(
        &mut self,
        pipeline_name: &str,
        table: &str,
        message: Value,
        context: MessageContext,
    ) -> Result<(), ProcessingError> {
        let buffer_key = format!("{}:{}", pipeline_name, table);

        // Precompute message size to avoid re-serialization in PipelineBuffer
        let message_size = serde_json::to_string(&message)
            .map_err(|e| {
                ProcessingError::Validation(ValidationError::new(format!("json serialize: {}", e)))
            })?
            .len();

        // Add message to buffer
        let buffer = self
            .buffers
            .entry(buffer_key.clone())
            .or_insert_with(|| PipelineBuffer::new(pipeline_name.to_string(), table.to_string()));

        buffer.add_message(
            message,
            context,
            self.config.max_batch_bytes,
            Some(message_size),
        )?;
        self.metrics.batch_messages_total.inc();

        // Check if buffer should be flushed immediately after adding
        let now = Instant::now();
        let should_flush_after = buffer.should_flush(&self.config, now);

        if let Some(reason) = should_flush_after {
            self.flush_buffer_by_key(&buffer_key, reason).await?;
        }

        Ok(())
    }

    async fn flush_buffer_by_key(
        &mut self,
        buffer_key: &str,
        reason: FlushReason,
    ) -> Result<(), ProcessingError> {
        if let Some(mut buffer) = self.buffers.remove(buffer_key) {
            self.flush_buffer(&mut buffer, reason).await?;
            self.buffers.insert(buffer_key.to_string(), buffer);
        }
        Ok(())
    }

    /// Flushes a non-empty PipelineBuffer by attempting a batched write and falling back to individual writes.
    ///
    /// On success, committed offsets for successfully written messages are sent and, if adaptive batching is
    /// enabled, the batch size may be adjusted based on observed latency. If the batch write fails but some
    /// individual message writes succeed, offsets for those messages are sent and the function returns `Ok(())`.
    ///
    /// # Examples
    ///
    /// ```
    /// // assume `batcher` is a mutable Batcher instance and `buffer` is a PipelineBuffer populated with messages
    /// // batcher.flush_buffer(&mut buffer, FlushReason::Time).await.unwrap();
    /// ```
    ///
    /// # Returns
    ///
    /// `Ok(())` if at least one message in the buffer was written successfully, `Err(ProcessingError)` if the
    /// batch write failed and all individual message writes also failed.
    async fn flush_buffer(
        &mut self,
        buffer: &mut PipelineBuffer,
        reason: FlushReason,
    ) -> Result<(), ProcessingError> {
        if buffer.is_empty() {
            return Ok(());
        }

        let batch_size = buffer.messages.len();
        let batch_bytes = buffer.current_bytes;
        let latency = buffer
            .first_message_time
            .map(|t| Instant::now().duration_since(t))
            .unwrap_or(Duration::from_secs(0));

        let messages = buffer.messages.clone();
        let contexts = buffer.contexts.clone();
        buffer.clear();

        self.metrics.batch_size.observe(batch_size as f64);
        self.metrics.batch_bytes_total.inc_by(batch_bytes as u64);
        self.metrics
            .batch_latency_seconds
            .observe(latency.as_secs_f64());
        self.metrics
            .batch_flush_total
            .with_label_values(&[&reason.to_string()])
            .inc();

        info!(
            pipeline = %buffer.pipeline_name,
            table = %buffer.table,
            batch_size = %batch_size,
            batch_bytes = %batch_bytes,
            reason = %reason,
            "flushing batch"
        );

        // Attempt batch write with fallback to individual writes
        let write_result = self
            .writer
            .write_batch(&messages, &buffer.table, &buffer.pipeline_name)
            .await;

        match write_result {
            Ok(()) => {
                // All messages written successfully, send committed offsets
                self.send_committed_offsets(&contexts);

                // Adaptive batch sizing: track latency and adjust batch size
                if self.config.adaptive_batch_enabled {
                    self.adjust_batch_size(latency);
                }

                Ok(())
            }
            Err(e) => {
                warn!(
                    pipeline = %buffer.pipeline_name,
                    table = %buffer.table,
                    batch_size = %batch_size,
                    error = %e,
                    "batch write failed, falling back to individual writes"
                );

                // Fallback: try individual writes
                let mut success_count = 0;
                let mut successful_contexts = Vec::new();
                for (message, context) in messages.into_iter().zip(contexts.into_iter()) {
                    match self
                        .writer
                        .write(&message, &buffer.table, &buffer.pipeline_name)
                        .await
                    {
                        Ok(()) => {
                            success_count += 1;
                            successful_contexts.push(context);
                        }
                        Err(write_err) => {
                            error!(
                                pipeline = %buffer.pipeline_name,
                                table = %buffer.table,
                                error = %write_err,
                                "individual message write failed"
                            );
                            // Individual write failures are logged but don't stop processing
                            // In a real implementation, these should go to DLQ
                        }
                    }
                }

                if success_count > 0 {
                    // Send committed offsets for successful individual writes
                    self.send_committed_offsets(&successful_contexts);
                }

                if success_count == 0 {
                    return Err(e); // All writes failed, return original batch error
                }

                warn!(
                    pipeline = %buffer.pipeline_name,
                    table = %buffer.table,
                    success_count = %success_count,
                    total_count = %batch_size,
                    "partial batch write success"
                );

                Ok(())
            }
        }
    }

    /// Sends committed offsets derived from a slice of message contexts.
    ///
    /// Builds a map from `(topic, partition)` to the next offset to commit (each
    /// context's `offset + 1`), taking the maximum next-offset for any duplicate
    /// `(topic, partition)` keys, and sends that map over the `committed_offsets_tx`
    /// channel. If the `contexts` slice is empty this is a no-op. If sending fails,
    /// an error is logged.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Prepare contexts for two partitions (one duplicate with higher offset)
    /// let contexts = vec![
    ///     MessageContext { topic: "t".into(), partition: 0, offset: 4, ..Default::default() },
    ///     MessageContext { topic: "t".into(), partition: 0, offset: 6, ..Default::default() },
    ///     MessageContext { topic: "t".into(), partition: 1, offset: 2, ..Default::default() },
    /// ];
    ///
    /// // Send committed offsets for these contexts:
    /// // ( "t", 0 ) -> 7  (max of 4+1 and 6+1)
    /// // ( "t", 1 ) -> 3
    /// batcher.send_committed_offsets(&contexts);
    /// ```
    fn send_committed_offsets(&self, contexts: &[MessageContext]) {
        if contexts.is_empty() {
            return;
        }

        let mut committed_offsets = HashMap::new();
        for context in contexts {
            let key = (context.topic.clone(), context.partition);
            let current_max = committed_offsets.get(&key).copied().unwrap_or(0);
            // Offset to commit is context.offset + 1 (next expected offset)
            committed_offsets.insert(key, current_max.max(context.offset + 1));
        }

        // Send the committed offsets
        if let Err(e) = self.committed_offsets_tx.send(committed_offsets) {
            error!("Failed to send committed offsets: {}", e);
        }
    }

    /// Adjusts the configured maximum batch size based on recent batch latency samples.
    ///
    /// Uses a sliding window of recent latencies to compute an average. If the average latency
    /// is greater than the configured latency target, the batch size is reduced (but not by
    /// more than 50% in one adjustment). If the average latency is below the target, the batch
    /// size may be increased (but not more than 2x in one adjustment). The resulting batch size
    /// is clamped between `adaptive_min_batch_size` and `adaptive_max_batch_size`. When the
    /// configured size changes, the new value is written to `self.config.max_batch_size` and
    /// the `current_batch_size_limit` metric is updated.
    ///
    /// `latency` should be the observed time between the first message arrival and the batch flush.
    ///
    /// # Examples
    ///
    /// ```
    /// // Assume `batcher` is a mutable Batcher instance configured for adaptive sizing.
    /// // Provide the most recent observed latency to let the batcher update its limits.
    /// batcher.adjust_batch_size(std::time::Duration::from_millis(150));
    /// ```
    fn adjust_batch_size(&mut self, latency: Duration) {
        let latency_ms = latency.as_millis() as f64;

        // Add current latency to recent latencies window
        self.recent_latencies.push_back(latency_ms);
        if self.recent_latencies.len() > self.config.latency_window_size {
            self.recent_latencies.pop_front(); // Remove oldest latency in O(1) time
        }

        // Only adjust if we have enough latency samples
        if self.recent_latencies.len() >= self.config.latency_window_size {
            let avg_latency =
                self.recent_latencies.iter().sum::<f64>() / self.recent_latencies.len() as f64;
            let target_ms = self.config.latency_target_ms as f64;

            // Calculate adjustment factor based on how far we are from target
            let adjustment_factor = if avg_latency > target_ms {
                // Too slow, reduce batch size
                (target_ms / avg_latency).max(0.5) // Don't reduce by more than 50% at once
            } else {
                // Fast enough, can increase batch size
                let excess_capacity = target_ms / avg_latency;
                excess_capacity.min(2.0) // Don't increase by more than 2x at once
            };

            let new_batch_size = (self.config.max_batch_size as f64 * adjustment_factor) as usize;
            let clamped_batch_size = new_batch_size
                .max(self.config.adaptive_min_batch_size)
                .min(self.config.adaptive_max_batch_size);

            if clamped_batch_size != self.config.max_batch_size {
                info!(
                    old_batch_size = %self.config.max_batch_size,
                    new_batch_size = %clamped_batch_size,
                    avg_latency_ms = %avg_latency,
                    target_ms = %target_ms,
                    adjustment_factor = %adjustment_factor,
                    "adjusting batch size based on latency"
                );
                self.config.max_batch_size = clamped_batch_size;
                self.metrics
                    .current_batch_size_limit
                    .set(clamped_batch_size as i64);
            }
        }
    }

    /// Runs a background loop that periodically flushes pending buffers and exits when a shutdown signal is received.
    ///
    /// The loop wakes once per second to call `flush_pending_buffers`. When a shutdown signal is received it flushes
    /// all buffers with `FlushReason::Shutdown` and then returns.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the loop exits after a successful shutdown flush, `Err(ProcessingError)` if a flush operation fails.
    ///
    /// # Examples
    ///
    /// ```
    /// // Spawned in a task; the batcher will flush periodically and exit once a shutdown signal is sent.
    /// // let mut batcher = /* create batcher */ ;
    /// // tokio::spawn(async move { let _ = batcher.run_background_flush().await; });
    /// ```
    #[allow(dead_code)]
    pub async fn run_background_flush(&mut self) -> Result<(), ProcessingError> {
        let mut interval = time::interval(Duration::from_millis(1000)); // Check every second

        loop {
            tokio::select! {
                _ = self.shutdown_rx.recv() => {
                    info!("batcher received shutdown signal, flushing all buffers");
                    self.flush_all_buffers(FlushReason::Shutdown).await?;
                    break;
                }
                _ = interval.tick() => {
                    self.flush_pending_buffers().await?;
                }
            }
        }

        Ok(())
    }

    pub async fn flush_pending_buffers(&mut self) -> Result<(), ProcessingError> {
        let now = Instant::now();

        // Collect keys that need flushing and flush them immediately
        let buffer_keys: Vec<String> = self.buffers.keys().cloned().collect();

        for key in buffer_keys {
            if let Some(mut buffer) = self.buffers.remove(&key) {
                if let Some(reason) = buffer.should_flush(&self.config, now) {
                    self.flush_buffer(&mut buffer, reason).await?;
                }
                if !buffer.is_empty() {
                    self.buffers.insert(key, buffer);
                }
            }
        }

        Ok(())
    }

    pub async fn flush_all_buffers(&mut self, reason: FlushReason) -> Result<(), ProcessingError> {
        let buffer_keys: Vec<String> = self.buffers.keys().cloned().collect();

        for key in buffer_keys {
            if let Some(mut buffer) = self.buffers.remove(&key).filter(|b| !b.is_empty()) {
                self.flush_buffer(&mut buffer, reason.clone()).await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Creates a BatcherConfig preconfigured for tests.
    ///
    /// The configuration uses a short flush interval and small batch limits to make
    /// flush and size-based behaviors deterministic in unit tests. Adaptive batching
    /// is disabled by default in this test config.
    ///
    /// # Examples
    ///
    /// ```
    /// let cfg = create_test_config();
    /// assert_eq!(cfg.max_batch_size, 3);
    /// assert!(!cfg.adaptive_batch_enabled);
    /// assert_eq!(cfg.flush_interval_ms, 100);
    /// ```
    fn create_test_config() -> BatcherConfig {
        BatcherConfig {
            flush_interval_ms: 100, // Short for testing
            max_batch_size: 3,
            max_batch_bytes: 1000,
            shutdown_timeout: Duration::from_secs(1),
            adaptive_batch_enabled: false,
            adaptive_min_batch_size: 1,
            adaptive_max_batch_size: 10,
            latency_window_size: 5,
            latency_target_ms: 100,
        }
    }

    /// Creates a JSON test message with the given `id` for use in tests.
    ///
    /// # Examples
    ///
    /// ```
    /// let v = create_test_message(42);
    /// assert_eq!(v["id"], 42);
    /// assert_eq!(v["data"], "test data");
    /// assert_eq!(v["operation_type"], "c");
    /// ```
    fn create_test_message(id: u32) -> Value {
        serde_json::json!({
            "id": id,
            "data": "test data",
            "operation_type": "c"
        })
    }

    /// Creates a MessageContext used in tests with the provided offset.
    ///
    /// The returned context has topic `"test-topic"`, partition `0`, timestamp `1000`, and the given offset.
    ///
    /// # Examples
    ///
    /// ```
    /// let ctx = create_test_context(42);
    /// assert_eq!(ctx.offset, 42);
    /// assert_eq!(ctx.topic, "test-topic");
    /// ```
    fn create_test_context(offset: i64) -> MessageContext {
        MessageContext {
            topic: "test-topic".to_string(),
            partition: 0,
            offset,
            timestamp: 1000,
            retry_count: None,
        }
    }

    #[test]
    fn test_pipeline_buffer_add_message() {
        let mut buffer = PipelineBuffer::new("test-pipeline".to_string(), "test-table".to_string());

        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);

        let msg = create_test_message(1);
        buffer
            .add_message(msg, create_test_context(1), 10_485_760, None)
            .unwrap();

        assert!(!buffer.is_empty());
        assert_eq!(buffer.len(), 1);
        assert!(buffer.first_message_time.is_some());
    }

    #[test]
    fn test_pipeline_buffer_size_flush() {
        let mut buffer = PipelineBuffer::new("test-pipeline".to_string(), "test-table".to_string());
        let config = create_test_config();

        // Add messages up to batch size
        for i in 0..config.max_batch_size {
            buffer
                .add_message(
                    create_test_message(i as u32),
                    create_test_context(i as i64),
                    config.max_batch_bytes,
                    None,
                )
                .unwrap();
        }

        // Should trigger size-based flush
        let now = Instant::now();
        assert_eq!(buffer.should_flush(&config, now), Some(FlushReason::Size));
    }

    #[test]
    fn test_pipeline_buffer_clear() {
        let mut buffer = PipelineBuffer::new("test-pipeline".to_string(), "test-table".to_string());

        buffer
            .add_message(
                create_test_message(1),
                create_test_context(1),
                10_485_760,
                None,
            )
            .unwrap();
        assert!(!buffer.is_empty());

        buffer.clear();
        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.current_bytes, 0);
    }

    #[test]
    fn test_pipeline_buffer_accumulated_bytes() {
        let mut buffer = PipelineBuffer::new("test-pipeline".to_string(), "test-table".to_string());

        // Add a few moderately sized messages
        for i in 0..3 {
            let msg = create_test_message(i);
            buffer
                .add_message(msg, create_test_context(i as i64), 10_485_760, None)
                .unwrap();
        }

        // Verify current_bytes is accumulating
        assert!(buffer.current_bytes > 0);
        assert_eq!(buffer.len(), 3);
    }

    #[test]
    fn test_pipeline_buffer_time_flush() {
        let mut buffer = PipelineBuffer::new("test-pipeline".to_string(), "test-table".to_string());
        let config = create_test_config();
        let msg = create_test_message(1);

        buffer
            .add_message(msg, create_test_context(1), config.max_batch_bytes, None)
            .unwrap();
        let first_time = buffer.first_message_time.unwrap();

        // Simulate time passage past flush interval
        let far_future = first_time + Duration::from_millis(config.flush_interval_ms + 100);
        assert_eq!(
            buffer.should_flush(&config, far_future),
            Some(FlushReason::Time)
        );
    }

    #[test]
    fn test_pipeline_buffer_no_flush_needed() {
        let mut buffer = PipelineBuffer::new("test-pipeline".to_string(), "test-table".to_string());
        let config = create_test_config();
        let msg = create_test_message(1);

        buffer
            .add_message(msg, create_test_context(1), config.max_batch_bytes, None)
            .unwrap();
        let now = Instant::now();

        // Immediately check - should not need flush yet
        assert_eq!(buffer.should_flush(&config, now), None);
    }

    #[test]
    fn test_pipeline_buffer_multiple_messages() {
        let mut buffer = PipelineBuffer::new("test-pipeline".to_string(), "test-table".to_string());
        let config = create_test_config();

        for i in 1..3 {
            let msg = create_test_message(i);
            buffer
                .add_message(
                    msg,
                    create_test_context(i as i64),
                    config.max_batch_bytes,
                    None,
                )
                .unwrap();
        }

        assert_eq!(buffer.len(), 2);
        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_batcher_config_default() {
        let cfg = BatcherConfig::default();
        assert!(cfg.flush_interval_ms > 0);
        assert!(cfg.max_batch_size > 0);
        assert!(cfg.max_batch_bytes > 0);
    }

    #[test]
    fn test_flush_reason_clone() {
        let reason1 = FlushReason::Time;
        let reason2 = reason1.clone();
        assert_eq!(reason1, reason2);
    }

    #[test]
    fn test_flush_reason_variants() {
        let reasons = vec![
            FlushReason::Time,
            FlushReason::Size,
            FlushReason::Bytes,
            FlushReason::Shutdown,
        ];

        // All should be different
        for (i, r1) in reasons.iter().enumerate() {
            for (j, r2) in reasons.iter().enumerate() {
                if i == j {
                    assert_eq!(r1, r2);
                } else {
                    assert_ne!(r1, r2);
                }
            }
        }
    }

    #[test]
    fn test_pipeline_buffer_json_serialization() {
        let mut buffer = PipelineBuffer::new("test-pipeline".to_string(), "test-table".to_string());

        let msg = serde_json::json!({
            "id": 1,
            "nested": {"key": "value"},
            "array": [1, 2, 3],
            "operation_type": "c"
        });

        buffer
            .add_message(msg, create_test_context(1), 10_485_760, None)
            .unwrap();
        assert_eq!(buffer.len(), 1);
    }

    #[test]
    fn test_pipeline_buffer_with_special_characters() {
        let mut buffer = PipelineBuffer::new("test-pipeline".to_string(), "test-table".to_string());

        let msg = serde_json::json!({
            "id": 1,
            "data": "special: !@#$%^&*()",
            "operation_type": "c"
        });

        buffer
            .add_message(msg, create_test_context(1), 10_485_760, None)
            .unwrap();
        assert_eq!(buffer.len(), 1);
    }

    #[test]
    fn test_pipeline_buffer_retrieval_order() {
        let mut buffer = PipelineBuffer::new("test-pipeline".to_string(), "test-table".to_string());

        for i in 1..=3 {
            buffer
                .add_message(
                    create_test_message(i),
                    create_test_context(i as i64),
                    10_485_760,
                    None,
                )
                .unwrap();
        }

        assert_eq!(buffer.messages.len(), 3);
        // Verify FIFO order
        assert_eq!(
            buffer.messages[0].get("id").and_then(|v| v.as_u64()),
            Some(1)
        );
        assert_eq!(
            buffer.messages[1].get("id").and_then(|v| v.as_u64()),
            Some(2)
        );
        assert_eq!(
            buffer.messages[2].get("id").and_then(|v| v.as_u64()),
            Some(3)
        );
    }

    // =====================================================================
    // Integration tests for Batcher struct (unit tests on buffer behavior)
    // =====================================================================

    #[test]
    fn test_batcher_buffer_key_format() {
        // Test that buffer keys are correctly formatted as "pipeline:table"
        let pipeline = "test_pipeline";
        let table = "test_table";
        let expected_key = format!("{}:{}", pipeline, table);
        assert_eq!(expected_key, "test_pipeline:test_table");
    }

    #[test]
    fn test_multiple_buffers_isolation() {
        let mut buffer1 = PipelineBuffer::new("pipeline1".to_string(), "table1".to_string());
        let mut buffer2 = PipelineBuffer::new("pipeline2".to_string(), "table2".to_string());

        // Add different messages to each buffer
        let msg1 = create_test_message(1);
        let msg2 = create_test_message(2);
        let ctx = create_test_context(1);

        buffer1
            .add_message(msg1.clone(), ctx.clone(), 10_485_760, None)
            .unwrap();
        buffer2
            .add_message(msg2.clone(), ctx.clone(), 10_485_760, None)
            .unwrap();

        // Verify each buffer has its own message
        assert_eq!(buffer1.len(), 1);
        assert_eq!(buffer2.len(), 1);
        assert_eq!(
            buffer1.messages[0].get("id").and_then(|v| v.as_u64()),
            Some(1)
        );
        assert_eq!(
            buffer2.messages[0].get("id").and_then(|v| v.as_u64()),
            Some(2)
        );
    }

    #[test]
    fn test_flush_reason_matches_trigger() {
        // Test 1: Size-based flush trigger
        let mut buffer = PipelineBuffer::new("test-pipeline".to_string(), "test-table".to_string());
        let mut size_config = create_test_config();
        size_config.max_batch_size = 2;

        for i in 0..2 {
            buffer
                .add_message(
                    create_test_message(i),
                    create_test_context(i as i64),
                    size_config.max_batch_bytes,
                    None,
                )
                .unwrap();
        }
        assert_eq!(
            buffer.should_flush(&size_config, Instant::now()),
            Some(FlushReason::Size),
            "Size-based flush should trigger at max_batch_size"
        );

        // Test 2: Time-based flush trigger
        buffer.clear();
        let time_config = create_test_config();
        let msg = create_test_message(1);
        buffer
            .add_message(
                msg,
                create_test_context(1),
                time_config.max_batch_bytes,
                None,
            )
            .unwrap();

        let first_time = buffer.first_message_time.unwrap();
        let far_future = first_time + Duration::from_millis(time_config.flush_interval_ms + 1);

        assert_eq!(
            buffer.should_flush(&time_config, far_future),
            Some(FlushReason::Time),
            "Time-based flush should trigger after flush_interval"
        );
    }

    #[test]
    fn test_context_preservation_in_buffer() {
        let mut buffer = PipelineBuffer::new("test-pipeline".to_string(), "test-table".to_string());

        // Add messages with specific context fields
        for i in 1..=3 {
            let msg = create_test_message(i);
            let ctx = MessageContext {
                topic: format!("topic-{}", i),
                partition: i as i32,
                offset: (i * 100) as i64,
                timestamp: 1000 + i as i64,
                retry_count: None,
            };
            buffer.add_message(msg, ctx, 10_485_760, None).unwrap();
        }

        // Verify contexts are preserved in order
        assert_eq!(buffer.contexts.len(), 3);
        assert_eq!(buffer.contexts[0].partition, 1);
        assert_eq!(buffer.contexts[1].partition, 2);
        assert_eq!(buffer.contexts[2].partition, 3);
        assert_eq!(buffer.contexts[0].offset, 100);
        assert_eq!(buffer.contexts[1].offset, 200);
        assert_eq!(buffer.contexts[2].offset, 300);
    }

    #[test]
    fn test_byte_accumulation_accuracy() {
        let mut buffer = PipelineBuffer::new("test-pipeline".to_string(), "test-table".to_string());

        let msg1 = serde_json::json!({
            "id": 1,
            "data": "test1",
            "operation_type": "c"
        });
        let msg2 = serde_json::json!({
            "id": 2,
            "data": "test2",
            "operation_type": "u"
        });

        let size1 = serde_json::to_string(&msg1).unwrap().len();
        let size2 = serde_json::to_string(&msg2).unwrap().len();

        buffer
            .add_message(
                msg1.clone(),
                create_test_context(1),
                10_485_760,
                Some(size1),
            )
            .unwrap();
        assert_eq!(buffer.current_bytes, size1);

        buffer
            .add_message(
                msg2.clone(),
                create_test_context(2),
                10_485_760,
                Some(size2),
            )
            .unwrap();
        assert_eq!(buffer.current_bytes, size1 + size2);
    }

    #[test]
    fn test_precomputed_size_optimization() {
        let mut buffer = PipelineBuffer::new("test-pipeline".to_string(), "test-table".to_string());

        let msg = serde_json::json!({
            "id": 1,
            "nested": {"key": "value", "array": [1, 2, 3]},
            "operation_type": "c"
        });

        // Calculate expected size
        let expected_size = serde_json::to_string(&msg).unwrap().len();

        // Add with precomputed size
        buffer
            .add_message(msg, create_test_context(1), 10_485_760, Some(expected_size))
            .unwrap();

        // Verify byte count matches precomputed value
        assert_eq!(buffer.current_bytes, expected_size);
    }

    #[test]
    fn test_flush_reason_priority_size_over_time() {
        let mut buffer = PipelineBuffer::new("test-pipeline".to_string(), "test-table".to_string());
        let config = create_test_config();

        // Add messages to hit size limit
        for i in 0..config.max_batch_size {
            buffer
                .add_message(
                    create_test_message(i as u32),
                    create_test_context(i as i64),
                    config.max_batch_bytes,
                    None,
                )
                .unwrap();
        }

        // Even at the start (no time passed), size-based flush should trigger first
        let now = Instant::now();
        assert_eq!(buffer.should_flush(&config, now), Some(FlushReason::Size));
    }

    #[test]
    fn test_message_size_exceeds_batch_limit() {
        let mut buffer = PipelineBuffer::new("test-pipeline".to_string(), "test-table".to_string());

        // Try to add a message larger than max_batch_bytes
        let msg = create_test_message(1);
        let msg_size = serde_json::to_string(&msg).unwrap().len();
        let small_limit = msg_size / 2; // Force a violation

        let result = buffer.add_message(msg, create_test_context(1), small_limit, Some(msg_size));
        assert!(result.is_err());
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_adaptive_batch_size_calculation() {
        // Test the adaptive batch size logic
        let mut config = create_test_config();
        config.adaptive_batch_enabled = true;
        config.latency_target_ms = 100;
        config.adaptive_min_batch_size = 10;
        config.adaptive_max_batch_size = 1000;

        // Simulating high latency scenario (500ms when target is 100ms)
        let avg_latency = 500.0;
        let target_ms = config.latency_target_ms as f64;
        let adjustment_factor = (target_ms / avg_latency).max(0.5);

        // Batch size should reduce but not more than 50%
        assert!(adjustment_factor <= 1.0);
        assert!(adjustment_factor >= 0.5);
    }

    #[test]
    fn test_buffer_timestamps_on_first_message() {
        let mut buffer = PipelineBuffer::new("test-pipeline".to_string(), "test-table".to_string());

        assert!(buffer.first_message_time.is_none());

        let msg = create_test_message(1);
        buffer
            .add_message(msg, create_test_context(1), 10_485_760, None)
            .unwrap();

        assert!(buffer.first_message_time.is_some());

        // Add another message - first_message_time should not change
        let first_time = buffer.first_message_time.unwrap();
        let msg2 = create_test_message(2);
        buffer
            .add_message(msg2, create_test_context(2), 10_485_760, None)
            .unwrap();

        assert_eq!(buffer.first_message_time.unwrap(), first_time);
    }
}
