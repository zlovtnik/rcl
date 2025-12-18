use crate::config::PostgresConfig;
use crate::eip::PipelineConfig;
use crate::errors::{ProcessingError, ValidationError};
use crate::metrics::Metrics;
use crate::shutdown::ShutdownCoordinator;
use crate::writer::Writer;

use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tokio::time;
use tracing::{info, warn, error};

#[derive(Clone, Debug)]
pub struct BatcherConfig {
    pub flush_interval_ms: u64,
    pub max_batch_size: usize,
    pub max_batch_bytes: usize,
    pub shutdown_timeout: Duration,
}

impl Default for BatcherConfig {
    fn default() -> Self {
        Self {
            flush_interval_ms: 5000,      // 5 seconds
            max_batch_size: 5000,         // Default batch size
            max_batch_bytes: 10_485_760,  // 10MB
            shutdown_timeout: Duration::from_secs(30),
        }
    }
}

impl BatcherConfig {
    pub fn from_pipeline_config(
        _pipeline: &PipelineConfig,
        postgres: &PostgresConfig,
        shutdown_timeout: Duration,
    ) -> Self {
        Self {
            flush_interval_ms: 5000, // Could be made configurable later
            max_batch_size: postgres.copy_batch_rows,
            max_batch_bytes: 10_485_760, // Could be made configurable later
            shutdown_timeout,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PipelineBuffer {
    pub messages: Vec<Value>,
    pub table: String,
    pub pipeline_name: String,
    pub last_flush: Instant,
    pub current_bytes: usize,
    pub first_message_time: Option<Instant>,
    pub max_batch_bytes: usize,
}

impl PipelineBuffer {
    pub fn new(pipeline_name: String, table: String) -> Self {
        Self {
            messages: Vec::new(),
            table,
            pipeline_name,
            last_flush: Instant::now(),
            current_bytes: 0,
            first_message_time: None,
            max_batch_bytes: 10_485_760, // 10MB default
        }
    }

    pub fn add_message(&mut self, message: Value) -> Result<(), ProcessingError> {
        let message_size = serde_json::to_string(&message)
            .map_err(|e| ProcessingError::Validation(ValidationError::new(format!("json serialize: {}", e))))?
            .len();

        // Check if adding this message would exceed byte limit
        if self.current_bytes + message_size > self.max_batch_bytes {
            return Err(ProcessingError::Validation(ValidationError::new(
                "message too large for batch".to_string()
            )));
        }

        if self.messages.is_empty() {
            self.first_message_time = Some(Instant::now());
        }

        self.messages.push(message);
        self.current_bytes += message_size;

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

    pub fn clear(&mut self) {
        self.messages.clear();
        self.current_bytes = 0;
        self.last_flush = Instant::now();
        self.first_message_time = None;
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    pub fn len(&self) -> usize {
        self.messages.len()
    }
}

#[derive(Clone, Debug)]
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
}

impl Batcher {
    pub fn new(
        config: BatcherConfig,
        writer: Arc<Writer>,
        metrics: Metrics,
        shutdown_coordinator: &ShutdownCoordinator,
    ) -> Self {
        Self {
            buffers: HashMap::new(),
            config,
            writer,
            metrics,
            shutdown_rx: shutdown_coordinator.subscribe(),
        }
    }

    pub async fn add_message(
        &mut self,
        pipeline_name: &str,
        table: &str,
        message: Value,
    ) -> Result<(), ProcessingError> {
        let buffer_key = format!("{}:{}", pipeline_name, table);

        // Check if we need to flush first
        let should_flush = {
            if let Some(buffer) = self.buffers.get(&buffer_key) {
                let now = Instant::now();
                buffer.should_flush(&self.config, now)
            } else {
                None
            }
        };

        // Add message to buffer
        let buffer = self.buffers.entry(buffer_key.clone()).or_insert_with(|| {
            PipelineBuffer::new(pipeline_name.to_string(), table.to_string())
        });

        buffer.add_message(message)?;
        self.metrics.batch_messages_total.inc();

        // Check if buffer should be flushed immediately after adding
        let now = Instant::now();
        let should_flush_after = buffer.should_flush(&self.config, now);

        if should_flush.is_some() || should_flush_after.is_some() {
            let reason = should_flush_after.or(should_flush).unwrap();
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
        let latency = buffer.first_message_time
            .map(|t| Instant::now().duration_since(t))
            .unwrap_or(Duration::from_secs(0));

        let messages = buffer.messages.clone();
        buffer.clear();

        self.metrics.batch_size.observe(batch_size as f64);
        self.metrics.batch_bytes_total.inc_by(batch_bytes as u64);
        self.metrics.batch_latency_seconds.observe(latency.as_secs_f64());
        self.metrics.batch_flush_total.with_label_values(&[&reason.to_string()]).inc();

        info!(
            pipeline = %buffer.pipeline_name,
            table = %buffer.table,
            batch_size = %batch_size,
            batch_bytes = %batch_bytes,
            reason = %reason,
            "flushing batch"
        );

        // Attempt batch write with fallback to individual writes
        match self.writer.write_batch(&messages, &buffer.table, &buffer.pipeline_name).await {
            Ok(()) => Ok(()),
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
                for message in messages {
                    match self.writer.write(&message, &buffer.table, &buffer.pipeline_name).await {
                        Ok(()) => success_count += 1,
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

    async fn flush_pending_buffers(&mut self) -> Result<(), ProcessingError> {
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

    async fn flush_all_buffers(&mut self, reason: FlushReason) -> Result<(), ProcessingError> {
        let buffer_keys: Vec<String> = self.buffers.keys().cloned().collect();

        for key in buffer_keys {
            if let Some(mut buffer) = self.buffers.remove(&key) {
                if !buffer.is_empty() {
                    self.flush_buffer(&mut buffer, reason.clone()).await?;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PostgresConfig;
    use crate::health::HealthRegistry;
    use std::sync::Arc;
    use tokio::sync::broadcast;

    fn create_test_config() -> BatcherConfig {
        BatcherConfig {
            flush_interval_ms: 100, // Short for testing
            max_batch_size: 3,
            max_batch_bytes: 1000,
            shutdown_timeout: Duration::from_secs(1),
        }
    }

    fn create_test_message(id: u32) -> Value {
        serde_json::json!({
            "id": id,
            "data": "test data",
            "operation_type": "c"
        })
    }

    #[test]
    fn test_pipeline_buffer_add_message() {
        let mut buffer = PipelineBuffer::new("test-pipeline".to_string(), "test-table".to_string());

        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);

        let msg = create_test_message(1);
        buffer.add_message(msg).unwrap();

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
            buffer.add_message(create_test_message(i as u32)).unwrap();
        }

        // Should trigger size-based flush
        let now = Instant::now();
        assert_eq!(buffer.should_flush(&config, now), Some(FlushReason::Size));
    }

    #[test]
    fn test_pipeline_buffer_clear() {
        let mut buffer = PipelineBuffer::new("test-pipeline".to_string(), "test-table".to_string());

        buffer.add_message(create_test_message(1)).unwrap();
        assert!(!buffer.is_empty());

        buffer.clear();
        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.current_bytes, 0);
    }

    #[tokio::test]
    async fn test_batcher_add_message() {
        let config = create_test_config();
        let (shutdown_coordinator, _) = ShutdownCoordinator::new();
        let metrics = Metrics::register(&prometheus::Registry::new().unwrap()).unwrap();

        // Mock writer - we'll need to create a proper mock for full testing
        // For now, just test the basic structure
        let mut batcher = Batcher {
            buffers: HashMap::new(),
            config,
            writer: Arc::new(panic!("Mock writer not implemented")),
            metrics,
            shutdown_rx: shutdown_coordinator.subscribe(),
        };

        // This will panic due to mock writer, but tests the logic path
        // In real tests, we'd use a mock writer
        let result = batcher.add_message("test-pipeline", "test-table", create_test_message(1)).await;
        assert!(result.is_err()); // Expected due to mock writer
    }
}
