use crate::errors::ProcessingError;
use crate::stages::{
    FilterStage, IdempotentReceiverStage, RouterStage, SplitterStage, TransformerStage,
};
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use tracing::{debug, info};

/// Core abstractions for Enterprise Integration Patterns

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct StageContext {
    pub correlation_id: String,
    pub pipeline_name: String,
    pub message_metadata: MessageMetadata,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct MessageMetadata {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
    pub headers: HashMap<String, String>,
}

impl MessageMetadata {
    pub fn from_kafka(topic: String, partition: i32, offset: i64, timestamp: Option<i64>) -> Self {
        Self {
            topic,
            partition,
            offset,
            timestamp,
            headers: HashMap::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum StageResult {
    /// Continue to next stage with this message
    Continue(Value),
    /// Skip this message (filtered out)
    Skip,
    /// Split into multiple messages
    Split(Vec<Value>),
    /// Error processing (send to DLQ if configured)
    #[allow(dead_code)]
    Error(StageError),
}

#[derive(Clone, Debug)]
pub struct StageError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

impl std::fmt::Display for StageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl StageError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable: false,
        }
    }
}

#[async_trait]
pub trait Stage: Send + Sync {
    /// Process a message through this stage
    async fn process(&self, ctx: &StageContext, msg: Value)
    -> Result<StageResult, ProcessingError>;

    /// Stage name for metrics/logging
    #[allow(dead_code)]
    fn name(&self) -> &str;

    /// Initialize stage (setup connections, caches, etc.)
    #[allow(dead_code)]
    async fn initialize(&self) -> Result<(), ProcessingError> {
        Ok(())
    }

    /// Cleanup resources
    #[allow(dead_code)]
    async fn shutdown(&self) -> Result<(), ProcessingError> {
        Ok(())
    }

    /// Health check
    #[allow(dead_code)]
    async fn health_check(&self) -> Result<(), ProcessingError> {
        Ok(())
    }
}

pub struct Pipeline {
    pub name: String,
    pub stages: Vec<Box<dyn Stage>>,
    pub config: PipelineConfig,
}

impl Pipeline {
    pub fn from_config(config: &PipelineConfig) -> Result<Self> {
        let mut stages = Vec::new();
        for stage_def in &config.stages {
            let stage = StageFactory::create(stage_def)?;
            stages.push(stage);
        }

        Ok(Self {
            name: config.name.clone(),
            stages,
            config: config.clone(),
        })
    }

    pub async fn execute(
        &self,
        ctx: &StageContext,
        msg: Value,
    ) -> Result<Vec<Value>, ProcessingError> {
        let mut current_messages = vec![msg];
        info!(pipeline = %ctx.pipeline_name, context = %ctx.correlation_id, initial_messages = 1, "starting pipeline execution");

        for (stage_idx, stage) in self.stages.iter().enumerate() {
            let mut next_messages = Vec::new();
            let messages_entering_stage = current_messages.len();
            info!(pipeline = %ctx.pipeline_name, context = %ctx.correlation_id, stage_index = stage_idx, messages_entering = messages_entering_stage, "processing stage");

            for msg in current_messages {
                let result = stage.process(ctx, msg).await?;
                match result {
                    StageResult::Continue(new_msg) => {
                        debug!(pipeline = %ctx.pipeline_name, context = %ctx.correlation_id, stage_index = stage_idx, "message continued to next stage");
                        next_messages.push(new_msg);
                    }
                    StageResult::Skip => {
                        info!(pipeline = %ctx.pipeline_name, context = %ctx.correlation_id, stage_index = stage_idx, "message skipped");
                    }
                    StageResult::Split(msgs) => {
                        info!(pipeline = %ctx.pipeline_name, context = %ctx.correlation_id, stage_index = stage_idx, split_count = msgs.len(), "message split");
                        next_messages.extend(msgs);
                    }
                    StageResult::Error(err) => {
                        return Err(ProcessingError::Stage(err));
                    }
                }
            }

            info!(pipeline = %ctx.pipeline_name, context = %ctx.correlation_id, stage_index = stage_idx, messages_entering = messages_entering_stage, messages_exiting = next_messages.len(), "stage processing completed");
            current_messages = next_messages;

            if current_messages.is_empty() {
                info!(pipeline = %ctx.pipeline_name, context = %ctx.correlation_id, stage_index = stage_idx, "pipeline halted: no messages left after stage");
                break;
            }
        }

        info!(pipeline = %ctx.pipeline_name, context = %ctx.correlation_id, final_message_count = current_messages.len(), "pipeline execution finished");
        Ok(current_messages)
    }
}

impl Clone for Pipeline {
    /// Create a new Pipeline with the same configuration and reconstructed stage instances.
    ///
    /// This produces a fresh Pipeline that shares the original configuration but recreates
    /// concrete stage implementations (stages are not cloned in-place).
    ///
    /// Panics if rebuilding the pipeline from its stored configuration fails.
    ///
    /// # Examples
    ///
    /// ```
    /// let cloned = pipeline.clone();
    /// assert_eq!(cloned.config.name, pipeline.config.name);
    /// ```
    fn clone(&self) -> Self {
        // Recreate the pipeline from config since stages can't be cloned
        Self::from_config(&self.config)
            .expect("Pipeline::clone failed: config was valid at construction")
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct BatchingConfig {
    #[serde(default)]
    pub adaptive_enabled: bool,
    #[serde(default = "BatchingConfig::default_min_batch_size")]
    pub min_batch_size: usize,
    #[serde(default = "BatchingConfig::default_max_batch_size")]
    pub max_batch_size: usize,
    #[serde(default = "BatchingConfig::default_latency_window_size")]
    pub latency_window_size: usize,
    #[serde(default = "BatchingConfig::default_latency_target_ms")]
    pub latency_target_ms: u64,
}

impl Default for BatchingConfig {
    /// Creates a BatchingConfig populated with the library's sensible default values.
    ///
    /// # Examples
    ///
    /// ```
    /// let cfg = crate::eip::BatchingConfig::default();
    /// assert!(!cfg.adaptive_enabled);
    /// assert!(cfg.min_batch_size > 0);
    /// assert!(cfg.max_batch_size >= cfg.min_batch_size);
    /// ```
    fn default() -> Self {
        Self {
            adaptive_enabled: false,
            min_batch_size: Self::default_min_batch_size(),
            max_batch_size: Self::default_max_batch_size(),
            latency_window_size: Self::default_latency_window_size(),
            latency_target_ms: Self::default_latency_target_ms(),
        }
    }
}

impl BatchingConfig {
    /// Default minimum batch size used when batching is enabled.
    ///
    /// This value is used as the lower bound for forming batches when adaptive batching is active.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(crate::default_min_batch_size(), 100);
    /// ```
    fn default_min_batch_size() -> usize {
        100
    }
    /// Default maximum batch size used when batching is enabled.
    ///
    /// # Returns
    /// The maximum number of items per batch (50,000).
    ///
    /// # Examples
    ///
    /// ```
    /// let max = default_max_batch_size();
    /// assert_eq!(max, 50_000);
    /// ```
    fn default_max_batch_size() -> usize {
        50000
    }
    /// Default latency window size used by adaptive batching.
    ///
    /// This is the number of recent latency samples considered when computing adaptive batch sizes.
    ///
    /// # Returns
    ///
    /// The default latency window size (10).
    ///
    /// # Examples
    ///
    /// ```
    /// let v = default_latency_window_size();
    /// assert_eq!(v, 10);
    /// ```
    fn default_latency_window_size() -> usize {
        10
    }
    /// Default latency target in milliseconds.
    ///
    /// # Examples
    ///
    /// ```
    /// let ms = default_latency_target_ms();
    /// assert_eq!(ms, 1000);
    /// ```
    ///
    /// # Returns
    ///
    /// `1000` — the default latency target in milliseconds.
    fn default_latency_target_ms() -> u64 {
        1000
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct PipelineConfig {
    pub name: String,
    pub topic: String,
    pub debezium_envelope: bool,
    pub staging_table: String,
    pub dlq: Option<DlqConfig>,
    pub stages: Vec<StageDefinition>,
    // Legacy fields for compatibility
    pub required_fields: Vec<String>,
    #[serde(default)]
    pub backpressure: BackpressureConfig,
    #[serde(default)]
    pub batching: BatchingConfig,
    /// Circuit breaker configuration for fault tolerance.
    ///
    /// Controls the behavior of the circuit breaker mechanism, which protects the pipeline from cascading failures.
    /// The circuit breaker operates in three states: Closed (normal operation), Open (fast-fail to prevent overload),
    /// and Half-Open (testing recovery). See [`crate::circuit_breaker::CircuitBreakerConfig`] for state details,
    /// failure/success thresholds, and recovery timeout settings.
    ///
    /// # Default Behavior
    ///
    /// If not specified, uses [`CircuitBreakerConfig::default`] with: enabled=true, failure_threshold=10,
    /// success_threshold=5, half_open_timeout_ms=30000.
    #[serde(default)]
    pub circuit_breaker: crate::circuit_breaker::CircuitBreakerConfig,
    /// Number of concurrent worker threads for pipeline message processing.
    ///
    /// Controls the parallelism level for processing messages through this pipeline:
    /// - **1 (default)**: Sequential processing, strict per-partition ordering preserved.
    /// - **>1**: Parallel message processing with specified thread count. Note: ordering across messages
    ///   in the same partition is NOT guaranteed with multiple threads (messages are distributed via round-robin).
    ///   Use this for throughput optimization when ordering is not critical.
    ///
    /// # Valid Range
    ///
    /// Must be >= 1 (0 or negative values are rejected during config validation).
    /// **Recommended values:** 1–8 for most use cases; larger values (8–16) for high-throughput scenarios
    /// with external ordering mechanisms or when key-based sharding is implemented upstream.
    ///
    /// # Default
    ///
    /// 1 (sequential processing, total ordering preserved).
    #[serde(default = "PipelineConfig::default_worker_threads")]
    pub worker_threads: usize,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct BackpressureConfig {
    #[serde(default = "BackpressureConfig::default_channel_capacity")]
    pub channel_capacity: usize,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            channel_capacity: Self::DEFAULT_CHANNEL_CAPACITY,
        }
    }
}

impl BackpressureConfig {
    pub const DEFAULT_CHANNEL_CAPACITY: usize = 20_000;

    pub fn default_channel_capacity() -> usize {
        Self::DEFAULT_CHANNEL_CAPACITY
    }
}

impl PipelineConfig {
    pub const DEFAULT_WORKER_THREADS: usize = 1;

    pub fn default_worker_threads() -> usize {
        Self::DEFAULT_WORKER_THREADS
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct StageDefinition {
    pub r#type: String,
    pub name: String,
    pub config: Value,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct DlqConfig {
    pub topic: String,
    pub max_retries: u16,
    #[serde(default = "DlqConfig::default_max_payload_bytes")]
    pub max_payload_bytes: usize,
}

impl DlqConfig {
    pub const DEFAULT_MAX_PAYLOAD_BYTES: usize = 1_000_000;

    pub fn default_max_payload_bytes() -> usize {
        Self::DEFAULT_MAX_PAYLOAD_BYTES
    }
}

pub struct StageFactory;

impl StageFactory {
    pub fn create(def: &StageDefinition) -> Result<Box<dyn Stage>> {
        match def.r#type.as_str() {
            "filter" => Ok(Box::new(FilterStage::from_config(
                def.name.clone(),
                def.config.clone(),
            )?)),
            "transformer" => Ok(Box::new(TransformerStage::from_config(
                def.name.clone(),
                def.config.clone(),
            )?)),
            "router" => Ok(Box::new(RouterStage::from_config(
                def.name.clone(),
                def.config.clone(),
            )?)),
            "splitter" => Ok(Box::new(SplitterStage::from_config(
                def.name.clone(),
                def.config.clone(),
            )?)),
            "idempotent_receiver" => Ok(Box::new(IdempotentReceiverStage::from_config(
                def.name.clone(),
                def.config.clone(),
            )?)),
            _ => Err(anyhow::anyhow!("unknown stage type: {}", def.r#type)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Creates a StageContext populated with fixed test values for correlation id, pipeline name, and message metadata.
    ///
    /// # Examples
    ///
    /// ```
    /// let ctx = create_test_context();
    /// assert_eq!(ctx.correlation_id, "test-correlation");
    /// assert_eq!(ctx.pipeline_name, "test-pipeline");
    /// assert_eq!(ctx.message_metadata.topic, "test-topic");
    /// ```
    fn create_test_context() -> StageContext {
        StageContext {
            correlation_id: "test-correlation".to_string(),
            pipeline_name: "test-pipeline".to_string(),
            message_metadata: MessageMetadata::from_kafka("test-topic".to_string(), 0, 0, None),
        }
    }

    /// Creates a boxed filter stage configured to include messages whose `status` field equals `"active"`.
    ///
    /// The stage is configured with an "include" mode, a single equality condition on the `status` field,
    /// and an `"AND"` logic for combining conditions.
    ///
    /// # Examples
    ///
    /// ```
    /// let stage = create_filter_stage();
    /// assert_eq!(stage.name(), "filter");
    /// ```
    #[allow(dead_code)]
    fn create_filter_stage() -> Box<dyn Stage> {
        let stage_def = StageDefinition {
            name: "filter".to_string(),
            r#type: "filter".to_string(),
            config: json!({
                "mode": "include",
                "conditions": [
                    {
                        "field": "status",
                        "equals": "active"
                    }
                ],
                "logic": "AND"
            }),
        };
        StageFactory::create(&stage_def).unwrap()
    }

    /// Creates a transformer stage that adds a `processed_at` field set to the current time.
    ///
    /// # Examples
    ///
    /// ```
    /// let stage = create_transformer_stage();
    /// assert_eq!(stage.name(), "transformer");
    /// ```
    #[allow(dead_code)]
    fn create_transformer_stage() -> Box<dyn Stage> {
        let stage_def = StageDefinition {
            name: "transformer".to_string(),
            r#type: "transformer".to_string(),
            config: json!({
                "transformations": [
                    {
                        "type": "add_field",
                        "name": "processed_at",
                        "value": "{{now}}"
                    }
                ]
            }),
        };
        StageFactory::create(&stage_def).unwrap()
    }

    /// Creates a boxed router stage configured to route messages by the `category` field.
    ///
    /// The stage routes `"electronics"` to topic `"inventory"`, uses `"general"` as the default
    /// route, and writes the chosen destination into the `destination` metadata field.
    ///
    /// # Examples
    ///
    /// ```
    /// let stage = create_router_stage();
    /// assert_eq!(stage.name(), "router");
    /// ```
    #[allow(dead_code)]
    fn create_router_stage() -> Box<dyn Stage> {
        let stage_def = StageDefinition {
            name: "router".to_string(),
            r#type: "router".to_string(),
            config: json!({
                "route_by": "category",
                "routes": {
                    "electronics": "inventory"
                },
                "default": "general",
                "metadata_field": "destination"
            }),
        };
        StageFactory::create(&stage_def).unwrap()
    }

    #[tokio::test]
    async fn test_pipeline_filter_only() {
        let filter_def = StageDefinition {
            name: "filter".to_string(),
            r#type: "filter".to_string(),
            config: json!({
                "mode": "include",
                "conditions": [
                    {
                        "field": "status",
                        "equals": "active"
                    }
                ],
                "logic": "AND"
            }),
        };

        let config = PipelineConfig {
            name: "test".to_string(),
            topic: "test-topic".to_string(),
            debezium_envelope: false,
            staging_table: "test-table".to_string(),
            dlq: None,
            stages: vec![filter_def],
            required_fields: vec![],
            backpressure: BackpressureConfig {
                channel_capacity: 100,
            },
            batching: Default::default(),
            circuit_breaker: Default::default(),
            worker_threads: 1,
        };

        let pipeline = Pipeline::from_config(&config).unwrap();

        let ctx = create_test_context();
        let msg = json!({"status": "active", "id": 123});

        let results = pipeline.execute(&ctx, msg).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("id"), Some(&json!(123)));
    }

    #[tokio::test]
    async fn test_pipeline_filter_skip() {
        let filter_def = StageDefinition {
            name: "filter".to_string(),
            r#type: "filter".to_string(),
            config: json!({
                "mode": "include",
                "conditions": [
                    {
                        "field": "status",
                        "equals": "active"
                    }
                ],
                "logic": "AND"
            }),
        };

        let config = PipelineConfig {
            name: "test".to_string(),
            topic: "test-topic".to_string(),
            debezium_envelope: false,
            staging_table: "test-table".to_string(),
            dlq: None,
            stages: vec![filter_def],
            required_fields: vec![],
            backpressure: BackpressureConfig {
                channel_capacity: 100,
            },
            batching: Default::default(),
            circuit_breaker: Default::default(),
            worker_threads: 1,
        };

        let pipeline = Pipeline::from_config(&config).unwrap();

        let ctx = create_test_context();
        let msg = json!({"status": "inactive", "id": 123});

        let results = pipeline.execute(&ctx, msg).await.unwrap();
        assert_eq!(results.len(), 0); // Message should be filtered out
    }

    #[tokio::test]
    async fn test_pipeline_with_transformer() {
        let filter_def = StageDefinition {
            name: "filter".to_string(),
            r#type: "filter".to_string(),
            config: json!({
                "mode": "include",
                "conditions": [
                    {
                        "field": "status",
                        "equals": "active"
                    }
                ],
                "logic": "AND"
            }),
        };

        let transformer_def = StageDefinition {
            name: "transformer".to_string(),
            r#type: "transformer".to_string(),
            config: json!({
                "transformations": [
                    {
                        "type": "add_field",
                        "name": "processed_at",
                        "value": "{{now}}"
                    }
                ]
            }),
        };

        let config = PipelineConfig {
            name: "test".to_string(),
            topic: "test-topic".to_string(),
            debezium_envelope: false,
            staging_table: "test-table".to_string(),
            dlq: None,
            stages: vec![filter_def, transformer_def],
            required_fields: vec![],
            backpressure: BackpressureConfig {
                channel_capacity: 100,
            },
            batching: Default::default(),
            circuit_breaker: Default::default(),
            worker_threads: 1,
        };

        let pipeline = Pipeline::from_config(&config).unwrap();

        let ctx = create_test_context();
        let msg = json!({"status": "active", "id": 123});

        let results = pipeline.execute(&ctx, msg).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("id"), Some(&json!(123)));
        assert!(results[0].get("processed_at").is_some()); // Should have been added by transformer
    }

    #[tokio::test]
    async fn test_pipeline_with_router() {
        let filter_def = StageDefinition {
            name: "filter".to_string(),
            r#type: "filter".to_string(),
            config: json!({
                "mode": "include",
                "conditions": [
                    {
                        "field": "status",
                        "equals": "active"
                    }
                ],
                "logic": "AND"
            }),
        };

        let router_def = StageDefinition {
            name: "router".to_string(),
            r#type: "router".to_string(),
            config: json!({
                "route_by": "category",
                "routes": {
                    "electronics": "inventory"
                },
                "default": "general",
                "metadata_field": "destination"
            }),
        };

        let config = PipelineConfig {
            name: "test".to_string(),
            topic: "test-topic".to_string(),
            debezium_envelope: false,
            staging_table: "test-table".to_string(),
            dlq: None,
            stages: vec![filter_def, router_def],
            required_fields: vec![],
            backpressure: BackpressureConfig {
                channel_capacity: 100,
            },
            batching: Default::default(),
            circuit_breaker: Default::default(),
            worker_threads: 1,
        };

        let pipeline = Pipeline::from_config(&config).unwrap();

        let ctx = create_test_context();
        let msg = json!({"status": "active", "category": "electronics", "id": 123});

        let results = pipeline.execute(&ctx, msg).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("destination"), Some(&json!("inventory")));
    }

    /// Verifies that cloning a `Pipeline` reproduces its configuration and runtime behavior.
    ///
    /// This test builds a pipeline with a filter and a transformer, clones it via `Clone`,
    /// and asserts that the clone has the same configuration and produces identical outputs
    /// (including transformed fields) when executing the same input message.
    ///
    /// # Examples
    ///
    /// ```
    /// // Build a pipeline config with a filter and transformer, clone the pipeline,
    /// // and assert both pipelines produce the same results.
    /// let config = PipelineConfig { /* ... */ };
    /// let original = Pipeline::from_config(&config).unwrap();
    /// let cloned = original.clone();
    /// let ctx = create_test_context();
    /// let msg = json!({"status": "active", "id": 123});
    /// let r1 = original.execute(&ctx, msg.clone()).await.unwrap();
    /// let r2 = cloned.execute(&ctx, msg).await.unwrap();
    /// assert_eq!(r1.len(), r2.len());
    /// assert_eq!(r1[0].get("id"), r2[0].get("id"));
    /// assert!(r2[0].get("processed_at").is_some());
    /// ```
    #[tokio::test]
    async fn test_pipeline_clone() {
        let filter_def = StageDefinition {
            name: "filter".to_string(),
            r#type: "filter".to_string(),
            config: json!({
                "mode": "include",
                "conditions": [
                    {
                        "field": "status",
                        "equals": "active"
                    }
                ],
                "logic": "AND"
            }),
        };

        let transformer_def = StageDefinition {
            name: "transformer".to_string(),
            r#type: "transformer".to_string(),
            config: json!({
                "transformations": [
                    {
                        "type": "add_field",
                        "name": "processed_at",
                        "value": "{{now}}"
                    }
                ]
            }),
        };

        let config = PipelineConfig {
            name: "test".to_string(),
            topic: "test-topic".to_string(),
            debezium_envelope: false,
            staging_table: "test-table".to_string(),
            dlq: None,
            stages: vec![filter_def, transformer_def],
            required_fields: vec![],
            backpressure: BackpressureConfig {
                channel_capacity: 100,
            },
            batching: Default::default(),
            circuit_breaker: Default::default(),
            worker_threads: 1,
        };

        let original_pipeline = Pipeline::from_config(&config).unwrap();
        let cloned_pipeline = original_pipeline.clone();

        // Verify the cloned pipeline has the same configuration
        assert_eq!(original_pipeline.name, cloned_pipeline.name);
        assert_eq!(original_pipeline.config.name, cloned_pipeline.config.name);
        assert_eq!(
            original_pipeline.config.stages.len(),
            cloned_pipeline.config.stages.len()
        );
        assert_eq!(original_pipeline.stages.len(), cloned_pipeline.stages.len());

        // Verify the cloned pipeline behaves identically
        let ctx = create_test_context();
        let msg = json!({"status": "active", "id": 123});

        let original_results = original_pipeline.execute(&ctx, msg.clone()).await.unwrap();
        let cloned_results = cloned_pipeline.execute(&ctx, msg).await.unwrap();

        assert_eq!(original_results.len(), cloned_results.len());
        assert_eq!(original_results[0].get("id"), cloned_results[0].get("id"));
        assert!(cloned_results[0].get("processed_at").is_some());
    }

    #[tokio::test]
    async fn test_stage_factory() {
        let stage_def = StageDefinition {
            name: "test-filter".to_string(),
            r#type: "filter".to_string(),
            config: json!({
                "mode": "include",
                "conditions": [
                    {
                        "field": "status",
                        "equals": "active"
                    }
                ],
                "logic": "AND"
            }),
        };

        let stage = StageFactory::create(&stage_def).unwrap();
        assert_eq!(stage.name(), "test-filter");
    }

    #[tokio::test]
    async fn test_stage_factory_idempotent_receiver() {
        let stage_def = StageDefinition {
            name: "idemp-receiver".to_string(),
            r#type: "idempotent_receiver".to_string(),
            config: json!({
                "key_field": "id",
                "ttl_seconds": 3600,
                "storage": "redis",
                "redis_url": "redis://localhost:6379",
                "fallback_on_error": "pass"
            }),
        };

        let stage = StageFactory::create(&stage_def).unwrap();
        assert_eq!(stage.name(), "idemp-receiver");
    }

    #[tokio::test]
    async fn test_stage_error_creation() {
        let err = StageError::new("TEST_CODE", "test message");
        assert_eq!(err.code, "TEST_CODE");
        assert_eq!(err.message, "test message");
        assert!(!err.retryable);
        assert_eq!(err.to_string(), "TEST_CODE: test message");
    }

    #[tokio::test]
    async fn test_pipeline_execute_with_error_stage() {
        // Create a mock stage that returns an error
        #[derive(Clone)]
        struct ErrorStage;

        #[async_trait]
        impl Stage for ErrorStage {
            /// Always produces a `StageResult::Error` containing a `StageError` intended for testing.
            ///
            /// # Examples
            ///
            /// ```no_run
            /// // Given a `stage` exposing the `process` method:
            /// let res = tokio::runtime::Runtime::new().unwrap().block_on(async {
            ///     stage.process(&ctx, message).await
            /// }).unwrap();
            ///
            /// match res {
            ///     StageResult::Error(err) => assert_eq!(err.code, "TEST_ERROR"),
            ///     _ => panic!("expected StageResult::Error"),
            /// }
            /// ```
            ///
            /// # Returns
            ///
            /// `StageResult::Error(StageError)` with code `TEST_ERROR` and message `"stage error for testing"`.
            async fn process(
                &self,
                _ctx: &StageContext,
                _msg: Value,
            ) -> Result<StageResult, ProcessingError> {
                Ok(StageResult::Error(StageError::new(
                    "TEST_ERROR",
                    "stage error for testing",
                )))
            }

            /// Stage name used for metrics and logging.
            ///
            /// # Examples
            ///
            /// ```
            /// // assuming `error_stage` implements `name() -> &str`
            /// let stage_name = error_stage.name();
            /// assert_eq!(stage_name, "error-stage");
            /// ```
            fn name(&self) -> &str {
                "error-stage"
            }
        }

        // Create a pipeline with the error stage
        let config = PipelineConfig {
            name: "test".to_string(),
            topic: "test-topic".to_string(),
            debezium_envelope: false,
            staging_table: "test-table".to_string(),
            dlq: None,
            stages: vec![],
            required_fields: vec![],
            backpressure: BackpressureConfig {
                channel_capacity: 100,
            },
            batching: Default::default(),
            circuit_breaker: Default::default(),
            worker_threads: 1,
        };

        // Manually create a pipeline with our error stage
        let mut pipeline = Pipeline::from_config(&config).unwrap();
        pipeline.stages = vec![Box::new(ErrorStage)];

        let ctx = create_test_context();
        let msg = json!({"id": 123});

        let result = pipeline.execute(&ctx, msg).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ProcessingError::Stage { .. }));
    }

    #[tokio::test]
    async fn test_pipeline_execute_empty_messages_after_filter() {
        // Create a filter that rejects everything
        let filter_def = StageDefinition {
            name: "filter".to_string(),
            r#type: "filter".to_string(),
            config: json!({
                "mode": "include",
                "conditions": [
                    {
                        "field": "status",
                        "equals": "active"
                    }
                ],
                "logic": "AND"
            }),
        };

        let config = PipelineConfig {
            name: "test".to_string(),
            topic: "test-topic".to_string(),
            debezium_envelope: false,
            staging_table: "test-table".to_string(),
            dlq: None,
            stages: vec![filter_def],
            required_fields: vec![],
            backpressure: BackpressureConfig {
                channel_capacity: 100,
            },
            batching: Default::default(),
            circuit_breaker: Default::default(),
            worker_threads: 1,
        };

        let pipeline = Pipeline::from_config(&config).unwrap();

        let ctx = create_test_context();
        let msg = json!({"status": "inactive", "id": 123});

        let results = pipeline.execute(&ctx, msg).await.unwrap();
        // Filter should skip the message, resulting in empty output
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn test_stage_default_lifecycle_methods() {
        // Test default implementations of initialize, shutdown, health_check
        #[derive(Clone)]
        struct DefaultLifecycleStage;

        #[async_trait]
        impl Stage for DefaultLifecycleStage {
            /// Passes the given message to the next stage without modification.
            ///
            /// The input `msg` is forwarded as-is inside a `StageResult::Continue`.
            ///
            /// # Examples
            ///
            /// ```
            /// use serde_json::json;
            ///
            /// // Example usage (pseudo-call):
            /// // let result = stage.process(&ctx, json!({"key":"value"})).await?;
            /// // assert!(matches!(result, crate::StageResult::Continue(_)));
            /// ```
            async fn process(
                &self,
                _ctx: &StageContext,
                msg: Value,
            ) -> Result<StageResult, ProcessingError> {
                Ok(StageResult::Continue(msg))
            }

            /// The default stage name used for lifecycle-only stages.
            ///
            /// # Examples
            ///
            /// ```
            /// struct S;
            /// impl S {
            ///     fn name(&self) -> &str { "default-lifecycle" }
            /// }
            /// let s = S;
            /// assert_eq!(s.name(), "default-lifecycle");
            /// ```
            ///
            /// @returns The default stage name `"default-lifecycle"`.
            fn name(&self) -> &str {
                "default-lifecycle"
            }
            // Using default implementations for initialize, shutdown, health_check
        }

        let stage = DefaultLifecycleStage;

        // All these should return Ok(())
        assert!(stage.initialize().await.is_ok());
        assert!(stage.shutdown().await.is_ok());
        assert!(stage.health_check().await.is_ok());
    }

    #[tokio::test]
    async fn test_message_metadata_creation() {
        let metadata =
            MessageMetadata::from_kafka("test-topic".to_string(), 3, 100, Some(1234567890));
        assert_eq!(metadata.topic, "test-topic");
        assert_eq!(metadata.partition, 3);
        assert_eq!(metadata.offset, 100);
        assert_eq!(metadata.timestamp, Some(1234567890));
        assert!(metadata.headers.is_empty());
    }
}
