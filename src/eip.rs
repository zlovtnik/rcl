use crate::errors::ProcessingError;
use crate::stages::{FilterStage, RouterStage, SplitterStage, TransformerStage};
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

/// Core abstractions for Enterprise Integration Patterns

#[derive(Clone, Debug)]
pub struct StageContext {
    pub correlation_id: String,
    pub pipeline_name: String,
    pub message_metadata: MessageMetadata,
}

#[derive(Clone, Debug)]
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

    pub fn retryable(mut self, retryable: bool) -> Self {
        self.retryable = retryable;
        self
    }
}

#[async_trait]
pub trait Stage: Send + Sync {
    /// Process a message through this stage
    async fn process(&self, ctx: &StageContext, msg: Value)
        -> Result<StageResult, ProcessingError>;

    /// Stage name for metrics/logging
    fn name(&self) -> &str;

    /// Initialize stage (setup connections, caches, etc.)
    async fn initialize(&self) -> Result<(), ProcessingError> {
        Ok(())
    }

    /// Cleanup resources
    async fn shutdown(&self) -> Result<(), ProcessingError> {
        Ok(())
    }

    /// Health check
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

        for stage in &self.stages {
            let mut next_messages = Vec::new();

            for msg in current_messages {
                let result = stage.process(ctx, msg).await?;
                match result {
                    StageResult::Continue(new_msg) => next_messages.push(new_msg),
                    StageResult::Skip => {
                        // Skip this message
                    }
                    StageResult::Split(msgs) => {
                        next_messages.extend(msgs);
                    }
                    StageResult::Error(err) => {
                        return Err(ProcessingError::Stage(err));
                    }
                }
            }

            current_messages = next_messages;

            if current_messages.is_empty() {
                break;
            }
        }

        Ok(current_messages)
    }
}

impl Clone for Pipeline {
    fn clone(&self) -> Self {
        // Recreate the pipeline from config since stages can't be cloned
        Self::from_config(&self.config)
            .expect("Pipeline::clone failed: config was valid at construction")
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
            _ => Err(anyhow::anyhow!("unknown stage type: {}", def.r#type)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn create_test_context() -> StageContext {
        StageContext {
            correlation_id: "test-correlation".to_string(),
            pipeline_name: "test-pipeline".to_string(),
            message_metadata: MessageMetadata::from_kafka("test-topic".to_string(), 0, 0, None),
        }
    }

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
        let pipeline = Pipeline {
            name: "test-pipeline".to_string(),
            stages: vec![create_filter_stage()],
            config: PipelineConfig {
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
            },
        };

        let ctx = create_test_context();
        let msg = json!({"status": "active", "id": 123});

        let results = pipeline.execute(&ctx, msg).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("id"), Some(&json!(123)));
    }

    #[tokio::test]
    async fn test_pipeline_filter_skip() {
        let pipeline = Pipeline {
            name: "test-pipeline".to_string(),
            stages: vec![create_filter_stage()],
            config: PipelineConfig {
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
            },
        };

        let ctx = create_test_context();
        let msg = json!({"status": "inactive", "id": 123});

        let results = pipeline.execute(&ctx, msg).await.unwrap();
        assert_eq!(results.len(), 0); // Message should be filtered out
    }

    #[tokio::test]
    async fn test_pipeline_with_transformer() {
        let pipeline = Pipeline {
            name: "test-pipeline".to_string(),
            stages: vec![create_filter_stage(), create_transformer_stage()],
            config: PipelineConfig {
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
            },
        };

        let ctx = create_test_context();
        let msg = json!({"status": "active", "id": 123});

        let results = pipeline.execute(&ctx, msg).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("id"), Some(&json!(123)));
        assert!(results[0].get("processed_at").is_some()); // Should have been added by transformer
    }

    #[tokio::test]
    async fn test_pipeline_with_router() {
        let pipeline = Pipeline {
            name: "test-pipeline".to_string(),
            stages: vec![create_filter_stage(), create_router_stage()],
            config: PipelineConfig {
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
            },
        };

        let ctx = create_test_context();
        let msg = json!({"status": "active", "category": "electronics", "id": 123});

        let results = pipeline.execute(&ctx, msg).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("destination"), Some(&json!("inventory")));
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
}
