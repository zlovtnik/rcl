# Enterprise Integration Patterns - Architecture Specification

## 1. Overview

This document specifies a composable, stage-based pipeline architecture that implements Enterprise Integration Patterns (EIP) for the streaming data loader.

### 1.1 Core Principles

- **Composability**: Patterns can be chained in any order
- **Configurability**: All patterns configured via JSON, no code changes
- **Observability**: Each stage emits metrics and structured logs
- **Isolation**: Stage failures don't cascade
- **Performance**: Async execution, minimal allocations

### 1.2 Architecture Diagram

```
┌─────────────┐
│   Kafka     │
│  Consumer   │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────────────────────────┐
│              Pipeline Stage Executor                     │
│                                                          │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐           │
│  │  Filter  │→→→│  Enrich  │→→→│Transform │→→→...     │
│  └──────────┘   └──────────┘   └──────────┘           │
│       ↓              ↓              ↓                    │
│    [Skip]        [Cache]        [Modify]                │
└─────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────┐
│   Batcher   │
│   Writer    │
└─────────────┘
```

---

## 2. Core Abstractions

### 2.1 Stage Trait

All EIP patterns implement this trait:

```rust
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

pub struct StageContext {
    pub correlation_id: String,
    pub pipeline_name: String,
    pub message_metadata: MessageMetadata,
}

pub struct MessageMetadata {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
    pub headers: HashMap<String, String>,
}

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

pub struct StageError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

#[async_trait]
pub trait Stage: Send + Sync {
    /// Process a message through this stage
    async fn process(&self, ctx: &StageContext, msg: Value) -> Result<StageResult>;
    
    /// Stage name for metrics/logging
    fn name(&self) -> &str;
    
    /// Initialize stage (setup connections, caches, etc.)
    async fn initialize(&mut self) -> Result<()> {
        Ok(())
    }
    
    /// Cleanup resources
    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
    
    /// Health check
    async fn health_check(&self) -> Result<()> {
        Ok(())
    }
}
```

### 2.2 Pipeline Definition

A pipeline is a sequence of stages:

```rust
pub struct Pipeline {
    pub name: String,
    pub stages: Vec<Box<dyn Stage>>,
    pub config: PipelineConfig,
    pub metrics: PipelineMetrics,
}

impl Pipeline {
    pub async fn execute(&self, ctx: StageContext, mut msg: Value) -> Result<Vec<Value>> {
        let mut messages = vec![msg];
        
        for stage in &self.stages {
            let mut next_messages = Vec::new();
            
            for current_msg in messages {
                let timer = self.metrics.stage_duration
                    .with_label_values(&[&self.name, stage.name()])
                    .start_timer();
                
                match stage.process(&ctx, current_msg).await {
                    Ok(StageResult::Continue(msg)) => {
                        next_messages.push(msg);
                    }
                    Ok(StageResult::Skip) => {
                        self.metrics.messages_filtered
                            .with_label_values(&[&self.name, stage.name()])
                            .inc();
                    }
                    Ok(StageResult::Split(msgs)) => {
                        next_messages.extend(msgs);
                        self.metrics.messages_split
                            .with_label_values(&[&self.name, stage.name()])
                            .inc();
                    }
                    Err(e) => {
                        self.metrics.stage_errors
                            .with_label_values(&[&self.name, stage.name()])
                            .inc();
                        return Err(e);
                    }
                }
                
                timer.observe_duration();
            }
            
            messages = next_messages;
            
            if messages.is_empty() {
                return Ok(vec![]);
            }
        }
        
        Ok(messages)
    }
}
```

---

## 3. Configuration Schema

### 3.1 Pipeline Configuration

```json
{
  "pipelines": [
    {
      "name": "orders-cdc",
      "topic": "cdc.orders",
      "stages": [
        {
          "type": "filter",
          "name": "operation-filter",
          "config": {
            "conditions": [
              {"field": "operation_type", "in": ["c", "u"]}
            ]
          }
        },
        {
          "type": "idempotent_receiver",
          "name": "dedup",
          "config": {
            "key_field": "order_id",
            "ttl_seconds": 3600,
            "storage": "redis",
            "redis_url": "redis://localhost:6379"
          }
        },
        {
          "type": "enricher",
          "name": "customer-lookup",
          "config": {
            "lookups": [
              {
                "key_field": "customer_id",
                "source": "postgres",
                "query": "SELECT name, tier FROM customers WHERE id = $1",
                "target_field": "customer",
                "cache_ttl_seconds": 300
              }
            ]
          }
        },
        {
          "type": "transformer",
          "name": "normalize-schema",
          "config": {
            "transformations": [
              {"type": "rename", "from": "cust_id", "to": "customer_id"},
              {"type": "convert", "field": "amount", "to": "decimal"},
              {"type": "add_field", "name": "processed_at", "value": "{{now}}"}
            ]
          }
        },
        {
          "type": "router",
          "name": "tenant-router",
          "config": {
            "route_by": "customer.tier",
            "routes": {
              "premium": "staging.premium_orders",
              "standard": "staging.standard_orders"
            },
            "default": "staging.orders"
          }
        }
      ],
      "staging_table": "staging.orders",
      "dlq": {
        "topic": "dlq.orders",
        "max_retries": 3
      }
    }
  ]
}
```

### 3.2 Stage Type Registry

```rust
pub struct StageFactory;

impl StageFactory {
    pub fn create(
        stage_type: &str,
        name: String,
        config: Value,
    ) -> Result<Box<dyn Stage>> {
        match stage_type {
            "filter" => Ok(Box::new(FilterStage::from_config(name, config)?)),
            "idempotent_receiver" => Ok(Box::new(IdempotentReceiverStage::from_config(name, config)?)),
            "enricher" => Ok(Box::new(EnricherStage::from_config(name, config)?)),
            "transformer" => Ok(Box::new(TransformerStage::from_config(name, config)?)),
            "router" => Ok(Box::new(RouterStage::from_config(name, config)?)),
            "splitter" => Ok(Box::new(SplitterStage::from_config(name, config)?)),
            "aggregator" => Ok(Box::new(AggregatorStage::from_config(name, config)?)),
            "wire_tap" => Ok(Box::new(WireTapStage::from_config(name, config)?)),
            _ => Err(anyhow::anyhow!("unknown stage type: {}", stage_type)),
        }
    }
}
```

---

## 4. Pattern Implementations

### 4.1 Filter Stage

**Purpose**: Skip messages that don't match criteria

**Configuration**:
```json
{
  "type": "filter",
  "name": "my-filter",
  "config": {
    "mode": "include",  // or "exclude"
    "conditions": [
      {"field": "status", "equals": "active"},
      {"field": "amount", "greater_than": 100},
      {"field": "tags", "contains": "urgent"},
      {"field": "email", "regex": ".*@company\\.com$"}
    ],
    "logic": "AND"  // or "OR"
  }
}
```

**Implementation**:
```rust
pub struct FilterStage {
    name: String,
    mode: FilterMode,
    conditions: Vec<Condition>,
    logic: Logic,
}

enum FilterMode { Include, Exclude }
enum Logic { And, Or }

struct Condition {
    field: String,
    operator: Operator,
}

enum Operator {
    Equals(Value),
    NotEquals(Value),
    GreaterThan(f64),
    LessThan(f64),
    Contains(String),
    Regex(regex::Regex),
    Exists,
    In(Vec<Value>),
}

#[async_trait]
impl Stage for FilterStage {
    async fn process(&self, ctx: &StageContext, msg: Value) -> Result<StageResult> {
        let matches = self.evaluate(&msg)?;
        
        let should_pass = match self.mode {
            FilterMode::Include => matches,
            FilterMode::Exclude => !matches,
        };
        
        if should_pass {
            Ok(StageResult::Continue(msg))
        } else {
            Ok(StageResult::Skip)
        }
    }
    
    fn name(&self) -> &str {
        &self.name
    }
}

impl FilterStage {
    fn evaluate(&self, msg: &Value) -> Result<bool> {
        let results: Vec<bool> = self.conditions
            .iter()
            .map(|c| c.evaluate(msg))
            .collect::<Result<Vec<_>>>()?;
        
        match self.logic {
            Logic::And => Ok(results.iter().all(|&b| b)),
            Logic::Or => Ok(results.iter().any(|&b| b)),
        }
    }
}
```

**Metrics**:
- `stage_messages_filtered{pipeline, stage}` - counter
- `stage_filter_matches{pipeline, stage, condition}` - counter

---

### 4.2 Idempotent Receiver Stage

**Purpose**: Detect and skip duplicate messages

**Configuration**:
```json
{
  "type": "idempotent_receiver",
  "name": "dedup",
  "config": {
    "key_field": "event_id",
    "key_template": "{{topic}}:{{partition}}:{{event_id}}",
    "ttl_seconds": 86400,
    "storage": "redis",
    "redis_url": "redis://localhost:6379",
    "fallback_on_error": "pass"  // or "fail"
  }
}
```

**Implementation**:
```rust
pub struct IdempotentReceiverStage {
    name: String,
    key_extractor: KeyExtractor,
    storage: Box<dyn DeduplicationStorage>,
    ttl: Duration,
    fallback: FallbackMode,
}

enum FallbackMode { Pass, Fail }

#[async_trait]
trait DeduplicationStorage: Send + Sync {
    async fn check_and_set(&self, key: &str, ttl: Duration) -> Result<bool>;
}

struct RedisStorage {
    client: redis::aio::ConnectionManager,
}

#[async_trait]
impl DeduplicationStorage for RedisStorage {
    async fn check_and_set(&self, key: &str, ttl: Duration) -> Result<bool> {
        let mut conn = self.client.clone();
        let result: Option<String> = redis::cmd("SET")
            .arg(key)
            .arg("1")
            .arg("NX")
            .arg("EX")
            .arg(ttl.as_secs())
            .query_async(&mut conn)
            .await?;
        Ok(result.is_some())
    }
}

#[async_trait]
impl Stage for IdempotentReceiverStage {
    async fn process(&self, ctx: &StageContext, msg: Value) -> Result<StageResult> {
        let key = self.key_extractor.extract(ctx, &msg)?;
        
        match self.storage.check_and_set(&key, self.ttl).await {
            Ok(is_new) => {
                if is_new {
                    Ok(StageResult::Continue(msg))
                } else {
                    Ok(StageResult::Skip)
                }
            }
            Err(e) => {
                match self.fallback {
                    FallbackMode::Pass => {
                        warn!("deduplication check failed, passing message: {}", e);
                        Ok(StageResult::Continue(msg))
                    }
                    FallbackMode::Fail => Err(e),
                }
            }
        }
    }
    
    fn name(&self) -> &str {
        &self.name
    }
}
```

**Metrics**:
- `stage_duplicates_detected{pipeline, stage}` - counter
- `stage_dedup_cache_errors{pipeline, stage}` - counter

---

### 4.3 Enricher Stage

**Purpose**: Add data from external sources

**Configuration**:
```json
{
  "type": "enricher",
  "name": "customer-enricher",
  "config": {
    "lookups": [
      {
        "name": "customer-data",
        "key_field": "customer_id",
        "source": "postgres",
        "query": "SELECT name, tier, region FROM customers WHERE id = $1",
        "target_field": "customer",
        "cache_ttl_seconds": 300,
        "on_miss": "null"  // or "error", "skip"
      },
      {
        "name": "geo-ip",
        "key_field": "ip_address",
        "source": "http",
        "url": "https://api.geo.example.com/lookup?ip={{ip_address}}",
        "target_field": "geo",
        "timeout_ms": 500,
        "cache_ttl_seconds": 3600
      }
    ],
    "parallel": true,
    "timeout_ms": 2000
  }
}
```

**Implementation**:
```rust
pub struct EnricherStage {
    name: String,
    lookups: Vec<Lookup>,
    parallel: bool,
    timeout: Duration,
}

struct Lookup {
    name: String,
    key_extractor: KeyExtractor,
    source: Box<dyn LookupSource>,
    target_field: String,
    cache: Option<Arc<Mutex<LruCache<String, Value>>>>,
    on_miss: OnMiss,
}

enum OnMiss { Null, Error, Skip }

#[async_trait]
trait LookupSource: Send + Sync {
    async fn lookup(&self, key: &str) -> Result<Option<Value>>;
}

struct PostgresLookup {
    pool: PgPool,
    query: String,
}

#[async_trait]
impl LookupSource for PostgresLookup {
    async fn lookup(&self, key: &str) -> Result<Option<Value>> {
        let row = sqlx::query(&self.query)
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;
        
        match row {
            Some(row) => {
                // Convert row to JSON
                let json = row_to_json(row)?;
                Ok(Some(json))
            }
            None => Ok(None),
        }
    }
}

#[async_trait]
impl Stage for EnricherStage {
    async fn process(&self, ctx: &StageContext, mut msg: Value) -> Result<StageResult> {
        if self.parallel {
            let futures = self.lookups.iter().map(|lookup| {
                lookup.enrich(&msg)
            });
            
            let results = timeout(
                self.timeout,
                futures::future::try_join_all(futures)
            ).await??;
            
            for (lookup, result) in self.lookups.iter().zip(results) {
                if let Some(value) = result {
                    set_field(&mut msg, &lookup.target_field, value)?;
                }
            }
        } else {
            for lookup in &self.lookups {
                if let Some(value) = lookup.enrich(&msg).await? {
                    set_field(&mut msg, &lookup.target_field, value)?;
                }
            }
        }
        
        Ok(StageResult::Continue(msg))
    }
    
    fn name(&self) -> &str {
        &self.name
    }
}

impl Lookup {
    async fn enrich(&self, msg: &Value) -> Result<Option<Value>> {
        let key = self.key_extractor.extract_from_message(msg)?;
        
        // Check cache first
        if let Some(cache) = &self.cache {
            if let Some(cached) = cache.lock().await.get(&key) {
                return Ok(Some(cached.clone()));
            }
        }
        
        // Lookup from source
        match self.source.lookup(&key).await {
            Ok(Some(value)) => {
                // Cache result
                if let Some(cache) = &self.cache {
                    cache.lock().await.put(key, value.clone());
                }
                Ok(Some(value))
            }
            Ok(None) => {
                match self.on_miss {
                    OnMiss::Null => Ok(Some(Value::Null)),
                    OnMiss::Error => Err(anyhow::anyhow!("lookup miss for key: {}", key)),
                    OnMiss::Skip => Ok(None),
                }
            }
            Err(e) => Err(e),
        }
    }
}
```

**Metrics**:
- `stage_enrichment_duration_seconds{pipeline, stage, lookup}` - histogram
- `stage_enrichment_cache_hits{pipeline, stage, lookup}` - counter
- `stage_enrichment_cache_misses{pipeline, stage, lookup}` - counter
- `stage_enrichment_errors{pipeline, stage, lookup}` - counter

---

### 4.4 Transformer Stage

**Purpose**: Modify message structure and content

**Configuration**:
```json
{
  "type": "transformer",
  "name": "normalize",
  "config": {
    "transformations": [
      {"type": "rename", "from": "cust_id", "to": "customer_id"},
      {"type": "rename", "from": "amt", "to": "amount"},
      {"type": "convert", "field": "amount", "to": "decimal"},
      {"type": "convert", "field": "created_at", "from": "unix_timestamp", "to": "iso8601"},
      {"type": "add_field", "name": "processed_at", "value": "{{now}}"},
      {"type": "remove_field", "name": "internal_notes"},
      {"type": "flatten", "field": "address", "prefix": "address_"},
      {"type": "script", "language": "rhai", "code": "msg.total = msg.quantity * msg.price;"}
    ]
  }
}
```

**Implementation**:
```rust
pub struct TransformerStage {
    name: String,
    transformations: Vec<Transformation>,
}

enum Transformation {
    Rename { from: String, to: String },
    Convert { field: String, converter: Converter },
    AddField { name: String, value: ValueGenerator },
    RemoveField { name: String },
    Flatten { field: String, prefix: String },
    Script { engine: ScriptEngine, code: String },
}

enum Converter {
    ToDecimal,
    ToInteger,
    ToString,
    UnixToIso8601,
    Iso8601ToUnix,
}

enum ValueGenerator {
    Literal(Value),
    Template(String),
    Now,
}

#[async_trait]
impl Stage for TransformerStage {
    async fn process(&self, ctx: &StageContext, mut msg: Value) -> Result<StageResult> {
        for transform in &self.transformations {
            transform.apply(&mut msg, ctx)?;
        }
        Ok(StageResult::Continue(msg))
    }
    
    fn name(&self) -> &str {
        &self.name
    }
}

impl Transformation {
    fn apply(&self, msg: &mut Value, ctx: &StageContext) -> Result<()> {
        match self {
            Transformation::Rename { from, to } => {
                if let Some(value) = remove_field(msg, from)? {
                    set_field(msg, to, value)?;
                }
            }
            Transformation::Convert { field, converter } => {
                if let Some(value) = get_field(msg, field)? {
                    let converted = converter.convert(value)?;
                    set_field(msg, field, converted)?;
                }
            }
            Transformation::AddField { name, value } => {
                let generated = value.generate(ctx)?;
                set_field(msg, name, generated)?;
            }
            Transformation::RemoveField { name } => {
                remove_field(msg, name)?;
            }
            Transformation::Flatten { field, prefix } => {
                if let Some(Value::Object(obj)) = remove_field(msg, field)? {
                    for (key, value) in obj {
                        let new_key = format!("{}{}", prefix, key);
                        set_field(msg, &new_key, value)?;
                    }
                }
            }
            Transformation::Script { engine, code } => {
                let mut context = ScriptContext {
                    message: msg.clone(),
                    stage_context: ctx.clone(),
                    variables: HashMap::new(),
                    host_functions: self.host_functions.clone(),
                };
                *msg = engine.execute(&mut context, code).await?;
            }
        }
        Ok(())
    }
}
```

**Metrics**:
- `stage_transformations_applied{pipeline, stage, type}` - counter
- `stage_transformation_errors{pipeline, stage, type}` - counter

---

### 4.5 Router Stage

**Purpose**: Route messages to different destinations

**Configuration**:
```json
{
  "type": "router",
  "name": "tenant-router",
  "config": {
    "route_by": "customer.tier",
    "routes": {
      "premium": "staging.premium_orders",
      "enterprise": "staging.enterprise_orders",
      "standard": "staging.standard_orders"
    },
    "default": "staging.orders",
    "metadata_field": "_destination_table"
  }
}
```

**Implementation**:
```rust
pub struct RouterStage {
    name: String,
    route_by: String,
    routes: HashMap<String, String>,
    default: Option<String>,
    metadata_field: String,
}

#[async_trait]
impl Stage for RouterStage {
    async fn process(&self, ctx: &StageContext, mut msg: Value) -> Result<StageResult> {
        let route_key = get_field(&msg, &self.route_by)?
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("route key not found"))?;
        
        let destination = self.routes.get(route_key)
            .or(self.default.as_ref())
            .ok_or_else(|| anyhow::anyhow!("no route for key: {}", route_key))?;
        
        // Add destination to message metadata
        set_field(&mut msg, &self.metadata_field, json!(destination))?;
        
        Ok(StageResult::Continue(msg))
    }
    
    fn name(&self) -> &str {
        &self.name
    }
}
```

**Metrics**:
- `stage_routes_taken{pipeline, stage, destination}` - counter

---

### 4.6 Splitter Stage

**Purpose**: Split one message into many

**Configuration**:
```json
{
  "type": "splitter",
  "name": "line-items-splitter",
  "config": {
    "split_field": "line_items",
    "preserve_fields": ["order_id", "customer_id", "order_date"],
    "flatten": true
  }
}
```

**Implementation**:
```rust
pub struct SplitterStage {
    name: String,
    split_field: String,
    preserve_fields: Vec<String>,
    flatten: bool,
}

#[async_trait]
impl Stage for SplitterStage {
    async fn process(&self, ctx: &StageContext, msg: Value) -> Result<StageResult> {
        let array = get_field(&msg, &self.split_field)?
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow::anyhow!("split field is not an array"))?;
        
        let preserved: Value = {
            let mut obj = serde_json::Map::new();
            for field in &self.preserve_fields {
                if let Some(value) = get_field(&msg, field)? {
                    obj.insert(field.clone(), value);
                }
            }
            Value::Object(obj)
        };
        
        let split_messages: Vec<Value> = array.iter().map(|item| {
            if self.flatten {
                // Merge preserved fields with array item
                merge_objects(preserved.clone(), item.clone())
            } else {
                // Nest array item under original field name
                let mut obj = preserved.clone();
                set_field(&mut obj, &self.split_field, item.clone()).unwrap();
                obj
            }
        }).collect();
        
        Ok(StageResult::Split(split_messages))
    }
    
    fn name(&self) -> &str {
        &self.name
    }
}
```

**Metrics**:
- `stage_messages_split{pipeline, stage}` - counter
- `stage_split_ratio{pipeline, stage}` - histogram

---

### 4.7 Script Engine Integration

**Purpose**: Enable custom message transformation logic using embedded scripting languages

#### ScriptEngine Trait

The core abstraction for script execution within the transformation pipeline:

```rust
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::time::Duration;

#[async_trait]
pub trait ScriptEngine: Send + Sync {
    /// Execute a script with the provided context
    async fn execute(
        &self,
        context: &mut ScriptContext,
        code: &str,
    ) -> Result<Value, EngineError>;

    /// Optionally compile/preload scripts for better performance
    async fn compile(&self, code: &str) -> Result<Box<dyn CompiledScript>> {
        // Default implementation returns None (no compilation)
        Err(EngineError::Unsupported("compilation not supported".to_string()))
    }

    /// Get engine metadata
    fn language(&self) -> &str;
    fn version(&self) -> &str;
}

#[async_trait]
pub trait CompiledScript: Send + Sync {
    /// Execute a pre-compiled script
    async fn execute(&self, context: &mut ScriptContext) -> Result<Value, EngineError>;
}
```

#### Execution Context

Scripts operate within a controlled `ScriptContext` that provides access to message data and limited host functions:

```rust
/// Execution context passed to scripts
pub struct ScriptContext {
    /// The current message being processed (as JSON)
    pub message: Value,

    /// Read-only stage context
    pub stage_context: StageContext,

    /// Script-local variables
    pub variables: HashMap<String, Value>,

    /// Host functions available to the script
    pub host_functions: HostFunctions,
}

/// Available host functions for scripts
pub struct HostFunctions {
    pub log: Box<dyn Fn(&str) -> Result<(), EngineError>>,
    pub now: Box<dyn Fn() -> Result<i64, EngineError>>,
    pub uuid: Box<dyn Fn() -> Result<String, EngineError>>,
    pub base64_encode: Box<dyn Fn(&str) -> Result<String, EngineError>>,
    pub base64_decode: Box<dyn Fn(&str) -> Result<String, EngineError>>,
    pub hash_sha256: Box<dyn Fn(&str) -> Result<String, EngineError>>,
    pub regex_match: Box<dyn Fn(&str, &str) -> Result<bool, EngineError>>,
    pub parse_json: Box<dyn Fn(&str) -> Result<Value, EngineError>>,
    pub stringify_json: Box<dyn Fn(&Value) -> Result<String, EngineError>>,
}
```

#### Error Handling

Structured errors for different failure modes:

```rust
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("script syntax error: {message}")]
    Syntax { message: String, line: Option<usize>, column: Option<usize> },

    #[error("script execution error: {message}")]
    Runtime { message: String },

    #[error("execution timeout after {duration:?}")]
    Timeout { duration: Duration },

    #[error("memory limit exceeded: used {used} bytes, limit {limit} bytes")]
    MemoryLimit { used: usize, limit: usize },

    #[error("execution gas limit exceeded: used {used}, limit {limit}")]
    GasLimit { used: u64, limit: u64 },

    #[error("host function error: {message}")]
    HostFunction { message: String },

    #[error("unsupported operation: {message}")]
    Unsupported { message: String },

    #[error("serialization error: {message}")]
    Serialization { message: String },
}
```

#### Execution Model

Scripts execute in per-message sandboxed VM instances with strict limits:

- **Isolation**: Each script runs in its own VM instance with no shared state
- **Time Limits**: Configurable execution timeout (default: 100ms)
- **Memory Limits**: Heap size restrictions (default: 1MB)
- **Gas Limits**: Operation counting for computational complexity control
- **Determinism**: No access to system time, randomness, or external state
- **IO-Free**: No filesystem, network, or system calls

#### Security Model

Capability-based security with explicit allowlisting:

- **No Direct IO**: Filesystem and network access completely blocked
- **Limited Host Functions**: Only explicitly registered functions available
- **Gas Metering**: Execution cost tracking with configurable limits
- **Memory Bounds**: Automatic heap size enforcement
- **Type Safety**: Strong typing prevents common vulnerabilities
- **No Reflection**: Limited to safe, controlled operations

#### Language Support

**Primary Language - Rhai**:
- Rust-based scripting language optimized for safety and performance
- Native JSON support with seamless serde integration
- Excellent error messages and debugging capabilities
- Mature ecosystem with comprehensive standard library

**Optional Languages**:
- **Lua**: Via `rlua` crate - mature, widely used, good performance
- **JavaScript**: Via `quickjs` or `deno_core` - modern syntax, extensive libraries
- Language selection configured per transformation

> **Security Note**: Only Rhai is covered by the full security review described in sections 991–1000. Lua (`rlua`) and JavaScript (`quickjs`/`deno_core`) have different sandboxing characteristics and require separate security audits before production use. Host-function and resource access (filesystem, network, timers, etc.) must be explicitly controlled and documented per-language. See follow-up audit and configuration guidance for implementation details.

#### Rhai Integration Pattern

Complete implementation example for Rhai engine integration:

```rust
use rhai::{Engine, Scope, AST, Dynamic};
use rhai::packages::Package;
use std::sync::Arc;
use tokio::time::timeout;

pub struct RhaiScriptEngine {
    engine: Arc<Engine>,
    host_functions: Arc<dyn HostFunctions + Send + Sync>,
    timeout: Duration,
    memory_limit: usize,
    gas_limit: u64,
}

impl RhaiScriptEngine {
    pub fn new(config: &ScriptConfig, host_functions: Arc<dyn HostFunctions + Send + Sync>) -> Result<Self> {
        let mut engine = Engine::new();

        // Configure engine limits
        engine.set_max_expr_depth(config.max_expr_depth.unwrap_or(64));
        engine.set_max_operations(config.max_operations.unwrap_or(10000));
        engine.set_max_string_size(config.max_string_size.unwrap_or(1024 * 1024));
        engine.set_max_array_size(config.max_array_size.unwrap_or(10000));

        // Register host functions
        register_host_functions(&mut engine, host_functions.clone())?;

        Ok(Self {
            engine: Arc::new(engine),
            host_functions,
            timeout: config.timeout.unwrap_or(Duration::from_millis(100)),
            memory_limit: config.memory_limit.unwrap_or(1024 * 1024), // 1MB
            gas_limit: config.gas_limit.unwrap_or(10000),
        })
    }

    fn register_host_functions(engine: &mut Engine, host_functions: Arc<dyn HostFunctions + Send + Sync>) -> Result<()> {
        // Logging functions - forward to host functions
        let hf_log = host_functions.clone();
        engine.register_fn("log", move |msg: &str| -> Result<(), Box<rhai::EvalAltResult>> {
            hf_log.log(msg).map_err(|e| format!("Log error: {}", e).into())
        });

        let hf_warn = host_functions.clone();
        engine.register_fn("warn", move |msg: &str| -> Result<(), Box<rhai::EvalAltResult>> {
            // Assuming warn is available on host_functions, otherwise use log
            hf_warn.log(msg).map_err(|e| format!("Warn error: {}", e).into())
        });

        let hf_error = host_functions.clone();
        engine.register_fn("error", move |msg: &str| -> Result<(), Box<rhai::EvalAltResult>> {
            // Assuming error is available on host_functions, otherwise use log
            hf_error.log(msg).map_err(|e| format!("Error error: {}", e).into())
        });

        // Time functions (deterministic)
        engine.register_fn("now", || {
            // Return configured "now" from context, not system time
            unimplemented!("provided by context")
        });

        // Utility functions
        engine.register_fn("uuid", || {
            uuid::Uuid::new_v4().to_string()
        });

        engine.register_fn("base64_encode", |input: &str| {
            base64::encode(input)
        });

        engine.register_fn("base64_decode", |input: &str| -> Result<String, Box<rhai::EvalAltResult>> {
            base64::decode(input)
                .map_err(|e| e.to_string().into())
                .and_then(|bytes| {
                    String::from_utf8(bytes)
                        .map_err(|e| e.to_string().into())
                })
        });

        engine.register_fn("hash_sha256", |input: &str| {
            use sha2::{Sha256, Digest};
            let mut hasher = Sha256::new();
            hasher.update(input);
            format!("{:x}", hasher.finalize())
        });

        engine.register_fn("regex_match", |pattern: &str, text: &str| -> Result<bool, Box<rhai::EvalAltResult>> {
            regex::Regex::new(pattern)
                .map_err(|e| e.to_string().into())
                .map(|re| re.is_match(text))
        });

        Ok(())
    }

#[async_trait]
impl ScriptEngine for RhaiScriptEngine {
    async fn execute(
        &self,
        context: &mut ScriptContext,
        code: &str,
    ) -> Result<Value, EngineError> {
        // Create script scope with context data
        let mut scope = Scope::new();

        // Expose message as 'msg' variable
        let msg_dynamic = rhai_json_to_dynamic(&context.message)?;
        scope.push("msg", msg_dynamic);

        // Expose variables
        for (key, value) in &context.variables {
            let dynamic = rhai_json_to_dynamic(value)?;
            scope.push(key, dynamic);
        }



        // Execute with timeout
        let engine = self.engine.clone();
        let code = code.to_string();

        let result = timeout(
            self.timeout,
            tokio::task::spawn_blocking(move || {
                engine.eval_with_scope::<Dynamic>(&mut scope, &code)
            })
        ).await
            .map_err(|_| EngineError::Timeout { duration: self.timeout })?
            .map_err(|e| EngineError::Runtime { message: e.to_string() })?;

        // Convert result back to JSON Value
        dynamic_to_json_value(&result)
    }

    async fn compile(&self, code: &str) -> Result<Box<dyn CompiledScript>> {
        let ast = self.engine.compile(code)
            .map_err(|e| EngineError::Syntax {
                message: e.to_string(),
                line: None,
                column: None,
            })?;

        Ok(Box::new(CompiledRhaiScript {
            ast,
            engine: self.engine.clone(),
            timeout: self.timeout,
        }))
    }

    fn language(&self) -> &str { "rhai" }
    fn version(&self) -> &str { env!("CARGO_PKG_VERSION") }
}

pub struct CompiledRhaiScript {
    ast: AST,
    engine: Arc<Engine>,
    timeout: Duration,
}

#[async_trait]
impl CompiledScript for CompiledRhaiScript {
    async fn execute(&self, context: &mut ScriptContext) -> Result<Value, EngineError> {
        let mut scope = Scope::new();

        // Same context setup as above...
        let msg_dynamic = rhai_json_to_dynamic(&context.message)?;
        scope.push("msg", msg_dynamic);

        for (key, value) in &context.variables {
            let dynamic = rhai_json_to_dynamic(value)?;
            scope.push(key, dynamic);
        }

        // Host functions are already registered globally on the engine, no need to push them to scope

        let result = timeout(self.timeout, async move {
            self.engine.eval_ast_with_scope::<Dynamic>(&mut scope, &self.ast)
        }).await
            .map_err(|_| EngineError::Timeout { duration: self.timeout })?
            .map_err(|e| EngineError::Runtime { message: e.to_string() })?;

        dynamic_to_json_value(&result)
    }
}

// Utility functions for JSON <-> Rhai conversion
fn rhai_json_to_dynamic(value: &Value) -> Result<Dynamic> {
    match value {
        Value::Null => Ok(Dynamic::UNIT),
        Value::Bool(b) => Ok(Dynamic::from(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Dynamic::from(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Dynamic::from(f))
            } else {
                Err(anyhow::anyhow!("unsupported number type"))
            }
        }
        Value::String(s) => Ok(Dynamic::from(s.clone())),
        Value::Array(arr) => {
            let mut rhai_array = rhai::Array::new();
            for item in arr {
                rhai_array.push(rhai_json_to_dynamic(item)?);
            }
            Ok(Dynamic::from(rhai_array))
        }
        Value::Object(obj) => {
            let mut rhai_map = rhai::Map::new();
            for (k, v) in obj {
                rhai_map.insert(k.clone(), rhai_json_to_dynamic(v)?);
            }
            Ok(Dynamic::from(rhai_map))
        }
    }
}

fn dynamic_to_json_value(dynamic: &Dynamic) -> Result<Value> {
    if dynamic.is_unit() {
        Ok(Value::Null)
    } else if let Some(b) = dynamic.as_bool() {
        Ok(Value::Bool(b))
    } else if let Some(i) = dynamic.as_int() {
        Ok(json!(i))
    } else if let Some(f) = dynamic.as_float() {
        Ok(json!(f))
    } else if let Some(s) = dynamic.as_str() {
        Ok(Value::String(s.to_string()))
    } else if let Some(arr) = dynamic.as_array() {
        let mut json_array = Vec::new();
        for item in arr {
            json_array.push(dynamic_to_json_value(item)?);
        }
        Ok(Value::Array(json_array))
    } else if let Some(map) = dynamic.as_map() {
        let mut json_obj = serde_json::Map::new();
        for (k, v) in map {
            json_obj.insert(k.clone(), dynamic_to_json_value(v)?);
        }
        Ok(Value::Object(json_obj))
    } else {
        Err(anyhow::anyhow!("unsupported Rhai type: {:?}", dynamic.type_name()))
    }
}
```

#### Configuration Schema

Update the transformer stage to support script transformations:

```json
{
  "type": "transformer",
  "name": "script-processor",
  "config": {
    "transformations": [
      {
        "type": "script",
        "language": "rhai",
        "code": "msg.total = msg.quantity * msg.price; msg.discounted_total = msg.total * (1.0 - msg.discount_rate);",
        "timeout_ms": 50,
        "memory_limit_bytes": 524288,
        "gas_limit": 5000
      },
      {
        "type": "script",
        "language": "rhai",
        "preload": true,
        "code": "fn calculate_tax(amount, rate) { amount * rate }",
        "compiled_id": "tax_calculator"
      },
      {
        "type": "script",
        "language": "rhai",
        "compiled_id": "tax_calculator",
        "code": "msg.tax_amount = calculate_tax(msg.subtotal, msg.tax_rate);"
      }
    ]
  }
}
```

#### Metrics

Script execution metrics integrate with the pipeline monitoring system:

- `stage_script_execution_duration_seconds{pipeline, stage, language}` - histogram
- `stage_script_execution_count{pipeline, stage, language, status}` - counter (success/error/timeout/memory_limit/gas_limit)
- `stage_script_compilation_duration_seconds{pipeline, stage, language}` - histogram
- `stage_script_cache_hits{pipeline, stage, language}` - counter
- `stage_script_errors{pipeline, stage, language, error_type}` - counter

---

## 5. Integration Points

### 5.1 Consumer Integration

```rust
// In consumer.rs
async fn handle_message(
    payload: &str,
    pipeline_config: &PipelineConfig,
    pipeline: &Pipeline,
    writer: &Writer,
) -> Result<(), ProcessingError> {
    // Parse raw message
    let mut value: Value = serde_json::from_str(payload)?;
    
    // Apply Debezium unwrapping if needed (becomes a stage)
    if pipeline_config.debezium_envelope {
        value = unwrap_debezium(value)?;
    }
    
    // Execute pipeline stages
    let ctx = StageContext {
        correlation_id: generate_correlation_id(),
        pipeline_name: pipeline_config.name.clone(),
        message_metadata: extract_metadata(),
    };
    
    let processed_messages = pipeline.execute(ctx, value).await?;
    
    // Write to database
    for msg in processed_messages {
        // Check for routing metadata
        let table = msg.get("_destination_table")
            .and_then(|v| v.as_str())
            .unwrap_or(&pipeline_config.staging_table);
        
        writer.write(&msg, table).await?;
    }
    
    Ok(())
}
```

### 5.2 Writer Integration

```rust
// In writer.rs
impl Writer {
    pub async fn write(&self, value: &Value, table: &str) -> Result<(), ProcessingError> {
        // Clean metadata fields before writing
        let mut cleaned = value.clone();
        self.remove_metadata_fields(&mut cleaned);

        self.write_to_table(&cleaned, table).await
    }

    fn remove_metadata_fields(&mut self, value: &mut Value) {
        if let Value::Object(obj) = value {
            obj.remove("_destination_table");
            obj.remove("_correlation_id");
            // ... other metadata fields
        }
    }
}
```

---

## 6. Testing Strategy

### 6.1 Unit Tests Per Stage

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_filter_stage_include() {
        let stage = FilterStage {
            name: "test".to_string(),
            mode: FilterMode::Include,
            conditions: vec![
                Condition {
                    field: "status".to_string(),
                    operator: Operator::Equals(json!("active")),
                }
            ],
            logic: Logic::And,
        };
        
        let ctx = StageContext::default();
        let msg = json!({"status": "active", "id": 123});
        
        let result = stage.process(&ctx, msg).await.unwrap();
        assert!(matches!(result, StageResult::Continue(_)));
    }
    
    #[tokio::test]
    async fn test_filter_stage_exclude() {
        let stage = FilterStage {
            name: "test".to_string(),
            mode: FilterMode::Include,
            conditions: vec![
                Condition {
                    field: "status".to_string(),
                    operator: Operator::Equals(json!("inactive")),
                }
            ],
            logic: Logic::And,
        };
        
        let ctx = StageContext::default();
        let msg = json!({"status": "active", "id": 123});
        
        let result = stage.process(&ctx, msg).await.unwrap();
        assert!(matches!(result, StageResult::Skip));
    }
}
```

### 6.2 Integration Tests

```rust
#[cfg(test)]
mod integration_tests {
    #[tokio::test]
    async fn test_full_pipeline() {
        let pipeline = Pipeline {
            name: "test-pipeline".to_string(),
            stages: vec![
                Box::new(create_filter_stage()),
                Box::new(create_enricher_stage()),
                Box::new(create_transformer_stage()),
            ],
            config: Default::default(),
            metrics: Default::default(),
        };
        
        let ctx = StageContext::default();
        let msg = json!({
            "order_id": 123,
            "customer_id": 456,
            "status": "active"
        });
        
        let results = pipeline.execute(ctx, msg).await.unwrap();
        
        assert_eq!(results.len(), 1);
        assert!(results[0].get("customer").is_some());
        assert!(results[0].get("processed_at").is_some());
    }
}
```
