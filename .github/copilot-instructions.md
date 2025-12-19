# GitHub Copilot Instructions for `rcl`

Rust-based CDC (Change Data Capture) pipeline: Kafka → Debezium decoder → EIP processors → Postgres.

## 🏗 Architecture Overview

**Core Flow:** Kafka StreamConsumer → mpsc channel (backpressure-gated) → Processing loop → EIP Pipeline → Writer (Postgres with retry/fallback).

**Four Concurrent Tasks (in `src/consumer.rs`):**
1. **Fetch Loop** - Polls `rdkafka::StreamConsumer`, tracks consumer lag, sends raw messages to bounded `mpsc`.
2. **Processing Loop** - Receives from `mpsc`, runs EIP pipeline (filter/transform/split/route), writes to DB, routes errors to DLQ, commits offsets.
3. **Heartbeat Task** - Monitors consumer staleness (staleness > `health_check_timeout_ms` = Unhealthy), updates health registry.
4. **Metrics Exporter** - HTTP server exposing Prometheus metrics, health checks, and readiness probes.

**Key Components:**
- [Consumer](src/consumer.rs): Subscription, offset commits, backpressure via `config.backpressure.channel_capacity`.
- [Writer](src/writer.rs): Postgres with exponential backoff retry for `TransportError`. Tries `COPY` (bulk) → falls back to `INSERT` (row). Validates table names to prevent SQL injection.
- [Decoder](src/decoder.rs): JSON parsing + Debezium envelope unwrapping (extracts `payload` → checks `op` → gets `after`/`before` → injects `operation_type`).
- [EIP Pipeline](src/eip.rs): Configurable stages (`Filter`, `Transformer`, `Router`, `Splitter`). Stages implement `async_trait::async_trait` `Stage` trait with lifecycle methods.
- [DLQ](src/dlq.rs): Kafka producer for dead-letter messages (format: original payload + metadata + error reason).
- [Config](src/config.rs): Loads from env, validates pipelines (topic → table mappings + EIP stage definitions + security settings).
- [Health Registry](src/health.rs): Tracks Kafka/Postgres/pipeline statuses via `ComponentStatus` (Healthy/Degraded/Unhealthy).
- [Shutdown Coordinator](src/shutdown.rs): Broadcast channel for graceful shutdown signals to all tasks.
- [Metrics](src/metrics.rs): Prometheus metrics with HTTP endpoints (`/metrics`, `/health`, `/ready`).
- [Offset Tracker](src/offset_tracker.rs): Persists consumed offsets to Postgres `offset_tracker` table for recovery on restart (exactly-once semantics).
- [Retry Strategy](src/retry.rs): Exponential backoff with jitter for retryable errors (TransportError).
- [OpenTelemetry Integration](otel-collector-config.yaml): Distributed tracing support via OTLP endpoint.

## 🛠 Developer Workflow

**Always use `Makefile`** for consistency:
- `make dev` - Format, lint, check, test (pre-commit gate).
- `make run` - Runs the binary with default config from env.
- `make docker-up` / `make docker-down` - Local Kafka/Postgres/Zookeeper/OpenTelemetry stack.

**Environment Variables:**
- `RCL_CONFIG_PATH` - Path to JSON configuration file (required)
- `RCL_KAFKA_BROKERS` - Kafka broker list (overrides config file)
- `RCL_KAFKA_GROUP_ID` - Consumer group ID (overrides config file)
- `RCL_POSTGRES_URL` - Postgres connection URL (overrides config file)
- `RCL_KAFKA_SASL_USERNAME` / `RCL_KAFKA_SASL_PASSWORD` - SASL authentication

**CLI Commands:**
- `cargo run --` - Runs the ingestion service (default command)
- `cargo run -- --validate-config` - Validates configuration and exits (CI/CD friendly)
- `cargo run -- replay --topic <topic> --partition <partition> --start-offset <offset> --end-offset <offset>` - Replay specific message range for debugging
- `cargo run -- dlq inspect --topic <topic> [--limit <n>]` - Inspect DLQ messages (default limit: 10)
- `cargo run -- dlq drain --topic <topic> [--output <file.json>] [--requeue]` - Export/requeue DLQ messages
- `cargo run -- load-test [--rate <rate>] [--duration-sec <seconds>]` - Generate synthetic load (default: 1000 msg/sec, 60 sec)

**Database Notes:**
- Schema in `sql/staging_tables.sql` defines staging table structure.
- Offset tracking table (`offset_tracker`) auto-created during Writer initialization (see `sql/offset_tracker.sql`).
- Use `sqlx::query()` (runtime) not `sqlx::query!()` (compile-time) because table names are dynamic (staging pattern).
- `RecordMeta::extract(value)` pulls metadata (`_meta_topic`, `_meta_partition`, `_meta_offset`, `_meta_ingest_ts`) + `operation_type` from JSON.


## 🧩 Code Patterns & Conventions

### Error Handling Strategy (`src/errors.rs`)
Use `thiserror` macros. **Critical distinction drives retry behavior:**
- **`TransportError`** - Retryable (DB connection lost, network timeout). Handled by `backoff::future::retry` + `ExponentialBackoff` in `writer.rs`.
- **`ProcessingError` / `ValidationError` / `DebeziumError`** - Permanent (malformed JSON, bad Debezium payload, schema violation). Routed to DLQ, NOT retried.
- Call `is_retryable()` on `TransportError` to determine strategy.

### Debezium Envelope Unwrapping (`src/decoder.rs`)
Mandatory pattern when `pipeline.debezium_envelope = true`:
1. Extract `payload` field from top-level JSON.
2. Read `op` code (`c`=create, `r`=read, `u`=update, `d`=delete) → converts to `Operation` enum.
3. Extract `after` (for c/r/u) or `before` (for d) → final row data.
4. Inject `"operation_type": "<c|r|u|d>"` into row JSON.
5. Validate required fields via `pipeline.required_fields`.

### EIP Pipeline Stages (`src/stages.rs`, `src/eip.rs`)
All stages implement async `Stage` trait with lifecycle management (via `async_trait`):
```rust
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
```

**Stage Types:**
- **FilterStage**: Conditions (equals, regex, contains, in-list) + logic (AND/OR). Returns `Skip` or `Continue`.
- **TransformerStage**: Field mappings (rename, copy, extract nested), type conversions, defaults. Returns `Continue(modified)`.
- **RouterStage**: Routes message to different table based on field value. Returns `Continue` with `_meta_table` injected.
- **SplitterStage**: Explodes array fields into multiple messages. Returns `Split(Vec<Value>)`.
- **IdempotentReceiverStage**: Deduplicates messages using Redis (default) or in-memory storage. Returns `Skip` for duplicates, `Continue` for new messages.

**Stage Results:**
- `StageResult::Continue(Value)` - Continue to next stage with modified message
- `StageResult::Skip` - Skip this message (filtered out)
- `StageResult::Split(Vec<Value>)` - Split into multiple messages
- `StageResult::Error(StageError)` - Processing error with retry flag

**Error Handling:**
- `StageError` contains `code`, `message`, and `retryable` flag
- Retryable errors may be retried within the pipeline
- Non-retryable errors route to DLQ if configured
- Always implement proper error handling in custom stages

### Batching & Buffering (`src/batcher.rs`)
Messages are buffered per pipeline/table combination and flushed based on:
- **Time threshold**: `flush_interval_ms` (default: 5000ms)
- **Message count**: `max_batch_size` (default: 5000 messages)
- **Byte threshold**: `max_batch_bytes` (default: 10MB)
- **Shutdown**: All buffers flushed during graceful shutdown

**BatcherConfig fields:**
- `flush_interval_ms` - Time-based flush interval (default: 5000ms)
- `max_batch_size` - Message count per batch (default: 5000 messages)
- `max_batch_bytes` - Cumulative byte limit per batch (default: 10MB)
- `shutdown_timeout` - Timeout for graceful flush on shutdown (default: 30s)
- `adaptive_batch_enabled` - Enable/disable adaptive batch sizing (default: false)
- `adaptive_min_batch_size` - Minimum batch size in adaptive mode (default: 100)
- `adaptive_max_batch_size` - Maximum batch size in adaptive mode (default: 50000)
- `latency_window_size` - Number of recent latencies to track for moving average (default: 10)
- `latency_target_ms` - Target write latency for adaptive sizing (default: 1000ms)

**Adaptive Batching:**
When `adaptive_batch_enabled` is true, batch sizes are automatically adjusted based on observed write latencies. The system tracks the last `latency_window_size` write operations and compares the moving average latency against `latency_target_ms`. If latency is below target, batch size gradually increases (up to `adaptive_max_batch_size`) to improve throughput; if latency exceeds target, batch size decreases (down to `adaptive_min_batch_size`) to reduce latency. This enables self-tuning under varying load without manual configuration. Disable by setting `adaptive_batch_enabled` to false to use fixed `max_batch_size` thresholds. Adaptive sizing respects all other flush triggers (time, byte limits).

Batching enables efficient `COPY` bulk inserts. Failed batches are caught early; individual messages route to DLQ.

### Async/Concurrency Patterns
- `tokio::spawn` for independent background tasks (fetch loop, heartbeat, graceful shutdown).
- **Backpressure**: Bounded `mpsc` channel (`config.backpressure.channel_capacity`) prevents memory bloat. If channel full, fetch loop blocks (intentional).
- Use `ShutdownCoordinator::subscribe()` to receive shutdown signals; tasks must respect signal + timeout.
- `ReceiverStream` wraps `mpsc::Receiver` for stream-like iteration.

### Circuit Breaker & Resilience (`src/circuit_breaker.rs`)
Prevents cascading failures by tracking pipeline health via three states (Closed → Open → Half-Open):
- **Closed** (normal): Requests pass through; failures tracked. Opens if `failure_threshold` consecutive failures occur (absolute count since last state transition, not time-windowed). Failure counter resets on state transition.
- **Open** (failing): All requests rejected immediately with `CircuitBreakerError`. Remains Open for `half_open_timeout_ms` (duration in Open state before allowing limited trials), then transitions to Half-Open.
- **Half-Open** (recovery): Limited requests allowed to test recovery. Closes if `success_threshold` consecutive successes occur (absolute count since entering Half-Open, reset on state transition); reopens immediately on first failure.
- **Success/Failure semantics**: A "success" is a downstream operation completing without error (i.e., post-processing completion), independent of circuit breaker checks. Failures are retryable errors (TransportError) that indicate transient issues.
- **Threshold counts**: Both `failure_threshold` and `success_threshold` are counts per state transition, NOT time-windowed or rate-based. Counters reset when state changes. To implement windowed or rate-based semantics, the `src/circuit_breaker.rs` implementation must be updated.
- Configured per-pipeline via `pipeline.circuit_breaker`. Call `circuit_breaker.try_execute()` before processing, `record_success()`/`record_failure()` after.

### Worker Pool & Parallel Processing (`src/worker_pool.rs`)
Enables per-pipeline parallelism via `pipeline.worker_threads`:
- **Default (1 thread)**: Sequential processing, preserves strict ordering within partition.
- **Multiple threads (worker_threads > 1)**: Parallel message processing with controlled concurrency per pipeline. **Important**: With multiple threads, ordering across messages in the same partition is NOT guaranteed unless additional per-key ordering coordination is implemented. Messages are distributed to workers via round-robin, so concurrent processing may reorder partition messages. (See ordering guarantees section below.)
- **Work-claiming**: Each message is locked/claimed by one worker thread when fetched from the shared channel. Retries for that message are handled by the same worker, preventing concurrent retries of the same message (critical for idempotency).
- `WorkerPoolCoordinator` manages spawn/shutdown of worker threads; respects `ShutdownCoordinator` signals.
- **Error routing and retry semantics** (see also lines 57–61):
  - **Permanent errors** (ValidationError, ProcessingError, DebeziumError): Immediately routed to DLQ, not retried.
  - **Retryable errors** (TransportError): Applied exponential backoff via retry logic (lines 57–61). If all retries exhausted, then routed to DLQ.
  - **Circuit breaker interaction**: Open circuit short-circuits retry attempts; messages may fail-fast or escalate to DLQ according to pipeline policy.
  - **Stage-level `retryable` flag** (line 114): `StageError.retryable` is consulted by pipeline-level policies but does not override them; it informs whether an error instance is considered retryable.
- **Ordering guarantees**: With `worker_threads=1`, strict per-partition ordering is preserved. With `worker_threads>1`, ordering is not guaranteed. If ordering-by-key is required, implement additional coordination (e.g., shard work by key) or document configuration/code paths enforcing it.
- **Router stage interaction with ordering**: RouterStage injects `_meta_table` and may override destination table; this does not re-route to different partitions but may affect DLQ routing if retries differ per stage configuration.


### Logging & Observability
- Use `tracing::{info!, warn!, error!}` macros with structured fields.
- **Always include context**: `warn!(context = %ctx.correlation_id(), topic = %topic, error = %err, "...")`.
- `MessageContext::correlation_id()` = `"{topic}:{partition}:{offset}"` for tracing through the pipeline.

**Prometheus Metrics Endpoints:**
- `/metrics` - Exposes all Prometheus metrics in text format
- `/health` - Simple health check (always returns 200)
- `/ready` - Readiness probe with component status (200=healthy, 503=unhealthy)
- Configured via `service.metrics_port` (default: 9090)

**Key Metrics to Track:**
- `messages_total` - Per-topic message counter
- `decode_failures` - JSON/Debezium parsing errors
- `processing_failures` - Pipeline stage and write failures
- `dlq_total` - Messages sent to dead letter queue
- `lag_ms` - Consumer lag by topic/partition
- `write_latency_seconds` - Database write duration histogram
- `batch_size` - Messages per batch histogram
- `last_poll_timestamp` - Timestamp of last successful poll

**OpenTelemetry Integration:**
- Distributed tracing via OTLP endpoint (`service.otlp_endpoint`)
- Automatic span creation for pipeline stages and DB operations
- Collector config in `otel-collector-config.yaml`
- Use `docker-compose` to run local OpenTelemetry collector

## ⚙️ Configuration Reference

**Service Configuration:**
- `service.log_level` - Debug/Info/Warn/Error (default: Info)
- `service.metrics_port` - HTTP port for metrics/health endpoints (default: 9090)
- `service.otlp_endpoint` - OpenTelemetry collector endpoint (optional)
- `service.health_check_timeout_ms` - Staleness threshold for health checks (default: 5000)
- `service.shutdown_timeout` - Graceful shutdown timeout (default: "30s")

**Kafka Configuration:**
- `kafka.brokers` - Broker list (required, overrideable via `RCL_KAFKA_BROKERS`)
- `kafka.group_id` - Consumer group (required, overrideable via `RCL_KAFKA_GROUP_ID`)
- `kafka.security` - SASL/SSL settings (optional)
  - `tls`, `sasl_enabled`, `sasl_mechanism`, `sasl_username`, `sasl_password`
  - `ssl_ca_location`, `ssl_certificate_location`, `ssl_key_location`, `ssl_key_password`
- `kafka.fetch` - Consumer fetch tuning (optional)
  - `max_bytes` (default: 5MB), `max_wait_ms` (default: 500ms)
- `kafka.session_timeout_ms` - Consumer session timeout (default: 45s)
- `kafka.max_inflight_messages` - Concurrent messages per partition (default: 500)
- `kafka.producer_retries` - DLQ producer retry count (default: 5)
- `kafka.dlq_message_timeout_ms` - DLQ message timeout (default: 15s)
- `kafka.compression` - Message compression (lz4/snappy/gzip)
- `kafka.staleness_threshold_seconds` - Lag threshold for staleness detection (default: 300)

**Postgres Configuration:**
- `postgres.url` - Connection URL (required, overrideable via `RCL_POSTGRES_URL`)
- `postgres.ssl_mode` - SSL mode (optional)
- `postgres.ssl_root_cert` - SSL root certificate path (optional)
- `postgres.pool` - Connection pool settings (optional)
  - `max_connections` (default: 10), `acquire_timeout_ms` (default: 5000)
- `postgres.copy_enabled` - Enable COPY bulk inserts (default: true)
- `postgres.copy_batch_rows` - COPY batch size (default: 5000)
- `postgres.insert_batch_rows` - INSERT batch size (default: 500)

**Pipeline Configuration:**
- `pipelines[]` - Array of pipeline definitions
  - `name` - Pipeline identifier (unique, required)
  - `topic` - Kafka topic to consume (unique per pipeline, required)
  - `debezium_envelope` - Enable Debezium envelope unwrapping (type: boolean, default: false)
  - `staging_table` - Target table name (required, validated for SQL injection)
  - `required_fields[]` - Fields required in every message (type: array of strings, optional)
  - `backpressure.channel_capacity` - Message buffer size per pipeline (type: integer ≥ 1; default: 20000; **semantics**: this is a per-pipeline global limit shared across all worker threads, not per-thread; capacity influences backpressure on Kafka consumer—if channel full, fetch loop blocks, slowing consumption)
  - `worker_threads` - Number of worker threads for parallel message processing (type: integer ≥ 1; default: 1; **validation**: must be positive; 0 or negative values invalid)
  - `circuit_breaker` - Fault tolerance configuration (optional; if omitted, defaults to: enabled=true, failure_threshold=10, success_threshold=5, half_open_timeout_ms=30000)
    - `enabled` - Enable circuit breaker (type: boolean, default: true)
    - `failure_threshold` - Consecutive failures required to Open circuit (type: integer ≥ 1, absolute count per transition, not windowed; default: 10)
    - `success_threshold` - Consecutive successes required to Close circuit (type: integer ≥ 1, absolute count per transition, not windowed; default: 5)
    - `half_open_timeout_ms` - Duration to remain Open before transitioning to Half-Open to allow trial requests (type: integer ≥ 1, milliseconds; default: 30000)
  - `dlq` - Dead letter queue configuration (optional)
    - `topic` - DLQ topic name (required if dlq specified)
    - `max_retries` - Retry attempts before DLQ (type: integer ≥ 0, default: 3)
    - `max_payload_bytes` - Max DLQ message size (type: integer ≥ 1, default: 1MB)
  - `stages[]` - EIP pipeline stages (type: array, required)
    - `type` - Stage type (required, enum: filter/transformer/router/splitter/idempotent_receiver)
    - `name` - Stage identifier (required)
    - `config` - Stage-specific configuration (varies by type, required for each stage)
      - **filter** stage: `field`, `operator` (equals/regex/contains/in), `value`, `mode` (include/exclude), `logic` (AND/OR for multiple conditions)
      - **transformer** stage: `operations` (array of rename/copy/extract/convert/default operations), `fields` (field config)
      - **router** stage: `field` (routing key), `routes` (table name mappings)
      - **splitter** stage: `field` (array field to explode)
      - **idempotent_receiver** stage: `key_field` (field for deduplication key), `ttl_seconds` (cache TTL), `storage` (redis or in-memory), `redis_url` (if storage=redis), `fallback_on_error` (pass or fail)

**Configuration Validation:**
- Table names validated against SQL injection patterns
- Pipeline names and topics must be unique
- Required fields cannot be empty
- Channel capacity must be > 0
- Pool settings validated if configured


## 🔒 Security & Best Practices

### SQL Injection Prevention
**Always validate table identifiers before dynamic SQL construction:**
```rust
validate_table_identifier(&table_name)?;  // Must pass before QueryBuilder::new().table(table_name)
```
- Table names from `PipelineConfig.table` or `RouterStage` overrides
- Validation rules: alphanumeric + underscore, start with letter/underscore, ≤63 chars
- Supports schema.table format (one dot separator max)

### Kafka Security Configuration
- **SASL Authentication**: Set `kafka.security.sasl_*` fields for secure broker access
- **SSL/TLS**: Configure `kafka.security.tls` and certificate paths for encrypted transport
- **Environment Overrides**: Use `RCL_KAFKA_SASL_USERNAME`/`RCL_KAFKA_SASL_PASSWORD` for secrets
- **Network Security**: Prefer SSL-enabled brokers in production environments

### Database Connection Security
- **SSL Connections**: Set `postgres.ssl_mode` and `postgres.ssl_root_cert` for encrypted DB connections
- **Connection Pooling**: Configure `postgres.pool` with appropriate `max_connections` limits
- **Credential Management**: Use environment variables for sensitive connection details
- **Network Isolation**: Prefer private networking between application and database

### Message Validation & Sanitization
- **Required Fields**: Always configure `pipeline.required_fields` to prevent malformed data
- **Schema Validation**: Use Debezium envelope validation when `debezium_envelope = true`
- **Payload Size Limits**: Configure `pipeline.dlq.max_payload_bytes` to prevent memory exhaustion
- **Input Sanitization**: Validate JSON structure before processing in pipeline stages

### Operational Security
- **Access Controls**: Restrict access to configuration files and environment variables
- **Audit Logging**: Enable structured logging with correlation IDs for traceability
- **Health Monitoring**: Use `/ready` endpoint for load balancer health checks
- **Metrics Security**: Protect metrics endpoints in production environments

## 🧪 Testing & Debugging

### Local Development Setup
```bash
# Start full stack (Kafka + Postgres + Zookeeper + OpenTelemetry)
make docker-up

# Validate configuration
cargo run -- --validate-config

# Run with sample config
RCL_CONFIG_PATH=config/example.json cargo run
```

### Debugging Techniques

**Replay Mode for Message Inspection:**
```bash
cargo run -- replay --topic cdc.orders --partition 0 --start-offset 1000 --end-offset 1100
```
- Useful for reproducing issues with specific messages
- Inspect message flow through pipeline stages
- Test configuration changes against historical data

**DLQ Inspection & Recovery:**
```bash
# Inspect recent failed messages
cargo run -- dlq inspect --topic dlq.orders --limit 20

# Export failures for analysis
cargo run -- dlq drain --topic dlq.orders --output failures.json

# Requeue messages after fixing issues
cargo run -- dlq drain --topic dlq.orders --requeue
```
- Analyze error patterns and root causes
- Export failed messages for post-mortem analysis
- Reprocess messages after configuration fixes

**Load Testing:**
```bash
# Generate synthetic load for performance testing
cargo run -- load-test --rate 5000 --duration-sec 300
```
- Test backpressure behavior under load
- Validate metrics collection and alerting
- Performance benchmark pipeline throughput

### Health Checks & Monitoring

**Readiness Probe:**
```bash
curl -f http://localhost:9090/ready || echo "Service unhealthy"
```
- Returns 200 when all components (Kafka/Postgres) are healthy
- Returns 503 with component status details when unhealthy
- Suitable for load balancer health checks

**Metrics Inspection:**
```bash
curl http://localhost:9090/metrics | grep -E "(lag_ms|processing_failures|messages_total)"
```
- Monitor consumer lag and processing failures
- Track message throughput and error rates
- Alert on metric thresholds

### Common Debugging Scenarios

**High Consumer Lag:**
1. Check `lag_ms` metric for affected partitions
2. Inspect pipeline processing bottlenecks
3. Verify `backpressure.channel_capacity` settings
4. Monitor database write performance

**DLQ Message Accumulation:**
1. Use `dlq inspect` to identify error patterns
2. Check configuration validation with `--validate-config`
3. Review pipeline stage error handling
4. Fix configuration and requeue messages

**Performance Issues:**
1. Monitor `write_latency_seconds` and `batch_size` histograms
2. Check connection pool utilization
3. Validate Kafka consumer configuration
4. Profile with load testing tools


## ⚠️ Critical Implementation Details

### Only `TransportError` is retried
Use `backoff::future::retry()` with `ExponentialBackoff` (configured in `src/retry.rs` and `writer.rs`).
- Exponential backoff policy: starts at ~1ms, doubles each retry, includes jitter to prevent thundering herd when Postgres recovers.
- Max retries prevent infinite loops on permanent failures (e.g., table doesn't exist).
- If all retries exhausted: `ProcessingError` sent to DLQ, offset committed (poison pill protection).
- **Permanent errors** (ValidationError, ProcessingError, DebeziumError) skip retry and route directly to DLQ

### Writer Fallback Strategy
1. Try `INSERT ... SELECT COPY` (bulk insert).
2. If COPY fails (e.g., type mismatch): Fall back to row-by-row `INSERT`.
3. If INSERT fails: Emit `ProcessingError` → DLQ.
- Metadata extraction via `RecordMeta::extract()` must occur BEFORE writing to preserve correlation.

### Consumer Offset Management, exactly-once with idempotent writes).
- `OffsetTracker` persists partition offsets to Postgres `offset_tracker` table for recovery on restart.
- On consumer lag spike: Health registry marks Kafka as `Degraded` (staleness > `health_check_timeout_ms`).
- Replay mode (`cargo run -- replay`) allows re-processing messages by seeking to specific offsets without affecting stored offset state.
- Query `SELECT * FROM offset_tracker` to inspect stored offset state per pipeline/topic/partition.

### Metrics Tracking
Update `src/metrics.rs` for:
- `messages_total` (per-topic counter).
- `processing_failures` (decode + stage errors).
- `lag_ms` (consumer lag).
- `batch_size` (messages in-flight).
- `write_latency_seconds` (DB write duration).
- Missing metrics = debugging blind spots.

### DLQ Format & Recovery
DLQ messages contain:
- Original payload (wrapped in `{value: ...}`).
- Metadata: `topic`, `partition`, `offset`, `error_code`, `error_message`.
- Use `cargo run -- dlq --topic <t> drain --output <file>` to export for post-mortem analysis.
- Use `--requeue` flag to reinject failed messages after fixing configuration.
