# rcl — Rust CDC pipeline

Lightweight Rust-based Change Data Capture (CDC) pipeline: Kafka → Debezium decoder → EIP processors → Postgres.

<!-- badges: build / license / crates / docs (add as needed) -->

## Quickstart

Clone and run with the example configuration:

```bash
git clone https://github.com/zlovtnik/rcl.git
cd rcl
RCL_CONFIG_PATH=config/example.json cargo run
```

Development helpers are available via the `Makefile`:

```bash
make dev      # format, lint, check, test
make run      # run the service with env config
make docker-up   # bring up local Kafka/Postgres stack for testing
```

## Overview

`rcl` consumes CDC events from Kafka, decodes Debezium envelopes, executes a configurable EIP-style processing pipeline (filter, transform, split, route), and writes results to Postgres. It provides metrics, health probes, DLQ support, and graceful shutdown handling.

Key features:
- At-least-once semantics: offsets committed after successful DB writes
- Debezium envelope unwrapping and validation
- Configurable EIP pipeline stages (`Filter`, `Transformer`, `Router`, `Splitter`, `IdempotentReceiver`)
- Bulk `COPY` with `INSERT` fallback and exponential backoff retries
- DLQ (dead-letter queue) for permanent failures
- Prometheus metrics and health/readiness probes
- Replay mode and DLQ inspection/repair CLI commands
- Circuit breaker for resilience
- Worker pool for parallel processing
- Adaptive batching

## Architecture

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
- [EIP Pipeline](src/eip.rs): Configurable stages (`Filter`, `Transformer`, `Router`, `Splitter`, `IdempotentReceiver`). Stages implement `async_trait::async_trait` `Stage` trait with lifecycle methods.
- [DLQ](src/dlq.rs): Kafka producer for dead-letter messages (format: original payload + metadata + error reason).
- [Config](src/config.rs): Loads from env, validates pipelines (topic → table mappings + EIP stage definitions + security settings).
- [Health Registry](src/health.rs): Tracks Kafka/Postgres/pipeline statuses via `ComponentStatus` (Healthy/Degraded/Unhealthy).
- [Shutdown Coordinator](src/shutdown.rs): Broadcast channel for graceful shutdown signals to all tasks.
- [Metrics](src/metrics.rs): Prometheus metrics with HTTP endpoints (`/metrics`, `/health`, `/ready`).
- [Offset Tracker](src/offset_tracker.rs): Persists consumed offsets to Postgres `offset_tracker` table for recovery on restart (exactly-once semantics).
- [Retry Strategy](src/retry.rs): Exponential backoff with jitter for retryable errors (TransportError).
- [Circuit Breaker](src/circuit_breaker.rs): Prevents cascading failures with three states (Closed → Open → Half-Open).
- [Worker Pool](src/worker_pool.rs): Enables per-pipeline parallelism via `pipeline.worker_threads`.
- [Batcher](src/batcher.rs): Buffers messages per pipeline/table, flushes based on time/count/bytes, with adaptive sizing.

See `src/` for implementation details.

## Installation

Add `rcl` as a dependency for internal crates or build from source:

```toml
# This crate is primarily an application (not a library) — run via `cargo run`.
```

## Configuration

Configuration is provided via JSON files and environment variables. Important env vars:

- `RCL_CONFIG_PATH` — path to the JSON config file (required)
- `RCL_KAFKA_BROKERS` — Kafka broker list (overrides config)
- `RCL_KAFKA_GROUP_ID` — consumer group id
- `RCL_POSTGRES_URL` — Postgres connection URL

Sample config: see `config/example.json`.

### Pipeline Configuration

Pipelines are defined in the config JSON:

```json
{
  "pipelines": [
    {
      "name": "orders_pipeline",
      "topic": "cdc.orders2",
      "debezium_envelope": true,
      "staging_table": "stg_orders",
      "required_fields": ["id"],
      "backpressure": {
        "channel_capacity": 20000
      },
      "worker_threads": 1,
      "circuit_breaker": {
        "enabled": true,
        "failure_threshold": 10,
        "success_threshold": 5,
        "half_open_timeout_ms": 30000
      },
      "stages": [
        {
          "type": "transformer",
          "name": "field_rename",
          "config": {
            "operations": [
              {"rename": {"from": "id", "to": "order_id"}},
              {"rename": {"from": "ts", "to": "event_timestamp"}},
              {"convert": {"field": "event_timestamp", "to": "timestamp"}}
            ]
          }
        }
      ]
    }
  ]
}
```

## Developer Workflow

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

## Testing & Debugging

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

## Security & Best Practices

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

## Contributing

Contributions are welcome. Please open issues or PRs against this repository. Follow the code style in `src/` and run `make dev` before submitting PRs.

## License

This repository does not include a LICENSE file by default — add one appropriate for your project.

## See Also

- `siwe-rs` README inspired the structure here: https://github.com/spruceid/siwe-rs
