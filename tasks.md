# RCLoader Dev Spec

## Overview
Rust-based ingestion service that pulls from Kafka, validates/unwraps payloads (JSON/Avro/Protobuf, Debezium envelopes), enriches temporal metadata, and writes batched records into Postgres staging tables for downstream dbt/warehouse workflows. Targets high-throughput, low-memory, predictable latency operations.

## Goals
- Kafka → Postgres staging with bounded memory and backpressure.
- Temporal support (system/valid time enrichment) and idempotent writes.
- DLQ and replay tooling; debuggable, observable, secure-by-config.
- Deployable via Docker/Kubernetes with minimal ops overhead.

## Non-goals (initial)
- Cross-cloud abstraction layer.
- Schema registry UI/authoring.
- Running dbt inside rcloader (dbt remains external/orchestrated).

## Architecture Sketch
- Tokio-based service with `rdkafka` consumer; bounded `mpsc` for backpressure.
- Pluggable decoders (JSON/Avro/Protobuf via `serde` adapters) and Debezium envelope handler.
- Postgres writer using `sqlx` with COPY (preferred) and prepared INSERT batch fallback.
- Config-driven topic → table routing with temporal and dedup options per pipeline.
- Metrics via Prometheus exporter; tracing via OpenTelemetry; structured logs.

## Config Schema (JSONC draft)
```jsonc
{
  "service": {
    "id": "rcloader",
    "env": "dev",
    "telemetry": {
      "metrics_port": 9090,
      "tracing": { "otlp_endpoint": "http://otel-collector:4317/" }
    }
  },
  "kafka": {
    "brokers": "kafka:9092",
    "group_id": "rcloader-consumer",
    "security": { "tls": false, "sasl": { "enabled": false } },
    "fetch": { "max_bytes": 5242880, "max_wait_ms": 500 },
    "session_timeout_ms": 45000,
    "max_inflight_messages": 500
  },
  "postgres": {
    "url": "postgres://user:pass@db:5432/warehouse",
    "pool": { "max_connections": 20, "acquire_timeout_ms": 5000 },
    "copy": { "enabled": true, "batch_rows": 5000 },
    "fallback_insert_batch_rows": 500
  },
  "pipelines": [
    {
      "name": "orders-cdc",
      "topic": "cdc.orders",
      "format": "json",          // json | avro | protobuf
      "debezium_envelope": true,
      "staging_table": "stg_orders",
      "temporal": {
        "inject_system_time": true,
        "valid_from_field": "valid_from",
        "valid_to_field": "valid_to"
      },
      "dedup": { "key_fields": ["order_id"], "event_time_field": "op_ts" },
      "dlq": { "topic": "dlq.orders", "max_retries": 3 },
      "batching": { "max_rows": 2000, "max_bytes": 1048576, "max_latency_ms": 200 },
      "backpressure": { "channel_capacity": 20000 }
    }
  ],
  "ops": {
    "replay": { "enabled": true },
    "dry_run": false,
    "log_level": "info"
  }
}
```

## Functional Requirements
- Consume Kafka topics with bounded buffers; commit offsets after successful writes.
- Decode payloads (JSON initially; Avro/Protobuf pluggable) and unwrap Debezium envelopes.
- Validate required fields; route hard failures to DLQ with context.
- Enrich temporal metadata (system time, optional valid time fields) per pipeline config.
- Write in batches to Postgres staging tables; prefer COPY, fallback to prepared INSERT batches.
- Idempotency via composite key hash (key_fields + event_time); reject/ignore duplicates per policy.

## Reliability & Performance Targets
- RSS < 500MB per instance at 10–50k msg/s.
- p95 end-to-end latency < 1s for small payloads.
- Retry with jitter and DLQ after N failures; replay from offset ranges.
- Graceful backoff on Postgres outages; no data loss with at-least-once semantics.

## Security
- TLS/SASL to Kafka; TLS to Postgres.
- Secrets via env/secret manager; no secrets in config files.
- Admin endpoints gated (token/RBAC) and disabled by default.

## Observability
- Prometheus metrics: throughput, lag, batch sizes, error rates, Postgres latency.
- Tracing: OpenTelemetry spans around consume/decode/write.
- Structured logs with correlation ids (topic-partition-offset) and pipeline name.

## Ops & Tooling
- Admin CLI/API: replay offsets/ranges, DLQ drain, dry-run validation mode.
- Config hot-reload (SIGHUP or fs notify) where safe.
- Load-test harness to validate memory/latency (e.g., k6 or Rust driver).

## Roadmap & Tasks
- Phase 0 (Week 1): Scaffold crate (Tokio, rdkafka, sqlx), config loader, health/ready endpoints.
- Phase 1 (Weeks 2-3): Consumer loop with backpressure, JSON decode + Debezium unwrap, validation + DLQ.
- Phase 2 (Weeks 4-5): Postgres writer (COPY + INSERT fallback), temporal enrichment, idempotent key hash.
- Phase 3 (Weeks 6-7): Retries with jitter, replay CLI, Prometheus/OTel, readiness/startup probes, load test harness.
- Phase 4 (Weeks 8-9): Config hot-reload, TLS/SASL, secret handling, RBAC for admin endpoints.
- Phase 5 (Weeks 10-12): dbt-friendly staging schemas/contracts, DLQ drain utility, tuning (fetch.max.bytes, channel bounds, COPY vs INSERT).

## Detailed Tasklist per Phase

**Phase 0 (Week 1): Foundations**
- Initialize crate layout, workspace config, rustfmt/clippy, CI lint/test job.
- Define config schema structs and loader (JSON/JSONC), default overrides from env.
- Stub health/readiness endpoints and minimal main wiring (Tokio runtime bootstrap).
- Prove connectivity: rdkafka client init against test brokers; sqlx pool init against dev Postgres.

**Phase 1 (Weeks 2-3): Ingestion Core**
- Implement consumer loop with bounded mpsc, backpressure, offset commit post-write hook.
- JSON decoder plus Debezium envelope unwrap; payload validation with per-pipeline required fields.
- DLQ publisher for hard failures, including context (topic/partition/offset/reason).
- Structured logging with correlation ids; basic metrics for consume loop (messages, lag, failures).

**Phase 2 (Weeks 4-5): Storage Path**
- Batch builder with size/byte/latency thresholds; adaptive batch sizing guardrails.
- Postgres writer: COPY fast-path, prepared INSERT batch fallback with retries.
- Temporal enrichment: inject system time, map valid_from/valid_to when configured.
- Idempotency/dedup: hash of key_fields + event_time; configurable upsert/ignore strategy.

**Phase 3 (Weeks 6-7): Reliability & Observability**
- Retry with jitter/backoff; classify retryable vs fatal errors.
- Replay CLI for offset range reprocessing; DLQ read/inspect path.
- Prometheus metrics surface (throughput, batch stats, Postgres latency, error rates, lag); OTel tracing spans for consume/decode/write.
- Readiness/startup probes; graceful shutdown with in-flight drain.
- Load-test harness to validate RSS and p95 latency targets on synthetic data.

**Phase 4 (Weeks 8-9): Ops & Security**
- Config hot-reload (SIGHUP/fs notify) for non-critical settings; guard rails for unsafe changes.
- TLS/SASL support for Kafka; TLS for Postgres; secret sourcing from env/secret manager.
- Admin surface: RBAC/token gating for replay/DLQ endpoints; audit logs for admin actions.
- Failure-mode drills: Postgres outage backoff, Kafka rebalance resilience.

**Phase 5 (Weeks 10-12): Integrations & Tuning**
- Emit dbt-friendly staging schemas/contracts and lineage metadata (topic, partition, offset, ingest ts).
- DLQ drain utility with re-queue or file export; S3 spill option for oversized batches (if configured).
- Performance tuning: rdkafka fetch.max.bytes, channel bounds, COPY vs INSERT thresholds, pool sizing.
- Benchmark report and tuning playbook; finalize production readiness checklist.

## Deliverables per Phase
- Code and CI green; config example checked in.
- Benchmarks showing memory/latency targets on sample load.
- E2E path: Kafka → rcloader → Postgres staging → manual dbt model smoke.
- Chaos drill: Postgres outage backoff, replay proves no data loss.
