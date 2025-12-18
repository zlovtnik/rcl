# ELT Infrastructure Refactor Roadmap

## Overview
This roadmap transforms your streaming CDC loader from a basic consumer into a production-grade, high-throughput ELT pipeline with proper batching, isolation, resilience, and observability.

---

## Phase 1: Foundation & Safety (Week 1)

### 1.1 Graceful Shutdown Implementation
**Priority: Critical** | **Effort: 4 hours**

- [x] Add `tokio::signal` dependency
- [x] Create `src/shutdown.rs` module with shutdown signal handling
- [x] Implement `ShutdownCoordinator` struct to broadcast shutdown signals
- [x] Add shutdown channels to fetch and processing loops
- [x] Ensure in-flight messages complete before exit
- [x] Final offset commit before shutdown
- [x] Add timeout for forced shutdown (30s default)
- [ ] Test with SIGTERM and SIGINT signals

**Deliverable:** Service stops gracefully without message loss

---

### 1.2 Health Check Endpoints
**Priority: High** | **Effort: 3 hours**

- [x] Add `/health` endpoint (always returns 200 if server is up)
- [x] Add `/ready` endpoint (checks Kafka connection, Postgres pool, pipeline health)
- [x] Implement health status tracking per pipeline
- [x] Add timestamp of last successful message processing
- [x] Return degraded status if any pipeline is unhealthy
- [x] Add configurable health check timeout in config

**Deliverable:** K8s-compatible liveness and readiness probes

---

### 1.3 Enhanced Configuration Validation
**Priority: Medium** | **Effort: 2 hours**

- [x] Add validation for duplicate pipeline names
- [x] Add validation for duplicate topics across pipelines
- [x] Validate staging table names at startup (call `validate_table_identifier`)
- [x] Add warnings for suboptimal config (e.g., batch_rows > channel_capacity)
- [x] Add `--validate-config` CLI flag to check config without starting

**Deliverable:** Fail fast on misconfiguration

---

## Phase 2: Batching & Throughput (Week 2)

### 2.1 Message Batching Coordinator
**Priority: Critical** | **Effort: 8 hours**

- [x] Create `src/batcher.rs` module
- [x] Implement `Batcher` struct with per-pipeline accumulation buffers
- [x] Add time-based flush (configurable, default 5s)
- [x] Add size-based flush (use `copy_batch_rows` from config)
- [x] Add byte-size-based flush (prevent memory bloat)
- [x] Implement flush-on-shutdown
- [x] Track batch metrics (size, flush reason)

**Config additions:**
```json
{
  "batch_flush_interval_ms": 5000,
  "batch_max_bytes": 10485760
}
```

**Deliverable:** Messages batched efficiently before writes

---

### 2.2 Integrate Batching into Consumer Loop
**Priority: Critical** | **Effort: 4 hours**

- [x] Replace individual `writer.write()` calls with `batcher.add()`
- [x] Add background task for time-based flush triggers
- [x] Commit offsets only after successful batch write
- [x] Handle batch write failures (see Phase 4.1 Transient Error Retry Logic)
- [x] Ensure ordering within partition is maintained

**Deliverable:** 10-50x throughput improvement on write path

---

### 2.3 Dynamic Batch Size Tuning (Optional)
**Priority: Low** | **Effort: 6 hours**

- [x] Track batch write latency percentiles
- [x] Implement adaptive batch sizing (increase if latency low, decrease if high)
- [x] Add metrics for batch size over time
- [x] Add config flag to enable/disable adaptive batching

**Deliverable:** Self-tuning batch sizes based on load

---

### 2.4 Basic Integration Verification
**Priority: High** | **Effort: 4 hours**

- [x] Verify graceful shutdown behavior (ensure no message loss)
- [x] Verify batching correctness (flush intervals and size triggers)
- [x] Verify basic DLQ functionality (bad messages routed correctly)
- [x] Verify per-pipeline isolation (preliminary check)

**Deliverable:** Core features verified early in development

---

## Phase 3: Per-Pipeline Isolation (Week 3)

### 3.1 Refactor to Per-Pipeline Channels
**Priority: High** | **Effort: 6 hours**

- [ ] Create separate `mpsc` channel per pipeline
- [ ] Update fetch loop to route messages to correct pipeline channel
- [ ] Update processing loop to spawn one task per pipeline
- [ ] Each pipeline task has its own batcher instance
- [ ] Ensure channels respect `backpressure.channel_capacity` config

**Deliverable:** Slow pipeline doesn't block fast ones

---

### 3.2 Per-Pipeline Worker Pool
**Priority: Medium** | **Effort: 5 hours**

- [ ] Add `worker_threads` config option per pipeline (default 1)
- [ ] Spawn N worker tasks per pipeline
- [ ] Implement work-stealing or round-robin message distribution
- [ ] Ensure offset commits happen in order (use tracking queue)
- [ ] Add per-worker metrics

**Config addition:**
```json
{
  "pipelines": [{
    "worker_threads": 2
  }]
}
```

**Deliverable:** Parallel processing within a single pipeline

---

### 3.3 Pipeline Circuit Breaker
**Priority: Medium** | **Effort: 4 hours**

- [ ] Implement circuit breaker pattern per pipeline
- [ ] Track consecutive failures (default threshold: 10)
- [ ] Open circuit after threshold, stop consuming from topic
- [ ] Half-open state with periodic retry attempts
- [ ] Close circuit after N successful writes
- [ ] Emit metrics for circuit breaker state changes
- [ ] Log clear alerts when circuit opens

**Deliverable:** Failing pipeline doesn't crash entire service

---

## Phase 4: Resilience & Retries (Week 4)

### 4.1 Transient Error Retry Logic
**Priority: High** | **Effort: 6 hours**

- [ ] Create `src/retry.rs` module with retry policies
- [ ] Implement exponential backoff (start 100ms, max 30s)
- [ ] Add jitter to prevent thundering herd
- [ ] Classify errors as retryable vs permanent
- [ ] Add max retry attempts config (default 3)
- [ ] Track retry metrics (attempts, success after N retries)
- [ ] For batch writes: retry entire batch or split into individual inserts
- [ ] Handle partial batch writes (retry logic on transient failures)

**Config addition:**
```json
{
  "retry": {
    "max_attempts": 3,
    "initial_backoff_ms": 100,
    "max_backoff_ms": 30000
  }
}
```

**Deliverable:** Transient DB/network failures don't cause data loss

---

### 4.2 Idempotency via Offset Tracking
**Priority: Medium** | **Effort: 8 hours**

- [ ] Create `offset_tracker` table in Postgres
  ```sql
  CREATE TABLE offset_tracker (
    pipeline_name TEXT,
    topic TEXT,
    partition INT,
    offset BIGINT,
    updated_at TIMESTAMPTZ,
    PRIMARY KEY (pipeline_name, topic, partition)
  );
  ```
- [ ] On startup, read last committed offsets from DB
- [ ] Seek Kafka consumer to stored offsets (override group offset)
- [ ] Update offset tracker in same transaction as data write
- [ ] Add migration script for new table
- [ ] Add config flag to enable/disable (default: disabled for backward compat)

**Deliverable:** Exactly-once semantics, safe restarts

---

### 4.3 Dead Letter Queue Enhancements
**Priority: Low** | **Effort: 4 hours**

- [ ] Track retry count in DLQ headers (currently hardcoded to "0")
- [ ] Implement DLQ consumer with retry logic
- [ ] Add exponential backoff between retry attempts
- [ ] Move to permanent failure topic after `max_retries`
- [ ] Add DLQ dashboard/alerting guidance in docs

**Deliverable:** Automated retry from DLQ

---

## Phase 5: Observability, Operations & Load Testing (Week 5)

### 5.1 Enhanced Metrics
**Priority: High** | **Effort: 5 hours**

- [ ] Add `batch_size` histogram per pipeline
- [ ] Add `batch_flush_reason` counter (time/size/bytes/shutdown)
- [ ] Add `channel_depth` gauge per pipeline
- [ ] Add `write_throughput_bytes` counter per pipeline
- [ ] Add `copy_vs_insert_ratio` counter per pipeline
- [ ] Add `retry_attempts` histogram per pipeline
- [ ] Add `circuit_breaker_state` gauge per pipeline
- [ ] Add `inflight_batches` gauge per pipeline

**Deliverable:** Complete observability into pipeline health

---

### 5.2 Structured Logging Improvements
**Priority: Medium** | **Effort: 3 hours**

- [ ] Add pipeline_name to all log events
- [ ] Add batch_id for tracking batch lifecycle
- [ ] Add correlation_id consistently across all operations
- [ ] Log batch flush decisions with reason
- [ ] Log slow batch writes (>1s threshold)
- [ ] Add sampling for high-frequency logs (1/100 messages)

**Deliverable:** Easier debugging and log analysis

---

### 5.3 Prometheus Grafana Dashboard
**Priority: Medium** | **Effort: 4 hours**

- [ ] Create example Grafana dashboard JSON
- [ ] Add panels for throughput, latency, lag per pipeline
- [ ] Add panels for batch sizes, flush reasons
- [ ] Add panels for error rates, retry rates
- [ ] Add panels for circuit breaker states
- [ ] Add alerting rules for critical metrics
- [ ] Document dashboard import process

**Deliverable:** Pre-built observability dashboard

---

### 5.4 Comprehensive Integration Suite
**Priority: High** | **Effort: 16 hours**

- [ ] Set up Testcontainers for Kafka + Postgres
- [ ] Test end-to-end message flow
- [ ] Test retry logic with transient failures
- [ ] Test circuit breaker behavior
- [ ] Test DLQ publishing and consumption
- [ ] Test per-pipeline isolation (full verification)

**Deliverable:** High-confidence integration test suite

---

### 5.5 Load Testing
**Priority: High** | **Effort: 6 hours**

- [ ] Create load test harness (Kafka producer pumping messages)
- [ ] Test at 10k, 50k, 100k msgs/sec
- [ ] Measure throughput, latency, memory usage
- [ ] Identify bottlenecks (CPU, network, disk, locks)

**Deliverable:** Validated performance at scale

---

## Phase 6: Performance & Scale (Week 6)

### 6.1 Connection Pool Optimization
**Priority: Medium** | **Effort: 3 hours**

- [ ] Make `max_connections` configurable (current: hardcoded 10)
- [ ] Add per-pipeline connection pool option
- [ ] Implement connection pool metrics (active, idle)
- [ ] Add connection pool health checks
- [ ] Tune `acquire_timeout` based on batch write latency

**Config addition:**
```json
{
  "postgres": {
    "max_connections": 50,
    "per_pipeline_pools": false
  }
}
```

**Deliverable:** Connection pool properly sized for workload

---

### 6.2 COPY Optimization
**Priority: Low** | **Effort: 4 hours**

- [ ] Fix CSV escaping bug (current: `replace('"', "\"\"")` is wrong)
- [ ] Use proper CSV library (csv crate) for COPY generation
- [ ] Implement binary COPY format option (faster than CSV)
- [ ] Add COPY buffer size tuning
- [ ] Benchmark COPY vs INSERT at various batch sizes

**Deliverable:** Optimized bulk write performance

---

### 6.3 Memory Management
**Priority: Medium** | **Effort: 4 hours**

- [ ] Implement bounded channel capacity checks before allocation
- [ ] Add memory pressure detection (track process RSS)
- [ ] Implement back-pressure to Kafka (pause consumption) when memory high
- [ ] Add config for max memory usage (soft limit)
- [ ] Add metrics for memory usage per pipeline
- [ ] Spill oversized batches to disk if necessary

**Deliverable:** Stable memory usage under high load

---

## Phase 7: Advanced Validation & Chaos (Week 7)

### 7.1 Chaos Testing
**Priority: Medium** | **Effort: 4 hours**

- [ ] Test Kafka broker failures (kill broker mid-consume)
- [ ] Test Postgres failures (kill connection mid-write)
- [ ] Test network partitions
- [ ] Test service restarts at random intervals
- [ ] Test out-of-order message scenarios
- [ ] Verify no data loss or duplication

**Deliverable:** Confidence in failure scenarios

---

### 7.2 Edge Case & Performance Validation
**Priority: High** | **Effort: 6 hours**

- [ ] Test with slow consumer (one pipeline lagging)
- [ ] Test with large messages (near max payload size)
- [ ] Verify behavior under extreme load spikes
- [ ] Validate memory limits under pressure

**Deliverable:** System proven robust under edge cases

---

## Phase 8: Documentation & Deployment (Week 8)

### 8.1 Operations Documentation
**Priority: High** | **Effort: 4 hours**

- [ ] Write deployment guide (K8s manifests, Helm chart)
- [ ] Write runbook for common issues (lag, circuit breaker open, DLQ buildup)
- [ ] Document all metrics and alerting thresholds
- [ ] Write migration guide from old version
- [ ] Document configuration best practices
- [ ] Create troubleshooting flowcharts

**Deliverable:** Complete operational documentation

---

### 8.2 Performance Tuning Guide
**Priority: Medium** | **Effort: 3 hours**

- [ ] Document how to tune batch sizes
- [ ] Document connection pool sizing
- [ ] Document worker thread configuration
- [ ] Document memory limits
- [ ] Provide example configs for different workloads (low latency vs high throughput)

**Deliverable:** Operators can tune for their workload

---

### 8.3 CI/CD Pipeline
**Priority: Medium** | **Effort: 4 hours**

- [ ] Set up GitHub Actions / GitLab CI
- [ ] Run unit tests, integration tests, lints
- [ ] Build Docker image
- [ ] Push to container registry
- [ ] Add versioning/tagging strategy
- [ ] Add automated release notes generation

**Deliverable:** Automated build and test pipeline

---

### 8.4 Advanced Operations Documentation
**Priority: High** | **Effort: 6 hours**

- [ ] Document secrets management procedures and rotation for Kafka/DB/API keys in K8s
  - **Deliverable:** Secure key rotation procedures documented
  - **Effort:** 1 hour
- [ ] Document recommended feature-flagging and blue/green/canary rollout patterns for backward compatibility and running old+new versions
  - **Deliverable:** Rollout strategy guide with compatibility patterns
  - **Effort:** 2 hours
- [ ] Define RTO/RPO targets plus backup and failover procedures for disaster recovery
  - **Deliverable:** Disaster recovery plan with RTO/RPO targets
  - **Effort:** 1.5 hours
- [ ] Document safe rollback steps (version pinning, DB migration rollbacks, playbooks)
  - **Deliverable:** Rollback procedures and playbooks
  - **Effort:** 1 hour
- [ ] Document concrete alert thresholds and escalation/on-call runbooks
  - **Deliverable:** Alerting and on-call runbooks with thresholds
  - **Effort:** 0.5 hours

**Deliverable:** Comprehensive advanced operations documentation

---

## Phase 9: Advanced Features (Optional, Week 9+)

### 9.1 Schema Evolution Support
**Priority: Low** | **Effort: 8 hours**

- [ ] Integrate with Schema Registry (Confluent or Apicurio)
- [ ] Handle schema version mismatches gracefully
- [ ] Support backward/forward compatibility checks
- [ ] Auto-create staging table columns for new fields

---

### 9.2 Multi-Tenancy
**Priority: Low** | **Effort: 10 hours**

- [ ] Add tenant_id field to all tables
- [ ] Route messages to tenant-specific staging tables
- [ ] Implement per-tenant rate limiting
- [ ] Add per-tenant metrics

---

### 9.3 Compression Support
**Priority: Low** | **Effort: 4 hours**

- [ ] Support compressed Kafka payloads (gzip, snappy, lz4)
- [ ] Decompress before processing
- [ ] Add metrics for compression ratios

---

### 9.4 Web UI for Monitoring
**Priority: Low** | **Effort: 12+ hours**

- [ ] Build admin UI showing pipeline status
- [ ] Display real-time metrics
- [ ] Allow manual circuit breaker reset
- [ ] Show recent DLQ messages
- [ ] Allow config reload without restart

---

## Summary Timeline

| Phase | Duration | Priority | Dependencies |
|-------|----------|----------|--------------|
| 1. Foundation & Safety | Week 1 | Critical | None |
| 2. Batching & Throughput | Week 2 | Critical | Phase 1 |
| 3. Per-Pipeline Isolation | Week 3 | High | Phase 2 |
| 4. Resilience & Retries | Week 4 | High | Phase 3 |
| 5. Observability | Week 5 | High | Phase 4 |
| 6. Performance & Scale | Week 6 | Medium | Phase 5 |
| 7. Testing & Validation | Week 7 | High | Phase 6 |
| 8. Documentation | Week 8 | High | Phase 7 |
| 9. Advanced Features | Week 9+ | Low | Phase 8 |

---

## Quick Wins (Do First)

If you need to prioritize, start with these high-impact, low-effort items:

1. **Graceful shutdown** (4h) - Prevents data loss
2. **Health check endpoints** (3h) - Enables proper deployment
3. **Message batching** (12h total: Phase 2.1 + 2.2) - Massive throughput improvement
4. **Enhanced metrics** (5h) - Visibility into what's happening
5. **Retry logic** (6h) - Resilience against transient failures

These 5 items (30 hours, ~1 week) will transform your service from MVP to production-ready.

---

## Success Metrics

Track these to measure improvement:

- **Throughput**: Messages/sec per pipeline (target: 10-50x improvement with batching)
- **Latency**: p50/p95/p99 end-to-end latency (target: <5s p95)
- **Reliability**: Uptime % (target: 99.9%+)
- **Data loss**: Zero tolerance
- **Recovery time**: Time to recover from failure (target: <1min)
- **Operational burden**: Incident count per week (target: near zero)