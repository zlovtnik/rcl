# RCLoader Benchmark & Tuning Guide

## Load Testing

RCLoader includes a built-in load generator for synthetic testing.

```bash
# Run load test with 5000 messages/sec for 60 seconds
cargo run --release -- load-test --rate 5000 --duration-sec 60
```

### Metrics to Watch

Access Prometheus metrics at `http://localhost:9090/metrics`.

*   `rcl_messages_total`: Throughput (msgs/sec).
*   `rcl_lag_ms`: Consumer lag. Should remain stable.
*   `rcl_write_latency_seconds`: Postgres write latency.
*   `rcl_batch_size`: Average batch size. Larger batches = better throughput.
*   `process_rss_bytes`: Memory usage.

## Tuning Playbook

### Kafka Consumer

*   `kafka.fetch.max_bytes`: Increase for higher throughput if messages are large. Default: 5MB.
*   `kafka.fetch.max_wait_ms`: Increase to allow more batching at consumer level. Default: 500ms.
*   `backpressure.channel_capacity`: Increase if consumer is blocked by writer, but watch memory usage.

### Postgres Writer

*   `postgres.copy_batch_rows`: Primary knob for write throughput.
    *   Start at 2000. Increase to 5000-10000 until latency spikes.
    *   Too large = memory pressure and long transactions.
*   `postgres.pool.max_connections`: Keep low (e.g., 10-20) to avoid contention.
*   `postgres.copy_enabled`: Always keep `true` for high volume. `INSERT` fallback is much slower.

### Memory Management

*   If RSS is too high (>500MB):
    *   Reduce `backpressure.channel_capacity`.
    *   Reduce `postgres.copy_batch_rows`.
    *   Check `jemalloc` usage (if enabled).

## Production Readiness Checklist

- [ ] **Security**: TLS enabled for Kafka and Postgres. Secrets loaded from env vars.
- [ ] **Observability**: Prometheus scraping configured. Alerts set for `rcl_lag_ms` > 10s and `rcl_processing_failures` > 0.
- [ ] **Reliability**: DLQ topic exists and is monitored.
- [ ] **Database**: Staging tables created with required metadata columns (see [sql/staging_tables.sql](sql/staging_tables.sql)):
  - `_meta_topic TEXT` (nullable) - Kafka source topic name
  - `_meta_partition BIGINT` (nullable) - Kafka partition number
  - `_meta_offset BIGINT` (nullable) - Kafka offset
  - `_meta_ingest_ts BIGINT` (nullable) - Ingest timestamp (milliseconds since epoch)
  
  These columns enable traceability and support replay operations. See example schema at [sql/staging_tables.sql](sql/staging_tables.sql) for reference implementation.
- [ ] **Capacity**: `ulimit -n` increased for open files (Kafka/Postgres connections).
- [ ] **Recovery**: Tested `replay` command for a range of offsets.
