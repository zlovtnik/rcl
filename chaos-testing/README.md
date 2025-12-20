# Chaos Testing Guide

Comprehensive chaos testing implementation for validating RCL resilience under failure scenarios.

## Overview

The chaos testing suite simulates real infrastructure failures on your docker-middleware-stack to verify:

- ✅ **No data loss** - All messages processed despite failures
- ✅ **No duplication** - Offset tracking prevents duplicate processing
- ✅ **Graceful recovery** - System returns to healthy state
- ✅ **Circuit breaker behavior** - Faults isolated properly
- ✅ **DLQ routing** - Failed messages properly captured
- ✅ **Ordering preservation** - Messages remain in order (per partition)

## Prerequisites

```bash
# 1. Start the docker middleware stack
cd docker-middleware-stack
make docker-up
cd ..

# 2. Start RCL service
cargo run

# 3. Start load generator (in another terminal)
cargo run -- load-test --rate 1000 --duration-sec 3600
```

## Chaos Scenarios

### Kafka Failures

#### 1. Broker Kill (Pause)
```bash
# Pause broker mid-consume, verify recovery
CHAOS_DURATION=30 chaos-testing/kafka-failures.sh broker-kill

# Validates:
# - Consumer marks Kafka as Degraded
# - Exponential backoff retry logic
# - No message loss on broker recovery
# - Offset tracking persists progress
```

**Expected Behavior:**
- Fetch loop blocks when broker unreachable
- Health registry marks Kafka as `Degraded`
- Channel depth may increase (backpressure)
- On recovery: consumer resumes from last offset, processes backlog

**Success Criteria:**
- No data loss
- No duplicates
- Recovery within 30 seconds of broker restart

---

#### 2. Broker Restart
```bash
# Kill and restart broker (full container restart)
CHAOS_DURATION=30 chaos-testing/kafka-failures.sh broker-restart

# Validates:
# - Consumer handles complete broker downtime
# - Rebalancing works correctly
# - Offset tracking prevents duplicates
```

**Expected Behavior:**
- Broker stops completely, all connections drop
- Consumer gets connection errors, starts retrying
- Broker restarts, rejoins cluster
- Consumer reconnects, continues from last offset

**Success Criteria:**
- All messages delivered exactly once
- No gaps in offset progression
- DLQ empty (no permanent failures)

---

#### 3. Broker Network Partition
```bash
# Partition broker from network for 30s
CHAOS_DURATION=30 chaos-testing/kafka-failures.sh broker-network-partition

# Validates:
# - Network-level failures handled same as kill
# - Timeout-based failure detection
```

---

#### 4. Rebalance Scenario
```bash
# Trigger consumer group rebalancing
CHAOS_DURATION=30 chaos-testing/kafka-failures.sh rebalance

# Validates:
# - Graceful handling of rebalance
# - Offset tracking survives rebalance
# - No message duplication during rebalance
```

---

### Postgres Failures

#### 1. Connection Pool Exhaustion
```bash
# Hold 15 connections, exhaust pool, release
CHAOS_DURATION=30 chaos-testing/postgres-failures.sh pool-exhaustion

# Validates:
# - Pool acquisition timeout behavior
# - Messages route to DLQ when connections unavailable
# - Successful retry once connections freed
# - Channel depth increases due to backpressure
```

**Expected Behavior:**
- Write attempts fail with `TransportError` (connection timeout)
- Messages retry with exponential backoff
- Connections eventually release, writes succeed
- Messages in backlog processed after recovery

**Success Criteria:**
- No message loss
- DLQ messages sent to dead-letter queue
- All messages eventually delivered (in batch or individually)

---

#### 2. Slow Writes
```bash
# Add statement timeout, trigger slow queries
CHAOS_DURATION=30 chaos-testing/postgres-failures.sh slow-writes

# Validates:
# - Increased write latency doesn't cause failures
# - Metrics track latency spikes
# - No timeout errors (timeout is large)
```

---

#### 3. Connection Drop
```bash
# Terminate all idle connections
CHAOS_DURATION=30 chaos-testing/postgres-failures.sh connection-drop

# Validates:
# - Dropped connections handled gracefully
# - Pool reconnects automatically
# - No duplicate writes
```

---

#### 4. Read-Only Mode
```bash
# Set database to READ-ONLY (simulates primary/replica issue)
CHAOS_DURATION=30 chaos-testing/postgres-failures.sh readonly-mode

# Validates:
# - Writes fail with permanent error (not TransportError)
# - Messages route to DLQ
# - No retry loop (permanent error, not retryable)
```

---

#### 5. Container Pause
```bash
# Pause Postgres container (most severe)
CHAOS_DURATION=30 chaos-testing/postgres-failures.sh container-pause

# Validates:
# - All operations timeout
# - Swift failure detection
# - Graceful recovery when container resumes
```

---

### Network Failures

#### 1. Latency Injection
```bash
# Add 250ms latency to all traffic
CHAOS_DURATION=30 chaos-testing/network-failures.sh latency

# Validates:
# - Increased latency doesn't cause failures
# - Metrics track latency increase
# - Timeouts don't trigger (configured > 250ms)
```

**Expected Behavior:**
- All operations take ~250ms longer
- No timeout errors
- Throughput slightly reduced

**Success Criteria:**
- No errors in metrics
- Latency metrics show 250ms+ increase
- All messages delivered

---

#### 2. Packet Loss
```bash
# Simulate 5% random packet loss
CHAOS_DURATION=30 chaos-testing/network-failures.sh packet-loss

# Validates:
# - TCP handles retransmission gracefully
# - No higher-level failures from packet loss
```

---

#### 3. Jitter
```bash
# Add variable network delay (100ms ± 50ms)
CHAOS_DURATION=30 chaos-testing/network-failures.sh jitter

# Validates:
# - Highly variable latency handled
# - No timeout errors
```

---

#### 4. Network Partition
```bash
# Partition service from Kafka/Postgres
CHAOS_DURATION=30 chaos-testing/network-failures.sh partition

# Validates:
# - Both Kafka and Postgres become unreachable
# - Health registry marks both as Unhealthy
# - All operations fail with timeouts
# - Circuit breaker opens
# - Clean recovery on partition heal
```

---

#### 5. Bandwidth Limit
```bash
# Limit to 1Mbps (very slow connection)
CHAOS_DURATION=30 chaos-testing/network-failures.sh bandwidth-limit

# Validates:
# - Very slow but persistent connection works
# - Throughput reduced accordingly
# - No timeout errors
```

---

### Service Restart Failures

#### 1. Graceful Restart
```bash
# SIGTERM → graceful shutdown → restart
CHAOS_DURATION=30 chaos-testing/service-restart.sh graceful-restart

# Validates:
# - Shutdown signal properly received
# - Batches flushed to DB
# - Offsets committed
# - Offset tracking prevents duplicates on restart
```

**Expected Behavior:**
- Service receives SIGTERM
- In-flight batch completes or retries
- All messages up to offset N committed
- Service restarts, reads last offset from DB
- Continues from offset N+1 (no duplicates)

**Success Criteria:**
- Service shuts down within 30s
- No messages lost
- No messages duplicated
- Offset in DB matches where service stopped

---

#### 2. Hard Restart
```bash
# SIGKILL → immediate termination → restart (no graceful shutdown)
CHAOS_DURATION=30 chaos-testing/service-restart.sh hard-restart

# Validates:
# - Hard kill doesn't lose data
# - Offset tracking prevents duplicates
# - Inflight batch is retried (still in channel)
```

**Expected Behavior:**
- Service killed abruptly
- Partial batch lost (not yet in DB)
- Service restarts
- Offset tracker shows last committed offset
- Reprocesses messages from last committed offset
- May see duplicates for batch being processed (circuit breaker may route to DLQ)

---

#### 3. Cascading Restarts
```bash
# Restart service 5 times with 30s intervals
CHAOS_DURATION=30 chaos-testing/service-restart.sh restart-cascade

# Validates:
# - Multiple restarts don't cause data loss
# - Offset tracking handles repeated starts
```

---

#### 4. Mid-Batch Restart
```bash
# Let service run, kill mid-write, verify recovery
CHAOS_DURATION=30 chaos-testing/service-restart.sh mid-batch-restart

# Validates:
# - Batch in progress is handled correctly
# - No partial writes to DB
# - Batch retried from kafka after restart
```

---

## Master Test Suite

Run all scenarios sequentially:

```bash
# Run all chaos tests (takes ~15-20 minutes)
CHAOS_DURATION=30 CHAOS_LOAD=true chaos-testing/run-all.sh

# Run specific scenario
chaos-testing/run-all.sh kafka-broker-kill

# Run with different duration
CHAOS_DURATION=60 chaos-testing/run-all.sh postgres-pool-exhaustion
```

Results saved to: `/tmp/chaos-test-results-<timestamp>/`

## Data Consistency Validation

After each chaos scenario, validate system state:

```bash
# Run all consistency checks
chaos-testing/validate-consistency.sh all

# Check specific aspect
chaos-testing/validate-consistency.sh data-loss       # Verify no messages lost
chaos-testing/validate-consistency.sh duplicates      # Verify no duplicate processing
chaos-testing/validate-consistency.sh ordering        # Verify partition ordering
chaos-testing/validate-consistency.sh dlq             # Check DLQ messages
chaos-testing/validate-consistency.sh offsets         # Validate offset tracking
chaos-testing/validate-consistency.sh counts          # Row counts in staging tables
chaos-testing/validate-consistency.sh connections     # Postgres connection pool health
chaos-testing/validate-consistency.sh performance     # Query performance
```

## Metrics to Monitor

During chaos testing, watch these Prometheus metrics:

### Availability Metrics
- `health_status{component="kafka"}` - Should go Degraded/Unhealthy then recover
- `health_status{component="postgres"}` - Same as above
- `circuit_breaker_state_per_pipeline` - May open during partition, should close after recovery

### Data Flow Metrics
- `messages_total` - Should increase (eventually, after recovery)
- `lag_ms` - May spike during failures
- `dlq_total` - Should increase for retryable failures, stay 0 for transient

### Error Metrics
- `processing_failures` - Should increase during chaos
- `retry_attempts` - May spike as retries trigger
- `write_latency_seconds` - Will increase during latency injection

### Recovery Metrics
- Recovery time = time from failure injection until `messages_total` resumes increasing
- If messages stuck in channel: `channel_depth_per_pipeline` will be high

## Expected Test Results

### Success Case
```
✓ Data loss check: PASSED (all messages delivered)
✓ Duplicate check: PASSED (no reprocessed offsets)
✓ Out-of-order check: PASSED (within partition ordering maintained)
✓ Circuit breaker: Transitions Open → HalfOpen → Closed
✓ DLQ: Populated only for permanent errors
✓ Offset tracker: Monotonically increasing per (pipeline, topic, partition)
```

### Failure Case (Needs Investigation)
```
✗ Data loss detected: Final message count < baseline
✗ Duplicates detected: Same offset processed twice
✗ Out-of-order messages: Partition ordering violated
✗ Circuit breaker stuck: Remains Open after recovery
✗ DLQ overflowing: Too many messages for transient failure
```

## Integration with CI/CD

Add to GitHub Actions:

```yaml
- name: Run Chaos Tests
  run: |
    docker-compose -f docker-middleware-stack/docker-compose.yml up -d
    cargo run &
    sleep 10
    CHAOS_DURATION=30 ./chaos-testing/run-all.sh
    ./chaos-testing/validate-consistency.sh all
```

## Troubleshooting

### Test Hangs
- Check if docker stack is running: `docker-compose ps`
- Check if service is responsive: `curl http://localhost:9090/ready`
- Check logs: `docker logs rcl` or service output

### False Positives
- Metrics query failures → ensure Prometheus is running on `:9090`
- Consistency checks fail → may need to wait longer for recovery
- DLQ messages → check if they're permanent errors (expected) vs transient (unexpected)

### Performance
- Slow Docker operations → consider running on native Linux vs Docker Desktop
- Network simulation → uses `tc` which requires `--privileged` mode
- Load generator CPU → may need to reduce `--rate` if system saturated

## Future Enhancements

- [ ] Automated Grafana dashboard updates during chaos
- [ ] Slack notifications of chaos events
- [ ] Integration with chaos-engineering frameworks (Gremlin, Chaos Toolkit)
- [ ] Synthetic transaction monitoring during chaos
- [ ] Auto-remediation triggers (e.g., auto-restart on circuit breaker open > 5min)
- [ ] Multi-region chaos scenarios
- [ ] Partial broker cluster failures (some healthy, some partitioned)
