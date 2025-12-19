# RCL Pipeline Grafana Dashboard Setup

## Overview

The RCL pipeline observability dashboard provides comprehensive real-time monitoring of message throughput, latency, error rates, retry behavior, and circuit breaker states across all pipelines.

## Dashboard Features

### 1. Message Throughput Panel
- **Metric**: `rate(messages_total[1m])`
- **Shows**: Current message processing rate in messages/second
- **Use Case**: Monitor pipeline velocity and identify processing slowdowns

### 2. Write Latency Percentiles Panel
- **Metrics**: 
  - p99 latency: `histogram_quantile(0.99, rate(write_latency_seconds_bucket[5m]))`
  - p95 latency: `histogram_quantile(0.95, rate(write_latency_seconds_bucket[5m]))`
  - p50 latency: `histogram_quantile(0.5, rate(write_latency_seconds_bucket[5m]))`
- **Shows**: Database write performance across percentiles
- **Thresholds**: 
  - Green: < 1s (healthy)
  - Yellow: 1-5s (degraded)
  - Red: > 5s (critical)

### 3. Consumer Lag Panel
- **Metric**: `lag_ms`
- **Shows**: How far behind the consumer is from latest messages
- **Thresholds**:
  - Green: < 300s (5 minutes)
  - Yellow: 300-600s (5-10 minutes)
  - Red: > 600s (10+ minutes)

### 4. Batch Sizes Panel
- **Metric**: `histogram_quantile(0.95, rate(batch_size_bucket[5m]))`
- **Shows**: 95th percentile batch size - how many messages per flush
- **Use Case**: Verify adaptive batch sizing is working

### 5. Batch Flush Reasons Panel
- **Metric**: `increase(batch_flush_total[5m])` by label `reason`
- **Shows**: Count of flushes per trigger reason:
  - `time`: Flush triggered by time threshold (`flush_interval_ms`)
  - `size`: Flush triggered by message count (`max_batch_size`)
  - `bytes`: Flush triggered by byte limit (`max_batch_bytes`)
  - `shutdown`: Flush during graceful shutdown
- **Unit**: Messages per flush (5-minute window)
- **Source**: RCL binary metric `batch_flush_total` (counter with "reason" label)
- **Use Case**: Understand which flush trigger is most common; indicates if adaptive batching is balanced

### 6. Error Rates Panel
- **Metrics**:
  - `rate(decode_failures[1m])`: JSON/Debezium parsing errors
  - `rate(processing_failures[1m])`: Pipeline stage and write failures
  - `rate(dlq_total[1m])`: Messages routed to dead letter queue
- **Thresholds**:
  - Green: < 1 error/sec
  - Yellow: 1-10 errors/sec
  - Red: > 10 errors/sec

### 7. Retry Attempts Panel
- **Metric**: `histogram_quantile(0.95, rate(retry_attempts_bucket[1m]))`
- **Shows**: Distribution of attempts per write operation (p95)
- **Unit**: Number of attempts (1 = first try, 2+ = retried)
- **Source**: RCL binary metric `retry_attempts` (histogram)
- **Use Case**: Identify if writes frequently require retries (indicates transient DB/network issues)

### 8. Channel Depth (Backpressure) Panel
- **Metric**: `channel_depth_per_pipeline`
- **Shows**: Current pending messages in the bounded `mpsc` channel per pipeline
- **Unit**: Messages (absolute count)
- **Source**: RCL binary metric `channel_depth_per_pipeline` (gauge with "pipeline" label)
- **Thresholds**:
  - Green: < 5000 (healthy buffering)
  - Yellow: 5000-15000 (elevated backpressure)
  - Red: > 15000 (critical, Kafka consumer may be blocked)
- **Use Case**: Indicates if message processing is falling behind; increase `backpressure.channel_capacity` or `worker_threads` if chronic

### 9. Circuit Breaker States Panel
- **Metric**: `circuit_breaker_state{pipeline=...}`
- **Shows**: State per pipeline:
  - `0` = Closed (healthy, processing normally) - Green
  - `1` = Open (unhealthy, not processing) - Red
  - `2` = Half-Open (recovering, limited processing) - Yellow
- **Unit**: Dimensionless state code
- **Source**: RCL binary metric `circuit_breaker_state` (gauge with "pipeline" label)
- **Use Case**: Identify failing pipelines at a glance

## Importing the Dashboard

### Method 1: Manual JSON Upload (Recommended)

1. Open Grafana at `http://localhost:3000`
2. Navigate to **Dashboards** → **New** → **Import**
3. In the "Import via panel JSON" box, paste the contents of:
   ```
   docker-middleware-stack/configs/grafana/dashboards/rcl-pipeline-overview.json
   ```
4. Click **Load**
5. Set Prometheus as the data source
6. Click **Import**

### Method 2: Docker Volume Mount (Best for Docker Setup)

The dashboard is automatically loaded if you add this to your `docker-compose.yml`:

```yaml
grafana:
  volumes:
    - ./docker-middleware-stack/configs/grafana/dashboards:/etc/grafana/provisioning/dashboards
    - ./docker-middleware-stack/configs/grafana/provisioning:/etc/grafana/provisioning
```

Then restart Grafana:
```bash
docker-compose restart grafana
```

## Setting Up Alerts

### Enable Alert Rules in Prometheus

1. Update your `prometheus.yml`:

```yaml
rule_files:
  - "/etc/prometheus/rcl_alert_rules.yml"

alerting:
  alertmanagers:
    - static_configs:
        - targets:
            - alertmanager:9093
```

2. Mount the rules file:
```bash
docker-compose.yml:
prometheus:
  volumes:
    - ./docker-middleware-stack/configs/prometheus/rcl_alert_rules.yml:/etc/prometheus/rcl_alert_rules.yml
```

3. Restart Prometheus:
```bash
docker-compose restart prometheus
```

### Configured Alert Rules

> **Validation Note**: All alert rules reference metrics that are actively collected by the RCL binary and exposed via Prometheus. Each metric is defined in `src/metrics.rs` and configured in Prometheus rules file `docker-middleware-stack/configs/prometheus/rcl_alert_rules.yml`. These alerts have been verified against the implementation to ensure metric names, labels, and thresholds are correct and achievable.

| Alert | Metric | Severity | Condition | Action |
|-------|--------|----------|-----------|--------|
| HighConsumerLag | `lag_ms` (Feature #3) | Warning | Lag > 10 minutes for 5 min | Check Kafka broker and network |
| CriticalConsumerLag | `lag_ms` (Feature #3) | Critical | Lag > 30 minutes for 2 min | Page on-call - pipeline stopped |
| HighErrorRate | `decode_failures`, `processing_failures` (Feature #6) | Warning | > 10 errors/sec for 2 min | Check application logs |
| DLQBacklog | `dlq_total` (Feature #6) | Warning | > 100 msg/sec to DLQ for 3 min | Review failed message patterns |
| SlowWriteLatency | `write_latency_seconds` (Feature #2) | Warning | p99 latency > 5s for 5 min | Check database performance |
| HighRetryRate | `retry_attempts` (Feature #7) | Warning | p95 attempts > 5 for 3 min | Check database connectivity; elevated retry count means transient failures |
| CircuitBreakerOpen | `circuit_breaker_state` (Feature #9) | Critical | Any circuit breaker = 1 for 1 min | Investigate pipeline failure cause |
| CircuitBreakerHalfOpen | `circuit_breaker_state` (Feature #9) | Warning | Any circuit breaker = 2 for 2 min | Monitor for recovery |
| HighChannelDepth | `channel_depth_per_pipeline` (Feature #8) | Warning | > 15000 pending messages for 3 min | Increase batch size or worker threads; Kafka consumer may be blocked |
| LowThroughput | `messages_total` (Feature #1) | Info | 0 < rate < 100 msg/sec for 10 min | Check if this is expected or investigation required |
| NoMessages | `messages_total` (Feature #1) | Warning | 0 messages in 5 min | Pipeline likely stuck or unsubscribed from topic |

## Dashboard Configuration Best Practices

### Viewing Per-Pipeline Metrics

Add variable filters to the dashboard:

1. **Settings** → **Variables** → **Add variable**
2. **Name**: `pipeline`
3. **Type**: Query
4. **Data source**: Prometheus
5. **Query**: `label_values(circuit_breaker_state, pipeline)`
6. **Multi-select**: Yes

Then update panel queries to use `{pipeline="$pipeline"}` to filter by selected pipeline.

### Setting Custom Time Ranges

Default is 6 hours. Adjust in the time picker at the top-right:
- **5 min** - Real-time monitoring
- **1 hour** - Detailed incident analysis
- **24 hours** - Trend analysis
- **7 days** - Capacity planning

### Alert Notification Channels

To receive alerts via Slack:

1. In Grafana, go to **Alerting** → **Notification channels**
2. Click **New channel**
3. **Type**: Slack
4. **Name**: Pipeline Alerts
5. **Webhook URL**: Your Slack webhook URL
6. **Message**: Configure custom alert message with `$message`, `$labels`, `$values`

Example message:
```
RCL Pipeline Alert: $title
Severity: $labels.severity
Description: $annotations.description
Value: $values
```

## Troubleshooting

### Dashboard is empty (no data)

1. **Check Prometheus connection**:
   - Verify Prometheus is running: `http://localhost:9090`
   - Check targets are healthy: **Status** → **Targets**

2. **Check metric names**:
   - Navigate to **Status** → **Targets**
   - Query a metric: `messages_total` in the query builder
   - If no results, metrics aren't being scraped

3. **Check RCL service**:
   - Verify RCL is running and exposing metrics on `:9090/metrics`
   - Check Prometheus scrape config includes RCL target

### Alerts not firing

1. **Check alert rules are loaded**:
   - Prometheus **Alerts** → verify rules appear

2. **Check alert manager**:
   - Verify Alertmanager is running: `http://localhost:9093`
   - Check **Alerts** tab to see pending/firing alerts

3. **Test alert manually**:
   - In Prometheus, query: `ALERTS{alertname="HighConsumerLag"}`
   - Should return value 1 if alert is active

## Performance Tips

### Reduce dashboard refresh rate if CPU usage is high

1. **Settings** → **Auto-refresh** → Select lower frequency
2. Increase from 10s to 30s or 1m for less frequent updates

### Use recording rules for complex queries

Create Prometheus recording rules to precompute expensive aggregations:

```yaml
groups:
  - name: rcl_recording_rules
    interval: 30s
    rules:
      - record: rcl:messages:rate1m
        expr: rate(messages_total[1m])
      - record: rcl:lag:max
        expr: max(lag_ms) by (topic)
```

## Next Steps

1. **Import the dashboard** using Method 1 or Method 2 above
2. **Configure alerts** to be notified of issues
3. **Add custom panels** for metrics specific to your setup
4. **Create runbooks** linked to alerts for incident response
5. **Monitor for 24 hours** to establish baseline performance

## Support

For issues with the dashboard or metrics:
1. Check Prometheus targets are healthy
2. Review RCL logs: `docker logs rcl`
3. Check Prometheus scrape logs: `docker logs prometheus`
4. Query metrics manually in Prometheus UI
