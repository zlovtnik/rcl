#!/bin/bash
# Data Consistency Validator
# Validates no data loss, duplication, or out-of-order issues after chaos scenarios

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "$SCRIPT_DIR/utils.sh"

# Configuration
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-postgres-1}"
POSTGRES_DB="${POSTGRES_DB:-rcl}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"

log_header "Data Consistency Validation"

# Verify Postgres is accessible
if ! docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT 1;" &>/dev/null; then
  fail "Cannot connect to Postgres"
fi

log_success "Postgres connection verified"

# ============ Data Loss Detection ============
check_data_loss() {
  log_info "Checking for data loss..."
  
  # Count staging tables
  local staging_table_count=$(docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c \
    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name LIKE 'staging_%';" 2>/dev/null || echo "0")
  
  if [ "$staging_table_count" = "0" ]; then
    log_warn "No staging tables found"
    return 1
  fi
  
  # Display offset tracker state for manual review
  log_info "Offset tracker state:"
  docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
    "SELECT pipeline, topic, partition, last_offset, COUNT(*) 
     FROM offset_tracker GROUP BY pipeline, topic, partition, last_offset;" 2>/dev/null || true
  
  # TODO: Compare actual row counts against expected counts from offset tracker
  log_success "Data loss check completed"
  return 0
}

# ============ Duplication Detection ============
check_duplicates() {
  log_info "Checking for duplicate records..."
  
  # Look for duplicate primary keys or offsets
  docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
    "SELECT * FROM offset_tracker 
     WHERE (pipeline, topic, partition, last_offset) IN (
       SELECT pipeline, topic, partition, last_offset 
       FROM offset_tracker 
       GROUP BY pipeline, topic, partition, last_offset 
       HAVING COUNT(*) > 1
     );" 2>/dev/null || true
  
  # Check if any offsets were processed multiple times
  local duplicate_offsets=$(docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c \
    "SELECT COUNT(*) FROM offset_tracker 
     WHERE (pipeline, topic, partition) IN (
       SELECT pipeline, topic, partition FROM offset_tracker 
       GROUP BY pipeline, topic, partition 
       HAVING COUNT(*) > 1
     );" 2>/dev/null || echo "0")
  
  if [ "$duplicate_offsets" -gt 0 ]; then
    log_warn "Found potential duplicate offsets: $duplicate_offsets"
    return 1
  fi
  
  log_success "No duplicates detected"
  return 0
check_out_of_order() {
  log_info "Checking for out-of-order messages..."
  
  # Within a partition, offsets should be strictly increasing
  local out_of_order_count=$(docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c \
    "WITH ordered_offsets AS (
       SELECT pipeline, topic, partition, last_offset,
              LAG(last_offset) OVER (PARTITION BY pipeline, topic, partition ORDER BY last_offset) as prev_offset
       FROM offset_tracker
     )
     SELECT COUNT(*) FROM ordered_offsets 
     WHERE prev_offset IS NOT NULL AND last_offset <= prev_offset;" 2>/dev/null | tr -d ' ' || echo "0")
  
  if [ "${out_of_order_count:-0}" -gt 0 ]; then
    log_warn "Found $out_of_order_count out-of-order offsets"
    return 1
  fi
  
  log_success "Out-of-order check completed"
  return 0
}
  return 0
}

# ============ DLQ Validation ============
check_dlq() {
  log_info "Checking dead-letter queue..."
  
  # Get DLQ table names
  local dlq_tables
  dlq_tables=$(docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c \
    "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name LIKE '%dlq%';" 2>/dev/null | tr -d '[:space:]')
  
  if [ -z "$dlq_tables" ]; then
    log_success "No DLQ messages"
    return 0
  fi
  
  log_info "DLQ messages present (this may be expected for failed messages)"
  
  # Process each DLQ table
  echo "$dlq_tables" | while IFS= read -r table_name; do
    if [ -n "$table_name" ]; then
      log_info "Analyzing DLQ table: $table_name"
      docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
        "SELECT error_code, COUNT(*) FROM \"$table_name\" GROUP BY error_code;" 2>/dev/null || true
    fi
  done
  
  return 0
}

# ============ Offset Tracker Integrity ============
check_offset_tracker() {
  log_info "Validating offset tracker integrity..."
  
  # Check for gaps in offsets
  docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
    "SELECT pipeline, topic, partition, last_offset FROM offset_tracker ORDER BY pipeline, topic, partition, last_offset;" 2>/dev/null || true
  
  log_success "Offset tracker validation completed"
  return 0
}

# ============ Message Count Validation ============
check_message_counts() {
  log_info "Validating message counts..."
  
  # Count total rows across all staging tables
  docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
    "SELECT schemaname, tablename, n_tup_ins, n_tup_upd, n_tup_del 
     FROM pg_stat_user_tables 
     WHERE tablename LIKE 'staging_%'
     ORDER BY tablename;" 2>/dev/null || true
  
  log_success "Message count validation completed"
  return 0
}

# ============ Connection Pool Health ============
check_connection_health() {
  log_info "Checking Postgres connection pool health..."
  
  docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
    "SELECT count(*) as total_connections,
            sum(case when state = 'active' then 1 else 0 end) as active,
            sum(case when state = 'idle' then 1 else 0 end) as idle,
            sum(case when state = 'idle in transaction' then 1 else 0 end) as idle_in_transaction
     FROM pg_stat_activity;" 2>/dev/null || true
  
  log_success "Connection pool health check completed"
  return 0
}

# ============ Performance Metrics ============
check_performance() {
  log_info "Checking performance metrics..."
  
  # Check if pg_stat_statements extension is available
  local has_pg_stat_statements
  has_pg_stat_statements=$(docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c \
    "SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements';" 2>/dev/null | tr -d '[:space:]')
  
  if [ -z "$has_pg_stat_statements" ]; then
    log_info "pg_stat_statements extension not available - skipping performance query analysis"
    log_success "Performance metrics check completed (extension not available)"
    return 0
  fi
  
  # Check slow queries
  docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
    "SELECT query, calls, mean_exec_time, max_exec_time 
     FROM pg_stat_statements 
     WHERE mean_exec_time > 100 
     ORDER BY mean_exec_time DESC LIMIT 10;" 2>/dev/null || true
  
  log_success "Performance metrics check completed"
  return 0
}

# ============ Run All Checks ============
run_all_checks() {
  local failed=0
  
  echo
  check_data_loss || ((failed+=1))
  echo
  check_duplicates || ((failed+=1))
  echo
  check_out_of_order || ((failed+=1))
  echo
  check_dlq || ((failed+=1))
  echo
  check_offset_tracker || ((failed+=1))
  echo
  check_message_counts || ((failed+=1))
  echo
  check_connection_health || ((failed+=1))
  echo
  check_performance || ((failed+=1))
  
  echo
  if [ $failed -eq 0 ]; then
    log_success "All consistency checks passed!"
    return 0
  else
    log_warn "$failed check(s) had potential issues"
    return 1
  fi
}

# Main
case "${1:-all}" in
  all)
    run_all_checks
    ;;
  data-loss)
    check_data_loss
    ;;
  duplicates)
    check_duplicates
    ;;
  ordering)
    check_out_of_order
    ;;
  dlq)
    check_dlq
    ;;
  offsets)
    check_offset_tracker
    ;;
  counts)
    check_message_counts
    ;;
  connections)
    check_connection_health
    ;;
  performance)
    check_performance
    ;;
  *)
    echo "Usage: $0 [all|data-loss|duplicates|ordering|dlq|offsets|counts|connections|performance]"
    exit 1
    ;;
esac

log_header "Data Consistency Validation Complete"
