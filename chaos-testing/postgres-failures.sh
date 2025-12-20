#!/bin/bash
# Postgres Chaos Testing Script
# Simulates Postgres failures, connection pool exhaustion, and latency

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "$SCRIPT_DIR/utils.sh"

# Configuration
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-postgres-1}"
POSTGRES_DB="${POSTGRES_DB:-rcl}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
DURATION_SECS="${DURATION_SECS:-30}"
SCENARIO="${1:-pool-exhaustion}"

log_header "Postgres Chaos Testing: $SCENARIO"

case "$SCENARIO" in
  pool-exhaustion)
    log_info "Exhausting Postgres connection pool..."
    
    # Hold connections by running long-running queries
    log_info "Starting 15 idle connections for ${DURATION_SECS}s..."
    for i in {1..15}; do
      docker exec -d "$POSTGRES_CONTAINER" \
        psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
        -c "SELECT pg_sleep(${DURATION_SECS})" \
        2>/dev/null || log_warn "Failed to spawn connection $i"
    done
    
    log_info "Connection pool exhausted. Monitoring for $DURATION_SECS seconds..."
    sleep "$DURATION_SECS"
    
    # Connections will release when pg_sleep completes
    log_info "Waiting for connections to release..."
    sleep 5
    
    log_success "Connection pool exhaustion test completed"
    ;;

  slow-writes)
    log_info "Injecting write latency into Postgres by adding delays to writes..."

    # Create a function that introduces latency on writes
    docker exec "$POSTGRES_CONTAINER" \
      psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
      CREATE OR REPLACE FUNCTION slow_write_func() RETURNS trigger AS \$\$
      BEGIN
        PERFORM pg_sleep(0.1);  -- Add 100ms delay per write
        RETURN NEW;
      END;
      \$\$ LANGUAGE plpgsql;" 2>/dev/null || log_warn "Failed to create slow write function"

    # Attach trigger to offset_tracker table (assuming it exists)
    docker exec "$POSTGRES_CONTAINER" \
      psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
      CREATE TRIGGER slow_write_trigger
      AFTER INSERT OR UPDATE ON offset_tracker
      FOR EACH ROW EXECUTE FUNCTION slow_write_func();" 2>/dev/null || log_warn "Failed to create trigger on offset_tracker"

    log_info "Write latency injected, simulating slow operations for ${DURATION_SECS}s..."
    sleep "$DURATION_SECS"

    # Cleanup: drop trigger and function
    log_info "Removing write latency..."
    docker exec "$POSTGRES_CONTAINER" \
      psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
      DROP TRIGGER IF EXISTS slow_write_trigger ON offset_tracker;" 2>/dev/null || true
    docker exec "$POSTGRES_CONTAINER" \
      psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
      DROP FUNCTION IF EXISTS slow_write_func();" 2>/dev/null || true

    log_success "Slow write latency scenario completed"
    ;;

  connection-drop)
    log_info "Dropping random Postgres connections..."
    
    log_info "Terminating all idle connections..."
    docker exec "$POSTGRES_CONTAINER" \
      psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
      "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
       WHERE pid <> pg_backend_pid() AND state = 'idle';" 2>/dev/null || true
    
    log_info "Connection drop scenario for ${DURATION_SECS}s..."
    sleep "$DURATION_SECS"
    
    log_success "Connection drop scenario completed"
    ;;

  readonly-mode)
    log_info "Setting Postgres to READ ONLY mode..."
    
    docker exec "$POSTGRES_CONTAINER" \
      psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
      "ALTER SYSTEM SET default_transaction_read_only = on;" 2>/dev/null || true
    
    docker exec "$POSTGRES_CONTAINER" \
      psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT pg_reload_conf();" 2>/dev/null || true
    
    log_info "Database in READ ONLY mode for ${DURATION_SECS}s..."
    sleep "$DURATION_SECS"
    
    log_info "Restoring READ WRITE mode..."
    docker exec "$POSTGRES_CONTAINER" \
      psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
      "ALTER SYSTEM SET default_transaction_read_only = off;" 2>/dev/null || true
    
    docker exec "$POSTGRES_CONTAINER" \
      psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT pg_reload_conf();" 2>/dev/null || true
    
    log_success "Read-only scenario completed"
    ;;

  container-pause)
    log_info "Pausing Postgres container $POSTGRES_CONTAINER for ${DURATION_SECS}s..."
    docker pause "$POSTGRES_CONTAINER" || fail "Failed to pause container"
    
    log_info "Container paused. System should handle connection timeouts..."
    sleep "$DURATION_SECS"
    
    log_info "Resuming Postgres container..."
    docker unpause "$POSTGRES_CONTAINER" || fail "Failed to unpause container"
    
    log_info "Waiting for Postgres to be ready..."
    local ready_timeout=60
    local ready_elapsed=0
    while (( ready_elapsed < ready_timeout )); do
      if docker exec "$POSTGRES_CONTAINER" pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; then
        log_info "Postgres is ready after ${ready_elapsed}s"
        break
      fi
      log_debug "Postgres not ready yet, waiting..."
      sleep 2
      ((ready_elapsed += 2))
    done
    
    if (( ready_elapsed >= ready_timeout )); then
      fail "Postgres did not become ready within ${ready_timeout}s after unpause"
    fi
    
    log_success "Container recovery completed"
    ;;

  *)
    echo "Unknown scenario: $SCENARIO"
    echo "Available scenarios: pool-exhaustion, slow-writes, connection-drop, readonly-mode, container-pause"
    exit 1
    ;;
esac

log_header "Postgres chaos test completed"
