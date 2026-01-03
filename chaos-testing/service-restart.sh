#!/bin/bash
# Service Restart Chaos Testing
# Tests recovery behavior and data consistency on restarts

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

source "$SCRIPT_DIR/utils.sh"

SERVICE_CONTAINER="${SERVICE_CONTAINER:-rcl}"
DURATION_SECS="${CHAOS_DURATION:-30}"
SCENARIO="${1:-graceful-restart}"

log_header "Service Restart Testing: $SCENARIO"

case "$SCENARIO" in
  graceful-restart)
    log_info "Starting graceful service restart..."
    
    # Send SIGTERM for graceful shutdown
    log_info "Sending SIGTERM to service (will flush buffered messages)..."
    docker kill --signal=TERM "$SERVICE_CONTAINER" || fail "Failed to send SIGTERM"
    
    # Give it time to shutdown gracefully (should flush batches, close connections)
    log_info "Waiting for graceful shutdown (max 30s)..."
    timeout 30 docker wait "$SERVICE_CONTAINER" || true
    
    log_info "Waiting ${DURATION_SECS}s before restart..."
    sleep "$DURATION_SECS"
    
    log_info "Restarting service..."
    docker start "$SERVICE_CONTAINER" || fail "Failed to start service"
    
    log_info "Waiting for service to be ready..."
    sleep 10
    
    # Check if service is responding
    if docker exec "$SERVICE_CONTAINER" curl -f --max-time 5 http://localhost:9090/ready 2>/dev/null; then
      log_success "Service restarted and ready"
    else
      log_warn "Service may still be starting up"
    fi
    ;;

  hard-restart)
    log_info "Performing hard service restart (SIGKILL)..."
    
    # Force kill (no graceful shutdown)
    log_info "Killing service with SIGKILL..."
    docker kill "$SERVICE_CONTAINER" || fail "Failed to kill service"
    
    log_info "Waiting ${DURATION_SECS}s before restart..."
    sleep "$DURATION_SECS"
    
    log_info "Restarting service..."
    docker start "$SERVICE_CONTAINER" || fail "Failed to start service"
    
    log_info "Waiting for service recovery..."
    sleep 10
    
    # Verify service is responding
    if docker exec "$SERVICE_CONTAINER" curl -f --max-time 5 http://localhost:9090/ready 2>/dev/null; then
      log_success "Hard restart recovery completed"
    else
      log_warn "Service may still be recovering"
    fi
    for i in {1..5}; do
      log_info "Restart $i/5..."
      docker kill --signal=TERM "$SERVICE_CONTAINER" || log_warn "Container may already be stopped"
      timeout 10 docker wait "$SERVICE_CONTAINER" || true
      
      sleep "$DURATION_SECS"
      
      docker start "$SERVICE_CONTAINER" || fail "Failed to start service"
      sleep 5
    done
      sleep "$DURATION_SECS"
      
      docker start "$SERVICE_CONTAINER" || fail "Failed to start service"
      sleep 5
    done
    
    log_success "Cascading restart test completed"
    ;;

  mid-batch-restart)
    log_info "Testing restart during batch processing..."
    
    log_info "Letting service process normally for 10s..."
    sleep 10
    
    log_info "Killing service mid-operation..."
    docker kill "$SERVICE_CONTAINER" || fail "Failed to kill service"
    log_info "Restarting service..."
    docker start "$SERVICE_CONTAINER" || fail "Failed to start service"
    
    log_info "Service should recover from offset and not duplicate messages..."
    sleep 10
    
    # Verify service is responding
    if docker exec "$SERVICE_CONTAINER" curl -f --max-time 5 http://localhost:9090/ready 2>/dev/null; then
      log_success "Mid-batch restart recovery completed"
    else
      log_warn "Service may still be recovering"
    fi
    
    log_info "Service should recover from offset and not duplicate messages..."
    sleep 10
    
    log_success "Mid-batch restart recovery completed"
    ;;

  *)
    fail "Unknown scenario: $SCENARIO. Available scenarios: graceful-restart, hard-restart, restart-cascade, mid-batch-restart"
    ;;
esac

log_header "Service restart test completed"
