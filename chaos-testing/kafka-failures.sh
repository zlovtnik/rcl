#!/bin/bash
# Kafka Chaos Testing Script
# Simulates Kafka broker failures and recovery

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
DOCKER_STACK_DIR="$ROOT_DIR/docker-middleware-stack"

source "$SCRIPT_DIR/utils.sh"

# Configuration
BROKER_CONTAINER="${KAFKA_CONTAINER:-kafka-1}"
DURATION_SECS="${CHAOS_DURATION:-30}"
SCENARIO="${1:-broker-kill}"

log_header "Kafka Chaos Testing: $SCENARIO"

case "$SCENARIO" in
  broker-kill)
    log_info "Killing Kafka broker $BROKER_CONTAINER for ${DURATION_SECS}s..."
    docker pause "$BROKER_CONTAINER" || fail "Failed to pause broker"
    
    log_info "Broker paused. Monitoring system for $DURATION_SECS seconds..."
    sleep "$DURATION_SECS"
    
    log_info "Resuming broker $BROKER_CONTAINER..."
    docker unpause "$BROKER_CONTAINER" || fail "Failed to unpause broker"
    
    log_info "Waiting for broker to rejoin cluster (30s)..."
    sleep 30
    
    log_success "Broker recovery completed"
    ;;

  broker-restart)
    log_info "Restarting Kafka broker $BROKER_CONTAINER..."
    docker stop "$BROKER_CONTAINER" || fail "Failed to stop broker"
    
    log_info "Broker stopped. Waiting ${DURATION_SECS}s before restart..."
    sleep "$DURATION_SECS"
    
    log_info "Starting broker $BROKER_CONTAINER..."
    docker start "$BROKER_CONTAINER" || fail "Failed to start broker"
    
    log_info "Waiting for broker to be ready (30s)..."
    sleep 30
    
    # Check broker health
    if docker exec "$BROKER_CONTAINER" kafka-broker-api-versions.sh 2>/dev/null | grep -q "ApiVersion"; then
      log_success "Broker restart completed and healthy"
    else
      log_warn "Broker may not be fully ready yet"
    fi
    ;;

  broker-network-partition)
    log_info "Creating network partition for broker $BROKER_CONTAINER..."
    # Block Kafka traffic on broker ports (9092 external, 9093 inter-broker)
    docker exec "$BROKER_CONTAINER" iptables -I INPUT -p tcp --dport 9092 -j DROP 2>/dev/null || \
      log_warn "iptables not available in container"
    docker exec "$BROKER_CONTAINER" iptables -I INPUT -p tcp --dport 9093 -j DROP 2>/dev/null || \
      log_warn "iptables not available in container"
    
    log_info "Network partitioned for ${DURATION_SECS}s..."
    sleep "$DURATION_SECS"
    
    log_info "Healing network partition..."
    docker exec "$BROKER_CONTAINER" iptables -D INPUT -p tcp --dport 9092 -j DROP 2>/dev/null || true
    docker exec "$BROKER_CONTAINER" iptables -D INPUT -p tcp --dport 9093 -j DROP 2>/dev/null || true
    
    log_success "Network partition healed"
    ;;

  rebalance)
    log_info "Triggering broker rebalance..."
    # Send SIGTERM to one broker to trigger rebalance
    docker exec "$BROKER_CONTAINER" bash -c 'kill -TERM 1' || true
    
    log_info "Rebalance in progress, waiting ${DURATION_SECS}s..."
    sleep "$DURATION_SECS"
    
    log_info "Rebalance should complete..."
    sleep 30
    
    log_success "Rebalance scenario completed"
    ;;

  *)
    echo "Available scenarios: broker-kill, broker-restart, broker-network-partition, rebalance"
    fail "Unknown scenario: $SCENARIO"
    ;;
esac

log_header "Kafka chaos test completed"
