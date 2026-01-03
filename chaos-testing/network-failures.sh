#!/bin/bash
# Network Chaos Testing Script
# Simulates network partitions, latency, packet loss, etc.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

source "$SCRIPT_DIR/utils.sh"

# Configuration
SERVICE_CONTAINER="${SERVICE_CONTAINER:-rcl-service}"
KAFKA_HOST="${KAFKA_HOST:-kafka-1}"
POSTGRES_HOST="${POSTGRES_HOST:-postgres-1}"
DURATION_SECS="${CHAOS_DURATION:-30}"
SCENARIO="${1:-latency}"

log_header "Network Chaos Testing: $SCENARIO"

# Note: For real network chaos, use toxiproxy or install tc (traffic control) in containers

case "$SCENARIO" in
  latency)
    log_info "Injecting network latency (250ms) for ${DURATION_SECS}s..."
    
    # Using tc (traffic control) requires privileged mode
    # This assumes containers are running with --privileged
    for container in "$KAFKA_HOST" "$POSTGRES_HOST"; do
      log_info "Adding latency to $container..."
      docker exec "$container" bash -c \
        'tc qdisc add dev eth0 root netem delay 250ms 2>/dev/null || \
         tc qdisc replace dev eth0 root netem delay 250ms' 2>/dev/null || \
        log_warn "Failed to add latency to $container (requires --privileged)"
    done
    
    log_info "Latency active for ${DURATION_SECS}s..."
    sleep "$DURATION_SECS"
    
    log_info "Removing latency..."
    for container in "$KAFKA_HOST" "$POSTGRES_HOST"; do
      docker exec "$container" bash -c 'tc qdisc del dev eth0 root' 2>/dev/null || true
    done
    
    log_success "Latency injection completed"
    ;;

  packet-loss)
    log_info "Injecting packet loss (5%) for ${DURATION_SECS}s..."
    
    for container in "$KAFKA_HOST" "$POSTGRES_HOST"; do
      log_info "Adding packet loss to $container..."
      docker exec "$container" bash -c \
        'tc qdisc add dev eth0 root netem loss 5% 2>/dev/null || \
         tc qdisc replace dev eth0 root netem loss 5%' 2>/dev/null || \
        log_warn "Failed to add packet loss to $container"
    done
    
    log_info "Packet loss active for ${DURATION_SECS}s..."
    sleep "$DURATION_SECS"
    
    log_info "Removing packet loss..."
    for container in "$KAFKA_HOST" "$POSTGRES_HOST"; do
      docker exec "$container" bash -c 'tc qdisc del dev eth0 root' 2>/dev/null || true
    done
    
    log_success "Packet loss injection completed"
    ;;

  jitter)
    log_info "Injecting network jitter (50ms) for ${DURATION_SECS}s..."
    
    for container in "$KAFKA_HOST" "$POSTGRES_HOST"; do
      log_info "Adding jitter to $container..."
      docker exec "$container" bash -c \
        'tc qdisc add dev eth0 root netem delay 100ms 50ms 2>/dev/null || \
         tc qdisc replace dev eth0 root netem delay 100ms 50ms' 2>/dev/null || \
        log_warn "Failed to add jitter to $container"
    done
    
    log_info "Jitter active for ${DURATION_SECS}s..."
    sleep "$DURATION_SECS"
    
    log_info "Removing jitter..."
    for container in "$KAFKA_HOST" "$POSTGRES_HOST"; do
      docker exec "$container" bash -c 'tc qdisc del dev eth0 root' 2>/dev/null || true
    done
    
    log_success "Jitter injection completed"
    ;;

  partition)
    log_info "Creating network partition by disconnecting Kafka from network for ${DURATION_SECS}s..."
    
    # Get the network name
    NETWORK=$(docker inspect "$KAFKA_HOST" -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}' | head -n1)
    
    # Disconnect Kafka from network
    docker network disconnect "$NETWORK" "$KAFKA_HOST" 2>/dev/null || \
      log_warn "Failed to disconnect $KAFKA_HOST from network"
    
    log_info "Partition active for ${DURATION_SECS}s..."
    sleep "$DURATION_SECS"
    
    log_info "Healing partition..."
    docker network connect "$NETWORK" "$KAFKA_HOST" 2>/dev/null || true
    
    log_success "Network partition healed"
    ;;

  bandwidth-limit)
    log_info "Limiting bandwidth to 1Mbps for ${DURATION_SECS}s..."
    
    for container in "$KAFKA_HOST" "$POSTGRES_HOST"; do
      log_info "Limiting bandwidth for $container..."
      docker exec "$container" bash -c \
        'tc qdisc add dev eth0 root tbf rate 1mbit burst 32kbit latency 400ms 2>/dev/null || \
         tc qdisc replace dev eth0 root tbf rate 1mbit burst 32kbit latency 400ms' 2>/dev/null || \
        log_warn "Failed to limit bandwidth for $container"
    done
    
    log_info "Bandwidth limit active for ${DURATION_SECS}s..."
    sleep "$DURATION_SECS"
    
    log_info "Removing bandwidth limit..."
    for container in "$KAFKA_HOST" "$POSTGRES_HOST"; do
      docker exec "$container" bash -c 'tc qdisc del dev eth0 root' 2>/dev/null || true
    done
    
    log_success "Bandwidth limit injection completed"
    ;;

  *)
    echo "Unknown scenario: $SCENARIO"
    echo "Available scenarios: latency, packet-loss, jitter, partition, bandwidth-limit"
    fail "Invalid scenario specified"
    ;;
esac

log_header "Network chaos test completed"
