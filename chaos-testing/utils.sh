#!/bin/bash
# Utility functions for chaos testing

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_header() {
  echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
  echo -e "${BLUE}  $1${NC}"
  echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
}

log_info() {
  echo -e "${BLUE}ℹ${NC} $1"
}

log_success() {
  echo -e "${GREEN}✓${NC} $1"
}

log_warn() {
  echo -e "${YELLOW}⚠${NC} $1"
}

fail() {
  echo -e "${RED}✗${NC} $1"
  exit 1
}

# Check if Docker container is running
is_container_running() {
  local container=$1
  docker inspect "$container" --format='{{.State.Running}}' 2>/dev/null | grep -q "true"
}

# Wait for container to be ready
wait_for_container() {
  local container=$1
  local timeout=${2:-60}
  local elapsed=0
  
  while (( elapsed < timeout )); do
    if is_container_running "$container"; then
      return 0
    fi
    sleep 1
    ((elapsed++))
  done
  
  return 1
}

# Get container IP
get_container_ip() {
  local container=$1
  docker inspect "$container" --format='{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}'
}

# Check service readiness via HTTP
check_service_ready() {
  local host=$1
  local port=${2:-9090}
  local path=${3:-/ready}
  
  curl -sf --max-time 10 "http://$host:$port$path" &>/dev/null && return 0 || return 1
}

# Capture baseline metrics from Prometheus
capture_baseline_metrics() {
  local prometheus_url=${1:-http://localhost:9090}
  
  log_info "Capturing baseline metrics from $prometheus_url"
  
  # Get baseline message count
  curl -s "$prometheus_url/api/v1/query?query=messages_total" | \
    python3 -m json.tool > /tmp/baseline_messages.json
  
  # Get baseline error count
  curl -s "$prometheus_url/api/v1/query?query=processing_failures" | \
    python3 -m json.tool > /tmp/baseline_errors.json
  
  log_success "Baseline metrics captured"
}

# Capture final metrics and calculate deltas
capture_final_metrics() {
  local prometheus_url=${1:-http://localhost:9090}
  
  log_info "Capturing final metrics from $prometheus_url"
  
  # Get final message count
  curl -s "$prometheus_url/api/v1/query?query=messages_total" | \
    python3 -m json.tool > /tmp/final_messages.json
  
  # Get final error count
  curl -s "$prometheus_url/api/v1/query?query=processing_failures" | \
    python3 -m json.tool > /tmp/final_errors.json
  
  log_success "Final metrics captured"
}

# Check for data loss
check_data_loss() {
  local baseline_file=$1
  local final_file=$2

  # Check if both files exist and are readable
  if [ ! -f "$baseline_file" ] || [ ! -r "$baseline_file" ]; then
    log_error "Baseline file '$baseline_file' does not exist or is not readable"
    return 1
  fi
  if [ ! -f "$final_file" ] || [ ! -r "$final_file" ]; then
    log_error "Final file '$final_file' does not exist or is not readable"
    return 1
  fi

  # Extract message counts with error handling
  local baseline_raw=$(grep -o '"value":"\?[0-9.]*' "$baseline_file" 2>/dev/null | tail -1)
  local final_raw=$(grep -o '"value":"\?[0-9.]*' "$final_file" 2>/dev/null | tail -1)

  if [ -z "$baseline_raw" ]; then
    log_warn "Could not find metric value in baseline file '$baseline_file'"
    return 1
  fi
  if [ -z "$final_raw" ]; then
    log_warn "Could not find metric value in final file '$final_file'"
    return 1
  fi

  local baseline=$(echo "$baseline_raw" | grep -o '[0-9.]*')
  local final=$(echo "$final_raw" | grep -o '[0-9.]*')

  if [ -z "$baseline" ] || [ -z "$final" ]; then
    log_warn "Failed to extract numeric values from metrics: baseline='$baseline', final='$final'"
    return 1
  fi

  # Determine if values are integers or decimals
  if [[ "$baseline" == *.* ]] || [[ "$final" == *.* ]]; then
    # Use bc for decimal comparison
    if command -v bc >/dev/null 2>&1; then
      if (( $(echo "$final < $baseline" | bc -l) )); then
        log_warn "Possible data loss detected! Final ($final) < Baseline ($baseline)"
        return 1
      fi
    else
      log_error "bc command not available for decimal comparison"
      return 1
    fi
  else
    # Use bash integer arithmetic
    if (( final < baseline )); then
      log_warn "Possible data loss detected! Final ($final) < Baseline ($baseline)"
      return 1
    fi
  fi

  return 0
}

# Check for duplicates (look for duplicate offsets in logs)
check_duplicates() {
  local log_file=${1:-/tmp/service.log}
  
  # Extract offsets and count duplicates
  if [ -f "$log_file" ]; then
    local duplicates=$(grep -o 'offset=[0-9]*' "$log_file" | sort | uniq -d | wc -l)
    if [ "$duplicates" -gt 0 ]; then
      log_warn "Found $duplicates duplicate offsets in logs"
      return 1
    fi
  fi
  
  return 0
}

# Monitor metrics during chaos period
monitor_metrics() {
  local duration=$1
  local prometheus_url=${2:-http://localhost:9090}
  local interval=5
  local elapsed=0
  
  log_info "Monitoring metrics for ${duration}s (interval: ${interval}s)"
  
  while [ $elapsed -lt $duration ]; do
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    # Get current lag
    local lag=$(curl -s --max-time 5 "$prometheus_url/api/v1/query?query=lag_ms" | \
      grep -o '"value":"\?[0-9.]*' | tail -1 | grep -o '[0-9.]*' || echo "N/A")
    
    # Get current throughput
    local throughput=$(curl -s --max-time 5 "$prometheus_url/api/v1/query?query=rate(messages_total[1m])" | \
      grep -o '"value":"\?[0-9.]*' | tail -1 | grep -o '[0-9.]*' || echo "N/A")
    
    # Get error rate
    local error_rate=$(curl -s --max-time 5 "$prometheus_url/api/v1/query?query=rate(processing_failures[1m])" | \
      grep -o '"value":"\?[0-9.]*' | tail -1 | grep -o '[0-9.]*' || echo "N/A")
    
    echo "[$timestamp] Lag: ${lag}ms | Throughput: ${throughput} msg/s | Errors: ${error_rate} /s"
    
    sleep "$interval"
    ((elapsed+=$interval))
  done
  
  log_success "Monitoring period completed"
}

# Generate chaos test report
generate_report() {
  local scenario=$1
  local passed=$2
  local duration=$3
  local recovery_time=$4
  
  local report_file="/tmp/chaos-test-${scenario}-$(date +%s).txt"
  
  cat > "$report_file" << EOF
Chaos Test Report
=================
Scenario: $scenario
Timestamp: $(date)
Duration: ${duration}s
Recovery Time: ${recovery_time}s
Result: $([ "$passed" = "true" ] && echo "PASSED" || echo "FAILED")

Metrics:
- Baseline messages: $(grep -o '"value":"\?[0-9.]*' /tmp/baseline_messages.json | tail -1)
- Final messages: $(grep -o '"value":"\?[0-9.]*' /tmp/final_messages.json | tail -1)
- Baseline errors: $(grep -o '"value":"\?[0-9.]*' /tmp/baseline_errors.json | tail -1)
- Final errors: $(grep -o '"value":"\?[0-9.]*' /tmp/final_errors.json | tail -1)

Notes:
- See logs at /tmp/service.log
- See metrics dumps at /tmp/*_messages.json and /tmp/*_errors.json
EOF
  
  log_success "Report generated: $report_file"
  cat "$report_file"
}

# Export functions
export -f log_header log_info log_success log_warn fail
export -f is_container_running wait_for_container get_container_ip
export -f check_service_ready capture_baseline_metrics capture_final_metrics
export -f check_data_loss check_duplicates monitor_metrics generate_report
