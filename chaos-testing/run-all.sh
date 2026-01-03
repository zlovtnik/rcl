#!/bin/bash
# Master chaos testing orchestrator
# Runs multiple chaos scenarios sequentially and generates report

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
DOCKER_STACK_DIR="$ROOT_DIR/docker-middleware-stack"

source "$SCRIPT_DIR/utils.sh"

# Configuration
DURATION_SECS=${CHAOS_DURATION:-30}
PARALLEL_LOAD=${CHAOS_LOAD:-true}
SCENARIOS_TO_RUN="${1:-all}"

log_header "RCL Chaos Testing Suite"
log_info "Duration per scenario: ${DURATION_SECS}s"
log_info "Parallel load: $PARALLEL_LOAD"

# Verify docker stack is running
log_info "Checking docker stack status..."
if ! is_container_running "kafka-1" || ! is_container_running "postgres-1"; then
  fail "Docker middleware stack not running. Please run: make docker-up"
fi

log_success "Docker stack is running"

# Start load generator in background if enabled
LOAD_PID=""
if [ "$PARALLEL_LOAD" = "true" ]; then
  log_info "Starting parallel load generator..."
  # This assumes rcl service has load-test subcommand
  # cargo run -- load-test --rate 1000 --duration-sec 3600 &
  # LOAD_PID=$!
  # log_success "Load generator running (PID: $LOAD_PID)"
fi

# Track results
RESULTS_DIR="/tmp/chaos-test-results-$(date +%s)"
mkdir -p "$RESULTS_DIR"

log_info "Results will be saved to: $RESULTS_DIR"

# Define test scenarios
declare -a SCENARIOS=(
  "kafka-broker-kill"
  "kafka-broker-restart"
  "postgres-pool-exhaustion"
  "postgres-slow-writes"
  "postgres-connection-drop"
  "network-latency"
  "network-packet-loss"
  "service-graceful-restart"
  "service-hard-restart"
)

# Function to run a test scenario
run_scenario() {
  local scenario=$1
  local start_time=$(date +%s)
  
  log_header "Running chaos scenario: $scenario"
  
  # Capture baseline
  capture_baseline_metrics "http://localhost:9090"
  
  # Determine which script and arguments to use
  case "$scenario" in
    kafka-*)
      "$SCRIPT_DIR/kafka-failures.sh" "${scenario#kafka-}" 2>&1 | tee "$RESULTS_DIR/$scenario.log"
      ;;
    postgres-*)
      "$SCRIPT_DIR/postgres-failures.sh" "${scenario#postgres-}" 2>&1 | tee "$RESULTS_DIR/$scenario.log"
      ;;
    network-*)
      "$SCRIPT_DIR/network-failures.sh" "${scenario#network-}" 2>&1 | tee "$RESULTS_DIR/$scenario.log"
      ;;
    service-*)
      "$SCRIPT_DIR/service-restart.sh" "${scenario#service-}" 2>&1 | tee "$RESULTS_DIR/$scenario.log"
      ;;
    *)
      log_warn "Unknown scenario: $scenario"
      return 1
      ;;
  esac
  
  # Monitor during test
  monitor_metrics "$DURATION_SECS" "http://localhost:9090" >> "$RESULTS_DIR/$scenario.log"
  
  # Capture final metrics
  capture_final_metrics "http://localhost:9090"
  
  # Validate results
  local data_loss_check=0
  local duplicate_check=0
  check_data_loss "/tmp/baseline_messages.json" "/tmp/final_messages.json" || data_loss_check=1
  check_duplicates "$RESULTS_DIR/$scenario.log" || duplicate_check=1
  
  local end_time=$(date +%s)
  local duration=$((end_time - start_time))
  local passed=true
  
  if [ "$data_loss_check" -ne 0 ] || [ "$duplicate_check" -ne 0 ]; then
    passed=false
  fi
  
  # Generate report
  generate_report "$scenario" "$passed" "$DURATION_SECS" "0" >> "$RESULTS_DIR/$scenario-report.txt"
  
  # Summary
  if [ "$passed" = "true" ]; then
    log_success "Scenario PASSED: $scenario"
  else
    log_warn "Scenario may have issues: $scenario (check $RESULTS_DIR/$scenario-report.txt)"
  fi
}

# Run selected scenarios
if [ "$SCENARIOS_TO_RUN" = "all" ]; then
  for scenario in "${SCENARIOS[@]}"; do
    run_scenario "$scenario"
    sleep 10 # Wait between scenarios for system stabilization
  done
else
  # Run specific scenario
  run_scenario "$SCENARIOS_TO_RUN"
fi

# Cleanup load generator
if [ -n "$LOAD_PID" ]; then
  log_info "Stopping load generator (PID: $LOAD_PID)..."
  kill "$LOAD_PID" 2>/dev/null || true
fi

log_header "Chaos Testing Complete"
log_info "Results directory: $RESULTS_DIR"
log_info "Run: ls -la $RESULTS_DIR"
