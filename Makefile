CARGO ?= cargo
.DEFAULT_GOAL := dev

CHAOS_TARGETS := chaos-all chaos-kafka-kill chaos-kafka-restart chaos-postgres-pool chaos-postgres-slow chaos-network-latency chaos-network-loss chaos-service-graceful chaos-service-hard

.PHONY: fmt lint test check run dev docker-up docker-down clean help $(CHAOS_TARGETS) validate-consistency

help:
	@echo "Available targets:"
	@echo "  fmt          Format the codebase with cargo fmt"
	@echo "  lint         Run clippy with warnings as errors"
	@echo "  check        Run cargo check"
	@echo "  test         Run the test suite"
	@echo "  run          Run the binary"
	@echo "  dev          Run fmt, lint, check, and test"
	@echo "  docker-up    Start docker-compose stack in detached mode"
	@echo "  docker-down  Stop docker-compose stack"
	@echo "  clean        Remove build artifacts"
	@echo ""
	@echo "Chaos Testing targets:"
	@echo "  chaos-all              Run all chaos test scenarios (30s each)"
	@echo "  chaos-kafka-kill       Kill Kafka broker mid-consume"
	@echo "  chaos-kafka-restart    Kill and restart Kafka broker"
	@echo "  chaos-postgres-pool    Exhaust Postgres connection pool"
	@echo "  chaos-postgres-slow    Inject latency into Postgres writes"
	@echo "  chaos-network-latency  Add 250ms network latency"
	@echo "  chaos-network-loss     Simulate 5% packet loss"
	@echo "  chaos-service-graceful Graceful service restart (SIGTERM)"
	@echo "  chaos-service-hard     Hard service restart (SIGKILL)"
	@echo "  validate-consistency   Run data consistency checks after chaos"
	@echo ""
	@echo "Environment variables:"
	@echo "  CHAOS_DURATION=N       Duration of chaos injection in seconds (default: 30)"
	@echo "  CHAOS_LOAD=true/false  Run with parallel load generator (default: true)"

fmt:
	$(CARGO) fmt

lint:
	$(CARGO) clippy -- -D warnings

check:
	$(CARGO) check

test:
	$(CARGO) test

run:
	$(CARGO) run

dev: fmt lint check test

docker-up:
	docker compose up -d

docker-down:
	docker compose down

clean:
	$(CARGO) clean

# Chaos Testing targets
chaos-all:
	@echo "Running all chaos scenarios..."
	CHAOS_DURATION=30 CHAOS_LOAD=true ./chaos-testing/run-all.sh

chaos-kafka-kill:
	@echo "Injecting Kafka broker kill failure..."
	CHAOS_DURATION=30 ./chaos-testing/kafka-failures.sh broker-kill

chaos-kafka-restart:
	@echo "Injecting Kafka broker restart..."
	CHAOS_DURATION=30 ./chaos-testing/kafka-failures.sh broker-restart

chaos-postgres-pool:
	@echo "Exhausting Postgres connection pool..."
	CHAOS_DURATION=30 ./chaos-testing/postgres-failures.sh pool-exhaustion

chaos-postgres-slow:
	@echo "Injecting Postgres write latency..."
	CHAOS_DURATION=30 ./chaos-testing/postgres-failures.sh slow-writes

chaos-network-latency:
	@echo "Injecting network latency..."
	CHAOS_DURATION=30 ./chaos-testing/network-failures.sh latency

chaos-network-loss:
	@echo "Injecting packet loss..."
	CHAOS_DURATION=30 ./chaos-testing/network-failures.sh packet-loss

chaos-service-graceful:
	@echo "Testing graceful service restart..."
	CHAOS_DURATION=30 ./chaos-testing/service-restart.sh graceful-restart

chaos-service-hard:
	@echo "Testing hard service restart..."
	CHAOS_DURATION=30 ./chaos-testing/service-restart.sh hard-restart

validate-consistency:
	@echo "Running data consistency validation..."
	./chaos-testing/validate-consistency.sh all
