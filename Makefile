CARGO ?= cargo
.DEFAULT_GOAL := dev

.PHONY: fmt lint test check run dev docker-up docker-down clean help

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
