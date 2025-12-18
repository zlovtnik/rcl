# rcl — Rust CDC pipeline

Lightweight Rust-based Change Data Capture (CDC) pipeline: Kafka → Debezium decoder → EIP processors → Postgres.

<!-- badges: build / license / crates / docs (add as needed) -->

## Quickstart

Clone and run with the example configuration:

```bash
git clone https://github.com/zlovtnik/rcl.git
cd rcl
RCL_CONFIG_PATH=config/example.json cargo run
```

Development helpers are available via the `Makefile`:

```bash
make dev      # format, lint, check, test
make run      # run the service with env config
make docker-up   # bring up local Kafka/Postgres stack for testing
```

## Overview

`rcl` consumes CDC events from Kafka, decodes Debezium envelopes, executes a configurable EIP-style processing pipeline (filter, transform, split, route), and writes results to Postgres. It provides metrics, health probes, DLQ support, and graceful shutdown handling.

Key features:
- At-least-once semantics: offsets committed after successful DB writes
- Debezium envelope unwrapping and validation
- Configurable EIP pipeline stages (`Filter`, `Transformer`, `Router`, `Splitter`)
- Bulk `COPY` with `INSERT` fallback and exponential backoff retries
- DLQ (dead-letter queue) for permanent failures
- Prometheus metrics and health/readiness probes
- Replay mode and DLQ inspection/repair CLI commands

## Architecture

Core concurrent tasks:

1. Fetch Loop — polls `rdkafka::StreamConsumer`, tracks lag, and pushes messages into a bounded `mpsc` for backpressure.
2. Processing Loop — runs EIP pipeline, writes to Postgres, routes permanent failures to DLQ, commits offsets.
3. Heartbeat Task — monitors consumer staleness and updates component health.
4. Metrics Exporter — HTTP server exposing `/metrics`, `/health`, and `/ready` endpoints.

See `src/` for implementation details.

## Installation

Add `rcl` as a dependency for internal crates or build from source:

```toml
# This crate is primarily an application (not a library) — run via `cargo run`.
```

## Configuration

Configuration is provided via JSON files and environment variables. Important env vars:

- `RCL_CONFIG_PATH` — path to the JSON config file (required)
- `RCL_KAFKA_BROKERS` — Kafka broker list (overrides config)
- `RCL_KAFKA_GROUP_ID` — consumer group id
- `RCL_POSTGRES_URL` — Postgres connection URL

Sample config: see `config/example.json`.

## Developer Workflow

- `make dev` runs formatting, linting, checks and tests.
- `cargo run -- --validate-config` validates your configuration and exits (useful for CI).
- Use `make docker-up` to spin up a local Kafka + Postgres stack for development and testing.

## Testing & Debugging

- Replay a topic range for debugging:

```bash
cargo run -- replay --topic cdc.orders --partition 0 --start-offset 1000 --end-offset 1100
```

- Inspect DLQ messages:

```bash
cargo run -- dlq inspect --topic dlq.orders --limit 20
```

## Security & Best Practices

- Validate table identifiers before constructing dynamic SQL to avoid SQL injection.
- Use environment variables for sensitive credentials.
- Protect metrics endpoints in production.

## Contributing

Contributions are welcome. Please open issues or PRs against this repository. Follow the code style in `src/` and run `make dev` before submitting PRs.

## License

This repository does not include a LICENSE file by default — add one appropriate for your project.

## See Also

- `siwe-rs` README inspired the structure here: https://github.com/spruceid/siwe-rs
