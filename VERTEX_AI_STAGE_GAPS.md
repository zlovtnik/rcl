# Fraud Detection Service Architecture & Implementation Plan

> **Last Updated:** December 19, 2025  
> **Status:** Separate microservice design (RCL stays unchanged)  
> **Tasks:** 40 actionable items identified  
> **Architecture:** CDC Pipeline (RCL) → Kafka (Confluent) → Fraud Service → Fraud DB

---

## 🎯 Executive Summary

**Revised Architecture:**
- **RCL (Unchanged):** Robust CDC pipeline produces `cdc.transactions`, `cdc.customers` to Confluent Cloud
- **Fraud Service (New):** Separate Rust microservice consumes `fraud.enriched` (Flink SQL enriched), calls Vertex AI, routes by risk
- **Output Topics:** `fraud.scored`, `fraud.alerts`, `dlq.fraud-detection`
- **Storage:** PostgreSQL with `approved_transactions`, `review_queue`, `blocked_transactions` tables

**Benefits:**
- ✅ RCL remains stable (proven CDC logic, minimal changes)
- ✅ Fraud service is independently scalable & restartable
- ✅ Reusable for compliance, anomaly detection, ML scoring workflows
- ✅ Clear separation: data capture vs ML intelligence
- ✅ Easy to test in isolation with mock Kafka/Vertex AI

**Total Implementation Effort:** ~80-100 engineering hours (new project from scratch)

---

## 🏗️ Architecture Diagram

```
┌──────────────────────────────────┐
│  PostgreSQL (Source Systems)     │
│  Orders, Customers, Transactions │
└──────────────┬───────────────────┘
               │
               ↓ (Debezium CDC)
┌──────────────────────────────────┐
│  RCL CDC Pipeline (UNCHANGED)    │
│  • Filter suspicious transactions│
│  • Enrich customer history       │
│  • Transform features            │
│  • Write to staging tables       │
└──────────────┬───────────────────┘
               │
               ↓ (Kafka messages)
┌──────────────────────────────────────────┐
│  Confluent Cloud                         │
│  Topics:                                 │
│  ├─ cdc.transactions (RCL output)       │
│  ├─ fraud.enriched (Flink SQL enriched) │
│  ├─ fraud.scored (fraud service output) │
│  ├─ fraud.alerts (high-risk alerts)     │
│  └─ dlq.fraud-detection (errors)        │
└──────────────┬──────────────────────────┘
               │
               ↓ (fraud.enriched topic)
┌──────────────────────────────────────────┐
│  FRAUD DETECTION SERVICE (NEW PROJECT)   │
│  • Kafka consumer (fraud.enriched)       │
│  • Stage 1: Filter (thresholds)          │
│  • Stage 2: Transformer (features)       │
│  • Stage 3: VertexAiStage (ML scoring)   │
│  • Stage 4: RouterStage (risk routing)   │
│  • PostgreSQL writer (3 tables by risk)  │
│  • Datadog metrics + tracing             │
│  • Circuit breaker for Vertex AI         │
│  • Health checks & graceful shutdown     │
└──────────────┬──────────────────────────┘
               │
               ├─→ fraud.scored (Kafka)
               ├─→ fraud.alerts (Kafka)
               ├─→ dlq.fraud-detection (Kafka)
               │
               ↓ (PostgreSQL writes)
┌──────────────────────────────────────────┐
│  PostgreSQL Fraud Tables                 │
│  ├─ approved_transactions (low risk)     │
│  ├─ review_queue (medium risk)           │
│  └─ blocked_transactions (high risk)     │
└──────────────────────────────────────────┘
               │
               ↓ (Dashboard + Alerts)
┌──────────────────────────────────────────┐
│  Datadog Observability                   │
│  • Real-time metrics dashboard           │
│  • APM distributed tracing               │
│  • Alert monitoring                      │
│  • Error analysis                        │
└──────────────────────────────────────────┘
```

---

## 📊 Component Breakdown

### RCL (CDC Pipeline) - **Minimal Changes Only**

| Task | Scope | Effort |
|------|-------|--------|
| Verify Confluent Cloud SASL/SSL config | Add example config + env var support | 2h |
| Update README for Confluent Cloud | Document Confluent setup | 2h |
| No code changes needed | RCL logic unchanged | 0h |

**Total RCL effort:** ~4 hours

---

### Fraud Detection Service (New Project) - **Full Implementation**

| Category | Tasks | Hours | Status |
|----------|-------|-------|--------|
| **Project Setup** | 1-4, 39-40 | 12h | Not started |
| **Core Stages** | 5-7, 21-25 | 18h | Not started |
| **Data Layer** | 9-10, 14, 20 | 12h | Not started |
| **Observability** | 7-8, 26-27, 34 | 16h | Not started |
| **Configuration** | 10-11, 16-19 | 10h | Not started |
| **Resilience** | 12-13, 15, 24, 37 | 14h | Not started |
| **Testing** | 28-30, 31 | 12h | Not started |
| **Documentation** | 32-35 | 10h | Not started |

**Total effort:** ~94 hours (new project from scratch)


### 1. **Fraud Detection Service Setup** (4 tasks)

| Task | Description | Dependencies |
|------|-------------|--------------|
| New Rust project | `cargo new fraud-detection-service` | None |
| Dependencies | Add: google-cloud, reqwest, statsd, sqlx, rdkafka | Cargo.toml |
| Main.rs skeleton | Config loader, Kafka consumer, stage pipeline, metrics | Config schema |
| GitHub project | New repo structure (src/, config/, sql/, tests/, docker/) | None |

---

### 2. **Kafka Consumer Integration** (1 task)

| Task | Consumes | Produces | Auth |
|------|----------|----------|------|
| Kafka consumer | `fraud.enriched` from Confluent | Messages to processing pipeline | Confluent SASL/SSL |

**Features:**
- Consumer lag tracking
- Partition assignment & rebalancing
- Offset management (resume from last offset)
- Metrics export to Datadog

---

### 3. **Fraud Scoring Pipeline** (5 tasks)

| Stage | Input | Processing | Output |
|-------|-------|-----------|--------|
| **FilterStage** | fraud.enriched message | Apply thresholds (amount, velocity) | Continue or Skip |
| **TransformerStage** | Filtered message | Extract features, type conversion | Transformed fields |
| **VertexAiStage** | Enriched features | Call Vertex AI endpoint | fraud_score (0.0-1.0) |
| **RouterStage** | Scored message | Route by risk category | Table destination + _meta_table |
| **PostgresWriter** | Routed messages | COPY bulk insert or INSERT row | Tables: approved/review/blocked |

---

### 4. **Risk-Based Routing** (1 task)

```
fraud_score [0.0-1.0]
   ↓
low_risk [0.0-0.5]     → approved_transactions (auto-approve, low risk)
medium_risk [0.5-0.8]  → review_queue (manual review, medium risk)
high_risk [0.8-1.0]    → blocked_transactions (auto-block, high risk)
   ↓
(inject _meta_table field for destination table)
```

---

### 5. **Datadog Integration** (3 tasks)

| Component | Integration | Metrics |
|-----------|-------------|---------|
| **Metrics Exporter** | statsd client to localhost:8125 | transactions_processed, fraud_detected, vertex_ai_latency, error_rate |
| **APM Tracing** | OpenTelemetry OTLP to Datadog | Distributed traces, span context, correlation IDs |
| **Dashboards & Alerts** | JSON specs for Datadog API | Dashboard visualization, critical/warning alerts |

---

### 6. **Confluent Cloud Configuration** (1 task - RCL only)

Add to RCL:
```json
{
  "kafka": {
    "brokers": "${CONFLUENT_BOOTSTRAP_SERVERS}",
    "security": {
      "sasl_enabled": true,
      "sasl_mechanism": "PLAIN",
      "sasl_username": "${CONFLUENT_API_KEY}",
      "sasl_password": "${CONFLUENT_API_SECRET}",
      "tls": true
    }
  }
}
```

---

### 7. **PostgreSQL Schema for Fraud Service** (1 task)

```sql
-- Table 1: Approved Transactions (low risk, auto-approve)
CREATE TABLE approved_transactions (
  id UUID PRIMARY KEY,
  transaction_id VARCHAR(50),
  amount DECIMAL(10,2),
  merchant_id VARCHAR(50),
  customer_id VARCHAR(50),
  fraud_score FLOAT,
  risk_category VARCHAR(20),
  detected_at TIMESTAMP,
  model_version VARCHAR(50)
);

-- Table 2: Review Queue (medium risk, manual review)
CREATE TABLE review_queue (
  id UUID PRIMARY KEY,
  -- same columns as approved_transactions
  -- PLUS: reviewer_id, review_status, review_timestamp (nullable)
);

-- Table 3: Blocked Transactions (high risk, auto-block)
CREATE TABLE blocked_transactions (
  id UUID PRIMARY KEY,
  -- same columns as approved_transactions
  -- PLUS: block_reason, appeal_id (nullable)
);
```

---

### 8. **Fraud Service Configuration** (2 tasks)

**config/confluent_cloud.json:**
```json
{
  "service": {
    "log_level": "Info",
    "metrics_port": 9091,
    "health_check_timeout_ms": 5000,
    "shutdown_timeout": "30s"
  },
  "kafka": {
    "brokers": "${CONFLUENT_BOOTSTRAP_SERVERS}",
    "group_id": "fraud-detection-service",
    "security": {
      "sasl_enabled": true,
      "sasl_username": "${CONFLUENT_API_KEY}",
      "sasl_password": "${CONFLUENT_API_SECRET}",
      "tls": true
    },
    "topics": {
      "source": "fraud.enriched",
      "output": "fraud.scored",
      "alerts": "fraud.alerts",
      "dlq": "dlq.fraud-detection"
    }
  },
  "vertex_ai": {
    "endpoint_url": "${VERTEX_AI_ENDPOINT}",
    "project_id": "${GCP_PROJECT_ID}",
    "location": "us-central1",
    "request_timeout_ms": 50
  },
  "postgres": {
    "url": "${POSTGRES_URL}",
    "pool": { "max_connections": 20, "acquire_timeout_ms": 5000 }
  },
  "datadog": {
    "enabled": true,
    "agent_host": "${DD_AGENT_HOST}",
    "agent_port": 8125,
    "service_name": "fraud-detection-service",
    "environment": "${DD_ENV}",
    "trace_enabled": true
  }
}
```

---

### 9. **Testing & Verification** (3 tasks)

| Test Type | Coverage | Tools |
|-----------|----------|-------|
| **Unit Tests** | VertexAiStage, RouterStage, feature mapping | Mock endpoints, serde validation |
| **Integration Tests** | End-to-end: Kafka → fraud service → Postgres | testcontainers, Docker Compose |
| **Load Tests** | Latency, throughput, circuit breaker behavior | Custom load generator |

---

### 10. **Deployment & Observability** (4 tasks)

| Deliverable | Audience | Format |
|-------------|----------|--------|
| **GCP Deployment Guide** | DevOps/SRE | Terraform templates, Cloud Run/GKE setup |
| **Confluent Cloud Guide** | Platform engineers | Broker config, topic creation, Schema Registry |
| **Datadog Integration Guide** | Ops/monitoring | Agent setup, APM config, dashboard import |
| **Demo Scenario** | QA/product | End-to-end flow, expected results, troubleshooting |

---

## 🔍 Detailed Implementation Plan

### Phase 1: Fraud Service Foundation (Tasks 1-4)
**Duration:** ~12 hours  
**Deliverable:** New project structure with Kafka consumer + basic config

1. Create GitHub project `fraud-detection-service`
2. Setup Cargo.toml with dependencies
3. Create main.rs skeleton (config loader, graceful shutdown)
4. Implement Kafka consumer (Confluent Cloud compatible)

---

### Phase 2: Fraud Scoring Pipeline (Tasks 5-7, 21-25)
**Duration:** ~18 hours  
**Deliverable:** VertexAI predictions → risk routing → Postgres writes

5. Create VertexAiStage (feature mapping, API client, error handling)
6. Create RouterStage (risk categorization, table routing)
7. Add Datadog metrics exporter (fraud service metrics)
8. Feature mapping & validation
9. Fraud_score injection & risk categorization
10. Vertex AI health checks & timeouts

---

### Phase 3: Data Layer & Resilience (Tasks 9-13, 14-15, 20)
**Duration:** ~12 hours  
**Deliverable:** PostgreSQL writes, circuit breaker, health registry

11. Implement PostgreSQL writer (COPY + INSERT fallback)
12. Add circuit breaker for Vertex AI endpoint
13. Create health registry (Kafka, Vertex AI, PostgreSQL status)
14. Add DLQ producer (error routing)
15. Implement graceful shutdown
16. Create PostgreSQL schema

---

### Phase 4: Configuration & Observability (Tasks 10-11, 26-27, 34)
**Duration:** ~16 hours  
**Deliverable:** Full Datadog visibility + config templates

17. Create config schema + example configs
18. Add environment variable overrides
19. Create Datadog dashboard (JSON spec)
20. Create Datadog alerts (JSON spec)
21. Datadog APM integration (tracing)

---

### Phase 5: Testing & Documentation (Tasks 28-30, 32-35)
**Duration:** ~12 hours  
**Deliverable:** Tested, documented, deployable

22. Unit tests (stages, feature mapping)
23. Integration tests (end-to-end Kafka → Postgres)
24. Load testing (latency verification, throughput)
25. GCP deployment guide (Terraform)
26. Confluent Cloud integration guide
27. Datadog setup guide
28. Demo scenario documentation

---

### Phase 6: Optimization & CI/CD (Tasks 31, 36-40)
**Duration:** ~12 hours (optional)  
**Deliverable:** Docker setup, CI/CD pipeline, caching layer

29. Docker multi-stage build + docker-compose
30. GitHub Actions CI/CD (test, build, push, deploy)
31. Redis caching for Vertex AI predictions (optional)
32. Fallback scoring logic (optional)
33. Update RCL README for Confluent Cloud

---

## 📝 RCL Enhancement (Minimal)

**What stays unchanged:**
- CDC pipeline logic
- Stage implementations
- Error handling patterns
- Health registry

**What gets added to RCL:**
1. **config/confluent_cloud_example.json** - Confluent Cloud broker setup
2. **README updates** - Mention fraud-detection-service integration
3. **Optional: Schema Registry config** - For message validation (nice-to-have)

**Effort:** ~4 hours total



## ✅ Checklist for Implementation Start

### Prerequisites
- [ ] Fraud-detection-service GitHub project created
- [ ] GCP project with Vertex AI API enabled
- [ ] Vertex AI model trained/deployed to endpoint
- [ ] Confluent Cloud cluster provisioned & topics created
- [ ] Datadog account with APM enabled
- [ ] PostgreSQL instance accessible
- [ ] Team reviewed architecture diagram

### Phase 1 Start (Foundation)
- [ ] Clone fraud-detection-service repo
- [ ] `cargo new fraud-detection-service`
- [ ] Add Cargo.toml dependencies
- [ ] Create main.rs skeleton with config loader
- [ ] Setup Kafka consumer (fraud.enriched topic)
- [ ] Configure SASL/SSL for Confluent Cloud

---

## 📊 Task Summary by Project

### RCL Project Changes (4 tasks, ~4 hours)
```
✅ Task 18: Add config/confluent_cloud_example.json
✅ Task 38: Update RCL README for Confluent Cloud
✅ (Optional) Schema Registry integration
✅ (No code changes needed)
```

### New Fraud Service (40 tasks, ~94 hours)
```
Phase 1 (4 tasks, 12h):
  ✅ Task 1: New project setup
  ✅ Task 2: Cargo.toml dependencies
  ✅ Task 3: Main.rs skeleton
  ✅ Task 4: Kafka consumer

Phase 2 (7 tasks, 18h):
  ✅ Task 5: VertexAiStage implementation
  ✅ Task 6: RouterStage (fraud routing)
  ✅ Task 7: Datadog metrics exporter
  ✅ Task 21-25: Feature mapping, validation, health checks

Phase 3 (6 tasks, 12h):
  ✅ Task 9: PostgreSQL writer
  ✅ Task 12-13: Circuit breaker & health registry
  ✅ Task 14-15: DLQ & graceful shutdown
  ✅ Task 20: PostgreSQL schema

Phase 4 (5 tasks, 16h):
  ✅ Task 8: Datadog APM tracing
  ✅ Task 10-11: Config schema + env vars
  ✅ Task 26-27: Datadog dashboard & alerts
  ✅ Task 34: Datadog integration guide

Phase 5 (4 tasks, 12h):
  ✅ Task 28-30: Unit, integration, load tests
  ✅ Task 31: Docker setup

Phase 6 (4 tasks, 12h):
  ✅ Task 32-35: Deployment & demo guides
  ✅ Task 39-40: CI/CD pipeline setup
  ✅ Task 36-37: Caching & fallback (optional)
```

---

## 🎯 Key Design Decisions

| Decision | Rationale | Impact |
|----------|-----------|--------|
| **Separate service** | Decouples ML scoring from CDC pipeline | Independent scaling, testing, deployment |
| **Confluent Cloud** | Managed Kafka, Schema Registry, monitoring | No Kafka ops, built-in security, compliance |
| **Vertex AI** | Google Cloud native, AutoML support, explanations | No ML engineering needed, easy to retrain |
| **Datadog** | Full stack observability (logs, metrics, traces) | Single pane of glass for debugging |
| **PostgreSQL** | Risk-based table separation | Easy to implement different policies per table |
| **Graceful shutdown** | Clean Kafka offset commits + Postgres flushes | No data loss on restart |
| **Circuit breaker** | Prevents cascading failures when Vertex AI down | High availability, error containment |
| **DLQ pattern** | Route errors to dead-letter topic for replay | Post-mortem analysis, reprocessing support |

---

## 🚀 Getting Started

**Step 1: Create New Project**
```bash
cd /path/to/projects
git init fraud-detection-service
cargo init fraud-detection-service --name fraud_detection_service
cd fraud-detection-service
```

**Step 2: Add Dependencies**
Edit `Cargo.toml` with:
- `tokio` (async runtime)
- `rdkafka` (Kafka consumer)
- `google-cloud-aiplatform` (Vertex AI client)
- `sqlx` (PostgreSQL)
- `statsd` (Datadog metrics)
- `opentelemetry` (tracing)
- `serde_json` (feature mapping)

**Step 3: Create Config Structure**
```
config/
  ├─ confluent_cloud.json     (Confluent Cloud setup)
  ├─ local_kafka.json          (Local testing)
  └─ example.json              (template)
```

**Step 4: Implement Stages**
```
src/
  ├─ main.rs                   (entry point)
  ├─ config.rs                 (config loader)
  ├─ consumer.rs               (Kafka consumer)
  ├─ stages/
  │  ├─ mod.rs
  │  ├─ vertex_ai.rs           (ML scoring)
  │  ├─ router.rs              (risk routing)
  │  └─ filter.rs              (optional thresholds)
  ├─ writer.rs                 (Postgres writer)
  ├─ metrics.rs                (Datadog metrics)
  ├─ health.rs                 (component status)
  ├─ dlq.rs                    (error routing)
  └─ errors.rs                 (error types)
```

---

## 📞 Open Questions to Resolve

1. **Batch vs Single Prediction:** Single transaction per API call (simpler) or batch multiple (higher throughput)?
2. **Cache Strategy:** Redis cache for frequent patterns? TTL = 5 mins?
3. **Fallback Logic:** When Vertex AI down, heuristic rules (velocity check, amount threshold)?
4. **Feature Attribution:** Store model explanations in fraud DB for audit trail?
5. **DLQ Retention:** How long to retain failed messages? 7 days? 30 days?
6. **PostgreSQL Backup:** Automated backups? Point-in-time recovery setup?
7. **Datadog Sampling:** Full traces or sampled? Performance vs cost tradeoff?
8. **Deployment Target:** Cloud Run, GKE, ECS, or self-hosted?

---

## 📚 Reference Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    RCL CDC Pipeline                              │
│  (Unchanged - proven, stable)                                   │
│  PostgreSQL → Debezium → Kafka (cdc.transactions, cdc.customers)│
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ↓ Confluent Cloud
┌─────────────────────────────────────────────────────────────────┐
│  Flink SQL Preprocessing (optional, provided by Confluent)       │
│  Join transaction + customer → fraud.enriched (with features)   │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ↓ fraud.enriched topic
┌─────────────────────────────────────────────────────────────────┐
│          Fraud Detection Service (NEW - scalable)                │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ 1. Kafka Consumer (fraud.enriched)                       │   │
│  │    → Consumer lag tracking                              │   │
│  │    → Offset management                                 │   │
│  │    → Datadog metrics export                            │   │
│  └──────────────────────────────────────────────────────────┘   │
│                           ↓                                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ 2. Fraud Scoring Pipeline                               │   │
│  │    → FilterStage (optional: threshold checks)          │   │
│  │    → TransformerStage (feature extraction)             │   │
│  │    → VertexAiStage (ML prediction: fraud_score)        │   │
│  │    → RouterStage (route by risk: low/med/high)         │   │
│  │    → PostgresWriter (3 tables by risk level)           │   │
│  │    → DLQ Producer (errors to dlq.fraud-detection)      │   │
│  └──────────────────────────────────────────────────────────┘   │
│                           ↓                                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ 3. Resilience & Observability                           │   │
│  │    → Circuit breaker (Vertex AI endpoint health)        │   │
│  │    → Health registry (Kafka/Vertex AI/Postgres status)  │   │
│  │    → Datadog metrics (fraud_detected, latency, etc.)    │   │
│  │    → Datadog APM traces (correlation IDs)              │   │
│  │    → /health & /ready endpoints (port 9091)            │   │
│  │    → Graceful shutdown (offset commits + flushes)      │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
         ↓                    ↓                    ↓
    ┌─────────┐          ┌──────────┐      ┌─────────────────┐
    │ Postgres│          │ Confluent│      │ Datadog         │
    │ Tables  │          │ Topics   │      │ Dashboard       │
    ├─────────┤          ├──────────┤      ├─────────────────┤
    │approved │          │fraud.    │      │ Metrics (real-  │
    │trans.   │          │scored    │      │ time)           │
    ├─────────┤          ├──────────┤      ├─────────────────┤
    │review   │          │fraud.    │      │ APM traces      │
    │queue    │          │alerts    │      │                 │
    ├─────────┤          ├──────────┤      ├─────────────────┤
    │blocked  │          │dlq.fraud │      │ Alerts &        │
    │trans.   │          │          │      │ notifications   │
    └─────────┘          └──────────┘      └─────────────────┘
```

---

## 📖 Documentation Deliverables

1. **Architecture Decision Records (ADR)**
   - Why separate service vs integrated stage
   - Why Confluent Cloud vs self-hosted Kafka
   - Why Vertex AI vs self-hosted model

2. **Deployment Guides**
   - GCP setup (Terraform templates)
   - Confluent Cloud integration
   - Datadog APM configuration
   - PostgreSQL schema migration

3. **Operational Runbooks**
   - Fraud service startup/shutdown
   - DLQ inspection & replay
   - Performance tuning (batch size, timeouts)
   - Troubleshooting (circuit breaker, latency)

4. **Demo Materials**
   - Sample transaction flow (end-to-end)
   - Datadog dashboard walkthrough
   - Confluent topic inspection
   - Error recovery scenarios

---

## 🎓 References

- **RCL Project:** [github.com/zlovtnik/rcl](https://github.com/zlovtnik/rcl)
- **RCL Instructions:** [.github/copilot-instructions.md](.github/copilot-instructions.md)
- **Project Spec:** [proj.md](proj.md)
- **Confluent Docs:** [docs.confluent.io](https://docs.confluent.io)
- **Vertex AI Docs:** [cloud.google.com/vertex-ai](https://cloud.google.com/vertex-ai)
- **Datadog Docs:** [docs.datadoghq.com](https://docs.datadoghq.com)

