# Fraud Detection Service: Quick Start Guide

> **Architecture:** Separate Rust microservice consuming CDC data from Confluent Cloud  
> **Status:** Ready to implement (40 tasks identified)  
> **Effort:** ~80-100 hours (new project from scratch)  
> **Timeline:** 6-8 weeks (parallel tracks)

---

## 🎯 What We're Building

```
RCL CDC Pipeline          Fraud Service              Output
──────────────────        ─────────────              ──────

PostgreSQL                fraud.enriched topic       
   ↓                      ✓ Consume from Kafka      fraud.scored topic
Debezium CDC              ✓ Call Vertex AI          fraud.alerts topic
   ↓                      ✓ Route by risk           dlq.fraud-detection
RCL Pipeline              ✓ Write to Postgres       
   ✓ Filter               ✓ Metrics → Datadog      PostgreSQL Tables
   ✓ Enrich               ✓ Traces → Datadog       ├─ approved (low)
   ✓ Transform            ✓ Health checks          ├─ review (medium)
   ✓ Write staging        ✓ Circuit breaker        └─ blocked (high)
   ↓
cdc.transactions
   ↓
Confluent Cloud
(Flink SQL enriches)
```

**Key Innovation:** Fraud service is **completely independent** from RCL. RCL stays stable, fraud service handles all ML complexity.

---

## 📋 The 40 Tasks (Organized by Phase)

### **Phase 1: Foundation (4 tasks, 12 hours)**
```
[ ] 1. Create new GitHub project: fraud-detection-service
[ ] 2. Add Cargo.toml with dependencies
[ ] 3. Create main.rs skeleton (config loader, graceful shutdown)
[ ] 4. Implement Kafka consumer (fraud.enriched topic)
```
**Deliverable:** Can consume from Confluent Cloud

---

### **Phase 2: Fraud Scoring (7 tasks, 18 hours)**
```
[ ] 5. Create VertexAiStage (calls Vertex AI API)
[ ] 6. Create RouterStage (routes by risk: low/medium/high)
[ ] 7. Add Datadog metrics exporter
[ ] 21. Add fraud_score validation & risk categorization
[ ] 22. Implement feature mapping (transform fields → model inputs)
[ ] 23. Add timeout handling for Vertex AI calls
[ ] 24. Add Vertex AI endpoint health checks
```
**Deliverable:** Scores transactions and routes by risk

---

### **Phase 3: Data & Resilience (6 tasks, 12 hours)**
```
[ ] 9. Implement PostgreSQL writer (COPY + INSERT fallback)
[ ] 12. Add circuit breaker for Vertex AI endpoint
[ ] 13. Create health registry (Kafka/Vertex AI/Postgres status)
[ ] 14. Add DLQ producer (error routing)
[ ] 15. Implement graceful shutdown
[ ] 20. Create PostgreSQL schema (3 tables: approved/review/blocked)
```
**Deliverable:** Reliable writes with error handling

---

### **Phase 4: Observability (5 tasks, 16 hours)**
```
[ ] 8. Add Datadog APM tracing (distributed traces)
[ ] 10. Create config schema
[ ] 11. Add environment variable overrides
[ ] 26. Create Datadog dashboard (JSON spec)
[ ] 27. Create Datadog alerts (critical + warning)
[ ] 34. Document Datadog integration guide
```
**Deliverable:** Full visibility in Datadog

---

### **Phase 5: Testing & Docs (8 tasks, 12 hours)**
```
[ ] 28. Unit tests (stages, feature mapping, validation)
[ ] 29. Integration tests (Kafka → Postgres end-to-end)
[ ] 30. Load testing (latency verification, throughput)
[ ] 31. Docker setup (multi-stage build + docker-compose)
[ ] 32. GCP deployment guide (Terraform templates)
[ ] 33. Confluent Cloud integration guide
[ ] 35. Create demo scenario documentation
[ ] 16-19. Create config examples (Confluent Cloud, local Kafka)
```
**Deliverable:** Tested, documented, demo-ready

---

### **Phase 6: Optimization (4 tasks, 12 hours)**
```
[ ] 36. Add Redis caching for Vertex AI predictions (optional)
[ ] 37. Add fallback scoring logic (optional)
[ ] 39. Add GitHub Actions CI/CD pipeline
[ ] 38. Update RCL README for Confluent Cloud integration
```
**Deliverable:** Production-ready with optimizations

---

## 🚀 Implementation Path

### **Week 1-2: Foundations (Phase 1 + Phase 2 start)**
- Day 1-2: New project setup, Kafka consumer
- Day 3-4: VertexAiStage + RouterStage
- Day 5: Feature mapping + validation
- **Checkpoint:** Can score transactions with Vertex AI

### **Week 3-4: Data Layer (Phase 3 + Phase 4 start)**
- Day 1-2: PostgreSQL writer + schema
- Day 3: Circuit breaker + health registry
- Day 4: DLQ + graceful shutdown
- Day 5: Datadog metrics integration
- **Checkpoint:** Reliable writes with metrics

### **Week 5-6: Observability & Testing (Phase 4 finish + Phase 5)**
- Day 1-2: Unit tests + integration tests
- Day 3: Load testing
- Day 4: Docker setup
- Day 5: Documentation + deployment guides
- **Checkpoint:** Tested and documented

### **Week 7-8: Polish & Deployment (Phase 6)**
- Day 1-2: CI/CD pipeline setup
- Day 3: Optimization (caching, fallback)
- Day 4: Demo scenario walkthrough
- Day 5: Production readiness review
- **Checkpoint:** Ready to deploy

---

## 🏗️ Component Architecture

```
fraud-detection-service/
├── src/
│   ├── main.rs                 # Entry point
│   ├── config.rs               # Config loader + validation
│   ├── consumer.rs             # Kafka consumer (fraud.enriched)
│   ├── stages/
│   │   ├── mod.rs
│   │   ├── vertex_ai.rs        # VertexAiStage: calls Vertex AI API
│   │   ├── router.rs           # RouterStage: routes by risk
│   │   └── filter.rs           # Optional: threshold checks
│   ├── writer.rs               # PostgreSQL writer (COPY + INSERT)
│   ├── metrics.rs              # Datadog metrics exporter
│   ├── health.rs               # Component health status
│   ├── dlq.rs                  # Dead letter queue producer
│   ├── circuit_breaker.rs      # Fault tolerance
│   ├── errors.rs               # Error types
│   └── shutdown.rs             # Graceful shutdown
├── config/
│   ├── example.json            # Template config
│   ├── confluent_cloud.json    # Confluent Cloud setup
│   └── local_kafka.json        # Local testing
├── sql/
│   └── schema.sql              # PostgreSQL tables
├── tests/
│   ├── unit/
│   ├── integration/
│   └── load/
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── .github/workflows/
│   ├── test.yml                # Unit tests
│   ├── integration.yml         # Integration tests
│   └── build.yml               # Build & push image
└── Cargo.toml
```

---

## 💾 PostgreSQL Schema

```sql
-- Approved Transactions (low risk, auto-approve)
CREATE TABLE approved_transactions (
  id UUID PRIMARY KEY,
  transaction_id VARCHAR(50),
  amount DECIMAL(10,2),
  merchant_id VARCHAR(50),
  customer_id VARCHAR(50),
  fraud_score FLOAT,
  risk_category VARCHAR(20),
  detected_at TIMESTAMP,
  model_version VARCHAR(50),
  created_at TIMESTAMP DEFAULT NOW()
);

-- Review Queue (medium risk, manual review)
CREATE TABLE review_queue (
  id UUID PRIMARY KEY,
  -- same fields as approved_transactions
  reviewer_id VARCHAR(50),
  review_status VARCHAR(20),
  review_timestamp TIMESTAMP
);

-- Blocked Transactions (high risk, auto-block)
CREATE TABLE blocked_transactions (
  id UUID PRIMARY KEY,
  -- same fields as approved_transactions
  block_reason TEXT,
  appeal_id VARCHAR(50)
);
```

---

## 🔌 Dependencies Summary

```toml
[dependencies]
# Runtime
tokio = { version = "1", features = ["full"] }

# Kafka
rdkafka = "0.35"
serde_json = "1.0"

# Vertex AI
google-cloud-aiplatform = "0.1"
google-cloud-auth = "0.1"
tonic = "0.10"
prost = "0.12"

# PostgreSQL
sqlx = { version = "0.7", features = ["postgres", "uuid", "chrono"] }

# Observability
opentelemetry = "0.20"
opentelemetry-otlp = "0.13"
tracing = "0.1"
tracing-subscriber = "0.3"
statsd = "0.15"  # or datadog crate

# Error handling
anyhow = "1.0"
thiserror = "1.0"

# Config
serde = { version = "1.0", features = ["derive"] }
toml = "0.8"

# Async patterns
backoff = { version = "0.4", features = ["tokio"] }
async-trait = "0.1"
```

---

## 🎯 Quick References

### Fraud Score Ranges
```
Low Risk [0.0 - 0.5]      → approved_transactions (auto-approve)
Medium Risk [0.5 - 0.8]   → review_queue (manual review)
High Risk [0.8 - 1.0]     → blocked_transactions (auto-block)
```

### Kafka Topics
```
Input:  fraud.enriched              (from Confluent/Flink SQL)
Output: fraud.scored                (scored transactions)
        fraud.alerts                (high-risk alerts)
        dlq.fraud-detection         (errors)
```

### Environment Variables (Minimum)
```
CONFLUENT_BOOTSTRAP_SERVERS=...
CONFLUENT_API_KEY=...
CONFLUENT_API_SECRET=...
VERTEX_AI_ENDPOINT=...
GCP_PROJECT_ID=...
POSTGRES_URL=...
DD_AGENT_HOST=localhost
DD_ENV=production
```

---

## 🚦 Success Metrics

### Performance
- [ ] **Vertex AI latency:** <50ms P95 (per prediction)
- [ ] **End-to-end latency:** <100ms P95 (Kafka → Postgres)
- [ ] **Throughput:** 5,000+ msg/sec sustained
- [ ] **Error rate:** <0.1%

### Reliability
- [ ] **Uptime:** 99.9% (SLA)
- [ ] **DLQ messages:** <1% of total volume
- [ ] **Circuit breaker:** Prevents cascading failures
- [ ] **Graceful shutdown:** No data loss

### Observability
- [ ] **Datadog dashboard:** Real-time metrics visible
- [ ] **Alert coverage:** Critical alerts on failures
- [ ] **Distributed tracing:** End-to-end request flow visible
- [ ] **Error analysis:** All DLQ messages inspectable

---

## 📚 Key Documents

1. **Full Implementation Plan:** [VERTEX_AI_STAGE_GAPS.md](VERTEX_AI_STAGE_GAPS.md)
2. **RCL Architecture:** [.github/copilot-instructions.md](.github/copilot-instructions.md)
3. **Project Requirements:** [proj.md](proj.md)
4. **Datadog Integration:** [Datadog APM Docs](https://docs.datadoghq.com/tracing/)
5. **Confluent Cloud:** [Confluent Cloud Docs](https://docs.confluent.io/cloud/current/home.html)
6. **Vertex AI:** [Google Cloud Vertex AI Docs](https://cloud.google.com/vertex-ai/docs)

---

## ✅ Pre-Implementation Checklist

Before starting Phase 1:
- [ ] GitHub project created: `fraud-detection-service`
- [ ] GCP project with Vertex AI API enabled
- [ ] Vertex AI model trained/deployed to endpoint
- [ ] Confluent Cloud cluster provisioned
- [ ] Datadog account with APM enabled
- [ ] PostgreSQL instance accessible (for testing)
- [ ] Rust 1.70+ installed
- [ ] Team reviewed this architecture

---

## 💡 Why This Approach?

| Decision | Benefit |
|----------|---------|
| **Separate Service** | RCL stays stable, fraud service evolves independently |
| **Confluent Cloud** | Managed Kafka, built-in security, monitoring |
| **Vertex AI** | No ML engineering, pre-built models, easy to retrain |
| **Datadog** | Single pane of glass (logs, metrics, traces) |
| **PostgreSQL Risk Tables** | Different policies per risk level |
| **Circuit Breaker** | Prevents cascading failures |
| **Graceful Shutdown** | No data loss on restart |
| **DLQ Pattern** | Post-mortem analysis + reprocessing |

---

## 🎓 Next Steps

1. **Read:** [VERTEX_AI_STAGE_GAPS.md](VERTEX_AI_STAGE_GAPS.md) (full 40-task breakdown)
2. **Plan:** Assign teams to phases (can be parallel)
3. **Setup:** Create GitHub project, initialize Cargo project
4. **Phase 1:** Get Kafka consumer working (2 weeks)
5. **Phase 2:** Add VertexAI scoring + routing (2 weeks)
6. **Demo:** Present fraud detection flow to stakeholders
7. **Production:** Deploy to GCP with Datadog monitoring

---

**Ready to start? Pick Phase 1 and begin! 🚀**
