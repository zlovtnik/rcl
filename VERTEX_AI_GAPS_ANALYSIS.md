# Vertex AI Stage - Critical Gaps & Assumptions

This document clarifies the differences between the `proj.md` vision and what currently exists in the `rcl` codebase.

## 🔴 Critical Gaps

### 1. **HTTP Client Infrastructure**
**proj.md assumes:** Vertex AI stage can call Google Cloud REST API
**Current code:** No HTTP client dependency (no reqwest, http, curl, etc.)

```
Gap: Missing reqwest + tokio configuration
Impact: Cannot make HTTPS calls to Vertex AI endpoint
Severity: BLOCKING - must add to Cargo.toml
```

### 2. **GCP Authentication**
**proj.md assumes:** Stage authenticates with Google Cloud (OAuth2 Bearer token)
**Current code:** No Google Cloud authentication infrastructure

```
Gap: No credential handling, JWT generation, or ADC support
Impact: Cannot authenticate with Vertex AI
Options:
  - Manual JWT from service account JSON
  - google-cloud-auth crate
  - Environment-based ADC fallback
Severity: BLOCKING - core requirement
```

### 3. **Feature Vector Construction**
**proj.md specifies:** Feature schema with 10+ fields (amount, merchant_category, etc.)
**Current code:** Message fields extracted with simple `serde_json` access

```
Gap: No specification of:
  - Required fields vs optional
  - Default values for missing fields
  - Computation of derived features (amount_vs_avg_ratio, velocity_flag)
  - Coordination with enricher stage (Flink SQL provides velocity features)
Impact: Cannot build correct feature vector for ML prediction
Severity: HIGH - logic correctness depends on this
```

### 4. **Message Enrichment Coordination**
**proj.md assumes:** Flink SQL in Confluent computes enriched features before rcl stage
**Current code:** rcl has no visibility into which features come from where

```
Gap: No specification of:
  - Expected message structure when entering Vertex AI stage
  - Which fields are already in message vs need to be looked up
  - Schema of fraud.enriched Kafka topic from Flink
Impact: Feature extraction logic may reference non-existent fields
Example: Is transaction_count_24h already in message from Flink, or must Vertex AI stage fetch it?
Severity: HIGH - data flow assumption
```

### 5. **Timeout & Fallback Strategy**
**proj.md specifies:** 500ms timeout for Vertex AI API calls
**Current code:** No timeout handling pattern established

```
Gap: No specification of what happens on timeout:
  - Skip the message? (don't score it)
  - Pass through with default score? (medium risk)
  - Route to manual review queue?
Current pattern: No tokio::time::timeout usage
Impact: Pipeline could hang if Vertex AI API latency > 500ms
Severity: HIGH - affects pipeline availability
```

### 6. **Risk Categorization Logic**
**proj.md shows:** fraud_probability → risk_category (low/medium/high)
**Current code:** No existing categorization pattern

```
Gap: Exact thresholds not fully specified:
  - Low: 0.0 to 0.5? (implied from threshold_medium_risk)
  - Medium: 0.5 to 0.8?
  - High: 0.8+?
Current: No pattern for threshold-based categorization
Severity: MEDIUM - straightforward to implement once thresholds finalized
```

### 7. **Prediction Caching**
**proj.md mentions:** cache_ttl_seconds configuration option
**Current code:** Redis client exists but caching not implemented anywhere

```
Gap: No caching layer for predictions:
  - Cache key generation strategy
  - TTL refresh logic
  - Fallback if Redis unavailable
Current: IdempotentReceiverStage uses Redis but different use case
Severity: MEDIUM - optimization, not core requirement (can be optional)
```

### 8. **Metrics & Observability**
**proj.md targets:** <100ms end-to-end latency
**Current code:** Vertex AI stage metrics don't exist yet

```
Gap: Missing metrics:
  - vertex_ai_prediction_latency (histogram)
  - vertex_ai_api_errors (counter)
  - vertex_ai_cache_hits/misses (counter)
  - vertex_ai_timeouts (counter)
Current: src/metrics.rs exists but no hooks for new stage
Impact: Cannot monitor whether <100ms SLA is met
Severity: MEDIUM - important for production monitoring
```

### 9. **Dynamic Table Routing**
**proj.md shows:** Router stage routes by risk_category to different tables
**Current code:** RouterStage exists but designed for field-based routing only

```
Gap: Confirmation needed:
  - Can Router stage route to different tables based on injected risk_category?
  - Are tables already created (approved_transactions, review_queue, blocked_transactions)?
  - Will writer handle multiple tables correctly?
Current: Router should work, but tables not yet defined in schema
Severity: MEDIUM - dependent on DB schema
```

### 10. **Error Classification for Vertex AI**
**proj.md assumes:** Some errors are retryable, some permanent
**Current code:** Error types exist but not applied to Vertex AI scenario

```
Gap: Specification missing for:
  - API 401 (auth failed) → ValidationError or TransportError?
  - API 500 (service down) → TransportError (retryable)
  - API timeout → skip message or retry?
  - Invalid response JSON → ValidationError → DLQ
Current: Error handling framework exists, mapping undefined
Severity: MEDIUM - affects resilience behavior
```

---

## 🟡 Moderate Gaps

### 11. **Testing Infrastructure for External APIs**
**proj.md assumes:** Stage can be tested with real/mock Vertex AI
**Current code:** No HTTP mocking framework in test dependencies

```
Gap: Testing approach undefined:
  - Unit tests: need httptest or wiremock for mocking
  - Integration tests: need realistic mock responses
  - Local dev: need quick way to test without GCP setup
Current: Tests use local config, no external API pattern
Severity: MEDIUM - affects development velocity
Solution: Add httptest or wiremock to dev dependencies
```

### 12. **Feature Attribution Handling**
**proj.md mentions:** Feature attributions in response from Vertex AI
**Current code:** No pattern for including explanations in output

```
Gap: Specification needed:
  - Always include attributions or only for high-risk predictions?
  - How many attributions to include (top 3, top 5)?
  - Impact on message size and pipeline throughput?
Current: No decision documented
Severity: LOW - nice-to-have for explainability
```

### 13. **Configuration Validation**
**proj.md specifies:** Schema for Vertex AI stage config
**Current code:** Validation patterns exist but not applied to VertexAI

```
Gap: Must validate:
  - threshold_medium_risk < threshold_high_risk
  - Both thresholds in [0.0, 1.0]
  - timeout_ms > 0
  - endpoint_id not empty
Current: Validation framework exists, just needs implementation
Severity: LOW - straightforward validation rules
```

---

## 🟢 Minor Gaps

### 14. **Documentation**
**proj.md shows:** Complete architecture and feature schema
**Current code:** No Vertex AI-specific documentation

```
Gap:
  - Feature mapping table (message fields → Vertex AI inputs)
  - Example request/response from Vertex AI API
  - Configuration template for fraud_detection.json
  - Troubleshooting guide
Current: General rcl docs exist
Severity: LOW - documentation can be created after implementation
```

### 15. **Performance Benchmarking**
**proj.md targets:** 10,000 TPS with <100ms latency
**Current code:** No performance testing framework for Vertex AI stage

```
Gap: Need benchmarks for:
  - Single prediction latency (p50, p95, p99)
  - Cache hit/miss impact
  - Worker thread scaling (4 threads per proj.md)
  - Impact on overall pipeline throughput
Current: Load testing framework exists in src/load_test.rs
Severity: LOW - can add benchmarks after implementation
```

---

## 🟡 Assumptions That Need Clarification

### A. Feature Enrichment Flow
**Current assumption:** Flink SQL provides enriched message to Kafka topic

```
✓ cdc.transactions (raw)
   ↓
✓ [Flink SQL enrichment]
   ↓
✓ fraud.enriched (with velocity features)
   ↓
→ rcl consumer reads fraud.enriched topic
→ Messages already have: customer_age_days, transaction_count_24h, etc.
→ Vertex AI stage extracts and calls API
```

**Verify:** Is the fraud.enriched topic schema documented? Which fields are guaranteed present?

### B. Model Training & Versioning
**Current assumption:** Vertex AI endpoint already deployed with trained model

```
Not in scope for this task:
  - Training the fraud detection model
  - Creating AutoML dataset
  - Deploying endpoint
  
Assumption: GCP_PROJECT_ID and VERTEX_AI_ENDPOINT_ID environment variables
provided by ops team with working endpoint
```

**Verify:** Will endpoint be provided, or is model training part of demo prep?

### C. Latency Budget Allocation
**Target:** <100ms end-to-end
**proj.md breakdown:**
  - Vertex AI prediction: <50ms
  - Postgres write: <20ms
  - Other stages: <30ms

**Verify:** Is the 50ms Vertex AI budget achievable with network latency included?

### D. Consumer Lag Impact
**Assumption:** Vertex AI stage will create consumer lag if predictions are slow

```
If Vertex AI prediction takes 50ms and we process 10,000 TPS:
  - 4 worker threads: each thread handles 2,500 TPS
  - Latency per message: ~50ms (prediction) + overhead
  - Consumer lag: will increase if Kafka produce rate > processing rate
```

**Verify:** Is scaling (worker_threads: 4) sufficient for target throughput?

### E. Failure Mode: What happens to transactions in `fraud.alerts` topic?
**proj.md shows:** fraud.alerts topic for high-risk alerts
**Assumption:** Some external system consumes this (not in rcl scope)

```
Does rcl need to:
  - Publish to fraud.alerts topic?
  - Or does Router stage handle this?
```

---

## ✅ What's Already Available

These patterns can be reused:

1. **Redis integration:** `IdempotentReceiverStage` uses Redis ✅
2. **Environment variables:** Config system supports `${VAR}` substitution ✅
3. **Error handling:** TransportError vs ValidationError distinction ✅
4. **Async/await:** Tokio runtime already running ✅
5. **Metrics:** Prometheus framework in place ✅
6. **Logging:** Structured tracing with correlation IDs ✅
7. **Testing:** Unit test patterns established ✅
8. **Configuration:** JSON config loading with validation ✅
9. **Connection pooling:** Postgres pool exists, can model HTTP client pool on it ✅
10. **Worker threads:** Already implemented and working ✅

---

## 🚀 Recommended Next Steps

### Before Implementation Begins:

1. **Schedule clarification call** to confirm:
   - ✅ Is fraud.enriched schema documented?
   - ✅ Will Vertex AI endpoint be provided or needs to be created?
   - ✅ What exactly should fallback_strategy do on timeout?
   - ✅ Are DB tables (approved, review_queue, blocked) being created separately?

2. **Create feature mapping document** with:
   - List of all required fields with types
   - List of optional fields with defaults
   - Which fields come from Flink vs raw transaction
   - Which fields need computation/lookup

3. **Design error handling matrix:**
   - API error code → ProcessingError type → outcome (retry/DLQ/skip)

4. **Set performance budgets:**
   - Vertex AI P95 latency target
   - Cache hit rate target
   - Acceptable DLQ error rate

### Starting Implementation:

1. **High Priority:**
   - Add reqwest to Cargo.toml
   - Implement VertexAIStage struct and from_config()
   - Implement Stage::process() with mock endpoint
   - Add to StageFactory

2. **Medium Priority:**
   - GCP auth (service account or ADC)
   - Feature extraction mapping
   - Risk categorization logic
   - Integration tests with httptest

3. **Lower Priority:**
   - Caching implementation
   - Metrics collection
   - Documentation
   - Performance benchmarking

---

## 📊 Comparison Matrix

| Aspect | proj.md | Current rcl | Gap |
|--------|---------|------------|-----|
| **Stage Architecture** | ✅ Defined | ✅ Exists | ✅ None |
| **HTTP Client** | ✅ Required | ❌ Missing | 🔴 Critical |
| **GCP Auth** | ✅ Required | ❌ Missing | 🔴 Critical |
| **Feature Schema** | ✅ Specified | ❌ Unclear coordination | 🟡 High |
| **Timeout Handling** | ✅ 500ms | ❌ No pattern | 🟡 High |
| **Fallback Strategy** | ❓ Implied | ❌ Undefined | 🟡 High |
| **Risk Categorization** | ✅ Described | ❌ Not implemented | 🟡 Medium |
| **Caching** | ✅ Optional | ❌ Not implemented | 🟡 Medium |
| **Error Handling** | ✅ Framework | ❌ Vertex AI mapping | 🟡 Medium |
| **Metrics** | ✅ Targets | ❌ Missing Vertex AI | 🟡 Medium |
| **Testing** | ✅ Needed | ❌ No mock API | 🟡 Medium |
| **Documentation** | ✅ High-level | ❌ Specific gaps | 🟡 Low |

