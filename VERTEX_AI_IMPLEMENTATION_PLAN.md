# Vertex AI Fraud Detection Stage - Implementation Plan

## 📋 Overview

This document outlines the complete implementation plan for adding a **Vertex AI Custom Stage** to the rcl CDC pipeline, enabling real-time ML-powered fraud detection as described in `proj.md`.

**Target:** Real-time fraud scoring with <100ms latency, integrated into the EIP pipeline architecture.

---

## 🔍 Current State Analysis

### Existing Architecture

The rcl codebase has:
- ✅ **Stage infrastructure** (`Stage` trait in `src/eip.rs`) with proven implementation patterns:
  - `FilterStage` - Message filtering
  - `TransformerStage` - Field mapping and transformations
  - `RouterStage` - Dynamic routing
  - `SplitterStage` - Array explosion
  - `IdempotentReceiverStage` - Deduplication with Redis

- ✅ **Error handling framework** (`src/errors.rs`):
  - `TransportError` (retryable) for network/DB failures
  - `ValidationError` (permanent) for schema violations
  - `ProcessingError` enum for all error types

- ✅ **Configuration system** (`src/config.rs`):
  - JSON-based pipeline configuration
  - Environment variable overrides
  - Validation framework

- ✅ **Async/concurrency model**:
  - Tokio runtime with multi-threaded executor
  - Worker pool coordination (`src/worker_pool.rs`)
  - Connection pooling (Postgres)
  - Backpressure via bounded mpsc channels

- ✅ **Observability**:
  - Prometheus metrics (`src/metrics.rs`)
  - Structured logging with tracing
  - Health checks
  - OpenTelemetry integration

### Critical Gaps Identified

#### 1. **No External HTTP API Integration** ⚠️
- **Current State:** No crate dependencies for HTTP clients (reqwest, http, etc.)
- **Impact:** Cannot make calls to Vertex AI prediction endpoint
- **Required:** Add `reqwest` with `tokio` runtime and optional GCP SDK

#### 2. **No GCP Authentication Infrastructure** ⚠️
- **Current State:** No Google Cloud credential handling
- **Impact:** Cannot authenticate with Vertex AI
- **Options:**
  - Manual JWT token creation from service account JSON
  - GCP authentication library (requires new dependency)
  - Application Default Credentials (ADC) as fallback

#### 3. **Feature Extraction Undefined** ⚠️
- **Problem:** Message fields → Vertex AI features mapping not specified
- **Current:** Only basic JSON field access exists
- **Required:** Document which message fields → which Vertex AI features
- **Note:** Enricher stage (Flink SQL) provides some features; need to coordinate

#### 4. **No Caching Layer for ML Predictions** ⚠️
- **Current:** Redis client exists (`src/stages.rs` line ~1200 for IdempotentReceiverStage)
- **Missing:** Prediction caching strategy not implemented anywhere
- **Required:** Implement cache key generation and TTL management

#### 5. **Timeout Handling is Ad-hoc** ⚠️
- **Issue:** 500ms timeout requirement conflicts with writer retry logic
- **Current:** No tokio::time::timeout pattern established
- **Risk:** If Vertex AI API hangs, could impact entire pipeline throughput
- **Required:** Define fallback_strategy (skip vs pass-through)

#### 6. **Message Mutation Side Effects** ⚠️
- **Issue:** Vertex AI stage adds `fraud_score`, `risk_category`, `feature_attributions`
- **Current:** TransformerStage does mutations; expected pattern is established
- **Risk:** Message size growth if many attributions included
- **Required:** Define safe mutation boundaries and size limits

#### 7. **No Staging Table Alternatives** ⚠️
- **Problem:** Dynamic table routing not designed for ML predictions
- **Current:** Router stage exists but designed for simple field-based routing
- **Required:** Ensure fraud results route to correct tables (`approved`, `review_queue`, `blocked`)

#### 8. **Metrics Gap** ⚠️
- **Missing:**
  - `vertex_ai_prediction_latency` histogram
  - `vertex_ai_api_errors` counter
  - Cache hit/miss metrics
  - Timeout events tracking
- **Impact:** Cannot monitor ML prediction performance
- **Required:** Extend `src/metrics.rs` with Vertex AI-specific metrics

#### 9. **Test Infrastructure for External APIs** ⚠️
- **Current:** Unit tests use local config; no external API mocking
- **Required:** Mock HTTP server (httptest or wiremock) for testing
- **Impact:** Cannot reliably test API integration without mocking

#### 10. **Documentation Gaps** ⚠️
- No feature schema documentation (which fields required/optional)
- No example Vertex AI request/response in proj.md
- No troubleshooting guide for common integration issues
- No performance tuning guidance

---

## 📊 18-Task Implementation Roadmap

### Phase 1: Foundation (Tasks 1-5)
Get the stage structure in place with proper module organization.

**Task 1: Update Cargo.toml with external dependencies**
- Add `reqwest` with `tokio` feature
- Add optional GCP auth crate (TBD: google-cloud-auth or manual JWT)
- Add optional caching support (Redis already present)
- Version constraints and security audit

**Task 2: Create VertexAIStage struct and configuration**
- Define `VertexAIStage` struct with fields:
  - `http_client: reqwest::Client` (with connection pooling)
  - `gcp_project_id: String`
  - `gcp_location: String` (default: us-central1)
  - `endpoint_id: String`
  - `model_type: ModelType` (enum: AutoML, Gemini)
  - `timeout_ms: u64` (default: 500)
  - `thresholds: RiskThresholds`
  - `cache: Option<PredictionCache>`
- Implement `from_config()` factory method

**Task 3: Implement Stage trait for VertexAIStage**
- `process()`: Core prediction logic
- `initialize()`: Setup HTTP client, validate connectivity
- `health_check()`: Verify endpoint reachable
- `shutdown()`: Cleanup resources

**Task 4: Integrate VertexAIStage into StageFactory**
- Add `"vertex_ai"` match arm in `src/eip.rs::StageFactory::create()`
- Wire up stage instantiation

**Task 5: Add VertexAIStage module declaration to main.rs**
- Decide: `mod stages;` inline vs `mod stages { pub mod vertex_ai; }`
- Recommendation: Create `src/stages/vertex_ai.rs` to keep `src/stages.rs` manageable

---

### Phase 2: Configuration & Auth (Tasks 6, 9)
Define the complete configuration schema and handle GCP authentication.

**Task 6: Define Vertex AI configuration schema and validation**
```json
{
  "type": "vertex_ai",
  "name": "fraud-scorer",
  "config": {
    "project_id": "${GCP_PROJECT_ID}",
    "location": "us-central1",
    "endpoint_id": "${VERTEX_AI_ENDPOINT_ID}",
    "model_type": "automl",
    "threshold_high_risk": 0.8,
    "threshold_medium_risk": 0.5,
    "timeout_ms": 500,
    "cache_enabled": true,
    "cache_ttl_seconds": 3600,
    "fallback_strategy": "skip"
  }
}
```
- Validation rules:
  - `threshold_medium_risk < threshold_high_risk`
  - Both thresholds in [0.0, 1.0]
  - `timeout_ms > 0`
  - `cache_ttl_seconds > 0` if cache enabled

**Task 9: Handle GCP authentication and credential management**
- Approach: Service account JSON from env var or ADC fallback
- Token management with expiration refresh
- Error handling for invalid credentials
- No credentials in code or config files

---

### Phase 3: Core Logic (Tasks 7, 10, 11)
Implement the fraud detection ML integration.

**Task 7: Implement Vertex AI HTTP client and prediction logic**
- HTTP endpoint: `https://[LOCATION]-aiplatform.googleapis.com/v1/projects/[PROJECT]/locations/[LOCATION]/endpoints/[ENDPOINT]:predict`
- Request format: Vertex AI prediction request
- Response parsing: Extract `fraud_probability` and feature attributions
- Error handling: API errors vs network errors
- Timeout handling: tokio::time::timeout with fallback strategy

**Task 10: Define feature extraction and message enrichment logic**
- Map message fields to Vertex AI features:
  - Required: `amount`, `customer_id`, `transaction_id`
  - Optional with defaults: `merchant_category`, `cross_border`, `device_id`, `ip_country`
  - Computed: `amount_vs_avg_ratio`, `velocity_flag`
- Coordinate with enricher stage (Flink SQL provides velocity features)
- Document which fields from original message vs enriched stream

**Task 11: Implement risk categorization and message mutation**
- Inject into message:
  - `fraud_score: f64` (3 decimal places)
  - `risk_category: String` (low/medium/high)
  - `ml_prediction_ts: String` (ISO8601)
  - `feature_attributions: Vec<FeatureAttribution>` (optional)
- Return `StageResult::Continue(modified_message)`

---

### Phase 4: Advanced Features (Tasks 8, 16, 17)
Performance optimization, monitoring, and resilience.

**Task 8: Implement optional Redis caching layer**
- Cache key: Hash of feature vector
- Check cache before API call
- Store response with TTL
- Handle cache failure gracefully (fallback to uncached prediction)

**Task 16: Performance and monitoring considerations**
- Metrics:
  - `vertex_ai_prediction_latency` (histogram)
  - `vertex_ai_api_errors` (counter by type)
  - `vertex_ai_cache_hits/misses` (counter)
  - `vertex_ai_timeouts` (counter)
  - `fraud_score_distribution` (histogram)
  - `risk_category_distribution` (gauge)
- SLA: Track end-to-end latency toward <100ms target
- Alerting thresholds (to be defined with ops team)

**Task 17: Handle edge cases and error scenarios**
- Missing features → ValidationError → DLQ
- Invalid predictions → ValidationError → DLQ
- 4xx API errors → ValidationError → DLQ
- 5xx API errors → TransportError → retry
- Network timeouts → TransportError → retry
- Timeout after retries → apply fallback_strategy
- Resource exhaustion → graceful degradation

---

### Phase 5: Testing (Task 12, 15)
Comprehensive test coverage and integration testing.

**Task 12: Write comprehensive unit tests for VertexAIStage**
- Config parsing and validation
- Feature extraction from messages
- Response parsing and error handling
- Risk categorization (low/medium/high boundaries)
- Timeout behavior
- Cache hit/miss scenarios
- Auth error handling

**Task 15: Integration testing with real/mock Vertex AI**
- Mock HTTP server simulating Vertex AI responses
- Full pipeline execution test
- Message transformation validation
- Error path validation
- Performance benchmarking (latency impact)

---

### Phase 6: Documentation & Configuration (Tasks 13, 14)
Make it usable for the hackathon demo.

**Task 13: Create example configuration file**
- `config/fraud_detection.json` with complete pipeline
- Includes filter → transformer → vertex_ai → router stages
- Environment variable placeholders documented
- Feature enrichment coordination notes

**Task 14: Update proj.md and documentation**
- Feature schema table (mapping message fields to Vertex AI inputs)
- Configuration reference
- Example request/response from Vertex AI
- Troubleshooting guide
- Performance characteristics and tuning

---

### Phase 7: Operations & Deployment (Task 18)
Production-ready setup.

**Task 18: Deployment and operational considerations**
- GCP service account credential management
- Endpoint versioning strategy
- Model rollback procedure
- Canary deployment support
- Secret injection for K8s/Cloud Run
- Monitoring dashboard setup
- Runbook for common issues

---

## 🚨 Critical Implementation Decisions

### 1. Feature Extraction Strategy
**Decision Point:** Which stage does feature engineering?

- **Option A (Current Assumption):** 
  - Flink SQL (Confluent) computes velocity features + enrichment
  - Transformer stage normalizes/formats features
  - Vertex AI stage just extracts from message and calls API
  - **Pros:** Separate concerns, reusable enrichment
  - **Cons:** Latency impact of multiple stages

- **Option B:** 
  - Vertex AI stage does all feature computation inline
  - **Pros:** Single stage, cleaner
  - **Cons:** Duplicates Flink SQL logic, harder to reuse

**Recommendation:** Option A (aligns with proj.md data flow)

### 2. Timeout Fallback Strategy
**Decision Point:** What happens if Vertex AI API times out after 500ms?

- **Option A:** Skip message (don't score it)
  - **Pros:** Safer, don't risk false positives
  - **Cons:** Unscored transactions slip through
  
- **Option B:** Pass through with default score (e.g., 0.5 = medium risk)
  - **Pros:** Catches more fraud
  - **Cons:** Risk of cascading false positives

- **Option C:** Route to review queue instead of auto-approve
  - **Pros:** Manual review catches issues
  - **Cons:** Adds operational burden

**Recommendation:** Option A (skip) with fallback_strategy configurable

### 3. Response Parsing Robustness
**Decision Point:** How strict on Vertex AI response validation?

- **Current:** Fail entire batch if one response invalid
- **Alternative:** Per-message error handling with DLQ routing
- **Recommendation:** Per-message (already supported by worker pool)

### 4. Caching Strategy
**Decision Point:** Cache predictions or always call API?

- **Fraud patterns change:** Caching might mask new attack patterns
- **ML model updates:** Changes won't be reflected in cache until TTL expires
- **Performance:** Caching is 100-1000x faster than API call

**Recommendation:** Make caching configurable; default OFF for fraud (safety first)

---

## 🎯 Success Criteria

### Functional Requirements
- ✅ VertexAIStage instantiates from JSON config
- ✅ Extracts features from message correctly
- ✅ Calls Vertex AI endpoint via HTTP
- ✅ Parses response and injects fraud_score, risk_category
- ✅ Handles timeouts without crashing
- ✅ Routes errors to DLQ appropriately
- ✅ Works in pipeline with other stages

### Performance Requirements
- ✅ Prediction latency <500ms per request (target: <50ms)
- ✅ End-to-end latency <100ms including stage overhead
- ✅ Throughput: 10,000+ TPS (with worker threads)
- ✅ Memory: No leaks with long-running pipeline

### Quality Requirements
- ✅ Unit test coverage >80%
- ✅ Integration tests with mock API
- ✅ Error path validation (all error types tested)
- ✅ Configuration validation (invalid configs rejected with clear errors)

### Operational Requirements
- ✅ GCP credentials managed securely (env vars, not in code)
- ✅ Fallback to ADC in GCP environments
- ✅ Metrics exported for monitoring
- ✅ Health checks working
- ✅ Graceful shutdown support

---

## 📦 Dependencies to Add

```toml
[dependencies]
reqwest = { version = "0.11", features = ["json", "cookies"] }
tokio-util = { version = "0.7", features = ["time"] }
# For GCP auth (choose one):
# Option 1: Use google-cloud-auth (simple but opinionated)
google-cloud-auth = { version = "0.13", optional = true }
# Option 2: Manual JWT (more control)
# jsonwebtoken = { version = "9.2", optional = true }

[dev-dependencies]
httptest = "0.15"  # or wiremock = "0.5"
```

---

## 🗓️ Timeline Estimate

Assuming 1 person, full-time:

| Phase | Tasks | Estimated Days | Dependencies |
|-------|-------|-----------------|--------------|
| 1 | 1-5 | 2-3 | None |
| 2 | 6, 9 | 2-3 | Phase 1 complete |
| 3 | 7, 10, 11 | 3-4 | Phase 2 complete |
| 4 | 8, 16, 17 | 2-3 | Phase 3 complete |
| 5 | 12, 15 | 2-3 | Phase 3 complete |
| 6 | 13, 14 | 1-2 | Phase 5 complete |
| 7 | 18 | 1 | All phases |
| **Total** | **18** | **13-19 days** | **Parallel possible** |

**Parallelization:** Tasks 6 & 9 (config/auth) can start after 5; Task 12 (tests) can start after 3

---

## 📝 Next Steps

1. **Immediate (Day 1):**
   - Review this plan with team
   - Decide on GCP auth approach
   - Finalize feature mapping spec

2. **Day 2-3:**
   - Execute Tasks 1-5 (Foundation)
   - Set up mock HTTP test framework

3. **Day 4-6:**
   - Execute Tasks 6, 9 (Config/Auth)
   - Begin Task 7 (HTTP logic)

4. **Day 7-11:**
   - Complete Tasks 7, 10, 11, 12, 15
   - Comprehensive testing

5. **Day 12-19:**
   - Tasks 8, 13, 14, 16, 17, 18
   - Documentation and ops readiness

---

## 🔗 Related Files to Review

- `src/eip.rs` (Stage trait, StageFactory)
- `src/stages.rs` (Existing stage implementations)
- `src/config.rs` (Configuration handling)
- `src/errors.rs` (Error types)
- `src/metrics.rs` (Metrics framework)
- `proj.md` (Architecture & requirements)
- `docker-middleware-stack/` (Test infrastructure)

