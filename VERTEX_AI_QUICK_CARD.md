# Vertex AI Stage - Implementation Quick Card

## 🎯 At a Glance

| Item | Status | Effort |
|------|--------|--------|
| **Stage Infrastructure** | ✅ Exists (use Stage trait) | - |
| **HTTP Client** | ❌ Missing (add reqwest) | 30m |
| **GCP Auth** | ❌ Missing (implement) | 2-3h |
| **Feature Schema** | ❓ Needs clarification | 1h |
| **Error Handling** | ✅ Framework exists | - |
| **Config System** | ✅ Exists (extend) | 1h |
| **Metrics** | ✅ Framework (add hooks) | 2h |
| **Testing** | ⚠️ Need mock API | 3h |
| **Documentation** | ⚠️ Partial (complete) | 2h |
| **TOTAL** | 🔴 13-19 days | 1 person |

---

## 🚨 Critical Path

```
1. Add reqwest to Cargo.toml (30 min)
   ↓
2. Define feature mapping (1 hr)
   ↓
3. Implement VertexAIStage struct (2-3 hrs)
   ↓
4. GCP authentication (2-3 hrs)
   ↓
5. HTTP client & prediction logic (3-4 hrs)
   ↓
6. Testing & validation (ongoing)
```

---

## 📋 The 18 Tasks Organized by Phase

### Phase 1: Foundation (Days 1-3)
- [ ] Task 1: Add reqwest, google-cloud-auth to Cargo.toml
- [ ] Task 2: Create VertexAIStage struct & config parsing
- [ ] Task 3: Implement Stage trait (process, initialize, health_check)
- [ ] Task 4: Add VertexAIStage to StageFactory::create()
- [ ] Task 5: Add module declarations (main.rs)

### Phase 2: Config & Auth (Days 4-6)
- [ ] Task 6: Configuration schema & validation
- [ ] Task 9: GCP credential handling

### Phase 3: Core Logic (Days 7-11)
- [ ] Task 7: HTTP client & Vertex AI prediction API
- [ ] Task 10: Feature extraction mapping
- [ ] Task 11: Risk categorization (low/medium/high)
- [ ] Task 12: Unit tests

### Phase 4: Advanced (Days 12-14)
- [ ] Task 8: Redis caching layer
- [ ] Task 16: Metrics & monitoring
- [ ] Task 17: Error edge cases

### Phase 5: Testing (Days 15-17)
- [ ] Task 15: Integration tests with mock API

### Phase 6: Documentation (Days 18-19)
- [ ] Task 13: Example config file
- [ ] Task 14: Update proj.md & docs

### Phase 7: Operations (Optional)
- [ ] Task 18: Deployment & operational readiness

---

## 🔴 The 5 Critical Gaps

### 1. No HTTP Client ⚠️
```
Missing: reqwest crate
Action: Add to Cargo.toml with tokio feature
Effort: 30 min
Blocking: Yes
```

### 2. No GCP Auth ⚠️
```
Missing: Google Cloud authentication
Action: Implement service account + ADC fallback
Effort: 2-3 hours
Blocking: Yes
```

### 3. Feature Mapping Unclear ⚠️
```
Missing: Which message fields → Vertex AI features?
Action: Clarify with stakeholders
Effort: 1 hour (depends on team input)
Blocking: High (logic correctness)
```

### 4. Timeout Strategy Undefined ⚠️
```
Missing: What happens on Vertex AI timeout?
Action: Decide: skip / pass-through / review
Effort: Implementation after decision
Blocking: Medium (affects resilience)
```

### 5. Error Classification Incomplete ⚠️
```
Missing: Which errors are retryable?
Action: Document error → action mapping
Effort: 1-2 hours
Blocking: Medium (affects reliability)
```

---

## 🟡 The 6 Major Gaps

| Gap | Severity | Action |
|-----|----------|--------|
| Testing infrastructure | High | Add httptest/wiremock for mock API |
| Database schema | High | Create approved/review/blocked tables |
| Risk categorization | High | Implement low/med/high logic |
| Metrics integration | Medium | Add Vertex AI-specific metrics |
| Config validation | Medium | Validate threshold constraints |
| Documentation | Medium | Feature mapping, examples, troubleshooting |

---

## 💡 Key Implementation Patterns

### From Existing Stages
- **FilterStage** (src/stages.rs ~20): Condition evaluation
- **TransformerStage** (src/stages.rs ~200): Message mutation
- **RouterStage** (src/stages.rs ~620): Dynamic routing
- **IdempotentReceiverStage** (src/stages.rs ~1100): Redis integration

### Config Pattern
```rust
pub fn from_config(name: String, config: Value) -> Result<Self> {
    let project_id = config.get("project_id").and_then(|v| v.as_str())?;
    let endpoint_id = config.get("endpoint_id").and_then(|v| v.as_str())?;
    // ... validate and return Self
}
```

### Stage Trait Pattern
```rust
#[async_trait]
impl Stage for VertexAIStage {
    async fn process(&self, ctx: &StageContext, msg: Value) 
        -> Result<StageResult, ProcessingError> {
        // Extract features
        // Call Vertex AI API
        // Inject fraud_score, risk_category
        // Return StageResult::Continue(modified_msg)
    }
}
```

### Error Handling Pattern
```rust
// Use existing error types:
TransportError → network/API failures → retryable
ValidationError → malformed data → DLQ
ProcessingError → stage failures → handled by pipeline
```

---

## 📊 Configuration Example

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
    "cache_enabled": false,
    "fallback_strategy": "skip"
  }
}
```

---

## ✅ Checklist for Launch

### Pre-Implementation
- [ ] Review VERTEX_AI_IMPLEMENTATION_PLAN.md
- [ ] Review VERTEX_AI_GAPS_ANALYSIS.md
- [ ] Get answers to 5 clarification questions
- [ ] Confirm feature mapping with team
- [ ] Decide on timeout fallback strategy
- [ ] Create database schema (if needed)

### Foundation Phase (Days 1-3)
- [ ] Add dependencies to Cargo.toml
- [ ] Create VertexAIStage scaffold
- [ ] Implement Stage trait skeleton
- [ ] Integration with StageFactory
- [ ] Compile successfully

### Core Logic Phase (Days 4-11)
- [ ] GCP authentication working
- [ ] HTTP client with connection pooling
- [ ] Feature extraction from message
- [ ] Vertex AI API calls (mocked first)
- [ ] Risk categorization
- [ ] Unit tests (>80% coverage)
- [ ] Error handling for all paths

### Testing Phase (Days 12-17)
- [ ] Mock HTTP endpoint
- [ ] Integration tests
- [ ] End-to-end pipeline test
- [ ] Performance benchmarks
- [ ] Error scenario validation

### Completion (Days 18-19)
- [ ] Documentation complete
- [ ] Example configuration working
- [ ] Metrics exposed
- [ ] Code review ready
- [ ] PR template filled

---

## 🎓 Success Metrics

### Functional
- ✅ Vertex AI stage loads from config
- ✅ Extracts features from message
- ✅ Calls Vertex AI endpoint
- ✅ Injects fraud_score & risk_category
- ✅ Handles timeouts gracefully

### Performance
- ✅ Prediction latency <500ms
- ✅ End-to-end <100ms (with other stages)
- ✅ 10,000+ TPS throughput

### Quality
- ✅ Unit test coverage >80%
- ✅ All error paths tested
- ✅ Config validation working
- ✅ Metrics exposed correctly

### Operational
- ✅ GCP credentials secure
- ✅ Health checks passing
- ✅ Graceful shutdown
- ✅ Documentation complete

---

## 📚 Key Files to Reference

```
src/eip.rs                  Stage trait, StageFactory
src/stages.rs               Existing stage implementations (1824 lines)
src/config.rs               Configuration system
src/errors.rs               Error types (TransportError, ValidationError)
src/metrics.rs              Prometheus metrics framework
src/main.rs                 Module declarations
proj.md                     Architecture & requirements (lines 91-250)
Cargo.toml                  Dependencies (add reqwest)
```

---

## ⏱️ Daily Breakdown

**Day 1-2:** Add deps, create scaffold, StageFactory integration
**Day 3-4:** Config schema, GCP auth setup
**Day 5-6:** HTTP client, basic prediction logic
**Day 7-8:** Feature extraction, risk categorization
**Day 9-10:** Testing (unit & integration)
**Day 11-12:** Metrics, error edge cases
**Day 13:** Mock API, integration tests
**Day 14-15:** Documentation, examples
**Day 16-19:** Polish, optimization, deployment readiness (if needed)

---

## 🔗 Related Documentation

- `VERTEX_AI_IMPLEMENTATION_PLAN.md` (2,500+ words, full roadmap)
- `VERTEX_AI_GAPS_ANALYSIS.md` (2,500+ words, detailed gaps)
- `tasks.md` (18-item todo list)
- `/Users/rcs/git/rcl/.github/copilot-instructions.md` (rcl patterns)

Good luck! 🚀

