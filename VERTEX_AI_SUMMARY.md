# Vertex AI Stage Implementation - Executive Summary

## 📌 Quick Overview

You've requested a task list to implement a **Vertex AI Custom Stage** for real-time fraud detection in the rcl CDC pipeline. Before implementation, I've analyzed the codebase against the project requirements.

**Status:** 18-task roadmap created with critical gaps identified.

---

## 🎯 Key Deliverables Created

1. **VERTEX_AI_IMPLEMENTATION_PLAN.md** (2,500+ words)
   - Complete 7-phase implementation roadmap
   - 18 actionable tasks with dependencies
   - Success criteria and timeline estimates
   - Decision points and recommendations

2. **VERTEX_AI_GAPS_ANALYSIS.md** (2,500+ words)
   - 18 specific gaps compared to proj.md
   - Gap severity ratings (Critical → Minor)
   - Assumptions that need clarification
   - Comparison matrix and quick reference

3. **Task List** (18 items in manage_todo_list)
   - Organized by phase and dependencies
   - Detailed descriptions and acceptance criteria
   - Current status tracking

---

## 🔴 Critical Gaps (Blocking Issues)

### 1. No HTTP Client
**Problem:** Can't call Vertex AI API (no reqwest dependency)
**Solution:** Add `reqwest` crate to Cargo.toml
**Effort:** 30 min

### 2. No GCP Authentication
**Problem:** Can't authenticate with Google Cloud
**Solution:** Implement OAuth2 with service account JSON or ADC
**Effort:** 2-3 hours

### 3. Feature Vector Not Specified
**Problem:** Unclear which message fields → Vertex AI features
**Solution:** Create feature mapping document with enricher stage
**Effort:** 1 hour (with team input)

### 4. Timeout Fallback Strategy Undefined
**Problem:** What happens if Vertex AI API times out?
**Solution:** Decide: skip message, pass with default, or route to review
**Effort:** 30 min decision + implementation

### 5. Data Flow Coordination Missing
**Problem:** How do Flink SQL enrichments integrate with Vertex AI stage?
**Solution:** Document which fields come from enricher vs raw transaction
**Effort:** 1-2 hours (clarification needed)

---

## 🟡 Major Gaps (High Priority)

| Gap | Impact | Effort | Blocker |
|-----|--------|--------|---------|
| Error classification for Vertex AI | Affects retry behavior | 2 hrs | ✓ |
| Database schema (approved/review/blocked tables) | Output routing | 1 hr | ✓ |
| Risk categorization logic | Core business logic | 1 hr | ✓ |
| Testing framework for external APIs | Development velocity | 3 hrs | - |
| Metrics integration | Production monitoring | 2 hrs | - |
| Configuration validation schema | Config reliability | 1 hr | ✓ |

---

## ✅ What's Already Ready

These patterns can be directly reused:

- ✅ **Stage architecture** (Stage trait, factories, pipeline execution)
- ✅ **Error handling framework** (TransportError, ValidationError, retries)
- ✅ **Config system** (JSON loading, env var overrides)
- ✅ **Async runtime** (Tokio already running)
- ✅ **Metrics framework** (Prometheus hooks available)
- ✅ **Worker threads** (4-thread parallelism ready)
- ✅ **Redis integration** (client already present for caching)

---

## 📊 Implementation by the Numbers

| Metric | Value |
|--------|-------|
| **Total Tasks** | 18 |
| **Critical Gaps** | 5 |
| **Major Gaps** | 6+ |
| **Estimated Duration** | 13-19 days (1 person) |
| **Parallel Work Possible** | Yes (config/auth independently) |
| **New Files** | 1-2 (stages/vertex_ai.rs + config) |
| **Modified Files** | 5-6 (Cargo.toml, eip.rs, main.rs, metrics.rs, etc.) |
| **Test Coverage Target** | >80% |
| **Dependencies to Add** | 2-3 (reqwest, GCP auth, optional others) |

---

## 🔧 Implementation Roadmap at a Glance

```
Phase 1: Foundation (Days 1-3)
├── Add dependencies to Cargo.toml
├── Create VertexAIStage struct
├── Implement Stage trait
├── Integrate with StageFactory
└── Add module declarations

Phase 2: Config & Auth (Days 4-6)
├── Configuration schema & validation
└── GCP credential handling

Phase 3: Core Logic (Days 7-11)
├── HTTP client & prediction API
├── Feature extraction mapping
├── Risk categorization
└── Unit tests

Phase 4: Advanced Features (Days 12-14)
├── Redis caching (optional)
├── Metrics & monitoring
└── Error edge cases

Phase 5: Testing (Days 15-17)
├── Comprehensive unit tests
└── Integration tests with mock API

Phase 6: Documentation (Days 18-19)
├── Configuration examples
└── proj.md & design docs
```

---

## ❓ Questions Needing Clarification

Before implementation, get answers to:

### Data Flow
1. What fields are **guaranteed** in message from enricher stage?
2. Are customer lookup fields (age, avg_transaction) already in message?
3. How are velocity features computed (customer_count_24h, etc.)?

### Feature Engineering
4. Which fields are **required** for Vertex AI prediction?
5. What are **default values** for missing optional fields?
6. How should computed fields be calculated (amount_vs_avg_ratio)?

### Error Handling
7. **Timeout action:** Skip message, pass-through with default, or review queue?
8. **API auth error:** Immediate DLQ or circuit breaker?
9. **Malformed response:** Retry or permanent failure?

### Database
10. Are tables `fraud.approved_transactions`, `fraud.review_queue`, `fraud.blocked_transactions` being created?
11. How will Router stage route messages to these different tables?

### Model & Deployment
12. Will Vertex AI endpoint be provided, or is model training in scope?
13. How to handle model version upgrades/rollbacks?
14. Can multiple endpoint versions coexist for canary deployment?

---

## 📋 Task List Quick Reference

| Phase | Task | Days | Priority |
|-------|------|------|----------|
| 1 | Update Cargo.toml | 0.5 | P0 |
| 1 | Create VertexAIStage struct | 0.5 | P0 |
| 1 | Implement Stage trait | 1 | P0 |
| 1 | Integrate StageFactory | 0.5 | P0 |
| 1 | Add module declarations | 0.5 | P0 |
| 2 | Configuration schema | 1 | P1 |
| 2 | GCP authentication | 1.5 | P1 |
| 3 | HTTP client & API | 1.5 | P0 |
| 3 | Feature extraction | 1 | P1 |
| 3 | Risk categorization | 0.5 | P1 |
| 4 | Redis caching | 1 | P3 |
| 4 | Metrics integration | 1 | P2 |
| 4 | Error handling | 1 | P2 |
| 5 | Unit tests | 1.5 | P1 |
| 5 | Integration tests | 1 | P1 |
| 6 | Example config | 1 | P2 |
| 6 | Documentation | 1 | P2 |
| 7 | Deployment readiness | 1 | P3 |

---

## 🚀 Recommended Start

### Week 1 Priority
1. **Clarify gaps** with stakeholders (30 min call)
2. **Create feature mapping document** (2-3 hrs)
3. **Implement Tasks 1-5** (Foundation - 2 days)
4. **Start Task 7** (Core HTTP logic - 1 day)

### High-Value Quick Wins
- **30 min:** Add reqwest to Cargo.toml
- **1 hr:** Create VertexAIStage scaffold with from_config()
- **2 hrs:** Mock HTTP endpoint for testing
- **1 day:** Full end-to-end with mock API working

---

## 📁 Documentation Files Created

1. **VERTEX_AI_IMPLEMENTATION_PLAN.md** - Full roadmap with phases, decisions, success criteria
2. **VERTEX_AI_GAPS_ANALYSIS.md** - Detailed gap analysis, assumptions, quick matrix
3. **tasks.md** (updated) - 18-item todo list with status tracking

---

## 🎓 Key Design Decisions to Make

| Decision | Options | Recommendation |
|----------|---------|-----------------|
| **Module organization** | Inline vs separate file | Separate `src/stages/vertex_ai.rs` |
| **Timeout fallback** | Skip/pass/review | Skip (safest for fraud) |
| **Caching default** | Enabled/disabled | Disabled (model updates critical) |
| **Auth method** | Service account/ADC | Service account with ADC fallback |
| **Feature defaults** | 0/"unknown"/error | Default with warning in logs |
| **Attribution inclusion** | Always/never/high-risk-only | Optional (configurable) |

---

## ✨ Next Immediate Action

**Review the two generated documents:**
1. Read `VERTEX_AI_IMPLEMENTATION_PLAN.md` for complete roadmap
2. Read `VERTEX_AI_GAPS_ANALYSIS.md` for specific gaps
3. Check `tasks.md` for todo list

**Then clarify 5 critical items:**
1. Feature mapping (which fields in message?)
2. Timeout action (skip/pass/review?)
3. Error classification (what's retryable?)
4. Database schema (tables ready?)
5. GCP setup (endpoint provided?)

**Then start Phase 1 (foundation) with high confidence.**

---

## 📞 Support

All documentation references:
- Existing codebase patterns from `src/stages.rs`, `src/eip.rs`, `src/config.rs`
- Architecture from `proj.md` (lines 91-250)
- GitHub copilot-instructions.md for rcl conventions

Good luck with the implementation! 🚀

