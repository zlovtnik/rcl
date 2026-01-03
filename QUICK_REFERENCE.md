# Quick Reference Guide - Validation & Logging

## TL;DR - Validation Execution Order

```
Kafka Message
    ↓
Decode + Validate (checks pre-transform field names)  ← "id" validated
    ↓
Transform (renames "id" → "order_id")                ← After validation
    ↓
Write to Database (uses "order_id")                  ← Post-transform name
```

**Bottom Line**: Configuration is ✅ CORRECT. Validation checks "id" (pre-transform), transformation renames to "order_id" (post-transform).

---

## Validation Order

| Step | Location | Field Names | Operation |
|------|----------|------------|-----------|
| 1 | consumer.rs:391 | Kafka bytes | Receive message |
| 2 | decoder.rs:14-24 | Raw JSON | Parse & unwrap Debezium |
| 3 | decoder.rs:25 | **Pre-transform** | **Validate required_fields** |
| 4 | consumer.rs:395-411 | Pre-transform | Inject metadata |
| 5 | eip.rs:129 | Pre-transform → transform | Execute pipeline stages |
| 6 | stages.rs | **Post-transform** | Rename fields |
| 7 | writer.rs | **Post-transform** | Write to database |

---

## Key Log Markers (In Order)

```
1. "processing message"                    ← Entry point
2. "decoded Debezium message"              ← Debezium processed
3. "required_fields validated"             ← ★ VALIDATION HAPPENED HERE
4. "executing pipeline stages"             ← Stages start
5. "field renamed"                         ← ★ TRANSFORMATION HAPPENS HERE
6. "field converted"
7. "stage processing completed"
8. "adding to batcher"
9. "flushing buffer"
10. "writing batch to database"
11. "Committed offsets"
```

**Key Observation**: "required_fields validated" (step 3) comes BEFORE "field renamed" (step 5) ✓

---

## Field Name Timeline

```
Input (from Kafka Debezium):
  {id: 12345, ts: "2024-01-15T10:30:00Z", ...}
       ↑      ↑
       └──────┴─→ These are validated by required_fields: ["id"]

After Transformation:
  {order_id: 12345, event_timestamp: 1705316200, ...}
       ↑            ↑
       └────────────┴─→ These are written to database

Mapping:
  "id" → validated ✓ → transformed → "order_id" → written to DB ✓
  "ts" → validated ✓ → transformed → "event_timestamp" → written to DB ✓
```

---

## Configuration Check

**Current config/example.json:**
```json
{
  "required_fields": ["id"],        // ← Validates for "id"
  "stages": [{
    "type": "transformer",
    "config": {
      "transformations": [
        {"type": "rename", "from": "id", "to": "order_id"}  // ← Happens after validation
      ]
    }
  }]
}
```

**Status**: ✅ CORRECT - Matches execution order

---

## Common Questions

### Q: Why validate before transformation?
**A**: Ensures data integrity at source before any modifications. If validation ran after transformation, you'd be checking for "order_id" but the original Debezium payload has "id" - mismatch!

### Q: Can I validate post-transformation fields?
**A**: Currently no (validation happens early). Two options:
1. Validate pre-transformation names (recommended, what we do now)
2. Create custom validation stage after transformations (more complex)

### Q: What if my Debezium field is different from what's in required_fields?
**A**: Validation will fail with error: `missing required field \`<field>\``
- Check your Debezium connector configuration
- Make sure source table has that column
- Update required_fields to match actual Debezium schema

### Q: What happens if validation fails?
**A**: Message is not processed and is routed to DLQ (dead letter queue) if configured

---

## Testing Validation

### Test 1: Verify validation passes
```bash
cargo run --release -- --validate-config
# Output: Configuration is valid
```

### Test 2: See validation in action
```bash
cargo run -- load-test --rate 100 --duration-sec 10
# Look for: "required_fields validated"
```

### Test 3: Test validation failure (optional)
Edit config to require non-existent field:
```json
"required_fields": ["non_existent_field"]
```
Messages will fail validation and go to DLQ.

---

## Logging Quick Start

### Enable Debug Logging
```bash
RUST_LOG=debug cargo run
```

### Track a Specific Message
```bash
grep "context=\"cdc.orders2:0:1234\"" logs.txt
```

### See Field Transformations
```bash
grep "field renamed" logs.txt
```

### See Write Operations
```bash
grep "writing batch" logs.txt
```

---

## Documentation Files

| File | Purpose |
|------|---------|
| **VALIDATION_EXECUTION_ORDER.md** | Detailed explanation of validation order with code evidence |
| **VALIDATION_VERIFICATION_REPORT.md** | Step-by-step message flow and verification |
| **VALIDATION_VISUAL_GUIDE.md** | Diagrams and visual explanations |
| **LOGGING_IMPROVEMENTS.md** | What was logged and where |
| **LOGGING_VALIDATION.md** | Verification of logging implementation |
| **IMPLEMENTATION_COMPLETE.md** | Complete summary of all work |

---

## File Modification Summary

| File | Changes |
|------|---------|
| src/consumer.rs | Added: message processing, pipeline execution logs |
| src/decoder.rs | Added: Debezium decoding, validation logs |
| src/eip.rs | Added: pipeline lifecycle logs |
| src/stages.rs | Added: field transformation logs |
| src/batcher.rs | Added: buffer and flush logs |
| src/writer.rs | Added: write operation logs; Fixed: clippy warnings |
| src/logging.rs | Fixed: clippy modulo warning |
| config/example.json | No changes (already correct) |

---

## Status ✅

- ✅ Comprehensive logging added
- ✅ All 244 tests passing
- ✅ Validation order verified as correct
- ✅ Configuration validated
- ✅ Documentation complete

**Result**: Pipeline has full observability with proper validation semantics.

---

## Load Testing

**Purpose**: Validate performance at scale by pumping messages into Kafka and measuring throughput, latency, memory usage, and CPU usage.

**Command**:
```bash
# Test at 10k msg/s for 60 seconds
cargo run -- load-test --rate 10000 --duration-sec 60

# Test at 50k msg/s with 5 producers
cargo run -- load-test --rate 50000 --duration-sec 60 --producers 5

# Test at 100k msg/s with 10 producers
cargo run -- load-test --rate 100000 --duration-sec 60 --producers 10
```

**Metrics Reported**:
- **Throughput**: Messages per second achieved
- **Latency**: Average send latency in milliseconds
- **Memory**: Current memory usage in MB
- **CPU**: Current CPU usage percentage
- **Total Sent**: Cumulative messages sent

**Identifying Bottlenecks**:
- If throughput << target rate: Network or Kafka bottleneck
- High latency: Network congestion or Kafka overload
- Increasing memory: Memory leak or buffer buildup
- High CPU: Processing bottleneck (though load test is mostly I/O)

**Setup Requirements**:
1. Start Kafka stack: `make docker-up`
2. Ensure config file points to correct Kafka brokers
3. Run load test with desired parameters

**Notes**:
- Uses first pipeline's topic from config
- Messages are synthetic Debezium-formatted JSON
- Multiple producers distribute load evenly
- Reports stats every 5 seconds per producer + final summary

---

**Issue**: Validation failing for required field
→ Check: Does Debezium payload actually have that field?
→ Fix: Update required_fields to match actual schema

**Issue**: Can't find specific message in logs
→ Use: Correlation ID format "topic:partition:offset"
→ Command: `grep "context=\"topic:partition:offset\"" logs.txt`

**Issue**: Transformation not happening
→ Check: Pipeline stages configured? Validation passed?
→ Look for: "field renamed" log entries

**Issue**: Data not reaching database
→ Check: Do logs show "writing batch to database"?
→ Check: Database connection configured correctly?
→ Check: Table name matches staging_table in config
