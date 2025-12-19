# Validation Execution Order - Visual Guide

## Message Lifecycle Timeline

```
TIME FLOWS DOWN ↓

┌─────────────────────────────────────────────────────────────────────────────┐
│ KAFKA MESSAGE (bytes from broker)                                           │
│ {payload: {op: "c", after: {id: 12345, ts: "2024-01-15T10:30:00Z"}}}      │
└─────────────────────────────────────────────────────────────────────────────┘
                                    ↓
                    [consumer.rs:391] decode_and_validate()
                                    ↓
        ┌───────────────────────────────────────────────────────┐
        │ DECODER PHASE (decoder.rs)                            │
        │                                                       │
        │ 1. Parse JSON                                         │
        │ 2. Unwrap Debezium envelope                           │
        │ 3. Extract "after" field                              │
        │ 4. Inject "operation_type"                            │
        │                                                       │
        │ Message now: {id: 12345, ts: "...", operation_type: "c"}
        └───────────────────────────────────────────────────────┘
                                    ↓
        ┌───────────────────────────────────────────────────────┐
        │ VALIDATION PHASE (decoder.rs:93-115)                  │
        │                                                       │
        │ Loop through required_fields: ["id"]                  │
        │   Check: field "id" exists? YES ✅                    │
        │   Check: field "id" is not null? YES ✅               │
        │                                                       │
        │ Status: VALIDATION PASSED ✓                           │
        └───────────────────────────────────────────────────────┘
                                    ↓
        Validated message returned to consumer.rs:422
                                    ↓
        ┌───────────────────────────────────────────────────────┐
        │ METADATA INJECTION (consumer.rs:395-411)              │
        │                                                       │
        │ Add: _meta_topic, _meta_partition,                    │
        │      _meta_offset, _meta_ingest_ts                    │
        │                                                       │
        │ Message now: {id: 12345, ts: "...",                   │
        │              operation_type: "c", _meta_*: "..."}     │
        └───────────────────────────────────────────────────────┘
                                    ↓
        ┌───────────────────────────────────────────────────────┐
        │ PIPELINE EXECUTION (consumer.rs:422)                  │
        │ pipeline.execute(&stage_ctx, decoded)                 │
        │                                                       │
        │ Enters: eip.rs:129 execute() method                   │
        └───────────────────────────────────────────────────────┘
                                    ↓
        ┌───────────────────────────────────────────────────────┐
        │ TRANSFORMATION STAGE (stages.rs)                      │
        │                                                       │
        │ For each transformation in order:                     │
        │   1. Rename "id" → "order_id"                         │
        │   2. Rename "ts" → "event_timestamp"                  │
        │   3. Convert "event_timestamp" (iso→unix)             │
        │                                                       │
        │ Message now: {order_id: 12345,                        │
        │              event_timestamp: 1705316200,             │
        │              operation_type: "c", _meta_*: "..."}     │
        └───────────────────────────────────────────────────────┘
                                    ↓
        ┌───────────────────────────────────────────────────────┐
        │ BATCHING (batcher.rs)                                 │
        │ Buffer messages and flush when triggered              │
        └───────────────────────────────────────────────────────┘
                                    ↓
        ┌───────────────────────────────────────────────────────┐
        │ DATABASE WRITE (writer.rs)                            │
        │                                                       │
        │ INSERT INTO stg_orders (                              │
        │   order_id, event_timestamp, operation_type, ...      │
        │ ) VALUES (12345, 1705316200, 'c', ...)                │
        │                                                       │
        │ Using transformed field names from transformer        │
        └───────────────────────────────────────────────────────┘
                                    ↓
        ┌───────────────────────────────────────────────────────┐
        │ OFFSET COMMIT (consumer.rs)                           │
        │ Mark message as processed                             │
        └───────────────────────────────────────────────────────┘
```

## Side-by-Side: Field Name Evolution

```
Kafka Payload              Debezium Unwrapped         After Transformation    Database Schema
──────────────────────────────────────────────────────────────────────────────────────────────

id: 12345                  id: 12345                  order_id: 12345         order_id INT
ts: "2024..."              ts: "2024..."              event_timestamp: 1705   event_timestamp INT
amount: 99.99              amount: 99.99              amount: 99.99           amount DECIMAL
customer_id: 789           customer_id: 789           customer_id: 789        customer_id INT
                           operation_type: "c"        operation_type: "c"     operation_type CHAR
                           (injected by decoder)      (unchanged)             (unchanged)

                          ↑ VALIDATION              ↑ TRANSFORMATION         ↑ WRITTEN TO DB
                            Checks for "id"          Renames "id" to           Using
                                                     "order_id"              "order_id"
```

## Validation Timing Diagram

```
Time →

Kafka Message
    │
    ├─→ decode_and_validate()
    │   │
    │   ├─→ Parse JSON ✓
    │   │
    │   ├─→ Unwrap Debezium ✓
    │   │
    │   ├─→ Check required_fields ← ★ VALIDATION HAPPENS HERE
    │   │   │
    │   │   ├─→ Field "id" exists? YES ✓
    │   │   └─→ Field "id" is null? NO ✓
    │   │
    │   └─→ Return validated message ✓
    │
    ├─→ Execute Pipeline Stages ← ★ TRANSFORMATIONS HAPPEN HERE
    │   │
    │   ├─→ Transformer Stage:
    │   │   ├─→ Rename "id" → "order_id" ✓
    │   │   ├─→ Rename "ts" → "event_timestamp" ✓
    │   │   └─→ Convert "event_timestamp" ✓
    │   │
    │   └─→ Return transformed message ✓
    │
    └─→ Write to Database
        └─→ Insert with "order_id", "event_timestamp" ✓


KEY POINT: ★★★★★
Validation checks for field "id" (pre-transformation name)
Transformations run AFTER validation passes
Database receives "order_id" (post-transformation name)
```

## Code Flow Diagram

```
consumer.rs
├─ fetch_loop() [Task 1]
│  └─ Polls Kafka messages
│
├─ process_message() [Main Processing] ← Message enters here
│  ├─ Line 391: decode_and_validate()
│  │  │
│  │  └─→ decoder.rs::decode_and_validate()
│  │     ├─ Parse JSON
│  │     ├─ unwrap_debezium()
│  │     └─ validate_required_fields() ← ★ VALIDATION
│  │        │
│  │        └─ Check pipeline.required_fields
│  │           │
│  │           ├─ For "id": field exists? YES ✓
│  │           └─ Return Ok(())
│  │
│  ├─ Inject metadata fields
│  │
│  ├─ Line 422: pipeline.execute()
│  │  │
│  │  └─→ eip.rs::execute()
│  │     ├─ Loop through stages
│  │     │
│  │     └─ For Stage 0 (transformer):
│  │        ├─ stages.rs::TransformerStage::process() ← ★ TRANSFORMATIONS
│  │        │  └─ For each transformation:
│  │        │     ├─ Rename "id" → "order_id"
│  │        │     ├─ Rename "ts" → "event_timestamp"
│  │        │     └─ Convert "event_timestamp"
│  │        │
│  │        └─ Return StageResult::Continue(modified_msg)
│  │
│  ├─ Batch messages
│  │
│  └─ Write to database
│     └─ writer.rs::write_batch()
│        └─ INSERT using transformed field names
│
└─ metrics_exporter() [Task 2]
   └─ Exposes /metrics endpoint
```

## Configuration Validation Check

```
                    ┌─────────────────────┐
                    │ Load config.json    │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │ Validate JSON       │
                    │ Syntax              │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────────────────┐
                    │ Validate Pipeline Config:       │
                    │ ├─ Name: "orders_pipeline" ✓   │
                    │ ├─ Topic: "cdc.orders2" ✓      │
                    │ ├─ Table: "stg_orders" ✓       │
                    │ ├─ Required fields: ["id"] ✓   │
                    │ └─ Stages: 1 transformer ✓     │
                    └──────────┬──────────────────────┘
                               │
                    ┌──────────▼──────────────────────┐
                    │ Validate Stage Configs:         │
                    │ ├─ Transformer stage ✓         │
                    │ └─ Transformations: 3 ops ✓    │
                    └──────────┬──────────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │ All valid ✓         │
                    │ Config OK           │
                    └─────────────────────┘
```

## Error Scenarios

### ❌ Scenario 1: Validation Would Fail if "id" Was Missing

```
Kafka: {payload: {op: "c", after: {ts: "...", amount: 99.99}}}
                                      ↓ No "id" field!

Debezium Unwrap: {ts: "...", amount: 99.99, operation_type: "c"}
                                      ↓

Validation Check: "id" in required_fields
                                      ↓
                          required_fields: ["id"]
                                      ↓
                          Field "id" exists? NO ❌
                                      ↓
                          Error: "missing required field `id`"
                                      ↓
                          Message → DLQ ❌
                          Pipeline stops here
```

### ❌ Scenario 2: Validation Would Fail if "id" Was Null

```
Kafka: {payload: {op: "c", after: {id: null, ts: "...", ...}}}
                                      ↓ id is null

Debezium Unwrap: {id: null, ts: "...", operation_type: "c"}
                                      ↓

Validation Check: "id" in required_fields AND not null
                                      ↓
                          required_fields: ["id"]
                                      ↓
                          Field "id" exists? YES ✓
                          Field "id" is null? YES ❌
                                      ↓
                          Error: "missing required field `id`"
                                      ↓
                          Message → DLQ ❌
                          Pipeline stops here
```

### ✅ Scenario 3: Happy Path (Current Configuration)

```
Kafka: {payload: {op: "c", after: {id: 12345, ts: "...", ...}}}
                                      ↓

Debezium Unwrap: {id: 12345, ts: "...", operation_type: "c"}
                                      ↓

Validation Check: "id" in required_fields AND not null
                                      ↓
                          required_fields: ["id"]
                                      ↓
                          Field "id" exists? YES ✓
                          Field "id" is null? NO ✓
                                      ↓
                          VALIDATION PASSED ✓
                                      ↓
                          Continue to transformation stage
                                      ↓
Transformation: Rename "id" → "order_id"
                                      ↓
Message: {order_id: 12345, event_timestamp: 1705316200, ...}
                                      ↓
Database Write: INSERT ... VALUES (12345, 1705316200, ...) ✓
```

## Conclusion

The validation → transformation → write sequence is correctly implemented:

```
✓ Validation checks PRE-TRANSFORMATION field names
✓ Transformation runs AFTER validation succeeds
✓ Database receives POST-TRANSFORMATION field names
✓ Error handling stops early (fail-fast semantics)
```

This design ensures data integrity and proper separation of concerns.
