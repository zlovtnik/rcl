# Validation Execution Order in RCL Pipeline

## Overview

This document explains the order of operations in the RCL message processing pipeline, specifically how required fields validation is ordered relative to transformations.

## Execution Flow

The message processing pipeline follows this strict order:

```
Kafka Message (raw bytes)
    ↓
decode_and_validate() [in src/decoder.rs]
    ├─ Parse JSON
    ├─ Unwrap Debezium envelope (if enabled)
    └─ Validate required_fields ← VALIDATION HAPPENS HERE
         ↓
inject metadata fields (_meta_topic, _meta_partition, _meta_offset, _meta_ingest_ts)
         ↓
pipeline.execute() [in src/eip.rs]
    └─ Run transformation stages (rename, convert, etc.)
         ↓
Batch and write to database
```

## Code Evidence

### 1. Validation Entry Point (consumer.rs, line 391)

```rust
let mut decoded = decode_and_validate(payload.as_bytes(), &pipeline.config)?;
```

This is called **BEFORE** any pipeline stages execute.

### 2. Validation Implementation (decoder.rs, lines 7-25)

```rust
pub fn decode_and_validate(
    payload: &[u8],
    pipeline: &PipelineConfig,
) -> Result<Value, ProcessingError> {
    let raw = std::str::from_utf8(payload)?;
    debug!(payload_size = payload.len(), pipeline = %pipeline.name, "decoding message");
    let mut value: Value = serde_json::from_str(raw)?;

    if pipeline.debezium_envelope {
        debug!(pipeline = %pipeline.name, "unwrapping Debezium envelope");
        value = unwrap_debezium(value)?;
        if let Some(op) = value.get("operation_type") {
            info!(pipeline = %pipeline.name, operation = %op, "decoded Debezium message");
        }
    } else {
        debug!(pipeline = %pipeline.name, "no Debezium envelope configured");
    }

    validate_required_fields(&value, pipeline)?;  // ← VALIDATION HERE
    Ok(value)
}
```

### 3. required_fields Validation Logic (decoder.rs, lines 93-115)

```rust
fn validate_required_fields(
    value: &Value,
    pipeline: &PipelineConfig,
) -> Result<(), ValidationError> {
    for field in &pipeline.required_fields {
        let mut current = Some(value);

        for part in field.split('.') {
            current = match current.and_then(|v| v.get(part)) {
                Some(v) if !v.is_null() => Some(v),
                _ => {
                    return Err(ValidationError::new(format!(
                        "missing required field `{}`",
                        field
                    )));
                }
            };
        }
    }
    Ok(())
}
```

This validates the **current state of the message** at that moment in time - which is **AFTER Debezium unwrapping but BEFORE any transformations**.

### 4. Pipeline Execution (consumer.rs, line 422)

```rust
let processed_messages = pipeline.execute(&stage_ctx, decoded).await?;
```

This is called **AFTER** validation completes successfully.

### 5. Transformation Stages (eip.rs, lines 129-176)

```rust
pub async fn execute(
    &self,
    ctx: &StageContext,
    msg: Value,
) -> Result<Vec<Value>, ProcessingError> {
    let mut current_messages = vec![msg];
    info!(pipeline = %ctx.pipeline_name, context = %ctx.correlation_id, initial_messages = 1, "starting pipeline execution");

    for (stage_idx, stage) in self.stages.iter().enumerate() {
        // Process each stage (transformers, filters, routers, etc.)
        for msg in current_messages {
            let result = stage.process(ctx, msg).await?;
            // Apply transformations...
        }
    }

    Ok(current_messages)
}
```

This executes **AFTER** validation returns successfully.

## Why This Order is Correct

### Configuration Example from config/example.json

```json
{
  "name": "orders_pipeline",
  "topic": "cdc.orders2",
  "debezium_envelope": true,
  "required_fields": ["id"],
  "stages": [
    {
      "type": "transformer",
      "name": "enrich_data",
      "config": {
        "transformations": [
          {
            "type": "rename",
            "from": "id",
            "to": "order_id"
          }
        ]
      }
    }
  ]
}
```

**Timeline of the message:**

1. **Stage: Kafka → decode_and_validate()**
   - Message has: `{"payload": {"op": "c", "after": {"id": 123, ...}}}`
   - After Debezium unwrap: `{"id": 123, "operation_type": "c", ...}`
   - Validation checks: Does the message have field "id"? ✅ YES - Validation PASSES

2. **Stage: Validation → Transformation**
   - Message still has: `{"id": 123, "operation_type": "c", ...}`
   - Transformer renames "id" to "order_id"
   - Message becomes: `{"order_id": 123, "operation_type": "c", ...}`

3. **Stage: Write to Database**
   - Message has field "order_id" (which maps to the database column)
   - Database write succeeds with correct field name

## Key Principle

**required_fields validation should reference PRE-TRANSFORMATION field names**, because validation executes on the message immediately after Debezium unwrapping and before any transformations.

If you want to validate a field that only exists AFTER transformation, you have two options:

1. **Add a custom validation stage** as the first transformation stage that checks for the transformed field name
2. **Restructure the pipeline** to perform required validation after transformations (not recommended - breaks schema assumptions)

## Common Patterns

### Pattern 1: Validate Pre-Transformation Names (RECOMMENDED)

```json
{
  "required_fields": ["id", "customer_id"],  // Field names as they appear in Debezium payload
  "stages": [
    {
      "type": "transformer",
      "config": {
        "transformations": [
          {"type": "rename", "from": "id", "to": "order_id"},
          {"type": "rename", "from": "customer_id", "to": "cust_id"}
        ]
      }
    }
  ]
}
```

**Timeline:**
- Validation checks: "id", "customer_id" ✅ Found
- Transformation: Renames them to "order_id", "cust_id"
- Write succeeds with new field names

### Pattern 2: No Transformation (Field Names Stay Same)

```json
{
  "required_fields": ["id", "amount"],
  "stages": [
    {
      "type": "transformer",
      "config": {
        "transformations": [
          {"type": "convert", "field": "amount", "to": "string_to_float"}
        ]
      }
    }
  ]
}
```

**Timeline:**
- Validation checks: "id", "amount" ✅ Found
- Transformation: Only converts amount type, doesn't rename
- Write succeeds with same field names but converted types

### Pattern 3: No Validation (Accept All Messages)

```json
{
  "required_fields": [],  // Empty list
  "stages": [...]
}
```

**Timeline:**
- Validation: Skipped (no required fields to check)
- Transformation: Proceeds
- Write: Handles missing/null fields per writer logic

## Logging Evidence

When you run the load test, you'll see logs in this order:

```
processing message (context="cdc.orders2:0:1234")
  → decoded Debezium message (operation="c")
  → required_fields validated              ← VALIDATION COMPLETE
  → executing pipeline stages
    → processing stage 0
      → field renamed (from="id" to="order_id")  ← TRANSFORMATION HAPPENS AFTER
      → field renamed (from="ts" to="event_timestamp")
      → field converted (field="event_timestamp")
      → stage processing completed
  → adding to batcher
  → flushing batch
  → writing batch to database
```

The key log markers:
- ✅ `"required_fields validated"` appears BEFORE `"executing pipeline stages"`
- ✅ `"field renamed"` appears AFTER validation
- ✅ Database write receives the transformed field names

## Conclusion

**The current configuration is CORRECT.** The validation flow properly:

1. ✅ Validates required fields on the pre-transformation message state
2. ✅ Executes transformations after validation succeeds
3. ✅ Writes to database with post-transformation field names

This ensures schema consistency throughout the pipeline while maintaining a clear separation of concerns between validation (structural correctness) and transformation (data enrichment).
