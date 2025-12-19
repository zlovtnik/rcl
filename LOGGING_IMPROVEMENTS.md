# Logging Improvements Summary

## Overview
Enhanced the RCL pipeline with comprehensive structured logging throughout the message processing lifecycle. All logs include:
- Structured fields for easy filtering and analysis
- Correlation IDs (topic:partition:offset) for tracing messages
- Pipeline names and context
- Operation details for debugging

## Logging Enhancements

### 1. Consumer Processing (`src/consumer.rs`)
**Message Intake:**
- `"processing message"` - When a message is fetched from Kafka
  - Fields: `context`, `topic`, `partition`, `offset`

**Pipeline Routing:**
- `"executing pipeline stages"` - Pipeline selected for message
  - Fields: `context`, `pipeline`, `stages` (count)

**Offset Management:**
- `"Committed offsets for X partitions"` - After successful writes

### 2. Debezium Decoding (`src/decoder.rs`)
**Envelope Unwrapping:**
- `"decoded Debezium message"` - After extracting payload
  - Fields: `pipeline`, `operation` (c/r/u/d)

**Field Extraction:**
- `"extracted Debezium fields"` - After extracting before/after data
  - Fields: `pipeline`, `context`, `operation`, `fields_count`

**Validation:**
- `"required fields validated"` - After checking required fields
  - Fields: `pipeline`, `context`, `total_fields`, `required_count`

### 3. EIP Pipeline Execution (`src/eip.rs`)
**Pipeline Lifecycle:**
- `"starting pipeline execution"` - Begin processing
  - Fields: `pipeline`, `context`, `initial_messages`

- `"processing stage"` - For each stage processed
  - Fields: `pipeline`, `context`, `stage_index`, `messages_count`

- `"pipeline execution finished"` - After all stages
  - Fields: `pipeline`, `context`, `final_message_count`

- `"pipeline execution completed"` - Ready for batching
  - Fields: `context`, `messages_after_stages`

### 4. Stage Processing (`src/stages.rs`)
**Transformer Stage:**
- `"field renamed"` - Each field mapping applied
  - Fields: `pipeline`, `context`, `from`, `to`

- `"field converted"` - Each type conversion applied
  - Fields: `pipeline`, `context`, `field`, `value`

- `"transformer stage completed"` - Stage finished
  - Fields: `pipeline`, `context`

**Stage Results:**
- `"stage processing completed"` - After stage execution
  - Fields: `pipeline`, `context`, `stage_index`, `messages_entering`, `messages_exiting`

### 5. Batching (`src/batcher.rs`)
**Message Buffering:**
- `"adding to batcher"` - Message added to batch buffer
  - Fields: `context`, `table` (optional)

**Buffer Flushing:**
- `"flushing buffer"` - Batch about to be written
  - Fields: `pipeline`, `table`, `batch_size`, `batch_bytes`, `reason` (time/size/byte), `latency_ms`

- `"flushing batch"` - Batch being formatted for write
  - Fields: `pipeline`, `table`, `batch_size`, `batch_bytes`, `reason`

### 6. Database Writing (`src/writer.rs`)
**Write Operations:**
- `"writing batch to database"` - Batch write started
  - Fields: `batch_size`, `table`, `pipeline`

- `"batch composition"` - Operation breakdown (c/u/d counts)
  - Fields: `batch_size`, `operations` (JSON)

- `"write attempt"` - Each retry attempt
  - Fields: `attempt`, `batch_size`, `table`

**Write Results:**
- `"COPY insert succeeded"` - Bulk insert successful
  - Fields: `batch_size`, `table`

- `"batch insert succeeded"` - Row-by-row insert successful
  - Fields: `batch_size`, `table`

- `"COPY failed, falling back to INSERT"` - Fallback triggered
  - Fields: `table`, `error`

## Message Flow Example

Processing a single message produces:
```
processing message (topic:partition:offset)
  ↓
decoded Debezium message (operation type)
  ↓
executing pipeline stages
  ↓
starting pipeline execution
  ↓
processing stage 0
  ↓
field renamed/converted (x N for each transformation)
  ↓
transformer stage completed
  ↓
stage processing completed
  ↓
pipeline execution finished
  ↓
pipeline execution completed
  ↓
adding to batcher
  ↓
[accumulate until flush triggers]
  ↓
flushing buffer (batch_size, reason)
  ↓
writing batch to database
  ↓
batch composition (operation counts)
  ↓
write attempt 1
  ↓
COPY insert succeeded (or fallback to INSERT)
  ↓
batch insert succeeded
  ↓
Committed offsets for X partitions
```

## Correlation IDs

All logs include a correlation ID in the format `topic:partition:offset`, making it easy to:
- Trace a specific message through the entire pipeline
- Search logs for messages from specific partitions
- Identify and investigate failures

Example: `"cdc.orders:2:10185"`

## Structured Fields for Filtering

Common fields across logs:
- `context` - Correlation ID (topic:partition:offset)
- `pipeline` - Pipeline name
- `table` - Target table
- `batch_size` - Number of messages in batch
- `stage_index` - EIP stage number
- `operation` - Debezium operation (c/r/u/d)
- `error` - Error message when applicable

## Benefits

1. **End-to-end Traceability** - Follow a message through the entire pipeline
2. **Performance Debugging** - See where messages spend time (batching, writing)
3. **Batch Analysis** - Understand batch composition and sizes
4. **Transformation Visibility** - See what fields are being transformed
5. **Fallback Tracking** - Monitor COPY to INSERT fallbacks
6. **Operational Insights** - Track flush triggers and reasons

## Testing

To see detailed logs:
```bash
# Start the service
RCL_CONFIG_PATH=config/example.json cargo run

# In another terminal, generate load
cargo run -- load-test --rate 500 --duration-sec 10
```

Logs will show:
- Message fetching from Kafka
- Debezium envelope unwrapping
- Field transformations
- Batch accumulation
- Database writes with timings
