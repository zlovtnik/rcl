use crate::eip::PipelineConfig;
use crate::errors::{DebeziumError, ProcessingError, ValidationError};
use crate::types::Operation;
use serde_json::Value;
use tracing::{debug, info};

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
        debug!(pipeline = %pipeline.name, operation = %value["operation_type"], "decoded Debezium message");
    }

    validate_required_fields(&value, pipeline)?;
    Ok(value)
}

fn unwrap_debezium(value: Value) -> Result<Value, ProcessingError> {
    let payload = value
        .get("payload")
        .cloned()
        .ok_or_else(|| DebeziumError::new("missing payload"))?;

    let op_str = payload
        .get("op")
        .and_then(|v| v.as_str())
        .ok_or_else(|| DebeziumError::new("missing op code"))?;

    let operation = Operation::try_from(op_str)
        .map_err(|e| DebeziumError::new(format!("invalid operation: {}", e)))?;

    let source_row = match operation {
        Operation::Create | Operation::Read | Operation::Update => payload.get("after"),
        Operation::Delete => payload.get("before"),
    };

    let row = source_row
        .cloned()
        .ok_or_else(|| DebeziumError::new("envelope missing before/after"))?;

    if let Value::Object(mut obj) = row {
        obj.insert(
            "operation_type".to_string(),
            Value::String(operation.to_string()),
        );
        Ok(Value::Object(obj))
    } else {
        Err(DebeziumError::new("before/after payload is not an object").into())
    }
}

/// Ensures all dotted-path fields declared in the pipeline are present and not null in the JSON value.
///
/// Traverses each path in `pipeline.required_fields` (dot-separated segments) and returns an error if any
/// segment is missing or has a `null` value.
///
/// # Parameters
///
/// - `value`: The JSON document to validate.
/// - `pipeline`: Pipeline configuration whose `required_fields` lists dot-separated paths that must exist in `value`.
///
/// # Errors
///
/// Returns `ValidationError` with message ``missing required field `<path>` `` where `<path>` is the required dotted path if any required field is absent or `null`.
///
/// # Examples
///
/// ```
/// use serde_json::json;
///
/// let value = json!({
///     "user": { "id": 1, "name": "alice" }
/// });
///
/// let pipeline = PipelineConfig {
///     required_fields: vec!["user.id".into(), "user.name".into()],
///     ..Default::default()
/// };
///
/// assert!(validate_required_fields(&value, &pipeline).is_ok());
/// ```
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eip::BackpressureConfig;
    use serde_json::json;

    /// Creates a test PipelineConfig with the specified Debezium envelope flag and required fields.
    ///
    /// # Examples
    ///
    /// ```
    /// let cfg = make_pipeline(true, vec!["id", "name"]);
    /// assert_eq!(cfg.name, "test");
    /// assert!(cfg.debezium_envelope);
    /// assert!(cfg.required_fields.contains(&"name".to_string()));
    /// ```
    fn make_pipeline(debezium: bool, required: Vec<&str>) -> PipelineConfig {
        PipelineConfig {
            name: "test".to_string(),
            topic: "t".to_string(),
            debezium_envelope: debezium,
            staging_table: "staging.test".to_string(),
            dlq: None,
            stages: vec![],
            required_fields: required.into_iter().map(|s| s.to_string()).collect(),
            backpressure: BackpressureConfig {
                channel_capacity: 100,
            },
            batching: Default::default(),
            circuit_breaker: Default::default(),
            worker_threads: 1,
            multi_tenancy: None,
        }
    }

    #[test]
    fn test_decode_debezium_create() {
        let payload = json!({
            "payload": {
                "op": "c",
                "after": { "id": 1, "name": "alice" }
            }
        })
        .to_string();

        let cfg = make_pipeline(true, vec![]);
        let res = decode_and_validate(payload.as_bytes(), &cfg).unwrap();
        assert_eq!(res.get("id").and_then(|v| v.as_i64()), Some(1));
        assert_eq!(res.get("name").and_then(|v| v.as_str()), Some("alice"));
        assert_eq!(
            res.get("operation_type").and_then(|v| v.as_str()),
            Some("c")
        );
    }

    #[test]
    fn test_decode_debezium_missing_op() {
        let payload = json!({
            "payload": { "after": {"id": 1} }
        })
        .to_string();

        let cfg = make_pipeline(true, vec![]);
        let err = decode_and_validate(payload.as_bytes(), &cfg).unwrap_err();
        match err {
            ProcessingError::Debezium(_) => {}
            _ => panic!("expected Debezium error"),
        }
    }

    #[test]
    fn test_validate_required_fields_missing() {
        let payload = json!({ "id": 1 }).to_string();
        let cfg = make_pipeline(false, vec!["name"]);
        let err = decode_and_validate(payload.as_bytes(), &cfg).unwrap_err();
        match err {
            ProcessingError::Validation(_) => {}
            _ => panic!("expected Validation error"),
        }
    }

    #[test]
    fn test_invalid_json() {
        let payload = b"{ invalid json }";
        let cfg = make_pipeline(false, vec![]);
        let err = decode_and_validate(payload, &cfg).unwrap_err();
        match err {
            ProcessingError::JsonDecode(_) => {}
            _ => panic!("expected JsonDecode error"),
        }
    }
}
