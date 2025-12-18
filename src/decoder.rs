use crate::config::PipelineConfig;
use crate::errors::{DebeziumError, ProcessingError, ValidationError};
use serde_json::Value;

pub fn decode_and_validate(
    payload: &[u8],
    pipeline: &PipelineConfig,
) -> Result<Value, ProcessingError> {
    let raw = std::str::from_utf8(payload)?;
    let mut value: Value = serde_json::from_str(raw)?;

    if pipeline.debezium_envelope {
        value = unwrap_debezium(value)?;
    }

    validate_required_fields(&value, pipeline)?;
    Ok(value)
}

fn unwrap_debezium(value: Value) -> Result<Value, ProcessingError> {
    let payload = value
        .get("payload")
        .cloned()
        .ok_or_else(|| DebeziumError::new("missing payload"))?;

    let op = payload
        .get("op")
        .and_then(|v| v.as_str())
        .ok_or_else(|| DebeziumError::new("missing op code"))?;

    let source_row = match op {
        "c" | "r" | "u" => payload.get("after"),
        "d" => payload.get("before"),
        other => {
            return Err(DebeziumError::new(format!(
                "unsupported op code `{other}`"
            ))
            .into())
        }
    };

    let row = source_row
        .cloned()
        .ok_or_else(|| DebeziumError::new("envelope missing before/after"))?;

    if let Value::Object(mut obj) = row {
        obj.insert("operation_type".to_string(), Value::String(op.to_string()));
        Ok(Value::Object(obj))
    } else {
        Err(DebeziumError::new("before/after payload is not an object").into())
    }
}

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
                    )))
                }
            };
        }
    }
    Ok(())
}
