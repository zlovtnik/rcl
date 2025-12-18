use crate::eip::{Stage, StageContext, StageError, StageResult};
use crate::errors::ProcessingError;
use anyhow::Result;
use async_trait::async_trait;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;

/// Filter Stage - Skip messages that don't match criteria
pub struct FilterStage {
    name: String,
    mode: FilterMode,
    conditions: Vec<Condition>,
    logic: Logic,
}

#[derive(Clone, Debug)]
enum FilterMode {
    Include,
    Exclude,
}

#[derive(Clone, Debug)]
enum Logic {
    And,
    Or,
}

#[derive(Clone, Debug)]
struct Condition {
    field: String,
    operator: Operator,
}

#[derive(Clone, Debug)]
enum Operator {
    Equals(Value),
    NotEquals(Value),
    GreaterThan(f64),
    LessThan(f64),
    Contains(String),
    Regex(Regex),
    Exists,
    In(Vec<Value>),
}

impl FilterStage {
    pub fn from_config(name: String, config: Value) -> Result<Self> {
        let mode = match config.get("mode").and_then(|v| v.as_str()) {
            Some("include") | None => FilterMode::Include,
            Some("exclude") => FilterMode::Exclude,
            Some(other) => return Err(anyhow::anyhow!("invalid filter mode: {}", other)),
        };

        let logic = match config.get("logic").and_then(|v| v.as_str()) {
            Some("OR") => Logic::Or,
            Some("AND") | None => Logic::And,
            Some(other) => return Err(anyhow::anyhow!("invalid logic: {}", other)),
        };

        let conditions_config = config
            .get("conditions")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow::anyhow!("missing conditions array"))?;

        let mut conditions = Vec::new();
        for cond_config in conditions_config {
            let field = cond_config
                .get("field")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("missing field in condition"))?
                .to_string();

            let operator = Self::parse_operator(cond_config)?;
            conditions.push(Condition { field, operator });
        }

        Ok(Self {
            name,
            mode,
            conditions,
            logic,
        })
    }

    fn parse_operator(config: &Value) -> Result<Operator> {
        if let Some(eq_val) = config.get("equals") {
            return Ok(Operator::Equals(eq_val.clone()));
        }
        if let Some(neq_val) = config.get("not_equals") {
            return Ok(Operator::NotEquals(neq_val.clone()));
        }
        if let Some(gt) = config.get("greater_than").and_then(|v| v.as_f64()) {
            return Ok(Operator::GreaterThan(gt));
        }
        if let Some(lt) = config.get("less_than").and_then(|v| v.as_f64()) {
            return Ok(Operator::LessThan(lt));
        }
        if let Some(contains) = config.get("contains").and_then(|v| v.as_str()) {
            return Ok(Operator::Contains(contains.to_string()));
        }
        if let Some(regex_str) = config.get("regex").and_then(|v| v.as_str()) {
            let regex = Regex::new(regex_str)?;
            return Ok(Operator::Regex(regex));
        }
        if config.get("exists").is_some() {
            return Ok(Operator::Exists);
        }
        if let Some(in_array) = config.get("in").and_then(|v| v.as_array()) {
            return Ok(Operator::In(in_array.clone()));
        }

        Err(anyhow::anyhow!("no valid operator found in condition"))
    }

    fn evaluate(&self, msg: &Value) -> Result<bool, ProcessingError> {
        let results: Vec<bool> = self.conditions.iter().map(|c| c.evaluate(msg)).collect();

        match self.logic {
            Logic::And => Ok(results.iter().all(|&b| b)),
            Logic::Or => Ok(results.iter().any(|&b| b)),
        }
    }
}

impl Condition {
    fn evaluate(&self, msg: &Value) -> bool {
        let field_value = get_field(msg, &self.field);

        match &self.operator {
            Operator::Equals(expected) => field_value == Some(expected),
            Operator::NotEquals(expected) => field_value != Some(expected),
            Operator::GreaterThan(thresh) => {
                if let Some(Value::Number(n)) = field_value {
                    if let Some(num_val) = n.as_f64() {
                        num_val > *thresh
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            Operator::LessThan(thresh) => {
                if let Some(Value::Number(n)) = field_value {
                    if let Some(num_val) = n.as_f64() {
                        num_val < *thresh
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            Operator::Contains(substring) => {
                if let Some(Value::String(s)) = field_value {
                    s.contains(substring)
                } else {
                    false
                }
            }
            Operator::Regex(regex) => {
                if let Some(Value::String(s)) = field_value {
                    regex.is_match(s)
                } else {
                    false
                }
            }
            Operator::Exists => field_value.is_some(),
            Operator::In(values) => field_value.is_some_and(|v| values.contains(v)),
        }
    }
}

#[async_trait]
impl Stage for FilterStage {
    async fn process(
        &self,
        _ctx: &StageContext,
        msg: Value,
    ) -> Result<StageResult, ProcessingError> {
        let matches = self.evaluate(&msg)?;

        let should_pass = match self.mode {
            FilterMode::Include => matches,
            FilterMode::Exclude => !matches,
        };

        if should_pass {
            Ok(StageResult::Continue(msg))
        } else {
            Ok(StageResult::Skip)
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Transformer Stage - Modify message structure and content
pub struct TransformerStage {
    name: String,
    transformations: Vec<Transformation>,
}

#[derive(Clone, Debug)]
enum Transformation {
    Rename { from: String, to: String },
    Convert { field: String, converter: Converter },
    AddField { name: String, value: ValueGenerator },
    RemoveField { name: String },
}

#[derive(Clone, Debug)]
enum Converter {
    ToDecimal,
    ToInteger,
    ToString,
}

#[derive(Clone, Debug)]
enum ValueGenerator {
    Literal(Value),
    Now,
}

impl TransformerStage {
    pub fn from_config(name: String, config: Value) -> Result<Self> {
        let transformations_config = config
            .get("transformations")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow::anyhow!("missing transformations array"))?;

        let mut transformations = Vec::new();
        for trans_config in transformations_config {
            let transformation = Self::parse_transformation(trans_config)?;
            transformations.push(transformation);
        }

        Ok(Self {
            name,
            transformations,
        })
    }

    fn parse_transformation(config: &Value) -> Result<Transformation> {
        let type_str = config
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing type in transformation"))?;

        match type_str {
            "rename" => {
                let from = config
                    .get("from")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("missing from in rename"))?
                    .to_string();
                let to = config
                    .get("to")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("missing to in rename"))?
                    .to_string();
                Ok(Transformation::Rename { from, to })
            }
            "convert" => {
                let field = config
                    .get("field")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("missing field in convert"))?
                    .to_string();
                let to = config
                    .get("to")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("missing to in convert"))?;
                let converter = match to {
                    "decimal" => Converter::ToDecimal,
                    "integer" => Converter::ToInteger,
                    "string" => Converter::ToString,
                    other => return Err(anyhow::anyhow!("unknown converter: {}", other)),
                };
                Ok(Transformation::Convert { field, converter })
            }
            "add_field" => {
                let name = config
                    .get("name")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("missing name in add_field"))?
                    .to_string();
                let value = if config.get("value").and_then(|v| v.as_str()) == Some("{{now}}") {
                    ValueGenerator::Now
                } else if let Some(literal) = config.get("value") {
                    ValueGenerator::Literal(literal.clone())
                } else {
                    return Err(anyhow::anyhow!("invalid value in add_field"));
                };
                Ok(Transformation::AddField { name, value })
            }
            "remove_field" => {
                let name = config
                    .get("name")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("missing name in remove_field"))?
                    .to_string();
                Ok(Transformation::RemoveField { name })
            }
            other => Err(anyhow::anyhow!("unknown transformation type: {}", other)),
        }
    }
}

impl Transformation {
    fn apply(&self, msg: &mut Value, ctx: &StageContext) -> Result<(), ProcessingError> {
        match self {
            Transformation::Rename { from, to } => {
                if let Some(value) = remove_field(msg, from).map_err(|e| {
                    ProcessingError::Stage(StageError::new("field_error", e.to_string()))
                })? {
                    set_field(msg, to, value).map_err(|e| {
                        ProcessingError::Stage(StageError::new("field_error", e.to_string()))
                    })?;
                }
            }
            Transformation::Convert { field, converter } => {
                if let Some(value) = get_field(msg, field).cloned() {
                    let converted = converter.convert(value)?;
                    set_field(msg, field, converted).map_err(|e| {
                        ProcessingError::Stage(StageError::new("field_error", e.to_string()))
                    })?;
                }
            }
            Transformation::AddField { name, value } => {
                let generated = value.generate(ctx)?;
                set_field(msg, name, generated).map_err(|e| {
                    ProcessingError::Stage(StageError::new("field_error", e.to_string()))
                })?;
            }
            Transformation::RemoveField { name } => {
                remove_field(msg, name).map_err(|e| {
                    ProcessingError::Stage(StageError::new("field_error", e.to_string()))
                })?;
            }
        }
        Ok(())
    }
}

impl Converter {
    fn convert(&self, value: Value) -> Result<Value, ProcessingError> {
        match self {
            Converter::ToDecimal => {
                if let Value::String(s) = value {
                    let f: f64 = s.parse().map_err(|e| {
                        ProcessingError::Stage(
                            StageError::new(
                                "conversion_error",
                                format!("Parse decimal error: {}", e),
                            )
                            .retryable(true),
                        )
                    })?;
                    let num = serde_json::Number::from_f64(f).ok_or_else(|| {
                        ProcessingError::Stage(StageError::new(
                            "conversion_error",
                            "Invalid decimal: NaN or infinity",
                        ))
                    })?;
                    Ok(Value::Number(num))
                } else {
                    Err(ProcessingError::Stage(StageError::new(
                        "conversion_error",
                        "cannot convert to decimal",
                    )))
                }
            }
            Converter::ToInteger => {
                if let Value::String(s) = value {
                    let i: i64 = s.parse().map_err(|e| {
                        ProcessingError::Stage(
                            StageError::new(
                                "conversion_error",
                                format!("Parse integer error: {}", e),
                            )
                            .retryable(true),
                        )
                    })?;
                    Ok(Value::Number(serde_json::Number::from(i)))
                } else {
                    Err(ProcessingError::Stage(StageError::new(
                        "conversion_error",
                        "cannot convert to integer",
                    )))
                }
            }
            Converter::ToString => Ok(Value::String(value.to_string())),
        }
    }
}

impl ValueGenerator {
    fn generate(&self, _ctx: &StageContext) -> Result<Value, ProcessingError> {
        match self {
            ValueGenerator::Literal(v) => Ok(v.clone()),
            ValueGenerator::Now => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|e| {
                        ProcessingError::Stage(StageError::new(
                            "time_error",
                            format!("Time error: {}", e),
                        ))
                    })?
                    .as_millis() as i64;
                Ok(Value::Number(serde_json::Number::from(now)))
            }
        }
    }
}

#[async_trait]
impl Stage for TransformerStage {
    async fn process(
        &self,
        ctx: &StageContext,
        mut msg: Value,
    ) -> Result<StageResult, ProcessingError> {
        for transform in &self.transformations {
            transform.apply(&mut msg, ctx)?;
        }
        Ok(StageResult::Continue(msg))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Router Stage - Route messages to different destinations
pub struct RouterStage {
    name: String,
    route_by: String,
    routes: HashMap<String, String>,
    default: Option<String>,
    metadata_field: String,
}

impl RouterStage {
    pub fn from_config(name: String, config: Value) -> Result<Self> {
        let route_by = config
            .get("route_by")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing route_by"))?
            .to_string();

        let routes = config
            .get("routes")
            .and_then(|v| v.as_object())
            .ok_or_else(|| anyhow::anyhow!("missing routes object"))?
            .iter()
            .map(|(k, v)| {
                let dest = v
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("route value must be string"))?;
                Ok((k.clone(), dest.to_string()))
            })
            .collect::<Result<HashMap<_, _>>>()?;

        let default = config
            .get("default")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let metadata_field = config
            .get("metadata_field")
            .and_then(|v| v.as_str())
            .unwrap_or("_destination_table")
            .to_string();

        Ok(Self {
            name,
            route_by,
            routes,
            default,
            metadata_field,
        })
    }
}

#[async_trait]
impl Stage for RouterStage {
    async fn process(
        &self,
        _ctx: &StageContext,
        mut msg: Value,
    ) -> Result<StageResult, ProcessingError> {
        let route_key = get_field(&msg, &self.route_by)
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                ProcessingError::Stage(StageError::new("routing_error", "route key not found"))
            })?;

        let destination = self
            .routes
            .get(route_key)
            .or(self.default.as_ref())
            .ok_or_else(|| {
                ProcessingError::Stage(StageError::new(
                    "routing_error",
                    format!("no route for key: {}", route_key),
                ))
            })?;

        set_field(
            &mut msg,
            &self.metadata_field,
            Value::String(destination.clone()),
        )
        .map_err(|e| ProcessingError::Stage(StageError::new("field_error", e.to_string())))?;

        Ok(StageResult::Continue(msg))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Splitter Stage - Split one message into many
pub struct SplitterStage {
    name: String,
    split_field: String,
    preserve_fields: Vec<String>,
    flatten: bool,
}

impl SplitterStage {
    pub fn from_config(name: String, config: Value) -> Result<Self> {
        let split_field = config
            .get("split_field")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing split_field"))?
            .to_string();

        let preserve_fields = config
            .get("preserve_fields")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let flatten = config
            .get("flatten")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        Ok(Self {
            name,
            split_field,
            preserve_fields,
            flatten,
        })
    }
}

#[async_trait]
impl Stage for SplitterStage {
    async fn process(
        &self,
        _ctx: &StageContext,
        msg: Value,
    ) -> Result<StageResult, ProcessingError> {
        let array = get_field(&msg, &self.split_field)
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                ProcessingError::Stage(StageError::new(
                    "split_error",
                    "split field is not an array",
                ))
            })?;

        let preserved: Value = {
            let mut obj = serde_json::json!({});
            for field in &self.preserve_fields {
                if let Some(value) = get_field(&msg, field) {
                    set_field(&mut obj, field, value.clone()).map_err(|e| {
                        ProcessingError::Stage(StageError::new("field_error", e.to_string()))
                    })?;
                }
            }
            obj
        };

        let mut split_messages = Vec::new();
        for item in array {
            let new_msg = if self.flatten {
                merge_objects(preserved.clone(), item.clone())
            } else {
                let mut obj = preserved.clone();
                set_field(&mut obj, &self.split_field, item.clone()).map_err(|e| {
                    ProcessingError::Stage(StageError::new("field_error", e.to_string()))
                })?;
                obj
            };
            split_messages.push(new_msg);
        }

        Ok(StageResult::Split(split_messages))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Utility functions for JSON manipulation
fn get_field<'a>(value: &'a Value, field: &str) -> Option<&'a Value> {
    let mut current = Some(value);
    for part in field.split('.') {
        current = current.and_then(|v| v.get(part));
    }
    current
}

/// Helper function to get the JSON type name for error messages
fn value_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn set_field(value: &mut Value, field: &str, new_value: Value) -> Result<()> {
    let parts: Vec<&str> = field.split('.').collect();
    if parts.is_empty() {
        return Err(anyhow::anyhow!("empty field path"));
    }

    let mut current = value;
    let mut new_value_opt = Some(new_value);
    for (i, part) in parts.iter().enumerate() {
        if i == parts.len() - 1 {
            // Last part, set the value
            if let Value::Object(ref mut map) = current {
                map.insert((*part).to_string(), new_value_opt.take().unwrap());
            } else {
                return Err(anyhow::anyhow!("cannot set field on non-object"));
            }
        } else {
            // Intermediate part, check that current is an object
            if !current.is_object() {
                return Err(anyhow::anyhow!(
                    "invalid path: cannot traverse through non-object at intermediate segment"
                ));
            }
            if let Value::Object(ref mut map) = current {
                // Check if the key exists and is not an object
                if let Some(existing) = map.get(*part) {
                    if !existing.is_object() {
                        return Err(anyhow::anyhow!(
                            "invalid path: intermediate key '{}' exists but is not an object (type: {})",
                            part,
                            value_type_name(existing)
                        ));
                    }
                }
                // Key either doesn't exist or is already an object, safe to proceed
                map.entry((*part).to_string())
                    .or_insert_with(|| Value::Object(serde_json::Map::new()));
                current = map.get_mut(*part).unwrap();
            }
        }
    }
    Ok(())
}

fn remove_field(value: &mut Value, field: &str) -> Result<Option<Value>> {
    let parts: Vec<&str> = field.split('.').collect();
    if parts.is_empty() {
        return Ok(None);
    }

    let mut current = value;
    for (i, part) in parts.iter().enumerate() {
        if i == parts.len() - 1 {
            // Last part, remove the value
            if let Value::Object(ref mut map) = current {
                return Ok(map.remove(*part));
            } else {
                return Ok(None);
            }
        } else {
            // Intermediate part
            if let Value::Object(ref mut map) = current {
                if let Some(next) = map.get_mut(*part) {
                    current = next;
                } else {
                    return Ok(None);
                }
            } else {
                return Ok(None);
            }
        }
    }
    Ok(None)
}

fn merge_objects(mut base: Value, other: Value) -> Value {
    if let (Value::Object(ref mut base_map), Value::Object(other_map)) = (&mut base, other) {
        for (k, v) in other_map {
            base_map.insert(k, v);
        }
    }
    base
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_filter_stage_include() {
        let stage = FilterStage {
            name: "test".to_string(),
            mode: FilterMode::Include,
            conditions: vec![Condition {
                field: "status".to_string(),
                operator: Operator::Equals(json!("active")),
            }],
            logic: Logic::And,
        };

        let ctx = StageContext {
            correlation_id: "test".to_string(),
            pipeline_name: "test".to_string(),
            message_metadata: crate::eip::MessageMetadata::from_kafka(
                "test".to_string(),
                0,
                0,
                None,
            ),
        };
        let msg = json!({"status": "active", "id": 123});

        let result = stage.process(&ctx, msg).await.unwrap();
        assert!(matches!(result, StageResult::Continue(_)));
    }

    #[tokio::test]
    async fn test_filter_stage_exclude() {
        let stage = FilterStage {
            name: "test".to_string(),
            mode: FilterMode::Include,
            conditions: vec![Condition {
                field: "status".to_string(),
                operator: Operator::Equals(json!("inactive")),
            }],
            logic: Logic::And,
        };

        let ctx = StageContext {
            correlation_id: "test".to_string(),
            pipeline_name: "test".to_string(),
            message_metadata: crate::eip::MessageMetadata::from_kafka(
                "test".to_string(),
                0,
                0,
                None,
            ),
        };
        let msg = json!({"status": "active", "id": 123});

        let result = stage.process(&ctx, msg).await.unwrap();
        assert!(matches!(result, StageResult::Skip));
    }

    #[tokio::test]
    async fn test_transformer_stage_rename() {
        let stage = TransformerStage {
            name: "test".to_string(),
            transformations: vec![Transformation::Rename {
                from: "old_field".to_string(),
                to: "new_field".to_string(),
            }],
        };

        let ctx = StageContext {
            correlation_id: "test".to_string(),
            pipeline_name: "test".to_string(),
            message_metadata: crate::eip::MessageMetadata::from_kafka(
                "test".to_string(),
                0,
                0,
                None,
            ),
        };
        let msg = json!({"old_field": "value", "id": 123});

        let result = stage.process(&ctx, msg).await.unwrap();
        match result {
            StageResult::Continue(new_msg) => {
                assert_eq!(new_msg.get("new_field"), Some(&json!("value")));
                assert!(new_msg.get("old_field").is_none());
            }
            _ => panic!("Expected Continue result"),
        }
    }

    #[tokio::test]
    async fn test_router_stage() {
        let stage = RouterStage {
            name: "test".to_string(),
            route_by: "category".to_string(),
            routes: [("electronics".to_string(), "inventory".to_string())].into(),
            default: Some("general".to_string()),
            metadata_field: "destination".to_string(),
        };

        let ctx = StageContext {
            correlation_id: "test".to_string(),
            pipeline_name: "test".to_string(),
            message_metadata: crate::eip::MessageMetadata::from_kafka(
                "test".to_string(),
                0,
                0,
                None,
            ),
        };
        let msg = json!({"category": "electronics", "id": 123});

        let result = stage.process(&ctx, msg).await.unwrap();
        match result {
            StageResult::Continue(new_msg) => {
                assert_eq!(new_msg.get("destination"), Some(&json!("inventory")));
            }
            _ => panic!("Expected Continue result"),
        }
    }

    #[tokio::test]
    async fn test_splitter_stage() {
        let stage = SplitterStage {
            name: "test".to_string(),
            split_field: "items".to_string(),
            preserve_fields: vec!["order_id".to_string()],
            flatten: false,
        };

        let ctx = StageContext {
            correlation_id: "test".to_string(),
            pipeline_name: "test".to_string(),
            message_metadata: crate::eip::MessageMetadata::from_kafka(
                "test".to_string(),
                0,
                0,
                None,
            ),
        };
        let msg = json!({
            "order_id": 123,
            "items": [{"name": "item1"}, {"name": "item2"}]
        });

        let result = stage.process(&ctx, msg).await.unwrap();
        match result {
            StageResult::Split(messages) => {
                assert_eq!(messages.len(), 2);
                for msg in &messages {
                    assert_eq!(msg.get("order_id"), Some(&json!(123)));
                    assert!(msg.get("items").is_some());
                }
            }
            _ => panic!("Expected Split result"),
        }
    }
}
