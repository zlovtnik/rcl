use crate::eip::{Stage, StageContext, StageError, StageResult};
use crate::errors::{ProcessingError, ValidationError};
use anyhow::Result;
use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, info};

/// Filter Stage - Skip messages that don't match criteria
#[derive(Debug)]
pub struct FilterStage {
    #[allow(dead_code)]
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
#[derive(Debug)]
pub struct TransformerStage {
    #[allow(dead_code)]
    name: String,
    transformations: Vec<Transformation>,
}

#[derive(Clone, Debug)]
enum Transformation {
    Rename {
        from: String,
        to: String,
    },
    Convert {
        field: String,
        converter: Converter,
    },
    AddField {
        name: String,
        value: ValueGenerator,
    },
    RemoveField {
        name: String,
    },
    Flatten {
        field: String,
        prefix: Option<String>,
    },
    Script {
        #[allow(dead_code)]
        engine: ScriptEngine,
        #[allow(dead_code)]
        code: String,
    },
}

#[allow(clippy::enum_variant_names)]
#[derive(Clone, Debug)]
enum Converter {
    ToDecimal,
    ToInteger,
    ToString,
    UnixToIso8601,
    Iso8601ToUnix,
}

#[derive(Clone, Debug)]
enum ScriptEngine {
    Rhai,
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
                    "unix_to_iso8601" => Converter::UnixToIso8601,
                    "iso8601_to_unix" => Converter::Iso8601ToUnix,
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
            "flatten" => {
                let field = config
                    .get("field")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("missing field in flatten"))?
                    .to_string();
                let prefix = config
                    .get("prefix")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                Ok(Transformation::Flatten { field, prefix })
            }
            "script" => {
                let engine_str = config
                    .get("engine")
                    .and_then(|v| v.as_str())
                    .unwrap_or("rhai");
                let engine = match engine_str {
                    "rhai" => ScriptEngine::Rhai,
                    other => return Err(anyhow::anyhow!("unsupported script engine: {}", other)),
                };
                let code = config
                    .get("code")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("missing code in script"))?
                    .to_string();
                Ok(Transformation::Script { engine, code })
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
            Transformation::Flatten { field, prefix } => {
                if let Some(Value::Object(obj)) = remove_field(msg, field).map_err(|e| {
                    ProcessingError::Stage(StageError::new("field_error", e.to_string()))
                })? {
                    for (k, v) in obj {
                        let new_key = if let Some(p) = prefix {
                            format!("{}{}", p, k)
                        } else {
                            k
                        };
                        set_field(msg, &new_key, v).map_err(|e| {
                            ProcessingError::Stage(StageError::new("field_error", e.to_string()))
                        })?;
                    }
                }
            }
            Transformation::Script { .. } => {
                return Err(ProcessingError::Stage(StageError::new(
                    "not_implemented",
                    "Scripting not supported yet",
                )));
            }
        }
        Ok(())
    }
}

impl Converter {
    /// Convert a JSON `Value` into the type indicated by this `Converter`.
    ///
    /// `ToDecimal` expects a string containing a floating-point number and produces
    /// `Value::Number` from that decimal; it fails if parsing fails or the value is
    /// NaN or infinite. `ToInteger` expects a string containing an integer and
    /// produces `Value::Number` from that integer; it fails if parsing fails.
    /// `ToString` returns the string representation of the input value.
    ///
    /// # Returns
    ///
    /// `Ok(Value)` with the converted JSON value on success, `Err(ProcessingError)`
    /// if the input type is not supported for the selected conversion or if parsing
    /// the numeric value fails.
    ///
    /// # Examples
    ///
    /// ```
    /// use serde_json::json;
    ///
    /// // ToDecimal
    /// let conv = crate::stages::Converter::ToDecimal;
    /// let res = conv.convert(json!("3.14")).unwrap();
    /// assert!(res.is_number());
    ///
    /// // ToInteger
    /// let conv = crate::stages::Converter::ToInteger;
    /// let res = conv.convert(json!("42")).unwrap();
    /// assert!(res.is_number());
    ///
    /// // ToString
    /// let conv = crate::stages::Converter::ToString;
    /// let res = conv.convert(json!(true)).unwrap();
    /// assert_eq!(res, json!("true"));
    /// ```
    fn convert(&self, value: Value) -> Result<Value, ProcessingError> {
        match self {
            Converter::ToDecimal => {
                if let Value::String(s) = value {
                    let f: f64 = s.parse().map_err(|e| {
                        ProcessingError::Stage(StageError::new(
                            "conversion_error",
                            format!("Parse decimal error: {}", e),
                        ))
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
                        ProcessingError::Stage(StageError::new(
                            "conversion_error",
                            format!("Parse integer error: {}", e),
                        ))
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
            Converter::UnixToIso8601 => {
                let ts = match value {
                    Value::Number(n) => n.as_i64().ok_or_else(|| {
                        ProcessingError::Stage(StageError::new(
                            "conversion_error",
                            "Invalid unix timestamp",
                        ))
                    })?,
                    Value::String(s) => s.parse::<i64>().map_err(|e| {
                        ProcessingError::Stage(StageError::new(
                            "conversion_error",
                            format!("Parse unix timestamp error: {}", e),
                        ))
                    })?,
                    _ => {
                        return Err(ProcessingError::Stage(StageError::new(
                            "conversion_error",
                            "cannot convert to iso8601",
                        )));
                    }
                };
                let dt = chrono::DateTime::from_timestamp(ts, 0).ok_or_else(|| {
                    ProcessingError::Stage(StageError::new(
                        "conversion_error",
                        "Invalid unix timestamp",
                    ))
                })?;
                Ok(Value::String(dt.to_rfc3339()))
            }
            Converter::Iso8601ToUnix => {
                if let Value::String(s) = value {
                    let dt = chrono::DateTime::parse_from_rfc3339(&s).map_err(|e| {
                        ProcessingError::Stage(StageError::new(
                            "conversion_error",
                            format!("Parse iso8601 error: {}", e),
                        ))
                    })?;
                    Ok(Value::Number(serde_json::Number::from(dt.timestamp())))
                } else {
                    Err(ProcessingError::Stage(StageError::new(
                        "conversion_error",
                        "cannot convert to unix timestamp",
                    )))
                }
            }
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
                    .as_secs() as i64;
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
        debug!(pipeline = %ctx.pipeline_name, context = %ctx.correlation_id, transformations = self.transformations.len(), "starting transformer stage");

        for (idx, transform) in self.transformations.iter().enumerate() {
            match transform {
                crate::stages::Transformation::Rename { from, to } => {
                    debug!(pipeline = %ctx.pipeline_name, context = %ctx.correlation_id, idx = idx, from = %from, to = %to, "renaming field");
                    transform.apply(&mut msg, ctx)?;
                    info!(pipeline = %ctx.pipeline_name, context = %ctx.correlation_id, from = %from, to = %to, "field renamed");
                }
                crate::stages::Transformation::Convert { field, .. } => {
                    debug!(pipeline = %ctx.pipeline_name, context = %ctx.correlation_id, idx = idx, field = %field, "converting field");
                    transform.apply(&mut msg, ctx)?;
                    if let Some(converted_val) = msg.get(field) {
                        info!(pipeline = %ctx.pipeline_name, context = %ctx.correlation_id, field = %field, value = %converted_val, "field converted");
                    }
                }
                _ => {
                    transform.apply(&mut msg, ctx)?;
                }
            }
        }

        info!(pipeline = %ctx.pipeline_name, context = %ctx.correlation_id, "transformer stage completed");
        Ok(StageResult::Continue(msg))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Router Stage - Route messages to different destinations
#[derive(Debug)]
pub struct RouterStage {
    #[allow(dead_code)]
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
#[derive(Debug)]
pub struct SplitterStage {
    #[allow(dead_code)]
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
            if let Value::Object(map) = current {
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
            if let Value::Object(map) = current {
                // Check if the key exists and is not an object
                #[allow(clippy::collapsible_if)]
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
            if let Value::Object(map) = current {
                return Ok(map.remove(*part));
            } else {
                return Ok(None);
            }
        } else {
            // Intermediate part
            if let Value::Object(map) = current {
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
    if let (Value::Object(base_map), Value::Object(other_map)) = (&mut base, other) {
        for (k, v) in other_map {
            base_map.insert(k, v);
        }
    }
    base
}

/// Idempotent Receiver Stage - Detect and skip duplicate messages
#[derive(Clone, Debug)]
enum FallbackMode {
    Pass,
    Fail,
}

#[async_trait]
trait DeduplicationStorage: Send + Sync {
    async fn check_and_set(&self, key: &str, ttl: Duration) -> Result<bool>;
}

struct RedisStorage {
    client: redis::Client,
    connection: tokio::sync::OnceCell<MultiplexedConnection>,
}

#[async_trait]
impl DeduplicationStorage for RedisStorage {
    /// Atomically records a key in Redis with a TTL and indicates whether the key was newly created.
    ///
    /// Attempts `SET key 1 NX EX <ttl>` and returns whether the set succeeded (meaning the key did not exist).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::time::Duration;
    /// // Assume `storage` is a RedisStorage instance available in scope.
    /// # async fn example(storage: &crate::RedisStorage) -> Result<(), Box<dyn std::error::Error>> {
    /// let created = storage.check_and_set("message:123", Duration::from_secs(60)).await?;
    /// if created {
    ///     println!("key was set (new)"); // `true` branch
    /// } else {
    ///     println!("key already existed"); // `false` branch
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Returns `true` if the key was set (did not previously exist), `false` otherwise.
    async fn check_and_set(&self, key: &str, ttl: Duration) -> Result<bool> {
        let conn = self
            .connection
            .get_or_try_init(|| async { self.client.get_multiplexed_async_connection().await })
            .await?;

        let mut conn = conn.clone();
        let result: Option<String> = redis::cmd("SET")
            .arg(key)
            .arg("1")
            .arg("NX")
            .arg("EX")
            .arg(ttl.as_secs())
            .query_async(&mut conn)
            .await?;
        Ok(result.is_some())
    }
}

pub struct IdempotentReceiverStage {
    #[allow(dead_code)]
    name: String,
    key_field: String,
    storage: Box<dyn DeduplicationStorage>,
    ttl: Duration,
    fallback: FallbackMode,
}

impl IdempotentReceiverStage {
    /// Constructs an IdempotentReceiverStage from a JSON configuration.
    ///
    /// The configuration must contain a `key_field` (string) used to extract the deduplication key from messages.
    /// Optional fields:
    /// - `ttl_seconds` (u64): time-to-live for deduplication keys in seconds (default 86400).
    /// - `storage` (string): deduplication backend; currently `"redis"` is supported (default `"redis"`).
    /// - `redis_url` (string): Redis connection URL (default `"redis://localhost:6379"`).
    /// - `fallback_on_error` (string): behavior when storage errors occur; `"pass"` (default) or `"fail"`.
    ///
    /// # Errors
    ///
    /// Returns an `Err` if `key_field` is missing or if an unsupported `storage` type is provided,
    /// or if creating the configured storage backend fails (for example, invalid Redis URL).
    ///
    /// # Examples
    ///
    /// ```
    /// use serde_json::json;
    ///
    /// let cfg = json!({
    ///     "key_field": "message_id",
    ///     "ttl_seconds": 3600,
    ///     "storage": "redis",
    ///     "redis_url": "redis://127.0.0.1:6379",
    ///     "fallback_on_error": "pass"
    /// });
    /// let stage = IdempotentReceiverStage::from_config("idemp".to_string(), cfg).unwrap();
    /// assert_eq!(stage.name(), "idemp");
    /// ```
    pub fn from_config(name: String, config: Value) -> Result<Self> {
        let key_field = config
            .get("key_field")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("key_field is required"))?
            .to_string();

        let ttl_seconds = config
            .get("ttl_seconds")
            .and_then(|v| v.as_u64())
            .unwrap_or(86400);

        let ttl = Duration::from_secs(ttl_seconds);

        let storage_type = config
            .get("storage")
            .and_then(|v| v.as_str())
            .unwrap_or("redis");

        let storage: Box<dyn DeduplicationStorage> = match storage_type {
            "redis" => {
                let redis_url = config
                    .get("redis_url")
                    .and_then(|v| v.as_str())
                    .unwrap_or("redis://localhost:6379");
                let client = redis::Client::open(redis_url)?;
                Box::new(RedisStorage {
                    client,
                    connection: tokio::sync::OnceCell::new(),
                })
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "unsupported storage type: {}",
                    storage_type
                ));
            }
        };

        let fallback = match config
            .get("fallback_on_error")
            .and_then(|v| v.as_str())
            .unwrap_or("pass")
        {
            "pass" => FallbackMode::Pass,
            "fail" => FallbackMode::Fail,
            _ => FallbackMode::Pass,
        };

        Ok(Self {
            name,
            key_field,
            storage,
            ttl,
            fallback,
        })
    }
}

#[async_trait]
impl Stage for IdempotentReceiverStage {
    /// Deduplicates a message by checking and recording its configured key and then deciding the stage outcome.
    ///
    /// Extracts the key from the configured `key_field` in the message; if the key is missing or not a string, returns a validation error.
    /// If the storage reports the key as new, continues with the message; if the key is already present, the message is skipped.
    /// If the storage operation fails, behavior is controlled by the stage's `fallback`:
    /// - `FallbackMode::Pass` logs a warning and continues with the message.
    /// - `FallbackMode::Fail` returns a stage processing error.
    ///
    /// # Returns
    ///
    /// `StageResult::Continue(msg)` if the message is considered new or fallback permits passing, `StageResult::Skip` if a duplicate was detected, or an appropriate `ProcessingError` on validation failure or fallback-triggered failure.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use tokio::runtime::Runtime;
    /// # async fn _example(stage: &impl crate::stages::Stage, ctx: &crate::stages::StageContext, msg: serde_json::Value) {
    /// let result = stage.process(ctx, msg).await;
    /// match result {
    ///     Ok(crate::stages::StageResult::Continue(_)) => println!("message accepted"),
    ///     Ok(crate::stages::StageResult::Skip) => println!("duplicate message skipped"),
    ///     Err(e) => eprintln!("processing error: {}", e),
    /// }
    /// # }
    /// ```
    async fn process(
        &self,
        _ctx: &StageContext,
        msg: Value,
    ) -> Result<StageResult, ProcessingError> {
        let key = match get_field(&msg, &self.key_field) {
            Some(Value::String(field_value)) => field_value.clone(),
            _ => {
                return Err(ProcessingError::Validation(ValidationError::new(format!(
                    "key field '{}' not found or not a string",
                    self.key_field
                ))));
            }
        };

        match self.storage.check_and_set(&key, self.ttl).await {
            Ok(is_new) => {
                if is_new {
                    Ok(StageResult::Continue(msg))
                } else {
                    Ok(StageResult::Skip)
                }
            }
            Err(e) => match self.fallback {
                FallbackMode::Pass => {
                    tracing::warn!("deduplication check failed, passing message: {}", e);
                    Ok(StageResult::Continue(msg))
                }
                FallbackMode::Fail => Err(ProcessingError::Stage(StageError::new(
                    "deduplication check failed",
                    e.to_string(),
                ))),
            },
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
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
    async fn test_filter_stage_include_skip() {
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

    #[tokio::test]
    async fn test_transformer_conversions_and_add_remove() {
        let stage = TransformerStage {
            name: "test".to_string(),
            transformations: vec![
                Transformation::Convert {
                    field: "n1".to_string(),
                    converter: Converter::ToDecimal,
                },
                Transformation::Convert {
                    field: "n2".to_string(),
                    converter: Converter::ToInteger,
                },
                Transformation::Convert {
                    field: "x".to_string(),
                    converter: Converter::ToString,
                },
                Transformation::AddField {
                    name: "now_ts".to_string(),
                    value: ValueGenerator::Now,
                },
                Transformation::RemoveField {
                    name: "remove_me".to_string(),
                },
            ],
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

        let msg = json!({"n1": "3.14", "n2": "42", "x": 10, "remove_me": "bye"});
        let result = stage.process(&ctx, msg).await.unwrap();
        match result {
            StageResult::Continue(new_msg) => {
                // n1 should be number
                assert!(new_msg.get("n1").and_then(|v| v.as_f64()).is_some());
                // n2 should be integer number
                assert!(new_msg.get("n2").and_then(|v| v.as_i64()).is_some());
                // x should be string
                assert!(new_msg.get("x").and_then(|v| v.as_str()).is_some());
                // now_ts exists
                assert!(new_msg.get("now_ts").is_some());
                // remove_me removed
                assert!(new_msg.get("remove_me").is_none());
            }
            _ => panic!("Expected Continue result"),
        }
    }

    #[tokio::test]
    async fn test_condition_operators() {
        // contains
        let cond = Condition {
            field: "a".to_string(),
            operator: Operator::Contains("ell".to_string()),
        };
        assert!(cond.evaluate(&json!({"a": "hello"})));

        // regex
        let cond = Condition {
            field: "a".to_string(),
            operator: Operator::Regex(regex::Regex::new("^h.*o$").unwrap()),
        };
        assert!(cond.evaluate(&json!({"a": "hello"})));

        // in
        let cond = Condition {
            field: "a".to_string(),
            operator: Operator::In(vec![json!(1), json!(2), json!(3)]),
        };
        assert!(cond.evaluate(&json!({"a": 2})));

        // greater/less than
        let cond = Condition {
            field: "n".to_string(),
            operator: Operator::GreaterThan(5.0),
        };
        assert!(cond.evaluate(&json!({"n": 6})));
        let cond = Condition {
            field: "n".to_string(),
            operator: Operator::LessThan(5.0),
        };
        assert!(cond.evaluate(&json!({"n": 4})));

        // exists
        let cond = Condition {
            field: "z".to_string(),
            operator: Operator::Exists,
        };
        assert!(cond.evaluate(&json!({"z": null}))); // exists even if null
    }

    struct MockStorage {
        ret: Result<bool, String>,
    }

    #[async_trait]
    impl DeduplicationStorage for MockStorage {
        /// Return the mocked check-and-set outcome stored in `self.ret`.
        ///
        /// This async method maps an internal `Result<bool, String>` (`self.ret`) to
        /// `Result<bool, anyhow::Error>`: if `self.ret` is `Ok(b)` it yields `Ok(b)`,
        /// otherwise it yields an `Err` wrapping the stored error string.
        ///
        /// # Examples
        ///
        /// ```
        /// // assumed: `storage` implements the method shown and is available in scope.
        /// // Await the mocked result and unwrap the boolean success indicator:
        /// let was_new = storage.check_and_set("key", std::time::Duration::from_secs(60)).await.unwrap();
        /// assert!(was_new == true || was_new == false);
        /// ```
        async fn check_and_set(&self, _key: &str, _ttl: std::time::Duration) -> Result<bool> {
            match &self.ret {
                Ok(b) => Ok(*b),
                Err(s) => Err(anyhow::anyhow!(s.clone())),
            }
        }
    }

    #[tokio::test]
    async fn test_idempotent_receiver_stage_pass_and_skip() {
        let storage_new = MockStorage { ret: Ok(true) };
        let stage = IdempotentReceiverStage {
            name: "idemp".to_string(),
            key_field: "id".to_string(),
            storage: Box::new(storage_new),
            ttl: std::time::Duration::from_secs(60),
            fallback: FallbackMode::Pass,
        };

        let ctx = StageContext {
            correlation_id: "c".to_string(),
            pipeline_name: "p".to_string(),
            message_metadata: crate::eip::MessageMetadata::from_kafka("t".to_string(), 0, 0, None),
        };

        let msg = json!({"id": "abc"});
        let res = stage.process(&ctx, msg.clone()).await.unwrap();
        assert!(matches!(res, StageResult::Continue(_)));

        let storage_dup = MockStorage { ret: Ok(false) };
        let stage2 = IdempotentReceiverStage {
            storage: Box::new(storage_dup),
            ..stage
        };
        let res2 = stage2.process(&ctx, msg).await.unwrap();
        assert!(matches!(res2, StageResult::Skip));
    }

    #[tokio::test]
    async fn test_idempotent_receiver_stage_error_fallback() {
        let storage_err: MockStorage = MockStorage {
            ret: Err("boom".to_string()),
        };
        let stage = IdempotentReceiverStage {
            name: "idemp".to_string(),
            key_field: "id".to_string(),
            storage: Box::new(storage_err),
            ttl: std::time::Duration::from_secs(60),
            fallback: FallbackMode::Pass,
        };

        let ctx = StageContext {
            correlation_id: "c".to_string(),
            pipeline_name: "p".to_string(),
            message_metadata: crate::eip::MessageMetadata::from_kafka("t".to_string(), 0, 0, None),
        };

        let msg = json!({"id": "abc"});
        // With fallback=Pass, error should result in Continue
        let res = stage.process(&ctx, msg).await.unwrap();
        assert!(matches!(res, StageResult::Continue(_)));
    }

    #[test]
    fn test_filter_stage_from_config_invalid_mode() {
        let config = json!({
            "mode": "invalid_mode",
            "conditions": [
                {
                    "field": "status",
                    "equals": "active"
                }
            ],
            "logic": "AND"
        });
        let result = FilterStage::from_config("test".to_string(), config);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid filter mode")
        );
    }

    #[test]
    fn test_filter_stage_from_config_invalid_logic() {
        let config = json!({
            "mode": "include",
            "conditions": [
                {
                    "field": "status",
                    "equals": "active"
                }
            ],
            "logic": "INVALID"
        });
        let result = FilterStage::from_config("test".to_string(), config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid logic"));
    }

    #[test]
    fn test_filter_stage_from_config_missing_conditions() {
        let config = json!({
            "mode": "include",
            "logic": "AND"
        });
        let result = FilterStage::from_config("test".to_string(), config);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("missing conditions")
        );
    }

    #[test]
    fn test_filter_stage_from_config_missing_field() {
        let config = json!({
            "mode": "include",
            "conditions": [
                {
                    "equals": "active"
                }
            ],
            "logic": "AND"
        });
        let result = FilterStage::from_config("test".to_string(), config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("missing field"));
    }

    #[test]
    fn test_filter_stage_from_config_no_valid_operator() {
        let config = json!({
            "mode": "include",
            "conditions": [
                {
                    "field": "status"
                }
            ],
            "logic": "AND"
        });
        let result = FilterStage::from_config("test".to_string(), config);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("no valid operator")
        );
    }

    #[test]
    fn test_filter_stage_from_config_invalid_regex() {
        let config = json!({
            "mode": "include",
            "conditions": [
                {
                    "field": "status",
                    "regex": "["
                }
            ],
            "logic": "AND"
        });
        let result = FilterStage::from_config("test".to_string(), config);
        assert!(result.is_err());
    }

    #[test]
    fn test_transformer_stage_from_config_invalid_type() {
        let config = json!({
            "transformations": [
                {
                    "type": "unknown_type",
                    "name": "field"
                }
            ]
        });
        let result = TransformerStage::from_config("test".to_string(), config);
        assert!(result.is_err());
    }

    #[test]
    fn test_router_stage_from_config_missing_field() {
        let config = json!({
            "routes": {
                "a": "b"
            },
            "default": "c"
        });
        let result = RouterStage::from_config("test".to_string(), config);
        assert!(result.is_err());
    }

    #[test]
    fn test_splitter_stage_from_config_missing_field() {
        let config = json!({
            "array_size_limit": 100
        });
        let result = SplitterStage::from_config("test".to_string(), config);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_condition_operators_all_variants() {
        let _ctx = StageContext {
            correlation_id: "test".to_string(),
            pipeline_name: "test".to_string(),
            message_metadata: crate::eip::MessageMetadata::from_kafka(
                "test".to_string(),
                0,
                0,
                None,
            ),
        };

        // Test NotEquals
        let msg = json!({"status": "inactive"});
        let cond = Condition {
            field: "status".to_string(),
            operator: Operator::NotEquals(json!("active")),
        };
        assert!(cond.evaluate(&msg));

        // Test GreaterThan
        let msg = json!({"age": 25});
        let cond = Condition {
            field: "age".to_string(),
            operator: Operator::GreaterThan(20.0),
        };
        assert!(cond.evaluate(&msg));

        // Test LessThan
        let msg = json!({"age": 15});
        let cond = Condition {
            field: "age".to_string(),
            operator: Operator::LessThan(20.0),
        };
        assert!(cond.evaluate(&msg));

        // Test Contains
        let msg = json!({"description": "hello world"});
        let cond = Condition {
            field: "description".to_string(),
            operator: Operator::Contains("world".to_string()),
        };
        assert!(cond.evaluate(&msg));

        // Test Regex
        let regex = Regex::new("^[0-9]+$").unwrap();
        let msg = json!({"value": "12345"});
        let cond = Condition {
            field: "value".to_string(),
            operator: Operator::Regex(regex),
        };
        assert!(cond.evaluate(&msg));

        // Test Exists
        let msg = json!({"field": null});
        let cond = Condition {
            field: "field".to_string(),
            operator: Operator::Exists,
        };
        assert!(cond.evaluate(&msg));

        // Test In
        let msg = json!({"status": "active"});
        let cond = Condition {
            field: "status".to_string(),
            operator: Operator::In(vec![json!("active"), json!("pending")]),
        };
        assert!(cond.evaluate(&msg));
    }
}
