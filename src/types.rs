use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Create,
    Read,
    Update,
    Delete,
}

#[derive(Debug, Clone)]
pub struct ParseOperationError {
    pub input: String,
}

impl std::fmt::Display for ParseOperationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid operation: {}", self.input)
    }
}

impl std::error::Error for ParseOperationError {}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Operation::Create => "c",
            Operation::Read => "r",
            Operation::Update => "u",
            Operation::Delete => "d",
        };
        f.write_str(s)
    }
}

impl TryFrom<&str> for Operation {
    type Error = ParseOperationError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "c" => Ok(Operation::Create),
            "r" => Ok(Operation::Read),
            "u" => Ok(Operation::Update),
            "d" => Ok(Operation::Delete),
            _ => Err(ParseOperationError {
                input: s.to_string(),
            }),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessageContext {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
}

impl MessageContext {
    #[allow(dead_code)]
    pub fn new(topic: String, partition: i32, offset: i64, timestamp: i64) -> Self {
        Self {
            topic,
            partition,
            offset,
            timestamp,
        }
    }

    pub fn correlation_id(&self) -> String {
        format!("{}:{}:{}", self.topic, self.partition, self.offset)
    }
}
