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
    #[serde(default)]
    pub retry_count: Option<u32>,
}

impl MessageContext {
    #[allow(dead_code)]
    pub fn new(
        topic: String,
        partition: i32,
        offset: i64,
        timestamp: i64,
        retry_count: Option<u32>,
    ) -> Self {
        Self {
            topic,
            partition,
            offset,
            timestamp,
            retry_count,
        }
    }

    /// creates a correlation identifier combining the message's topic, partition, and offset.
    ///
    /// # Returns
    ///
    /// A `String` in the format `<topic>:<partition>:<offset>`.
    ///
    /// # Examples
    ///
    /// ```
    /// let ctx = MessageContext::new("topic".to_string(), 1, 42, 0);
    /// assert_eq!(ctx.correlation_id(), "topic:1:42");
    /// ```
    pub fn correlation_id(&self) -> String {
        format!("{}:{}:{}", self.topic, self.partition, self.offset)
    }
}

/// Multi-tenancy context for message processing
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct TenantContext {
    pub tenant_id: String,
}

impl TenantContext {
    pub fn new(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_try_from_and_display() {
        assert_eq!(Operation::try_from("c").unwrap(), Operation::Create);
        assert_eq!(Operation::try_from("r").unwrap(), Operation::Read);
        assert_eq!(Operation::try_from("u").unwrap(), Operation::Update);
        assert_eq!(Operation::try_from("d").unwrap(), Operation::Delete);

        let op = Operation::Create;
        assert_eq!(op.to_string(), "c");
    }

    #[test]
    fn test_operation_try_from_invalid() {
        let err = Operation::try_from("x").unwrap_err();
        assert_eq!(err.input, "x");
    }

    #[test]
    fn test_message_context_correlation_id() {
        let ctx = MessageContext::new("topic".to_string(), 1, 42, 0, None);
        assert_eq!(ctx.correlation_id(), "topic:1:42");
    }

    #[test]
    fn test_tenant_context_creation() {
        let tenant = TenantContext::new("tenant-123");
        assert_eq!(tenant.tenant_id, "tenant-123");
    }

    #[test]
    fn test_tenant_context_equality() {
        let tenant1 = TenantContext::new("tenant-123");
        let tenant2 = TenantContext::new("tenant-123");
        assert_eq!(tenant1, tenant2);
    }
}
