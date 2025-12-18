use serde::Serialize;
use serde_json::Error as JsonError;
use sqlx::Error as SqlxError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProcessingError {
    /// Raised when a message is missing the expected payload bytes.
    #[error("missing payload")]
    MissingPayload,
    /// Raised when payload bytes cannot be converted to UTF-8.
    #[error("invalid utf8: {0}")]
    InvalidUtf8(#[from] std::str::Utf8Error),
    /// Raised when JSON parsing fails while decoding the payload.
    #[error("json decode error: {0}")]
    JsonDecode(#[from] JsonError),
    /// Raised when the Debezium envelope is malformed or incomplete.
    #[error("debezium unwrap failed: {0}")]
    Debezium(#[from] DebeziumError),
    /// Raised when a message violates pipeline validation rules.
    #[error("validation failed: {0}")]
    Validation(#[from] ValidationError),
    /// Raised when downstream transport or storage calls fail.
    #[error("transport: {0}")]
    Transport(#[from] TransportError),
}

#[derive(Debug, Error)]
#[error("{message}")]
pub struct DebeziumError {
    message: String,
}

impl DebeziumError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, Error)]
#[error("{message}")]
pub struct ValidationError {
    message: String,
}

impl ValidationError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, Error)]
#[error("{context}: {source}")]
pub struct TransportError {
    context: &'static str,
    #[source]
    source: TransportSource,
}

impl TransportError {
    pub fn new(context: &'static str, source: impl Into<TransportSource>) -> Self {
        Self {
            context,
            source: source.into(),
        }
    }
}

#[derive(Debug, Error)]
pub enum TransportSource {
    #[error(transparent)]
    Sqlx(#[from] SqlxError),
    #[error(transparent)]
    Json(#[from] JsonError),
}

#[derive(Debug, Serialize, Clone, Copy)]
pub struct PublicErrorReason {
    pub code: &'static str,
    pub message: &'static str,
}

impl ProcessingError {
    pub fn public_reason(&self) -> PublicErrorReason {
        match self {
            ProcessingError::MissingPayload => PublicErrorReason {
                code: "missing_payload",
                message: "payload is missing",
            },
            ProcessingError::InvalidUtf8(_) => PublicErrorReason {
                code: "invalid_utf8",
                message: "payload is not valid utf-8",
            },
            ProcessingError::JsonDecode(_) => PublicErrorReason {
                code: "json_decode",
                message: "failed to decode json",
            },
            ProcessingError::Debezium(_) => PublicErrorReason {
                code: "debezium",
                message: "failed to unwrap debezium envelope",
            },
            ProcessingError::Validation(_) => PublicErrorReason {
                code: "validation",
                message: "validation failed",
            },
            ProcessingError::Transport(_) => PublicErrorReason {
                code: "transport",
                message: "transport error",
            },
        }
    }
}
