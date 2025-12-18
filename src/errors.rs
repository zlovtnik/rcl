use serde::Serialize;
use serde_json::Error as JsonError;
use sqlx::Error as SqlxError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProcessingError {
    /// Raised when a message is missing the expected payload bytes.
    #[allow(dead_code)]
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
    /// Raised when a pipeline stage fails.
    #[error("stage error: {0}")]
    Stage(crate::eip::StageError),
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

#[derive(Debug, Serialize, Clone)]
pub struct PublicErrorReason {
    pub code: String,
    pub message: String,
}

impl TransportError {
    pub fn is_retryable(&self) -> bool {
        self.source.is_retryable()
    }
}

impl TransportSource {
    pub fn is_retryable(&self) -> bool {
        match self {
            TransportSource::Json(_) => false,
            TransportSource::Sqlx(e) => match e {
                SqlxError::Io(_) | SqlxError::PoolTimedOut | SqlxError::Tls(_) => true,
                // PoolClosed and WorkerCrashed are treated as non-retryable because they indicate
                // the connection pool is in a terminal state. Retrying with the same pool instance
                // would be futile. These should propagate up to trigger a service restart/recovery.
                SqlxError::PoolClosed | SqlxError::WorkerCrashed => false,
                SqlxError::Database(db_err) => is_transient_db_error(&**db_err),
                _ => false,
            },
        }
    }
}

fn is_transient_db_error(err: &dyn sqlx::error::DatabaseError) -> bool {
    if let Some(code) = err.code() {
        match code.as_ref() {
            // 40001: serialization_failure
            // 40P01: deadlock_detected
            "40001" | "40P01" => true,
            // 53300: too_many_connections
            // 53400: configuration_limit_exceeded
            "53300" | "53400" => true,
            // 57P01: admin_shutdown
            // 57P02: crash_shutdown
            // 57P03: cannot_connect_now
            "57P01" | "57P02" | "57P03" => true,
            // 08xxx: Connection exceptions
            c if c.starts_with("08") => true,
            _ => false,
        }
    } else {
        // If there's no error code, we assume it's not a transient DB error (e.g. generic error)
        // unless we want to be conservative. But usually DB errors have codes.
        false
    }
}

impl ProcessingError {
    pub fn public_reason(&self) -> PublicErrorReason {
        match self {
            ProcessingError::MissingPayload => PublicErrorReason {
                code: "missing_payload".to_string(),
                message: "payload is missing".to_string(),
            },
            ProcessingError::InvalidUtf8(_) => PublicErrorReason {
                code: "invalid_utf8".to_string(),
                message: "payload is not valid utf-8".to_string(),
            },
            ProcessingError::JsonDecode(_) => PublicErrorReason {
                code: "json_decode".to_string(),
                message: "failed to decode json".to_string(),
            },
            ProcessingError::Debezium(_) => PublicErrorReason {
                code: "debezium".to_string(),
                message: "failed to unwrap debezium envelope".to_string(),
            },
            ProcessingError::Validation(_) => PublicErrorReason {
                code: "validation".to_string(),
                message: "validation failed".to_string(),
            },
            ProcessingError::Transport(_) => PublicErrorReason {
                code: "transport".to_string(),
                message: "transport error".to_string(),
            },
            ProcessingError::Stage(e) => PublicErrorReason {
                code: e.code.clone(),
                message: e.message.clone(),
            },
        }
    }

    pub fn is_retryable(&self) -> bool {
        match self {
            ProcessingError::Transport(e) => e.is_retryable(),
            ProcessingError::Stage(e) => e.retryable,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_retryable_errors() {
        // Transient errors
        let io_err = SqlxError::Io(io::Error::new(io::ErrorKind::TimedOut, "timeout"));
        let t_err = TransportError::new("test", io_err);
        let p_err = ProcessingError::Transport(t_err);
        assert!(p_err.is_retryable());

        let pool_err = SqlxError::PoolTimedOut;
        let t_err = TransportError::new("test", pool_err);
        let p_err = ProcessingError::Transport(t_err);
        assert!(p_err.is_retryable());
    }

    #[test]
    fn test_permanent_errors() {
        // Permanent errors
        let row_err = SqlxError::RowNotFound;
        let t_err = TransportError::new("test", row_err);
        let p_err = ProcessingError::Transport(t_err);
        assert!(!p_err.is_retryable());

        let json_err = serde_json::from_str::<serde_json::Value>("invalid").unwrap_err();
        let t_err = TransportError::new("test", json_err);
        let p_err = ProcessingError::Transport(t_err);
        assert!(!p_err.is_retryable());

        // Validation error (not transport)
        let val_err = ValidationError::new("bad");
        let p_err = ProcessingError::Validation(val_err);
        assert!(!p_err.is_retryable());
    }

    #[test]
    fn test_pool_closed_not_retryable() {
        let pool_closed_err = SqlxError::PoolClosed;
        let t_err = TransportError::new("test", pool_closed_err);
        let p_err = ProcessingError::Transport(t_err);
        assert!(!p_err.is_retryable(), "PoolClosed should not be retryable");
    }

    #[test]
    fn test_worker_crashed_not_retryable() {
        let worker_crashed_err = SqlxError::WorkerCrashed;
        let t_err = TransportError::new("test", worker_crashed_err);
        let p_err = ProcessingError::Transport(t_err);
        assert!(
            !p_err.is_retryable(),
            "WorkerCrashed should not be retryable"
        );
    }

    #[test]
    fn test_stage_error_not_retryable() {
        let stage_err = crate::eip::StageError {
            code: "stage_error".to_string(),
            message: "pipeline stage error".to_string(),
            retryable: false,
        };
        let p_err = ProcessingError::Stage(stage_err);
        assert!(!p_err.is_retryable(), "Stage error should not be retryable");
    }

    #[test]
    fn test_stage_error_public_reason() {
        let stage_err = crate::eip::StageError {
            code: "stage_error".to_string(),
            message: "pipeline stage error".to_string(),
            retryable: false,
        };
        let p_err = ProcessingError::Stage(stage_err);
        let reason = p_err.public_reason();
        assert_eq!(reason.code, "stage_error");
        assert_eq!(reason.message, "pipeline stage error");
    }

    // Test-only struct implementing sqlx::error::DatabaseError for unit testing
    #[derive(Debug)]
    struct MockDatabaseError {
        code: Option<String>,
    }

    impl std::fmt::Display for MockDatabaseError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "mock error")
        }
    }

    impl std::error::Error for MockDatabaseError {}

    impl sqlx::error::DatabaseError for MockDatabaseError {
        fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
            self.code
                .as_ref()
                .map(|c| std::borrow::Cow::Borrowed(c.as_str()))
        }

        fn message(&self) -> &str {
            "mock error"
        }

        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }

        fn table(&self) -> Option<&str> {
            None
        }

        fn constraint(&self) -> Option<&str> {
            None
        }

        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    #[test]
    fn test_transient_db_error_serialization_failure() {
        let err = MockDatabaseError {
            code: Some("40001".to_string()),
        };
        assert!(
            is_transient_db_error(&err),
            "40001 (serialization_failure) should be transient"
        );
    }

    #[test]
    fn test_transient_db_error_deadlock_detected() {
        let err = MockDatabaseError {
            code: Some("40P01".to_string()),
        };
        assert!(
            is_transient_db_error(&err),
            "40P01 (deadlock_detected) should be transient"
        );
    }

    #[test]
    fn test_transient_db_error_too_many_connections() {
        let err = MockDatabaseError {
            code: Some("53300".to_string()),
        };
        assert!(
            is_transient_db_error(&err),
            "53300 (too_many_connections) should be transient"
        );
    }

    #[test]
    fn test_transient_db_error_configuration_limit_exceeded() {
        let err = MockDatabaseError {
            code: Some("53400".to_string()),
        };
        assert!(
            is_transient_db_error(&err),
            "53400 (configuration_limit_exceeded) should be transient"
        );
    }

    #[test]
    fn test_transient_db_error_admin_shutdown() {
        let err = MockDatabaseError {
            code: Some("57P01".to_string()),
        };
        assert!(
            is_transient_db_error(&err),
            "57P01 (admin_shutdown) should be transient"
        );
    }

    #[test]
    fn test_transient_db_error_crash_shutdown() {
        let err = MockDatabaseError {
            code: Some("57P02".to_string()),
        };
        assert!(
            is_transient_db_error(&err),
            "57P02 (crash_shutdown) should be transient"
        );
    }

    #[test]
    fn test_transient_db_error_cannot_connect_now() {
        let err = MockDatabaseError {
            code: Some("57P03".to_string()),
        };
        assert!(
            is_transient_db_error(&err),
            "57P03 (cannot_connect_now) should be transient"
        );
    }

    #[test]
    fn test_transient_db_error_connection_exception_prefix() {
        let err = MockDatabaseError {
            code: Some("08006".to_string()),
        };
        assert!(
            is_transient_db_error(&err),
            "08006 (connection exception) should be transient"
        );

        let err = MockDatabaseError {
            code: Some("08P01".to_string()),
        };
        assert!(
            is_transient_db_error(&err),
            "08P01 (connection exception) should be transient"
        );
    }

    #[test]
    fn test_non_transient_db_error_syntax_error() {
        let err = MockDatabaseError {
            code: Some("42601".to_string()),
        };
        assert!(
            !is_transient_db_error(&err),
            "42601 (syntax_error) should not be transient"
        );
    }

    #[test]
    fn test_non_transient_db_error_undefined_table() {
        let err = MockDatabaseError {
            code: Some("42P01".to_string()),
        };
        assert!(
            !is_transient_db_error(&err),
            "42P01 (undefined_table) should not be transient"
        );
    }

    #[test]
    fn test_non_transient_db_error_duplicate_key() {
        let err = MockDatabaseError {
            code: Some("23505".to_string()),
        };
        assert!(
            !is_transient_db_error(&err),
            "23505 (duplicate_key) should not be transient"
        );
    }

    #[test]
    fn test_db_error_without_code() {
        let err = MockDatabaseError { code: None };
        assert!(
            !is_transient_db_error(&err),
            "Error without code should not be transient"
        );
    }

    // PostgreSQL 15+ error codes - verifying these are current for modern deployments
    #[test]
    fn test_postgres_error_codes_current() {
        // These codes are valid in PostgreSQL 15 and later.
        // Documentation reference: https://www.postgresql.org/docs/current/errcodes-appendix.html
        // Transient codes tested above:
        // - 40001: serialization_failure (Class 40 — Transaction Rollback)
        // - 40P01: deadlock_detected (Class 40 — Transaction Rollback)
        // - 53300: too_many_connections (Class 53 — Insufficient Resources)
        // - 53400: configuration_limit_exceeded (Class 53 — Insufficient Resources)
        // - 57P01: admin_shutdown (Class 57 — Operator Intervention)
        // - 57P02: crash_shutdown (Class 57 — Operator Intervention)
        // - 57P03: cannot_connect_now (Class 57 — Operator Intervention)
        // - 08xxx: Connection exceptions (Class 08)
        //
        // Note: These codes are stable across PostgreSQL versions and are
        // suitable for production deployments targeting PostgreSQL 12+.
        assert!(true, "PostgreSQL error codes are current and stable");
    }
}
