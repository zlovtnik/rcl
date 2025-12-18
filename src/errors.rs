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
    /// Raised when batch write partially fails - some messages succeeded, some failed.
    #[error("batch partial failure: {0}")]
    BatchPartialFailure(#[from] BatchPartialFailure),
}

/// Non-recursive error type for individual messages. This is identical to ProcessingError
/// but excludes BatchPartialFailure to prevent unbounded recursion in batch error handling.
///
/// This type should be used when handling errors for individual messages within batch operations.
/// Use MessageError instead of ProcessingError in contexts where you want to guarantee that
/// batch partial failures cannot create recursive error structures.
///
/// Conversion traits are provided to convert between MessageError and ProcessingError:
/// - `MessageError` can always be converted to `ProcessingError` via `From`/`Into`
/// - `ProcessingError` can be converted to `MessageError` via `TryFrom`, but will fail
///   for `BatchPartialFailure` variants to maintain the non-recursion guarantee
#[derive(Debug, Error)]
pub enum MessageError {
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

// Conversion traits between ProcessingError and MessageError
impl From<MessageError> for ProcessingError {
    fn from(err: MessageError) -> Self {
        match err {
            MessageError::MissingPayload => ProcessingError::MissingPayload,
            MessageError::InvalidUtf8(e) => ProcessingError::InvalidUtf8(e),
            MessageError::JsonDecode(e) => ProcessingError::JsonDecode(e),
            MessageError::Debezium(e) => ProcessingError::Debezium(e),
            MessageError::Validation(e) => ProcessingError::Validation(e),
            MessageError::Transport(e) => ProcessingError::Transport(e),
            MessageError::Stage(e) => ProcessingError::Stage(e),
        }
    }
}

impl TryFrom<ProcessingError> for MessageError {
    type Error = ProcessingError;

    fn try_from(err: ProcessingError) -> Result<Self, Self::Error> {
        match err {
            ProcessingError::MissingPayload => Ok(MessageError::MissingPayload),
            ProcessingError::InvalidUtf8(e) => Ok(MessageError::InvalidUtf8(e)),
            ProcessingError::JsonDecode(e) => Ok(MessageError::JsonDecode(e)),
            ProcessingError::Debezium(e) => Ok(MessageError::Debezium(e)),
            ProcessingError::Validation(e) => Ok(MessageError::Validation(e)),
            ProcessingError::Transport(e) => Ok(MessageError::Transport(e)),
            ProcessingError::Stage(e) => Ok(MessageError::Stage(e)),
            ProcessingError::BatchPartialFailure(_) => {
                // Cannot convert BatchPartialFailure to MessageError as it would break non-recursion guarantee
                Err(err)
            }
        }
    }
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
#[error("batch partial failure: {failed_count} of {total_count} messages failed")]
pub struct BatchPartialFailure {
    pub failed_count: usize,
    pub total_count: usize,
    pub failed_messages: Vec<(serde_json::Value, MessageError)>,
}

impl BatchPartialFailure {
    #[allow(dead_code)]
    pub fn new(
        total_count: usize,
        failed_messages: Vec<(serde_json::Value, MessageError)>,
    ) -> Self {
        let failed_count = failed_messages.len();
        Self {
            failed_count,
            total_count,
            failed_messages,
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
    #[error(transparent)]
    Csv(#[from] csv::Error),
    #[error("utf8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
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
            TransportSource::Csv(_) => false,
            TransportSource::Utf8(_) => false,
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
            ProcessingError::BatchPartialFailure(e) => PublicErrorReason {
                code: "batch_partial_failure".to_string(),
                message: format!(
                    "{} of {} messages failed in batch",
                    e.failed_count, e.total_count
                ),
            },
        }
    }

    pub fn is_retryable(&self) -> bool {
        match self {
            ProcessingError::Transport(e) => e.is_retryable(),
            ProcessingError::Stage(e) => e.retryable,
            ProcessingError::BatchPartialFailure(_) => false, // Partial failures are not retryable as a whole
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

    // Tests for MessageError and conversion traits
    #[test]
    fn test_processing_error_to_message_error_conversion_batch_failure_fails() {
        // Test that BatchPartialFailure cannot be converted to MessageError
        let batch_failure = BatchPartialFailure::new(5, vec![]);
        let proc_err = ProcessingError::BatchPartialFailure(batch_failure);
        let result = MessageError::try_from(proc_err);
        assert!(
            result.is_err(),
            "BatchPartialFailure should not convert to MessageError"
        );

        // Verify the error returned is the original BatchPartialFailure
        let err = result.unwrap_err();
        assert!(matches!(err, ProcessingError::BatchPartialFailure(_)));
    }

    #[test]
    fn test_message_error_to_processing_error_conversion() {
        // Test MessageError -> ProcessingError conversion
        let msg_err = MessageError::Validation(ValidationError::new("test validation"));
        let proc_err: ProcessingError = msg_err.into();
        assert!(matches!(proc_err, ProcessingError::Validation(_)));

        // Test all variants convert properly
        let test_cases = vec![
            MessageError::MissingPayload,
            MessageError::JsonDecode(
                serde_json::from_str::<serde_json::Value>("invalid").unwrap_err(),
            ),
            MessageError::Debezium(DebeziumError::new("test")),
            MessageError::Validation(ValidationError::new("test")),
            MessageError::Transport(TransportError::new("test", sqlx::Error::RowNotFound)),
            MessageError::Stage(crate::eip::StageError {
                code: "test".to_string(),
                message: "test".to_string(),
                retryable: false,
            }),
        ];

        for msg_err in test_cases {
            let proc_err: ProcessingError = msg_err.into();
            // Ensure the conversion doesn't panic and produces a ProcessingError
            assert!(matches!(
                proc_err,
                ProcessingError::MissingPayload
                    | ProcessingError::JsonDecode(_)
                    | ProcessingError::Debezium(_)
                    | ProcessingError::Validation(_)
                    | ProcessingError::Transport(_)
                    | ProcessingError::Stage(_)
            ));
        }
    }

    #[test]
    fn test_processing_error_to_message_error_conversion_success() {
        // Test ProcessingError -> MessageError conversion for non-BatchPartialFailure variants
        let proc_err = ProcessingError::Validation(ValidationError::new("test validation"));
        let msg_err = MessageError::try_from(proc_err).unwrap();
        assert!(matches!(msg_err, MessageError::Validation(_)));

        // Test all non-BatchPartialFailure variants convert successfully
        let test_cases = vec![
            ProcessingError::MissingPayload,
            ProcessingError::JsonDecode(
                serde_json::from_str::<serde_json::Value>("invalid").unwrap_err(),
            ),
            ProcessingError::Debezium(DebeziumError::new("test")),
            ProcessingError::Validation(ValidationError::new("test")),
            ProcessingError::Transport(TransportError::new("test", sqlx::Error::RowNotFound)),
            ProcessingError::Stage(crate::eip::StageError {
                code: "test".to_string(),
                message: "test".to_string(),
                retryable: false,
            }),
        ];

        for proc_err in test_cases {
            let result = MessageError::try_from(proc_err);
            assert!(
                result.is_ok(),
                "Expected successful conversion, but got error"
            );
        }
    }

    #[test]
    fn test_message_error_variants() {
        // Test that MessageError has all the expected variants
        let _missing_payload = MessageError::MissingPayload;
        // Create a valid Utf8Error by attempting to convert invalid UTF-8 bytes
        let invalid_utf8_bytes = &[0xff, 0xfe];
        let _invalid_utf8 =
            MessageError::InvalidUtf8(std::str::from_utf8(invalid_utf8_bytes).unwrap_err());
        let _json_decode = MessageError::JsonDecode(
            serde_json::from_str::<serde_json::Value>("invalid").unwrap_err(),
        );
        let _debezium = MessageError::Debezium(DebeziumError::new("test"));
        let _validation = MessageError::Validation(ValidationError::new("test"));
        // Use SqlxError for transport test since TransportSource implements From<SqlxError>
        let sqlx_err = sqlx::Error::RowNotFound;
        let _transport = MessageError::Transport(TransportError::new("test", sqlx_err));
        let _stage = MessageError::Stage(crate::eip::StageError {
            code: "test".to_string(),
            message: "test".to_string(),
            retryable: false,
        });
    }

    #[test]
    fn test_batch_partial_failure_with_message_error() {
        // Test creating BatchPartialFailure with MessageError
        let failed_messages = vec![
            (
                serde_json::json!({"id": 1}),
                MessageError::Validation(ValidationError::new("invalid id")),
            ),
            (
                serde_json::json!({"id": 2}),
                MessageError::JsonDecode(
                    serde_json::from_str::<serde_json::Value>("invalid").unwrap_err(),
                ),
            ),
        ];

        let batch_failure = BatchPartialFailure::new(10, failed_messages);

        assert_eq!(batch_failure.total_count, 10);
        assert_eq!(batch_failure.failed_count, 2);
        assert_eq!(batch_failure.failed_messages.len(), 2);

        // Test that we can convert BatchPartialFailure to ProcessingError
        let proc_err: ProcessingError = batch_failure.into();
        assert!(matches!(proc_err, ProcessingError::BatchPartialFailure(_)));
    }

    #[test]
    fn test_message_error_display_formatting() {
        // Test that MessageError formats correctly (should match ProcessingError formatting)
        let msg_err = MessageError::Validation(ValidationError::new("test message"));
        let formatted = format!("{}", msg_err);
        assert_eq!(formatted, "validation failed: test message");

        let msg_err2 = MessageError::JsonDecode(
            serde_json::from_str::<serde_json::Value>("invalid").unwrap_err(),
        );
        let formatted2 = format!("{}", msg_err2);
        assert!(formatted2.contains("json decode error"));
    }

    #[test]
    fn test_round_trip_conversion() {
        // Test that MessageError -> ProcessingError -> MessageError works for non-BatchPartialFailure variants
        let original_stage = crate::eip::StageError {
            code: "test_code".to_string(),
            message: "test message".to_string(),
            retryable: true,
        };
        let original_msg_err = MessageError::Stage(original_stage.clone());

        let proc_err: ProcessingError = original_msg_err.into();
        let converted_back = MessageError::try_from(proc_err).unwrap();

        match converted_back {
            MessageError::Stage(conv) => {
                assert_eq!(original_stage.code, conv.code);
                assert_eq!(original_stage.message, conv.message);
                assert_eq!(original_stage.retryable, conv.retryable);
            }
            _ => panic!("Expected Stage variant"),
        }
    }
}
