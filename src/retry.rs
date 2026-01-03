use std::time::Duration;

/// Configuration for retry behavior with exponential backoff and jitter.
///
/// This configuration controls how transient errors are retried:
/// - `max_attempts`: Maximum number of retry attempts (including initial attempt)
/// - `initial_backoff_ms`: Initial backoff delay in milliseconds
/// - `max_backoff_ms`: Maximum backoff delay in milliseconds (jitter cap)
///
/// # Examples
///
/// ```
/// use rcl::retry::RetryConfig;
/// let cfg = RetryConfig {
///     max_attempts: 3,
///     initial_backoff_ms: 100,
///     max_backoff_ms: 30_000,
/// };
/// ```
#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 30_000,
        }
    }
}

impl RetryConfig {
    /// Build an ExponentialBackoff instance from this configuration.
    ///
    /// The backoff uses jitter to prevent thundering herd when multiple instances
    /// recover from transient failures simultaneously.
    ///
    /// # Returns
    ///
    /// An `ExponentialBackoff` configured with exponential backoff between retries.
    pub fn build_backoff(&self) -> backoff::ExponentialBackoff {
        // Calculate max_elapsed_time as the sum of actual retry intervals
        // (not the simple product max_backoff_ms * max_attempts which overestimates)
        let mut total_backoff_ms: u64 = 0;
        let multiplier = 2.0_f64;
        for i in 0..self.max_attempts {
            let interval_ms = (self.initial_backoff_ms as f64 * multiplier.powi(i as i32)) as u64;
            let capped_interval = interval_ms.min(self.max_backoff_ms);
            total_backoff_ms = total_backoff_ms.saturating_add(capped_interval);
        }

        backoff::ExponentialBackoff {
            initial_interval: Duration::from_millis(self.initial_backoff_ms),
            max_interval: Duration::from_millis(self.max_backoff_ms),
            multiplier: 2.0,
            max_elapsed_time: Some(Duration::from_millis(total_backoff_ms)),
            ..backoff::ExponentialBackoff::default()
        }
    }

    /// Validate this configuration.
    ///
    /// # Errors
    ///
    /// Returns an error string if:
    /// - max_attempts is 0
    /// - initial_backoff_ms is 0
    /// - max_backoff_ms is less than initial_backoff_ms
    pub fn validate(&self) -> Result<(), String> {
        if self.max_attempts == 0 {
            return Err("max_attempts must be > 0".to_string());
        }
        if self.initial_backoff_ms == 0 {
            return Err("initial_backoff_ms must be > 0".to_string());
        }
        if self.max_backoff_ms < self.initial_backoff_ms {
            return Err("max_backoff_ms must be >= initial_backoff_ms".to_string());
        }
        Ok(())
    }
}

/// Retry statistics tracked during a retry operation.
///
/// This structure is used to record metrics about retry attempts, allowing
/// observability into transient failure patterns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub struct RetryStats {
    /// Total number of attempts (including initial)
    pub attempts: u32,
    /// Whether the final attempt succeeded
    pub succeeded: bool,
}

#[allow(dead_code)]
impl RetryStats {
    pub fn new(attempts: u32, succeeded: bool) -> Self {
        Self {
            attempts,
            succeeded,
        }
    }

    /// Number of retries (attempts - 1)
    pub fn retry_count(&self) -> u32 {
        self.attempts.saturating_sub(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_retry_config() {
        let cfg = RetryConfig::default();
        assert_eq!(cfg.max_attempts, 3);
        assert_eq!(cfg.initial_backoff_ms, 100);
        assert_eq!(cfg.max_backoff_ms, 30_000);
    }

    #[test]
    fn test_retry_config_validation_valid() {
        let cfg = RetryConfig {
            max_attempts: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 30_000,
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_retry_config_validation_zero_max_attempts() {
        let cfg = RetryConfig {
            max_attempts: 0,
            initial_backoff_ms: 100,
            max_backoff_ms: 30_000,
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_retry_config_validation_zero_initial_backoff() {
        let cfg = RetryConfig {
            max_attempts: 3,
            initial_backoff_ms: 0,
            max_backoff_ms: 30_000,
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_retry_config_validation_invalid_backoff_range() {
        let cfg = RetryConfig {
            max_attempts: 3,
            initial_backoff_ms: 1000,
            max_backoff_ms: 100,
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_retry_stats_retry_count() {
        let stats = RetryStats::new(1, true);
        assert_eq!(stats.retry_count(), 0);

        let stats = RetryStats::new(3, true);
        assert_eq!(stats.retry_count(), 2);

        let stats = RetryStats::new(5, false);
        assert_eq!(stats.retry_count(), 4);
    }

    #[test]
    fn test_retry_stats_zero_attempts() {
        let stats = RetryStats::new(0, false);
        assert_eq!(stats.retry_count(), 0);
    }

    #[test]
    fn test_build_backoff() {
        let cfg = RetryConfig {
            max_attempts: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 5000,
        };
        let backoff = cfg.build_backoff();
        assert_eq!(backoff.initial_interval, Duration::from_millis(100));
        assert_eq!(backoff.max_interval, Duration::from_millis(5000));
    }
}
