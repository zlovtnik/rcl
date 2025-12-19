use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{error, info, warn};

/// Circuit breaker state
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CircuitBreakerState {
    /// Circuit is closed - requests proceed normally
    Closed,
    /// Circuit is open - requests are rejected
    Open,
    /// Circuit is half-open - limited requests allowed to test recovery
    HalfOpen,
}

impl std::fmt::Display for CircuitBreakerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitBreakerState::Closed => write!(f, "Closed"),
            CircuitBreakerState::Open => write!(f, "Open"),
            CircuitBreakerState::HalfOpen => write!(f, "HalfOpen"),
        }
    }
}

/// Configuration for circuit breaker behavior
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct CircuitBreakerConfig {
    /// Enable/disable circuit breaker (default: true)
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Number of consecutive failures before opening circuit (default: 10)
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: usize,

    /// Number of consecutive successes to close circuit from half-open (default: 5)
    #[serde(default = "default_success_threshold")]
    pub success_threshold: usize,

    /// Time in milliseconds before transitioning from open to half-open (default: 30000)
    #[serde(default = "default_half_open_timeout_ms")]
    pub half_open_timeout_ms: u64,
}

fn default_enabled() -> bool {
    true
}

fn default_failure_threshold() -> usize {
    10
}

fn default_success_threshold() -> usize {
    5
}

fn default_half_open_timeout_ms() -> u64 {
    30000
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            failure_threshold: default_failure_threshold(),
            success_threshold: default_success_threshold(),
            half_open_timeout_ms: default_half_open_timeout_ms(),
        }
    }
}

/// Circuit breaker for pipeline failure detection and fast-fail behavior
#[derive(Clone)]
pub struct CircuitBreaker {
    pipeline_name: String,
    config: CircuitBreakerConfig,
    state: Arc<parking_lot::RwLock<CircuitBreakerState>>,
    failure_count: Arc<AtomicUsize>,
    success_count: Arc<AtomicUsize>,
    last_open_time: Arc<AtomicU64>,
}

impl CircuitBreaker {
    /// Create a new circuit breaker for the given pipeline
    pub fn new(pipeline_name: String, config: CircuitBreakerConfig) -> Self {
        Self {
            pipeline_name,
            config,
            state: Arc::new(parking_lot::RwLock::new(CircuitBreakerState::Closed)),
            failure_count: Arc::new(AtomicUsize::new(0)),
            success_count: Arc::new(AtomicUsize::new(0)),
            last_open_time: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Check if a request can proceed; returns error if circuit is open
    pub fn try_execute(&self) -> Result<(), CircuitBreakerError> {
        if !self.config.enabled {
            return Ok(());
        }

        let current_state = *self.state.read();

        match current_state {
            CircuitBreakerState::Closed => Ok(()),
            CircuitBreakerState::Open => {
                // Check if we should transition to half-open
                let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
                    Ok(duration) => duration.as_millis() as u64,
                    Err(_) => {
                        error!(
                            pipeline_name = %self.pipeline_name,
                            "system clock error in circuit breaker - keeping circuit open"
                        );
                        return Err(CircuitBreakerError::CircuitOpen);
                    }
                };
                let last_open = self.last_open_time.load(Ordering::Relaxed);

                if now - last_open >= self.config.half_open_timeout_ms {
                    // Transition to half-open
                    let mut state = self.state.write();
                    if *state == CircuitBreakerState::Open {
                        *state = CircuitBreakerState::HalfOpen;
                        self.success_count.store(0, Ordering::Relaxed);
                        warn!(
                            pipeline_name = %self.pipeline_name,
                            "circuit breaker transitioning to half-open state"
                        );
                        return Ok(());
                    }
                }
                Err(CircuitBreakerError::CircuitOpen)
            }
            CircuitBreakerState::HalfOpen => Ok(()),
        }
    }

    /// Record a successful message processing
    pub fn record_success(&self) {
        if !self.config.enabled {
            return;
        }

        let current_state = *self.state.read();

        match current_state {
            CircuitBreakerState::Closed => {
                // Reset failure count on success while closed
                self.failure_count.store(0, Ordering::Relaxed);
            }
            CircuitBreakerState::HalfOpen => {
                let new_success_count = self.success_count.fetch_add(1, Ordering::Relaxed) + 1;

                if new_success_count >= self.config.success_threshold {
                    // Transition to closed
                    let mut state = self.state.write();
                    if *state == CircuitBreakerState::HalfOpen {
                        *state = CircuitBreakerState::Closed;
                        self.failure_count.store(0, Ordering::Relaxed);
                        self.success_count.store(0, Ordering::Relaxed);
                        info!(
                            pipeline_name = %self.pipeline_name,
                            "circuit breaker closed after {} successful messages",
                            new_success_count
                        );
                    }
                }
            }
            CircuitBreakerState::Open => {
                // Ignore successes while open
            }
        }
    }

    /// Record a message processing failure
    pub fn record_failure(&self) {
        if !self.config.enabled {
            return;
        }

        let current_state = *self.state.read();

        match current_state {
            CircuitBreakerState::Closed => {
                let new_failure_count = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;

                if new_failure_count >= self.config.failure_threshold {
                    // Transition to open
                    let mut state = self.state.write();
                    if *state == CircuitBreakerState::Closed {
                        *state = CircuitBreakerState::Open;
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64;
                        self.last_open_time.store(now, Ordering::Relaxed);
                        self.success_count.store(0, Ordering::Relaxed);

                        error!(
                            pipeline_name = %self.pipeline_name,
                            failure_count = new_failure_count,
                            threshold = self.config.failure_threshold,
                            "circuit breaker opened due to consecutive failures"
                        );
                    }
                }
            }
            CircuitBreakerState::HalfOpen => {
                // Any failure while half-open immediately transitions to open
                let mut state = self.state.write();
                if *state == CircuitBreakerState::HalfOpen {
                    *state = CircuitBreakerState::Open;
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;
                    self.last_open_time.store(now, Ordering::Relaxed);
                    self.failure_count.store(1, Ordering::Relaxed);
                    self.success_count.store(0, Ordering::Relaxed);

                    error!(
                        pipeline_name = %self.pipeline_name,
                        "circuit breaker reopened (failure during half-open state)"
                    );
                }
            }
            CircuitBreakerState::Open => {
                // Increment failure count (used for metrics) but don't transition
                self.failure_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Get the current state of the circuit breaker
    pub fn get_state(&self) -> CircuitBreakerState {
        *self.state.read()
    }

    /// Get the current failure count
    #[allow(dead_code)]
    pub fn get_failure_count(&self) -> usize {
        self.failure_count.load(Ordering::Relaxed)
    }

    /// Get the current success count (for half-open state)
    #[allow(dead_code)]
    pub fn get_success_count(&self) -> usize {
        self.success_count.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone)]
pub enum CircuitBreakerError {
    CircuitOpen,
}

impl std::fmt::Display for CircuitBreakerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitBreakerError::CircuitOpen => write!(f, "circuit breaker is open"),
        }
    }
}

impl std::error::Error for CircuitBreakerError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_breaker_starts_closed() {
        let cb = CircuitBreaker::new("test".to_string(), CircuitBreakerConfig::default());
        assert_eq!(cb.get_state(), CircuitBreakerState::Closed);
        assert!(cb.try_execute().is_ok());
    }

    #[test]
    fn test_circuit_breaker_opens_after_threshold() {
        let cb = CircuitBreaker::new(
            "test".to_string(),
            CircuitBreakerConfig {
                enabled: true,
                failure_threshold: 3,
                success_threshold: 5,
                half_open_timeout_ms: 100,
            },
        );

        // Record 3 failures
        for _ in 0..3 {
            cb.record_failure();
        }

        assert_eq!(cb.get_state(), CircuitBreakerState::Open);
        assert!(cb.try_execute().is_err());
    }

    #[test]
    fn test_circuit_breaker_resets_failures_on_success() {
        let cb = CircuitBreaker::new(
            "test".to_string(),
            CircuitBreakerConfig {
                enabled: true,
                failure_threshold: 3,
                success_threshold: 5,
                half_open_timeout_ms: 100,
            },
        );

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.get_failure_count(), 2);

        cb.record_success();
        assert_eq!(cb.get_failure_count(), 0);
    }

    #[test]
    fn test_circuit_breaker_disabled() {
        let cb = CircuitBreaker::new(
            "test".to_string(),
            CircuitBreakerConfig {
                enabled: false,
                failure_threshold: 1,
                success_threshold: 1,
                half_open_timeout_ms: 100,
            },
        );

        // Record many failures
        for _ in 0..10 {
            cb.record_failure();
        }

        // Circuit breaker should remain closed when disabled
        assert_eq!(cb.get_state(), CircuitBreakerState::Closed);
        assert!(cb.try_execute().is_ok());
    }

    #[test]
    fn test_circuit_breaker_transitions_half_open_to_closed() {
        let cb = CircuitBreaker::new(
            "test".to_string(),
            CircuitBreakerConfig {
                enabled: true,
                failure_threshold: 2,
                success_threshold: 2,
                half_open_timeout_ms: 0, // Immediate timeout
            },
        );

        // Open the circuit
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.get_state(), CircuitBreakerState::Open);

        // Immediately try to execute (should transition to half-open)
        assert!(cb.try_execute().is_ok());
        assert_eq!(cb.get_state(), CircuitBreakerState::HalfOpen);

        // Record successes
        cb.record_success();
        cb.record_success();
        assert_eq!(cb.get_state(), CircuitBreakerState::Closed);
    }

    #[test]
    fn test_circuit_breaker_half_open_fails_on_error() {
        let cb = CircuitBreaker::new(
            "test".to_string(),
            CircuitBreakerConfig {
                enabled: true,
                failure_threshold: 1,
                success_threshold: 5,
                half_open_timeout_ms: 0, // Immediate timeout
            },
        );

        // Open the circuit
        cb.record_failure();
        assert_eq!(cb.get_state(), CircuitBreakerState::Open);

        // Transition to half-open
        assert!(cb.try_execute().is_ok());
        assert_eq!(cb.get_state(), CircuitBreakerState::HalfOpen);

        // Record a failure while half-open - should reopen immediately
        cb.record_failure();
        assert_eq!(cb.get_state(), CircuitBreakerState::Open);
    }
}
