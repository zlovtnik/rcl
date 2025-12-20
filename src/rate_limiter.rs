/// Per-tenant rate limiting for multi-tenancy support
/// Implements token bucket algorithm for fair rate limiting across tenants

use anyhow::Result;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::debug;

/// Rate limiter configuration for a tenant
#[derive(Clone, Debug)]
pub struct TenantRateLimitConfig {
    /// Maximum messages per second for this tenant
    pub max_messages_per_sec: u32,
    /// Maximum bytes per second for this tenant
    pub max_bytes_per_sec: u64,
    /// Bucket refill interval (how often tokens are added)
    pub bucket_refill_interval_ms: u64,
}

impl Default for TenantRateLimitConfig {
    fn default() -> Self {
        Self {
            max_messages_per_sec: 10000,
            max_bytes_per_sec: 100_000_000, // 100 MB/s
            bucket_refill_interval_ms: 100,
        }
    }
}

/// Token bucket for a single tenant
#[derive(Clone, Debug)]
struct TokenBucket {
    /// Maximum message tokens
    max_message_tokens: f64,
    /// Current message tokens available
    message_tokens: f64,
    /// Maximum byte tokens
    max_byte_tokens: f64,
    /// Current byte tokens available
    byte_tokens: f64,
    /// Last refill time
    last_refill: Instant,
    /// Configuration for this tenant
    config: TenantRateLimitConfig,
}

impl TokenBucket {
    fn new(config: TenantRateLimitConfig) -> Self {
        let max_message_tokens = config.max_messages_per_sec as f64;
        let max_byte_tokens = config.max_bytes_per_sec as f64;

        Self {
            max_message_tokens,
            message_tokens: max_message_tokens,
            max_byte_tokens,
            byte_tokens: max_byte_tokens,
            last_refill: Instant::now(),
            config,
        }
    }

    /// Refill tokens based on elapsed time
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill);
        let elapsed_secs = elapsed.as_secs_f64();

        // Refill tokens based on elapsed time (tokens per second * elapsed seconds)
        let message_refill = self.config.max_messages_per_sec as f64 * elapsed_secs;
        self.message_tokens = (self.message_tokens + message_refill).min(self.max_message_tokens);

        let byte_refill = self.config.max_bytes_per_sec as f64 * elapsed_secs;
        self.byte_tokens = (self.byte_tokens + byte_refill).min(self.max_byte_tokens);

        self.last_refill = now;
    }

    /// Check if we can accept a message of given size
    fn allow(&mut self, message_size_bytes: u64) -> bool {
        self.refill();

        let message_size = message_size_bytes as f64;

        // Check both message and byte limits
        if self.message_tokens >= 1.0 && self.byte_tokens >= message_size {
            self.message_tokens -= 1.0;
            self.byte_tokens -= message_size;
            true
        } else {
            false
        }
    }

    /// Get current tokens available (for metrics)
    fn message_tokens_available(&mut self) -> f64 {
        self.refill();
        self.message_tokens
    }

    /// Get current byte tokens available (for metrics)
    fn byte_tokens_available(&mut self) -> f64 {
        self.refill();
        self.byte_tokens
    }
}

/// Per-tenant rate limiter
pub struct TenantRateLimiter {
    /// Map of tenant_id -> token bucket
    buckets: Arc<Mutex<HashMap<String, TokenBucket>>>,
    /// Default configuration for new tenants
    default_config: TenantRateLimitConfig,
    /// Per-tenant configurations (overrides for specific tenants)
    tenant_configs: Arc<Mutex<HashMap<String, TenantRateLimitConfig>>>,
}

impl TenantRateLimiter {
    /// Create a new rate limiter with default config
    pub fn new(default_config: TenantRateLimitConfig) -> Self {
        Self {
            buckets: Arc::new(Mutex::new(HashMap::new())),
            default_config,
            tenant_configs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Set a custom rate limit for a specific tenant
    pub fn set_tenant_limit(
        &self,
        tenant_id: impl Into<String>,
        config: TenantRateLimitConfig,
    ) -> Result<()> {
        let tenant_id = tenant_id.into();
        // Acquire buckets first to maintain consistent lock ordering
        let mut buckets = self.buckets.lock().unwrap();
        let mut configs = self.tenant_configs.lock().unwrap();
        
        configs.insert(tenant_id.clone(), config.clone());
        
        if let Some(bucket) = buckets.get_mut(&tenant_id) {
            *bucket = TokenBucket::new(config);
        }

        Ok(())
    }

    /// Check if a message from a tenant can be allowed
    pub fn allow_message(&self, tenant_id: &str, message_size_bytes: u64) -> Result<bool> {
        // Get config before acquiring buckets lock to avoid potential deadlocks
        let config = self.get_config_for_tenant(tenant_id);

        let mut buckets = self.buckets.lock().unwrap();

        // Get or create bucket for this tenant
        let bucket = buckets.entry(tenant_id.to_string()).or_insert_with(|| {
            TokenBucket::new(config)
        });

        Ok(bucket.allow(message_size_bytes))
    }

    /// Get metrics for a tenant's current token status
    pub fn get_tenant_metrics(&self, tenant_id: &str) -> Result<TenantRateLimitMetrics> {
        // Get config before acquiring buckets lock to avoid potential deadlocks
        let config = self.get_config_for_tenant(tenant_id);

        let mut buckets = self.buckets.lock().unwrap();

        let bucket = buckets.entry(tenant_id.to_string()).or_insert_with(|| {
            TokenBucket::new(config)
        });

        let message_tokens = bucket.message_tokens_available();
        let byte_tokens = bucket.byte_tokens_available();

        Ok(TenantRateLimitMetrics {
            tenant_id: tenant_id.to_string(),
            message_tokens_available: message_tokens,
            max_message_tokens: bucket.max_message_tokens,
            byte_tokens_available: byte_tokens,
            max_byte_tokens: bucket.max_byte_tokens,
        })
    }

    /// Get configuration for a tenant
    fn get_config_for_tenant(&self, tenant_id: &str) -> TenantRateLimitConfig {
        let configs = self.tenant_configs.lock().unwrap();
        configs
            .get(tenant_id)
            .cloned()
            .unwrap_or_else(|| self.default_config.clone())
    }

    /// Reset rate limits for all tenants (for testing)
    #[cfg(test)]
    pub fn reset_all(&self) {
        let mut buckets = self.buckets.lock().unwrap();
        buckets.clear();
    }
}

/// Metrics for a tenant's rate limit status
#[derive(Clone, Debug)]
pub struct TenantRateLimitMetrics {
    pub tenant_id: String,
    pub message_tokens_available: f64,
    pub max_message_tokens: f64,
    pub byte_tokens_available: f64,
    pub max_byte_tokens: f64,
}

impl TenantRateLimitMetrics {
    /// Get message utilization percentage
    pub fn message_utilization_percent(&self) -> f64 {
        if self.max_message_tokens > 0.0 {
            ((self.max_message_tokens - self.message_tokens_available) / self.max_message_tokens)
                * 100.0
        } else {
            0.0
        }
    }

    /// Get byte utilization percentage
    pub fn byte_utilization_percent(&self) -> f64 {
        if self.max_byte_tokens > 0.0 {
            ((self.max_byte_tokens - self.byte_tokens_available) / self.max_byte_tokens) * 100.0
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_token_bucket_creation() {
        let config = TenantRateLimitConfig {
            max_messages_per_sec: 100,
            max_bytes_per_sec: 1_000_000,
            bucket_refill_interval_ms: 100,
        };
        let bucket = TokenBucket::new(config.clone());
        assert_eq!(bucket.max_message_tokens, 100.0);
        assert_eq!(bucket.max_byte_tokens, 1_000_000.0);
    }

    #[test]
    fn test_allow_message_success() {
        let config = TenantRateLimitConfig::default();
        let mut bucket = TokenBucket::new(config);
        assert!(bucket.allow(1000)); // Should succeed with plenty of tokens
    }

    #[test]
    fn test_allow_message_failure_after_exhaustion() {
        let config = TenantRateLimitConfig {
            max_messages_per_sec: 1,
            max_bytes_per_sec: 10_000,
            bucket_refill_interval_ms: 100,
        };
        let mut bucket = TokenBucket::new(config);
        assert!(bucket.allow(1000)); // First message succeeds
        assert!(!bucket.allow(1000)); // Second message fails (no tokens)
    }

    #[test]
    fn test_token_refill() {
        let config = TenantRateLimitConfig {
            max_messages_per_sec: 10,
            max_bytes_per_sec: 100_000,
            bucket_refill_interval_ms: 100,
        };
        let mut bucket = TokenBucket::new(config);

        // Consume all tokens
        for _ in 0..10 {
            assert!(bucket.allow(1000));
        }
        assert!(!bucket.allow(1000)); // No tokens left

        // Wait for refill
        thread::sleep(Duration::from_millis(150));

        // After refill, should be able to consume again
        assert!(bucket.allow(1000));
    }

    #[test]
    fn test_rate_limiter_multiple_tenants() {
        let limiter = TenantRateLimiter::new(TenantRateLimitConfig {
            max_messages_per_sec: 100,
            max_bytes_per_sec: 1_000_000,
            bucket_refill_interval_ms: 100,
        });

        // Both tenants should be able to process messages independently
        assert!(limiter.allow_message("tenant1", 1000).unwrap());
        assert!(limiter.allow_message("tenant2", 1000).unwrap());
    }

    #[test]
    fn test_tenant_custom_limits() {
        let limiter = TenantRateLimiter::new(TenantRateLimitConfig {
            max_messages_per_sec: 1000,
            max_bytes_per_sec: 10_000_000,
            bucket_refill_interval_ms: 100,
        });

        // Set custom limit for tenant1
        let custom_config = TenantRateLimitConfig {
            max_messages_per_sec: 1,
            max_bytes_per_sec: 10_000,
            bucket_refill_interval_ms: 100,
        };
        limiter.set_tenant_limit("tenant1", custom_config).unwrap();

        // tenant1 should be rate-limited
        assert!(limiter.allow_message("tenant1", 1000).unwrap());
        assert!(!limiter.allow_message("tenant1", 1000).unwrap());

        // tenant2 should use default limit
        assert!(limiter.allow_message("tenant2", 1000).unwrap());
    }

    #[test]
    fn test_get_tenant_metrics() {
        let limiter = TenantRateLimiter::new(TenantRateLimitConfig::default());

        let metrics = limiter.get_tenant_metrics("tenant1").unwrap();
        assert_eq!(metrics.tenant_id, "tenant1");
        assert!(metrics.message_tokens_available > 0.0);
        assert!(metrics.byte_tokens_available > 0.0);
    }
}
