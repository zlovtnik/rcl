use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ComponentStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Debug, Clone, Serialize)]
pub struct PipelineHealth {
    pub status: ComponentStatus,
    pub registered_at: DateTime<Utc>,
    pub last_processed_at: Option<DateTime<Utc>>,
    pub error_count: u64,
    pub last_error: Option<String>,
}

impl Default for PipelineHealth {
    fn default() -> Self {
        Self {
            status: ComponentStatus::Healthy,
            registered_at: Utc::now(),
            last_processed_at: None,
            error_count: 0,
            last_error: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SystemHealth {
    pub status: ComponentStatus,
    pub kafka: ComponentStatus,
    pub postgres: ComponentStatus,
    pub pipelines: HashMap<String, PipelineHealth>,
}

pub struct HealthRegistry {
    kafka_status: RwLock<ComponentStatus>,
    postgres_status: RwLock<ComponentStatus>,
    pipelines: RwLock<HashMap<String, PipelineHealth>>,
    timeout: Duration,
}

impl HealthRegistry {
    pub fn new(timeout: Duration) -> Self {
        Self {
            kafka_status: RwLock::new(ComponentStatus::Healthy),
            postgres_status: RwLock::new(ComponentStatus::Healthy),
            pipelines: RwLock::new(HashMap::new()),
            timeout,
        }
    }

    pub fn register_pipeline(&self, name: &str) {
        let mut pipelines = self
            .pipelines
            .write()
            .expect("Pipeline registry lock poisoned");
        pipelines.insert(name.to_string(), PipelineHealth::default());
    }

    pub fn update_pipeline_success(&self, name: &str) -> Result<(), String> {
        let mut pipelines = self
            .pipelines
            .write()
            .map_err(|e| format!("Lock poisoned: {}", e))?;
        if let Some(p) = pipelines.get_mut(name) {
            p.status = ComponentStatus::Healthy;
            p.last_processed_at = Some(Utc::now());
        } else {
            return Err(format!("Pipeline '{}' not registered", name));
        }
        Ok(())
    }

    pub fn update_pipeline_error(&self, name: &str, error: String) -> Result<(), String> {
        let mut pipelines = self
            .pipelines
            .write()
            .map_err(|e| format!("Lock poisoned: {}", e))?;
        if let Some(p) = pipelines.get_mut(name) {
            p.status = ComponentStatus::Degraded;
            p.error_count += 1;
            p.last_error = Some(error);
        } else {
            return Err(format!("Pipeline '{}' not registered", name));
        }
        Ok(())
    }

    pub fn set_kafka_status(&self, status: ComponentStatus) -> Result<(), String> {
        let mut guard = self
            .kafka_status
            .write()
            .map_err(|e| format!("Lock poisoned: {}", e))?;
        *guard = status;
        Ok(())
    }

    pub fn set_postgres_status(&self, status: ComponentStatus) -> Result<(), String> {
        let mut guard = self
            .postgres_status
            .write()
            .map_err(|e| format!("Lock poisoned: {}", e))?;
        *guard = status;
        Ok(())
    }

    /// Computes the current SystemHealth by combining component statuses and per-pipeline health.
    ///
    /// The returned SystemHealth contains:
    /// - `status`: overall system status (Unhealthy > Degraded > Healthy). Kafka or Postgres being `Unhealthy` forces overall `Unhealthy`; `Degraded` promotes overall status to `Degraded` only if no `Unhealthy` is present. Pipeline health is evaluated inline and can also promote the overall status.
    /// - `kafka` and `postgres`: current component statuses read from the registry.
    /// - `pipelines`: cloned pipeline health entries; any pipeline that has not had activity within the registry's timeout and is otherwise `Healthy` is marked `Degraded` and given `last_error = Some("Pipeline stalled")`.
    ///
    /// Lock poisoning is handled by recovering the inner values when reading shared state.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use chrono::Utc;
    ///
    /// let registry = HealthRegistry::new(Duration::from_secs(60));
    /// registry.register_pipeline("p1".to_string());
    /// let system = registry.get_status();
    /// assert_eq!(system.kafka, ComponentStatus::Healthy);
    /// assert_eq!(system.postgres, ComponentStatus::Healthy);
    /// assert!(system.pipelines.contains_key("p1"));
    /// ```
    pub fn get_status(&self) -> SystemHealth {
        let kafka = self
            .kafka_status
            .read()
            .map(|g| g.clone())
            .unwrap_or_else(|poisoned| poisoned.into_inner().clone());
        let postgres = self
            .postgres_status
            .read()
            .map(|g| g.clone())
            .unwrap_or_else(|poisoned| poisoned.into_inner().clone());

        let pipelines_guard = self
            .pipelines
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        // Initialize status with Healthy, then update based on kafka and postgres immediately
        let mut status = ComponentStatus::Healthy;
        if kafka == ComponentStatus::Unhealthy || postgres == ComponentStatus::Unhealthy {
            status = ComponentStatus::Unhealthy;
        } else if kafka == ComponentStatus::Degraded || postgres == ComponentStatus::Degraded {
            status = ComponentStatus::Degraded;
        }

        let mut pipelines = HashMap::with_capacity(pipelines_guard.len());
        let timeout = chrono::Duration::from_std(self.timeout).unwrap_or(chrono::Duration::MAX);
        let now = Utc::now();

        // Single pass: compute pipeline health, insert into map, and update overall status inline
        for (name, p) in pipelines_guard.iter() {
            let mut p_health = p.clone();
            let last_activity = p_health.last_processed_at.unwrap_or(p_health.registered_at);

            if now.signed_duration_since(last_activity) > timeout
                && p_health.status == ComponentStatus::Healthy
            {
                p_health.status = ComponentStatus::Degraded;
                p_health.last_error = Some("Pipeline stalled".to_string());
            }

            // Update overall status inline: Unhealthy immediately, Degraded only if not already Unhealthy
            if p_health.status == ComponentStatus::Unhealthy {
                status = ComponentStatus::Unhealthy;
            } else if p_health.status == ComponentStatus::Degraded
                && status != ComponentStatus::Unhealthy
            {
                status = ComponentStatus::Degraded;
            }

            pipelines.insert(name.clone(), p_health);
        }

        SystemHealth {
            status,
            kafka,
            postgres,
            pipelines,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_update_pipeline() {
        let reg = HealthRegistry::new(std::time::Duration::from_secs(60));
        reg.register_pipeline("p1");

        // update success should work
        assert!(reg.update_pipeline_success("p1").is_ok());

        // update error increments count
        assert!(reg.update_pipeline_error("p1", "err".to_string()).is_ok());

        let status = reg.get_status();
        assert!(status.pipelines.contains_key("p1"));
        let p = status.pipelines.get("p1").unwrap();
        assert_eq!(p.error_count, 1);
        assert_eq!(p.status, ComponentStatus::Degraded);
    }

    #[test]
    fn test_set_component_status_changes_overall() {
        let reg = HealthRegistry::new(std::time::Duration::from_secs(60));
        reg.register_pipeline("p2");
        assert!(reg.set_kafka_status(ComponentStatus::Unhealthy).is_ok());
        let s = reg.get_status();
        assert_eq!(s.kafka, ComponentStatus::Unhealthy);
        assert_eq!(s.status, ComponentStatus::Unhealthy);
    }

    #[test]
    fn test_pipeline_stall_becomes_degraded() {
        // zero timeout causes immediate stall detection
        let reg = HealthRegistry::new(std::time::Duration::from_secs(0));
        reg.register_pipeline("p3");
        let s = reg.get_status();
        // pipeline should be degraded due to zero timeout
        let p = s.pipelines.get("p3").unwrap();
        assert_eq!(p.status, ComponentStatus::Degraded);
        assert_eq!(s.status, ComponentStatus::Degraded);
    }

    #[test]
    fn test_update_unregistered_pipeline_returns_error() {
        let reg = HealthRegistry::new(std::time::Duration::from_secs(60));
        assert!(reg.update_pipeline_success("nope").is_err());
    }
}