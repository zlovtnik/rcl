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

        let mut status = ComponentStatus::Healthy;
        if kafka == ComponentStatus::Unhealthy || postgres == ComponentStatus::Unhealthy {
            status = ComponentStatus::Unhealthy;
        }

        let mut pipelines = HashMap::with_capacity(pipelines_guard.len());
        let timeout = chrono::Duration::from_std(self.timeout).unwrap_or(chrono::Duration::MAX);
        let now = Utc::now();

        for (name, p) in pipelines_guard.iter() {
            let mut p_health = p.clone();
            let last_activity = p_health.last_processed_at.unwrap_or(p_health.registered_at);

            if now.signed_duration_since(last_activity) > timeout
                && p_health.status == ComponentStatus::Healthy
            {
                p_health.status = ComponentStatus::Degraded;
                p_health.last_error = Some("Pipeline stalled".to_string());
            }

            if p_health.status != ComponentStatus::Healthy && status == ComponentStatus::Healthy {
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
