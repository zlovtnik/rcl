use crate::config::ServiceConfig;
use opentelemetry::global;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
#[cfg(test)]
use serial_test::serial;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry};

pub struct TracingGuard {
    _has_otlp: bool,
}

impl Drop for TracingGuard {
    fn drop(&mut self) {
        if self._has_otlp {
            opentelemetry::global::shutdown_tracer_provider();
        }
    }
}

pub fn init(
    cfg: &ServiceConfig,
) -> Result<TracingGuard, Box<dyn std::error::Error + Send + Sync + 'static>> {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let default_directive = cfg.log_level.to_string();
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_directive));

    let fmt_layer = fmt::layer().with_target(false).json();

    let registry = Registry::default().with(filter).with(fmt_layer);

    let has_otlp = if let Some(endpoint) = &cfg.otlp_endpoint {
        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .tonic()
                    .with_endpoint(endpoint),
            )
            .install_batch(opentelemetry_sdk::runtime::Tokio)?;

        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
        registry.with(telemetry).try_init()?;
        true
    } else {
        registry.try_init()?;
        false
    };

    Ok(TracingGuard {
        _has_otlp: has_otlp,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LogLevel;
    use crate::config::ServiceConfig;

    #[serial]
    #[test]
    fn test_init_no_otlp() {
        let cfg = ServiceConfig {
            log_level: LogLevel::Info,
            metrics_port: 9000,
            otlp_endpoint: None,
            health_check_timeout_ms: 5000,
            shutdown_timeout: "30s".to_string(),
        };

        let guard = init(&cfg).expect("init should succeed");
        // No OTLP endpoint configured
        assert!(!guard._has_otlp);
    }
}
