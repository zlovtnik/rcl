use crate::config::ServiceConfig;
use opentelemetry::global;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
#[cfg(test)]
use serial_test::serial;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry};

pub struct TracingGuard {
    has_otlp: bool,
}

impl Drop for TracingGuard {
    fn drop(&mut self) {
        if self.has_otlp {
            opentelemetry::global::shutdown_tracer_provider();
        }
    }
}

/// Initializes global tracing, logging, and optional OTLP exporting based on the given configuration.
///
/// This sets the global W3C trace context propagator, configures a logging filter (from the
/// environment if present, otherwise from `cfg.log_level`), and installs a JSON-formatted
/// tracing/logging layer. If `cfg.otlp_endpoint` is set, an OTLP exporter is created and attached,
/// and the resulting tracer is shut down when the returned `TracingGuard` is dropped.
///
/// # Parameters
///
/// - `cfg`: configuration whose `log_level` determines the default logging filter and whose
///   optional `otlp_endpoint` enables OTLP exporting when present.
///
/// # Returns
///
/// A `TracingGuard` whose `_has_otlp` field is `true` if an OTLP exporter was configured, `false` otherwise.
///
/// # Examples
///
/// ```
/// // Construct a ServiceConfig with no OTLP endpoint (pseudo-code; adapt to your config type)
/// let cfg = ServiceConfig { log_level: "info".into(), otlp_endpoint: None };
/// let guard = init(&cfg).expect("failed to initialize tracing");
/// assert!(!guard._has_otlp);
/// ```
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

    Ok(TracingGuard { has_otlp })
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
        // Verify that the guard was created successfully without OTLP
        // The has_otlp field is private and used only in Drop for cleanup
        drop(guard); // Verify drop completes without panic
    }
}