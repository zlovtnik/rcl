use crate::config::ServiceConfig;
use opentelemetry::global;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
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
