use crate::config::LogLevel;
use tracing_subscriber::{fmt, EnvFilter};

pub fn init(level: &LogLevel) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let default_directive = level.to_string();
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(default_directive));
    fmt()
        .with_env_filter(filter)
        .with_target(false)
        .json()
        .try_init()?;
    Ok(())
}
