use tokio::signal;
use tokio::sync::broadcast;
use tracing::{info, warn};

const SHUTDOWN_BROADCAST_CAPACITY: usize = 16;

/// A coordinator for managing graceful shutdown across async tasks.
///
/// This struct uses a Tokio broadcast channel to notify multiple subscribers when a shutdown signal is received.
pub struct ShutdownCoordinator {
    notify_shutdown: broadcast::Sender<()>,
}

/// Default implementation for creating a `ShutdownCoordinator`.
impl Default for ShutdownCoordinator {
    fn default() -> Self {
        let (notify_shutdown, _) = broadcast::channel(SHUTDOWN_BROADCAST_CAPACITY);
        Self { notify_shutdown }
    }
}

impl ShutdownCoordinator {
    /// Creates a new `ShutdownCoordinator` and returns it along with a receiver for shutdown notifications.
    ///
    /// Callers should clone or subscribe to the receiver to await shutdown notifications in their tasks.
    pub fn new() -> (Self, broadcast::Receiver<()>) {
        let (notify_shutdown, rx) = broadcast::channel(SHUTDOWN_BROADCAST_CAPACITY);
        (Self { notify_shutdown }, rx)
    }

    /// Returns a new receiver for shutdown notifications.
    ///
    /// Callers can use this to subscribe to shutdown events.
    #[allow(dead_code)]
    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.notify_shutdown.subscribe()
    }

    /// Asynchronously waits for a termination signal and broadcasts shutdown to all subscribers.
    ///
    /// On Unix systems, listens for SIGTERM and Ctrl+C signals. On non-Unix systems, listens only for Ctrl+C.
    /// This method will block until a signal is received, then send a notification to all subscribers.
    pub async fn wait_for_signal(&self) {
        let ctrl_c = async {
            signal::ctrl_c()
                .await
                .expect("failed to install Ctrl+C handler");
        };

        #[cfg(unix)]
        let terminate = async {
            signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("failed to install signal handler")
                .recv()
                .await;
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => info!("received ctrl-c signal"),
            _ = terminate => info!("received terminate signal"),
        }

        info!("signal received, starting graceful shutdown");
        if let Err(err) = self.notify_shutdown.send(()) {
            warn!(%err, "shutdown broadcast had no receivers");
        }
    }
}
