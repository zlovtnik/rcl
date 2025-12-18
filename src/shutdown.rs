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

    /// Waits for an operating-system termination signal and then broadcasts a shutdown notification to all subscribers.
    ///
    /// On Unix, this listens for SIGTERM and Ctrl+C; on non-Unix platforms, it listens only for Ctrl+C. After a signal is received, a unit `()` is sent on the internal broadcast channel to notify all subscribers.
    ///
    /// # Examples
    ///
    /// ```
    /// use tokio::sync::broadcast;
    /// // Create a coordinator and subscribe to shutdown notifications.
    /// let (coord, mut rx) = crate::ShutdownCoordinator::new();
    ///
    /// // Simulate a shutdown by sending directly on the sender (useful for tests).
    /// let _ = coord.notify_shutdown.send(());
    ///
    /// // The receiver will receive the unit value sent above.
    /// assert_eq!(rx.try_recv().unwrap(), ());
    /// ```
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shutdown_coordinator_new() {
        let (coordinator, _rx) = ShutdownCoordinator::new();
        let mut subscriber = coordinator.subscribe();

        // Initially there should be no message
        assert!(matches!(
            subscriber.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));

        // Sending a shutdown should be received by the subscriber
        let _ = coordinator.notify_shutdown.send(());
        assert!(matches!(subscriber.try_recv(), Ok(())));
    }

    #[test]
    fn test_shutdown_coordinator_default() {
        let coordinator = ShutdownCoordinator::default();
        let mut subscriber = coordinator.subscribe();

        // Initially there should be no message
        assert!(matches!(
            subscriber.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));

        // Sending a shutdown should be received by the subscriber
        let _ = coordinator.notify_shutdown.send(());
        assert!(matches!(subscriber.try_recv(), Ok(())));
    }

    #[test]
    fn test_shutdown_coordinator_subscribe_multiple() {
        let coordinator = ShutdownCoordinator::new().0;
        let mut sub1 = coordinator.subscribe();
        let mut sub2 = coordinator.subscribe();
        let mut sub3 = coordinator.subscribe();

        // Initially all subscribers should have no message
        assert!(matches!(
            sub1.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            sub2.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            sub3.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));

        // Send one shutdown and ensure all observers receive it
        let _ = coordinator.notify_shutdown.send(());
        assert!(matches!(sub1.try_recv(), Ok(())));
        assert!(matches!(sub2.try_recv(), Ok(())));
        assert!(matches!(sub3.try_recv(), Ok(())));
    }

    #[tokio::test]
    async fn test_shutdown_coordinator_broadcast() {
        let (coordinator, mut rx) = ShutdownCoordinator::new();

        // Clone for the spawn
        let coordinator_clone = ShutdownCoordinator {
            notify_shutdown: coordinator.notify_shutdown.clone(),
        };

        // Spawn a task that will send the shutdown signal
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            let _ = coordinator_clone.notify_shutdown.send(());
        });

        // Wait for the signal
        let result = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv()).await;

        let recv_val = result
            .expect("should receive shutdown signal")
            .expect("recv failed");
        assert_eq!(recv_val, ());
    }

    #[tokio::test]
    async fn test_shutdown_coordinator_multiple_receivers() {
        let (coordinator, mut rx1) = ShutdownCoordinator::new();
        let mut rx2 = coordinator.subscribe();
        let mut rx3 = coordinator.subscribe();

        let coordinator_clone = ShutdownCoordinator {
            notify_shutdown: coordinator.notify_shutdown.clone(),
        };

        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            let _ = coordinator_clone.notify_shutdown.send(());
        });

        // All receivers should get the signal
        let timeout = std::time::Duration::from_millis(100);

        let r1 = tokio::time::timeout(timeout, rx1.recv()).await;
        let r2 = tokio::time::timeout(timeout, rx2.recv()).await;
        let r3 = tokio::time::timeout(timeout, rx3.recv()).await;

        assert!(r1.is_ok());
        assert!(r2.is_ok());
        assert!(r3.is_ok());
    }

    #[test]
    fn test_shutdown_broadcast_capacity() {
        // Verify the broadcast capacity constant
        assert_eq!(SHUTDOWN_BROADCAST_CAPACITY, 16);
    }
}