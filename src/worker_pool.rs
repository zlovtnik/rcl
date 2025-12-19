#![allow(dead_code)]
/// Worker pool coordinator for parallel message processing within a single pipeline
///
/// This module implements a round-robin work distribution pattern where multiple worker tasks
/// process messages from individual channels while maintaining offset ordering guarantees
/// via a shared offset tracker.
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};

/// Tracks which offsets have been processed and are ready to commit
/// Maintains ordering: only offsets in a contiguous sequence from the start can be committed
#[derive(Clone)]
pub struct OffsetTracker {
    /// Next offset that should be committed
    next_to_commit: Arc<Mutex<i64>>,
    /// Offsets that have been processed but haven't been committed yet (out of order)
    pending: Arc<Mutex<BTreeMap<i64, ()>>>,
    /// Watermark: highest offset seen so far
    watermark: Arc<Mutex<i64>>,
}

impl OffsetTracker {
    /// Create a new offset tracker starting from the given offset
    pub fn new(start_offset: i64) -> Self {
        Self {
            next_to_commit: Arc::new(Mutex::new(start_offset)),
            pending: Arc::new(Mutex::new(BTreeMap::new())),
            watermark: Arc::new(Mutex::new(start_offset - 1)),
        }
    }

    /// Mark an offset as processed
    /// Returns `true` if this offset advances the commit frontier (contiguous from start)
    pub async fn mark_processed(&self, offset: i64) -> bool {
        let mut pending = self.pending.lock().await;
        let mut next = self.next_to_commit.lock().await;

        // Record this offset in pending
        pending.insert(offset, ());

        // Capture the current next value before advancing
        let old_next = *next;

        // Check if we can advance the commit frontier
        loop {
            if pending.contains_key(&*next) {
                pending.remove(&*next);
                *next += 1;
            } else {
                break;
            }
        }

        // Update watermark
        let mut watermark = self.watermark.lock().await;
        if offset > *watermark {
            *watermark = offset;
        }

        // Return true if the frontier moved (any advancement, not just to offset+1)
        *next > old_next
    }

    /// Get the current offset that can be safely committed
    pub async fn get_committable_offset(&self) -> i64 {
        *self.next_to_commit.lock().await - 1
    }

    /// Get the watermark (highest offset seen)
    pub async fn get_watermark(&self) -> i64 {
        *self.watermark.lock().await
    }

    /// Get the number of pending offsets
    pub async fn pending_count(&self) -> usize {
        self.pending.lock().await.len()
    }

    /// Reset to a new starting offset (for test purposes and recovery)
    pub async fn reset(&self, start_offset: i64) {
        *self.next_to_commit.lock().await = start_offset;
        self.pending.lock().await.clear();
        *self.watermark.lock().await = start_offset - 1;
    }
}

/// Configuration for worker pool behavior
#[derive(Clone, Debug)]
pub struct WorkerPoolConfig {
    /// Number of worker threads (default 1)
    pub num_workers: usize,
    /// Channel capacity per worker
    pub worker_channel_capacity: usize,
}

impl Default for WorkerPoolConfig {
    fn default() -> Self {
        Self {
            num_workers: 1,
            worker_channel_capacity: 5000,
        }
    }
}

/// Metrics for worker pool operations
#[derive(Clone, Debug)]
pub struct WorkerPoolMetrics {
    /// Messages processed per worker
    pub processed_per_worker: Arc<Mutex<Vec<u64>>>,
    /// Worker busy time (ms) per worker
    pub busy_time_per_worker: Arc<Mutex<Vec<u64>>>,
    /// Current queue depths per worker
    pub queue_depth_per_worker: Arc<Mutex<Vec<usize>>>,
}

impl WorkerPoolMetrics {
    pub fn new(num_workers: usize) -> Self {
        Self {
            processed_per_worker: Arc::new(Mutex::new(vec![0; num_workers])),
            busy_time_per_worker: Arc::new(Mutex::new(vec![0; num_workers])),
            queue_depth_per_worker: Arc::new(Mutex::new(vec![0; num_workers])),
        }
    }

    pub async fn record_message_processed(&self, worker_id: usize) {
        let mut processed = self.processed_per_worker.lock().await;
        if worker_id < processed.len() {
            processed[worker_id] += 1;
        }
    }

    pub async fn set_queue_depth(&self, worker_id: usize, depth: usize) {
        let mut queue = self.queue_depth_per_worker.lock().await;
        if worker_id < queue.len() {
            queue[worker_id] = depth;
        }
    }
}

/// Work distribution strategy for assigning messages to workers
#[derive(Debug, Clone, Copy)]
pub enum WorkDistributionStrategy {
    /// Round-robin: assign messages to workers in rotation
    RoundRobin,
}

/// Message wrapper for the worker queue
#[derive(Clone)]
pub struct WorkItem<T: Clone + Send + 'static> {
    pub offset: i64,
    pub data: T,
}

/// Builder for creating a worker pool with flexible configuration
pub struct WorkerPoolBuilder {
    num_workers: usize,
    worker_channel_capacity: usize,
}

impl WorkerPoolBuilder {
    pub fn new(num_workers: usize) -> Self {
        Self {
            num_workers: num_workers.max(1),
            worker_channel_capacity: 5000,
        }
    }

    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.worker_channel_capacity = capacity;
        self
    }

    pub fn build(self) -> WorkerPoolCoordinator {
        let offset_tracker = OffsetTracker::new(0);
        let metrics = WorkerPoolMetrics::new(self.num_workers);

        let mut worker_txs = Vec::new();
        let mut worker_rxs = Vec::new();
        for _ in 0..self.num_workers {
            let (tx, rx) = mpsc::channel(self.worker_channel_capacity);
            worker_txs.push(tx);
            worker_rxs.push(rx);
        }

        WorkerPoolCoordinator {
            num_workers: self.num_workers,
            worker_senders: worker_txs,
            worker_receivers: worker_rxs,
            offset_tracker,
            metrics,
            next_worker: Arc::new(Mutex::new(0)),
        }
    }
}

/// Coordinates work distribution across multiple worker tasks
pub struct WorkerPoolCoordinator {
    num_workers: usize,
    worker_senders: Vec<mpsc::Sender<Vec<u8>>>,
    worker_receivers: Vec<mpsc::Receiver<Vec<u8>>>,
    pub offset_tracker: OffsetTracker,
    pub metrics: WorkerPoolMetrics,
    next_worker: Arc<Mutex<usize>>,
}

impl WorkerPoolCoordinator {
    /// Create a new worker pool with the given number of workers
    pub fn new(num_workers: usize) -> Self {
        WorkerPoolBuilder::new(num_workers).build()
    }

    /// Get number of workers
    pub fn worker_count(&self) -> usize {
        self.num_workers
    }

    /// Assign a message to a worker based on the distribution strategy
    pub async fn assign_to_worker(&self, worker_id: usize, data: Vec<u8>) -> Result<(), String> {
        if worker_id >= self.num_workers {
            return Err(format!("Worker ID {} out of range", worker_id));
        }

        self.worker_senders[worker_id]
            .send(data)
            .await
            .map_err(|e| format!("Failed to send to worker: {}", e))
    }

    /// Round-robin assignment to next available worker
    pub async fn assign_round_robin(&self, data: Vec<u8>) -> Result<(), String> {
        let mut next = self.next_worker.lock().await;
        let worker_id = *next % self.num_workers;
        *next = (*next + 1) % self.num_workers;
        drop(next);

        self.assign_to_worker(worker_id, data).await
    }

    /// Get the worker receivers (consumes them - call this once to spawn workers)
    pub fn take_receivers(&mut self) -> Vec<mpsc::Receiver<Vec<u8>>> {
        // Take the existing receivers and replace with empty vec
        std::mem::take(&mut self.worker_receivers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_offset_tracker_contiguous_commits() {
        let tracker = OffsetTracker::new(0);

        // Mark offsets out of order
        assert!(tracker.mark_processed(0).await); // Advances frontier: next=0→1
        assert!(!tracker.mark_processed(2).await); // Out of order, doesn't advance: pending={2}
        assert!(!tracker.mark_processed(1).await); // Fills gap and advances past 2: next becomes 3

        // Now 0, 1, and 2 are all committed
        assert_eq!(tracker.get_committable_offset().await, 2);
        assert_eq!(tracker.pending_count().await, 0); // Everything processed
    }

    #[tokio::test]
    async fn test_offset_tracker_watermark() {
        let tracker = OffsetTracker::new(0);

        tracker.mark_processed(5).await;
        assert_eq!(tracker.get_watermark().await, 5);
        assert_eq!(tracker.get_committable_offset().await, -1); // Nothing contiguous yet

        tracker.mark_processed(0).await;
        tracker.mark_processed(1).await;
        tracker.mark_processed(2).await;
        assert_eq!(tracker.get_committable_offset().await, 2);
    }

    #[tokio::test]
    async fn test_worker_pool_builder() {
        let pool = WorkerPoolBuilder::new(4)
            .with_channel_capacity(1000)
            .build();

        assert_eq!(pool.worker_count(), 4);
    }

    #[tokio::test]
    async fn test_worker_pool_coordinator_new() {
        let coordinator = WorkerPoolCoordinator::new(3);
        assert_eq!(coordinator.worker_count(), 3);
        assert_eq!(
            coordinator.offset_tracker.get_committable_offset().await,
            -1
        );
    }

    #[tokio::test]
    async fn test_worker_pool_metrics() {
        let metrics = WorkerPoolMetrics::new(2);
        metrics.set_queue_depth(0, 5).await;
        assert_eq!(metrics.queue_depth_per_worker.lock().await[0], 5);
    }

    #[tokio::test]
    async fn test_offset_tracker_pending_count() {
        let tracker = OffsetTracker::new(0);

        tracker.mark_processed(2).await;
        tracker.mark_processed(3).await;
        assert_eq!(tracker.pending_count().await, 2);

        tracker.mark_processed(0).await;
        tracker.mark_processed(1).await;
        assert_eq!(tracker.pending_count().await, 0);
    }

    #[tokio::test]
    async fn test_offset_tracker_reset() {
        let tracker = OffsetTracker::new(0);

        tracker.mark_processed(1).await;
        tracker.reset(100).await;

        assert_eq!(tracker.get_committable_offset().await, 99);
        assert_eq!(tracker.pending_count().await, 0);
    }
}
