#![forbid(unsafe_code)]

use std::sync::atomic::{AtomicU64, Ordering};

pub mod auth;
pub mod persistence;
pub mod service;
pub mod state;

#[derive(Debug, Default)]
pub struct RuntimeMetrics {
    active_workers: AtomicU64,
    stale_workers: AtomicU64,
    checkpoint_writes: AtomicU64,
    failed_writes: AtomicU64,
    shard_assignment_count: AtomicU64,
    storage_bytes_read: AtomicU64,
    storage_bytes_written: AtomicU64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeMetricsSnapshot {
    pub active_workers: u64,
    pub stale_workers: u64,
    pub checkpoint_writes: u64,
    pub failed_writes: u64,
    pub shard_assignment_count: u64,
    pub storage_bytes_read: u64,
    pub storage_bytes_written: u64,
}

impl RuntimeMetrics {
    pub fn set_active_workers(&self, value: u64) {
        self.active_workers.store(value, Ordering::Relaxed);
    }

    pub fn set_stale_workers(&self, value: u64) {
        self.stale_workers.store(value, Ordering::Relaxed);
    }

    pub fn inc_checkpoint_writes(&self) {
        self.checkpoint_writes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_failed_writes(&self) {
        self.failed_writes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_shard_assignment_count(&self) {
        self.shard_assignment_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_storage_bytes_read(&self, value: u64) {
        self.storage_bytes_read.fetch_add(value, Ordering::Relaxed);
    }

    pub fn add_storage_bytes_written(&self, value: u64) {
        self.storage_bytes_written
            .fetch_add(value, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> RuntimeMetricsSnapshot {
        RuntimeMetricsSnapshot {
            active_workers: self.active_workers.load(Ordering::Relaxed),
            stale_workers: self.stale_workers.load(Ordering::Relaxed),
            checkpoint_writes: self.checkpoint_writes.load(Ordering::Relaxed),
            failed_writes: self.failed_writes.load(Ordering::Relaxed),
            shard_assignment_count: self.shard_assignment_count.load(Ordering::Relaxed),
            storage_bytes_read: self.storage_bytes_read.load(Ordering::Relaxed),
            storage_bytes_written: self.storage_bytes_written.load(Ordering::Relaxed),
        }
    }
}

pub fn init_tracing_from_env() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_snapshot_defaults_to_zero() {
        let metrics = RuntimeMetrics::default();
        assert_eq!(metrics.snapshot(), RuntimeMetricsSnapshot::default());
    }

    #[test]
    fn metrics_counters_and_gauges_update() {
        let metrics = RuntimeMetrics::default();
        metrics.set_active_workers(10);
        metrics.set_stale_workers(2);
        metrics.inc_checkpoint_writes();
        metrics.inc_checkpoint_writes();
        metrics.inc_failed_writes();
        metrics.inc_shard_assignment_count();
        metrics.inc_shard_assignment_count();
        metrics.add_storage_bytes_read(2048);
        metrics.add_storage_bytes_written(1024);

        assert_eq!(
            metrics.snapshot(),
            RuntimeMetricsSnapshot {
                active_workers: 10,
                stale_workers: 2,
                checkpoint_writes: 2,
                failed_writes: 1,
                shard_assignment_count: 2,
                storage_bytes_read: 2048,
                storage_bytes_written: 1024,
            }
        );
    }

    #[test]
    fn tracing_init_is_idempotent() {
        init_tracing_from_env();
        init_tracing_from_env();
    }
}
