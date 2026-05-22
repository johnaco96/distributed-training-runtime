#![forbid(unsafe_code)]
// tonic::Status is large by design; boxing it throughout the gRPC service layer
// would add noise without benefit since Status is always the Err path, not the hot path.
#![allow(clippy::result_large_err)]

use std::sync::atomic::{AtomicU64, Ordering};

pub mod audit;
pub mod auth;
pub mod ha_lock;
pub mod persistence;
pub mod service;
pub mod state;
pub mod telemetry;

#[derive(Debug, Default)]
pub struct RuntimeMetrics {
    runs_total: AtomicU64,
    datasets_total: AtomicU64,
    workers_total: AtomicU64,
    active_workers: AtomicU64,
    stale_workers: AtomicU64,
    failed_workers: AtomicU64,
    heartbeats_total: AtomicU64,
    shard_assignment_count: AtomicU64,
    checkpoints_started_total: AtomicU64,
    checkpoint_writes: AtomicU64,
    checkpoint_artifacts_reported_total: AtomicU64,
    latest_checkpoint_reads_total: AtomicU64,
    failed_writes: AtomicU64,
    auth_allowed_total: AtomicU64,
    auth_denied_total: AtomicU64,
    storage_bytes_read: AtomicU64,
    storage_bytes_written: AtomicU64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeMetricsSnapshot {
    pub runs_total: u64,
    pub datasets_total: u64,
    pub workers_total: u64,
    pub active_workers: u64,
    pub stale_workers: u64,
    pub failed_workers: u64,
    pub heartbeats_total: u64,
    pub checkpoint_writes: u64,
    pub checkpoints_started_total: u64,
    pub checkpoint_artifacts_reported_total: u64,
    pub latest_checkpoint_reads_total: u64,
    pub failed_writes: u64,
    pub shard_assignment_count: u64,
    pub auth_allowed_total: u64,
    pub auth_denied_total: u64,
    pub storage_bytes_read: u64,
    pub storage_bytes_written: u64,
}

impl RuntimeMetrics {
    pub fn inc_runs_total(&self) {
        self.runs_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_datasets_total(&self) {
        self.datasets_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_workers_total(&self) {
        self.workers_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn set_active_workers(&self, v: u64) {
        self.active_workers.store(v, Ordering::Relaxed);
    }
    pub fn set_stale_workers(&self, v: u64) {
        self.stale_workers.store(v, Ordering::Relaxed);
    }
    pub fn set_failed_workers(&self, v: u64) {
        self.failed_workers.store(v, Ordering::Relaxed);
    }
    pub fn inc_heartbeats_total(&self) {
        self.heartbeats_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_checkpoint_starts(&self) {
        self.checkpoints_started_total
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_checkpoint_writes(&self) {
        self.checkpoint_writes.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_checkpoint_artifacts_reported_total(&self) {
        self.checkpoint_artifacts_reported_total
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_latest_checkpoint_reads_total(&self) {
        self.latest_checkpoint_reads_total
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_failed_writes(&self) {
        self.failed_writes.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_shard_assignment_count(&self) {
        self.shard_assignment_count.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_auth_allowed_total(&self) {
        self.auth_allowed_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_auth_denied_total(&self) {
        self.auth_denied_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn add_storage_bytes_read(&self, v: u64) {
        self.storage_bytes_read.fetch_add(v, Ordering::Relaxed);
    }
    pub fn add_storage_bytes_written(&self, v: u64) {
        self.storage_bytes_written.fetch_add(v, Ordering::Relaxed);
    }
    pub fn snapshot(&self) -> RuntimeMetricsSnapshot {
        RuntimeMetricsSnapshot {
            runs_total: self.runs_total.load(Ordering::Relaxed),
            datasets_total: self.datasets_total.load(Ordering::Relaxed),
            workers_total: self.workers_total.load(Ordering::Relaxed),
            active_workers: self.active_workers.load(Ordering::Relaxed),
            stale_workers: self.stale_workers.load(Ordering::Relaxed),
            failed_workers: self.failed_workers.load(Ordering::Relaxed),
            heartbeats_total: self.heartbeats_total.load(Ordering::Relaxed),
            checkpoint_writes: self.checkpoint_writes.load(Ordering::Relaxed),
            checkpoints_started_total: self.checkpoints_started_total.load(Ordering::Relaxed),
            checkpoint_artifacts_reported_total: self
                .checkpoint_artifacts_reported_total
                .load(Ordering::Relaxed),
            latest_checkpoint_reads_total: self
                .latest_checkpoint_reads_total
                .load(Ordering::Relaxed),
            failed_writes: self.failed_writes.load(Ordering::Relaxed),
            shard_assignment_count: self.shard_assignment_count.load(Ordering::Relaxed),
            auth_allowed_total: self.auth_allowed_total.load(Ordering::Relaxed),
            auth_denied_total: self.auth_denied_total.load(Ordering::Relaxed),
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
