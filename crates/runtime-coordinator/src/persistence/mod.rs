#![forbid(unsafe_code)]

//! Durable metadata for the coordinator: the storage contract and its
//! implementations.
//!
//! [`MetadataStore`] is the seam that lets the coordinator run either
//! single-node or replicated without the rest of the codebase knowing which:
//!
//! | Module | Backend | Used for |
//! |---|---|---|
//! | `sqlite` | SQLite (WAL) | single-node and local development |
//! | `postgres` | PostgreSQL | replicated, HA deployments |
//!
//! Both are held to the same schema, and both record applied migrations in a
//! `schema_migrations` ledger so a database created by an older build converges
//! on startup.

mod postgres;
mod sqlite;

pub use postgres::PostgresMetadataStore;
pub use sqlite::{latest_sqlite_migration_version, SqliteMetadataStore};

use crate::state::{CoordinatorCheckpointRecord, DatasetRecord, RunRecord, WorkerRecord};
use runtime_checkpoint::{ArtifactRecord, CheckpointState as DomainCheckpointState};

/// Boxed error returned by all [`MetadataStore`] methods.
///
/// Using a boxed trait object keeps the trait object-safe and avoids forcing a
/// single error type on SQLite and PostgreSQL implementations.
pub type PersistResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync + 'static>>;

// ─── persistence data transfer types ─────────────────────────────────────────

/// A single shard-progress record as seen by the persistence layer.
#[derive(Clone, Debug)]
pub struct ShardProgressEntry {
    pub run_id: String,
    pub dataset_id: String,
    pub shard_id: String,
    pub worker_id: String,
    pub lease_epoch: u64,
    pub progress_offset: i64,
    pub is_complete: bool,
    pub updated_at_millis: u64,
}

/// A single audit-event record as seen by the persistence layer.
#[derive(Clone, Debug)]
pub struct AuditEventEntry {
    pub occurred_at_millis: u64,
    pub kind: Option<String>,
    pub outcome: Option<String>,
    pub subject: Option<String>,
    pub role: Option<String>,
    pub worker_id: Option<String>,
    pub run_id: Option<String>,
    pub dataset_id: Option<String>,
    pub checkpoint_id: Option<String>,
    pub reason: Option<String>,
    pub request_name: Option<String>,
}

/// Trait that abstracts over the coordinator's metadata persistence back-end.
///
/// Both [`SqliteMetadataStore`] (single-node, local dev) and
/// [`PostgresMetadataStore`] (distributed, production)
/// implement this trait so [`crate::state::CoordinatorState`] can hold an
/// `Arc<dyn MetadataStore>` without caring which back-end is in use.
pub trait MetadataStore: Send + Sync {
    // ── original methods ──────────────────────────────────────────────────────
    fn save_run(&self, run: &RunRecord) -> PersistResult<()>;
    /// Load every persisted run.  Called once on coordinator startup to
    /// re-hydrate the run registry (durable run lifecycle states).
    fn load_all_runs(&self) -> PersistResult<Vec<RunRecord>>;
    fn save_worker(&self, w: &WorkerRecord, world_size: u32) -> PersistResult<()>;
    fn save_dataset(&self, d: &DatasetRecord) -> PersistResult<()>;
    fn save_checkpoint(
        &self,
        run_id: &str,
        idempotency_key: &str,
        rec: &CoordinatorCheckpointRecord,
    ) -> PersistResult<()>;
    fn save_checkpoint_artifact(
        &self,
        run_id: &str,
        checkpoint_id: &str,
        a: &ArtifactRecord,
    ) -> PersistResult<()>;
    fn mark_checkpoint_committed(
        &self,
        run_id: &str,
        checkpoint_id: &str,
        committed_at_millis: u64,
    ) -> PersistResult<()>;
    fn load_latest_checkpoint(
        &self,
        run_id: &str,
    ) -> PersistResult<Option<CoordinatorCheckpointRecord>>;
    /// All checkpoints for a run, newest step first, with artifacts.
    fn load_checkpoints_for_run(
        &self,
        run_id: &str,
    ) -> PersistResult<Vec<CoordinatorCheckpointRecord>>;
    /// Mark a checkpoint FAILED (corrupt-checkpoint reporting).
    fn mark_checkpoint_failed(&self, run_id: &str, checkpoint_id: &str) -> PersistResult<()>;
    /// Point (or clear) the run's latest-committed-checkpoint pointer.
    fn set_latest_checkpoint(&self, run_id: &str, latest: Option<(&str, u64)>)
        -> PersistResult<()>;
    /// Remove a checkpoint's rows entirely (retention GC).
    fn delete_checkpoint(&self, run_id: &str, checkpoint_id: &str) -> PersistResult<()>;

    // ── shard-progress persistence (resume-from-mid-shard) ───────────────────

    /// Upsert a shard-progress record.  Keyed on (run_id, dataset_id, shard_id).
    fn save_shard_progress(&self, p: &ShardProgressEntry) -> PersistResult<()>;

    /// Load all shard-progress records (across all runs).  Called once on
    /// coordinator startup to re-hydrate the in-memory DashMap.
    fn load_all_shard_progress(&self) -> PersistResult<Vec<ShardProgressEntry>>;

    // ── distributed 2PC checkpoint commit tracking ───────────────────────────

    /// Record that *worker_id* voted to commit *checkpoint_id*.
    /// Idempotent — duplicate calls are silently ignored.
    fn record_checkpoint_commit(
        &self,
        run_id: &str,
        checkpoint_id: &str,
        worker_id: &str,
        committed_at_millis: u64,
    ) -> PersistResult<()>;

    /// Return every (run_id, checkpoint_id, worker_id) commit vote.
    /// Called once on coordinator startup to re-hydrate the in-memory vote
    /// set for any in-progress checkpoints.
    fn load_all_checkpoint_commits(&self) -> PersistResult<Vec<(String, String, String)>>;

    // ── durable audit log ────────────────────────────────────────────────────

    /// Append one audit event to durable storage.
    fn insert_audit_event(&self, event: &AuditEventEntry) -> PersistResult<()>;

    // ── durable run state machine (full hydration on restart) ────────────────

    /// Upsert one idempotency-key mapping.
    /// `scope` IN (run, worker, dataset, checkpoint, checkpoint_artifact).
    fn save_idempotency_key(&self, scope: &str, key: &str, value: &str) -> PersistResult<()>;

    /// Return every (scope, key, value) idempotency mapping.
    fn load_all_idempotency_keys(&self) -> PersistResult<Vec<(String, String, String)>>;

    /// Return every worker across all runs. Deadline fields are placeholders;
    /// the caller must re-arm liveness deadlines after a restart.
    fn load_all_workers(&self) -> PersistResult<Vec<WorkerRecord>>;

    /// Return every dataset (with shards) across all runs.
    fn load_all_datasets(&self) -> PersistResult<Vec<DatasetRecord>>;

    /// Return every checkpoint (any state) with artifacts and the expected
    /// worker set, as `(run_id, record)` pairs.
    fn load_all_checkpoints(&self) -> PersistResult<Vec<(String, CoordinatorCheckpointRecord)>>;
}

/// Map a persisted checkpoint-state integer back to the domain enum.
///
/// Shared by both stores: the column is written as `i32` in each, and an
/// unrecognised value degrades to `Initiated` rather than panicking, so a
/// database written by a newer build stays readable.
/// Round-trip a persisted `state as i32` back to the domain enum.
pub(crate) fn checkpoint_state_from_i32(value: i32) -> DomainCheckpointState {
    use DomainCheckpointState as S;
    for candidate in [
        S::Initiated,
        S::UploadInProgress,
        S::Validating,
        S::Committed,
        S::Failed,
        S::Aborted,
    ] {
        if candidate as i32 == value {
            return candidate;
        }
    }
    S::Initiated
}
