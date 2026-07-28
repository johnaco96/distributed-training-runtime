//! The coordinator's in-memory state, and the rules that govern it.
//!
//! [`CoordinatorState`] is a single type with a single lock discipline (see
//! its docs), but it answers to four independent concerns. Each lives in its
//! own file:
//!
//! | Module | Owns |
//! |---|---|
//! | `runs` | run registration, the durable lifecycle state machine, admin actions |
//! | `workers` | registration, heartbeat liveness, deregistration, rendezvous |
//! | `checkpoints` | the distributed two-phase checkpoint commit |
//! | `shards` | dataset registration, shard assignment, leases, progress |
//!
//! This module keeps what they share: the state struct itself, the record and
//! error types, construction, and rehydration from the metadata store on
//! restart.
#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime};

use dashmap::DashMap;

use runtime_checkpoint::{
    ArtifactRecord, CheckpointError, CheckpointId as DomainCheckpointId, CheckpointRecord,
    CheckpointState as DomainCheckpointState, RunId as DomainRunId, WorkerId as DomainWorkerId,
};
use runtime_proto::runtime::v1::{
    CheckpointId, CheckpointState, DatasetId, DatasetMetadata, DeregisterReason,
    RegisterDatasetRequest, RegisterRunRequest, RegisterWorkerRequest, RunId, RunState, ShardId,
    ShardLeaseState, ShardMetadata, WorkerId, WorkerState,
};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const WORKER_FAILURE_INTERVAL: Duration = Duration::from_secs(90);
const SHARD_LEASE_DURATION: Duration = Duration::from_secs(30);

#[cfg(test)]
use crate::persistence::SqliteMetadataStore;
use crate::persistence::{MetadataStore, ShardProgressEntry};
use crate::rendezvous::{RendezvousConfig, RendezvousSnapshot, RunRendezvous};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DatasetPlacementPolicy {
    Deterministic,
    CacheAware,
}

/// Shared coordinator state — lock-free at the collection level via DashMap.
///
/// Each top-level map is a [`DashMap`] that shards keys across independent
/// read-write locks.  Nested per-run collections (workers, datasets,
/// checkpoints) are stored as `Arc<RwLock<HashMap>>`, so operations on
/// different runs are fully concurrent: a heartbeat for run A does not block
/// any operation for run B.
///
/// Concurrency properties:
/// - Different-run operations: non-contending (separate DashMap shards + per-run locks)
/// - Same-run reads (shard assignment, heartbeat read): concurrent via `RwLock::read`
/// - Same-run writes (heartbeat update, begin_checkpoint): serialised per run
/// - Shard progress updates: lock-free per shard via DashMap
/// - Idempotency checks: atomic via `DashMap::entry()` shard-level locking
pub struct CoordinatorState {
    // Run registry — read-mostly after registration
    runs: DashMap<String, RunRecord>,
    run_idempotency_keys: DashMap<String, String>,

    // Per-run worker state.  Arc<RwLock<...>> gives per-run serialisation
    // while allowing concurrent access across different runs.
    workers_by_run: DashMap<String, Arc<RwLock<HashMap<String, WorkerRecord>>>>,
    worker_idempotency_keys: DashMap<String, String>,

    // Per-run dataset + shard metadata
    datasets_by_run: DashMap<String, Arc<RwLock<HashMap<String, DatasetRecord>>>>,
    dataset_idempotency_keys: DashMap<String, String>,

    // Per-shard progress — lock-free concurrent updates via DashMap
    shard_progress: DashMap<ShardProgressKey, ShardProgressRecord>,

    // Per-run checkpoint records
    checkpoints_by_run: DashMap<String, Arc<RwLock<HashMap<String, CoordinatorCheckpointRecord>>>>,
    checkpoint_idempotency_keys: DashMap<String, String>,
    checkpoint_artifact_idempotency_keys: DashMap<String, String>,

    // per-checkpoint commit votes for distributed 2PC.
    // Key: (run_id, checkpoint_id) → set of worker_ids that called CommitCheckpoint.
    // The global commit fires when commit_set.len() == expected_artifact_count.
    checkpoint_commits: DashMap<(String, String), Arc<Mutex<HashSet<String>>>>,

    // Per-run rendezvous barrier for torch.distributed process-group
    // formation.  In-memory only: after a coordinator restart or failover the
    // workers re-join and form a fresh round (round numbers are wall-clock
    // seeded so they stay monotonic).
    rendezvous_by_run: DashMap<String, Arc<Mutex<RunRendezvous>>>,

    placement_policy: DatasetPlacementPolicy,
    /// Optional persistence back-end.  Either [`SqliteMetadataStore`] (single-
    /// node) or [`crate::pg_store::PostgresMetadataStore`] (distributed HA).
    /// Wrapped in Arc so it can be shared with the audit sink.
    metadata_store: Option<Arc<dyn MetadataStore>>,
    next_run_id: AtomicU64,
    next_worker_id: AtomicU64,
    next_dataset_id: AtomicU64,
    next_shard_id: AtomicU64,
    next_checkpoint_id: AtomicU64,
}

// ─── record types ────────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub struct RunRecord {
    pub run_id: String,
    pub run_name: String,
    pub assignment_policy: i32,
    pub storage_backend_kind: i32,
    pub world_size: u32,
    pub run_version: String,
    pub registered_at: SystemTime,
    pub active_worker_count: u32,
    pub latest_checkpoint_id: Option<CheckpointId>,
    pub latest_checkpoint_state: i32,
    /// Elastic training bounds; both zero for fixed-size runs.
    pub min_world_size: u32,
    pub max_world_size: u32,
    /// Elastic rendezvous settle window in milliseconds; 0 = server default.
    pub rendezvous_settle_timeout_ms: u32,
    /// Durable run lifecycle state ([`RunState`] as i32); see the proto for
    /// the transition diagram.
    pub state: i32,
    /// Human-readable explanation of the current state.
    pub state_reason: String,
    /// Registration idempotency key, persisted so a restarted coordinator
    /// still deduplicates RegisterRun retries.
    pub idempotency_key: String,
    /// When `state` last changed (in-memory only; used for retention cleanup
    /// of terminal runs — the clock restarts after a coordinator restart).
    pub state_updated_at: SystemTime,
    /// Number of times this run has been administratively retried after a
    /// failure (persisted).
    pub retry_count: u32,
}

impl RunRecord {
    pub fn is_terminal(&self) -> bool {
        matches!(
            RunState::try_from(self.state),
            Ok(RunState::Completed) | Ok(RunState::Failed) | Ok(RunState::Cancelled)
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct WorkerRecord {
    pub run_id: String,
    pub worker_id: String,
    pub rank: u32,
    pub host: String,
    pub sdk_version: String,
    pub protocol_version: String,
    pub state: i32,
    pub registered_at: SystemTime,
    pub last_heartbeat_at: SystemTime,
    pub next_heartbeat_deadline: SystemTime,
    pub failure_deadline: SystemTime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DatasetRecord {
    pub run_id: String,
    pub dataset_id: String,
    pub name: String,
    pub dataset_uri: String,
    pub format: String,
    pub shard_count: u32,
    pub dataset_version: String,
    pub labels: HashMap<String, String>,
    pub registered_at: SystemTime,
    pub shards: Vec<ShardRecord>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ShardRecord {
    pub shard_id: String,
    pub dataset_id: String,
    pub shard_uri: String,
    pub byte_range_start: i64,
    pub byte_range_end: i64,
    pub size_bytes: i64,
    pub checksum: String,
    pub checksum_algorithm: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ShardProgressKey {
    run_id: String,
    dataset_id: String,
    shard_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ShardProgressRecord {
    pub run_id: String,
    pub dataset_id: String,
    pub shard_id: String,
    pub worker_id: String,
    pub lease_epoch: u64,
    pub progress_offset: i64,
    pub is_complete: bool,
    pub updated_at: SystemTime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RegisteredRun {
    pub run_id: RunId,
    pub registered_at: SystemTime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RegisteredWorker {
    pub worker_id: WorkerId,
    pub state: i32,
    pub lease_renewal_deadline: SystemTime,
    pub server_time: SystemTime,
    pub is_new: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WorkerHeartbeat {
    pub state: i32,
    pub next_heartbeat_deadline: SystemTime,
    pub server_time: SystemTime,
    /// Current run lifecycle state, so workers observe cancellation and
    /// other terminal transitions without extra RPCs.
    pub run_state: i32,
    /// Whether this heartbeat changed the worker's state (e.g. Stale →
    /// Active). Steady-state heartbeats leave counts untouched, so callers
    /// can skip the O(workers) recount that would otherwise run on every
    /// heartbeat of every worker.
    pub state_changed: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RegisteredDataset {
    pub dataset_id: DatasetId,
    pub accepted_shard_count: u32,
    pub registered_at: SystemTime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ShardAssignmentRecord {
    pub run_id: String,
    pub dataset_id: String,
    pub shard_id: String,
    pub worker_id: String,
    pub lease_epoch: u64,
    pub lease_state: i32,
    pub lease_expires_at: SystemTime,
    pub progress_offset: i64,
    pub shard_uri: String,
    pub checksum: String,
    pub checksum_algorithm: String,
    pub size_bytes: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ShardAssignments {
    pub assignments: Vec<ShardAssignmentRecord>,
    pub assignment_policy: i32,
    pub server_time: SystemTime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RenewedShardLease {
    pub accepted: bool,
    pub lease_state: i32,
    pub lease_expires_at: SystemTime,
    pub server_time: SystemTime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReportedShardProgress {
    pub accepted: bool,
    pub stored_progress_offset: i64,
    pub lease_state: i32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CoordinatorCheckpointRecord {
    pub checkpoint: CheckpointRecord,
    pub initiated_by_worker_id: String,
    pub committed_at_millis: Option<u64>,
    pub expected_worker_ids: Vec<String>,
    pub expected_artifact_count: u32,
}

// ─── error types ─────────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BeginCheckpointError {
    RunNotFound,
    WorkerNotFound,
    InvalidWorkerId,
    CheckpointConflict,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReportCheckpointArtifactError {
    RunNotFound,
    WorkerNotFound,
    CheckpointNotFound,
    InvalidInput(&'static str),
    InvalidState,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommitCheckpointError {
    RunNotFound,
    CheckpointNotFound,
    InvalidInput(&'static str),
    InvalidState,
    /// The metadata store rejected the commit-vote write.  The vote was not
    /// recorded; the caller should retry the commit.
    PersistenceUnavailable,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GetLatestCheckpointError {
    InvalidInput(&'static str),
    RunNotFound,
    NoCommittedCheckpoint,
    LatestCheckpointMissing,
    LatestCheckpointNotCommitted,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CommitCheckpointResult {
    pub record: CoordinatorCheckpointRecord,
    pub committed_at_millis: u64,
    /// `true` when all workers have voted and the checkpoint is durably committed;
    /// `false` when this worker's vote was recorded but other workers have not
    /// yet called CommitCheckpoint.
    pub globally_committed: bool,
}

/// A checkpoint dropped by the retention sweep, and the artifacts it owned.
///
/// Retention deletes metadata; the bytes are the caller's to reclaim. The
/// artifact URIs are carried out here because removing the records is the last
/// moment they are knowable — see
/// [`CoordinatorState::enforce_checkpoint_retention`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReclaimedCheckpoint {
    pub run_id: String,
    pub checkpoint_id: String,
    /// As reported by workers: an `s3://…` URI on the worker-direct path, or a
    /// coordinator-relative storage key for artifacts streamed through it.
    pub artifact_uris: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoordinatorStatusSnapshot {
    pub run_count: usize,
    pub worker_count: usize,
    pub active_worker_count: u64,
    pub checkpoint_count: usize,
    pub latest_checkpoint_id: Option<String>,
}
pub enum GetShardAssignmentsError {
    RunNotFound,
    WorkerNotFound,
    DatasetNotFound,
    EmptyWorldSize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RenewShardLeaseError {
    RunNotFound,
    WorkerNotFound,
    ShardNotFound,
    EmptyWorldSize,
    NotShardOwner,
    InvalidLeaseEpoch,
    WorkerNotActive,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RendezvousError {
    RunNotFound,
    WorkerNotFound,
    MissingHost,
    /// The run is in a terminal state; the worker should exit instead of
    /// re-forming a process group.
    RunTerminal(i32),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeregisterWorkerError {
    RunNotFound,
    WorkerNotFound,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ListCheckpointsError {
    RunNotFound,
    PersistenceUnavailable,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReportCorruptError {
    RunNotFound,
    CheckpointNotFound,
    PersistenceUnavailable,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CancelRunError {
    RunNotFound,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunActionError {
    RunNotFound,
    /// The action is not valid from the run's current state.
    InvalidState {
        current: i32,
        action: &'static str,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReportShardProgressError {
    RunNotFound,
    WorkerNotFound,
    DatasetNotFound,
    ShardNotFound,
    EmptyWorldSize,
    NotShardOwner,
    InvalidLeaseEpoch,
    WorkerNotActive,
}

// ─── CoordinatorState implementation ─────────────────────────────────────────

impl Default for CoordinatorState {
    fn default() -> Self {
        Self {
            runs: DashMap::new(),
            run_idempotency_keys: DashMap::new(),
            workers_by_run: DashMap::new(),
            worker_idempotency_keys: DashMap::new(),
            datasets_by_run: DashMap::new(),
            dataset_idempotency_keys: DashMap::new(),
            shard_progress: DashMap::new(),
            checkpoints_by_run: DashMap::new(),
            checkpoint_idempotency_keys: DashMap::new(),
            checkpoint_artifact_idempotency_keys: DashMap::new(),
            checkpoint_commits: DashMap::new(),
            rendezvous_by_run: DashMap::new(),
            metadata_store: None,
            next_run_id: AtomicU64::new(0),
            next_worker_id: AtomicU64::new(0),
            next_dataset_id: AtomicU64::new(0),
            next_shard_id: AtomicU64::new(0),
            next_checkpoint_id: AtomicU64::new(0),
            placement_policy: DatasetPlacementPolicy::Deterministic,
        }
    }
}

// `CoordinatorState` is one type with one lock discipline, but its surface
// spans four independent concerns. Rust lets an inherent `impl` be split
// across modules of the same crate, so each concern lives in its own file
// while the public API (`crate::state::CoordinatorState`) is unchanged.
mod checkpoints;
mod runs;
mod shards;
mod workers;

use workers::refresh_active_worker_count;

impl CoordinatorState {
    pub fn with_placement_policy(placement_policy: DatasetPlacementPolicy) -> Self {
        Self {
            placement_policy,
            ..Self::default()
        }
    }

    /// Select the shard placement policy after construction — used by the
    /// binary to honour `--cache-aware-placement` regardless of which
    /// metadata-store constructor built the state.
    pub fn set_placement_policy(&mut self, placement_policy: DatasetPlacementPolicy) {
        self.placement_policy = placement_policy;
    }

    pub fn with_metadata_store(metadata_store: Arc<dyn MetadataStore>) -> Self {
        let mut state = Self {
            metadata_store: Some(Arc::clone(&metadata_store)),
            ..Self::default()
        };
        state.hydrate_from_store(metadata_store.as_ref());
        state
    }

    /// Re-hydrate in-memory caches from the durable store after a coordinator restart.
    ///
    /// Called once inside [`with_metadata_store`] before the gRPC server starts
    /// so the coordinator can resume mid-shard reads and in-flight 2PC commits
    /// without requiring workers to re-send progress reports.
    fn hydrate_from_store(&mut self, store: &dyn MetadataStore) {
        // ── Full run state machine hydration ────────────────────────────────
        // Runs (with durable lifecycle state), workers, datasets, checkpoints
        // (including in-flight 2PC), and every idempotency map come back, so
        // clients continue mid-run across a coordinator restart or HA
        // failover without re-registering anything.  Terminal runs stay
        // queryable until retention cleanup.

        if let Ok(runs) = store.load_all_runs() {
            for run in runs {
                bump_counter_from_id(&self.next_run_id, &run.run_id, "run-");
                // The runs table also carries the registration idempotency
                // key (belt-and-braces with the idempotency_keys table below).
                if !run.idempotency_key.is_empty() {
                    self.run_idempotency_keys
                        .insert(run.idempotency_key.clone(), run.run_id.clone());
                }
                self.runs.insert(run.run_id.clone(), run);
            }
        }

        let now = SystemTime::now();
        if let Ok(workers) = store.load_all_workers() {
            for mut worker in workers {
                bump_counter_from_id(&self.next_worker_id, &worker.worker_id, "worker-");
                // Re-arm liveness: pre-crash deadlines cannot be trusted after
                // a restart.  The worker keeps its persisted state but gets a
                // fresh window to heartbeat before reconciliation marks it
                // stale/failed.
                worker.next_heartbeat_deadline = now + HEARTBEAT_INTERVAL;
                worker.failure_deadline = now + WORKER_FAILURE_INTERVAL;
                let workers_arc = self
                    .workers_by_run
                    .entry(worker.run_id.clone())
                    .or_insert_with(|| Arc::new(RwLock::new(HashMap::new())))
                    .clone();
                workers_arc
                    .write()
                    .unwrap()
                    .insert(worker.worker_id.clone(), worker);
            }
        }
        // active_worker_count is derived state — recompute from hydrated workers.
        let hydrated_run_ids: Vec<String> = self
            .workers_by_run
            .iter()
            .map(|e| e.key().clone())
            .collect();
        for run_id in hydrated_run_ids {
            refresh_active_worker_count(&self.runs, &self.workers_by_run, &run_id);
        }

        if let Ok(datasets) = store.load_all_datasets() {
            for dataset in datasets {
                bump_counter_from_id(&self.next_dataset_id, &dataset.dataset_id, "dataset-");
                for shard in &dataset.shards {
                    bump_counter_from_id(&self.next_shard_id, &shard.shard_id, "shard-");
                }
                let datasets_arc = self
                    .datasets_by_run
                    .entry(dataset.run_id.clone())
                    .or_insert_with(|| Arc::new(RwLock::new(HashMap::new())))
                    .clone();
                datasets_arc
                    .write()
                    .unwrap()
                    .insert(dataset.dataset_id.clone(), dataset);
            }
        }

        if let Ok(checkpoints) = store.load_all_checkpoints() {
            for (run_id, record) in checkpoints {
                bump_counter_from_id(
                    &self.next_checkpoint_id,
                    &record.checkpoint.checkpoint_id.0,
                    "checkpoint-",
                );
                let checkpoints_arc = self
                    .checkpoints_by_run
                    .entry(run_id)
                    .or_insert_with(|| Arc::new(RwLock::new(HashMap::new())))
                    .clone();
                checkpoints_arc
                    .write()
                    .unwrap()
                    .insert(record.checkpoint.checkpoint_id.0.clone(), record);
            }
        }

        if let Ok(keys) = store.load_all_idempotency_keys() {
            for (scope, key, value) in keys {
                match scope.as_str() {
                    "run" => self.run_idempotency_keys.insert(key, value),
                    "worker" => self.worker_idempotency_keys.insert(key, value),
                    "dataset" => self.dataset_idempotency_keys.insert(key, value),
                    "checkpoint" => self.checkpoint_idempotency_keys.insert(key, value),
                    "checkpoint_artifact" => {
                        self.checkpoint_artifact_idempotency_keys.insert(key, value)
                    }
                    _ => None,
                };
            }
        }
        // restore shard-progress offsets so workers get the correct
        // resume position on GetShardAssignment after a coordinator restart.
        if let Ok(entries) = store.load_all_shard_progress() {
            for entry in entries {
                let key = ShardProgressKey {
                    run_id: entry.run_id.clone(),
                    dataset_id: entry.dataset_id.clone(),
                    shard_id: entry.shard_id.clone(),
                };
                let record = ShardProgressRecord {
                    run_id: entry.run_id,
                    dataset_id: entry.dataset_id,
                    shard_id: entry.shard_id,
                    worker_id: entry.worker_id,
                    lease_epoch: entry.lease_epoch,
                    progress_offset: entry.progress_offset,
                    is_complete: entry.is_complete,
                    updated_at: std::time::UNIX_EPOCH
                        + Duration::from_millis(entry.updated_at_millis),
                };
                self.shard_progress.insert(key, record);
            }
        }
        // restore per-worker commit votes so in-flight 2PC commits that
        // arrived before the restart are not lost and the vote count is correct.
        if let Ok(commits) = store.load_all_checkpoint_commits() {
            for (run_id, checkpoint_id, worker_id) in commits {
                let key = (run_id, checkpoint_id);
                let set_arc = self
                    .checkpoint_commits
                    .entry(key)
                    .or_insert_with(|| Arc::new(Mutex::new(HashSet::new())))
                    .clone();
                set_arc.lock().unwrap().insert(worker_id);
            }
        }
    }

    /// Persist one idempotency-key mapping so retried RPCs stay idempotent
    /// across coordinator restarts.
    fn persist_idempotency_key(&self, scope: &str, key: &str, value: &str) {
        if key.is_empty() {
            return;
        }
        if let Some(store) = &self.metadata_store {
            store
                .save_idempotency_key(scope, key, value)
                .expect("persist idempotency key");
        }
    }

    pub fn status_snapshot(&self) -> CoordinatorStatusSnapshot {
        let run_count = self.runs.len();
        let mut worker_count = 0usize;
        let mut active_worker_count = 0u64;
        for entry in self.workers_by_run.iter() {
            let workers = entry.value().read().unwrap();
            worker_count += workers.len();
            active_worker_count += workers
                .values()
                .filter(|w| w.state == WorkerState::Active as i32)
                .count() as u64;
        }
        let checkpoint_count = self
            .checkpoints_by_run
            .iter()
            .map(|e| e.value().read().unwrap().len())
            .sum();
        let latest_checkpoint_id = self
            .runs
            .iter()
            .filter_map(|e| e.latest_checkpoint_id.as_ref().map(|id| id.value.clone()))
            .max();

        CoordinatorStatusSnapshot {
            run_count,
            worker_count,
            active_worker_count,
            checkpoint_count,
            latest_checkpoint_id,
        }
    }
}

/// Advance a generated-ID counter past a hydrated ID so post-restart
/// generation never collides with persisted entities.
///
/// Only IDs matching the coordinator's own `<prefix><n>` format move the
/// counter; client-supplied IDs are ignored.
fn bump_counter_from_id(counter: &AtomicU64, id: &str, prefix: &str) {
    if let Some(n) = id.strip_prefix(prefix).and_then(|s| s.parse::<u64>().ok()) {
        counter.fetch_max(n, Ordering::Relaxed);
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_millis() as u64
}

fn scoped_idempotency_key(run_id: &str, idempotency_key: &str) -> String {
    if idempotency_key.is_empty() {
        return String::new();
    }
    format!("{run_id}:{idempotency_key}")
}

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::test_support::*;

    use tempfile::tempdir;

    #[test]
    fn full_run_state_survives_coordinator_restart() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("coordinator.db");
        let store = SqliteMetadataStore::open(&path).unwrap();
        store.init_schema().unwrap();
        let state = CoordinatorState::with_metadata_store(Arc::new(store));

        // Register a run, two workers, a dataset, and an in-flight checkpoint
        // with worker A's artifact + 2PC vote already recorded.
        let (run_id, worker_a, worker_b) = setup_two_worker_checkpoint(&state, "ckpt-2pc");
        state
            .register_dataset(dataset_request(run_id.clone(), "dataset-key"))
            .expect("dataset should register");
        let first = state
            .commit_checkpoint(
                run_id.value.clone(),
                "ckpt-2pc".to_string(),
                worker_a.value.clone(),
                "c1".to_string(),
            )
            .unwrap();
        assert!(!first.globally_committed);
        drop(state);

        // Restart the coordinator: everything must come back from the store
        // WITHOUT clients re-registering anything.
        let reopened = SqliteMetadataStore::open(&path).unwrap();
        reopened.init_schema().unwrap();
        let recreated = CoordinatorState::with_metadata_store(Arc::new(reopened));

        // Run metadata survives.
        let run = recreated.get_run(&run_id).expect("run should be hydrated");
        assert_eq!(run.run_name, "test-run");
        assert_eq!(run.world_size, 4);
        assert_eq!(run.run_version, "v1");
        assert_eq!(run.active_worker_count, 2);

        // Idempotent re-registration returns the SAME run (durable key map).
        let replayed = recreated.register_run(register_request("run-key"));
        assert_eq!(replayed.run_id.value, run_id.value);

        // Workers survive, still active, with re-armed liveness deadlines.
        let hydrated_a = recreated
            .get_worker_for_test(&run_id, &worker_a)
            .expect("worker a should be hydrated");
        assert_eq!(hydrated_a.state, WorkerState::Active as i32);
        assert!(hydrated_a.next_heartbeat_deadline > SystemTime::now());
        recreated.reconcile_worker_liveness();
        assert_eq!(
            recreated
                .get_worker_for_test(&run_id, &worker_a)
                .unwrap()
                .state,
            WorkerState::Active as i32,
            "re-armed deadline must survive immediate reconciliation"
        );

        // Dataset + shards survive.
        let dataset_id = DatasetId {
            value: "dataset-test".to_string(),
        };
        let assignments = recreated
            .get_shard_assignments(&run_id, &worker_a, &dataset_id, None)
            .unwrap_or_else(|_| panic!("hydrated dataset should serve shard assignments"));
        assert!(!assignments.assignments.is_empty());

        // The in-flight checkpoint survives with its expected worker set, so
        // worker B's vote alone completes the 2PC commit after the restart.
        let ckpt = recreated
            .get_checkpoint(&run_id, "ckpt-2pc")
            .expect("in-flight checkpoint should be hydrated");
        assert_eq!(ckpt.expected_artifact_count, 2);
        assert_eq!(ckpt.checkpoint.artifacts.len(), 2);
        let second = recreated
            .commit_checkpoint(
                run_id.value.clone(),
                "ckpt-2pc".to_string(),
                worker_b.value,
                "c2".to_string(),
            )
            .expect("post-restart vote should commit");
        assert!(second.globally_committed);

        // ID counters are reseeded: a brand-new run must not collide with the
        // hydrated one.
        let fresh = recreated.register_run(register_request("another-run-key"));
        assert_ne!(fresh.run_id.value, run_id.value);
        assert_eq!(fresh.run_id.value, "run-2");
    }
}
