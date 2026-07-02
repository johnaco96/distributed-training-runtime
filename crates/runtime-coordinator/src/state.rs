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
    CheckpointId, CheckpointState, DatasetId, DatasetMetadata, RegisterDatasetRequest,
    RegisterRunRequest, RegisterWorkerRequest, RunId, ShardId, ShardLeaseState, ShardMetadata,
    WorkerId, WorkerState,
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

    // Gap 2: per-checkpoint commit votes for distributed 2PC.
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

impl CoordinatorState {
    pub fn with_placement_policy(placement_policy: DatasetPlacementPolicy) -> Self {
        Self {
            placement_policy,
            ..Self::default()
        }
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
        // Gap 1: restore shard-progress offsets so workers get the correct
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
        // Gap 2: restore per-worker commit votes so in-flight 2PC commits that
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

    fn ensure_worker_active_for_lease_actions(worker: &WorkerRecord) -> Result<(), WorkerState> {
        if worker.state == WorkerState::Active as i32 {
            return Ok(());
        }
        Err(WorkerState::try_from(worker.state).unwrap_or(WorkerState::Unspecified))
    }

    // ── persistence helpers ──────────────────────────────────────────────────

    fn persist_run(&self, run: &RunRecord) {
        if let Some(store) = &self.metadata_store {
            store.save_run(run).expect("persist run");
        }
    }
    fn persist_worker(&self, worker: &WorkerRecord, world_size: u32) {
        if let Some(store) = &self.metadata_store {
            store
                .save_worker(worker, world_size)
                .expect("persist worker");
        }
    }
    fn persist_dataset(&self, dataset: &DatasetRecord) {
        if let Some(store) = &self.metadata_store {
            store.save_dataset(dataset).expect("persist dataset");
        }
    }
    fn persist_checkpoint(&self, run_id: &str, idem: &str, rec: &CoordinatorCheckpointRecord) {
        if let Some(store) = &self.metadata_store {
            store
                .save_checkpoint(run_id, idem, rec)
                .expect("persist checkpoint");
        }
    }
    fn persist_artifact(&self, run_id: &str, checkpoint_id: &str, artifact: &ArtifactRecord) {
        if let Some(store) = &self.metadata_store {
            store
                .save_checkpoint_artifact(run_id, checkpoint_id, artifact)
                .expect("persist artifact");
        }
    }

    fn persist_shard_progress(&self, progress: &ShardProgressRecord) {
        if let Some(store) = &self.metadata_store {
            let millis = progress
                .updated_at
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let entry = ShardProgressEntry {
                run_id: progress.run_id.clone(),
                dataset_id: progress.dataset_id.clone(),
                shard_id: progress.shard_id.clone(),
                worker_id: progress.worker_id.clone(),
                lease_epoch: progress.lease_epoch,
                progress_offset: progress.progress_offset,
                is_complete: progress.is_complete,
                updated_at_millis: millis,
            };
            // Shard progress is an at-least-once resume hint: if this write is
            // lost the worker simply re-processes a few records after a
            // coordinator restart.  Log and continue rather than crashing the
            // coordinator over a transient DB error in the hot path.
            if let Err(e) = store.save_shard_progress(&entry) {
                tracing::error!(
                    error = %e,
                    run_id = %entry.run_id,
                    shard_id = %entry.shard_id,
                    "failed to persist shard progress (resume offset may be stale)"
                );
            }
        }
    }

    // ── public state methods ─────────────────────────────────────────────────

    pub fn begin_checkpoint(
        &self,
        run_id: String,
        worker_id: String,
        checkpoint_id: String,
        step: u64,
        idempotency_key: String,
    ) -> Result<CoordinatorCheckpointRecord, BeginCheckpointError> {
        if worker_id.trim().is_empty() {
            return Err(BeginCheckpointError::InvalidWorkerId);
        }
        if !self.runs.contains_key(&run_id) {
            return Err(BeginCheckpointError::RunNotFound);
        }
        if !self
            .workers_by_run
            .get(&run_id)
            .is_some_and(|arc| arc.read().unwrap().contains_key(&worker_id))
        {
            return Err(BeginCheckpointError::WorkerNotFound);
        }

        let scoped_key = checkpoint_idempotency_key(&run_id, &idempotency_key);

        // Atomic idempotency check via DashMap entry shard lock.
        // If the key already exists, return the previously-created checkpoint.
        if !scoped_key.is_empty() {
            if let Some(existing_id) = self.checkpoint_idempotency_keys.get(&scoped_key) {
                let existing_id = existing_id.clone();
                let record = self
                    .checkpoints_by_run
                    .get(&run_id)
                    .and_then(|arc| arc.read().unwrap().get(&existing_id).cloned())
                    .expect("checkpoint idempotency key must reference an existing checkpoint");
                return Ok(record);
            }
        }

        let checkpoint_id = if checkpoint_id.trim().is_empty() {
            self.generate_checkpoint_id()
        } else {
            checkpoint_id
        };

        // Get or create the per-run checkpoint map.
        let checkpoints_arc = self
            .checkpoints_by_run
            .entry(run_id.clone())
            .or_insert_with(|| Arc::new(RwLock::new(HashMap::new())))
            .value()
            .clone();
        let mut checkpoints = checkpoints_arc.write().unwrap();

        if let Some(existing) = checkpoints.get(&checkpoint_id) {
            // Any worker may join an in-progress checkpoint for the same step.
            if existing.checkpoint.step == step {
                return Ok(existing.clone());
            }
            return Err(BeginCheckpointError::CheckpointConflict);
        }

        let now_millis = now_millis();
        let expected_worker_ids = self
            .workers_by_run
            .get(&run_id)
            .map(|arc| {
                arc.read()
                    .unwrap()
                    .values()
                    .filter(|w| w.state == WorkerState::Active as i32)
                    .map(|w| w.worker_id.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let record = CoordinatorCheckpointRecord {
            checkpoint: CheckpointRecord::new(
                DomainRunId(run_id.clone()),
                DomainCheckpointId(checkpoint_id.clone()),
                step,
                now_millis,
            ),
            initiated_by_worker_id: worker_id,
            committed_at_millis: None,
            expected_artifact_count: expected_worker_ids.len() as u32,
            expected_worker_ids,
        };
        checkpoints.insert(checkpoint_id.clone(), record.clone());
        drop(checkpoints);

        if !idempotency_key.is_empty() {
            self.checkpoint_idempotency_keys
                .insert(scoped_key, checkpoint_id);
        }
        Ok(record)
    }

    pub fn get_checkpoint(
        &self,
        run_id: &RunId,
        checkpoint_id: &str,
    ) -> Option<CoordinatorCheckpointRecord> {
        self.checkpoints_by_run
            .get(&run_id.value)
            .and_then(|arc| arc.read().unwrap().get(checkpoint_id).cloned())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn report_checkpoint_artifact(
        &self,
        run_id: String,
        worker_id: String,
        checkpoint_id: String,
        artifact_name: String,
        artifact_uri: String,
        size_bytes: u64,
        checksum: String,
        checksum_algorithm: String,
        idempotency_key: String,
    ) -> Result<CoordinatorCheckpointRecord, ReportCheckpointArtifactError> {
        if artifact_name.trim().is_empty() {
            return Err(ReportCheckpointArtifactError::InvalidInput(
                "artifact_name must be non-empty",
            ));
        }
        if artifact_uri.trim().is_empty() {
            return Err(ReportCheckpointArtifactError::InvalidInput(
                "artifact_uri must be non-empty",
            ));
        }
        if !self.runs.contains_key(&run_id) {
            return Err(ReportCheckpointArtifactError::RunNotFound);
        }
        if !self
            .workers_by_run
            .get(&run_id)
            .is_some_and(|arc| arc.read().unwrap().contains_key(&worker_id))
        {
            return Err(ReportCheckpointArtifactError::WorkerNotFound);
        }

        let scoped_idempotency_key =
            checkpoint_artifact_idempotency_key(&run_id, &checkpoint_id, &idempotency_key);
        if !idempotency_key.is_empty()
            && self
                .checkpoint_artifact_idempotency_keys
                .contains_key(&scoped_idempotency_key)
        {
            return self
                .checkpoints_by_run
                .get(&run_id)
                .and_then(|arc| arc.read().unwrap().get(&checkpoint_id).cloned())
                .ok_or(ReportCheckpointArtifactError::CheckpointNotFound);
        }

        let checkpoints_arc = self
            .checkpoints_by_run
            .get(&run_id)
            .ok_or(ReportCheckpointArtifactError::CheckpointNotFound)?
            .value()
            .clone();
        let mut checkpoints = checkpoints_arc.write().unwrap();

        let record = checkpoints
            .get_mut(&checkpoint_id)
            .ok_or(ReportCheckpointArtifactError::CheckpointNotFound)?;

        let now_millis = now_millis();
        if record.checkpoint.state == DomainCheckpointState::Initiated {
            record
                .checkpoint
                .transition_to(DomainCheckpointState::UploadInProgress, now_millis)
                .map_err(map_checkpoint_error)?;
        }
        let domain_worker_id = DomainWorkerId(worker_id.clone());
        record
            .checkpoint
            .add_artifact(
                ArtifactRecord {
                    worker_id: domain_worker_id,
                    artifact_name,
                    storage_key: artifact_uri,
                    size_bytes,
                    checksum,
                    checksum_algorithm,
                },
                now_millis,
            )
            .map_err(map_checkpoint_error)?;
        let duplicate_reports = record
            .checkpoint
            .artifacts
            .iter()
            .filter(|a| a.worker_id.0 == worker_id)
            .count();
        if duplicate_reports > 1 {
            let latest = record
                .checkpoint
                .artifacts
                .last()
                .expect("artifact list must contain at least one item")
                .clone();
            let first = record
                .checkpoint
                .artifacts
                .iter()
                .find(|a| a.worker_id.0 == worker_id)
                .expect("worker artifact must exist");
            if first != &latest {
                return Err(ReportCheckpointArtifactError::InvalidInput(
                    "worker already reported conflicting artifact metadata",
                ));
            }
            record.checkpoint.artifacts.pop();
        }
        let response = record.clone();
        drop(checkpoints);

        if let Some(last) = response.checkpoint.artifacts.last() {
            self.persist_artifact(&run_id, &checkpoint_id, last);
        }
        self.persist_checkpoint(&run_id, "", &response);
        if !idempotency_key.is_empty() {
            self.checkpoint_artifact_idempotency_keys
                .insert(scoped_idempotency_key, checkpoint_id);
        }
        Ok(response)
    }

    pub fn commit_checkpoint(
        &self,
        run_id: String,
        checkpoint_id: String,
        worker_id: String, // Gap 2: per-worker 2PC commit vote
        _idempotency_key: String,
    ) -> Result<CommitCheckpointResult, CommitCheckpointError> {
        if run_id.trim().is_empty() {
            return Err(CommitCheckpointError::InvalidInput(
                "run_id must be non-empty",
            ));
        }
        if checkpoint_id.trim().is_empty() {
            return Err(CommitCheckpointError::InvalidInput(
                "checkpoint_id must be non-empty",
            ));
        }
        if !self.runs.contains_key(&run_id) {
            return Err(CommitCheckpointError::RunNotFound);
        }

        let checkpoints_arc = self
            .checkpoints_by_run
            .get(&run_id)
            .ok_or(CommitCheckpointError::CheckpointNotFound)?
            .value()
            .clone();

        // Fast path: already globally committed (idempotent re-commit).
        {
            let checkpoints = checkpoints_arc.read().unwrap();
            if let Some(record) = checkpoints.get(&checkpoint_id) {
                if record.checkpoint.state == DomainCheckpointState::Committed {
                    return Ok(CommitCheckpointResult {
                        record: record.clone(),
                        committed_at_millis: record
                            .committed_at_millis
                            .unwrap_or(record.checkpoint.updated_at_millis),
                        globally_committed: true,
                    });
                }
            }
        }

        // Gap 2: record this worker's commit vote (idempotent via HashSet insert).
        let all_voted = if !worker_id.is_empty() {
            let commit_key = (run_id.clone(), checkpoint_id.clone());
            let set_arc = self
                .checkpoint_commits
                .entry(commit_key)
                .or_insert_with(|| Arc::new(Mutex::new(HashSet::new())))
                .clone();
            let mut set = set_arc.lock().unwrap();
            // Persist the vote *before* recording it in memory: if the DB
            // write fails we return Unavailable and the worker retries, at
            // which point persistence is re-attempted.  Inserting into the
            // in-memory set first would make the retry a no-op and the vote
            // would never become durable.
            if !set.contains(&worker_id) {
                if let Some(store) = &self.metadata_store {
                    store
                        .record_checkpoint_commit(&run_id, &checkpoint_id, &worker_id, now_millis())
                        .map_err(|e| {
                            tracing::error!(
                                error = %e,
                                run_id = %run_id,
                                checkpoint_id = %checkpoint_id,
                                worker_id = %worker_id,
                                "failed to persist checkpoint commit vote"
                            );
                            CommitCheckpointError::PersistenceUnavailable
                        })?;
                }
                set.insert(worker_id.clone());
            }
            let commit_count = set.len();
            drop(set);

            // Check whether all workers have voted by comparing against the
            // expected artifact count (which equals world_size at begin_checkpoint).
            let checkpoints = checkpoints_arc.read().unwrap();
            let record = checkpoints
                .get(&checkpoint_id)
                .ok_or(CommitCheckpointError::CheckpointNotFound)?;
            commit_count >= record.expected_artifact_count as usize
        } else {
            // Legacy path: no worker_id supplied → skip vote-count gate and
            // rely solely on artifact completeness (backward compatible).
            true
        };

        if !all_voted {
            // Some workers haven't committed yet — return a pending result.
            let checkpoints = checkpoints_arc.read().unwrap();
            let record = checkpoints
                .get(&checkpoint_id)
                .ok_or(CommitCheckpointError::CheckpointNotFound)?;
            return Ok(CommitCheckpointResult {
                record: record.clone(),
                committed_at_millis: record.checkpoint.updated_at_millis,
                globally_committed: false,
            });
        }

        // All votes in — acquire the write lock and perform the global commit.
        let mut checkpoints = checkpoints_arc.write().unwrap();

        let record = checkpoints
            .get_mut(&checkpoint_id)
            .ok_or(CommitCheckpointError::CheckpointNotFound)?;

        // Double-check under write lock (another worker may have beaten us).
        if record.checkpoint.state == DomainCheckpointState::Committed {
            let committed_at = record
                .committed_at_millis
                .unwrap_or(record.checkpoint.updated_at_millis);
            return Ok(CommitCheckpointResult {
                record: record.clone(),
                committed_at_millis: committed_at,
                globally_committed: true,
            });
        }

        if record.checkpoint.artifacts.is_empty() {
            return Err(CommitCheckpointError::InvalidState);
        }
        if record.expected_artifact_count == 0 {
            return Err(CommitCheckpointError::InvalidState);
        }
        if record.checkpoint.artifacts.len() != record.expected_artifact_count as usize {
            return Err(CommitCheckpointError::InvalidState);
        }
        let reported_workers: std::collections::BTreeSet<String> = record
            .checkpoint
            .artifacts
            .iter()
            .map(|a| a.worker_id.0.clone())
            .collect();
        let expected_workers: std::collections::BTreeSet<String> =
            record.expected_worker_ids.iter().cloned().collect();
        if reported_workers != expected_workers {
            return Err(CommitCheckpointError::InvalidState);
        }

        if matches!(
            record.checkpoint.state,
            DomainCheckpointState::Failed | DomainCheckpointState::Aborted
        ) {
            return Err(CommitCheckpointError::InvalidState);
        }

        let now_millis_val = now_millis();
        if record.checkpoint.state == DomainCheckpointState::UploadInProgress {
            record
                .checkpoint
                .transition_to(DomainCheckpointState::Validating, now_millis_val)
                .map_err(map_commit_checkpoint_error)?;
        }
        if record.checkpoint.state == DomainCheckpointState::Validating {
            record
                .checkpoint
                .transition_to(DomainCheckpointState::Committed, now_millis_val)
                .map_err(map_commit_checkpoint_error)?;
        }
        if record.checkpoint.state != DomainCheckpointState::Committed {
            return Err(CommitCheckpointError::InvalidState);
        }
        record.committed_at_millis = Some(now_millis_val);
        let committed_at_millis = now_millis_val;

        let response = record.clone();
        drop(checkpoints);

        // Update the run's latest checkpoint pointer.
        if let Some(mut run) = self.runs.get_mut(&run_id) {
            run.latest_checkpoint_id = Some(CheckpointId {
                value: checkpoint_id.clone(),
            });
            run.latest_checkpoint_state = CheckpointState::Committed as i32;
        }

        if let Some(store) = &self.metadata_store {
            store
                .mark_checkpoint_committed(
                    &run_id,
                    &response.checkpoint.checkpoint_id.0,
                    committed_at_millis,
                )
                .expect("mark committed");
        }
        Ok(CommitCheckpointResult {
            record: response,
            committed_at_millis,
            globally_committed: true,
        })
    }

    pub fn get_latest_checkpoint(
        &self,
        run_id: String,
    ) -> Result<CoordinatorCheckpointRecord, GetLatestCheckpointError> {
        if run_id.trim().is_empty() {
            return Err(GetLatestCheckpointError::InvalidInput(
                "run_id must be non-empty",
            ));
        }

        // Try the persistence back-end first (survives coordinator restarts).
        if let Some(store) = &self.metadata_store {
            if let Some(record) = store
                .load_latest_checkpoint(&run_id)
                .expect("load latest checkpoint")
            {
                return Ok(record);
            }
        }

        let run = self
            .runs
            .get(&run_id)
            .ok_or(GetLatestCheckpointError::RunNotFound)?;
        let latest_checkpoint_id = run
            .latest_checkpoint_id
            .as_ref()
            .ok_or(GetLatestCheckpointError::NoCommittedCheckpoint)?
            .value
            .clone();
        drop(run);

        let record = self
            .checkpoints_by_run
            .get(&run_id)
            .and_then(|arc| arc.read().unwrap().get(&latest_checkpoint_id).cloned())
            .ok_or(GetLatestCheckpointError::LatestCheckpointMissing)?;

        if record.checkpoint.state != DomainCheckpointState::Committed {
            return Err(GetLatestCheckpointError::LatestCheckpointNotCommitted);
        }

        Ok(record)
    }

    pub fn register_run(&self, request: RegisterRunRequest) -> RegisteredRun {
        // Atomic idempotency check.
        if !request.idempotency_key.is_empty() {
            if let Some(run_id) = self.run_idempotency_keys.get(&request.idempotency_key) {
                let run_id = run_id.clone();
                let record = self
                    .runs
                    .get(&run_id)
                    .expect("idempotency key must reference an existing run");
                return RegisteredRun {
                    run_id: RunId {
                        value: record.run_id.clone(),
                    },
                    registered_at: record.registered_at,
                };
            }
        }

        let run_id = self.generate_run_id();
        let registered_at = SystemTime::now();
        let record = RunRecord {
            run_id: run_id.clone(),
            run_name: request.run_name,
            assignment_policy: request.assignment_policy,
            storage_backend_kind: request.storage_backend_kind,
            world_size: request.world_size,
            run_version: request.run_version,
            registered_at,
            active_worker_count: 0,
            latest_checkpoint_id: None,
            latest_checkpoint_state: CheckpointState::Unspecified as i32,
            min_world_size: request.min_world_size,
            max_world_size: request.max_world_size,
            rendezvous_settle_timeout_ms: request.rendezvous_settle_timeout_ms,
        };

        self.runs.insert(run_id.clone(), record.clone());
        if !request.idempotency_key.is_empty() {
            self.run_idempotency_keys
                .insert(request.idempotency_key, run_id.clone());
        }
        self.persist_run(&record);

        RegisteredRun {
            run_id: RunId { value: run_id },
            registered_at,
        }
    }

    pub fn get_run(&self, run_id: &RunId) -> Option<RunRecord> {
        self.runs.get(&run_id.value).map(|r| r.clone())
    }

    pub fn worker_counts(&self, run_id: &RunId) -> Option<(u64, u64)> {
        let arc = self.workers_by_run.get(&run_id.value)?;
        let workers = arc.read().unwrap();
        let active = workers
            .values()
            .filter(|w| w.state == WorkerState::Active as i32)
            .count() as u64;
        let stale = workers
            .values()
            .filter(|w| w.state == WorkerState::Stale as i32)
            .count() as u64;
        Some((active, stale))
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

    /// Reconciles in-memory worker liveness against the current server time.
    ///
    /// Marks active workers stale when heartbeat deadlines pass and stale
    /// workers failed when the longer failure deadline passes.  Workers that
    /// newly transition to FAILED are removed from their run's rendezvous:
    /// a completed round they held a rank in is invalidated so the surviving
    /// workers re-rendezvous (rank replacement / elastic shrink).
    pub fn reconcile_worker_liveness(&self) {
        let now = SystemTime::now();
        let newly_failed = reconcile_worker_liveness(&self.runs, &self.workers_by_run, now);
        for (run_id, worker_id) in newly_failed {
            if let Some(rendezvous) = self.rendezvous_by_run.get(&run_id) {
                let invalidated = rendezvous.lock().unwrap().on_worker_failed(&worker_id, now);
                if invalidated {
                    tracing::info!(
                        run_id = %run_id,
                        worker_id = %worker_id,
                        "worker failure invalidated rendezvous round; remaining workers must re-join"
                    );
                }
            }
        }
    }

    // ── rendezvous ───────────────────────────────────────────────────────────

    /// Fetch or lazily create the rendezvous for a run using the run's
    /// world-size / elasticity configuration.
    fn rendezvous_for_run(&self, run: &RunRecord) -> Arc<Mutex<RunRendezvous>> {
        self.rendezvous_by_run
            .entry(run.run_id.clone())
            .or_insert_with(|| {
                let settle_timeout = if run.rendezvous_settle_timeout_ms > 0 {
                    Duration::from_millis(u64::from(run.rendezvous_settle_timeout_ms))
                } else {
                    crate::rendezvous::DEFAULT_SETTLE_TIMEOUT
                };
                Arc::new(Mutex::new(RunRendezvous::new(RendezvousConfig {
                    world_size: run.world_size,
                    min_world_size: run.min_world_size,
                    max_world_size: run.max_world_size,
                    settle_timeout,
                })))
            })
            .value()
            .clone()
    }

    fn require_registered_worker(
        &self,
        run_id: &str,
        worker_id: &str,
    ) -> Result<RunRecord, RendezvousError> {
        let run = self
            .runs
            .get(run_id)
            .map(|r| r.clone())
            .ok_or(RendezvousError::RunNotFound)?;
        let known = self
            .workers_by_run
            .get(run_id)
            .map(|arc| arc.read().unwrap().contains_key(worker_id))
            .unwrap_or(false);
        if !known {
            return Err(RendezvousError::WorkerNotFound);
        }
        Ok(run)
    }

    pub fn join_rendezvous(
        &self,
        run_id: &str,
        worker_id: &str,
        host: &str,
        store_port: u32,
    ) -> Result<RendezvousSnapshot, RendezvousError> {
        if host.is_empty() {
            return Err(RendezvousError::MissingHost);
        }
        let run = self.require_registered_worker(run_id, worker_id)?;
        let rendezvous = self.rendezvous_for_run(&run);
        let snapshot =
            rendezvous
                .lock()
                .unwrap()
                .join(worker_id, host, store_port, SystemTime::now());
        Ok(snapshot)
    }

    pub fn get_rendezvous_state(
        &self,
        run_id: &str,
        worker_id: &str,
    ) -> Result<RendezvousSnapshot, RendezvousError> {
        let run = self.require_registered_worker(run_id, worker_id)?;
        let rendezvous = self.rendezvous_for_run(&run);
        let snapshot = rendezvous
            .lock()
            .unwrap()
            .state_for(worker_id, SystemTime::now());
        Ok(snapshot)
    }

    /// Latest rendezvous round for a run, or 0 when no rendezvous exists.
    /// Reported on heartbeat responses so workers can cheaply detect
    /// membership changes.
    pub fn current_rendezvous_round(&self, run_id: &str) -> u64 {
        self.rendezvous_by_run
            .get(run_id)
            .map(|r| r.lock().unwrap().current_round())
            .unwrap_or(0)
    }

    pub fn get_dataset(&self, run_id: &RunId, dataset_id: &DatasetId) -> Option<DatasetRecord> {
        self.datasets_by_run
            .get(&run_id.value)
            .and_then(|arc| arc.read().unwrap().get(&dataset_id.value).cloned())
    }

    pub fn register_dataset(&self, request: RegisterDatasetRequest) -> Option<RegisteredDataset> {
        let run_id = request.run_id.as_ref()?.value.clone();

        if !self.runs.contains_key(&run_id) {
            return None;
        }

        let idempotency_key = dataset_idempotency_key(&run_id, &request.idempotency_key);
        if !idempotency_key.is_empty() {
            if let Some(dataset_id) = self.dataset_idempotency_keys.get(&idempotency_key) {
                let dataset_id = dataset_id.clone();
                let record = self
                    .datasets_by_run
                    .get(&run_id)
                    .and_then(|arc| arc.read().unwrap().get(&dataset_id).cloned())
                    .expect("dataset idempotency key must reference an existing dataset");
                return Some(RegisteredDataset {
                    dataset_id: DatasetId {
                        value: record.dataset_id.clone(),
                    },
                    accepted_shard_count: record.shards.len() as u32,
                    registered_at: record.registered_at,
                });
            }
        }

        let dataset = request.dataset.unwrap_or_default();
        let dataset_id =
            dataset_id_from_metadata(&dataset).unwrap_or_else(|| self.generate_dataset_id());
        let registered_at = SystemTime::now();
        let shards = request
            .shards
            .into_iter()
            .map(|shard| self.shard_record(shard, &dataset_id))
            .collect::<Vec<_>>();

        let record = DatasetRecord {
            run_id: run_id.clone(),
            dataset_id: dataset_id.clone(),
            name: dataset.name,
            dataset_uri: dataset.dataset_uri,
            format: dataset.format,
            shard_count: dataset.shard_count,
            dataset_version: dataset.dataset_version,
            labels: dataset.labels,
            registered_at,
            shards,
        };
        let accepted_shard_count = record.shards.len() as u32;

        let datasets_arc = self
            .datasets_by_run
            .entry(run_id.clone())
            .or_insert_with(|| Arc::new(RwLock::new(HashMap::new())))
            .value()
            .clone();
        datasets_arc
            .write()
            .unwrap()
            .insert(dataset_id.clone(), record.clone());

        if !request.idempotency_key.is_empty() {
            self.dataset_idempotency_keys
                .insert(idempotency_key, dataset_id.clone());
        }

        self.persist_dataset(&record);
        Some(RegisteredDataset {
            dataset_id: DatasetId { value: dataset_id },
            accepted_shard_count,
            registered_at,
        })
    }

    pub fn get_shard_assignments(
        &self,
        run_id: &RunId,
        worker_id: &WorkerId,
        dataset_id: &DatasetId,
        cached_shard_hints: Option<&[String]>,
    ) -> Result<ShardAssignments, GetShardAssignmentsError> {
        let run = self
            .runs
            .get(&run_id.value)
            .ok_or(GetShardAssignmentsError::RunNotFound)?
            .clone();

        let worker = self
            .workers_by_run
            .get(&run_id.value)
            .and_then(|arc| arc.read().unwrap().get(&worker_id.value).cloned())
            .ok_or(GetShardAssignmentsError::WorkerNotFound)?;

        let dataset = self
            .datasets_by_run
            .get(&run_id.value)
            .and_then(|arc| arc.read().unwrap().get(&dataset_id.value).cloned())
            .ok_or(GetShardAssignmentsError::DatasetNotFound)?;

        if run.world_size == 0 {
            return Err(GetShardAssignmentsError::EmptyWorldSize);
        }

        let server_time = SystemTime::now();
        let lease_expires_at = server_time + SHARD_LEASE_DURATION;
        let world_size = run.world_size as usize;
        let worker_rank = worker.rank as usize;

        let mut candidate_shards: Vec<ShardRecord> = dataset
            .shards
            .iter()
            .enumerate()
            .filter(|(shard_index, _)| *shard_index % world_size == worker_rank)
            .map(|(_, shard)| shard.clone())
            .collect();

        if self.placement_policy == DatasetPlacementPolicy::CacheAware {
            let hint_set: HashSet<&str> = cached_shard_hints
                .unwrap_or(&[])
                .iter()
                .map(String::as_str)
                .collect();
            candidate_shards.sort_by_key(|shard| {
                if hint_set.contains(shard.shard_id.as_str()) {
                    0usize
                } else {
                    1usize
                }
            });
        }

        let assignments = candidate_shards
            .into_iter()
            .map(|shard| ShardAssignmentRecord {
                run_id: run.run_id.clone(),
                dataset_id: dataset.dataset_id.clone(),
                shard_id: shard.shard_id.clone(),
                worker_id: worker.worker_id.clone(),
                lease_epoch: 1,
                lease_state: ShardLeaseState::Leased as i32,
                lease_expires_at,
                progress_offset: self
                    .shard_progress
                    .get(&ShardProgressKey {
                        run_id: run.run_id.clone(),
                        dataset_id: dataset.dataset_id.clone(),
                        shard_id: shard.shard_id.clone(),
                    })
                    .map(|p| p.progress_offset)
                    .unwrap_or_default(),
                shard_uri: shard.shard_uri,
            })
            .collect();

        Ok(ShardAssignments {
            assignments,
            assignment_policy: run.assignment_policy,
            server_time,
        })
    }

    pub fn renew_shard_lease(
        &self,
        run_id: &RunId,
        worker_id: &WorkerId,
        shard_id: &ShardId,
        lease_epoch: u64,
    ) -> Result<RenewedShardLease, RenewShardLeaseError> {
        let run = self
            .runs
            .get(&run_id.value)
            .ok_or(RenewShardLeaseError::RunNotFound)?
            .clone();

        let worker = self
            .workers_by_run
            .get(&run_id.value)
            .and_then(|arc| arc.read().unwrap().get(&worker_id.value).cloned())
            .ok_or(RenewShardLeaseError::WorkerNotFound)?;

        let shard_index = self
            .datasets_by_run
            .get(&run_id.value)
            .and_then(|arc| {
                let datasets = arc.read().unwrap();
                datasets.values().find_map(|dataset| {
                    dataset
                        .shards
                        .iter()
                        .enumerate()
                        .find(|(_, shard)| shard.shard_id == shard_id.value)
                        .map(|(i, _)| i)
                })
            })
            .ok_or(RenewShardLeaseError::ShardNotFound)?;

        if run.world_size == 0 {
            return Err(RenewShardLeaseError::EmptyWorldSize);
        }

        let world_size = run.world_size as usize;
        let worker_rank = worker.rank as usize;
        if shard_index % world_size != worker_rank {
            return Err(RenewShardLeaseError::NotShardOwner);
        }
        Self::ensure_worker_active_for_lease_actions(&worker)
            .map_err(|_| RenewShardLeaseError::WorkerNotActive)?;

        if lease_epoch != 1 {
            return Err(RenewShardLeaseError::InvalidLeaseEpoch);
        }

        let server_time = SystemTime::now();
        Ok(RenewedShardLease {
            accepted: true,
            lease_state: ShardLeaseState::Leased as i32,
            lease_expires_at: server_time + SHARD_LEASE_DURATION,
            server_time,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn report_shard_progress(
        &self,
        run_id: &RunId,
        worker_id: &WorkerId,
        dataset_id: &DatasetId,
        shard_id: &ShardId,
        lease_epoch: u64,
        progress_offset: i64,
        is_complete: bool,
    ) -> Result<ReportedShardProgress, ReportShardProgressError> {
        let run = self
            .runs
            .get(&run_id.value)
            .ok_or(ReportShardProgressError::RunNotFound)?
            .clone();

        let worker = self
            .workers_by_run
            .get(&run_id.value)
            .and_then(|arc| arc.read().unwrap().get(&worker_id.value).cloned())
            .ok_or(ReportShardProgressError::WorkerNotFound)?;

        let (shard_index, _) = self
            .datasets_by_run
            .get(&run_id.value)
            .and_then(|arc| {
                let datasets = arc.read().unwrap();
                datasets.get(&dataset_id.value).and_then(|dataset| {
                    dataset
                        .shards
                        .iter()
                        .enumerate()
                        .find(|(_, shard)| shard.shard_id == shard_id.value)
                        .map(|(i, s)| (i, s.clone()))
                })
            })
            .ok_or(ReportShardProgressError::ShardNotFound)?;

        // Verify the dataset exists (separate from the shard check above).
        if !self
            .datasets_by_run
            .get(&run_id.value)
            .is_some_and(|arc| arc.read().unwrap().contains_key(&dataset_id.value))
        {
            return Err(ReportShardProgressError::DatasetNotFound);
        }

        if run.world_size == 0 {
            return Err(ReportShardProgressError::EmptyWorldSize);
        }

        let world_size = run.world_size as usize;
        let worker_rank = worker.rank as usize;
        if shard_index % world_size != worker_rank {
            return Err(ReportShardProgressError::NotShardOwner);
        }
        Self::ensure_worker_active_for_lease_actions(&worker)
            .map_err(|_| ReportShardProgressError::WorkerNotActive)?;

        if lease_epoch != 1 {
            return Err(ReportShardProgressError::InvalidLeaseEpoch);
        }

        let key = ShardProgressKey {
            run_id: run_id.value.clone(),
            dataset_id: dataset_id.value.clone(),
            shard_id: shard_id.value.clone(),
        };
        let record = ShardProgressRecord {
            run_id: run_id.value.clone(),
            dataset_id: dataset_id.value.clone(),
            shard_id: shard_id.value.clone(),
            worker_id: worker_id.value.clone(),
            lease_epoch,
            progress_offset,
            is_complete,
            updated_at: SystemTime::now(),
        };
        // Gap 1: persist shard progress to the durable store so we can
        // hydrate it on coordinator restart (resume-from-mid-shard).
        self.persist_shard_progress(&record);
        self.shard_progress.insert(key, record);

        Ok(ReportedShardProgress {
            accepted: true,
            stored_progress_offset: progress_offset,
            lease_state: ShardLeaseState::Leased as i32,
        })
    }

    pub fn get_shard_progress(
        &self,
        run_id: &RunId,
        dataset_id: &DatasetId,
        shard_id: &ShardId,
    ) -> Option<ShardProgressRecord> {
        self.shard_progress
            .get(&ShardProgressKey {
                run_id: run_id.value.clone(),
                dataset_id: dataset_id.value.clone(),
                shard_id: shard_id.value.clone(),
            })
            .map(|r| r.clone())
    }

    pub fn register_worker(&self, request: RegisterWorkerRequest) -> Option<RegisteredWorker> {
        let run_id = request.run_id.as_ref()?.value.clone();

        if !self.runs.contains_key(&run_id) {
            return None;
        }

        let world_size = self
            .runs
            .get(&run_id)
            .expect("run must exist when registering worker")
            .world_size;

        let idempotency_key = worker_idempotency_key(&run_id, &request.idempotency_key);
        if !idempotency_key.is_empty() {
            if let Some(existing_worker_id) = self.worker_idempotency_keys.get(&idempotency_key) {
                let existing_worker_id = existing_worker_id.clone();
                let record = self
                    .workers_by_run
                    .get(&run_id)
                    .and_then(|arc| arc.read().unwrap().get(&existing_worker_id).cloned())
                    .expect("worker idempotency key must reference an existing worker");
                return Some(RegisteredWorker {
                    worker_id: WorkerId {
                        value: record.worker_id.clone(),
                    },
                    state: record.state,
                    lease_renewal_deadline: record.registered_at + HEARTBEAT_INTERVAL,
                    server_time: record.registered_at,
                    is_new: false,
                });
            }
        }

        let worker_id = request
            .worker_id
            .as_ref()
            .map(|w| w.value.as_str())
            .filter(|v| !v.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| self.generate_worker_id());
        let now = SystemTime::now();
        let next_heartbeat_deadline = now + HEARTBEAT_INTERVAL;
        let failure_deadline = now + WORKER_FAILURE_INTERVAL;
        let worker = WorkerRecord {
            run_id: run_id.clone(),
            worker_id: worker_id.clone(),
            rank: request.rank,
            host: request.host,
            sdk_version: request.sdk_version,
            protocol_version: request.protocol_version,
            state: WorkerState::Active as i32,
            registered_at: now,
            last_heartbeat_at: now,
            next_heartbeat_deadline,
            failure_deadline,
        };

        let workers_arc = self
            .workers_by_run
            .entry(run_id.clone())
            .or_insert_with(|| Arc::new(RwLock::new(HashMap::new())))
            .value()
            .clone();
        workers_arc
            .write()
            .unwrap()
            .insert(worker_id.clone(), worker.clone());

        refresh_active_worker_count(&self.runs, &self.workers_by_run, &run_id);

        if !request.idempotency_key.is_empty() {
            self.worker_idempotency_keys
                .insert(idempotency_key, worker_id.clone());
        }

        self.persist_worker(&worker, world_size);

        Some(RegisteredWorker {
            worker_id: WorkerId { value: worker_id },
            state: WorkerState::Active as i32,
            lease_renewal_deadline: next_heartbeat_deadline,
            server_time: now,
            is_new: true,
        })
    }

    pub fn record_heartbeat(
        &self,
        run_id: &RunId,
        worker_id: &WorkerId,
    ) -> Option<WorkerHeartbeat> {
        if !self.runs.contains_key(&run_id.value) {
            return None;
        }

        let workers_arc = self.workers_by_run.get(&run_id.value)?;
        let mut workers = workers_arc.write().unwrap();
        let worker = workers.get_mut(&worker_id.value)?;

        let now = SystemTime::now();
        let next_heartbeat_deadline = now + HEARTBEAT_INTERVAL;
        let failure_deadline = now + WORKER_FAILURE_INTERVAL;
        worker.last_heartbeat_at = now;
        worker.next_heartbeat_deadline = next_heartbeat_deadline;
        worker.failure_deadline = failure_deadline;
        worker.state = WorkerState::Active as i32;
        let state = worker.state;
        let worker_snapshot = worker.clone();
        drop(workers);

        let world_size = self
            .runs
            .get(&run_id.value)
            .expect("run must exist when recording heartbeat")
            .world_size;

        refresh_active_worker_count(&self.runs, &self.workers_by_run, &run_id.value);
        self.persist_worker(&worker_snapshot, world_size);

        Some(WorkerHeartbeat {
            state,
            next_heartbeat_deadline,
            server_time: now,
        })
    }

    // ── test helpers ─────────────────────────────────────────────────────────

    #[cfg(test)]
    pub fn set_worker_heartbeat_deadline_for_test(
        &self,
        run_id: &RunId,
        worker_id: &WorkerId,
        deadline: SystemTime,
    ) -> bool {
        self.workers_by_run
            .get(&run_id.value)
            .and_then(|arc| {
                arc.write().unwrap().get_mut(&worker_id.value).map(|w| {
                    w.next_heartbeat_deadline = deadline;
                })
            })
            .is_some()
    }

    #[cfg(test)]
    pub fn set_worker_failure_deadline_for_test(
        &self,
        run_id: &RunId,
        worker_id: &WorkerId,
        deadline: SystemTime,
    ) -> bool {
        self.workers_by_run
            .get(&run_id.value)
            .and_then(|arc| {
                arc.write().unwrap().get_mut(&worker_id.value).map(|w| {
                    w.failure_deadline = deadline;
                })
            })
            .is_some()
    }

    /// Test helper: read a single worker record without going through the gRPC layer.
    #[cfg(test)]
    pub fn get_worker_for_test(
        &self,
        run_id: &RunId,
        worker_id: &WorkerId,
    ) -> Option<WorkerRecord> {
        self.workers_by_run
            .get(&run_id.value)
            .and_then(|arc| arc.read().unwrap().get(&worker_id.value).cloned())
    }

    /// Test helper: total number of registered runs.
    #[cfg(test)]
    pub fn run_count_for_test(&self) -> usize {
        self.runs.len()
    }

    // ── ID generators ────────────────────────────────────────────────────────

    fn generate_run_id(&self) -> String {
        let id = self.next_run_id.fetch_add(1, Ordering::Relaxed) + 1;
        format!("run-{id}")
    }

    fn generate_worker_id(&self) -> String {
        let id = self.next_worker_id.fetch_add(1, Ordering::Relaxed) + 1;
        format!("worker-{id}")
    }

    fn generate_dataset_id(&self) -> String {
        let id = self.next_dataset_id.fetch_add(1, Ordering::Relaxed) + 1;
        format!("dataset-{id}")
    }

    fn generate_shard_id(&self) -> String {
        let id = self.next_shard_id.fetch_add(1, Ordering::Relaxed) + 1;
        format!("shard-{id}")
    }

    fn generate_checkpoint_id(&self) -> String {
        let id = self.next_checkpoint_id.fetch_add(1, Ordering::Relaxed) + 1;
        format!("checkpoint-{id}")
    }

    fn shard_record(&self, shard: ShardMetadata, dataset_id: &str) -> ShardRecord {
        let shard_id = shard
            .shard_id
            .as_ref()
            .map(|s| s.value.as_str())
            .filter(|v| !v.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| self.generate_shard_id());
        let shard_dataset_id = shard
            .dataset_id
            .as_ref()
            .map(|d| d.value.as_str())
            .filter(|v| !v.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| dataset_id.to_string());

        ShardRecord {
            shard_id,
            dataset_id: shard_dataset_id,
            shard_uri: shard.shard_uri,
            byte_range_start: shard.byte_range_start,
            byte_range_end: shard.byte_range_end,
            size_bytes: shard.size_bytes,
            checksum: shard.checksum,
            checksum_algorithm: shard.checksum_algorithm,
        }
    }
}

// ─── free helper functions ────────────────────────────────────────────────────

/// Returns the (run_id, worker_id) pairs of workers that newly transitioned
/// to FAILED during this pass, so the caller can propagate the failure to
/// per-run rendezvous state.
fn reconcile_worker_liveness(
    runs: &DashMap<String, RunRecord>,
    workers_by_run: &DashMap<String, Arc<RwLock<HashMap<String, WorkerRecord>>>>,
    now: SystemTime,
) -> Vec<(String, String)> {
    let mut changed_run_ids = Vec::new();
    let mut newly_failed = Vec::new();

    for run_entry in workers_by_run.iter() {
        let run_id = run_entry.key().clone();
        let workers_arc = run_entry.value().clone();
        let mut changed = false;
        {
            let mut workers = workers_arc.write().unwrap();
            for worker in workers.values_mut() {
                if worker.state == WorkerState::Active as i32 {
                    if worker.next_heartbeat_deadline < now {
                        worker.state = WorkerState::Stale as i32;
                        changed = true;
                    }
                } else if worker.state == WorkerState::Stale as i32 && worker.failure_deadline < now
                {
                    worker.state = WorkerState::Failed as i32;
                    newly_failed.push((run_id.clone(), worker.worker_id.clone()));
                    changed = true;
                }
            }
        }
        if changed {
            changed_run_ids.push(run_id);
        }
    }

    for run_id in changed_run_ids {
        refresh_active_worker_count(runs, workers_by_run, &run_id);
    }

    newly_failed
}

fn refresh_active_worker_count(
    runs: &DashMap<String, RunRecord>,
    workers_by_run: &DashMap<String, Arc<RwLock<HashMap<String, WorkerRecord>>>>,
    run_id: &str,
) {
    let active_worker_count = workers_by_run
        .get(run_id)
        .map(|arc| {
            arc.read()
                .unwrap()
                .values()
                .filter(|w| w.state == WorkerState::Active as i32)
                .count() as u32
        })
        .unwrap_or_default();
    if let Some(mut run) = runs.get_mut(run_id) {
        run.active_worker_count = active_worker_count;
    }
}

fn worker_idempotency_key(run_id: &str, idempotency_key: &str) -> String {
    scoped_idempotency_key(run_id, idempotency_key)
}

fn dataset_idempotency_key(run_id: &str, idempotency_key: &str) -> String {
    scoped_idempotency_key(run_id, idempotency_key)
}

fn checkpoint_idempotency_key(run_id: &str, idempotency_key: &str) -> String {
    scoped_idempotency_key(run_id, idempotency_key)
}

fn checkpoint_artifact_idempotency_key(
    run_id: &str,
    checkpoint_id: &str,
    idempotency_key: &str,
) -> String {
    if idempotency_key.is_empty() {
        return String::new();
    }
    format!("{run_id}:{checkpoint_id}:{idempotency_key}")
}

fn map_checkpoint_error(error: CheckpointError) -> ReportCheckpointArtifactError {
    match error {
        CheckpointError::InvalidArtifact(_) => ReportCheckpointArtifactError::InvalidInput(
            "artifact_name and artifact_uri must be non-empty",
        ),
        CheckpointError::TerminalState { .. } | CheckpointError::InvalidTransition { .. } => {
            ReportCheckpointArtifactError::InvalidState
        }
        _ => ReportCheckpointArtifactError::InvalidState,
    }
}

fn map_commit_checkpoint_error(error: CheckpointError) -> CommitCheckpointError {
    match error {
        CheckpointError::InvalidTransition { .. } | CheckpointError::TerminalState { .. } => {
            CommitCheckpointError::InvalidState
        }
        _ => CommitCheckpointError::InvalidState,
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

fn dataset_id_from_metadata(dataset: &DatasetMetadata) -> Option<String> {
    dataset
        .dataset_id
        .as_ref()
        .map(|d| d.value.as_str())
        .filter(|v| !v.is_empty())
        .map(ToOwned::to_owned)
}

// ─── tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use runtime_proto::runtime::v1::{
        AssignmentPolicy, DatasetMetadata, ShardMetadata, StorageBackendKind,
    };
    use tempfile::tempdir;

    fn register_request(idempotency_key: &str) -> RegisterRunRequest {
        RegisterRunRequest {
            idempotency_key: idempotency_key.to_string(),
            run_name: "test-run".to_string(),
            assignment_policy: AssignmentPolicy::StaticRank as i32,
            storage_backend_kind: StorageBackendKind::LocalFilesystem as i32,
            world_size: 4,
            run_version: "v1".to_string(),
            ..Default::default()
        }
    }

    fn worker_request(run_id: RunId, idempotency_key: &str) -> RegisterWorkerRequest {
        RegisterWorkerRequest {
            idempotency_key: idempotency_key.to_string(),
            run_id: Some(run_id),
            worker_id: Some(WorkerId {
                value: format!("worker-{idempotency_key}"),
            }),
            rank: 0,
            host: "localhost".to_string(),
            sdk_version: "0.1.0".to_string(),
            protocol_version: "v1".to_string(),
        }
    }

    fn dataset_request(run_id: RunId, idempotency_key: &str) -> RegisterDatasetRequest {
        RegisterDatasetRequest {
            idempotency_key: idempotency_key.to_string(),
            run_id: Some(run_id),
            dataset: Some(DatasetMetadata {
                dataset_id: Some(DatasetId {
                    value: "dataset-test".to_string(),
                }),
                name: "training-data".to_string(),
                dataset_uri: "s3://bucket/dataset".to_string(),
                format: "jsonl".to_string(),
                shard_count: 1,
                dataset_version: "v1".to_string(),
                labels: std::collections::HashMap::new(),
                registered_at: None,
            }),
            shards: vec![ShardMetadata {
                shard_id: Some(ShardId {
                    value: "shard-test-0".to_string(),
                }),
                dataset_id: None,
                shard_uri: "s3://bucket/dataset/shard-0.jsonl".to_string(),
                byte_range_start: 0,
                byte_range_end: 99,
                size_bytes: 100,
                checksum: "checksum-0".to_string(),
                checksum_algorithm: "sha256".to_string(),
            }],
        }
    }

    #[test]
    fn active_worker_count_is_unchanged_before_heartbeat_deadline() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .expect("worker should register")
            .worker_id;

        assert!(state.set_worker_heartbeat_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::now() + Duration::from_secs(60),
        ));

        state.reconcile_worker_liveness();

        let record = state.get_run(&run_id).expect("run should exist");
        let worker = state
            .get_worker_for_test(&run_id, &worker_id)
            .expect("worker should exist");
        assert_eq!(record.active_worker_count, 1);
        assert_eq!(worker.state, WorkerState::Active as i32);
    }

    #[test]
    fn reconcile_worker_liveness_marks_expired_active_workers_stale() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .expect("worker should register")
            .worker_id;

        assert!(state.set_worker_heartbeat_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));

        state.reconcile_worker_liveness();

        let record = state.get_run(&run_id).expect("run should exist");
        let worker = state
            .get_worker_for_test(&run_id, &worker_id)
            .expect("worker should exist");
        assert_eq!(worker.state, WorkerState::Stale as i32);
        assert_eq!(record.active_worker_count, 0);
    }

    #[test]
    fn heartbeat_reactivates_stale_worker() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .expect("worker should register")
            .worker_id;
        assert!(state.set_worker_heartbeat_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();

        let heartbeat = state
            .record_heartbeat(&run_id, &worker_id)
            .expect("stale worker heartbeat should succeed");

        let record = state.get_run(&run_id).expect("run should exist");
        assert_eq!(heartbeat.state, WorkerState::Active as i32);
        assert_eq!(record.active_worker_count, 1);
    }

    #[test]
    fn reconcile_worker_liveness_marks_stale_workers_failed_after_failure_deadline() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .expect("worker should register")
            .worker_id;

        assert!(state.set_worker_heartbeat_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();
        assert!(state.set_worker_failure_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));

        state.reconcile_worker_liveness();

        let record = state.get_run(&run_id).expect("run should exist");
        let worker = state
            .get_worker_for_test(&run_id, &worker_id)
            .expect("worker should exist");
        assert_eq!(worker.state, WorkerState::Failed as i32);
        assert_eq!(record.active_worker_count, 0);
    }

    #[test]
    fn heartbeat_reactivates_failed_worker() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .expect("worker should register")
            .worker_id;
        assert!(state.set_worker_heartbeat_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();
        assert!(state.set_worker_failure_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();

        let heartbeat = state
            .record_heartbeat(&run_id, &worker_id)
            .expect("failed worker heartbeat should succeed");

        let record = state.get_run(&run_id).expect("run should exist");
        let worker = state
            .get_worker_for_test(&run_id, &worker_id)
            .expect("worker should exist");
        assert_eq!(heartbeat.state, WorkerState::Active as i32);
        assert_eq!(worker.state, WorkerState::Active as i32);
        assert_eq!(record.active_worker_count, 1);
    }

    #[test]
    fn worker_death_simulation_marks_worker_failed() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .expect("worker should register")
            .worker_id;

        assert_eq!(
            state
                .get_run(&run_id)
                .expect("run should exist")
                .active_worker_count,
            1
        );

        assert!(state.set_worker_heartbeat_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();

        let stale_worker = state
            .get_worker_for_test(&run_id, &worker_id)
            .expect("worker should exist");
        assert_eq!(stale_worker.state, WorkerState::Stale as i32);

        assert!(state.set_worker_failure_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();

        let failed_worker = state
            .get_worker_for_test(&run_id, &worker_id)
            .expect("worker should exist");
        assert_eq!(failed_worker.state, WorkerState::Failed as i32);
    }

    #[test]
    fn failed_worker_is_excluded_from_run_status() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .expect("worker should register")
            .worker_id;

        assert!(state.set_worker_heartbeat_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();
        assert!(state.set_worker_failure_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();

        let run = state.get_run(&run_id).expect("run should exist");
        assert_eq!(run.active_worker_count, 0);
    }

    #[test]
    fn failed_worker_record_is_retained() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .expect("worker should register")
            .worker_id;

        assert!(state.set_worker_heartbeat_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();
        assert!(state.set_worker_failure_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();

        let worker = state.get_worker_for_test(&run_id, &worker_id);
        assert!(worker.is_some(), "failed worker record should be retained");
    }

    #[test]
    fn heartbeat_after_simulated_death_reactivates_worker() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .expect("worker should register")
            .worker_id;

        assert!(state.set_worker_heartbeat_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();
        assert!(state.set_worker_failure_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();

        let heartbeat = state
            .record_heartbeat(&run_id, &worker_id)
            .expect("failed worker heartbeat should succeed");
        let worker = state
            .get_worker_for_test(&run_id, &worker_id)
            .expect("worker should exist");
        let run = state.get_run(&run_id).expect("run should exist");

        assert_eq!(heartbeat.state, WorkerState::Active as i32);
        assert_eq!(worker.state, WorkerState::Active as i32);
        assert_eq!(run.active_worker_count, 1);
    }

    #[test]
    fn stale_worker_cannot_renew_shard_lease() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .expect("worker should register")
            .worker_id;
        let _dataset = state
            .register_dataset(dataset_request(run_id.clone(), "dataset-key"))
            .expect("dataset should register");
        assert!(state.set_worker_heartbeat_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();
        let error = state
            .renew_shard_lease(
                &run_id,
                &worker_id,
                &ShardId {
                    value: "shard-test-0".to_string(),
                },
                1,
            )
            .expect_err("stale worker should not renew leases");
        assert_eq!(error, RenewShardLeaseError::WorkerNotActive);
    }

    #[test]
    fn failed_worker_cannot_report_shard_progress() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .expect("worker should register")
            .worker_id;
        let dataset = state
            .register_dataset(dataset_request(run_id.clone(), "dataset-key"))
            .expect("dataset should register");
        assert!(state.set_worker_heartbeat_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();
        assert!(state.set_worker_failure_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();
        let dataset_id = dataset.dataset_id;
        let shard_id = ShardId {
            value: "shard-test-0".to_string(),
        };
        let error = state
            .report_shard_progress(&run_id, &worker_id, &dataset_id, &shard_id, 1, 42, false)
            .expect_err("failed worker should not report progress");
        assert_eq!(error, ReportShardProgressError::WorkerNotActive);
    }

    #[test]
    fn register_run_stores_run_metadata() {
        let state = CoordinatorState::default();

        let registered = state.register_run(register_request("register-once"));
        let record = state
            .get_run(&registered.run_id)
            .expect("registered run should be stored");

        assert_eq!(record.run_id, registered.run_id.value);
        assert_eq!(record.run_name, "test-run");
        assert_eq!(
            record.assignment_policy,
            AssignmentPolicy::StaticRank as i32
        );
        assert_eq!(
            record.storage_backend_kind,
            StorageBackendKind::LocalFilesystem as i32
        );
        assert_eq!(record.world_size, 4);
        assert_eq!(record.run_version, "v1");
        assert_eq!(record.active_worker_count, 0);
        assert_eq!(record.latest_checkpoint_id, None);
        assert_eq!(
            record.latest_checkpoint_state,
            CheckpointState::Unspecified as i32
        );
    }

    #[test]
    fn begin_checkpoint_requires_existing_run_and_worker() {
        let state = CoordinatorState::default();
        let err = state
            .begin_checkpoint(
                "missing".to_string(),
                "worker-1".to_string(),
                "c1".to_string(),
                10,
                String::new(),
            )
            .expect_err("missing run should fail");
        assert_eq!(err, BeginCheckpointError::RunNotFound);

        let run_id = state.register_run(register_request("run-key")).run_id;
        let err = state
            .begin_checkpoint(
                run_id.value.clone(),
                "missing-worker".to_string(),
                "c1".to_string(),
                10,
                String::new(),
            )
            .expect_err("missing worker should fail");
        assert_eq!(err, BeginCheckpointError::WorkerNotFound);
    }

    #[test]
    fn begin_checkpoint_generates_id_and_supports_idempotency() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .expect("worker registration should succeed")
            .worker_id;

        let first = state
            .begin_checkpoint(
                run_id.value.clone(),
                worker_id.value.clone(),
                String::new(),
                55,
                "idem-1".to_string(),
            )
            .expect("begin checkpoint should succeed");
        assert_eq!(first.checkpoint.checkpoint_id.0, "checkpoint-1");
        assert_eq!(
            first.checkpoint.state,
            runtime_checkpoint::CheckpointState::Initiated
        );

        let second = state
            .begin_checkpoint(
                run_id.value.clone(),
                worker_id.value,
                "different-checkpoint".to_string(),
                77,
                "idem-1".to_string(),
            )
            .expect("idempotent begin should return first");
        assert_eq!(
            second.checkpoint.checkpoint_id,
            first.checkpoint.checkpoint_id
        );
        assert_eq!(second.checkpoint.step, first.checkpoint.step);
    }

    #[test]
    fn committed_checkpoint_survives_state_recreation() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("coordinator.db");
        let store = SqliteMetadataStore::open(&path).unwrap();
        store.init_schema().unwrap();
        let state = CoordinatorState::with_metadata_store(Arc::new(store));

        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_0 = state
            .register_worker(worker_request(run_id.clone(), "worker-0"))
            .unwrap()
            .worker_id;
        let worker_1 = state
            .register_worker(RegisterWorkerRequest {
                worker_id: Some(WorkerId {
                    value: "worker-1".to_string(),
                }),
                rank: 1,
                ..worker_request(run_id.clone(), "worker-1")
            })
            .unwrap()
            .worker_id;

        state
            .register_dataset(RegisterDatasetRequest {
                idempotency_key: "dataset-key".to_string(),
                run_id: Some(run_id.clone()),
                dataset: Some(DatasetMetadata {
                    dataset_id: Some(DatasetId {
                        value: "dataset-1".to_string(),
                    }),
                    name: "training".to_string(),
                    shard_count: 2,
                    ..DatasetMetadata::default()
                }),
                shards: vec![
                    ShardMetadata {
                        shard_id: Some(ShardId {
                            value: "shard-0".to_string(),
                        }),
                        shard_uri: "file:///dataset/0".to_string(),
                        size_bytes: 100,
                        ..ShardMetadata::default()
                    },
                    ShardMetadata {
                        shard_id: Some(ShardId {
                            value: "shard-1".to_string(),
                        }),
                        shard_uri: "file:///dataset/1".to_string(),
                        size_bytes: 200,
                        ..ShardMetadata::default()
                    },
                ],
            })
            .expect("dataset should register");

        state
            .begin_checkpoint(
                run_id.value.clone(),
                worker_0.value.clone(),
                "checkpoint-1".to_string(),
                10,
                "begin".to_string(),
            )
            .unwrap();
        state
            .report_checkpoint_artifact(
                run_id.value.clone(),
                worker_0.value.clone(),
                "checkpoint-1".to_string(),
                "model.bin".to_string(),
                "file:///ckpt/worker-0-model.bin".to_string(),
                111,
                "sum0".to_string(),
                "sha256".to_string(),
                "art-0".to_string(),
            )
            .unwrap();
        state
            .report_checkpoint_artifact(
                run_id.value.clone(),
                worker_1.value.clone(),
                "checkpoint-1".to_string(),
                "model.bin".to_string(),
                "file:///ckpt/worker-1-model.bin".to_string(),
                222,
                "sum1".to_string(),
                "sha256".to_string(),
                "art-1".to_string(),
            )
            .unwrap();
        state
            .commit_checkpoint(
                run_id.value.clone(),
                "checkpoint-1".to_string(),
                String::new(), // legacy: no worker_id → skip vote gate
                "commit".to_string(),
            )
            .unwrap();

        drop(state);

        let reopened_store = SqliteMetadataStore::open(&path).unwrap();
        reopened_store.init_schema().unwrap();
        let recreated = CoordinatorState::with_metadata_store(Arc::new(reopened_store));

        let latest = recreated
            .get_latest_checkpoint(run_id.value.clone())
            .unwrap();
        assert_eq!(latest.checkpoint.checkpoint_id.0, "checkpoint-1");
        assert_eq!(latest.checkpoint.state, DomainCheckpointState::Committed);
        assert_eq!(latest.checkpoint.artifacts.len(), 2);
    }

    #[test]
    fn get_latest_checkpoint_requires_existing_run_and_commit() {
        let state = CoordinatorState::default();
        let err = state
            .get_latest_checkpoint("missing".to_string())
            .expect_err("missing run should fail");
        assert_eq!(err, GetLatestCheckpointError::RunNotFound);

        let run_id = state.register_run(register_request("run-key")).run_id;
        let err = state
            .get_latest_checkpoint(run_id.value.clone())
            .expect_err("run without committed checkpoint should fail");
        assert_eq!(err, GetLatestCheckpointError::NoCommittedCheckpoint);
    }

    #[test]
    fn get_latest_checkpoint_returns_only_committed_record() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .unwrap()
            .worker_id;

        state
            .begin_checkpoint(
                run_id.value.clone(),
                worker_id.value.clone(),
                "ckpt-pending".to_string(),
                1,
                "begin-1".to_string(),
            )
            .unwrap();
        let err = state
            .get_latest_checkpoint(run_id.value.clone())
            .expect_err("uncommitted checkpoint should not be latest");
        assert_eq!(err, GetLatestCheckpointError::NoCommittedCheckpoint);

        state
            .begin_checkpoint(
                run_id.value.clone(),
                worker_id.value.clone(),
                "ckpt-committed".to_string(),
                2,
                "begin-2".to_string(),
            )
            .unwrap();
        state
            .report_checkpoint_artifact(
                run_id.value.clone(),
                worker_id.value.clone(),
                "ckpt-committed".to_string(),
                "model.bin".to_string(),
                "s3://bucket/model.bin".to_string(),
                10,
                "sum".to_string(),
                "sha256".to_string(),
                "artifact-1".to_string(),
            )
            .unwrap();
        state
            .commit_checkpoint(
                run_id.value.clone(),
                "ckpt-committed".to_string(),
                String::new(),
                "commit-1".to_string(),
            )
            .unwrap();

        let latest = state
            .get_latest_checkpoint(run_id.value.clone())
            .expect("latest committed checkpoint should be returned");
        assert_eq!(latest.checkpoint.checkpoint_id.0, "ckpt-committed");
        assert_eq!(latest.checkpoint.state, DomainCheckpointState::Committed);
        assert_eq!(latest.checkpoint.artifacts.len(), 1);

        state
            .begin_checkpoint(
                run_id.value.clone(),
                worker_id.value.clone(),
                "ckpt-committed-2".to_string(),
                3,
                "begin-3".to_string(),
            )
            .unwrap();
        state
            .report_checkpoint_artifact(
                run_id.value.clone(),
                worker_id.value,
                "ckpt-committed-2".to_string(),
                "model-2.bin".to_string(),
                "s3://bucket/model-2.bin".to_string(),
                11,
                "sum2".to_string(),
                "sha256".to_string(),
                "artifact-2".to_string(),
            )
            .unwrap();
        state
            .commit_checkpoint(
                run_id.value.clone(),
                "ckpt-committed-2".to_string(),
                String::new(),
                "commit-2".to_string(),
            )
            .unwrap();

        let latest = state
            .get_latest_checkpoint(run_id.value)
            .expect("newest committed checkpoint should be latest");
        assert_eq!(latest.checkpoint.checkpoint_id.0, "ckpt-committed-2");
    }

    #[test]
    fn register_run_is_idempotent_for_seen_key() {
        let state = CoordinatorState::default();

        let first = state.register_run(register_request("same-key"));
        let second = state.register_run(RegisterRunRequest {
            run_name: "different".to_string(),
            ..register_request("same-key")
        });

        assert_eq!(first, second);
        assert_eq!(state.run_count_for_test(), 1);
    }

    #[test]
    fn empty_idempotency_key_creates_distinct_runs() {
        let state = CoordinatorState::default();

        let first = state.register_run(register_request(""));
        let second = state.register_run(register_request(""));

        assert_ne!(first.run_id, second.run_id);
        assert_eq!(state.run_count_for_test(), 2);
    }

    #[test]
    fn report_checkpoint_artifact_updates_state_and_is_idempotent() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .unwrap()
            .worker_id;
        state
            .begin_checkpoint(
                run_id.value.clone(),
                worker_id.value.clone(),
                "ckpt-1".to_string(),
                1,
                "checkpoint-key".to_string(),
            )
            .unwrap();

        let reported = state
            .report_checkpoint_artifact(
                run_id.value.clone(),
                worker_id.value.clone(),
                "ckpt-1".to_string(),
                "model.bin".to_string(),
                "s3://bucket/model.bin".to_string(),
                10,
                "sum".to_string(),
                "sha256".to_string(),
                "artifact-key".to_string(),
            )
            .unwrap();
        assert_eq!(
            reported.checkpoint.state,
            DomainCheckpointState::UploadInProgress
        );
        assert_eq!(reported.checkpoint.artifacts.len(), 1);

        let repeated = state
            .report_checkpoint_artifact(
                run_id.value.clone(),
                worker_id.value.clone(),
                "ckpt-1".to_string(),
                "model.bin".to_string(),
                "s3://bucket/model.bin".to_string(),
                10,
                "sum".to_string(),
                "sha256".to_string(),
                "artifact-key".to_string(),
            )
            .unwrap();
        assert_eq!(repeated.checkpoint.artifacts.len(), 1);
    }

    #[test]
    fn commit_checkpoint_requires_existing_run_and_checkpoint() {
        let state = CoordinatorState::default();
        let err = state
            .commit_checkpoint(
                "missing".to_string(),
                "ckpt-1".to_string(),
                String::new(),
                String::new(),
            )
            .expect_err("missing run should fail");
        assert_eq!(err, CommitCheckpointError::RunNotFound);

        let run_id = state.register_run(register_request("run-key")).run_id;
        let err = state
            .commit_checkpoint(
                run_id.value,
                "missing-ckpt".to_string(),
                String::new(),
                String::new(),
            )
            .expect_err("missing checkpoint should fail");
        assert_eq!(err, CommitCheckpointError::CheckpointNotFound);
    }

    #[test]
    fn commit_checkpoint_requires_artifacts() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .unwrap()
            .worker_id;
        state
            .begin_checkpoint(
                run_id.value.clone(),
                worker_id.value,
                "ckpt-1".to_string(),
                1,
                "checkpoint-key".to_string(),
            )
            .unwrap();

        let err = state
            .commit_checkpoint(
                run_id.value,
                "ckpt-1".to_string(),
                String::new(),
                String::new(),
            )
            .expect_err("checkpoint without artifacts should fail");
        assert_eq!(err, CommitCheckpointError::InvalidState);
    }

    #[test]
    fn commit_checkpoint_transitions_and_updates_latest() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .unwrap()
            .worker_id;
        state
            .begin_checkpoint(
                run_id.value.clone(),
                worker_id.value.clone(),
                "ckpt-1".to_string(),
                1,
                "checkpoint-key".to_string(),
            )
            .unwrap();
        state
            .report_checkpoint_artifact(
                run_id.value.clone(),
                worker_id.value,
                "ckpt-1".to_string(),
                "model.bin".to_string(),
                "s3://bucket/model.bin".to_string(),
                10,
                "sum".to_string(),
                "sha256".to_string(),
                "artifact-key".to_string(),
            )
            .unwrap();

        let committed = state
            .commit_checkpoint(
                run_id.value.clone(),
                "ckpt-1".to_string(),
                String::new(),
                "commit-1".to_string(),
            )
            .expect("commit should succeed");
        assert_eq!(
            committed.record.checkpoint.state,
            DomainCheckpointState::Committed
        );
        assert!(committed.record.committed_at_millis.is_some());
        assert_eq!(
            committed.committed_at_millis,
            committed.record.committed_at_millis.unwrap()
        );

        let run = state.get_run(&run_id).unwrap();
        assert_eq!(
            run.latest_checkpoint_id,
            Some(CheckpointId {
                value: "ckpt-1".to_string()
            })
        );
        assert_eq!(
            run.latest_checkpoint_state,
            CheckpointState::Committed as i32
        );

        let repeated = state
            .commit_checkpoint(
                run_id.value,
                "ckpt-1".to_string(),
                String::new(),
                "commit-2".to_string(),
            )
            .expect("repeated commit should be harmless");
        assert_eq!(
            repeated.record.checkpoint.state,
            DomainCheckpointState::Committed
        );
    }

    /// Register a run with two workers, begin a checkpoint, and report one
    /// artifact per worker.  Returns (run_id, worker_a, worker_b).
    fn setup_two_worker_checkpoint(
        state: &CoordinatorState,
        checkpoint_id: &str,
    ) -> (RunId, WorkerId, WorkerId) {
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_a = state
            .register_worker(worker_request(run_id.clone(), "worker-a"))
            .expect("worker a should register")
            .worker_id;
        let mut request_b = worker_request(run_id.clone(), "worker-b");
        request_b.rank = 1;
        let worker_b = state
            .register_worker(request_b)
            .expect("worker b should register")
            .worker_id;

        state
            .begin_checkpoint(
                run_id.value.clone(),
                worker_a.value.clone(),
                checkpoint_id.to_string(),
                1,
                format!("begin-{checkpoint_id}"),
            )
            .expect("begin checkpoint should succeed");
        for (index, worker) in [&worker_a, &worker_b].iter().enumerate() {
            state
                .report_checkpoint_artifact(
                    run_id.value.clone(),
                    worker.value.clone(),
                    checkpoint_id.to_string(),
                    format!("model-{index}.bin"),
                    format!("s3://bucket/model-{index}.bin"),
                    10,
                    "sum".to_string(),
                    "sha256".to_string(),
                    format!("artifact-{checkpoint_id}-{index}"),
                )
                .expect("artifact report should succeed");
        }
        (run_id, worker_a, worker_b)
    }

    #[test]
    fn commit_checkpoint_with_worker_votes_waits_for_all_workers() {
        let state = CoordinatorState::default();
        let (run_id, worker_a, worker_b) = setup_two_worker_checkpoint(&state, "ckpt-2pc");

        // First vote: recorded but the global commit must wait.
        let first = state
            .commit_checkpoint(
                run_id.value.clone(),
                "ckpt-2pc".to_string(),
                worker_a.value.clone(),
                "c1".to_string(),
            )
            .expect("first vote should be accepted");
        assert!(!first.globally_committed);
        assert_ne!(
            first.record.checkpoint.state,
            DomainCheckpointState::Committed
        );

        // Duplicate vote from the same worker is idempotent — still pending.
        let duplicate = state
            .commit_checkpoint(
                run_id.value.clone(),
                "ckpt-2pc".to_string(),
                worker_a.value.clone(),
                "c1-dup".to_string(),
            )
            .expect("duplicate vote should be accepted");
        assert!(!duplicate.globally_committed);

        // Second worker's vote completes the global commit.
        let second = state
            .commit_checkpoint(
                run_id.value.clone(),
                "ckpt-2pc".to_string(),
                worker_b.value.clone(),
                "c2".to_string(),
            )
            .expect("final vote should commit");
        assert!(second.globally_committed);
        assert_eq!(
            second.record.checkpoint.state,
            DomainCheckpointState::Committed
        );

        // Any further commit is an idempotent no-op reporting global commit.
        let repeat = state
            .commit_checkpoint(
                run_id.value,
                "ckpt-2pc".to_string(),
                worker_a.value,
                "c3".to_string(),
            )
            .expect("post-commit vote should be harmless");
        assert!(repeat.globally_committed);
    }

    #[test]
    fn checkpoint_commit_votes_survive_coordinator_restart() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("coordinator.db");
        let store = SqliteMetadataStore::open(&path).unwrap();
        store.init_schema().unwrap();
        let state = CoordinatorState::with_metadata_store(Arc::new(store));

        let (run_id, worker_a, worker_b) = setup_two_worker_checkpoint(&state, "ckpt-2pc");
        let first = state
            .commit_checkpoint(
                run_id.value.clone(),
                "ckpt-2pc".to_string(),
                worker_a.value.clone(),
                "c1".to_string(),
            )
            .expect("first vote should be accepted");
        assert!(!first.globally_committed);
        drop(state);

        // Simulate a coordinator restart: reopen the store and rebuild state.
        let reopened = SqliteMetadataStore::open(&path).unwrap();
        reopened.init_schema().unwrap();
        let recreated = CoordinatorState::with_metadata_store(Arc::new(reopened));

        // Clients rebuild run/checkpoint state via idempotent re-registration.
        let (run_id, _, worker_b_again) = setup_two_worker_checkpoint(&recreated, "ckpt-2pc");
        assert_eq!(worker_b_again.value, worker_b.value);

        // Worker A's pre-restart vote was hydrated from the store, so
        // worker B's single vote must complete the global commit.
        let second = recreated
            .commit_checkpoint(
                run_id.value,
                "ckpt-2pc".to_string(),
                worker_b.value,
                "c2".to_string(),
            )
            .expect("post-restart vote should commit");
        assert!(second.globally_committed);
        assert_eq!(
            second.record.checkpoint.state,
            DomainCheckpointState::Committed
        );
    }

    #[test]
    fn shard_progress_survives_coordinator_restart() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("coordinator.db");
        let store = SqliteMetadataStore::open(&path).unwrap();
        store.init_schema().unwrap();
        let state = CoordinatorState::with_metadata_store(Arc::new(store));

        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .expect("worker should register")
            .worker_id;
        let dataset = state
            .register_dataset(dataset_request(run_id.clone(), "dataset-key"))
            .expect("dataset should register");
        let dataset_id = dataset.dataset_id;
        let shard_id = ShardId {
            value: "shard-test-0".to_string(),
        };

        let reported = state
            .report_shard_progress(&run_id, &worker_id, &dataset_id, &shard_id, 1, 42, false)
            .expect("progress report should be accepted");
        assert_eq!(reported.stored_progress_offset, 42);
        drop(state);

        // Simulate a coordinator restart.
        let reopened = SqliteMetadataStore::open(&path).unwrap();
        reopened.init_schema().unwrap();
        let recreated = CoordinatorState::with_metadata_store(Arc::new(reopened));

        // The offset must be visible immediately after hydration, without the
        // worker having to re-send a progress report.
        let progress = recreated
            .get_shard_progress(&run_id, &dataset_id, &shard_id)
            .expect("shard progress should be hydrated after restart");
        assert_eq!(progress.progress_offset, 42);
        assert_eq!(progress.worker_id, worker_id.value);
        assert!(!progress.is_complete);
    }
}
