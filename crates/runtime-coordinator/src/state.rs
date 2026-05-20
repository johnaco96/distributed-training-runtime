#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, SystemTime};

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

use crate::persistence::SqliteMetadataStore;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DatasetPlacementPolicy {
    Deterministic,
    CacheAware,
}

/// Shared coordinator state.
///
/// This is intentionally in-memory only for the MVP coordinator. It is not
/// persisted and will be lost when the process exits.
#[derive(Debug)]
pub struct CoordinatorState {
    inner: RwLock<CoordinatorStateInner>,
    placement_policy: DatasetPlacementPolicy,
    metadata_store: Option<SqliteMetadataStore>,
    next_run_id: AtomicU64,
    next_worker_id: AtomicU64,
    next_dataset_id: AtomicU64,
    next_shard_id: AtomicU64,
    next_checkpoint_id: AtomicU64,
}

#[derive(Debug, Default)]
struct CoordinatorStateInner {
    runs: HashMap<String, RunRecord>,
    run_idempotency_keys: HashMap<String, String>,
    workers_by_run: HashMap<String, HashMap<String, WorkerRecord>>,
    worker_idempotency_keys: HashMap<String, String>,
    datasets_by_run: HashMap<String, HashMap<String, DatasetRecord>>,
    dataset_idempotency_keys: HashMap<String, String>,
    shard_progress: HashMap<ShardProgressKey, ShardProgressRecord>,
    checkpoints_by_run: HashMap<String, HashMap<String, CoordinatorCheckpointRecord>>,
    checkpoint_idempotency_keys: HashMap<String, String>,
    checkpoint_artifact_idempotency_keys: HashMap<String, String>,
}

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
}

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

impl Default for CoordinatorState {
    fn default() -> Self {
        Self {
            inner: RwLock::new(CoordinatorStateInner::default()),
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

    fn ensure_worker_active_for_lease_actions(worker: &WorkerRecord) -> Result<(), WorkerState> {
        if worker.state == WorkerState::Active as i32 {
            return Ok(());
        }
        Err(WorkerState::try_from(worker.state).unwrap_or(WorkerState::Unspecified))
    }

    pub fn with_metadata_store(metadata_store: SqliteMetadataStore) -> Self {
        Self {
            metadata_store: Some(metadata_store),
            ..Self::default()
        }
    }

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
        let mut inner = self.inner.write().expect("coordinator state lock poisoned");
        if !inner.runs.contains_key(&run_id) {
            return Err(BeginCheckpointError::RunNotFound);
        }
        if !inner
            .workers_by_run
            .get(&run_id)
            .is_some_and(|workers| workers.contains_key(&worker_id))
        {
            return Err(BeginCheckpointError::WorkerNotFound);
        }

        let scoped_idempotency_key = checkpoint_idempotency_key(&run_id, &idempotency_key);
        if let Some(existing_checkpoint_id) =
            idempotency_lookup(&inner.checkpoint_idempotency_keys, &scoped_idempotency_key)
        {
            let record = inner
                .checkpoints_by_run
                .get(&run_id)
                .and_then(|records| records.get(&existing_checkpoint_id))
                .cloned()
                .expect("checkpoint idempotency key must reference an existing checkpoint");
            return Ok(record);
        }

        let checkpoint_id = if checkpoint_id.trim().is_empty() {
            self.generate_checkpoint_id()
        } else {
            checkpoint_id
        };

        if let Some(existing) = inner
            .checkpoints_by_run
            .get(&run_id)
            .and_then(|records| records.get(&checkpoint_id))
        {
            if existing.initiated_by_worker_id == worker_id && existing.checkpoint.step == step {
                return Ok(existing.clone());
            }
            return Err(BeginCheckpointError::CheckpointConflict);
        }

        let now_millis = now_millis();
        let record = CoordinatorCheckpointRecord {
            checkpoint: CheckpointRecord::new(
                DomainRunId(run_id.clone()),
                DomainCheckpointId(checkpoint_id.clone()),
                step,
                now_millis,
            ),
            initiated_by_worker_id: worker_id,
            committed_at_millis: None,
        };
        inner
            .checkpoints_by_run
            .entry(run_id.clone())
            .or_default()
            .insert(checkpoint_id.clone(), record.clone());

        if !idempotency_key.is_empty() {
            inner
                .checkpoint_idempotency_keys
                .insert(scoped_idempotency_key, checkpoint_id);
        }
        Ok(record)
    }

    pub fn get_checkpoint(
        &self,
        run_id: &RunId,
        checkpoint_id: &str,
    ) -> Option<CoordinatorCheckpointRecord> {
        let inner = self.inner.read().expect("coordinator state lock poisoned");
        inner
            .checkpoints_by_run
            .get(&run_id.value)
            .and_then(|records| records.get(checkpoint_id))
            .cloned()
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
        let mut inner = self.inner.write().expect("coordinator state lock poisoned");
        if !inner.runs.contains_key(&run_id) {
            return Err(ReportCheckpointArtifactError::RunNotFound);
        }
        if !inner
            .workers_by_run
            .get(&run_id)
            .is_some_and(|workers| workers.contains_key(&worker_id))
        {
            return Err(ReportCheckpointArtifactError::WorkerNotFound);
        }
        let scoped_idempotency_key =
            checkpoint_artifact_idempotency_key(&run_id, &checkpoint_id, &idempotency_key);
        if !idempotency_key.is_empty()
            && inner
                .checkpoint_artifact_idempotency_keys
                .contains_key(&scoped_idempotency_key)
        {
            return inner
                .checkpoints_by_run
                .get(&run_id)
                .and_then(|records| records.get(&checkpoint_id))
                .cloned()
                .ok_or(ReportCheckpointArtifactError::CheckpointNotFound);
        }
        let record = inner
            .checkpoints_by_run
            .get_mut(&run_id)
            .and_then(|records| records.get_mut(&checkpoint_id))
            .ok_or(ReportCheckpointArtifactError::CheckpointNotFound)?;
        let now_millis = now_millis();
        if record.checkpoint.state == DomainCheckpointState::Initiated {
            record
                .checkpoint
                .transition_to(DomainCheckpointState::UploadInProgress, now_millis)
                .map_err(map_checkpoint_error)?;
        }
        record
            .checkpoint
            .add_artifact(
                ArtifactRecord {
                    worker_id: DomainWorkerId(worker_id),
                    artifact_name,
                    storage_key: artifact_uri,
                    size_bytes,
                    checksum,
                    checksum_algorithm,
                },
                now_millis,
            )
            .map_err(map_checkpoint_error)?;
        let response = record.clone();
        if let Some(last) = response.checkpoint.artifacts.last() {
            self.persist_artifact(&run_id, &checkpoint_id, last);
        }
        self.persist_checkpoint(&run_id, "", &response);
        if !idempotency_key.is_empty() {
            inner
                .checkpoint_artifact_idempotency_keys
                .insert(scoped_idempotency_key, checkpoint_id);
        }
        Ok(response)
    }

    pub fn commit_checkpoint(
        &self,
        run_id: String,
        checkpoint_id: String,
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

        let mut inner = self.inner.write().expect("coordinator state lock poisoned");
        if !inner.runs.contains_key(&run_id) {
            return Err(CommitCheckpointError::RunNotFound);
        }
        let record = inner
            .checkpoints_by_run
            .get_mut(&run_id)
            .and_then(|records| records.get_mut(&checkpoint_id))
            .ok_or(CommitCheckpointError::CheckpointNotFound)?;

        if record.checkpoint.artifacts.is_empty() {
            return Err(CommitCheckpointError::InvalidState);
        }

        if matches!(
            record.checkpoint.state,
            DomainCheckpointState::Failed | DomainCheckpointState::Aborted
        ) {
            return Err(CommitCheckpointError::InvalidState);
        }

        let committed_at_millis = if record.checkpoint.state == DomainCheckpointState::Committed {
            record
                .committed_at_millis
                .unwrap_or(record.checkpoint.updated_at_millis)
        } else {
            let now_millis = now_millis();
            if record.checkpoint.state == DomainCheckpointState::UploadInProgress {
                record
                    .checkpoint
                    .transition_to(DomainCheckpointState::Validating, now_millis)
                    .map_err(map_commit_checkpoint_error)?;
            }
            if record.checkpoint.state == DomainCheckpointState::Validating {
                record
                    .checkpoint
                    .transition_to(DomainCheckpointState::Committed, now_millis)
                    .map_err(map_commit_checkpoint_error)?;
            }
            if record.checkpoint.state != DomainCheckpointState::Committed {
                return Err(CommitCheckpointError::InvalidState);
            }
            record.committed_at_millis = Some(now_millis);
            now_millis
        };

        let response = record.clone();
        if let Some(run) = inner.runs.get_mut(&run_id) {
            run.latest_checkpoint_id = Some(CheckpointId {
                value: checkpoint_id,
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

        if let Some(store) = &self.metadata_store {
            if let Some(record) = store
                .load_latest_checkpoint(&run_id)
                .expect("load latest checkpoint")
            {
                return Ok(record);
            }
        }

        let inner = self.inner.read().expect("coordinator state lock poisoned");
        let run = inner
            .runs
            .get(&run_id)
            .ok_or(GetLatestCheckpointError::RunNotFound)?;
        let latest_checkpoint_id = run
            .latest_checkpoint_id
            .as_ref()
            .ok_or(GetLatestCheckpointError::NoCommittedCheckpoint)?;

        let record = inner
            .checkpoints_by_run
            .get(&run_id)
            .and_then(|records| records.get(&latest_checkpoint_id.value))
            .cloned()
            .ok_or(GetLatestCheckpointError::LatestCheckpointMissing)?;

        if record.checkpoint.state != DomainCheckpointState::Committed {
            return Err(GetLatestCheckpointError::LatestCheckpointNotCommitted);
        }

        Ok(record)
    }

    pub fn register_run(&self, request: RegisterRunRequest) -> RegisteredRun {
        let mut inner = self.inner.write().expect("coordinator state lock poisoned");

        if let Some(run_id) =
            idempotency_lookup(&inner.run_idempotency_keys, &request.idempotency_key)
        {
            let record = inner
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
        };

        if !request.idempotency_key.is_empty() {
            inner
                .run_idempotency_keys
                .insert(request.idempotency_key, run_id.clone());
        }
        inner.runs.insert(run_id.clone(), record.clone());
        self.persist_run(&record);

        RegisteredRun {
            run_id: RunId { value: run_id },
            registered_at,
        }
    }

    pub fn get_run(&self, run_id: &RunId) -> Option<RunRecord> {
        let inner = self.inner.read().expect("coordinator state lock poisoned");
        inner.runs.get(&run_id.value).cloned()
    }

    pub fn worker_counts(&self, run_id: &RunId) -> Option<(u64, u64)> {
        let inner = self.inner.read().expect("coordinator state lock poisoned");
        let workers = inner.workers_by_run.get(&run_id.value)?;
        let active = workers
            .values()
            .filter(|worker| worker.state == WorkerState::Active as i32)
            .count() as u64;
        let stale = workers
            .values()
            .filter(|worker| worker.state == WorkerState::Stale as i32)
            .count() as u64;
        Some((active, stale))
    }

    /// Reconciles in-memory worker liveness against the current server time.
    ///
    /// The MVP coordinator marks active workers stale when heartbeat deadlines
    /// pass and stale workers failed when the longer failure deadline passes.
    /// It does not remove or recover workers and it does not revoke shard
    /// leases.
    pub fn reconcile_worker_liveness(&self) {
        let mut inner = self.inner.write().expect("coordinator state lock poisoned");
        reconcile_worker_liveness(&mut inner, SystemTime::now());
    }

    #[cfg(test)]
    pub fn set_worker_heartbeat_deadline_for_test(
        &self,
        run_id: &RunId,
        worker_id: &WorkerId,
        deadline: SystemTime,
    ) -> bool {
        let mut inner = self.inner.write().expect("coordinator state lock poisoned");
        inner
            .workers_by_run
            .get_mut(&run_id.value)
            .and_then(|workers| workers.get_mut(&worker_id.value))
            .map(|worker| {
                worker.next_heartbeat_deadline = deadline;
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
        let mut inner = self.inner.write().expect("coordinator state lock poisoned");
        inner
            .workers_by_run
            .get_mut(&run_id.value)
            .and_then(|workers| workers.get_mut(&worker_id.value))
            .map(|worker| {
                worker.failure_deadline = deadline;
            })
            .is_some()
    }

    pub fn get_dataset(&self, run_id: &RunId, dataset_id: &DatasetId) -> Option<DatasetRecord> {
        let inner = self.inner.read().expect("coordinator state lock poisoned");
        inner
            .datasets_by_run
            .get(&run_id.value)
            .and_then(|datasets| datasets.get(&dataset_id.value))
            .cloned()
    }

    pub fn register_dataset(&self, request: RegisterDatasetRequest) -> Option<RegisteredDataset> {
        let run_id = request.run_id.as_ref()?.value.clone();
        let mut inner = self.inner.write().expect("coordinator state lock poisoned");

        if !inner.runs.contains_key(&run_id) {
            return None;
        }

        let idempotency_key = dataset_idempotency_key(&run_id, &request.idempotency_key);
        if let Some(dataset_id) =
            idempotency_lookup(&inner.dataset_idempotency_keys, &idempotency_key)
        {
            let record = inner
                .datasets_by_run
                .get(&run_id)
                .and_then(|datasets| datasets.get(&dataset_id))
                .expect("dataset idempotency key must reference an existing dataset");
            return Some(RegisteredDataset {
                dataset_id: DatasetId {
                    value: record.dataset_id.clone(),
                },
                accepted_shard_count: record.shards.len() as u32,
                registered_at: record.registered_at,
            });
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

        inner
            .datasets_by_run
            .entry(run_id.clone())
            .or_default()
            .insert(dataset_id.clone(), record.clone());

        if !request.idempotency_key.is_empty() {
            inner
                .dataset_idempotency_keys
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
        let inner = self.inner.read().expect("coordinator state lock poisoned");
        let run = inner
            .runs
            .get(&run_id.value)
            .ok_or(GetShardAssignmentsError::RunNotFound)?;

        let worker = inner
            .workers_by_run
            .get(&run_id.value)
            .and_then(|workers| workers.get(&worker_id.value))
            .ok_or(GetShardAssignmentsError::WorkerNotFound)?;
        let dataset = inner
            .datasets_by_run
            .get(&run_id.value)
            .and_then(|datasets| datasets.get(&dataset_id.value))
            .ok_or(GetShardAssignmentsError::DatasetNotFound)?;

        if run.world_size == 0 {
            return Err(GetShardAssignmentsError::EmptyWorldSize);
        }

        let server_time = SystemTime::now();
        let lease_expires_at = server_time + SHARD_LEASE_DURATION;
        let world_size = run.world_size as usize;
        let worker_rank = worker.rank as usize;
        let mut candidate_shards: Vec<&ShardRecord> = dataset
            .shards
            .iter()
            .enumerate()
            .filter(|(shard_index, _)| *shard_index % world_size == worker_rank)
            .map(|(_, shard)| shard)
            .collect();

        if self.placement_policy == DatasetPlacementPolicy::CacheAware {
            let hint_set: HashSet<&str> = cached_shard_hints
                .unwrap_or(&[])
                .iter()
                .map(String::as_str)
                .collect();
            candidate_shards.sort_by_key(|shard| {
                if hint_set.contains(shard.shard_id.as_str()) {
                    0
                } else {
                    1
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
                progress_offset: inner
                    .shard_progress
                    .get(&ShardProgressKey {
                        run_id: run.run_id.clone(),
                        dataset_id: dataset.dataset_id.clone(),
                        shard_id: shard.shard_id.clone(),
                    })
                    .map(|progress| progress.progress_offset)
                    .unwrap_or_default(),
                shard_uri: shard.shard_uri.clone(),
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
        let inner = self.inner.read().expect("coordinator state lock poisoned");
        let run = inner
            .runs
            .get(&run_id.value)
            .ok_or(RenewShardLeaseError::RunNotFound)?;

        let worker = inner
            .workers_by_run
            .get(&run_id.value)
            .and_then(|workers| workers.get(&worker_id.value))
            .ok_or(RenewShardLeaseError::WorkerNotFound)?;

        let (shard_index, _) = inner
            .datasets_by_run
            .get(&run_id.value)
            .and_then(|datasets| {
                datasets.values().find_map(|dataset| {
                    dataset
                        .shards
                        .iter()
                        .enumerate()
                        .find(|(_, shard)| shard.shard_id == shard_id.value)
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
        Self::ensure_worker_active_for_lease_actions(worker)
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
        let mut inner = self.inner.write().expect("coordinator state lock poisoned");
        let run = inner
            .runs
            .get(&run_id.value)
            .ok_or(ReportShardProgressError::RunNotFound)?;

        let worker = inner
            .workers_by_run
            .get(&run_id.value)
            .and_then(|workers| workers.get(&worker_id.value))
            .ok_or(ReportShardProgressError::WorkerNotFound)?;

        let dataset = inner
            .datasets_by_run
            .get(&run_id.value)
            .and_then(|datasets| datasets.get(&dataset_id.value))
            .ok_or(ReportShardProgressError::DatasetNotFound)?;

        let shard_index = dataset
            .shards
            .iter()
            .position(|shard| shard.shard_id == shard_id.value)
            .ok_or(ReportShardProgressError::ShardNotFound)?;

        if run.world_size == 0 {
            return Err(ReportShardProgressError::EmptyWorldSize);
        }

        let world_size = run.world_size as usize;
        let worker_rank = worker.rank as usize;
        if shard_index % world_size != worker_rank {
            return Err(ReportShardProgressError::NotShardOwner);
        }
        Self::ensure_worker_active_for_lease_actions(worker)
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
        inner.shard_progress.insert(key, record);

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
        let inner = self.inner.read().expect("coordinator state lock poisoned");
        inner
            .shard_progress
            .get(&ShardProgressKey {
                run_id: run_id.value.clone(),
                dataset_id: dataset_id.value.clone(),
                shard_id: shard_id.value.clone(),
            })
            .cloned()
    }

    pub fn register_worker(&self, request: RegisterWorkerRequest) -> Option<RegisteredWorker> {
        let run_id = request.run_id.as_ref()?.value.clone();
        let mut inner = self.inner.write().expect("coordinator state lock poisoned");

        if !inner.runs.contains_key(&run_id) {
            return None;
        }

        let world_size = inner
            .runs
            .get(&run_id)
            .expect("run must exist when registering worker")
            .world_size;
        let idempotency_key = worker_idempotency_key(&run_id, &request.idempotency_key);
        if let Some(worker_id) =
            idempotency_lookup(&inner.worker_idempotency_keys, &idempotency_key)
        {
            let record = inner
                .workers_by_run
                .get(&run_id)
                .and_then(|workers| workers.get(&worker_id))
                .expect("worker idempotency key must reference an existing worker");
            return Some(RegisteredWorker {
                worker_id: WorkerId {
                    value: record.worker_id.clone(),
                },
                state: record.state,
                lease_renewal_deadline: record.registered_at + HEARTBEAT_INTERVAL,
                server_time: record.registered_at,
            });
        }

        let worker_id = request
            .worker_id
            .as_ref()
            .map(|worker_id| worker_id.value.as_str())
            .filter(|worker_id| !worker_id.is_empty())
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

        inner
            .workers_by_run
            .entry(run_id.clone())
            .or_default()
            .insert(worker_id.clone(), worker.clone());
        refresh_active_worker_count(&mut inner, &run_id);

        if !request.idempotency_key.is_empty() {
            inner
                .worker_idempotency_keys
                .insert(idempotency_key, worker_id.clone());
        }

        self.persist_worker(&worker, world_size);

        Some(RegisteredWorker {
            worker_id: WorkerId { value: worker_id },
            state: WorkerState::Active as i32,
            lease_renewal_deadline: next_heartbeat_deadline,
            server_time: now,
        })
    }

    pub fn record_heartbeat(
        &self,
        run_id: &RunId,
        worker_id: &WorkerId,
    ) -> Option<WorkerHeartbeat> {
        let mut inner = self.inner.write().expect("coordinator state lock poisoned");

        if !inner.runs.contains_key(&run_id.value) {
            return None;
        }

        let worker = inner
            .workers_by_run
            .get_mut(&run_id.value)
            .and_then(|workers| workers.get_mut(&worker_id.value))?;
        let now = SystemTime::now();
        let next_heartbeat_deadline = now + HEARTBEAT_INTERVAL;
        let failure_deadline = now + WORKER_FAILURE_INTERVAL;
        worker.last_heartbeat_at = now;
        worker.next_heartbeat_deadline = next_heartbeat_deadline;
        worker.failure_deadline = failure_deadline;
        worker.state = WorkerState::Active as i32;
        let state = worker.state;
        let worker_snapshot = worker.clone();
        let world_size = inner
            .runs
            .get(&run_id.value)
            .expect("run must exist when recording heartbeat")
            .world_size;

        refresh_active_worker_count(&mut inner, &run_id.value);
        self.persist_worker(&worker_snapshot, world_size);

        Some(WorkerHeartbeat {
            state,
            next_heartbeat_deadline,
            server_time: now,
        })
    }

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
            .map(|shard_id| shard_id.value.as_str())
            .filter(|shard_id| !shard_id.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| self.generate_shard_id());
        let shard_dataset_id = shard
            .dataset_id
            .as_ref()
            .map(|dataset_id| dataset_id.value.as_str())
            .filter(|dataset_id| !dataset_id.is_empty())
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

fn idempotency_lookup(
    idempotency_keys: &HashMap<String, String>,
    idempotency_key: &str,
) -> Option<String> {
    if idempotency_key.is_empty() {
        return None;
    }

    idempotency_keys.get(idempotency_key).cloned()
}

fn reconcile_worker_liveness(inner: &mut CoordinatorStateInner, now: SystemTime) {
    let mut changed_run_ids = Vec::new();

    for (run_id, workers) in &mut inner.workers_by_run {
        let mut changed = false;
        for worker in workers.values_mut() {
            if worker.state == WorkerState::Active as i32 {
                if worker.next_heartbeat_deadline < now {
                    worker.state = WorkerState::Stale as i32;
                    changed = true;
                }
            } else if worker.state == WorkerState::Stale as i32 && worker.failure_deadline < now {
                worker.state = WorkerState::Failed as i32;
                changed = true;
            }
        }

        if changed {
            changed_run_ids.push(run_id.clone());
        }
    }

    for run_id in changed_run_ids {
        refresh_active_worker_count(inner, &run_id);
    }
}

fn refresh_active_worker_count(inner: &mut CoordinatorStateInner, run_id: &str) {
    let active_worker_count = inner
        .workers_by_run
        .get(run_id)
        .map(|workers| {
            workers
                .values()
                .filter(|worker| worker.state == WorkerState::Active as i32)
                .count() as u32
        })
        .unwrap_or_default();

    if let Some(run) = inner.runs.get_mut(run_id) {
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
        .map(|dataset_id| dataset_id.value.as_str())
        .filter(|dataset_id| !dataset_id.is_empty())
        .map(ToOwned::to_owned)
}

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
            .inner
            .read()
            .unwrap()
            .workers_by_run
            .get(&run_id.value)
            .and_then(|workers| workers.get(&worker_id.value))
            .cloned()
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
            .inner
            .read()
            .unwrap()
            .workers_by_run
            .get(&run_id.value)
            .and_then(|workers| workers.get(&worker_id.value))
            .cloned()
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
            .inner
            .read()
            .unwrap()
            .workers_by_run
            .get(&run_id.value)
            .and_then(|workers| workers.get(&worker_id.value))
            .cloned()
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
            .inner
            .read()
            .unwrap()
            .workers_by_run
            .get(&run_id.value)
            .and_then(|workers| workers.get(&worker_id.value))
            .cloned()
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
            .inner
            .read()
            .unwrap()
            .workers_by_run
            .get(&run_id.value)
            .and_then(|workers| workers.get(&worker_id.value))
            .cloned()
            .expect("worker should exist");
        assert_eq!(stale_worker.state, WorkerState::Stale as i32);

        assert!(state.set_worker_failure_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();

        let failed_worker = state
            .inner
            .read()
            .unwrap()
            .workers_by_run
            .get(&run_id.value)
            .and_then(|workers| workers.get(&worker_id.value))
            .cloned()
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

        let worker = state
            .inner
            .read()
            .unwrap()
            .workers_by_run
            .get(&run_id.value)
            .and_then(|workers| workers.get(&worker_id.value))
            .cloned();
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
            .inner
            .read()
            .unwrap()
            .workers_by_run
            .get(&run_id.value)
            .and_then(|workers| workers.get(&worker_id.value))
            .cloned()
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
        let dataset = state
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
        let state = CoordinatorState::with_metadata_store(store);

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
                "commit".to_string(),
            )
            .unwrap();

        drop(state);

        let reopened_store = SqliteMetadataStore::open(&path).unwrap();
        reopened_store.init_schema().unwrap();
        let recreated = CoordinatorState::with_metadata_store(reopened_store);

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
        assert_eq!(state.inner.read().unwrap().runs.len(), 1);
    }

    #[test]
    fn empty_idempotency_key_creates_distinct_runs() {
        let state = CoordinatorState::default();

        let first = state.register_run(register_request(""));
        let second = state.register_run(register_request(""));

        assert_ne!(first.run_id, second.run_id);
        assert_eq!(state.inner.read().unwrap().runs.len(), 2);
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
            .commit_checkpoint("missing".to_string(), "ckpt-1".to_string(), String::new())
            .expect_err("missing run should fail");
        assert_eq!(err, CommitCheckpointError::RunNotFound);

        let run_id = state.register_run(register_request("run-key")).run_id;
        let err = state
            .commit_checkpoint(run_id.value, "missing-ckpt".to_string(), String::new())
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
            .commit_checkpoint(run_id.value, "ckpt-1".to_string(), String::new())
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
            .commit_checkpoint(run_id.value, "ckpt-1".to_string(), "commit-2".to_string())
            .expect("repeated commit should be harmless");
        assert_eq!(
            repeated.record.checkpoint.state,
            DomainCheckpointState::Committed
        );
    }
}
