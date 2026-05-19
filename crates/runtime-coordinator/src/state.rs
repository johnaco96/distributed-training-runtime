#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, SystemTime};

use runtime_proto::runtime::v1::{
    CheckpointId, CheckpointState, DatasetId, DatasetMetadata, RegisterDatasetRequest,
    RegisterRunRequest, RegisterWorkerRequest, RunId, ShardId, ShardLeaseState, ShardMetadata,
    WorkerId, WorkerState,
};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const SHARD_LEASE_DURATION: Duration = Duration::from_secs(30);

/// Shared coordinator state.
///
/// This is intentionally in-memory only for the MVP coordinator. It is not
/// persisted and will be lost when the process exits.
#[derive(Debug, Default)]
pub struct CoordinatorState {
    inner: RwLock<CoordinatorStateInner>,
    next_run_id: AtomicU64,
    next_worker_id: AtomicU64,
    next_dataset_id: AtomicU64,
    next_shard_id: AtomicU64,
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

#[derive(Clone, Debug, PartialEq, Eq)]
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
}

impl CoordinatorState {
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
        inner.runs.insert(run_id.clone(), record);

        RegisteredRun {
            run_id: RunId { value: run_id },
            registered_at,
        }
    }

    pub fn get_run(&self, run_id: &RunId) -> Option<RunRecord> {
        let inner = self.inner.read().expect("coordinator state lock poisoned");
        inner.runs.get(&run_id.value).cloned()
    }

    /// Reconciles in-memory worker liveness against the current server time.
    ///
    /// The MVP coordinator only marks active workers stale when their heartbeat
    /// deadline has passed. It does not fail, remove, or recover workers and it
    /// does not revoke shard leases.
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
            .insert(dataset_id.clone(), record);

        if !request.idempotency_key.is_empty() {
            inner
                .dataset_idempotency_keys
                .insert(idempotency_key, dataset_id.clone());
        }

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
        let assignments = dataset
            .shards
            .iter()
            .enumerate()
            .filter(|(shard_index, _)| *shard_index % world_size == worker_rank)
            .map(|(_, shard)| ShardAssignmentRecord {
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
        };

        inner
            .workers_by_run
            .entry(run_id.clone())
            .or_default()
            .insert(worker_id.clone(), worker);
        refresh_active_worker_count(&mut inner, &run_id);

        if !request.idempotency_key.is_empty() {
            inner
                .worker_idempotency_keys
                .insert(idempotency_key, worker_id.clone());
        }

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
        worker.last_heartbeat_at = now;
        worker.next_heartbeat_deadline = next_heartbeat_deadline;
        worker.state = WorkerState::Active as i32;
        let state = worker.state;

        refresh_active_worker_count(&mut inner, &run_id.value);

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
            if worker.state == WorkerState::Active as i32 && worker.next_heartbeat_deadline < now {
                worker.state = WorkerState::Stale as i32;
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
    use runtime_proto::runtime::v1::{AssignmentPolicy, StorageBackendKind};

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
}
