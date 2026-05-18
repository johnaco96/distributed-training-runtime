#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::SystemTime;

use runtime_proto::runtime::v1::{CheckpointId, CheckpointState, RegisterRunRequest, RunId};

/// Shared coordinator state.
///
/// This is intentionally in-memory only for the MVP coordinator. It is not
/// persisted and will be lost when the process exits.
#[derive(Debug, Default)]
pub struct CoordinatorState {
    inner: RwLock<CoordinatorStateInner>,
    next_run_id: AtomicU64,
}

#[derive(Debug, Default)]
struct CoordinatorStateInner {
    runs: HashMap<String, RunRecord>,
    idempotency_keys: HashMap<String, String>,
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
pub struct RegisteredRun {
    pub run_id: RunId,
    pub registered_at: SystemTime,
}

impl CoordinatorState {
    pub fn register_run(&self, request: RegisterRunRequest) -> RegisteredRun {
        let mut inner = self.inner.write().expect("coordinator state lock poisoned");

        if let Some(run_id) = idempotency_lookup(&inner, &request.idempotency_key) {
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
                .idempotency_keys
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

    fn generate_run_id(&self) -> String {
        let id = self.next_run_id.fetch_add(1, Ordering::Relaxed) + 1;
        format!("run-{id}")
    }
}

fn idempotency_lookup(inner: &CoordinatorStateInner, idempotency_key: &str) -> Option<String> {
    if idempotency_key.is_empty() {
        return None;
    }

    inner.idempotency_keys.get(idempotency_key).cloned()
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
