//! Shared fixtures for the `CoordinatorState` unit tests.

use super::*;
use runtime_proto::runtime::v1::{AssignmentPolicy, StorageBackendKind};

pub(crate) fn register_request(idempotency_key: &str) -> RegisterRunRequest {
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

pub(crate) fn worker_request(run_id: RunId, idempotency_key: &str) -> RegisterWorkerRequest {
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

pub(crate) fn dataset_request(run_id: RunId, idempotency_key: &str) -> RegisterDatasetRequest {
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

/// Register a run with two workers, begin a checkpoint, and report one
/// artifact per worker.  Returns (run_id, worker_a, worker_b).
pub(crate) fn setup_two_worker_checkpoint(
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

pub(crate) fn register_lifecycle_run(state: &CoordinatorState, world_size: u32) -> RunId {
    let registered = state.register_run(RegisterRunRequest {
        idempotency_key: format!("lifecycle-{world_size}"),
        run_name: "lifecycle".to_string(),
        assignment_policy: AssignmentPolicy::StaticRank as i32,
        storage_backend_kind: StorageBackendKind::LocalFilesystem as i32,
        world_size,
        run_version: "v1".to_string(),
        ..Default::default()
    });
    registered.run_id
}

pub(crate) fn register_lifecycle_worker(state: &CoordinatorState, run_id: &RunId, name: &str) {
    state
        .register_worker(RegisterWorkerRequest {
            idempotency_key: String::new(),
            run_id: Some(run_id.clone()),
            worker_id: Some(WorkerId {
                value: name.to_string(),
            }),
            rank: 0,
            host: "host".to_string(),
            sdk_version: String::new(),
            protocol_version: String::new(),
        })
        .expect("worker registers");
}

pub(crate) fn run_state_of(state: &CoordinatorState, run_id: &RunId) -> i32 {
    state.get_run(run_id).expect("run exists").state
}

/// Register a run + worker and commit `count` checkpoints at steps 1..=count.
pub(crate) fn state_with_committed_checkpoints(
    state: &CoordinatorState,
    count: u64,
) -> (RunId, WorkerId) {
    let run_id = state.register_run(register_request("ckpt-run")).run_id;
    let worker = state
        .register_worker(worker_request(run_id.clone(), "ckpt-worker"))
        .expect("worker registers")
        .worker_id;
    for step in 1..=count {
        let checkpoint_id = format!("ckpt-{step}");
        state
            .begin_checkpoint(
                run_id.value.clone(),
                worker.value.clone(),
                checkpoint_id.clone(),
                step,
                format!("begin-{checkpoint_id}"),
            )
            .expect("begin");
        state
            .report_checkpoint_artifact(
                run_id.value.clone(),
                worker.value.clone(),
                checkpoint_id.clone(),
                "model.bin".to_string(),
                format!("file:///a/{checkpoint_id}/model.bin"),
                10,
                "sum".to_string(),
                "sha256".to_string(),
                format!("artifact-{checkpoint_id}"),
            )
            .expect("artifact");
        state
            .commit_checkpoint(
                run_id.value.clone(),
                checkpoint_id.clone(),
                format!("commit-{checkpoint_id}"),
                String::new(),
            )
            .expect("commit");
    }
    (run_id, worker)
}
