#![forbid(unsafe_code)]

use std::sync::Arc;

use runtime_proto::runtime::v1::coordinator_service_server::CoordinatorService;
use runtime_proto::runtime::v1::*;
use tonic::{Request, Response, Status};

use crate::state::{
    BeginCheckpointError, CommitCheckpointError, CoordinatorState, GetLatestCheckpointError,
    GetShardAssignmentsError, RenewShardLeaseError, ReportCheckpointArtifactError,
    ReportShardProgressError,
};

#[derive(Clone, Debug)]
pub struct CoordinatorServiceImpl {
    state: Arc<CoordinatorState>,
}

impl CoordinatorServiceImpl {
    pub fn new(state: Arc<CoordinatorState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl CoordinatorService for CoordinatorServiceImpl {
    async fn register_run(
        &self,
        request: Request<RegisterRunRequest>,
    ) -> Result<Response<RegisterRunResponse>, Status> {
        let registered = self.state.register_run(request.into_inner());

        let response = RegisterRunResponse {
            run_id: Some(registered.run_id),
            coordinator_version: env!("CARGO_PKG_VERSION").to_string(),
            registered_at: Some(registered.registered_at.into()),
        };

        Ok(Response::new(response))
    }

    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        let request = request.into_inner();
        let run_id = request
            .run_id
            .clone()
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;

        let registered = self
            .state
            .register_worker(request)
            .ok_or_else(|| Status::not_found(format!("run {} was not found", run_id.value)))?;

        let response = RegisterWorkerResponse {
            worker_id: Some(registered.worker_id),
            state: registered.state,
            lease_renewal_deadline: Some(registered.lease_renewal_deadline.into()),
            server_time: Some(registered.server_time.into()),
        };

        Ok(Response::new(response))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let request = request.into_inner();
        let run_id = request
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;
        let worker_id = request
            .worker_id
            .ok_or_else(|| Status::invalid_argument("worker_id is required"))?;

        let heartbeat = self
            .state
            .record_heartbeat(&run_id, &worker_id)
            .ok_or_else(|| Status::not_found("run or worker was not found"))?;

        let response = HeartbeatResponse {
            state: heartbeat.state,
            next_heartbeat_deadline: Some(heartbeat.next_heartbeat_deadline.into()),
            server_time: Some(heartbeat.server_time.into()),
        };

        Ok(Response::new(response))
    }

    async fn register_dataset(
        &self,
        request: Request<RegisterDatasetRequest>,
    ) -> Result<Response<RegisterDatasetResponse>, Status> {
        let request = request.into_inner();
        let run_id = request
            .run_id
            .clone()
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;

        let registered = self
            .state
            .register_dataset(request)
            .ok_or_else(|| Status::not_found(format!("run {} was not found", run_id.value)))?;

        let response = RegisterDatasetResponse {
            dataset_id: Some(registered.dataset_id),
            accepted_shard_count: registered.accepted_shard_count,
            registered_at: Some(registered.registered_at.into()),
        };

        Ok(Response::new(response))
    }

    async fn get_shard_assignment(
        &self,
        request: Request<GetShardAssignmentRequest>,
    ) -> Result<Response<GetShardAssignmentResponse>, Status> {
        let request = request.into_inner();
        let run_id = request
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;
        let worker_id = request
            .worker_id
            .ok_or_else(|| Status::invalid_argument("worker_id is required"))?;
        let dataset_id = request
            .dataset_id
            .ok_or_else(|| Status::invalid_argument("dataset_id is required"))?;

        let shard_assignments = self
            .state
            .get_shard_assignments(&run_id, &worker_id, &dataset_id)
            .map_err(|error| match error {
                GetShardAssignmentsError::RunNotFound => {
                    Status::not_found(format!("run {} was not found", run_id.value))
                }
                GetShardAssignmentsError::WorkerNotFound => {
                    Status::not_found(format!("worker {} was not found", worker_id.value))
                }
                GetShardAssignmentsError::DatasetNotFound => {
                    Status::not_found(format!("dataset {} was not found", dataset_id.value))
                }
                GetShardAssignmentsError::EmptyWorldSize => {
                    Status::failed_precondition("run world_size must be greater than zero")
                }
            })?;

        let assignments = shard_assignments
            .assignments
            .into_iter()
            .map(|assignment| ShardAssignment {
                run_id: Some(RunId {
                    value: assignment.run_id,
                }),
                dataset_id: Some(DatasetId {
                    value: assignment.dataset_id,
                }),
                shard_id: Some(ShardId {
                    value: assignment.shard_id,
                }),
                worker_id: Some(WorkerId {
                    value: assignment.worker_id,
                }),
                lease_epoch: assignment.lease_epoch,
                lease_state: assignment.lease_state,
                lease_expires_at: Some(assignment.lease_expires_at.into()),
                progress_offset: assignment.progress_offset,
                shard_uri: assignment.shard_uri,
            })
            .collect();

        let response = GetShardAssignmentResponse {
            assignments,
            assignment_policy: shard_assignments.assignment_policy,
            server_time: Some(shard_assignments.server_time.into()),
        };

        Ok(Response::new(response))
    }

    async fn renew_shard_lease(
        &self,
        request: Request<RenewShardLeaseRequest>,
    ) -> Result<Response<RenewShardLeaseResponse>, Status> {
        let request = request.into_inner();
        let run_id = request
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;
        let worker_id = request
            .worker_id
            .ok_or_else(|| Status::invalid_argument("worker_id is required"))?;
        let shard_id = request
            .shard_id
            .ok_or_else(|| Status::invalid_argument("shard_id is required"))?;

        let renewed = self
            .state
            .renew_shard_lease(&run_id, &worker_id, &shard_id, request.lease_epoch)
            .map_err(|error| match error {
                RenewShardLeaseError::RunNotFound => {
                    Status::not_found(format!("run {} was not found", run_id.value))
                }
                RenewShardLeaseError::WorkerNotFound => {
                    Status::not_found(format!("worker {} was not found", worker_id.value))
                }
                RenewShardLeaseError::ShardNotFound => {
                    Status::not_found(format!("shard {} was not found", shard_id.value))
                }
                RenewShardLeaseError::EmptyWorldSize => {
                    Status::failed_precondition("run world_size must be greater than zero")
                }
                RenewShardLeaseError::NotShardOwner => Status::failed_precondition(format!(
                    "worker {} is not the owner of shard {}",
                    worker_id.value, shard_id.value
                )),
                RenewShardLeaseError::InvalidLeaseEpoch => {
                    Status::failed_precondition("lease_epoch does not match current assignment")
                }
            })?;

        let response = RenewShardLeaseResponse {
            accepted: renewed.accepted,
            lease_state: renewed.lease_state,
            lease_expires_at: Some(renewed.lease_expires_at.into()),
            server_time: Some(renewed.server_time.into()),
        };

        Ok(Response::new(response))
    }

    async fn report_shard_progress(
        &self,
        request: Request<ReportShardProgressRequest>,
    ) -> Result<Response<ReportShardProgressResponse>, Status> {
        let request = request.into_inner();
        let run_id = request
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;
        let worker_id = request
            .worker_id
            .ok_or_else(|| Status::invalid_argument("worker_id is required"))?;
        let dataset_id = request
            .dataset_id
            .ok_or_else(|| Status::invalid_argument("dataset_id is required"))?;
        let shard_id = request
            .shard_id
            .ok_or_else(|| Status::invalid_argument("shard_id is required"))?;

        let reported = self
            .state
            .report_shard_progress(
                &run_id,
                &worker_id,
                &dataset_id,
                &shard_id,
                request.lease_epoch,
                request.progress_offset,
                request.is_complete,
            )
            .map_err(|error| match error {
                ReportShardProgressError::RunNotFound => {
                    Status::not_found(format!("run {} was not found", run_id.value))
                }
                ReportShardProgressError::WorkerNotFound => {
                    Status::not_found(format!("worker {} was not found", worker_id.value))
                }
                ReportShardProgressError::DatasetNotFound => {
                    Status::not_found(format!("dataset {} was not found", dataset_id.value))
                }
                ReportShardProgressError::ShardNotFound => {
                    Status::not_found(format!("shard {} was not found", shard_id.value))
                }
                ReportShardProgressError::EmptyWorldSize => {
                    Status::failed_precondition("run world_size must be greater than zero")
                }
                ReportShardProgressError::NotShardOwner => Status::failed_precondition(format!(
                    "worker {} is not the owner of shard {}",
                    worker_id.value, shard_id.value
                )),
                ReportShardProgressError::InvalidLeaseEpoch => {
                    Status::failed_precondition("lease_epoch does not match current assignment")
                }
            })?;

        let response = ReportShardProgressResponse {
            accepted: reported.accepted,
            stored_progress_offset: reported.stored_progress_offset,
            lease_state: reported.lease_state,
        };

        Ok(Response::new(response))
    }

    async fn begin_checkpoint(
        &self,
        request: Request<BeginCheckpointRequest>,
    ) -> Result<Response<BeginCheckpointResponse>, Status> {
        let request = request.into_inner();
        let run_id = request
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;
        let worker_id = request
            .worker_id
            .ok_or_else(|| Status::invalid_argument("worker_id is required"))?;

        let checkpoint = self
            .state
            .begin_checkpoint(
                run_id.value.clone(),
                worker_id.value.clone(),
                request
                    .checkpoint_id
                    .map(|checkpoint_id| checkpoint_id.value)
                    .unwrap_or_default(),
                request.step,
                request.idempotency_key,
            )
            .map_err(|error| match error {
                BeginCheckpointError::RunNotFound => {
                    Status::not_found(format!("run {} was not found", run_id.value))
                }
                BeginCheckpointError::WorkerNotFound => {
                    Status::not_found(format!("worker {} was not found", worker_id.value))
                }
                BeginCheckpointError::InvalidWorkerId => {
                    Status::invalid_argument("worker_id must be non-empty")
                }
                BeginCheckpointError::CheckpointConflict => {
                    Status::already_exists("checkpoint_id already exists with conflicting metadata")
                }
            })?;
        let created_at = std::time::UNIX_EPOCH
            + std::time::Duration::from_millis(checkpoint.checkpoint.created_at_millis);

        Ok(Response::new(BeginCheckpointResponse {
            checkpoint_id: Some(CheckpointId {
                value: checkpoint.checkpoint.checkpoint_id.0,
            }),
            state: CheckpointState::Initiated as i32,
            created_at: Some(created_at.into()),
            artifact_specs: Vec::new(),
        }))
    }

    async fn report_checkpoint_artifact(
        &self,
        request: Request<ReportCheckpointArtifactRequest>,
    ) -> Result<Response<ReportCheckpointArtifactResponse>, Status> {
        let request = request.into_inner();
        let run_id = request
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;
        let worker_id = request
            .worker_id
            .ok_or_else(|| Status::invalid_argument("worker_id is required"))?;
        let checkpoint_id = request
            .checkpoint_id
            .ok_or_else(|| Status::invalid_argument("checkpoint_id is required"))?;
        let artifact_report = request
            .artifact_report
            .ok_or_else(|| Status::invalid_argument("artifact_report is required"))?;

        let checkpoint = self
            .state
            .report_checkpoint_artifact(
                run_id.value.clone(),
                worker_id.value.clone(),
                checkpoint_id.value.clone(),
                artifact_report.artifact_name,
                artifact_report.artifact_uri,
                artifact_report.size_bytes.max(0) as u64,
                artifact_report.checksum,
                artifact_report.checksum_algorithm,
                request.idempotency_key,
            )
            .map_err(|error| match error {
                ReportCheckpointArtifactError::RunNotFound => {
                    Status::not_found(format!("run {} was not found", run_id.value))
                }
                ReportCheckpointArtifactError::WorkerNotFound => {
                    Status::not_found(format!("worker {} was not found", worker_id.value))
                }
                ReportCheckpointArtifactError::CheckpointNotFound => {
                    Status::not_found(format!("checkpoint {} was not found", checkpoint_id.value))
                }
                ReportCheckpointArtifactError::InvalidInput(message) => {
                    Status::invalid_argument(message)
                }
                ReportCheckpointArtifactError::InvalidState => Status::failed_precondition(
                    "checkpoint is in terminal state or cannot accept artifacts",
                ),
            })?;

        Ok(Response::new(ReportCheckpointArtifactResponse {
            accepted: true,
            checkpoint_state: checkpoint_state_to_proto(checkpoint.checkpoint.state),
        }))
    }

    async fn commit_checkpoint(
        &self,
        request: Request<CommitCheckpointRequest>,
    ) -> Result<Response<CommitCheckpointResponse>, Status> {
        let request = request.into_inner();
        let run_id = request
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;
        let checkpoint_id = request
            .checkpoint_id
            .ok_or_else(|| Status::invalid_argument("checkpoint_id is required"))?;

        let checkpoint = self
            .state
            .commit_checkpoint(
                run_id.value.clone(),
                checkpoint_id.value.clone(),
                request.idempotency_key,
            )
            .map_err(|error| match error {
                CommitCheckpointError::RunNotFound => {
                    Status::not_found(format!("run {} was not found", run_id.value))
                }
                CommitCheckpointError::CheckpointNotFound => {
                    Status::not_found(format!("checkpoint {} was not found", checkpoint_id.value))
                }
                CommitCheckpointError::InvalidInput(message) => Status::invalid_argument(message),
                CommitCheckpointError::InvalidState => Status::failed_precondition(
                    "checkpoint is in terminal state, missing artifacts, or cannot be committed",
                ),
            })?;
        Ok(Response::new(CommitCheckpointResponse {
            state: checkpoint_state_to_proto(checkpoint.record.checkpoint.state),
            committed_at: Some(millis_to_timestamp(checkpoint.committed_at_millis)),
        }))
    }

    async fn get_latest_checkpoint(
        &self,
        request: Request<GetLatestCheckpointRequest>,
    ) -> Result<Response<GetLatestCheckpointResponse>, Status> {
        let request = request.into_inner();
        let run_id = request
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;

        let checkpoint = self
            .state
            .get_latest_checkpoint(run_id.value.clone())
            .map_err(|error| match error {
                GetLatestCheckpointError::InvalidInput(message) => {
                    Status::invalid_argument(message)
                }
                GetLatestCheckpointError::RunNotFound
                | GetLatestCheckpointError::NoCommittedCheckpoint => {
                    Status::not_found("run has no committed checkpoint")
                }
                GetLatestCheckpointError::LatestCheckpointMissing
                | GetLatestCheckpointError::LatestCheckpointNotCommitted => {
                    Status::failed_precondition(
                        "latest checkpoint metadata is inconsistent or not committed",
                    )
                }
            })?;

        let manifest = CheckpointManifest {
            run_id: Some(RunId {
                value: checkpoint.checkpoint.run_id.0,
            }),
            checkpoint_id: Some(CheckpointId {
                value: checkpoint.checkpoint.checkpoint_id.0,
            }),
            step: checkpoint.checkpoint.step,
            state: checkpoint_state_to_proto(checkpoint.checkpoint.state),
            storage_backend_kind: self
                .state
                .get_run(&run_id)
                .ok_or_else(|| Status::failed_precondition("run metadata missing"))?
                .storage_backend_kind,
            artifacts: checkpoint
                .checkpoint
                .artifacts
                .into_iter()
                .map(|artifact| ArtifactReport {
                    artifact_name: artifact.artifact_name,
                    artifact_uri: artifact.storage_key,
                    size_bytes: artifact.size_bytes as i64,
                    checksum: artifact.checksum,
                    checksum_algorithm: artifact.checksum_algorithm,
                    uploaded_at: Some(millis_to_timestamp(checkpoint.checkpoint.updated_at_millis)),
                })
                .collect(),
            created_at: Some(millis_to_timestamp(checkpoint.checkpoint.created_at_millis)),
            committed_at: checkpoint.committed_at_millis.map(millis_to_timestamp),
        };

        Ok(Response::new(GetLatestCheckpointResponse {
            manifest: Some(manifest),
        }))
    }

    async fn get_run_status(
        &self,
        request: Request<GetRunStatusRequest>,
    ) -> Result<Response<GetRunStatusResponse>, Status> {
        let request = request.into_inner();
        let run_id = request
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;

        self.state.reconcile_worker_liveness();

        let record = self
            .state
            .get_run(&run_id)
            .ok_or_else(|| Status::not_found("run was not found"))?;

        let response = GetRunStatusResponse {
            run_id: Some(run_id),
            run_version: record.run_version,
            assignment_policy: record.assignment_policy,
            storage_backend_kind: record.storage_backend_kind,
            latest_checkpoint_id: record.latest_checkpoint_id,
            latest_checkpoint_state: record.latest_checkpoint_state,
            active_worker_count: record.active_worker_count,
            server_time: Some(std::time::SystemTime::now().into()),
        };

        Ok(Response::new(response))
    }
}

fn checkpoint_state_to_proto(state: runtime_checkpoint::CheckpointState) -> i32 {
    match state {
        runtime_checkpoint::CheckpointState::Initiated => CheckpointState::Initiated as i32,
        runtime_checkpoint::CheckpointState::UploadInProgress => {
            CheckpointState::UploadInProgress as i32
        }
        runtime_checkpoint::CheckpointState::Validating => CheckpointState::Validating as i32,
        runtime_checkpoint::CheckpointState::Committed => CheckpointState::Committed as i32,
        runtime_checkpoint::CheckpointState::Failed => CheckpointState::Failed as i32,
        runtime_checkpoint::CheckpointState::Aborted => CheckpointState::Aborted as i32,
    }
}
fn millis_to_timestamp(millis: u64) -> prost_types::Timestamp {
    let seconds = (millis / 1000) as i64;
    let nanos = ((millis % 1000) * 1_000_000) as i32;
    prost_types::Timestamp { seconds, nanos }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime_proto::runtime::v1::{AssignmentPolicy, StorageBackendKind};

    fn register_request(idempotency_key: &str) -> RegisterRunRequest {
        register_request_with_world_size(idempotency_key, 8)
    }

    fn register_request_with_world_size(
        idempotency_key: &str,
        world_size: u32,
    ) -> RegisterRunRequest {
        RegisterRunRequest {
            idempotency_key: idempotency_key.to_string(),
            run_name: "service-test-run".to_string(),
            assignment_policy: AssignmentPolicy::ReassignQueue as i32,
            storage_backend_kind: StorageBackendKind::S3Compatible as i32,
            world_size,
            run_version: "2026-05-18".to_string(),
        }
    }

    fn worker_request(run_id: RunId, idempotency_key: &str) -> RegisterWorkerRequest {
        worker_request_with_rank(run_id, idempotency_key, 3)
    }

    fn worker_request_with_rank(
        run_id: RunId,
        idempotency_key: &str,
        rank: u32,
    ) -> RegisterWorkerRequest {
        RegisterWorkerRequest {
            idempotency_key: idempotency_key.to_string(),
            run_id: Some(run_id),
            worker_id: None,
            rank,
            host: "worker-host".to_string(),
            sdk_version: "sdk-0.1.0".to_string(),
            protocol_version: "runtime.v1".to_string(),
        }
    }

    async fn create_run(service: &CoordinatorServiceImpl) -> RunId {
        create_run_with_world_size(service, 8).await
    }

    async fn create_run_with_world_size(
        service: &CoordinatorServiceImpl,
        world_size: u32,
    ) -> RunId {
        service
            .register_run(Request::new(register_request_with_world_size(
                "service-key",
                world_size,
            )))
            .await
            .expect("register_run should succeed")
            .into_inner()
            .run_id
            .expect("register response should include run_id")
    }

    fn dataset_request(
        run_id: RunId,
        idempotency_key: &str,
        dataset_id: Option<&str>,
        shard_ids: &[Option<&str>],
    ) -> RegisterDatasetRequest {
        let dataset_id = dataset_id.map(|value| DatasetId {
            value: value.to_string(),
        });
        let shards = shard_ids
            .iter()
            .enumerate()
            .map(|(index, shard_id)| ShardMetadata {
                shard_id: (*shard_id).map(|value| ShardId {
                    value: value.to_string(),
                }),
                dataset_id: None,
                shard_uri: format!("s3://bucket/dataset/shard-{index}.jsonl"),
                byte_range_start: (index as i64) * 100,
                byte_range_end: ((index as i64) + 1) * 100 - 1,
                size_bytes: 100,
                checksum: format!("checksum-{index}"),
                checksum_algorithm: "sha256".to_string(),
            })
            .collect();

        RegisterDatasetRequest {
            idempotency_key: idempotency_key.to_string(),
            run_id: Some(run_id),
            dataset: Some(DatasetMetadata {
                dataset_id,
                name: "training-data".to_string(),
                dataset_uri: "s3://bucket/dataset".to_string(),
                format: "jsonl".to_string(),
                shard_count: shard_ids.len() as u32,
                dataset_version: "v1".to_string(),
                labels: [("split".to_string(), "train".to_string())]
                    .into_iter()
                    .collect(),
                registered_at: None,
            }),
            shards,
        }
    }

    #[tokio::test]
    async fn register_run_and_get_run_status_round_trip() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));

        let register_response = service
            .register_run(Request::new(register_request("service-key")))
            .await
            .expect("register_run should succeed")
            .into_inner();
        let run_id = register_response
            .run_id
            .clone()
            .expect("register response should include run_id");

        let status = service
            .get_run_status(Request::new(GetRunStatusRequest {
                run_id: Some(run_id.clone()),
            }))
            .await
            .expect("get_run_status should succeed")
            .into_inner();

        assert_eq!(status.run_id, Some(run_id));
        assert_eq!(status.run_version, "2026-05-18");
        assert_eq!(
            status.assignment_policy,
            AssignmentPolicy::ReassignQueue as i32
        );
        assert_eq!(
            status.storage_backend_kind,
            StorageBackendKind::S3Compatible as i32
        );
        assert_eq!(status.latest_checkpoint_id, None);
        assert_eq!(
            status.latest_checkpoint_state,
            CheckpointState::Unspecified as i32
        );
        assert_eq!(status.active_worker_count, 0);
        assert!(status.server_time.is_some());
    }

    #[tokio::test]
    async fn register_run_reuses_run_for_idempotency_key() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));

        let first = service
            .register_run(Request::new(register_request("service-key")))
            .await
            .expect("first register_run should succeed")
            .into_inner();
        let second = service
            .register_run(Request::new(register_request("service-key")))
            .await
            .expect("second register_run should succeed")
            .into_inner();

        assert_eq!(first.run_id, second.run_id);
        assert_eq!(first.registered_at, second.registered_at);
    }

    #[tokio::test]
    async fn register_dataset_under_existing_run_succeeds() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run(&service).await;

        let response = service
            .register_dataset(Request::new(dataset_request(
                run_id,
                "dataset-key",
                Some("dataset-explicit"),
                &[Some("shard-a"), Some("shard-b")],
            )))
            .await
            .expect("register_dataset should succeed")
            .into_inner();

        assert_eq!(
            response.dataset_id,
            Some(DatasetId {
                value: "dataset-explicit".to_string(),
            })
        );
        assert_eq!(response.accepted_shard_count, 2);
        assert!(response.registered_at.is_some());
    }

    #[tokio::test]
    async fn register_dataset_under_unknown_run_returns_not_found() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));

        let error = service
            .register_dataset(Request::new(dataset_request(
                RunId {
                    value: "missing".to_string(),
                },
                "dataset-key",
                Some("dataset-explicit"),
                &[Some("shard-a")],
            )))
            .await
            .expect_err("unknown run should be missing");

        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn register_dataset_stores_shard_metadata() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run(&service).await;

        let response = service
            .register_dataset(Request::new(dataset_request(
                run_id.clone(),
                "dataset-key",
                Some("dataset-explicit"),
                &[Some("shard-a"), Some("shard-b")],
            )))
            .await
            .expect("register_dataset should succeed")
            .into_inner();
        let dataset_id = response
            .dataset_id
            .expect("register dataset response should include dataset_id");
        let record = service
            .state
            .get_dataset(&run_id, &dataset_id)
            .expect("registered dataset should be stored");

        assert_eq!(record.run_id, run_id.value);
        assert_eq!(record.dataset_id, "dataset-explicit");
        assert_eq!(record.name, "training-data");
        assert_eq!(record.dataset_uri, "s3://bucket/dataset");
        assert_eq!(record.format, "jsonl");
        assert_eq!(record.shard_count, 2);
        assert_eq!(record.dataset_version, "v1");
        assert_eq!(record.labels.get("split"), Some(&"train".to_string()));
        assert_eq!(record.shards.len(), 2);
        assert_eq!(record.shards[0].shard_id, "shard-a");
        assert_eq!(record.shards[0].dataset_id, "dataset-explicit");
        assert_eq!(
            record.shards[0].shard_uri,
            "s3://bucket/dataset/shard-0.jsonl"
        );
        assert_eq!(record.shards[0].byte_range_start, 0);
        assert_eq!(record.shards[0].byte_range_end, 99);
        assert_eq!(record.shards[0].size_bytes, 100);
        assert_eq!(record.shards[0].checksum, "checksum-0");
        assert_eq!(record.shards[0].checksum_algorithm, "sha256");
        assert_eq!(record.shards[1].shard_id, "shard-b");
    }

    #[tokio::test]
    async fn register_dataset_reuses_dataset_for_idempotency_key() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run(&service).await;

        let first = service
            .register_dataset(Request::new(dataset_request(
                run_id.clone(),
                "dataset-key",
                None,
                &[Some("shard-a")],
            )))
            .await
            .expect("first register_dataset should succeed")
            .into_inner();
        let second = service
            .register_dataset(Request::new(dataset_request(
                run_id,
                "dataset-key",
                Some("different-dataset"),
                &[Some("different-shard"), Some("another-shard")],
            )))
            .await
            .expect("second register_dataset should succeed")
            .into_inner();

        assert_eq!(first.dataset_id, second.dataset_id);
        assert_eq!(first.accepted_shard_count, second.accepted_shard_count);
        assert_eq!(first.registered_at, second.registered_at);
    }

    #[tokio::test]
    async fn empty_dataset_id_generates_dataset_id() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run(&service).await;

        let response = service
            .register_dataset(Request::new(dataset_request(
                run_id,
                "dataset-key",
                None,
                &[Some("shard-a")],
            )))
            .await
            .expect("register_dataset should succeed")
            .into_inner();

        assert_eq!(
            response.dataset_id,
            Some(DatasetId {
                value: "dataset-1".to_string(),
            })
        );
    }

    #[tokio::test]
    async fn empty_shard_id_generates_shard_ids() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run(&service).await;

        let response = service
            .register_dataset(Request::new(dataset_request(
                run_id.clone(),
                "dataset-key",
                Some("dataset-explicit"),
                &[None, None],
            )))
            .await
            .expect("register_dataset should succeed")
            .into_inner();
        let dataset_id = response
            .dataset_id
            .expect("register dataset response should include dataset_id");
        let record = service
            .state
            .get_dataset(&run_id, &dataset_id)
            .expect("registered dataset should be stored");

        assert_eq!(record.shards[0].shard_id, "shard-1");
        assert_eq!(record.shards[1].shard_id, "shard-2");
    }

    #[tokio::test]
    async fn register_worker_under_existing_run_succeeds() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run(&service).await;

        let response = service
            .register_worker(Request::new(worker_request(run_id, "worker-key")))
            .await
            .expect("register_worker should succeed")
            .into_inner();

        assert_eq!(
            response.worker_id,
            Some(WorkerId {
                value: "worker-1".to_string(),
            })
        );
        assert_eq!(response.state, WorkerState::Active as i32);
        assert!(response.lease_renewal_deadline.is_some());
        assert!(response.server_time.is_some());
    }

    #[tokio::test]
    async fn register_worker_under_unknown_run_returns_not_found() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));

        let error = service
            .register_worker(Request::new(worker_request(
                RunId {
                    value: "missing".to_string(),
                },
                "worker-key",
            )))
            .await
            .expect_err("unknown run should be missing");

        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn heartbeat_for_registered_worker_succeeds() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run(&service).await;
        let worker_id = service
            .register_worker(Request::new(worker_request(run_id.clone(), "worker-key")))
            .await
            .expect("register_worker should succeed")
            .into_inner()
            .worker_id
            .expect("register worker response should include worker_id");

        let response = service
            .heartbeat(Request::new(HeartbeatRequest {
                run_id: Some(run_id),
                worker_id: Some(worker_id),
                idempotency_key: "heartbeat-key".to_string(),
                client_time: None,
                active_shard_ids: Vec::new(),
            }))
            .await
            .expect("heartbeat should succeed")
            .into_inner();

        assert_eq!(response.state, WorkerState::Active as i32);
        assert!(response.next_heartbeat_deadline.is_some());
        assert!(response.server_time.is_some());
    }

    #[tokio::test]
    async fn heartbeat_for_unknown_worker_returns_not_found() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run(&service).await;

        let error = service
            .heartbeat(Request::new(HeartbeatRequest {
                run_id: Some(run_id),
                worker_id: Some(WorkerId {
                    value: "missing-worker".to_string(),
                }),
                idempotency_key: "heartbeat-key".to_string(),
                client_time: None,
                active_shard_ids: Vec::new(),
            }))
            .await
            .expect_err("unknown worker should be missing");

        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn worker_registration_updates_active_worker_count() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run(&service).await;

        service
            .register_worker(Request::new(worker_request(run_id.clone(), "worker-key")))
            .await
            .expect("register_worker should succeed");

        let status = service
            .get_run_status(Request::new(GetRunStatusRequest {
                run_id: Some(run_id),
            }))
            .await
            .expect("get_run_status should succeed")
            .into_inner();

        assert_eq!(status.active_worker_count, 1);
    }

    #[tokio::test]
    async fn get_run_status_marks_stale_worker_inactive_after_deadline() {
        let state = Arc::new(CoordinatorState::default());
        let service = CoordinatorServiceImpl::new(Arc::clone(&state));
        let run_id = create_run(&service).await;
        let worker_id = service
            .register_worker(Request::new(worker_request(run_id.clone(), "worker-key")))
            .await
            .expect("register_worker should succeed")
            .into_inner()
            .worker_id
            .expect("register worker response should include worker_id");
        assert!(state.set_worker_heartbeat_deadline_for_test(
            &run_id,
            &worker_id,
            std::time::SystemTime::UNIX_EPOCH,
        ));

        let status = service
            .get_run_status(Request::new(GetRunStatusRequest {
                run_id: Some(run_id.clone()),
            }))
            .await
            .expect("get_run_status should succeed")
            .into_inner();

        assert_eq!(status.active_worker_count, 0);
        let record = state.get_run(&run_id).expect("run should exist");
        assert_eq!(record.active_worker_count, 0);
    }

    #[tokio::test]
    async fn heartbeat_reactivates_stale_worker_for_status() {
        let state = Arc::new(CoordinatorState::default());
        let service = CoordinatorServiceImpl::new(Arc::clone(&state));
        let run_id = create_run(&service).await;
        let worker_id = service
            .register_worker(Request::new(worker_request(run_id.clone(), "worker-key")))
            .await
            .expect("register_worker should succeed")
            .into_inner()
            .worker_id
            .expect("register worker response should include worker_id");
        assert!(state.set_worker_heartbeat_deadline_for_test(
            &run_id,
            &worker_id,
            std::time::SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();

        let heartbeat = service
            .heartbeat(Request::new(HeartbeatRequest {
                run_id: Some(run_id.clone()),
                worker_id: Some(worker_id),
                idempotency_key: "heartbeat-key".to_string(),
                client_time: None,
                active_shard_ids: Vec::new(),
            }))
            .await
            .expect("heartbeat should succeed")
            .into_inner();
        let status = service
            .get_run_status(Request::new(GetRunStatusRequest {
                run_id: Some(run_id),
            }))
            .await
            .expect("get_run_status should succeed")
            .into_inner();

        assert_eq!(heartbeat.state, WorkerState::Active as i32);
        assert_eq!(status.active_worker_count, 1);
    }

    #[tokio::test]
    async fn register_worker_reuses_worker_for_idempotency_key() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run(&service).await;

        let first = service
            .register_worker(Request::new(worker_request(run_id.clone(), "worker-key")))
            .await
            .expect("first register_worker should succeed")
            .into_inner();
        let second = service
            .register_worker(Request::new(worker_request(run_id, "worker-key")))
            .await
            .expect("second register_worker should succeed")
            .into_inner();

        assert_eq!(first.worker_id, second.worker_id);
        assert_eq!(first.state, second.state);
        assert_eq!(first.lease_renewal_deadline, second.lease_renewal_deadline);
    }

    async fn register_dataset_for_assignment(
        service: &CoordinatorServiceImpl,
        run_id: RunId,
    ) -> DatasetId {
        service
            .register_dataset(Request::new(dataset_request(
                run_id,
                "dataset-key",
                Some("dataset-explicit"),
                &[
                    Some("shard-0"),
                    Some("shard-1"),
                    Some("shard-2"),
                    Some("shard-3"),
                    Some("shard-4"),
                ],
            )))
            .await
            .expect("register_dataset should succeed")
            .into_inner()
            .dataset_id
            .expect("register dataset response should include dataset_id")
    }

    async fn register_worker_for_assignment(
        service: &CoordinatorServiceImpl,
        run_id: RunId,
        rank: u32,
        idempotency_key: &str,
    ) -> WorkerId {
        service
            .register_worker(Request::new(worker_request_with_rank(
                run_id,
                idempotency_key,
                rank,
            )))
            .await
            .expect("register_worker should succeed")
            .into_inner()
            .worker_id
            .expect("register worker response should include worker_id")
    }

    #[tokio::test]
    async fn get_shard_assignment_under_unknown_run_returns_not_found() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));

        let error = service
            .get_shard_assignment(Request::new(GetShardAssignmentRequest {
                run_id: Some(RunId {
                    value: "missing-run".to_string(),
                }),
                worker_id: Some(WorkerId {
                    value: "worker-1".to_string(),
                }),
                dataset_id: Some(DatasetId {
                    value: "dataset-1".to_string(),
                }),
                idempotency_key: "assignment-key".to_string(),
            }))
            .await
            .expect_err("unknown run should be missing");

        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn get_shard_assignment_under_unknown_worker_returns_not_found() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run_with_world_size(&service, 2).await;
        let dataset_id = register_dataset_for_assignment(&service, run_id.clone()).await;

        let error = service
            .get_shard_assignment(Request::new(GetShardAssignmentRequest {
                run_id: Some(run_id),
                worker_id: Some(WorkerId {
                    value: "missing-worker".to_string(),
                }),
                dataset_id: Some(dataset_id),
                idempotency_key: "assignment-key".to_string(),
            }))
            .await
            .expect_err("unknown worker should be missing");

        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn get_shard_assignment_under_unknown_dataset_returns_not_found() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run_with_world_size(&service, 2).await;
        let worker_id =
            register_worker_for_assignment(&service, run_id.clone(), 0, "worker-key").await;

        let error = service
            .get_shard_assignment(Request::new(GetShardAssignmentRequest {
                run_id: Some(run_id),
                worker_id: Some(worker_id),
                dataset_id: Some(DatasetId {
                    value: "missing-dataset".to_string(),
                }),
                idempotency_key: "assignment-key".to_string(),
            }))
            .await
            .expect_err("unknown dataset should be missing");

        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn static_rank_assignment_returns_matching_shards() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run_with_world_size(&service, 2).await;
        let dataset_id = register_dataset_for_assignment(&service, run_id.clone()).await;
        let worker_id =
            register_worker_for_assignment(&service, run_id.clone(), 1, "worker-key").await;

        let response = service
            .get_shard_assignment(Request::new(GetShardAssignmentRequest {
                run_id: Some(run_id.clone()),
                worker_id: Some(worker_id.clone()),
                dataset_id: Some(dataset_id.clone()),
                idempotency_key: "assignment-key".to_string(),
            }))
            .await
            .expect("get_shard_assignment should succeed")
            .into_inner();

        let shard_ids = response
            .assignments
            .iter()
            .map(|assignment| assignment.shard_id.as_ref().unwrap().value.as_str())
            .collect::<Vec<_>>();
        assert_eq!(shard_ids, vec!["shard-1", "shard-3"]);
        assert_eq!(
            response.assignment_policy,
            AssignmentPolicy::ReassignQueue as i32
        );
        assert!(response.server_time.is_some());
        for assignment in response.assignments {
            assert_eq!(assignment.run_id, Some(run_id.clone()));
            assert_eq!(assignment.dataset_id, Some(dataset_id.clone()));
            assert_eq!(assignment.worker_id, Some(worker_id.clone()));
            assert_eq!(assignment.lease_epoch, 1);
            assert_eq!(assignment.lease_state, ShardLeaseState::Leased as i32);
            assert!(assignment.lease_expires_at.is_some());
            assert_eq!(assignment.progress_offset, 0);
        }
    }

    #[tokio::test]
    async fn rank_zero_and_rank_one_get_different_deterministic_shard_subsets() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run_with_world_size(&service, 2).await;
        let dataset_id = register_dataset_for_assignment(&service, run_id.clone()).await;
        let worker_zero =
            register_worker_for_assignment(&service, run_id.clone(), 0, "worker-zero").await;
        let worker_one =
            register_worker_for_assignment(&service, run_id.clone(), 1, "worker-one").await;

        let rank_zero = service
            .get_shard_assignment(Request::new(GetShardAssignmentRequest {
                run_id: Some(run_id.clone()),
                worker_id: Some(worker_zero),
                dataset_id: Some(dataset_id.clone()),
                idempotency_key: "assignment-zero".to_string(),
            }))
            .await
            .expect("rank zero assignment should succeed")
            .into_inner();
        let rank_one = service
            .get_shard_assignment(Request::new(GetShardAssignmentRequest {
                run_id: Some(run_id),
                worker_id: Some(worker_one),
                dataset_id: Some(dataset_id),
                idempotency_key: "assignment-one".to_string(),
            }))
            .await
            .expect("rank one assignment should succeed")
            .into_inner();

        let rank_zero_shards = rank_zero
            .assignments
            .iter()
            .map(|assignment| assignment.shard_id.as_ref().unwrap().value.as_str())
            .collect::<Vec<_>>();
        let rank_one_shards = rank_one
            .assignments
            .iter()
            .map(|assignment| assignment.shard_id.as_ref().unwrap().value.as_str())
            .collect::<Vec<_>>();

        assert_eq!(rank_zero_shards, vec!["shard-0", "shard-2", "shard-4"]);
        assert_eq!(rank_one_shards, vec!["shard-1", "shard-3"]);
        assert_ne!(rank_zero_shards, rank_one_shards);
    }

    #[tokio::test]
    async fn get_shard_assignment_includes_shard_uri_from_metadata() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run_with_world_size(&service, 2).await;
        let dataset_id = register_dataset_for_assignment(&service, run_id.clone()).await;
        let worker_id =
            register_worker_for_assignment(&service, run_id.clone(), 0, "worker-key").await;

        let response = service
            .get_shard_assignment(Request::new(GetShardAssignmentRequest {
                run_id: Some(run_id),
                worker_id: Some(worker_id),
                dataset_id: Some(dataset_id),
                idempotency_key: "assignment-key".to_string(),
            }))
            .await
            .expect("get_shard_assignment should succeed")
            .into_inner();

        let shard_uris = response
            .assignments
            .iter()
            .map(|assignment| assignment.shard_uri.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            shard_uris,
            vec![
                "s3://bucket/dataset/shard-0.jsonl",
                "s3://bucket/dataset/shard-2.jsonl",
                "s3://bucket/dataset/shard-4.jsonl",
            ]
        );
    }

    fn renew_request(
        run_id: RunId,
        worker_id: WorkerId,
        shard_id: &str,
        lease_epoch: u64,
    ) -> RenewShardLeaseRequest {
        RenewShardLeaseRequest {
            run_id: Some(run_id),
            worker_id: Some(worker_id),
            shard_id: Some(ShardId {
                value: shard_id.to_string(),
            }),
            lease_epoch,
            idempotency_key: "renew-key".to_string(),
        }
    }

    #[tokio::test]
    async fn renew_shard_lease_under_unknown_run_returns_not_found() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));

        let error = service
            .renew_shard_lease(Request::new(renew_request(
                RunId {
                    value: "missing-run".to_string(),
                },
                WorkerId {
                    value: "worker-1".to_string(),
                },
                "shard-0",
                1,
            )))
            .await
            .expect_err("unknown run should be missing");

        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn renew_shard_lease_under_unknown_worker_returns_not_found() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run_with_world_size(&service, 2).await;
        register_dataset_for_assignment(&service, run_id.clone()).await;

        let error = service
            .renew_shard_lease(Request::new(renew_request(
                run_id,
                WorkerId {
                    value: "missing-worker".to_string(),
                },
                "shard-0",
                1,
            )))
            .await
            .expect_err("unknown worker should be missing");

        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn renew_shard_lease_under_unknown_shard_returns_not_found() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run_with_world_size(&service, 2).await;
        register_dataset_for_assignment(&service, run_id.clone()).await;
        let worker_id =
            register_worker_for_assignment(&service, run_id.clone(), 0, "worker-key").await;

        let error = service
            .renew_shard_lease(Request::new(renew_request(
                run_id,
                worker_id,
                "missing-shard",
                1,
            )))
            .await
            .expect_err("unknown shard should be missing");

        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn begin_checkpoint_succeeds_for_existing_run_and_worker() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run(&service).await;
        let worker_id = service
            .register_worker(Request::new(worker_request(run_id.clone(), "worker-key")))
            .await
            .expect("register worker should succeed")
            .into_inner()
            .worker_id
            .expect("worker id should be set");

        let response = service
            .begin_checkpoint(Request::new(BeginCheckpointRequest {
                run_id: Some(run_id),
                worker_id: Some(worker_id),
                checkpoint_id: Some(CheckpointId {
                    value: "ckpt-explicit".to_string(),
                }),
                step: 9,
                idempotency_key: "cp-key-1".to_string(),
            }))
            .await
            .expect("begin checkpoint should succeed")
            .into_inner();
        assert_eq!(
            response.checkpoint_id,
            Some(CheckpointId {
                value: "ckpt-explicit".to_string()
            })
        );
        assert_eq!(response.state, CheckpointState::Initiated as i32);
        assert!(response.created_at.is_some());
        assert!(response.artifact_specs.is_empty());
    }

    #[tokio::test]
    async fn begin_checkpoint_returns_not_found_for_unknown_run_or_worker() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_err = service
            .begin_checkpoint(Request::new(BeginCheckpointRequest {
                run_id: Some(RunId {
                    value: "missing".to_string(),
                }),
                worker_id: Some(WorkerId {
                    value: "worker-1".to_string(),
                }),
                checkpoint_id: None,
                step: 1,
                idempotency_key: String::new(),
            }))
            .await
            .expect_err("unknown run should fail");
        assert_eq!(run_err.code(), tonic::Code::NotFound);

        let run_id = create_run(&service).await;
        let worker_err = service
            .begin_checkpoint(Request::new(BeginCheckpointRequest {
                run_id: Some(run_id),
                worker_id: Some(WorkerId {
                    value: "missing-worker".to_string(),
                }),
                checkpoint_id: None,
                step: 1,
                idempotency_key: String::new(),
            }))
            .await
            .expect_err("unknown worker should fail");
        assert_eq!(worker_err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn begin_checkpoint_generates_id_and_is_idempotent_by_key() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run(&service).await;
        let worker_id = service
            .register_worker(Request::new(worker_request(run_id.clone(), "worker-key")))
            .await
            .expect("register worker should succeed")
            .into_inner()
            .worker_id
            .expect("worker id should be set");

        let first = service
            .begin_checkpoint(Request::new(BeginCheckpointRequest {
                run_id: Some(run_id.clone()),
                worker_id: Some(worker_id.clone()),
                checkpoint_id: None,
                step: 12,
                idempotency_key: "cp-key-1".to_string(),
            }))
            .await
            .expect("first begin should succeed")
            .into_inner();
        let second = service
            .begin_checkpoint(Request::new(BeginCheckpointRequest {
                run_id: Some(run_id),
                worker_id: Some(worker_id),
                checkpoint_id: Some(CheckpointId {
                    value: "different".to_string(),
                }),
                step: 999,
                idempotency_key: "cp-key-1".to_string(),
            }))
            .await
            .expect("idempotent begin should succeed")
            .into_inner();
        assert_eq!(first.checkpoint_id, second.checkpoint_id);
        assert_eq!(first.state, CheckpointState::Initiated as i32);
        assert_eq!(first.checkpoint_id.unwrap().value, "checkpoint-1");
    }

    fn artifact_request(
        run_id: RunId,
        worker_id: WorkerId,
        checkpoint_id: &str,
        artifact_name: &str,
        artifact_uri: &str,
        idempotency_key: &str,
    ) -> ReportCheckpointArtifactRequest {
        ReportCheckpointArtifactRequest {
            run_id: Some(run_id),
            worker_id: Some(worker_id),
            checkpoint_id: Some(CheckpointId {
                value: checkpoint_id.to_string(),
            }),
            artifact_report: Some(ArtifactReport {
                artifact_name: artifact_name.to_string(),
                artifact_uri: artifact_uri.to_string(),
                size_bytes: 10,
                checksum: "sum".to_string(),
                checksum_algorithm: "sha256".to_string(),
                uploaded_at: None,
            }),
            idempotency_key: idempotency_key.to_string(),
        }
    }

    #[tokio::test]
    async fn report_checkpoint_artifact_succeeds_and_supports_idempotency() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run(&service).await;
        let worker_id = service
            .register_worker(Request::new(worker_request(run_id.clone(), "worker-key")))
            .await
            .unwrap()
            .into_inner()
            .worker_id
            .unwrap();
        service
            .begin_checkpoint(Request::new(BeginCheckpointRequest {
                run_id: Some(run_id.clone()),
                worker_id: Some(worker_id.clone()),
                checkpoint_id: Some(CheckpointId {
                    value: "ckpt-1".to_string(),
                }),
                step: 1,
                idempotency_key: "cp-key".to_string(),
            }))
            .await
            .unwrap();

        let first = service
            .report_checkpoint_artifact(Request::new(artifact_request(
                run_id.clone(),
                worker_id.clone(),
                "ckpt-1",
                "a.bin",
                "s3://bucket/a.bin",
                "artifact-key",
            )))
            .await
            .unwrap()
            .into_inner();
        assert!(first.accepted);
        assert_eq!(
            first.checkpoint_state,
            CheckpointState::UploadInProgress as i32
        );

        let second = service
            .report_checkpoint_artifact(Request::new(artifact_request(
                run_id.clone(),
                worker_id.clone(),
                "ckpt-1",
                "a.bin",
                "s3://bucket/a.bin",
                "artifact-key",
            )))
            .await
            .unwrap()
            .into_inner();
        assert!(second.accepted);

        let third = service
            .report_checkpoint_artifact(Request::new(artifact_request(
                run_id.clone(),
                worker_id.clone(),
                "ckpt-1",
                "b.bin",
                "s3://bucket/b.bin",
                "artifact-key-2",
            )))
            .await
            .unwrap();
        assert!(third.get_ref().accepted);
    }

    #[tokio::test]
    async fn report_checkpoint_artifact_unknown_entities_return_not_found() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let err = service
            .report_checkpoint_artifact(Request::new(artifact_request(
                RunId {
                    value: "missing".to_string(),
                },
                WorkerId {
                    value: "worker-1".to_string(),
                },
                "ckpt-1",
                "a.bin",
                "s3://bucket/a.bin",
                "",
            )))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn commit_checkpoint_succeeds_and_is_idempotent() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run(&service).await;
        let worker_id = service
            .register_worker(Request::new(worker_request(run_id.clone(), "worker-key")))
            .await
            .unwrap()
            .into_inner()
            .worker_id
            .unwrap();
        service
            .begin_checkpoint(Request::new(BeginCheckpointRequest {
                run_id: Some(run_id.clone()),
                worker_id: Some(worker_id.clone()),
                checkpoint_id: Some(CheckpointId {
                    value: "ckpt-commit".to_string(),
                }),
                step: 1,
                idempotency_key: "cp-key".to_string(),
            }))
            .await
            .unwrap();
        service
            .report_checkpoint_artifact(Request::new(artifact_request(
                run_id.clone(),
                worker_id,
                "ckpt-commit",
                "a.bin",
                "s3://bucket/a.bin",
                "artifact-key",
            )))
            .await
            .unwrap();
        let first = service
            .commit_checkpoint(Request::new(CommitCheckpointRequest {
                run_id: Some(run_id.clone()),
                checkpoint_id: Some(CheckpointId {
                    value: "ckpt-commit".to_string(),
                }),
                idempotency_key: "commit-1".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(first.state, CheckpointState::Committed as i32);
        assert!(first.committed_at.is_some());
        let second = service
            .commit_checkpoint(Request::new(CommitCheckpointRequest {
                run_id: Some(run_id.clone()),
                checkpoint_id: Some(CheckpointId {
                    value: "ckpt-commit".to_string(),
                }),
                idempotency_key: "commit-2".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(second.state, CheckpointState::Committed as i32);

        let status = service
            .get_run_status(Request::new(GetRunStatusRequest {
                run_id: Some(run_id),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            status.latest_checkpoint_id,
            Some(CheckpointId {
                value: "ckpt-commit".to_string()
            })
        );
        assert_eq!(
            status.latest_checkpoint_state,
            CheckpointState::Committed as i32
        );
    }

    #[tokio::test]
    async fn commit_checkpoint_returns_expected_errors() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let missing_run = service
            .commit_checkpoint(Request::new(CommitCheckpointRequest {
                run_id: Some(RunId {
                    value: "missing".to_string(),
                }),
                checkpoint_id: Some(CheckpointId {
                    value: "ckpt-1".to_string(),
                }),
                idempotency_key: String::new(),
            }))
            .await
            .unwrap_err();
        assert_eq!(missing_run.code(), tonic::Code::NotFound);

        let run_id = create_run(&service).await;
        let missing_checkpoint = service
            .commit_checkpoint(Request::new(CommitCheckpointRequest {
                run_id: Some(run_id.clone()),
                checkpoint_id: Some(CheckpointId {
                    value: "missing".to_string(),
                }),
                idempotency_key: String::new(),
            }))
            .await
            .unwrap_err();
        assert_eq!(missing_checkpoint.code(), tonic::Code::NotFound);

        let worker_id = service
            .register_worker(Request::new(worker_request(run_id.clone(), "worker-key")))
            .await
            .unwrap()
            .into_inner()
            .worker_id
            .unwrap();
        service
            .begin_checkpoint(Request::new(BeginCheckpointRequest {
                run_id: Some(run_id.clone()),
                worker_id: Some(worker_id),
                checkpoint_id: Some(CheckpointId {
                    value: "ckpt-empty".to_string(),
                }),
                step: 1,
                idempotency_key: "begin-empty".to_string(),
            }))
            .await
            .unwrap();
        let no_artifacts = service
            .commit_checkpoint(Request::new(CommitCheckpointRequest {
                run_id: Some(run_id),
                checkpoint_id: Some(CheckpointId {
                    value: "ckpt-empty".to_string(),
                }),
                idempotency_key: String::new(),
            }))
            .await
            .unwrap_err();
        assert_eq!(no_artifacts.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn get_latest_checkpoint_returns_not_found_for_unknown_run() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let error = service
            .get_latest_checkpoint(Request::new(GetLatestCheckpointRequest {
                run_id: Some(RunId {
                    value: "missing".to_string(),
                }),
            }))
            .await
            .expect_err("unknown run should fail");
        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn get_latest_checkpoint_returns_not_found_without_commit() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run(&service).await;
        let error = service
            .get_latest_checkpoint(Request::new(GetLatestCheckpointRequest {
                run_id: Some(run_id.clone()),
            }))
            .await
            .expect_err("no committed checkpoint should fail");
        assert_eq!(error.code(), tonic::Code::NotFound);

        let worker_id = service
            .register_worker(Request::new(worker_request(run_id.clone(), "worker-key")))
            .await
            .unwrap()
            .into_inner()
            .worker_id
            .unwrap();
        service
            .begin_checkpoint(Request::new(BeginCheckpointRequest {
                run_id: Some(run_id.clone()),
                worker_id: Some(worker_id),
                checkpoint_id: Some(CheckpointId {
                    value: "pending".to_string(),
                }),
                step: 1,
                idempotency_key: "begin".to_string(),
            }))
            .await
            .unwrap();
        let error = service
            .get_latest_checkpoint(Request::new(GetLatestCheckpointRequest {
                run_id: Some(run_id),
            }))
            .await
            .expect_err("pending checkpoint should not be returned");
        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn get_latest_checkpoint_returns_committed_manifest_and_latest_order() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run(&service).await;
        let worker_id = service
            .register_worker(Request::new(worker_request(run_id.clone(), "worker-key")))
            .await
            .unwrap()
            .into_inner()
            .worker_id
            .unwrap();

        for (ckpt, step, artifact) in [("ckpt-1", 1, "a.bin"), ("ckpt-2", 2, "b.bin")] {
            service
                .begin_checkpoint(Request::new(BeginCheckpointRequest {
                    run_id: Some(run_id.clone()),
                    worker_id: Some(worker_id.clone()),
                    checkpoint_id: Some(CheckpointId {
                        value: ckpt.to_string(),
                    }),
                    step,
                    idempotency_key: format!("begin-{ckpt}"),
                }))
                .await
                .unwrap();
            service
                .report_checkpoint_artifact(Request::new(artifact_request(
                    run_id.clone(),
                    worker_id.clone(),
                    ckpt,
                    artifact,
                    &format!("s3://bucket/{artifact}"),
                    &format!("artifact-{ckpt}"),
                )))
                .await
                .unwrap();
            service
                .commit_checkpoint(Request::new(CommitCheckpointRequest {
                    run_id: Some(run_id.clone()),
                    checkpoint_id: Some(CheckpointId {
                        value: ckpt.to_string(),
                    }),
                    idempotency_key: format!("commit-{ckpt}"),
                }))
                .await
                .unwrap();
        }

        let response = service
            .get_latest_checkpoint(Request::new(GetLatestCheckpointRequest {
                run_id: Some(run_id),
            }))
            .await
            .unwrap()
            .into_inner();
        let manifest = response.manifest.expect("manifest required");
        assert_eq!(manifest.checkpoint_id.unwrap().value, "ckpt-2");
        assert_eq!(manifest.state, CheckpointState::Committed as i32);
        assert_eq!(manifest.artifacts.len(), 1);
        assert_eq!(manifest.artifacts[0].artifact_name, "b.bin");
    }

    #[tokio::test]
    async fn renew_shard_lease_for_deterministic_owner_succeeds() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run_with_world_size(&service, 2).await;
        register_dataset_for_assignment(&service, run_id.clone()).await;
        let worker_id =
            register_worker_for_assignment(&service, run_id.clone(), 0, "worker-key").await;

        let response = service
            .renew_shard_lease(Request::new(renew_request(run_id, worker_id, "shard-0", 1)))
            .await
            .expect("renew_shard_lease should succeed")
            .into_inner();

        assert!(response.accepted);
        assert_eq!(response.lease_state, ShardLeaseState::Leased as i32);
        assert!(response.lease_expires_at.is_some());
        assert!(response.server_time.is_some());
    }

    #[tokio::test]
    async fn renew_shard_lease_for_non_owner_returns_failed_precondition() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run_with_world_size(&service, 2).await;
        register_dataset_for_assignment(&service, run_id.clone()).await;
        let worker_id =
            register_worker_for_assignment(&service, run_id.clone(), 1, "worker-key").await;

        let error = service
            .renew_shard_lease(Request::new(renew_request(run_id, worker_id, "shard-0", 1)))
            .await
            .expect_err("non-owner should not renew shard lease");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn renew_shard_lease_with_wrong_lease_epoch_returns_failed_precondition() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run_with_world_size(&service, 2).await;
        register_dataset_for_assignment(&service, run_id.clone()).await;
        let worker_id =
            register_worker_for_assignment(&service, run_id.clone(), 0, "worker-key").await;

        let error = service
            .renew_shard_lease(Request::new(renew_request(run_id, worker_id, "shard-0", 0)))
            .await
            .expect_err("wrong lease epoch should not renew shard lease");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }

    fn progress_request(
        run_id: RunId,
        worker_id: WorkerId,
        dataset_id: DatasetId,
        shard_id: &str,
        lease_epoch: u64,
        progress_offset: i64,
        is_complete: bool,
    ) -> ReportShardProgressRequest {
        ReportShardProgressRequest {
            run_id: Some(run_id),
            worker_id: Some(worker_id),
            dataset_id: Some(dataset_id),
            shard_id: Some(ShardId {
                value: shard_id.to_string(),
            }),
            lease_epoch,
            progress_offset,
            is_complete,
            idempotency_key: "progress-key".to_string(),
        }
    }

    #[tokio::test]
    async fn report_shard_progress_under_unknown_run_returns_not_found() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));

        let error = service
            .report_shard_progress(Request::new(progress_request(
                RunId {
                    value: "missing-run".to_string(),
                },
                WorkerId {
                    value: "worker-1".to_string(),
                },
                DatasetId {
                    value: "dataset-explicit".to_string(),
                },
                "shard-0",
                1,
                10,
                false,
            )))
            .await
            .expect_err("unknown run should be missing");

        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn report_shard_progress_under_unknown_worker_returns_not_found() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run_with_world_size(&service, 2).await;
        let dataset_id = register_dataset_for_assignment(&service, run_id.clone()).await;

        let error = service
            .report_shard_progress(Request::new(progress_request(
                run_id,
                WorkerId {
                    value: "missing-worker".to_string(),
                },
                dataset_id,
                "shard-0",
                1,
                10,
                false,
            )))
            .await
            .expect_err("unknown worker should be missing");

        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn report_shard_progress_under_unknown_dataset_returns_not_found() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run_with_world_size(&service, 2).await;
        let worker_id =
            register_worker_for_assignment(&service, run_id.clone(), 0, "worker-key").await;

        let error = service
            .report_shard_progress(Request::new(progress_request(
                run_id,
                worker_id,
                DatasetId {
                    value: "missing-dataset".to_string(),
                },
                "shard-0",
                1,
                10,
                false,
            )))
            .await
            .expect_err("unknown dataset should be missing");

        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn report_shard_progress_under_unknown_shard_returns_not_found() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run_with_world_size(&service, 2).await;
        let dataset_id = register_dataset_for_assignment(&service, run_id.clone()).await;
        let worker_id =
            register_worker_for_assignment(&service, run_id.clone(), 0, "worker-key").await;

        let error = service
            .report_shard_progress(Request::new(progress_request(
                run_id,
                worker_id,
                dataset_id,
                "missing-shard",
                1,
                10,
                false,
            )))
            .await
            .expect_err("unknown shard should be missing");

        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn report_shard_progress_for_deterministic_owner_succeeds() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run_with_world_size(&service, 2).await;
        let dataset_id = register_dataset_for_assignment(&service, run_id.clone()).await;
        let worker_id =
            register_worker_for_assignment(&service, run_id.clone(), 0, "worker-key").await;

        let response = service
            .report_shard_progress(Request::new(progress_request(
                run_id.clone(),
                worker_id.clone(),
                dataset_id.clone(),
                "shard-0",
                1,
                123,
                true,
            )))
            .await
            .expect("report_shard_progress should succeed")
            .into_inner();

        assert!(response.accepted);
        assert_eq!(response.stored_progress_offset, 123);
        assert_eq!(response.lease_state, ShardLeaseState::Leased as i32);

        let stored = service
            .state
            .get_shard_progress(
                &run_id,
                &dataset_id,
                &ShardId {
                    value: "shard-0".to_string(),
                },
            )
            .expect("accepted progress should be stored");
        assert_eq!(stored.worker_id, worker_id.value);
        assert_eq!(stored.lease_epoch, 1);
        assert_eq!(stored.progress_offset, 123);
        assert!(stored.is_complete);
    }

    #[tokio::test]
    async fn report_shard_progress_for_non_owner_returns_failed_precondition() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run_with_world_size(&service, 2).await;
        let dataset_id = register_dataset_for_assignment(&service, run_id.clone()).await;
        let worker_id =
            register_worker_for_assignment(&service, run_id.clone(), 1, "worker-key").await;

        let error = service
            .report_shard_progress(Request::new(progress_request(
                run_id, worker_id, dataset_id, "shard-0", 1, 10, false,
            )))
            .await
            .expect_err("non-owner should not report shard progress");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn report_shard_progress_with_wrong_lease_epoch_returns_failed_precondition() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run_with_world_size(&service, 2).await;
        let dataset_id = register_dataset_for_assignment(&service, run_id.clone()).await;
        let worker_id =
            register_worker_for_assignment(&service, run_id.clone(), 0, "worker-key").await;

        let error = service
            .report_shard_progress(Request::new(progress_request(
                run_id, worker_id, dataset_id, "shard-0", 2, 10, false,
            )))
            .await
            .expect_err("wrong lease epoch should not report shard progress");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn get_run_status_returns_not_found_for_unknown_run() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));

        let error = service
            .get_run_status(Request::new(GetRunStatusRequest {
                run_id: Some(RunId {
                    value: "missing".to_string(),
                }),
            }))
            .await
            .expect_err("unknown run should be missing");

        assert_eq!(error.code(), tonic::Code::NotFound);
    }
}
