#![forbid(unsafe_code)]

use std::sync::Arc;

use runtime_proto::runtime::v1::coordinator_service_server::CoordinatorService;
use runtime_proto::runtime::v1::*;
use tonic::{Request, Response, Status};

use crate::state::CoordinatorState;

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
        _request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        Err(Status::unimplemented(
            "RegisterWorker is not implemented yet",
        ))
    }

    async fn heartbeat(
        &self,
        _request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        Err(Status::unimplemented("Heartbeat is not implemented yet"))
    }

    async fn register_dataset(
        &self,
        _request: Request<RegisterDatasetRequest>,
    ) -> Result<Response<RegisterDatasetResponse>, Status> {
        Err(Status::unimplemented(
            "RegisterDataset is not implemented yet",
        ))
    }

    async fn get_shard_assignment(
        &self,
        _request: Request<GetShardAssignmentRequest>,
    ) -> Result<Response<GetShardAssignmentResponse>, Status> {
        Err(Status::unimplemented(
            "GetShardAssignment is not implemented yet",
        ))
    }

    async fn renew_shard_lease(
        &self,
        _request: Request<RenewShardLeaseRequest>,
    ) -> Result<Response<RenewShardLeaseResponse>, Status> {
        Err(Status::unimplemented(
            "RenewShardLease is not implemented yet",
        ))
    }

    async fn report_shard_progress(
        &self,
        _request: Request<ReportShardProgressRequest>,
    ) -> Result<Response<ReportShardProgressResponse>, Status> {
        Err(Status::unimplemented(
            "ReportShardProgress is not implemented yet",
        ))
    }

    async fn begin_checkpoint(
        &self,
        _request: Request<BeginCheckpointRequest>,
    ) -> Result<Response<BeginCheckpointResponse>, Status> {
        Err(Status::unimplemented(
            "BeginCheckpoint is not implemented yet",
        ))
    }

    async fn report_checkpoint_artifact(
        &self,
        _request: Request<ReportCheckpointArtifactRequest>,
    ) -> Result<Response<ReportCheckpointArtifactResponse>, Status> {
        Err(Status::unimplemented(
            "ReportCheckpointArtifact is not implemented yet",
        ))
    }

    async fn commit_checkpoint(
        &self,
        _request: Request<CommitCheckpointRequest>,
    ) -> Result<Response<CommitCheckpointResponse>, Status> {
        Err(Status::unimplemented(
            "CommitCheckpoint is not implemented yet",
        ))
    }

    async fn get_latest_checkpoint(
        &self,
        _request: Request<GetLatestCheckpointRequest>,
    ) -> Result<Response<GetLatestCheckpointResponse>, Status> {
        Err(Status::unimplemented(
            "GetLatestCheckpoint is not implemented yet",
        ))
    }

    async fn get_run_status(
        &self,
        request: Request<GetRunStatusRequest>,
    ) -> Result<Response<GetRunStatusResponse>, Status> {
        let request = request.into_inner();
        let run_id = request
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use runtime_proto::runtime::v1::{AssignmentPolicy, StorageBackendKind};

    fn register_request(idempotency_key: &str) -> RegisterRunRequest {
        RegisterRunRequest {
            idempotency_key: idempotency_key.to_string(),
            run_name: "service-test-run".to_string(),
            assignment_policy: AssignmentPolicy::ReassignQueue as i32,
            storage_backend_kind: StorageBackendKind::S3Compatible as i32,
            world_size: 8,
            run_version: "2026-05-18".to_string(),
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
