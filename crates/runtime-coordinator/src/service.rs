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

    fn worker_request(run_id: RunId, idempotency_key: &str) -> RegisterWorkerRequest {
        RegisterWorkerRequest {
            idempotency_key: idempotency_key.to_string(),
            run_id: Some(run_id),
            worker_id: None,
            rank: 3,
            host: "worker-host".to_string(),
            sdk_version: "sdk-0.1.0".to_string(),
            protocol_version: "runtime.v1".to_string(),
        }
    }

    async fn create_run(service: &CoordinatorServiceImpl) -> RunId {
        service
            .register_run(Request::new(register_request("service-key")))
            .await
            .expect("register_run should succeed")
            .into_inner()
            .run_id
            .expect("register response should include run_id")
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
