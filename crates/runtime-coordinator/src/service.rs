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
        _request: Request<RegisterRunRequest>,
    ) -> Result<Response<RegisterRunResponse>, Status> {
        let _ = &self.state;
        Err(Status::unimplemented("RegisterRun is not implemented yet"))
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

        let response = GetRunStatusResponse {
            run_id: request.run_id,
            run_version: String::new(),
            assignment_policy: 0,
            storage_backend_kind: 0,
            latest_checkpoint_id: None,
            latest_checkpoint_state: 0,
            active_worker_count: 0,
            server_time: None,
        };

        Ok(Response::new(response))
    }
}
