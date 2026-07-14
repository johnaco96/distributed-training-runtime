#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use runtime_proto::protocol_version;
use runtime_proto::runtime::v1::coordinator_service_server::CoordinatorService;
use runtime_proto::runtime::v1::*;
use runtime_storage::{S3Storage, S3StorageConfig};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info, warn};

use crate::audit::{AuditEvent, AuditEventKind, AuditLogger, AuditOutcome};
use crate::auth::{AuthorizationError, Authorizer};
use crate::state::{
    BeginCheckpointError, CommitCheckpointError, CoordinatorState, GetLatestCheckpointError,
    GetShardAssignmentsError, RenewShardLeaseError, ReportCheckpointArtifactError,
    ReportShardProgressError, RunActionError,
};
use crate::{RuntimeMetrics, RuntimeMetricsSnapshot};

#[derive(Clone)]
pub struct CoordinatorServiceImpl {
    state: Arc<CoordinatorState>,
    metrics: Arc<RuntimeMetrics>,
    authorizer: Authorizer,
    audit: AuditLogger,
    storage_root: Option<Arc<PathBuf>>,
    s3_backend: Option<Arc<S3Storage>>,
    s3_bucket: Option<String>,
    /// When `true`, every streamed checkpoint artifact is re-read from disk
    /// immediately after writing and its SHA-256 is verified against the
    /// inline checksum computed during the stream.  Detects partial writes,
    /// silent truncation, and filesystem corruption.  Default: `true`.
    verify_checkpoint_artifacts: bool,
}

impl CoordinatorServiceImpl {
    pub fn new(state: Arc<CoordinatorState>) -> Self {
        Self::with_metrics_authorizer_and_audit(
            state,
            Arc::new(RuntimeMetrics::default()),
            Authorizer::new(crate::auth::AuthMode::Disabled),
            AuditLogger::tracing(),
        )
    }

    pub fn with_metrics_authorizer_and_audit(
        state: Arc<CoordinatorState>,
        metrics: Arc<RuntimeMetrics>,
        authorizer: Authorizer,
        audit: AuditLogger,
    ) -> Self {
        Self {
            state,
            metrics,
            authorizer,
            audit,
            storage_root: None,
            s3_backend: None,
            s3_bucket: None,
            verify_checkpoint_artifacts: true,
        }
    }

    /// Enable or disable post-write checksum verification for checkpoint artifacts.
    ///
    /// When enabled (the default), every artifact written by
    /// `stream_checkpoint_artifact` is immediately re-read from disk and its
    /// SHA-256 is compared to the inline checksum computed during streaming.
    /// Disable only when I/O overhead is measurably unacceptable and storage
    /// reliability is otherwise guaranteed.
    pub fn with_artifact_verification(mut self, enabled: bool) -> Self {
        self.verify_checkpoint_artifacts = enabled;
        self
    }

    pub fn with_storage_root(mut self, root: PathBuf) -> Self {
        self.storage_root = Some(Arc::new(root));
        self
    }

    pub async fn with_s3_backend(mut self, config: S3StorageConfig) -> Result<Self, String> {
        let bucket = config.bucket.clone();
        // new_async builds the client inside the current Tokio runtime so the
        // Hyper connection pool and the runtime are always co-located — avoids
        // the nested-runtime panic that S3Storage::new() causes when called
        // from within #[tokio::main].
        let backend = S3Storage::new_async(config)
            .await
            .map_err(|e| format!("S3 backend init failed: {e}"))?;
        self.s3_backend = Some(Arc::new(backend));
        self.s3_bucket = Some(bucket);
        Ok(self)
    }

    pub fn metrics_snapshot(&self) -> RuntimeMetricsSnapshot {
        self.metrics.snapshot()
    }

    fn auth_context<T>(
        &self,
        request: &Request<T>,
        request_name: &'static str,
    ) -> Result<Option<crate::auth::AuthContext>, Status> {
        // The verified mTLS leaf certificate, when present, feeds SPIFFE auth.
        let peer_leaf: Option<Vec<u8>> = request
            .peer_certs()
            .and_then(|certs| certs.first().map(|c| c.as_ref().to_vec()));
        match self
            .authorizer
            .authenticate(request.metadata(), peer_leaf.as_deref())
        {
            Ok(ctx) => {
                if let Some(c) = &ctx {
                    self.audit.emit(AuditEvent {
                        kind: Some(AuditEventKind::AuthMetadataAccepted),
                        outcome: Some(AuditOutcome::Success),
                        subject: Some(c.subject.clone()),
                        role: Some(c.role_label().to_string()),
                        worker_id: c.worker_id.clone(),
                        run_id: None,
                        dataset_id: None,
                        checkpoint_id: None,
                        reason: Some(format!("source={}", c.source.as_str())),
                        request_name: Some(request_name.to_string()),
                    });
                }
                Ok(ctx)
            }
            Err(e) => {
                self.audit.emit(AuditEvent {
                    kind: Some(AuditEventKind::AuthMetadataRejected),
                    outcome: Some(AuditOutcome::Rejected),
                    subject: None,
                    role: None,
                    worker_id: None,
                    run_id: None,
                    dataset_id: None,
                    checkpoint_id: None,
                    reason: Some(e.as_str().to_string()),
                    request_name: Some(request_name.to_string()),
                });
                self.metrics.inc_auth_denied_total();
                Err(e.to_status())
            }
        }
    }

    /// Deny with an audited `AuthorizationDenied` event and a denied-metric bump.
    fn deny(
        &self,
        ctx: &crate::auth::AuthContext,
        reason: &str,
        request_name: &'static str,
    ) -> Status {
        self.audit.emit(AuditEvent {
            kind: Some(AuditEventKind::AuthorizationDenied),
            outcome: Some(AuditOutcome::Rejected),
            subject: Some(ctx.subject.clone()),
            role: Some(ctx.role_label().to_string()),
            worker_id: ctx.worker_id.clone(),
            run_id: None,
            dataset_id: None,
            checkpoint_id: None,
            reason: Some(reason.to_string()),
            request_name: Some(request_name.to_string()),
        });
        self.metrics.inc_auth_denied_total();
        AuthorizationError::PermissionDenied.to_status()
    }

    fn allow(&self) {
        self.audit.emit(AuditEvent::new(
            AuditEventKind::AuthorizationAllowed,
            AuditOutcome::Success,
        ));
        self.metrics.inc_auth_allowed_total();
    }

    /// Require a specific [`Permission`]; used for management/read RPCs.
    fn require_permission<T>(
        &self,
        request: &Request<T>,
        permission: crate::auth::Permission,
        request_name: &'static str,
    ) -> Result<(), Status> {
        let ctx = self.auth_context(request, request_name)?;
        if let Some(c) = ctx {
            if !c.has_permission(permission) {
                return Err(self.deny(&c, "permission_denied", request_name));
            }
            self.allow();
        }
        Ok(())
    }

    /// Management operations (lifecycle, registration, dataset, commit): admin
    /// or operator roles.
    fn require_admin<T>(
        &self,
        request: &Request<T>,
        request_name: &'static str,
    ) -> Result<(), Status> {
        self.require_permission(request, crate::auth::Permission::Manage, request_name)
    }

    /// Read-only inspection RPCs: admin, operator, or viewer roles.
    fn require_read<T>(
        &self,
        request: &Request<T>,
        request_name: &'static str,
    ) -> Result<(), Status> {
        self.require_permission(request, crate::auth::Permission::Read, request_name)
    }

    /// Worker-plane self operations require the `worker` role explicitly (an
    /// admin does not masquerade as a worker for registration/heartbeat).
    fn require_worker_capability<T>(
        &self,
        request: &Request<T>,
        request_name: &'static str,
    ) -> Result<(), Status> {
        let ctx = self.auth_context(request, request_name)?;
        if let Some(c) = ctx {
            if !c.is_worker() {
                return Err(self.deny(&c, "permission_denied", request_name));
            }
            self.allow();
        }
        Ok(())
    }

    /// Allow a management-capable caller (admin/operator), or a worker acting on
    /// its own `worker_id`.
    fn allow_admin_or_worker_self<T>(
        &self,
        request: &Request<T>,
        worker_id: &str,
        request_name: &'static str,
    ) -> Result<(), Status> {
        let ctx = self.auth_context(request, request_name)?;
        if let Some(c) = ctx {
            if c.has_permission(crate::auth::Permission::Manage) {
                self.allow();
                return Ok(());
            }
            if c.is_worker() {
                match c.worker_id.clone() {
                    None => {
                        self.audit.emit(AuditEvent {
                            kind: Some(AuditEventKind::AuthorizationDenied),
                            outcome: Some(AuditOutcome::Rejected),
                            subject: Some(c.subject.clone()),
                            role: Some("worker".to_string()),
                            worker_id: None,
                            run_id: None,
                            dataset_id: None,
                            checkpoint_id: None,
                            reason: Some("missing_identity".to_string()),
                            request_name: Some(request_name.to_string()),
                        });
                        self.metrics.inc_auth_denied_total();
                        return Err(AuthorizationError::MissingIdentity.to_status());
                    }
                    Some(provided) if provided == worker_id => {
                        self.allow();
                        return Ok(());
                    }
                    Some(provided) => {
                        self.audit.emit(AuditEvent {
                            kind: Some(AuditEventKind::AuthorizationDenied),
                            outcome: Some(AuditOutcome::Rejected),
                            subject: Some(c.subject.clone()),
                            role: Some("worker".to_string()),
                            worker_id: Some(provided),
                            run_id: None,
                            dataset_id: None,
                            checkpoint_id: None,
                            reason: Some("worker_identity_mismatch".to_string()),
                            request_name: Some(request_name.to_string()),
                        });
                        self.metrics.inc_auth_denied_total();
                        return Err(AuthorizationError::WorkerIdentityMismatch.to_status());
                    }
                }
            }
            Err(self.deny(&c, "permission_denied", request_name))
        } else {
            Ok(())
        }
    }

    fn update_worker_metrics(&self, run_id: &RunId) {
        if let Some((active, stale)) = self.state.worker_counts(run_id) {
            self.metrics.set_active_workers(active);
            self.metrics.set_stale_workers(stale);
        }
    }
}

/// Resolve a slash-separated storage key to a path under the coordinator's
/// local `--storage-root`, one path component per key segment.
fn artifact_local_path(storage_root: &Path, artifact_key: &runtime_storage::StorageKey) -> PathBuf {
    let mut p = storage_root.to_path_buf();
    for segment in artifact_key.as_str().split('/') {
        p.push(segment);
    }
    p
}

fn cached_shard_hints(metadata: &tonic::metadata::MetadataMap) -> Option<Vec<String>> {
    metadata
        .get("x-dtruntime-cached-shards")
        .and_then(|v| v.to_str().ok())
        .map(|raw| {
            raw.split(',')
                .map(|v| v.trim())
                .filter(|v| !v.is_empty())
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
        })
}

fn invalid_argument(message: impl Into<String>) -> Status {
    Status::invalid_argument(message.into())
}

fn not_found(message: impl Into<String>) -> Status {
    Status::not_found(message.into())
}

fn failed_precondition(message: impl Into<String>) -> Status {
    Status::failed_precondition(message.into())
}

fn already_exists(message: impl Into<String>) -> Status {
    Status::already_exists(message.into())
}

/// Map an admin run-action failure onto a gRPC status: a missing run is
/// `NOT_FOUND`, an action invalid from the current lifecycle state is
/// `FAILED_PRECONDITION` (retrying won't help until the run changes state).
fn run_action_status(err: RunActionError, run_id: &str) -> Status {
    match err {
        RunActionError::RunNotFound => not_found(format!("run {run_id} was not found")),
        RunActionError::InvalidState { current, action } => {
            let state = RunState::try_from(current).unwrap_or(RunState::Unspecified);
            // Friendly label: RUN_STATE_RUNNING -> "running".
            let label = state
                .as_str_name()
                .strip_prefix("RUN_STATE_")
                .unwrap_or(state.as_str_name())
                .to_ascii_lowercase();
            failed_precondition(format!("cannot {action} run {run_id} while it is {label}"))
        }
    }
}

const MAX_IDEMPOTENCY_KEY_LEN: usize = 256;
const RUNTIME_CAPABILITIES: [&str; 10] = [
    "checkpoint-cleanup-library",
    "elastic-training",
    "idempotency-key-validation",
    "lease-active-worker-gate",
    "local-scaling-benchmarks",
    "rendezvous",
    "run-lifecycle",
    "s3-storage",
    "stream-checkpoint-artifact",
    "worker-deregistration",
];

fn validate_idempotency_key(operation: &str, idempotency_key: &str) -> Result<(), Status> {
    if !idempotency_key.is_empty() && idempotency_key.len() > MAX_IDEMPOTENCY_KEY_LEN {
        return Err(invalid_argument(format!(
            "{operation} idempotency_key must be <= {MAX_IDEMPOTENCY_KEY_LEN} bytes"
        )));
    }
    Ok(())
}

#[tonic::async_trait]
impl CoordinatorService for CoordinatorServiceImpl {
    async fn get_runtime_info(
        &self,
        request: Request<GetRuntimeInfoRequest>,
    ) -> Result<Response<GetRuntimeInfoResponse>, Status> {
        self.require_read(&request, "read_request")?;
        let version = protocol_version();
        let capabilities = RuntimeCapabilities {
            protocol_version: Some(version),
            features: RUNTIME_CAPABILITIES
                .iter()
                .map(|feature| (*feature).to_string())
                .collect(),
        };

        Ok(Response::new(GetRuntimeInfoResponse {
            protocol_version: Some(version),
            capabilities: Some(capabilities),
        }))
    }

    async fn register_run(
        &self,
        request: Request<RegisterRunRequest>,
    ) -> Result<Response<RegisterRunResponse>, Status> {
        self.require_admin(&request, "admin_request")?;
        let request = request.into_inner();
        validate_idempotency_key("register_run", &request.idempotency_key)?;
        if request.min_world_size > 0 && request.max_world_size < request.min_world_size {
            return Err(invalid_argument(
                "max_world_size must be >= min_world_size for elastic runs",
            ));
        }
        if request.min_world_size == 0 && request.max_world_size > 0 {
            return Err(invalid_argument(
                "min_world_size is required when max_world_size is set",
            ));
        }
        let registered = self.state.register_run(request);
        self.metrics.inc_runs_total();

        let response = RegisterRunResponse {
            run_id: Some(registered.run_id),
            coordinator_version: env!("CARGO_PKG_VERSION").to_string(),
            registered_at: Some(registered.registered_at.into()),
        };

        self.audit.emit(AuditEvent {
            kind: Some(AuditEventKind::RunRegistered),
            outcome: Some(AuditOutcome::Success),
            run_id: response.run_id.as_ref().map(|r| r.value.clone()),
            ..AuditEvent::default()
        });
        Ok(Response::new(response))
    }

    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        self.require_worker_capability(&request, "worker_request")?;
        let request = request.into_inner();
        validate_idempotency_key("register_worker", &request.idempotency_key)?;
        let run_id = request
            .run_id
            .clone()
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;

        if self.state.run_is_terminal(&run_id.value) == Some(true) {
            return Err(failed_precondition(format!(
                "run {} is in a terminal state and no longer accepts workers",
                run_id.value
            )));
        }

        let registered = self
            .state
            .register_worker(request)
            .ok_or_else(|| Status::not_found(format!("run {} was not found", run_id.value)))?;
        let worker_id_value = registered.worker_id.value.clone();
        if registered.is_new {
            self.metrics.inc_workers_total();
        }

        let response = RegisterWorkerResponse {
            worker_id: Some(registered.worker_id),
            state: registered.state,
            lease_renewal_deadline: Some(registered.lease_renewal_deadline.into()),
            server_time: Some(registered.server_time.into()),
        };
        self.update_worker_metrics(&run_id);
        info!(run_id = %run_id.value, worker_id = %worker_id_value, active_workers = self.metrics_snapshot().active_workers, "worker registered");

        self.audit.emit(AuditEvent {
            kind: Some(AuditEventKind::WorkerRegistered),
            outcome: Some(AuditOutcome::Success),
            run_id: Some(run_id.value.clone()),
            worker_id: Some(worker_id_value),
            ..AuditEvent::default()
        });
        Ok(Response::new(response))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let auth_worker_id = request
            .get_ref()
            .worker_id
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("worker_id is required"))?
            .value
            .clone();
        self.allow_admin_or_worker_self(&request, &auth_worker_id, "worker_self_request")?;
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
            .ok_or_else(|| {
                warn!(run_id = %run_id.value, worker_id = %worker_id.value, "heartbeat rejected for unknown worker");
                not_found(format!(
                    "worker {} was not found under run {}",
                    worker_id.value, run_id.value
                ))
            })?;

        let response = HeartbeatResponse {
            state: heartbeat.state,
            next_heartbeat_deadline: Some(heartbeat.next_heartbeat_deadline.into()),
            server_time: Some(heartbeat.server_time.into()),
            rendezvous_round: self.state.current_rendezvous_round(&run_id.value),
            run_state: heartbeat.run_state,
        };
        self.metrics.inc_heartbeats_total();
        // The active/stale gauges only move when a worker's state flips;
        // recounting the whole roster on every steady-state heartbeat is
        // O(workers²) per heartbeat wave. Reconciliation and the flip path
        // keep the gauges fresh.
        if heartbeat.state_changed {
            self.update_worker_metrics(&run_id);
        }
        // debug: heartbeats arrive continuously from every worker; info-level
        // logging here floods production logs.
        tracing::debug!(run_id = %run_id.value, worker_id = %worker_id.value, "heartbeat accepted");

        self.audit.emit(AuditEvent {
            kind: Some(AuditEventKind::WorkerHeartbeat),
            outcome: Some(AuditOutcome::Success),
            run_id: Some(run_id.value.clone()),
            worker_id: Some(worker_id.value.clone()),
            ..AuditEvent::default()
        });
        Ok(Response::new(response))
    }

    async fn register_dataset(
        &self,
        request: Request<RegisterDatasetRequest>,
    ) -> Result<Response<RegisterDatasetResponse>, Status> {
        self.require_admin(&request, "admin_request")?;
        let request = request.into_inner();
        validate_idempotency_key("register_dataset", &request.idempotency_key)?;
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
        self.metrics.inc_datasets_total();

        Ok(Response::new(response))
    }

    async fn get_shard_assignment(
        &self,
        request: Request<GetShardAssignmentRequest>,
    ) -> Result<Response<GetShardAssignmentResponse>, Status> {
        let cached_hints = cached_shard_hints(request.metadata());
        let auth_worker_id = request
            .get_ref()
            .worker_id
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("worker_id is required"))?
            .value
            .clone();
        self.allow_admin_or_worker_self(&request, &auth_worker_id, "worker_self_request")?;
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
            .get_shard_assignments(&run_id, &worker_id, &dataset_id, cached_hints.as_deref())
            .map_err(|error| match error {
                GetShardAssignmentsError::RunNotFound => {
                    not_found(format!("run {} was not found", run_id.value))
                }
                GetShardAssignmentsError::WorkerNotFound => {
                    not_found(format!("worker {} was not found", worker_id.value))
                }
                GetShardAssignmentsError::DatasetNotFound => {
                    not_found(format!("dataset {} was not found", dataset_id.value))
                }
                GetShardAssignmentsError::EmptyWorldSize => {
                    failed_precondition("run world_size must be greater than zero")
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
                checksum: assignment.checksum,
                checksum_algorithm: assignment.checksum_algorithm,
                size_bytes: assignment.size_bytes,
            })
            .collect();

        let response = GetShardAssignmentResponse {
            assignments,
            assignment_policy: shard_assignments.assignment_policy,
            server_time: Some(shard_assignments.server_time.into()),
        };
        for _ in &response.assignments {
            self.metrics.inc_shard_assignment_count();
        }
        info!(run_id = %run_id.value, worker_id = %worker_id.value, shard_count = response.assignments.len(), "shard assignment produced");

        Ok(Response::new(response))
    }

    async fn renew_shard_lease(
        &self,
        request: Request<RenewShardLeaseRequest>,
    ) -> Result<Response<RenewShardLeaseResponse>, Status> {
        let auth_worker_id = request
            .get_ref()
            .worker_id
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("worker_id is required"))?
            .value
            .clone();
        self.allow_admin_or_worker_self(&request, &auth_worker_id, "worker_self_request")?;
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
                    not_found(format!("run {} was not found", run_id.value))
                }
                RenewShardLeaseError::WorkerNotFound => {
                    not_found(format!("worker {} was not found", worker_id.value))
                }
                RenewShardLeaseError::ShardNotFound => {
                    not_found(format!("shard {} was not found", shard_id.value))
                }
                RenewShardLeaseError::EmptyWorldSize => {
                    failed_precondition("run world_size must be greater than zero")
                }
                RenewShardLeaseError::NotShardOwner => {
                    warn!(run_id = %run_id.value, worker_id = %worker_id.value, shard_id = %shard_id.value, "non-owner shard lease renewal attempt rejected");
                    failed_precondition(format!(
                        "worker {} is not the owner of shard {}",
                        worker_id.value, shard_id.value
                    ))
                }
                RenewShardLeaseError::InvalidLeaseEpoch => {
                    warn!(run_id = %run_id.value, worker_id = %worker_id.value, shard_id = %shard_id.value, lease_epoch = request.lease_epoch, "stale lease epoch in shard lease renewal");
                    failed_precondition(
                        "stale lease epoch: request lease_epoch does not match current assignment",
                    )
                }
                RenewShardLeaseError::WorkerNotActive => failed_precondition(
                    "worker must be active before renewing shard leases; heartbeat first to reactivate",
                ),
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
        let auth_worker_id = request
            .get_ref()
            .worker_id
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("worker_id is required"))?
            .value
            .clone();
        self.allow_admin_or_worker_self(&request, &auth_worker_id, "worker_self_request")?;
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
            .map_err(|error| {
                match error {
                ReportShardProgressError::RunNotFound => {
                    not_found(format!("run {} was not found", run_id.value))
                }
                ReportShardProgressError::WorkerNotFound => {
                    not_found(format!("worker {} was not found", worker_id.value))
                }
                ReportShardProgressError::DatasetNotFound => {
                    not_found(format!("dataset {} was not found", dataset_id.value))
                }
                ReportShardProgressError::ShardNotFound => {
                    not_found(format!("shard {} was not found", shard_id.value))
                }
                ReportShardProgressError::EmptyWorldSize => {
                    failed_precondition("run world_size must be greater than zero")
                }
                ReportShardProgressError::NotShardOwner => {
                    warn!(run_id = %run_id.value, worker_id = %worker_id.value, shard_id = %shard_id.value, "non-owner shard progress report rejected");
                    failed_precondition(format!(
                        "worker {} is not the owner of shard {}",
                        worker_id.value, shard_id.value
                    ))
                }
                ReportShardProgressError::InvalidLeaseEpoch => {
                    warn!(run_id = %run_id.value, worker_id = %worker_id.value, shard_id = %shard_id.value, lease_epoch = request.lease_epoch, "stale lease epoch in shard progress report");
                    failed_precondition("stale lease epoch: request lease_epoch does not match current assignment")
                }
                ReportShardProgressError::WorkerNotActive => failed_precondition(
                    "worker must be active before reporting shard progress; heartbeat first to reactivate",
                ),
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
        let auth_worker_id = request
            .get_ref()
            .worker_id
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("worker_id is required"))?
            .value
            .clone();
        self.allow_admin_or_worker_self(&request, &auth_worker_id, "worker_self_request")?;
        let request = request.into_inner();
        validate_idempotency_key("begin_checkpoint", &request.idempotency_key)?;
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
                    already_exists("checkpoint_id already exists with conflicting metadata")
                }
            })?;
        let created_at = std::time::UNIX_EPOCH
            + std::time::Duration::from_millis(checkpoint.checkpoint.created_at_millis);
        self.metrics.inc_checkpoint_starts();

        Ok(Response::new(BeginCheckpointResponse {
            checkpoint_id: Some(CheckpointId {
                value: checkpoint.checkpoint.checkpoint_id.0,
            }),
            state: CheckpointState::Initiated as i32,
            created_at: Some(created_at.into()),
            artifact_specs: Vec::new(),
        }))
    }

    async fn stream_checkpoint_artifact(
        &self,
        request: Request<Streaming<StreamCheckpointArtifactRequest>>,
    ) -> Result<Response<StreamCheckpointArtifactResponse>, Status> {
        // Resolve storage root — coordinator must be started with --storage-root.
        let storage_root = self.storage_root.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "coordinator storage root not configured; start with --storage-root <path>",
            )
        })?;

        let mut stream = request.into_inner();

        // First message must be the metadata header.
        let meta = match stream.message().await? {
            Some(msg) => match msg.payload {
                Some(stream_checkpoint_artifact_request::Payload::Meta(m)) => m,
                _ => {
                    return Err(Status::invalid_argument(
                        "first message must be StreamCheckpointArtifactMeta",
                    ))
                }
            },
            None => {
                return Err(Status::invalid_argument(
                    "stream closed before meta message",
                ))
            }
        };

        let run_id = meta
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?
            .value;
        let worker_id = meta
            .worker_id
            .ok_or_else(|| Status::invalid_argument("worker_id is required"))?
            .value;
        let checkpoint_id = meta
            .checkpoint_id
            .ok_or_else(|| Status::invalid_argument("checkpoint_id is required"))?
            .value;
        let artifact_name = meta.artifact_name;

        if artifact_name.trim().is_empty() {
            return Err(Status::invalid_argument("artifact_name must be non-empty"));
        }

        // Derive storage path from coordinator root.
        // Layout: <root>/runs/<run_id>/checkpoints/<checkpoint_id>/workers/<worker_id>/<artifact_name>
        let artifact_key = runtime_storage::checkpoint_worker_artifact(
            &run_id,
            &checkpoint_id,
            &worker_id,
            &artifact_name,
        );
        let artifact_path = artifact_local_path(storage_root.as_ref(), &artifact_key);

        if let Some(parent) = artifact_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                Status::internal(format!("failed to create artifact directory: {e}"))
            })?;
        }

        // Open file for writing. Streaming chunks arrive from the network and
        // are written immediately — tokio::fs keeps the async executor unblocked.
        let mut file = tokio::fs::File::create(&artifact_path)
            .await
            .map_err(|e| Status::internal(format!("failed to create artifact file: {e}")))?;

        let mut hasher = Sha256::new();
        let mut bytes_received: i64 = 0;
        let mut expected_seq: u64 = 0;
        // Buffer bytes for S3 upload when backend is configured.
        let upload_to_s3 = self.s3_backend.is_some();
        let mut s3_buf: Vec<u8> = if upload_to_s3 {
            Vec::with_capacity(meta.total_size_bytes.max(0) as usize)
        } else {
            Vec::new()
        };

        // Consume chunk messages, writing each to disk as it arrives.
        while let Some(msg) = stream.message().await? {
            match msg.payload {
                Some(stream_checkpoint_artifact_request::Payload::Chunk(chunk)) => {
                    if chunk.sequence != expected_seq {
                        return Err(Status::invalid_argument(format!(
                            "out-of-order chunk: expected seq {expected_seq}, got {}",
                            chunk.sequence
                        )));
                    }
                    file.write_all(&chunk.data).await.map_err(|e| {
                        Status::internal(format!("write failed at seq {}: {e}", chunk.sequence))
                    })?;
                    hasher.update(&chunk.data);
                    if upload_to_s3 {
                        s3_buf.extend_from_slice(&chunk.data);
                    }
                    bytes_received += chunk.data.len() as i64;
                    expected_seq += 1;
                }
                Some(stream_checkpoint_artifact_request::Payload::Meta(_)) => {
                    return Err(Status::invalid_argument(
                        "unexpected meta message after stream started",
                    ));
                }
                None => {
                    return Err(Status::invalid_argument(
                        "received message with empty payload",
                    ));
                }
            }
        }

        file.flush()
            .await
            .map_err(|e| Status::internal(format!("failed to flush artifact file: {e}")))?;
        drop(file);

        let checksum = hex::encode(hasher.finalize());

        // Post-write integrity verification: re-read the stored file from disk
        // and confirm its SHA-256 matches the inline checksum computed while
        // streaming.  Catches partial writes, silent truncation, and filesystem
        // corruption before the coordinator reports the artifact as accepted.
        if self.verify_checkpoint_artifacts {
            let stored = tokio::fs::read(&artifact_path).await.map_err(|e| {
                Status::data_loss(format!(
                    "artifact verification: could not re-read {}: {e}",
                    artifact_path.display()
                ))
            })?;
            let stored_checksum = hex::encode(Sha256::digest(&stored));
            if stored_checksum != checksum {
                return Err(Status::data_loss(format!(
                    "artifact checksum mismatch after write \
                     (stream_sha256={checksum} disk_sha256={stored_checksum}); \
                     artifact may be corrupt — rejecting to prevent silent data loss"
                )));
            }
        }

        // Upload to S3 when backend is configured. The local file is written first
        // for immediate coordinator-side availability; S3 provides durable object storage.
        let artifact_uri = if let (Some(s3), Some(bucket)) = (&self.s3_backend, &self.s3_bucket) {
            // put_async uses the AWS SDK client natively in the async context,
            // avoiding the nested-runtime panic that spawn_blocking + block_on causes.
            s3.put_async(&artifact_key, s3_buf)
                .await
                .map_err(|e| Status::internal(format!("S3 upload failed: {e}")))?;
            info!(
                run_id = %run_id,
                worker_id = %worker_id,
                checkpoint_id = %checkpoint_id,
                bucket = %bucket,
                key = %artifact_key.as_str(),
                "stream_checkpoint_artifact: artifact uploaded to S3"
            );
            format!("s3://{}/{}", bucket, artifact_key.as_str())
        } else {
            artifact_key.as_str().to_string()
        };
        let size_bytes = bytes_received.max(0) as u64;

        // Record the artifact in coordinator state — same path as ReportCheckpointArtifact
        // but called internally after the coordinator has already written the bytes.
        self.state
            .report_checkpoint_artifact(
                run_id.clone(),
                worker_id.clone(),
                checkpoint_id.clone(),
                artifact_name,
                artifact_uri.clone(),
                size_bytes,
                checksum.clone(),
                "sha256".to_string(),
                meta.idempotency_key,
            )
            .map_err(|error| match error {
                ReportCheckpointArtifactError::RunNotFound => {
                    not_found(format!("run {run_id} was not found"))
                }
                ReportCheckpointArtifactError::WorkerNotFound => {
                    not_found(format!("worker {worker_id} was not found"))
                }
                ReportCheckpointArtifactError::CheckpointNotFound => {
                    not_found(format!("checkpoint {checkpoint_id} was not found"))
                }
                ReportCheckpointArtifactError::InvalidInput(message) => invalid_argument(message),
                ReportCheckpointArtifactError::InvalidState => failed_precondition(
                    "checkpoint is in terminal state or cannot accept artifacts",
                ),
            })?;

        self.metrics.add_storage_bytes_written(size_bytes);
        self.metrics.inc_checkpoint_artifacts_reported_total();

        info!(
            run_id = %run_id,
            worker_id = %worker_id,
            checkpoint_id = %checkpoint_id,
            bytes_received,
            "stream_checkpoint_artifact: artifact written and registered"
        );

        Ok(Response::new(StreamCheckpointArtifactResponse {
            checkpoint_id: Some(CheckpointId {
                value: checkpoint_id,
            }),
            artifact_uri,
            bytes_received,
            checksum,
            checksum_algorithm: "sha256".to_string(),
        }))
    }

    async fn report_checkpoint_artifact(
        &self,
        request: Request<ReportCheckpointArtifactRequest>,
    ) -> Result<Response<ReportCheckpointArtifactResponse>, Status> {
        let (metadata, extensions, request) = request.into_parts();
        validate_idempotency_key("report_checkpoint_artifact", &request.idempotency_key)?;
        let run_id = request
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;
        let worker_id = request
            .worker_id
            .ok_or_else(|| Status::invalid_argument("worker_id is required"))?;
        let auth_req = Request::from_parts(metadata, extensions, ());
        self.allow_admin_or_worker_self(&auth_req, &worker_id.value, "worker_self_request")?;
        let checkpoint_id = request
            .checkpoint_id
            .ok_or_else(|| Status::invalid_argument("checkpoint_id is required"))?;
        let artifact_report = request
            .artifact_report
            .ok_or_else(|| Status::invalid_argument("artifact_report is required"))?;
        let reported_size_bytes = artifact_report.size_bytes.max(0) as u64;

        let checkpoint = self
            .state
            .report_checkpoint_artifact(
                run_id.value.clone(),
                worker_id.value.clone(),
                checkpoint_id.value.clone(),
                artifact_report.artifact_name,
                artifact_report.artifact_uri,
                reported_size_bytes,
                artifact_report.checksum,
                artifact_report.checksum_algorithm,
                request.idempotency_key,
            )
            .map_err(|error| match error {
                ReportCheckpointArtifactError::RunNotFound => {
                    not_found(format!("run {} was not found", run_id.value))
                }
                ReportCheckpointArtifactError::WorkerNotFound => {
                    not_found(format!("worker {} was not found", worker_id.value))
                }
                ReportCheckpointArtifactError::CheckpointNotFound => {
                    not_found(format!("checkpoint {} was not found", checkpoint_id.value))
                }
                ReportCheckpointArtifactError::InvalidInput(message) => invalid_argument(message),
                ReportCheckpointArtifactError::InvalidState => failed_precondition(
                    "checkpoint is in terminal state or cannot accept artifacts",
                ),
            })?;
        self.metrics.add_storage_bytes_written(reported_size_bytes);
        self.metrics.inc_checkpoint_artifacts_reported_total();

        Ok(Response::new(ReportCheckpointArtifactResponse {
            accepted: true,
            checkpoint_state: checkpoint_state_to_proto(checkpoint.checkpoint.state),
        }))
    }

    async fn commit_checkpoint(
        &self,
        request: Request<CommitCheckpointRequest>,
    ) -> Result<Response<CommitCheckpointResponse>, Status> {
        self.require_admin(&request, "admin_request")?;
        let request = request.into_inner();
        validate_idempotency_key("commit_checkpoint", &request.idempotency_key)?;
        let run_id = request
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;
        let checkpoint_id = request
            .checkpoint_id
            .ok_or_else(|| Status::invalid_argument("checkpoint_id is required"))?;
        // Gap 2: extract caller's worker_id for 2PC vote tracking.
        // Empty string triggers the legacy single-caller path (artifact-count only).
        let worker_id = request.worker_id.map(|w| w.value).unwrap_or_default();

        let checkpoint = self
            .state
            .commit_checkpoint(
                run_id.value.clone(),
                checkpoint_id.value.clone(),
                worker_id.clone(),
                request.idempotency_key,
            )
            .map_err(|error| {
                self.metrics.inc_failed_writes();
                error!(run_id = %run_id.value, checkpoint_id = %checkpoint_id.value, ?error, "checkpoint commit failed");
                match error {
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
                    CommitCheckpointError::PersistenceUnavailable => Status::unavailable(
                        "commit vote could not be durably recorded; retry the commit",
                    ),
                }
            })?;
        if checkpoint.globally_committed {
            self.metrics.inc_checkpoint_writes();
            info!(
                run_id = %run_id.value,
                checkpoint_id = %checkpoint_id.value,
                worker_id = %worker_id,
                "checkpoint globally committed"
            );
        } else {
            info!(
                run_id = %run_id.value,
                checkpoint_id = %checkpoint_id.value,
                worker_id = %worker_id,
                "checkpoint commit vote recorded — waiting for remaining workers"
            );
        }
        Ok(Response::new(CommitCheckpointResponse {
            state: checkpoint_state_to_proto(checkpoint.record.checkpoint.state),
            committed_at: Some(millis_to_timestamp(checkpoint.committed_at_millis)),
            globally_committed: checkpoint.globally_committed,
        }))
    }

    async fn get_latest_checkpoint(
        &self,
        request: Request<GetLatestCheckpointRequest>,
    ) -> Result<Response<GetLatestCheckpointResponse>, Status> {
        // Read-only checkpoint metadata: workers need it to restore, admins
        // to inspect.  Any authenticated principal is accepted.
        self.auth_context(&request, "checkpoint_read")?;
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

        self.metrics.inc_latest_checkpoint_reads_total();

        let storage_backend_kind = self
            .state
            .get_run(&run_id)
            .ok_or_else(|| Status::failed_precondition("run metadata missing"))?
            .storage_backend_kind;
        let manifest = record_to_manifest(checkpoint, storage_backend_kind);

        Ok(Response::new(GetLatestCheckpointResponse {
            manifest: Some(manifest),
        }))
    }

    async fn list_checkpoints(
        &self,
        request: Request<ListCheckpointsRequest>,
    ) -> Result<Response<ListCheckpointsResponse>, Status> {
        self.auth_context(&request, "checkpoint_read")?;
        let request = request.into_inner();
        let run_id = request
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;

        let records = self
            .state
            .list_checkpoints(
                &run_id.value,
                request.committed_only,
                request.limit as usize,
            )
            .map_err(|error| match error {
                crate::state::ListCheckpointsError::RunNotFound => {
                    not_found(format!("run {} was not found", run_id.value))
                }
                crate::state::ListCheckpointsError::PersistenceUnavailable => {
                    Status::unavailable("metadata store unavailable; retry")
                }
            })?;

        let storage_backend_kind = self
            .state
            .get_run(&run_id)
            .map(|r| r.storage_backend_kind)
            .unwrap_or_default();
        Ok(Response::new(ListCheckpointsResponse {
            checkpoints: records
                .into_iter()
                .map(|record| record_to_manifest(record, storage_backend_kind))
                .collect(),
        }))
    }

    async fn report_checkpoint_corrupt(
        &self,
        request: Request<ReportCheckpointCorruptRequest>,
    ) -> Result<Response<ReportCheckpointCorruptResponse>, Status> {
        let auth_worker_id = request
            .get_ref()
            .worker_id
            .as_ref()
            .map(|w| w.value.clone())
            .unwrap_or_default();
        self.allow_admin_or_worker_self(&request, &auth_worker_id, "worker_self_request")?;
        let request = request.into_inner();
        validate_idempotency_key("report_checkpoint_corrupt", &request.idempotency_key)?;
        let run_id = request
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;
        let checkpoint_id = request
            .checkpoint_id
            .ok_or_else(|| Status::invalid_argument("checkpoint_id is required"))?;

        let fallback = self
            .state
            .report_checkpoint_corrupt(&run_id.value, &checkpoint_id.value, &request.reason)
            .map_err(|error| match error {
                crate::state::ReportCorruptError::RunNotFound => {
                    not_found(format!("run {} was not found", run_id.value))
                }
                crate::state::ReportCorruptError::CheckpointNotFound => not_found(format!(
                    "checkpoint {} was not found under run {}",
                    checkpoint_id.value, run_id.value
                )),
                crate::state::ReportCorruptError::PersistenceUnavailable => {
                    Status::unavailable("metadata store unavailable; retry")
                }
            })?;

        self.audit.emit(AuditEvent {
            kind: Some(AuditEventKind::CheckpointCorruptReported),
            outcome: Some(AuditOutcome::Success),
            run_id: Some(run_id.value.clone()),
            checkpoint_id: Some(checkpoint_id.value.clone()),
            reason: Some(request.reason.clone()),
            ..AuditEvent::default()
        });

        Ok(Response::new(ReportCheckpointCorruptResponse {
            state: CheckpointState::Failed as i32,
            has_fallback: fallback.is_some(),
            fallback_checkpoint_id: fallback.map(|value| CheckpointId { value }),
        }))
    }

    type FetchCheckpointArtifactStream = std::pin::Pin<
        Box<
            dyn futures_core::Stream<Item = Result<FetchCheckpointArtifactChunk, Status>>
                + Send
                + 'static,
        >,
    >;

    async fn fetch_checkpoint_artifact(
        &self,
        request: Request<FetchCheckpointArtifactRequest>,
    ) -> Result<Response<Self::FetchCheckpointArtifactStream>, Status> {
        self.auth_context(&request, "checkpoint_read")?;
        let request = request.into_inner();
        let run_id = request
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?
            .value;
        let checkpoint_id = request
            .checkpoint_id
            .ok_or_else(|| Status::invalid_argument("checkpoint_id is required"))?
            .value;
        let owner_worker_id = request
            .owner_worker_id
            .ok_or_else(|| Status::invalid_argument("owner_worker_id is required"))?
            .value;
        let artifact_name = request.artifact_name;
        if artifact_name.trim().is_empty() {
            return Err(invalid_argument("artifact_name must be non-empty"));
        }

        let storage_root = self.storage_root.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "coordinator storage root not configured; start with --storage-root <path>",
            )
        })?;
        let artifact_key = runtime_storage::checkpoint_worker_artifact(
            &run_id,
            &checkpoint_id,
            &owner_worker_id,
            &artifact_name,
        );
        let artifact_path = artifact_local_path(storage_root.as_ref(), &artifact_key);

        let mut file = tokio::fs::File::open(&artifact_path).await.map_err(|e| {
            not_found(format!(
                "artifact {artifact_name} of checkpoint {checkpoint_id} not found in coordinator storage: {e}"
            ))
        })?;

        let metrics = self.metrics.clone();
        let (tx, rx) =
            tokio::sync::mpsc::channel::<Result<FetchCheckpointArtifactChunk, Status>>(8);
        tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            const CHUNK: usize = 512 * 1024;
            let mut hasher = Sha256::new();
            let mut sequence: u64 = 0;
            let mut total: i64 = 0;
            let mut buf = vec![0u8; CHUNK];
            loop {
                match file.read(&mut buf).await {
                    Ok(0) => {
                        let checksum = hex::encode(hasher.finalize());
                        let _ = tx
                            .send(Ok(FetchCheckpointArtifactChunk {
                                data: Vec::new(),
                                sequence,
                                last: true,
                                total_size_bytes: total,
                                checksum,
                                checksum_algorithm: "sha256".to_string(),
                            }))
                            .await;
                        metrics.add_storage_bytes_read(total.max(0) as u64);
                        break;
                    }
                    Ok(n) => {
                        hasher.update(&buf[..n]);
                        total += n as i64;
                        if tx
                            .send(Ok(FetchCheckpointArtifactChunk {
                                data: buf[..n].to_vec(),
                                sequence,
                                last: false,
                                total_size_bytes: 0,
                                checksum: String::new(),
                                checksum_algorithm: String::new(),
                            }))
                            .await
                            .is_err()
                        {
                            break; // client went away
                        }
                        sequence += 1;
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Err(Status::internal(format!("artifact read failed: {e}"))))
                            .await;
                        break;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn get_run_status(
        &self,
        request: Request<GetRunStatusRequest>,
    ) -> Result<Response<GetRunStatusResponse>, Status> {
        self.require_read(&request, "read_request")?;
        let request = request.into_inner();
        let run_id = request
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;

        self.state.reconcile_worker_liveness();
        self.update_worker_metrics(&run_id);

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
            state: record.state,
            state_reason: record.state_reason,
        };

        Ok(Response::new(response))
    }

    async fn deregister_worker(
        &self,
        request: Request<DeregisterWorkerRequest>,
    ) -> Result<Response<DeregisterWorkerResponse>, Status> {
        let auth_worker_id = request
            .get_ref()
            .worker_id
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("worker_id is required"))?
            .value
            .clone();
        self.allow_admin_or_worker_self(&request, &auth_worker_id, "worker_self_request")?;
        let request = request.into_inner();
        validate_idempotency_key("deregister_worker", &request.idempotency_key)?;
        let run_id = request
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;
        let worker_id = request
            .worker_id
            .ok_or_else(|| Status::invalid_argument("worker_id is required"))?;
        let reason =
            DeregisterReason::try_from(request.reason).unwrap_or(DeregisterReason::Unspecified);

        let (worker_state, run_state) = self
            .state
            .deregister_worker(&run_id.value, &worker_id.value, reason)
            .map_err(|error| match error {
                crate::state::DeregisterWorkerError::RunNotFound => {
                    not_found(format!("run {} was not found", run_id.value))
                }
                crate::state::DeregisterWorkerError::WorkerNotFound => not_found(format!(
                    "worker {} was not found under run {}",
                    worker_id.value, run_id.value
                )),
            })?;
        self.update_worker_metrics(&run_id);
        info!(
            run_id = %run_id.value,
            worker_id = %worker_id.value,
            reason = ?reason,
            "worker deregistered"
        );

        self.audit.emit(AuditEvent {
            kind: Some(AuditEventKind::WorkerDeregistered),
            outcome: Some(AuditOutcome::Success),
            run_id: Some(run_id.value.clone()),
            worker_id: Some(worker_id.value.clone()),
            reason: Some(format!("{reason:?}")),
            ..AuditEvent::default()
        });

        Ok(Response::new(DeregisterWorkerResponse {
            state: worker_state,
            run_state,
            server_time: Some(std::time::SystemTime::now().into()),
        }))
    }

    async fn cancel_run(
        &self,
        request: Request<CancelRunRequest>,
    ) -> Result<Response<CancelRunResponse>, Status> {
        self.require_admin(&request, "admin_request")?;
        let request = request.into_inner();
        validate_idempotency_key("cancel_run", &request.idempotency_key)?;
        let run_id = request
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;

        let state = self
            .state
            .cancel_run(&run_id.value, &request.reason)
            .map_err(|crate::state::CancelRunError::RunNotFound| {
                not_found(format!("run {} was not found", run_id.value))
            })?;
        info!(run_id = %run_id.value, reason = %request.reason, "run cancelled");

        self.audit.emit(AuditEvent {
            kind: Some(AuditEventKind::RunCancelled),
            outcome: Some(AuditOutcome::Success),
            run_id: Some(run_id.value.clone()),
            reason: if request.reason.is_empty() {
                None
            } else {
                Some(request.reason.clone())
            },
            ..AuditEvent::default()
        });

        Ok(Response::new(CancelRunResponse {
            state,
            server_time: Some(std::time::SystemTime::now().into()),
        }))
    }

    async fn pause_run(
        &self,
        request: Request<RunActionRequest>,
    ) -> Result<Response<RunActionResponse>, Status> {
        self.require_admin(&request, "admin_request")?;
        let request = request.into_inner();
        validate_idempotency_key("pause_run", &request.idempotency_key)?;
        let run_id = request
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;

        let (state, state_reason) = self
            .state
            .pause_run(&run_id.value, &request.reason)
            .map_err(|e| run_action_status(e, &run_id.value))?;
        info!(run_id = %run_id.value, reason = %request.reason, "run paused");

        self.audit.emit(AuditEvent {
            kind: Some(AuditEventKind::RunPaused),
            outcome: Some(AuditOutcome::Success),
            run_id: Some(run_id.value.clone()),
            reason: (!request.reason.is_empty()).then(|| request.reason.clone()),
            ..AuditEvent::default()
        });

        Ok(Response::new(RunActionResponse {
            state,
            state_reason,
            server_time: Some(std::time::SystemTime::now().into()),
        }))
    }

    async fn resume_run(
        &self,
        request: Request<RunActionRequest>,
    ) -> Result<Response<RunActionResponse>, Status> {
        self.require_admin(&request, "admin_request")?;
        let request = request.into_inner();
        validate_idempotency_key("resume_run", &request.idempotency_key)?;
        let run_id = request
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;

        let (state, state_reason) = self
            .state
            .resume_run(&run_id.value)
            .map_err(|e| run_action_status(e, &run_id.value))?;
        info!(run_id = %run_id.value, "run resumed");

        self.audit.emit(AuditEvent {
            kind: Some(AuditEventKind::RunResumed),
            outcome: Some(AuditOutcome::Success),
            run_id: Some(run_id.value.clone()),
            reason: (!request.reason.is_empty()).then(|| request.reason.clone()),
            ..AuditEvent::default()
        });

        Ok(Response::new(RunActionResponse {
            state,
            state_reason,
            server_time: Some(std::time::SystemTime::now().into()),
        }))
    }

    async fn retry_run(
        &self,
        request: Request<RunActionRequest>,
    ) -> Result<Response<RunActionResponse>, Status> {
        self.require_admin(&request, "admin_request")?;
        let request = request.into_inner();
        validate_idempotency_key("retry_run", &request.idempotency_key)?;
        let run_id = request
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;

        let (state, state_reason, retry_count) = self
            .state
            .retry_run(&run_id.value)
            .map_err(|e| run_action_status(e, &run_id.value))?;
        info!(run_id = %run_id.value, retry_count, "run retried");

        self.audit.emit(AuditEvent {
            kind: Some(AuditEventKind::RunRetried),
            outcome: Some(AuditOutcome::Success),
            run_id: Some(run_id.value.clone()),
            reason: Some(format!("retry #{retry_count}")),
            ..AuditEvent::default()
        });

        Ok(Response::new(RunActionResponse {
            state,
            state_reason,
            server_time: Some(std::time::SystemTime::now().into()),
        }))
    }

    async fn drain_run(
        &self,
        request: Request<RunActionRequest>,
    ) -> Result<Response<RunActionResponse>, Status> {
        self.require_admin(&request, "admin_request")?;
        let request = request.into_inner();
        validate_idempotency_key("drain_run", &request.idempotency_key)?;
        let run_id = request
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;

        let (state, state_reason) = self
            .state
            .drain_run(&run_id.value, &request.reason)
            .map_err(|e| run_action_status(e, &run_id.value))?;
        info!(run_id = %run_id.value, reason = %request.reason, "run draining");

        self.audit.emit(AuditEvent {
            kind: Some(AuditEventKind::RunDrained),
            outcome: Some(AuditOutcome::Success),
            run_id: Some(run_id.value.clone()),
            reason: (!request.reason.is_empty()).then(|| request.reason.clone()),
            ..AuditEvent::default()
        });

        Ok(Response::new(RunActionResponse {
            state,
            state_reason,
            server_time: Some(std::time::SystemTime::now().into()),
        }))
    }

    async fn list_runs(
        &self,
        request: Request<ListRunsRequest>,
    ) -> Result<Response<ListRunsResponse>, Status> {
        self.require_read(&request, "read_request")?;
        let request = request.into_inner();
        self.state.reconcile_worker_liveness();

        // RUN_STATE_UNSPECIFIED (0) means "all states"; any other value filters.
        let filter =
            (request.state_filter != RunState::Unspecified as i32).then_some(request.state_filter);

        let runs = self
            .state
            .list_runs(filter)
            .into_iter()
            .map(|r| RunSummary {
                run_id: Some(RunId { value: r.run_id }),
                run_name: r.run_name,
                state: r.state,
                state_reason: r.state_reason,
                world_size: r.world_size,
                active_worker_count: r.active_worker_count,
                latest_checkpoint_id: r.latest_checkpoint_id,
                latest_checkpoint_state: r.latest_checkpoint_state,
                retry_count: r.retry_count,
                registered_at: Some(r.registered_at.into()),
            })
            .collect();

        Ok(Response::new(ListRunsResponse {
            runs,
            server_time: Some(std::time::SystemTime::now().into()),
        }))
    }

    async fn list_workers(
        &self,
        request: Request<ListWorkersRequest>,
    ) -> Result<Response<ListWorkersResponse>, Status> {
        self.require_read(&request, "read_request")?;
        let request = request.into_inner();
        let run_id = request
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;

        self.state.reconcile_worker_liveness();

        // WORKER_STATE_UNSPECIFIED (0) means "all states".
        let filter = (request.state_filter != WorkerState::Unspecified as i32)
            .then_some(request.state_filter);

        let workers = self
            .state
            .list_workers(&run_id.value, filter)
            .ok_or_else(|| not_found(format!("run {} was not found", run_id.value)))?
            .into_iter()
            .map(|w| WorkerInfo {
                worker_id: Some(WorkerId { value: w.worker_id }),
                rank: w.rank,
                host: w.host,
                state: w.state,
                sdk_version: w.sdk_version,
                last_heartbeat_at: Some(w.last_heartbeat_at.into()),
                next_heartbeat_deadline: Some(w.next_heartbeat_deadline.into()),
                failure_deadline: Some(w.failure_deadline.into()),
            })
            .collect();

        Ok(Response::new(ListWorkersResponse {
            workers,
            server_time: Some(std::time::SystemTime::now().into()),
        }))
    }

    async fn list_shard_progress(
        &self,
        request: Request<ListShardProgressRequest>,
    ) -> Result<Response<ListShardProgressResponse>, Status> {
        self.require_read(&request, "read_request")?;
        let request = request.into_inner();
        let run_id = request
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;
        let dataset_filter = request
            .dataset_id
            .map(|d| d.value)
            .filter(|d| !d.is_empty());

        let shards = self
            .state
            .list_shard_progress(&run_id.value, dataset_filter.as_deref())
            .into_iter()
            .map(|s| ShardProgressInfo {
                shard_id: Some(ShardId { value: s.shard_id }),
                dataset_id: Some(DatasetId {
                    value: s.dataset_id,
                }),
                worker_id: Some(WorkerId { value: s.worker_id }),
                lease_epoch: s.lease_epoch,
                progress_offset: s.progress_offset,
                is_complete: s.is_complete,
                updated_at: Some(s.updated_at.into()),
            })
            .collect();

        Ok(Response::new(ListShardProgressResponse {
            shards,
            server_time: Some(std::time::SystemTime::now().into()),
        }))
    }

    async fn join_rendezvous(
        &self,
        request: Request<JoinRendezvousRequest>,
    ) -> Result<Response<JoinRendezvousResponse>, Status> {
        let auth_worker_id = request
            .get_ref()
            .worker_id
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("worker_id is required"))?
            .value
            .clone();
        self.allow_admin_or_worker_self(&request, &auth_worker_id, "worker_self_request")?;
        let request = request.into_inner();
        validate_idempotency_key("join_rendezvous", &request.idempotency_key)?;
        let run_id = request
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;
        let worker_id = request
            .worker_id
            .ok_or_else(|| Status::invalid_argument("worker_id is required"))?;

        // Reconcile first so a dead worker's stale round is invalidated before
        // this join lands in it.
        self.state.reconcile_worker_liveness();

        let snapshot = self
            .state
            .join_rendezvous(
                &run_id.value,
                &worker_id.value,
                &request.host,
                request.store_port,
            )
            .map_err(map_rendezvous_error)?;
        info!(
            run_id = %run_id.value,
            worker_id = %worker_id.value,
            round = snapshot.round,
            complete = snapshot.complete,
            world_size = snapshot.world_size,
            "rendezvous join"
        );

        Ok(Response::new(JoinRendezvousResponse {
            info: Some(rendezvous_snapshot_to_proto(snapshot)),
        }))
    }

    async fn get_rendezvous_state(
        &self,
        request: Request<GetRendezvousStateRequest>,
    ) -> Result<Response<GetRendezvousStateResponse>, Status> {
        let auth_worker_id = request
            .get_ref()
            .worker_id
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("worker_id is required"))?
            .value
            .clone();
        self.allow_admin_or_worker_self(&request, &auth_worker_id, "worker_self_request")?;
        let request = request.into_inner();
        let run_id = request
            .run_id
            .ok_or_else(|| Status::invalid_argument("run_id is required"))?;
        let worker_id = request
            .worker_id
            .ok_or_else(|| Status::invalid_argument("worker_id is required"))?;

        self.state.reconcile_worker_liveness();

        let snapshot = self
            .state
            .get_rendezvous_state(&run_id.value, &worker_id.value)
            .map_err(map_rendezvous_error)?;

        Ok(Response::new(GetRendezvousStateResponse {
            info: Some(rendezvous_snapshot_to_proto(snapshot)),
        }))
    }
}

fn map_rendezvous_error(error: crate::state::RendezvousError) -> Status {
    match error {
        crate::state::RendezvousError::RunNotFound => not_found("run was not found"),
        crate::state::RendezvousError::WorkerNotFound => {
            not_found("worker was not found; call RegisterWorker before JoinRendezvous")
        }
        crate::state::RendezvousError::MissingHost => invalid_argument("host is required"),
        crate::state::RendezvousError::RunTerminal(state) => failed_precondition(format!(
            "run is in terminal state {:?}; workers should exit",
            RunState::try_from(state).unwrap_or(RunState::Unspecified)
        )),
    }
}

fn rendezvous_snapshot_to_proto(snapshot: crate::rendezvous::RendezvousSnapshot) -> RendezvousInfo {
    RendezvousInfo {
        round: snapshot.round,
        state: if snapshot.complete {
            RendezvousRoundState::Complete as i32
        } else {
            RendezvousRoundState::Waiting as i32
        },
        world_size: snapshot.world_size,
        rank: snapshot.rank.unwrap_or(0),
        has_rank: snapshot.rank.is_some(),
        store_addr: snapshot.store_addr,
        store_port: snapshot.store_port,
        server_time: Some(std::time::SystemTime::now().into()),
    }
}

fn record_to_manifest(
    record: crate::state::CoordinatorCheckpointRecord,
    storage_backend_kind: i32,
) -> CheckpointManifest {
    CheckpointManifest {
        run_id: Some(RunId {
            value: record.checkpoint.run_id.0,
        }),
        checkpoint_id: Some(CheckpointId {
            value: record.checkpoint.checkpoint_id.0,
        }),
        step: record.checkpoint.step,
        state: checkpoint_state_to_proto(record.checkpoint.state),
        storage_backend_kind,
        artifacts: record
            .checkpoint
            .artifacts
            .into_iter()
            .map(|artifact| ArtifactReport {
                artifact_name: artifact.artifact_name,
                artifact_uri: artifact.storage_key,
                size_bytes: artifact.size_bytes as i64,
                checksum: artifact.checksum,
                checksum_algorithm: artifact.checksum_algorithm,
                uploaded_at: Some(millis_to_timestamp(record.checkpoint.updated_at_millis)),
            })
            .collect(),
        created_at: Some(millis_to_timestamp(record.checkpoint.created_at_millis)),
        committed_at: record.committed_at_millis.map(millis_to_timestamp),
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
    use crate::state::DatasetPlacementPolicy;
    use runtime_proto::runtime::v1::{AssignmentPolicy, StorageBackendKind};
    use runtime_proto::{PROTOCOL_MAJOR, PROTOCOL_MINOR, PROTOCOL_PATCH};
    use tonic::{Code, Request};

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
            ..Default::default()
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

    fn build_service() -> CoordinatorServiceImpl {
        CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()))
    }

    fn build_service_with_metadata_auth() -> CoordinatorServiceImpl {
        let state = Arc::new(CoordinatorState::default());
        let audit_sink = Arc::new(crate::audit::InMemoryAuditSink::default());
        let audit = AuditLogger::new(audit_sink);
        CoordinatorServiceImpl::with_metrics_authorizer_and_audit(
            state,
            Arc::new(RuntimeMetrics::default()),
            Authorizer::new(crate::auth::AuthMode::Metadata),
            audit,
        )
    }

    const JWT_TEST_SECRET: &[u8] = b"service-integration-jwt-secret";

    /// A service whose authenticator validates JWT bearer tokens (HS256) and
    /// has the dev metadata-header fallback disabled — production posture.
    fn build_service_with_jwt() -> (CoordinatorServiceImpl, tempfile::TempDir) {
        use crate::security::jwt::{JwtConfig, JwtValidator};
        use crate::security::keyset::{KeySource, ReloadableKeySet};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("jwt-secret");
        std::fs::write(&path, JWT_TEST_SECRET).unwrap();
        let keys = Arc::new(
            ReloadableKeySet::load(KeySource::HmacSecret {
                path,
                algorithm: jsonwebtoken::Algorithm::HS256,
            })
            .unwrap(),
        );
        let validator = Arc::new(JwtValidator::new(JwtConfig::default(), keys));
        let authenticator = Authorizer::new(crate::auth::AuthMode::Metadata)
            .with_jwt(validator)
            .with_metadata_fallback(false);
        let service = CoordinatorServiceImpl::with_metrics_authorizer_and_audit(
            Arc::new(CoordinatorState::default()),
            Arc::new(RuntimeMetrics::default()),
            authenticator,
            AuditLogger::new(Arc::new(crate::audit::InMemoryAuditSink::default())),
        );
        (service, dir)
    }

    fn mint_jwt(roles: serde_json::Value, exp_offset_secs: i64) -> String {
        use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        encode(
            &Header::new(Algorithm::HS256),
            &serde_json::json!({
                "sub": "svc-account",
                "exp": now + exp_offset_secs,
                "roles": roles,
            }),
            &EncodingKey::from_secret(JWT_TEST_SECRET),
        )
        .unwrap()
    }

    fn request_with_bearer<T>(token: &str, inner: T) -> Request<T> {
        let mut request = Request::new(inner);
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        request
    }

    fn request_as_role<T>(role: &str, inner: T) -> Request<T> {
        let mut request = Request::new(inner);
        request
            .metadata_mut()
            .insert("x-dtruntime-subject", "rbac-subject".parse().unwrap());
        request
            .metadata_mut()
            .insert("x-dtruntime-role", role.parse().unwrap());
        request
    }

    #[tokio::test]
    async fn viewer_can_read_but_not_manage() {
        let service = build_service_with_metadata_auth();
        // Viewer may list runs (read) ...
        service
            .list_runs(request_as_role(
                "viewer",
                ListRunsRequest { state_filter: 0 },
            ))
            .await
            .expect("viewer should read");
        // ... but not register (manage/register) ...
        let register_err = service
            .register_run(request_as_role("viewer", register_request("v-key")))
            .await
            .expect_err("viewer must not register runs");
        assert_permission_denied(register_err);
        // ... nor cancel (manage).
        let cancel_err = service
            .cancel_run(request_as_role(
                "viewer",
                CancelRunRequest {
                    run_id: Some(RunId {
                        value: "any".to_string(),
                    }),
                    reason: String::new(),
                    idempotency_key: String::new(),
                },
            ))
            .await
            .expect_err("viewer must not cancel");
        assert_permission_denied(cancel_err);
    }

    #[tokio::test]
    async fn operator_can_manage_runs() {
        let service = build_service_with_metadata_auth();
        let run_id = service
            .register_run(request_as_role("operator", register_request("op-key")))
            .await
            .expect("operator should register")
            .into_inner()
            .run_id
            .unwrap();
        service
            .pause_run(request_as_role(
                "operator",
                RunActionRequest {
                    run_id: Some(run_id.clone()),
                    reason: "op maintenance".to_string(),
                    idempotency_key: String::new(),
                },
            ))
            .await
            .expect("operator should pause");
    }

    #[tokio::test]
    async fn jwt_admin_token_authorizes_management() {
        let (service, _dir) = build_service_with_jwt();
        let token = mint_jwt(serde_json::json!(["admin"]), 300);
        service
            .register_run(request_with_bearer(&token, register_request("jwt-key")))
            .await
            .expect("valid admin JWT should authorize register_run");
    }

    #[tokio::test]
    async fn jwt_viewer_token_cannot_manage() {
        let (service, _dir) = build_service_with_jwt();
        let token = mint_jwt(serde_json::json!(["viewer"]), 300);
        // Read is allowed.
        service
            .list_runs(request_with_bearer(
                &token,
                ListRunsRequest { state_filter: 0 },
            ))
            .await
            .expect("viewer JWT should read");
        // Management is denied.
        let err = service
            .register_run(request_with_bearer(&token, register_request("v2-key")))
            .await
            .expect_err("viewer JWT must not register");
        assert_permission_denied(err);
    }

    // ── threat-model validation ──────────────────────────────────────────────

    #[tokio::test]
    async fn threat_expired_token_is_rejected() {
        let (service, _dir) = build_service_with_jwt();
        let token = mint_jwt(serde_json::json!(["admin"]), -300);
        let err = service
            .register_run(request_with_bearer(&token, register_request("exp-key")))
            .await
            .expect_err("expired token must be rejected");
        assert_unauthenticated(err);
    }

    #[tokio::test]
    async fn threat_forged_token_signature_is_rejected() {
        let (service, _dir) = build_service_with_jwt();
        let mut token = mint_jwt(serde_json::json!(["admin"]), 300);
        // Corrupt the signature segment.
        let last = token.pop().unwrap();
        token.push(if last == 'A' { 'B' } else { 'A' });
        let err = service
            .register_run(request_with_bearer(&token, register_request("forge-key")))
            .await
            .expect_err("forged signature must be rejected");
        assert_unauthenticated(err);
    }

    #[tokio::test]
    async fn threat_header_identity_ignored_when_fallback_disabled() {
        // With JWT auth and header fallback disabled, a caller cannot escalate
        // by asserting `x-dtruntime-role: admin` — no bearer, no access.
        let (service, _dir) = build_service_with_jwt();
        let err = service
            .register_run(request_as_role("admin", register_request("spoof-key")))
            .await
            .expect_err("spoofed header identity must not authorize");
        assert_unauthenticated(err);
    }

    fn request_as_admin<T>(inner: T) -> Request<T> {
        let mut request = Request::new(inner);
        request
            .metadata_mut()
            .insert("x-dtruntime-subject", "admin-user".parse().unwrap());
        request
            .metadata_mut()
            .insert("x-dtruntime-role", "admin".parse().unwrap());
        request
    }

    fn request_as_worker<T>(worker_id: &str, inner: T) -> Request<T> {
        let mut request = Request::new(inner);
        request
            .metadata_mut()
            .insert("x-dtruntime-subject", "worker-user".parse().unwrap());
        request
            .metadata_mut()
            .insert("x-dtruntime-role", "worker".parse().unwrap());
        request
            .metadata_mut()
            .insert("x-dtruntime-worker-id", worker_id.parse().unwrap());
        request
    }

    fn request_without_auth<T>(inner: T) -> Request<T> {
        Request::new(inner)
    }

    fn request_with_invalid_role<T>(inner: T) -> Request<T> {
        let mut request = Request::new(inner);
        request
            .metadata_mut()
            .insert("x-dtruntime-subject", "bad-role-subject".parse().unwrap());
        // An unrecognized role token — rejected as Unauthenticated (InvalidRole).
        request
            .metadata_mut()
            .insert("x-dtruntime-role", "superuser".parse().unwrap());
        request
    }

    fn assert_unauthenticated(status: Status) {
        assert_eq!(status.code(), Code::Unauthenticated);
    }

    fn assert_permission_denied(status: Status) {
        assert_eq!(status.code(), Code::PermissionDenied);
    }
    fn build_service_cache_aware() -> CoordinatorServiceImpl {
        CoordinatorServiceImpl::new(Arc::new(CoordinatorState::with_placement_policy(
            DatasetPlacementPolicy::CacheAware,
        )))
    }

    #[tokio::test]
    async fn get_runtime_info_returns_protocol_version_and_capabilities() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let response = service
            .get_runtime_info(Request::new(GetRuntimeInfoRequest {}))
            .await
            .expect("get_runtime_info should succeed")
            .into_inner();

        let version = response
            .protocol_version
            .expect("runtime info must include protocol version");
        assert_eq!(version.major, PROTOCOL_MAJOR);
        assert_eq!(version.minor, PROTOCOL_MINOR);
        assert_eq!(version.patch, PROTOCOL_PATCH);

        let capabilities = response
            .capabilities
            .expect("runtime info must include capabilities");
        assert_eq!(
            capabilities
                .protocol_version
                .expect("capabilities should include version"),
            version
        );

        let mut features = capabilities.features;
        features.sort_unstable();
        let mut expected = RUNTIME_CAPABILITIES
            .iter()
            .map(|feature| (*feature).to_string())
            .collect::<Vec<_>>();
        expected.sort_unstable();
        assert_eq!(features, expected);
    }

    #[tokio::test]
    async fn metadata_auth_register_run_authorization_matrix() {
        let service = build_service_with_metadata_auth();
        service
            .register_run(request_as_admin(register_request("admin-key")))
            .await
            .expect("admin should register run");

        let worker_error = service
            .register_run(request_as_worker(
                "worker-1",
                register_request("worker-key"),
            ))
            .await
            .expect_err("worker should be denied");
        assert_permission_denied(worker_error);

        let missing_error = service
            .register_run(request_without_auth(register_request("missing-key")))
            .await
            .expect_err("missing auth should be rejected");
        assert_unauthenticated(missing_error);

        let invalid_error = service
            .register_run(request_with_invalid_role(register_request("invalid-key")))
            .await
            .expect_err("invalid role should be rejected");
        assert_unauthenticated(invalid_error);
    }

    #[tokio::test]
    async fn metadata_auth_worker_scoped_operations_enforce_worker_self() {
        let service = build_service_with_metadata_auth();
        let run_id = service
            .register_run(request_as_admin(register_request("run-key")))
            .await
            .unwrap()
            .into_inner()
            .run_id
            .unwrap();
        let worker_id = service
            .register_worker(request_as_worker(
                "worker-1",
                worker_request(run_id.clone(), "register-worker"),
            ))
            .await
            .unwrap()
            .into_inner()
            .worker_id
            .unwrap();
        let dataset_id = service
            .register_dataset(request_as_admin(dataset_request(
                run_id.clone(),
                "dataset-key",
                Some("dataset-explicit"),
                &[Some("shard-a"), Some("shard-b")],
            )))
            .await
            .unwrap()
            .into_inner()
            .dataset_id
            .unwrap();

        service
            .heartbeat(request_as_worker(
                &worker_id.value,
                HeartbeatRequest {
                    run_id: Some(run_id.clone()),
                    worker_id: Some(worker_id.clone()),
                    idempotency_key: "hb-ok".to_string(),
                    client_time: None,
                    active_shard_ids: vec![],
                },
            ))
            .await
            .expect("worker self heartbeat should pass");
        let hb_err = service
            .heartbeat(request_as_worker(
                "other-worker",
                HeartbeatRequest {
                    run_id: Some(run_id.clone()),
                    worker_id: Some(worker_id.clone()),
                    idempotency_key: "hb-bad".to_string(),
                    client_time: None,
                    active_shard_ids: vec![],
                },
            ))
            .await
            .expect_err("worker mismatch should be denied");
        assert_permission_denied(hb_err);

        let missing_worker_id_error = service
            .heartbeat(request_without_auth(HeartbeatRequest {
                run_id: Some(run_id.clone()),
                worker_id: Some(worker_id.clone()),
                idempotency_key: "hb-missing".to_string(),
                client_time: None,
                active_shard_ids: vec![],
            }))
            .await
            .expect_err("missing metadata should reject");
        assert_unauthenticated(missing_worker_id_error);

        let assignment_err = service
            .get_shard_assignment(request_as_worker(
                "other-worker",
                GetShardAssignmentRequest {
                    run_id: Some(run_id.clone()),
                    worker_id: Some(worker_id.clone()),
                    dataset_id: Some(dataset_id.clone()),
                    idempotency_key: "assign-bad".to_string(),
                },
            ))
            .await
            .expect_err("mismatch should deny assignment");
        assert_permission_denied(assignment_err);
    }

    #[tokio::test]
    async fn metadata_auth_admin_only_and_worker_scoped_matrix_core_paths() {
        let service = build_service_with_metadata_auth();
        let run_id = service
            .register_run(request_as_admin(register_request("run-matrix")))
            .await
            .unwrap()
            .into_inner()
            .run_id
            .unwrap();
        let worker_id = service
            .register_worker(request_as_worker(
                "seed-worker",
                worker_request(run_id.clone(), "worker-matrix"),
            ))
            .await
            .unwrap()
            .into_inner()
            .worker_id
            .unwrap();
        let dataset_id = service
            .register_dataset(request_as_admin(dataset_request(
                run_id.clone(),
                "dataset-matrix",
                Some("dataset-m"),
                &[Some("shard-0")],
            )))
            .await
            .unwrap()
            .into_inner()
            .dataset_id
            .unwrap();

        let deny = service
            .register_dataset(request_as_worker(
                &worker_id.value,
                dataset_request(
                    run_id.clone(),
                    "dataset-deny",
                    Some("d2"),
                    &[Some("shard-1")],
                ),
            ))
            .await
            .expect_err("worker denied");
        assert_permission_denied(deny);
        assert_unauthenticated(
            service
                .register_dataset(request_without_auth(dataset_request(
                    run_id.clone(),
                    "dataset-missing",
                    Some("d3"),
                    &[Some("shard-2")],
                )))
                .await
                .expect_err("missing"),
        );
        assert_unauthenticated(
            service
                .register_dataset(request_with_invalid_role(dataset_request(
                    run_id.clone(),
                    "dataset-invalid",
                    Some("d4"),
                    &[Some("shard-3")],
                )))
                .await
                .expect_err("invalid"),
        );

        service
            .get_run_status(request_as_admin(GetRunStatusRequest {
                run_id: Some(run_id.clone()),
            }))
            .await
            .unwrap();
        assert_permission_denied(
            service
                .get_run_status(request_as_worker(
                    &worker_id.value,
                    GetRunStatusRequest {
                        run_id: Some(run_id.clone()),
                    },
                ))
                .await
                .expect_err("worker denied"),
        );

        service
            .get_shard_assignment(request_as_worker(
                &worker_id.value,
                GetShardAssignmentRequest {
                    run_id: Some(run_id.clone()),
                    worker_id: Some(worker_id.clone()),
                    dataset_id: Some(dataset_id.clone()),
                    idempotency_key: "ok".into(),
                },
            ))
            .await
            .unwrap();
        assert_permission_denied(
            service
                .get_shard_assignment(request_as_worker(
                    "other",
                    GetShardAssignmentRequest {
                        run_id: Some(run_id.clone()),
                        worker_id: Some(worker_id.clone()),
                        dataset_id: Some(dataset_id.clone()),
                        idempotency_key: "bad".into(),
                    },
                ))
                .await
                .expect_err("mismatch"),
        );

        let ckpt = service
            .begin_checkpoint(request_as_worker(
                &worker_id.value,
                BeginCheckpointRequest {
                    run_id: Some(run_id.clone()),
                    worker_id: Some(worker_id.clone()),
                    checkpoint_id: Some(CheckpointId {
                        value: "ckpt-m".into(),
                    }),
                    step: 1,
                    idempotency_key: "begin-m".into(),
                },
            ))
            .await
            .unwrap()
            .into_inner()
            .checkpoint_id
            .unwrap();
        assert_permission_denied(
            service
                .begin_checkpoint(request_as_worker(
                    "other",
                    BeginCheckpointRequest {
                        run_id: Some(run_id.clone()),
                        worker_id: Some(worker_id.clone()),
                        checkpoint_id: Some(CheckpointId {
                            value: "ckpt-bad".into(),
                        }),
                        step: 1,
                        idempotency_key: "begin-bad".into(),
                    },
                ))
                .await
                .expect_err("mismatch"),
        );

        service
            .report_checkpoint_artifact(request_as_worker(
                &worker_id.value,
                artifact_request(
                    run_id.clone(),
                    worker_id.clone(),
                    &ckpt.value,
                    "weights.bin",
                    "s3://bucket/weights.bin",
                    "artifact-m",
                ),
            ))
            .await
            .unwrap();
        assert_permission_denied(
            service
                .report_checkpoint_artifact(request_as_worker(
                    "other",
                    artifact_request(
                        run_id.clone(),
                        worker_id.clone(),
                        &ckpt.value,
                        "weights2.bin",
                        "s3://bucket/weights2.bin",
                        "artifact-bad",
                    ),
                ))
                .await
                .expect_err("mismatch"),
        );

        service
            .commit_checkpoint(request_as_admin(CommitCheckpointRequest {
                run_id: Some(run_id.clone()),
                checkpoint_id: Some(ckpt.clone()),
                idempotency_key: "commit-m".into(),
                worker_id: None,
            }))
            .await
            .unwrap();
        assert_permission_denied(
            service
                .commit_checkpoint(request_as_worker(
                    &worker_id.value,
                    CommitCheckpointRequest {
                        run_id: Some(run_id.clone()),
                        checkpoint_id: Some(ckpt.clone()),
                        idempotency_key: "commit-deny".into(),
                        worker_id: None,
                    },
                ))
                .await
                .expect_err("worker denied"),
        );

        service
            .get_latest_checkpoint(request_as_admin(GetLatestCheckpointRequest {
                run_id: Some(run_id.clone()),
            }))
            .await
            .unwrap();
        // Checkpoint metadata reads are allowed for workers too: restore runs
        // under worker identity.
        service
            .get_latest_checkpoint(request_as_worker(
                &worker_id.value,
                GetLatestCheckpointRequest {
                    run_id: Some(run_id.clone()),
                },
            ))
            .await
            .expect("worker may read checkpoint metadata for restore");

        service
            .get_runtime_info(request_as_admin(GetRuntimeInfoRequest {}))
            .await
            .unwrap();
        assert_permission_denied(
            service
                .get_runtime_info(request_as_worker(
                    &worker_id.value,
                    GetRuntimeInfoRequest {},
                ))
                .await
                .expect_err("worker denied"),
        );
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
    async fn list_runs_returns_registered_run_and_filters_by_state() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run(&service).await;

        // Unfiltered: the newly registered run is visible and PENDING.
        let all = service
            .list_runs(Request::new(ListRunsRequest { state_filter: 0 }))
            .await
            .expect("list_runs should succeed")
            .into_inner();
        assert_eq!(all.runs.len(), 1);
        assert_eq!(all.runs[0].run_id.as_ref(), Some(&run_id));
        assert_eq!(all.runs[0].state, RunState::Pending as i32);
        assert_eq!(all.runs[0].retry_count, 0);

        // Filtering to a state the run is not in returns nothing.
        let failed = service
            .list_runs(Request::new(ListRunsRequest {
                state_filter: RunState::Failed as i32,
            }))
            .await
            .expect("list_runs should succeed")
            .into_inner();
        assert!(failed.runs.is_empty());
    }

    #[tokio::test]
    async fn pause_resume_cycle_reports_states() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run(&service).await;

        let paused = service
            .pause_run(Request::new(RunActionRequest {
                run_id: Some(run_id.clone()),
                reason: "maintenance".to_string(),
                idempotency_key: String::new(),
            }))
            .await
            .expect("pause_run should succeed")
            .into_inner();
        assert_eq!(paused.state, RunState::Paused as i32);
        assert!(paused.state_reason.contains("maintenance"));

        let resumed = service
            .resume_run(Request::new(RunActionRequest {
                run_id: Some(run_id.clone()),
                reason: String::new(),
                idempotency_key: String::new(),
            }))
            .await
            .expect("resume_run should succeed")
            .into_inner();
        // No active workers -> back to PENDING for a fresh launch.
        assert_eq!(resumed.state, RunState::Pending as i32);
    }

    #[tokio::test]
    async fn pause_invalid_state_maps_to_failed_precondition() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run(&service).await;
        // Cancel first so the run is terminal and not pausable.
        service
            .cancel_run(Request::new(CancelRunRequest {
                run_id: Some(run_id.clone()),
                reason: String::new(),
                idempotency_key: String::new(),
            }))
            .await
            .expect("cancel_run should succeed");

        let status = service
            .pause_run(Request::new(RunActionRequest {
                run_id: Some(run_id.clone()),
                reason: String::new(),
                idempotency_key: String::new(),
            }))
            .await
            .expect_err("pause of a cancelled run should fail");
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn admin_action_on_unknown_run_is_not_found() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let status = service
            .drain_run(Request::new(RunActionRequest {
                run_id: Some(RunId {
                    value: "ghost".to_string(),
                }),
                reason: String::new(),
                idempotency_key: String::new(),
            }))
            .await
            .expect_err("drain of an unknown run should fail");
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn list_workers_unknown_run_is_not_found() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let status = service
            .list_workers(Request::new(ListWorkersRequest {
                run_id: Some(RunId {
                    value: "ghost".to_string(),
                }),
                state_filter: 0,
            }))
            .await
            .expect_err("list_workers for an unknown run should fail");
        assert_eq!(status.code(), tonic::Code::NotFound);
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

        // A second *distinct* artifact from the same worker is legitimate:
        // distributed checkpoint formats write several files per rank.
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
            .unwrap()
            .into_inner();
        assert!(third.accepted);

        // Re-reporting an existing artifact name with different metadata is a
        // conflict.
        let mut conflicting = artifact_request(
            run_id.clone(),
            worker_id.clone(),
            "ckpt-1",
            "a.bin",
            "s3://bucket/a.bin",
            "artifact-key-3",
        );
        conflicting.artifact_report.as_mut().unwrap().checksum = "different-sum".to_string();
        let fourth = service
            .report_checkpoint_artifact(Request::new(conflicting))
            .await
            .unwrap_err();
        assert_eq!(fourth.code(), tonic::Code::InvalidArgument);
        assert!(fourth
            .message()
            .contains("worker already reported conflicting artifact metadata"));
        assert_eq!(service.metrics_snapshot().storage_bytes_written, 30);
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
                worker_id: None,
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
                worker_id: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(second.state, CheckpointState::Committed as i32);
        assert_eq!(service.metrics_snapshot().checkpoint_writes, 2);

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
                worker_id: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(missing_run.code(), tonic::Code::NotFound);
        assert_eq!(service.metrics_snapshot().failed_writes, 1);

        let run_id = create_run(&service).await;
        let missing_checkpoint = service
            .commit_checkpoint(Request::new(CommitCheckpointRequest {
                run_id: Some(run_id.clone()),
                checkpoint_id: Some(CheckpointId {
                    value: "missing".to_string(),
                }),
                idempotency_key: String::new(),
                worker_id: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(missing_checkpoint.code(), tonic::Code::NotFound);
        assert_eq!(service.metrics_snapshot().failed_writes, 2);

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
                worker_id: None,
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
                    worker_id: None,
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

    #[tokio::test]
    async fn register_worker_updates_active_worker_metric() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run(&service).await;
        service
            .register_worker(Request::new(worker_request(run_id, "worker-key")))
            .await
            .unwrap();
        assert_eq!(service.metrics_snapshot().active_workers, 1);
    }

    #[tokio::test]
    async fn register_worker_increments_workers_total_once_for_new_registration() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run(&service).await;

        service
            .register_worker(Request::new(worker_request(run_id.clone(), "worker-key")))
            .await
            .expect("first registration should succeed");
        service
            .register_worker(Request::new(worker_request(run_id, "worker-key")))
            .await
            .expect("idempotent retry should succeed");

        assert_eq!(service.metrics_snapshot().workers_total, 1);
    }

    #[tokio::test]
    async fn get_run_status_updates_stale_worker_metric() {
        let state = Arc::new(CoordinatorState::default());
        let service = CoordinatorServiceImpl::new(state.clone());
        let run_id = create_run(&service).await;
        let worker_id = service
            .register_worker(Request::new(worker_request(run_id.clone(), "worker-key")))
            .await
            .unwrap()
            .into_inner()
            .worker_id
            .unwrap();
        assert!(state.set_worker_heartbeat_deadline_for_test(
            &run_id,
            &worker_id,
            std::time::SystemTime::now() - std::time::Duration::from_secs(1)
        ));
        service
            .get_run_status(Request::new(GetRunStatusRequest {
                run_id: Some(run_id),
            }))
            .await
            .unwrap();
        assert_eq!(service.metrics_snapshot().stale_workers, 1);
    }

    #[tokio::test]
    async fn heartbeat_increments_heartbeats_total_for_registered_worker() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run(&service).await;
        let worker_id = service
            .register_worker(Request::new(worker_request(run_id.clone(), "worker-key")))
            .await
            .expect("register_worker should succeed")
            .into_inner()
            .worker_id
            .expect("worker_id should be present");

        service
            .heartbeat(Request::new(HeartbeatRequest {
                run_id: Some(run_id),
                worker_id: Some(worker_id),
                active_shard_ids: Vec::new(),
                client_time: None,
                idempotency_key: "heartbeat-key".to_string(),
            }))
            .await
            .expect("heartbeat should succeed");

        assert_eq!(service.metrics_snapshot().heartbeats_total, 1);
    }

    #[tokio::test]
    async fn unknown_worker_heartbeat_does_not_increment_heartbeats_total() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run(&service).await;

        let err = service
            .heartbeat(Request::new(HeartbeatRequest {
                run_id: Some(run_id),
                worker_id: Some(WorkerId {
                    value: "missing-worker".to_string(),
                }),
                active_shard_ids: Vec::new(),
                client_time: None,
                idempotency_key: "heartbeat-key".to_string(),
            }))
            .await
            .expect_err("heartbeat for unknown worker should fail");

        assert_eq!(err.code(), tonic::Code::NotFound);
        assert_eq!(service.metrics_snapshot().heartbeats_total, 0);
    }

    #[tokio::test]
    async fn shard_assignment_increments_metric() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run_with_world_size(&service, 2).await;
        let dataset_id = register_dataset_for_assignment(&service, run_id.clone()).await;
        let worker_id = register_worker_for_assignment(&service, run_id.clone(), 0, "k").await;
        service
            .get_shard_assignment(Request::new(GetShardAssignmentRequest {
                run_id: Some(run_id),
                worker_id: Some(worker_id),
                dataset_id: Some(dataset_id),
                idempotency_key: "metrics-shard-assignment".to_string(),
            }))
            .await
            .unwrap();
        assert!(service.metrics_snapshot().shard_assignment_count > 0);
    }

    #[tokio::test]
    async fn metrics_snapshot_available_from_service() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let snapshot = service.metrics_snapshot();
        assert_eq!(snapshot, RuntimeMetricsSnapshot::default());
    }

    #[tokio::test]
    async fn register_run_rejects_overlong_idempotency_key() {
        let service = build_service();
        let key = "k".repeat(MAX_IDEMPOTENCY_KEY_LEN + 1);
        let err = service
            .register_run(Request::new(register_request(&key)))
            .await
            .expect_err("overlong key should be rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn begin_checkpoint_rejects_overlong_idempotency_key() {
        let service = build_service();
        let run = service
            .register_run(Request::new(register_request("run")))
            .await
            .expect("register run")
            .into_inner();
        let run_id = run.run_id.expect("run id");
        let worker = service
            .register_worker(Request::new(worker_request(run_id.clone(), "worker")))
            .await
            .expect("register worker")
            .into_inner();
        let worker_id = worker.worker_id.expect("worker id");

        let key = "k".repeat(MAX_IDEMPOTENCY_KEY_LEN + 1);
        let err = service
            .begin_checkpoint(Request::new(BeginCheckpointRequest {
                run_id: Some(run_id),
                worker_id: Some(worker_id),
                checkpoint_id: None,
                step: 1,
                idempotency_key: key,
            }))
            .await
            .expect_err("overlong key should be rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn cache_aware_assignment_prefers_cached_hint_order() {
        let service = build_service_cache_aware();
        let run_id = create_run_with_world_size(&service, 1).await;
        let worker_id = register_worker_for_assignment(&service, run_id.clone(), 0, "worker").await;
        let dataset_id = register_dataset_for_assignment(&service, run_id.clone()).await;
        let mut request = Request::new(GetShardAssignmentRequest {
            run_id: Some(run_id),
            worker_id: Some(worker_id),
            dataset_id: Some(dataset_id),
            idempotency_key: "cache-hint".to_string(),
        });
        request.metadata_mut().insert(
            "x-dtruntime-cached-shards",
            "shard-3,missing".parse().expect("metadata"),
        );
        let response = service
            .get_shard_assignment(request)
            .await
            .expect("shard assignment")
            .into_inner();
        let first = response.assignments[0]
            .shard_id
            .as_ref()
            .expect("shard id")
            .value
            .clone();
        assert_eq!(first, "shard-3");
    }

    // ── StreamCheckpointArtifact ──────────────────────────────────────────────
    // The streaming RPC handler requires a live gRPC transport to construct
    // tonic::Streaming<T>. Streaming behaviour is covered by the Python
    // integration tests (scripts/run_integration_validation.sh and
    // python/tests/test_multi_worker_async_benchmark.py). The tests below cover
    // the service-level configuration that the handler depends on.

    #[test]
    fn stream_checkpoint_artifact_service_has_no_storage_root_by_default() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        assert!(service.storage_root.is_none());
    }

    #[test]
    fn with_storage_root_sets_storage_root() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()))
            .with_storage_root(tmp.path().to_path_buf());
        assert!(service.storage_root.is_some());
        assert_eq!(service.storage_root.as_ref().unwrap().as_ref(), tmp.path());
    }

    #[test]
    fn with_storage_root_is_cloneable() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()))
            .with_storage_root(tmp.path().to_path_buf());
        let cloned = service.clone();
        assert_eq!(cloned.storage_root.as_ref().unwrap().as_ref(), tmp.path());
    }

    #[test]
    fn runtime_capabilities_includes_stream_checkpoint_artifact() {
        assert!(
            RUNTIME_CAPABILITIES.contains(&"stream-checkpoint-artifact"),
            "capabilities must advertise stream-checkpoint-artifact"
        );
    }

    // ── rendezvous ────────────────────────────────────────────────────────────

    async fn register_rendezvous_worker(
        service: &CoordinatorServiceImpl,
        run_id: &RunId,
        worker_id: &str,
    ) {
        service
            .register_worker(Request::new(RegisterWorkerRequest {
                idempotency_key: format!("rdzv-{worker_id}"),
                run_id: Some(run_id.clone()),
                worker_id: Some(WorkerId {
                    value: worker_id.to_string(),
                }),
                rank: 0,
                host: "worker-host".to_string(),
                sdk_version: "sdk-0.1.0".to_string(),
                protocol_version: "runtime.v1".to_string(),
            }))
            .await
            .expect("register_worker should succeed");
    }

    async fn join(
        service: &CoordinatorServiceImpl,
        run_id: &RunId,
        worker_id: &str,
        host: &str,
        store_port: u32,
    ) -> RendezvousInfo {
        service
            .join_rendezvous(Request::new(JoinRendezvousRequest {
                run_id: Some(run_id.clone()),
                worker_id: Some(WorkerId {
                    value: worker_id.to_string(),
                }),
                host: host.to_string(),
                store_port,
                previous_round: 0,
                idempotency_key: String::new(),
            }))
            .await
            .expect("join_rendezvous should succeed")
            .into_inner()
            .info
            .expect("join response should include info")
    }

    #[tokio::test]
    async fn join_rendezvous_completes_and_assigns_distinct_ranks() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run_with_world_size(&service, 2).await;
        register_rendezvous_worker(&service, &run_id, "w0").await;
        register_rendezvous_worker(&service, &run_id, "w1").await;

        let first = join(&service, &run_id, "w0", "10.0.0.1", 29501).await;
        assert_eq!(first.state, RendezvousRoundState::Waiting as i32);
        assert!(!first.has_rank);

        let second = join(&service, &run_id, "w1", "10.0.0.2", 29502).await;
        assert_eq!(second.state, RendezvousRoundState::Complete as i32);
        assert_eq!(second.world_size, 2);
        assert!(second.has_rank);

        let poll = service
            .get_rendezvous_state(Request::new(GetRendezvousStateRequest {
                run_id: Some(run_id.clone()),
                worker_id: Some(WorkerId {
                    value: "w0".to_string(),
                }),
            }))
            .await
            .expect("get_rendezvous_state should succeed")
            .into_inner()
            .info
            .expect("state response should include info");
        assert_eq!(poll.state, RendezvousRoundState::Complete as i32);
        assert!(poll.has_rank);
        assert_ne!(poll.rank, second.rank, "ranks must be distinct");
        // Store endpoint is the rank-0 participant's host/port.
        let expected = if poll.rank == 0 {
            ("10.0.0.1", 29501)
        } else {
            ("10.0.0.2", 29502)
        };
        assert_eq!(poll.store_addr, expected.0);
        assert_eq!(poll.store_port, expected.1);
        assert_eq!(poll.store_addr, second.store_addr);
    }

    #[tokio::test]
    async fn join_rendezvous_requires_registered_worker() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run_with_world_size(&service, 1).await;

        let status = service
            .join_rendezvous(Request::new(JoinRendezvousRequest {
                run_id: Some(run_id),
                worker_id: Some(WorkerId {
                    value: "ghost".to_string(),
                }),
                host: "10.0.0.9".to_string(),
                store_port: 0,
                previous_round: 0,
                idempotency_key: String::new(),
            }))
            .await
            .expect_err("join for unregistered worker must fail");
        assert_eq!(status.code(), Code::NotFound);
    }

    #[tokio::test]
    async fn join_rendezvous_requires_host() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run_with_world_size(&service, 1).await;
        register_rendezvous_worker(&service, &run_id, "w0").await;

        let status = service
            .join_rendezvous(Request::new(JoinRendezvousRequest {
                run_id: Some(run_id),
                worker_id: Some(WorkerId {
                    value: "w0".to_string(),
                }),
                host: String::new(),
                store_port: 0,
                previous_round: 0,
                idempotency_key: String::new(),
            }))
            .await
            .expect_err("join without host must fail");
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn heartbeat_reports_current_rendezvous_round() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run_with_world_size(&service, 1).await;
        register_rendezvous_worker(&service, &run_id, "w0").await;

        let before = service
            .heartbeat(Request::new(HeartbeatRequest {
                run_id: Some(run_id.clone()),
                worker_id: Some(WorkerId {
                    value: "w0".to_string(),
                }),
                idempotency_key: String::new(),
                client_time: None,
                active_shard_ids: vec![],
            }))
            .await
            .expect("heartbeat should succeed")
            .into_inner();
        assert_eq!(before.rendezvous_round, 0, "no rendezvous exists yet");

        let info = join(&service, &run_id, "w0", "10.0.0.1", 0).await;
        assert_eq!(info.state, RendezvousRoundState::Complete as i32);

        let after = service
            .heartbeat(Request::new(HeartbeatRequest {
                run_id: Some(run_id.clone()),
                worker_id: Some(WorkerId {
                    value: "w0".to_string(),
                }),
                idempotency_key: String::new(),
                client_time: None,
                active_shard_ids: vec![],
            }))
            .await
            .expect("heartbeat should succeed")
            .into_inner();
        assert_eq!(after.rendezvous_round, info.round);
    }

    #[tokio::test]
    async fn cancel_run_flows_through_heartbeat_and_blocks_new_workers() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run_with_world_size(&service, 2).await;
        register_rendezvous_worker(&service, &run_id, "w0").await;

        let cancelled = service
            .cancel_run(Request::new(CancelRunRequest {
                run_id: Some(run_id.clone()),
                reason: "test".to_string(),
                idempotency_key: String::new(),
            }))
            .await
            .expect("cancel_run should succeed")
            .into_inner();
        assert_eq!(cancelled.state, RunState::Cancelled as i32);

        let heartbeat = service
            .heartbeat(Request::new(HeartbeatRequest {
                run_id: Some(run_id.clone()),
                worker_id: Some(WorkerId {
                    value: "w0".to_string(),
                }),
                idempotency_key: String::new(),
                client_time: None,
                active_shard_ids: vec![],
            }))
            .await
            .expect("heartbeat still answered")
            .into_inner();
        assert_eq!(heartbeat.run_state, RunState::Cancelled as i32);

        let status = service
            .register_worker(Request::new(RegisterWorkerRequest {
                idempotency_key: String::new(),
                run_id: Some(run_id.clone()),
                worker_id: Some(WorkerId {
                    value: "late".to_string(),
                }),
                rank: 0,
                host: "h".to_string(),
                sdk_version: String::new(),
                protocol_version: String::new(),
            }))
            .await
            .expect_err("terminal runs must reject new workers");
        assert_eq!(status.code(), Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn deregister_worker_completes_run_when_roster_finishes() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let run_id = create_run_with_world_size(&service, 1).await;
        register_rendezvous_worker(&service, &run_id, "w0").await;

        let response = service
            .deregister_worker(Request::new(DeregisterWorkerRequest {
                run_id: Some(run_id.clone()),
                worker_id: Some(WorkerId {
                    value: "w0".to_string(),
                }),
                reason: DeregisterReason::Completed as i32,
                idempotency_key: String::new(),
            }))
            .await
            .expect("deregister should succeed")
            .into_inner();
        assert_eq!(response.state, WorkerState::Completed as i32);
        assert_eq!(response.run_state, RunState::Completed as i32);

        let status = service
            .get_run_status(Request::new(GetRunStatusRequest {
                run_id: Some(run_id.clone()),
            }))
            .await
            .expect("status should succeed")
            .into_inner();
        assert_eq!(status.state, RunState::Completed as i32);
        assert!(status.state_reason.contains("completed"));
    }

    #[tokio::test]
    async fn register_run_rejects_invalid_elastic_bounds() {
        let service = CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default()));
        let status = service
            .register_run(Request::new(RegisterRunRequest {
                idempotency_key: "elastic-invalid".to_string(),
                run_name: "elastic".to_string(),
                assignment_policy: AssignmentPolicy::StaticRank as i32,
                storage_backend_kind: StorageBackendKind::LocalFilesystem as i32,
                world_size: 4,
                run_version: "v1".to_string(),
                min_world_size: 3,
                max_world_size: 2,
                rendezvous_settle_timeout_ms: 0,
            }))
            .await
            .expect_err("min > max must be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
    }
}
