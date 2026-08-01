"""Python gRPC client for the Distributed Training Runtime coordinator.

Covers the full coordinator API — run/worker registration, heartbeats,
rendezvous, shard leasing, the two-phase checkpoint commit, and the admin
RPCs — over an optionally TLS/mTLS-secured channel, with bearer-token (OIDC)
or metadata-header caller identity attached to every call.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Sequence

if TYPE_CHECKING:
    from dtruntime.generated import runtime_pb2


class RuntimeClient:
    """gRPC client for the coordinator service.

    The high-level SDK entry points build on this class: dataset consumption
    lives in ``dtruntime.data`` (``DistributedShardDataset``), checkpointing
    in ``dtruntime.checkpointing`` / ``dtruntime.async_checkpoint``, and
    process-group setup in ``dtruntime.torch_dist``. This class owns the
    channel and keeps the generated coordinator stub available for
    low-level use.
    """

    def __init__(
        self,
        address: str,
        tls_ca_cert: str | Path | None = None,
        tls_client_cert: str | Path | None = None,
        tls_client_key: str | Path | None = None,
        auth_subject: str | None = None,
        auth_role: str | None = None,
        auth_worker_id: str | None = None,
        auth_token: str | None = None,
    ) -> None:
        """Create a client connected to a coordinator gRPC address."""
        try:
            import grpc

            from dtruntime.generated import runtime_pb2, runtime_pb2_grpc
        except ImportError as exc:
            raise RuntimeError(
                "RuntimeClient requires the grpcio runtime dependency and generated "
                "runtime_pb2/runtime_pb2_grpc modules. Install dtruntime with its "
                "runtime dependencies and regenerate protobuf files if needed."
            ) from exc

        self.address = _normalize_grpc_target(address)
        self._runtime_pb2 = runtime_pb2

        tls_ca_cert = tls_ca_cert or os.getenv("DTRUNTIME_TLS_CA_CERT")
        tls_client_cert = tls_client_cert or os.getenv("DTRUNTIME_TLS_CLIENT_CERT")
        tls_client_key = tls_client_key or os.getenv("DTRUNTIME_TLS_CLIENT_KEY")

        auth_subject = auth_subject or os.getenv("DTRUNTIME_AUTH_SUBJECT")
        auth_role = auth_role or os.getenv("DTRUNTIME_AUTH_ROLE")
        auth_worker_id = auth_worker_id or os.getenv("DTRUNTIME_AUTH_WORKER_ID")
        auth_token = auth_token or os.getenv("DTRUNTIME_AUTH_TOKEN")

        # Retain the *resolved* connection settings (arguments merged with the
        # environment), not the raw arguments. `with_auth` replays these into
        # derived clients, so anything stored before the fallback above would
        # be silently dropped from every client built by `for_worker`.
        self._tls_ca_cert = tls_ca_cert
        self._tls_client_cert = tls_client_cert
        self._tls_client_key = tls_client_key
        self._auth_token = auth_token
        self._auth_metadata: list[tuple[str, str]] = []
        # A bearer token (OIDC/JWT) is the strongest credential; when present it
        # is validated server-side and the dev metadata-identity headers are
        # ignored, so we only attach the ones the caller actually set.
        if auth_token:
            token = auth_token.strip()
            # Accept either a bare token or a full "Bearer <token>" value.
            if token.lower().startswith("bearer "):
                token = token[len("bearer "):].strip()
            self._auth_metadata.append(("authorization", f"Bearer {token}"))
        if auth_subject:
            self._auth_metadata.append(("x-dtruntime-subject", auth_subject))
        if auth_role:
            self._auth_metadata.append(("x-dtruntime-role", auth_role))
        if auth_worker_id:
            self._auth_metadata.append(("x-dtruntime-worker-id", auth_worker_id))

        if (tls_client_cert is None) != (tls_client_key is None):
            raise ValueError("tls_client_cert and tls_client_key must be provided together")

        if tls_ca_cert is None and tls_client_cert is None:
            self._channel = grpc.insecure_channel(self.address)
        else:
            root_certificates = _read_optional_pem(tls_ca_cert, "tls_ca_cert")
            certificate_chain = _read_optional_pem(tls_client_cert, "tls_client_cert")
            private_key = _read_optional_pem(tls_client_key, "tls_client_key")
            creds = grpc.ssl_channel_credentials(
                root_certificates=root_certificates,
                private_key=private_key,
                certificate_chain=certificate_chain,
            )
            self._channel = grpc.secure_channel(self.address, creds)
        self._stub = runtime_pb2_grpc.CoordinatorServiceStub(self._channel)
        self.stub = self._stub
        self._closed = False

    def with_auth(
        self,
        subject: str | None = None,
        role: str | None = None,
        worker_id: str | None = None,
    ) -> "RuntimeClient":
        """Create a new RuntimeClient sharing connection settings with a different identity.

        The bearer token carries over: it authenticates the *caller*, while the
        metadata headers describe which identity the caller is acting for. A
        derived client that dropped the token would be rejected outright by a
        coordinator running with auth enforced.
        """
        return RuntimeClient(
            self.address,
            tls_ca_cert=self._tls_ca_cert,
            tls_client_cert=self._tls_client_cert,
            tls_client_key=self._tls_client_key,
            auth_subject=subject,
            auth_role=role,
            auth_worker_id=worker_id,
            auth_token=self._auth_token,
        )

    def for_worker(
        self,
        worker_id: str,
        worker_role: str = "worker",
        worker_subject: str | None = None,
    ) -> "RuntimeClient":
        subject = worker_subject or worker_id
        return self.with_auth(subject=subject, role=worker_role, worker_id=worker_id)

    def rpc_metadata(
        self,
        worker_id: str | None = None,
        extra: Sequence[tuple[str, str]] | None = None,
    ) -> list[tuple[str, str]]:
        """Build call metadata for coordinator RPCs.

        Includes configured caller identity metadata when present and can
        optionally include worker-scoped metadata and extra call-specific
        metadata entries.
        """
        metadata = list(self._auth_metadata)
        has_identity_metadata = any(key in {"x-dtruntime-subject", "x-dtruntime-role", "x-dtruntime-worker-id"} for key, _ in metadata)
        if worker_id and has_identity_metadata and not any(key == "x-dtruntime-worker-id" for key, _ in metadata):
            metadata.append(("x-dtruntime-worker-id", worker_id))
        if extra:
            metadata.extend(extra)
        return metadata

    def register_run(
        self,
        run_name: str,
        world_size: int,
        assignment_policy: int,
        storage_backend_kind: int,
        run_version: str = "",
        idempotency_key: str = "",
    ) -> "runtime_pb2.RegisterRunResponse":
        """Register a run with the coordinator and return its response."""
        request = self._runtime_pb2.RegisterRunRequest(
            idempotency_key=idempotency_key,
            run_name=run_name,
            assignment_policy=assignment_policy,
            storage_backend_kind=storage_backend_kind,
            world_size=world_size,
            run_version=run_version,
        )
        return self._stub.RegisterRun(request, metadata=self.rpc_metadata())

    def register_worker(
        self,
        run_id: str,
        worker_id: str = "",
        rank: int = 0,
        host: str = "",
        sdk_version: str = "0.1.0",
        protocol_version: str = "runtime.v1",
        idempotency_key: str = "",
    ) -> "runtime_pb2.RegisterWorkerResponse":
        """Register a worker with the coordinator and return its response."""
        request = self._runtime_pb2.RegisterWorkerRequest(
            idempotency_key=idempotency_key,
            run_id=self._runtime_pb2.RunId(value=run_id),
            worker_id=self._runtime_pb2.WorkerId(value=worker_id),
            rank=rank,
            host=host,
            sdk_version=sdk_version,
            protocol_version=protocol_version,
        )
        return self._stub.RegisterWorker(request, metadata=self.rpc_metadata(worker_id=worker_id))

    def register_dataset(
        self,
        run_id: str,
        name: str,
        uri: str,
        format: str,
        shards: Optional[list[dict[str, Any]]] = None,
        dataset_id: str = "",
        dataset_version: str = "",
        labels: Optional[dict[str, str]] = None,
        idempotency_key: str = "",
    ) -> "runtime_pb2.RegisterDatasetResponse":
        """Register dataset metadata with the coordinator and return its response."""
        shard_metadata = [
            self._shard_metadata_from_dict(shard, dataset_id) for shard in shards or []
        ]
        request = self._runtime_pb2.RegisterDatasetRequest(
            idempotency_key=idempotency_key,
            run_id=self._runtime_pb2.RunId(value=run_id),
            dataset=self._runtime_pb2.DatasetMetadata(
                dataset_id=self._runtime_pb2.DatasetId(value=dataset_id),
                name=name,
                dataset_uri=uri,
                format=format,
                shard_count=len(shard_metadata),
                dataset_version=dataset_version,
                labels=labels or {},
            ),
            shards=shard_metadata,
        )
        return self._stub.RegisterDataset(request, metadata=self.rpc_metadata())

    def _shard_metadata_from_dict(
        self, shard: dict[str, Any], dataset_id: str
    ) -> "runtime_pb2.ShardMetadata":
        """Convert a high-level shard dictionary into generated shard metadata."""
        return self._runtime_pb2.ShardMetadata(
            shard_id=self._runtime_pb2.ShardId(value=shard.get("shard_id", "")),
            dataset_id=self._runtime_pb2.DatasetId(value=dataset_id),
            shard_uri=shard.get("shard_uri", shard.get("uri", "")),
            byte_range_start=shard.get("byte_range_start", 0),
            byte_range_end=shard.get("byte_range_end", 0),
            size_bytes=shard.get("size_bytes", 0),
            checksum=shard.get("checksum", ""),
            checksum_algorithm=shard.get("checksum_algorithm", ""),
        )

    def request_shard(
        self,
        run_id: str,
        worker_id: str,
        dataset_id: str,
        idempotency_key: str = "",
        cached_shard_ids: Optional[list[str]] = None,
    ) -> "runtime_pb2.GetShardAssignmentResponse":
        """Request shard assignments from the coordinator and return its response."""
        request = self._runtime_pb2.GetShardAssignmentRequest(
            run_id=self._runtime_pb2.RunId(value=run_id),
            worker_id=self._runtime_pb2.WorkerId(value=worker_id),
            dataset_id=self._runtime_pb2.DatasetId(value=dataset_id),
            idempotency_key=idempotency_key,
        )
        extra_metadata: list[tuple[str, str]] = []
        if cached_shard_ids:
            extra_metadata.append(("x-dtruntime-cached-shards", ",".join(cached_shard_ids)))
        return self._stub.GetShardAssignment(
            request,
            metadata=self.rpc_metadata(worker_id=worker_id, extra=extra_metadata),
        )

    def report_shard_progress(
        self,
        run_id: str,
        worker_id: str,
        dataset_id: str,
        shard_id: str,
        lease_epoch: int,
        progress_offset: int,
        is_complete: bool = False,
        idempotency_key: str = "",
    ) -> "runtime_pb2.ReportShardProgressResponse":
        """Report how far this worker has read into its assigned shard."""
        request = self._runtime_pb2.ReportShardProgressRequest(
            run_id=self._runtime_pb2.RunId(value=run_id),
            worker_id=self._runtime_pb2.WorkerId(value=worker_id),
            dataset_id=self._runtime_pb2.DatasetId(value=dataset_id),
            shard_id=self._runtime_pb2.ShardId(value=shard_id),
            lease_epoch=lease_epoch,
            progress_offset=progress_offset,
            is_complete=is_complete,
            idempotency_key=idempotency_key,
        )
        return self._stub.ReportShardProgress(
            request, metadata=self.rpc_metadata(worker_id=worker_id)
        )

    def begin_checkpoint(
        self,
        run_id: str,
        worker_id: str,
        step: int,
        checkpoint_id: str = "",
        idempotency_key: str = "",
    ) -> "runtime_pb2.BeginCheckpointResponse":
        """Begin a checkpoint with the coordinator and return its response."""
        request = self._runtime_pb2.BeginCheckpointRequest(
            run_id=self._runtime_pb2.RunId(value=run_id),
            worker_id=self._runtime_pb2.WorkerId(value=worker_id),
            checkpoint_id=self._runtime_pb2.CheckpointId(value=checkpoint_id),
            step=step,
            idempotency_key=idempotency_key,
        )
        return self._stub.BeginCheckpoint(request, metadata=self.rpc_metadata(worker_id=worker_id))


    def stream_checkpoint_artifact(
        self,
        run_id: str,
        worker_id: str,
        checkpoint_id: str,
        artifact_name: str,
        data: bytes,
        chunk_size: int = 512 * 1024,
        idempotency_key: str = "",
    ) -> "runtime_pb2.StreamCheckpointArtifactResponse":
        """Stream checkpoint artifact bytes to the coordinator for async storage.

        The coordinator writes chunks to storage using its tokio async runtime as
        they arrive — the caller's training loop is unblocked as soon as the stream
        is accepted, while Rust handles all I/O concurrently.
        """
        total_size = len(data)

        def _generate_requests():
            yield self._runtime_pb2.StreamCheckpointArtifactRequest(
                meta=self._runtime_pb2.StreamCheckpointArtifactMeta(
                    run_id=self._runtime_pb2.RunId(value=run_id),
                    worker_id=self._runtime_pb2.WorkerId(value=worker_id),
                    checkpoint_id=self._runtime_pb2.CheckpointId(value=checkpoint_id),
                    artifact_name=artifact_name,
                    total_size_bytes=total_size,
                    idempotency_key=idempotency_key,
                )
            )
            for sequence, offset in enumerate(range(0, total_size, chunk_size)):
                yield self._runtime_pb2.StreamCheckpointArtifactRequest(
                    chunk=self._runtime_pb2.StreamCheckpointArtifactChunk(
                        data=data[offset : offset + chunk_size],
                        sequence=sequence,
                    )
                )

        return self._stub.StreamCheckpointArtifact(
            _generate_requests(),
            metadata=self.rpc_metadata(worker_id=worker_id),
        )

    def report_checkpoint_artifact(
        self,
        run_id: str,
        worker_id: str,
        checkpoint_id: str,
        artifact_name: str,
        artifact_uri: str = "",
        size_bytes: int = 0,
        checksum: str = "",
        checksum_algorithm: str = "",
        idempotency_key: str = "",
    ) -> "runtime_pb2.ReportCheckpointArtifactResponse":
        """Report checkpoint artifact metadata to the coordinator."""
        request = self._runtime_pb2.ReportCheckpointArtifactRequest(
            run_id=self._runtime_pb2.RunId(value=run_id),
            worker_id=self._runtime_pb2.WorkerId(value=worker_id),
            checkpoint_id=self._runtime_pb2.CheckpointId(value=checkpoint_id),
            artifact_report=self._runtime_pb2.ArtifactReport(
                artifact_name=artifact_name,
                artifact_uri=artifact_uri,
                size_bytes=size_bytes,
                checksum=checksum,
                checksum_algorithm=checksum_algorithm,
            ),
            idempotency_key=idempotency_key,
        )
        return self._stub.ReportCheckpointArtifact(request, metadata=self.rpc_metadata(worker_id=worker_id))

    def commit_checkpoint(
        self,
        run_id: str,
        checkpoint_id: str,
        idempotency_key: str = "",
        worker_id: str = "",
    ) -> "runtime_pb2.CommitCheckpointResponse":
        """Commit a checkpoint with the coordinator and return its response.

        When ``worker_id`` is provided the coordinator records it as a 2PC
        commit vote and only performs the global commit once every expected
        worker has voted; the response's ``globally_committed`` flag reports
        whether the global commit has happened.  An empty ``worker_id`` keeps
        the legacy single-caller behaviour (rank 0 commits on behalf of all).
        """
        request = self._runtime_pb2.CommitCheckpointRequest(
            run_id=self._runtime_pb2.RunId(value=run_id),
            checkpoint_id=self._runtime_pb2.CheckpointId(value=checkpoint_id),
            idempotency_key=idempotency_key,
        )
        if worker_id:
            request.worker_id.value = worker_id
        return self._stub.CommitCheckpoint(
            request, metadata=self.rpc_metadata(worker_id=worker_id or None)
        )

    def restore_latest(
        self,
        run_id: str,
    ) -> "runtime_pb2.GetLatestCheckpointResponse":
        """Fetch latest committed checkpoint metadata from the coordinator."""
        request = self._runtime_pb2.GetLatestCheckpointRequest(
            run_id=self._runtime_pb2.RunId(value=run_id),
        )
        return self._stub.GetLatestCheckpoint(request, metadata=self.rpc_metadata())

    def get_runtime_info(
        self, timeout: float | None = None
    ) -> "runtime_pb2.GetRuntimeInfoResponse":
        """Fetch the coordinator's protocol version and advertised capabilities.

        Used as a lightweight liveness/handshake probe (``dtruntime-admin
        doctor``): a successful response confirms the coordinator is reachable
        and negotiates protocol compatibility.
        """
        request = self._runtime_pb2.GetRuntimeInfoRequest()
        return self._stub.GetRuntimeInfo(
            request, metadata=self.rpc_metadata(), timeout=timeout
        )

    def list_checkpoints(
        self,
        run_id: str,
        committed_only: bool = False,
        limit: int = 0,
    ) -> "runtime_pb2.ListCheckpointsResponse":
        """List checkpoints for a run, newest step first."""
        request = self._runtime_pb2.ListCheckpointsRequest(
            run_id=self._runtime_pb2.RunId(value=run_id),
            committed_only=committed_only,
            limit=limit,
        )
        return self._stub.ListCheckpoints(request, metadata=self.rpc_metadata())

    def report_checkpoint_corrupt(
        self,
        run_id: str,
        worker_id: str,
        checkpoint_id: str,
        reason: str,
        idempotency_key: str = "",
    ) -> "runtime_pb2.ReportCheckpointCorruptResponse":
        """Mark a checkpoint corrupt; the latest pointer falls back to the
        newest remaining committed checkpoint."""
        request = self._runtime_pb2.ReportCheckpointCorruptRequest(
            run_id=self._runtime_pb2.RunId(value=run_id),
            worker_id=self._runtime_pb2.WorkerId(value=worker_id),
            checkpoint_id=self._runtime_pb2.CheckpointId(value=checkpoint_id),
            reason=reason,
            idempotency_key=idempotency_key,
        )
        return self._stub.ReportCheckpointCorrupt(
            request, metadata=self.rpc_metadata(worker_id=worker_id)
        )

    def fetch_checkpoint_artifact(
        self,
        run_id: str,
        worker_id: str,
        checkpoint_id: str,
        artifact_name: str,
        owner_worker_id: str,
        destination: "str | Path",
    ) -> tuple[int, str]:
        """Download one checkpoint artifact through the coordinator into
        ``destination``.  Returns (size_bytes, checksum) as computed by the
        coordinator while streaming; callers verify against the manifest."""
        request = self._runtime_pb2.FetchCheckpointArtifactRequest(
            run_id=self._runtime_pb2.RunId(value=run_id),
            worker_id=self._runtime_pb2.WorkerId(value=worker_id),
            checkpoint_id=self._runtime_pb2.CheckpointId(value=checkpoint_id),
            artifact_name=artifact_name,
            owner_worker_id=self._runtime_pb2.WorkerId(value=owner_worker_id),
        )
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        total = 0
        checksum = ""
        with destination.open("wb") as out:
            for chunk in self._stub.FetchCheckpointArtifact(
                request, metadata=self.rpc_metadata(worker_id=worker_id)
            ):
                if chunk.data:
                    out.write(chunk.data)
                    total += len(chunk.data)
                if chunk.last:
                    checksum = chunk.checksum
        return total, checksum

    def join_rendezvous(
        self,
        run_id: str,
        worker_id: str,
        host: str,
        store_port: int = 0,
        previous_round: int = 0,
        idempotency_key: str = "",
    ) -> "runtime_pb2.JoinRendezvousResponse":
        """Join the rendezvous barrier for torch.distributed process-group formation.

        ``host`` must be reachable by the other workers; if this worker is
        elected rank 0 it hosts the c10d TCPStore on ``store_port`` (0 lets the
        coordinator substitute the conventional default, 29500).
        """
        request = self._runtime_pb2.JoinRendezvousRequest(
            run_id=self._runtime_pb2.RunId(value=run_id),
            worker_id=self._runtime_pb2.WorkerId(value=worker_id),
            host=host,
            store_port=store_port,
            previous_round=previous_round,
            idempotency_key=idempotency_key,
        )
        return self._stub.JoinRendezvous(request, metadata=self.rpc_metadata(worker_id=worker_id))

    def get_rendezvous_state(
        self,
        run_id: str,
        worker_id: str,
    ) -> "runtime_pb2.GetRendezvousStateResponse":
        """Poll the rendezvous state for this worker's run."""
        request = self._runtime_pb2.GetRendezvousStateRequest(
            run_id=self._runtime_pb2.RunId(value=run_id),
            worker_id=self._runtime_pb2.WorkerId(value=worker_id),
        )
        return self._stub.GetRendezvousState(
            request, metadata=self.rpc_metadata(worker_id=worker_id)
        )

    def deregister_worker(
        self,
        run_id: str,
        worker_id: str,
        reason: int = 0,
        idempotency_key: str = "",
    ) -> "runtime_pb2.DeregisterWorkerResponse":
        """Gracefully deregister a worker (completed, preempted, or error).

        Takes effect immediately: the rendezvous membership changes without
        waiting out the heartbeat failure deadline, and the run transitions to
        COMPLETED once every worker has deregistered as completed.
        """
        request = self._runtime_pb2.DeregisterWorkerRequest(
            run_id=self._runtime_pb2.RunId(value=run_id),
            worker_id=self._runtime_pb2.WorkerId(value=worker_id),
            reason=reason,
            idempotency_key=idempotency_key,
        )
        return self._stub.DeregisterWorker(
            request, metadata=self.rpc_metadata(worker_id=worker_id)
        )

    def cancel_run(
        self,
        run_id: str,
        reason: str = "",
        idempotency_key: str = "",
    ) -> "runtime_pb2.CancelRunResponse":
        """Cancel a run (admin).  Terminal and idempotent; workers observe the
        state on their next heartbeat and exit."""
        request = self._runtime_pb2.CancelRunRequest(
            run_id=self._runtime_pb2.RunId(value=run_id),
            reason=reason,
            idempotency_key=idempotency_key,
        )
        return self._stub.CancelRun(request, metadata=self.rpc_metadata())

    def get_run_status(
        self, run_id: str, timeout: float | None = None
    ) -> "runtime_pb2.GetRunStatusResponse":
        """Fetch run status including the lifecycle state (admin)."""
        request = self._runtime_pb2.GetRunStatusRequest(
            run_id=self._runtime_pb2.RunId(value=run_id),
        )
        return self._stub.GetRunStatus(
            request, metadata=self.rpc_metadata(), timeout=timeout
        )

    def _run_action(
        self, rpc, run_id: str, reason: str, idempotency_key: str, timeout: float | None
    ) -> "runtime_pb2.RunActionResponse":
        request = self._runtime_pb2.RunActionRequest(
            run_id=self._runtime_pb2.RunId(value=run_id),
            reason=reason,
            idempotency_key=idempotency_key,
        )
        return rpc(request, metadata=self.rpc_metadata(), timeout=timeout)

    def pause_run(
        self,
        run_id: str,
        reason: str = "",
        idempotency_key: str = "",
        timeout: float | None = None,
    ) -> "runtime_pb2.RunActionResponse":
        """Pause a PENDING/RUNNING/RECOVERING run (admin).

        The run stops making progress but is not terminal; resume returns it to
        service.  Fails with FAILED_PRECONDITION if the run is not in a pausable
        state.
        """
        return self._run_action(self._stub.PauseRun, run_id, reason, idempotency_key, timeout)

    def resume_run(
        self,
        run_id: str,
        reason: str = "",
        idempotency_key: str = "",
        timeout: float | None = None,
    ) -> "runtime_pb2.RunActionResponse":
        """Resume a PAUSED run (admin).  Returns to RUNNING when workers are
        still connected, otherwise PENDING so a fresh launch can form the
        process group."""
        return self._run_action(self._stub.ResumeRun, run_id, reason, idempotency_key, timeout)

    def retry_run(
        self,
        run_id: str,
        reason: str = "",
        idempotency_key: str = "",
        timeout: float | None = None,
    ) -> "runtime_pb2.RunActionResponse":
        """Retry a FAILED run (admin): reset to PENDING, bump the retry counter,
        and clear the stale worker roster so a fresh launch resumes from the
        latest committed checkpoint."""
        return self._run_action(self._stub.RetryRun, run_id, reason, idempotency_key, timeout)

    def drain_run(
        self,
        run_id: str,
        reason: str = "",
        idempotency_key: str = "",
        timeout: float | None = None,
    ) -> "runtime_pb2.RunActionResponse":
        """Drain a run (admin): move it to DRAINING so workers finish current
        work, checkpoint, and exit gracefully; the run completes when the roster
        empties."""
        return self._run_action(self._stub.DrainRun, run_id, reason, idempotency_key, timeout)

    def list_runs(
        self, state_filter: int = 0, timeout: float | None = None
    ) -> "runtime_pb2.ListRunsResponse":
        """List runs (admin), optionally filtered to one lifecycle state.

        ``state_filter=0`` (RUN_STATE_UNSPECIFIED) returns all runs.
        """
        request = self._runtime_pb2.ListRunsRequest(state_filter=state_filter)
        return self._stub.ListRuns(request, metadata=self.rpc_metadata(), timeout=timeout)

    def list_workers(
        self, run_id: str, state_filter: int = 0, timeout: float | None = None
    ) -> "runtime_pb2.ListWorkersResponse":
        """List a run's workers (admin), optionally filtered to one worker
        state (``state_filter=0`` returns all)."""
        request = self._runtime_pb2.ListWorkersRequest(
            run_id=self._runtime_pb2.RunId(value=run_id),
            state_filter=state_filter,
        )
        return self._stub.ListWorkers(request, metadata=self.rpc_metadata(), timeout=timeout)

    def list_shard_progress(
        self, run_id: str, dataset_id: str = "", timeout: float | None = None
    ) -> "runtime_pb2.ListShardProgressResponse":
        """List shard-progress / lease records for a run (admin), optionally
        restricted to one dataset (empty ``dataset_id`` = all datasets)."""
        request = self._runtime_pb2.ListShardProgressRequest(
            run_id=self._runtime_pb2.RunId(value=run_id),
            dataset_id=self._runtime_pb2.DatasetId(value=dataset_id) if dataset_id else None,
        )
        return self._stub.ListShardProgress(
            request, metadata=self.rpc_metadata(), timeout=timeout
        )

    def heartbeat(
        self,
        run_id: str,
        worker_id: str,
        active_shard_ids: Optional[list[str]] = None,
        idempotency_key: str = "",
    ) -> "runtime_pb2.HeartbeatResponse":
        """Send a worker heartbeat to the coordinator and return its response."""
        request = self._runtime_pb2.HeartbeatRequest(
            run_id=self._runtime_pb2.RunId(value=run_id),
            worker_id=self._runtime_pb2.WorkerId(value=worker_id),
            idempotency_key=idempotency_key,
            active_shard_ids=[
                self._runtime_pb2.ShardId(value=shard_id)
                for shard_id in active_shard_ids or []
            ],
        )
        return self._stub.Heartbeat(request, metadata=self.rpc_metadata(worker_id=worker_id))

    def close(self) -> None:
        """Close the underlying gRPC channel."""
        if self._closed:
            return

        close = getattr(self._channel, "close", None)
        if close is not None:
            close()
        self._closed = True

    def __enter__(self) -> "RuntimeClient":
        return self

    def __exit__(
        self, _exc_type: object, _exc: object, _traceback: object
    ) -> Optional[bool]:
        self.close()
        return None


def connect(address: str) -> RuntimeClient:
    """Create a RuntimeClient for the given coordinator address."""
    return RuntimeClient(address)


def _read_optional_pem(path_value: str | Path | None, label: str) -> bytes | None:
    if path_value is None:
        return None
    path = Path(path_value)
    try:
        return path.read_bytes()
    except OSError as exc:
        raise ValueError(f"failed to read {label} file at {path}: {exc}") from exc


def _normalize_grpc_target(address: str) -> str:
    value = address.strip()
    if value.startswith("http://") or value.startswith("https://"):
        value = value.split("://", 1)[1]
    return value.rstrip("/")
