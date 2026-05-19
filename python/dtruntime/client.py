"""Minimal Python client skeleton for the Distributed Training Runtime coordinator."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from dtruntime.generated import runtime_pb2


class RuntimeClient:
    """Minimal gRPC client wrapper for the coordinator service.

    The high-level SDK methods for worker registration and heartbeat call the
    generated coordinator gRPC stub directly. Additional high-level SDK methods
    for datasets, shard leases, and checkpoints will be added in later Phase 4
    work. This class owns the channel and keeps the generated coordinator stub
    available for low-level use.
    """

    def __init__(self, address: str) -> None:
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

        self.address = address
        self._runtime_pb2 = runtime_pb2
        self._channel: Any = grpc.insecure_channel(address)
        self._stub = runtime_pb2_grpc.CoordinatorServiceStub(self._channel)
        self.stub = self._stub
        self._closed = False

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
        return self._stub.RegisterWorker(request)

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
        return self._stub.RegisterDataset(request)

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
    ) -> "runtime_pb2.GetShardAssignmentResponse":
        """Request shard assignments from the coordinator and return its response."""
        request = self._runtime_pb2.GetShardAssignmentRequest(
            run_id=self._runtime_pb2.RunId(value=run_id),
            worker_id=self._runtime_pb2.WorkerId(value=worker_id),
            dataset_id=self._runtime_pb2.DatasetId(value=dataset_id),
            idempotency_key=idempotency_key,
        )
        return self._stub.GetShardAssignment(request)

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
        return self._stub.BeginCheckpoint(request)


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
        return self._stub.ReportCheckpointArtifact(request)

    def commit_checkpoint(
        self,
        run_id: str,
        checkpoint_id: str,
        idempotency_key: str = "",
    ) -> "runtime_pb2.CommitCheckpointResponse":
        """Commit a checkpoint with the coordinator and return its response."""
        request = self._runtime_pb2.CommitCheckpointRequest(
            run_id=self._runtime_pb2.RunId(value=run_id),
            checkpoint_id=self._runtime_pb2.CheckpointId(value=checkpoint_id),
            idempotency_key=idempotency_key,
        )
        return self._stub.CommitCheckpoint(request)

    def restore_latest(
        self,
        run_id: str,
    ) -> "runtime_pb2.GetLatestCheckpointResponse":
        """Fetch latest committed checkpoint metadata from the coordinator."""
        request = self._runtime_pb2.GetLatestCheckpointRequest(
            run_id=self._runtime_pb2.RunId(value=run_id),
        )
        return self._stub.GetLatestCheckpoint(request)

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
        return self._stub.Heartbeat(request)

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

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> Optional[bool]:
        self.close()
        return None


def connect(address: str) -> RuntimeClient:
    """Create a RuntimeClient for the given coordinator address."""
    return RuntimeClient(address)
