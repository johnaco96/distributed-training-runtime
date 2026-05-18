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
