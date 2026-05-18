"""Minimal Python client skeleton for the Distributed Training Runtime coordinator."""

from __future__ import annotations

from typing import Any, Optional


class RuntimeClient:
    """Minimal gRPC client wrapper for the coordinator service.

    The high-level SDK methods for runs, workers, datasets, shard leases, and
    checkpoints will be added in later Phase 4 work. For now, this class owns
    the channel and exposes the generated coordinator stub for low-level use.
    """

    def __init__(self, address: str) -> None:
        """Create a client connected to a coordinator gRPC address."""
        try:
            import grpc
            from dtruntime.generated import runtime_pb2_grpc
        except ImportError as exc:
            raise RuntimeError(
                "RuntimeClient requires the grpcio runtime dependency and generated "
                "runtime_pb2_grpc module. Install dtruntime with its runtime "
                "dependencies and regenerate protobuf files if needed."
            ) from exc

        self.address = address
        self._channel: Any = grpc.insecure_channel(address)
        self.stub = runtime_pb2_grpc.CoordinatorServiceStub(self._channel)
        self._closed = False

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
