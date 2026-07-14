"""Unit tests for dtruntime.checkpointing (no coordinator, no process group)."""

from __future__ import annotations

import hashlib
import threading
import types
from pathlib import Path

import pytest

from dtruntime.checkpointing import (
    AsyncSaveHandle,
    CheckpointError,
    DistributedCheckpointer,
    _artifact_owner,
)


class FakeCtx:
    def __init__(self, client) -> None:
        self.client = client
        self.run_id = "run-1"
        self.worker_id = "w0"


def _checkpointer(client) -> DistributedCheckpointer:
    return DistributedCheckpointer(FakeCtx(client))


def test_artifact_owner_parses_storage_layout() -> None:
    uri = "/root/runs/run-1/checkpoints/ckpt-1/workers/node-0/__0_0.distcp"
    assert _artifact_owner(uri) == "node-0"
    # Windows separators and nested artifact names are handled too.
    uri = r"C:\store\runs\r\checkpoints\c\workers\host-3\sub\part.bin"
    assert _artifact_owner(uri) == "host-3"
    with pytest.raises(CheckpointError):
        _artifact_owner("/no/such/layout.bin")


def test_upload_retries_transient_failures_then_succeeds() -> None:
    class FlakyClient:
        def __init__(self) -> None:
            self.attempts = 0
            self.reported = []

        def stream_checkpoint_artifact(self, **kwargs):
            self.attempts += 1
            if self.attempts < 3:
                raise RuntimeError("transient upload failure")
            checksum = hashlib.sha256(kwargs["data"]).hexdigest()
            return types.SimpleNamespace(artifact_uri="/r/workers/w0/a", checksum=checksum)

        def report_checkpoint_artifact(self, **kwargs):
            self.reported.append(kwargs["artifact_name"])
            return types.SimpleNamespace(accepted=True)

    client = FlakyClient()
    checkpointer = _checkpointer(client)
    checkpointer._upload_artifact_with_retry("ckpt-1", "a.bin", b"payload",
        hashlib.sha256(b"payload").hexdigest())
    assert client.attempts == 3
    assert client.reported == ["a.bin"]


def test_upload_gives_up_after_max_attempts() -> None:
    class DeadClient:
        def stream_checkpoint_artifact(self, **kwargs):
            raise RuntimeError("storage down")

    checkpointer = _checkpointer(DeadClient())
    with pytest.raises(CheckpointError, match="after 3 attempts"):
        checkpointer._upload_artifact_with_retry("ckpt-1", "a.bin", b"x", "sum")


def test_upload_rejects_coordinator_checksum_mismatch() -> None:
    class LyingClient:
        def stream_checkpoint_artifact(self, **kwargs):
            return types.SimpleNamespace(artifact_uri="/u", checksum="different")

    checkpointer = _checkpointer(LyingClient())
    with pytest.raises(CheckpointError):
        checkpointer._upload_artifact_with_retry(
            "ckpt-1", "a.bin", b"x", hashlib.sha256(b"x").hexdigest()
        )


def _manifest(checkpoint_id: str, artifacts: list[tuple[str, str, int]]):
    return types.SimpleNamespace(
        checkpoint_id=types.SimpleNamespace(value=checkpoint_id),
        step=7,
        artifacts=[
            types.SimpleNamespace(
                artifact_name=name,
                artifact_uri=f"/store/runs/r/checkpoints/{checkpoint_id}/workers/w0/{name}",
                checksum=checksum,
                checksum_algorithm="sha256",
                size_bytes=size,
            )
            for name, checksum, size in artifacts
        ],
    )


def test_download_all_detects_checksum_mismatch(tmp_path: Path) -> None:
    class ServingClient:
        def fetch_checkpoint_artifact(self, destination, **kwargs):
            Path(destination).write_bytes(b"tampered")
            return len(b"tampered"), hashlib.sha256(b"tampered").hexdigest()

    checkpointer = _checkpointer(ServingClient())
    good = hashlib.sha256(b"original").hexdigest()
    reason = checkpointer._download_all(
        _manifest("ckpt-9", [("a.bin", good, len(b"original"))]), tmp_path
    )
    assert reason is not None and "checksum mismatch" in reason


def test_download_all_accepts_matching_artifacts(tmp_path: Path) -> None:
    payload = b"model-bytes"
    checksum = hashlib.sha256(payload).hexdigest()

    class ServingClient:
        def fetch_checkpoint_artifact(self, destination, **kwargs):
            Path(destination).write_bytes(payload)
            return len(payload), checksum

    checkpointer = _checkpointer(ServingClient())
    reason = checkpointer._download_all(
        _manifest("ckpt-9", [("a.bin", checksum, len(payload))]), tmp_path
    )
    assert reason is None
    assert (tmp_path / "a.bin").read_bytes() == payload


def test_async_handle_reports_errors_and_timeouts() -> None:
    handle = AsyncSaveHandle()
    with pytest.raises(TimeoutError):
        handle.result(timeout=0.01)

    handle._error = RuntimeError("boom")
    handle._done.set()
    with pytest.raises(CheckpointError):
        handle.result(timeout=1)

    ok = AsyncSaveHandle()
    ok.checkpoint_id = "ckpt-1"

    def finish() -> None:
        ok._done.set()

    threading.Timer(0.05, finish).start()
    assert ok.result(timeout=5) == "ckpt-1"
    assert ok.done()
