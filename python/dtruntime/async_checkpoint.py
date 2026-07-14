"""Staged asynchronous checkpointing — the "stall killer" pipeline.

Implements hierarchical checkpoint data movement so the training loop only
blocks for the in-memory snapshot (stage 0); serialization, local durability,
remote upload, and the coordinator's 2PC commit all happen on a background
thread:

    stage 0  snapshot        state dict → host-memory copy      (BLOCKING)
    stage 1  local durability serialize + fsync to staging dir   (async)
    stage 2  remote durability upload to S3/MinIO or file dest   (async)
    stage 3  commit           report artifact + 2PC commit vote  (async)

The only time the caller waits is stage 0 — that duration is exposed as
``handle.t_stall_s`` and is the headline "checkpoint stall" metric.  Stage 2
prefers the Rust ``runtime-uploader`` binary (streaming upload, no Python in
the data path) and falls back to pure Python when it is not built.

The local staged copy doubles as a restore fast path: ``restore_latest``
loads it directly when its SHA-256 matches the committed manifest, avoiding
the remote round-trip entirely.

Usage::

    ckptr = AsyncCheckpointer(client, run_id, worker_id, staging_dir)
    handle = ckptr.save(model.state_dict(), step=global_step)  # ~ms block
    ...training continues...
    handle.wait()                      # only if you need the commit barrier
    state, info = ckptr.restore_latest()
"""

from __future__ import annotations

import copy
import json
import os
import pickle
import queue
import shutil
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

# One shared implementation with data.cache and friends.
from ._util import file_uri_to_path as _file_uri_to_path
from ._util import make_s3_client as _make_s3_client
from ._util import parse_s3_uri as _parse_s3_uri
from ._util import sha256_file as _sha256_file

# Deliberately the same class as the DCP path: CheckpointError must NOT be a
# RuntimeError, or run_elastic would misclassify a failed checkpoint as a
# collective/membership error and tear down the process group.
from .checkpointing import CheckpointError

__all__ = ["AsyncCheckpointer", "CheckpointHandle", "CheckpointError"]

_SENTINEL = object()


def _torch():
    try:
        import torch

        return torch
    except ImportError:
        return None


def _snapshot(value: Any) -> Any:
    """Stage 0: deep-copy *value* with tensors detached and cloned to CPU.

    This is the only work done on the caller's thread.  For GPU tensors the
    clone is the device→host transfer; everything downstream operates on the
    snapshot so training can mutate the live parameters immediately.
    """
    torch = _torch()
    if torch is not None and torch.is_tensor(value):
        return value.detach().to("cpu", copy=True)
    if isinstance(value, dict):
        return {k: _snapshot(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        seq = [_snapshot(v) for v in value]
        return type(value)(seq) if isinstance(value, tuple) else seq
    return copy.deepcopy(value)


def _serialize_to(path: Path, state: Any) -> None:
    """Stage 1: write *state* atomically (tmp + fsync + rename)."""
    torch = _torch()
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "wb") as f:
        if torch is not None:
            torch.save(state, f)
        else:
            pickle.dump(state, f, protocol=pickle.HIGHEST_PROTOCOL)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _find_uploader_bin() -> Optional[Path]:
    """Locate the Rust runtime-uploader binary, if built."""
    env = os.environ.get("DTRUNTIME_UPLOADER_BIN")
    if env:
        p = Path(env)
        return p if p.is_file() else None
    exe = ".exe" if os.name == "nt" else ""
    repo_root = Path(__file__).resolve().parent.parent.parent
    for profile in ("release", "debug"):
        p = repo_root / "target" / profile / f"runtime-uploader{exe}"
        if p.is_file():
            return p
    found = shutil.which("runtime-uploader")
    return Path(found) if found else None


@dataclass
class CheckpointHandle:
    """Tracks one staged checkpoint through the pipeline."""

    checkpoint_id: str
    step: int
    artifact_name: str
    staging_path: Path
    #: Seconds the training thread was blocked (stage 0 snapshot).
    t_stall_s: float = 0.0
    #: Seconds from save() until the staged file was fsynced locally.
    t_local_s: Optional[float] = None
    #: Seconds from save() until this worker's commit vote was accepted.
    t_committed_s: Optional[float] = None
    #: True once every expected worker's vote landed (2PC global commit).
    globally_committed: bool = False
    uri: str = ""
    sha256: str = ""
    size_bytes: int = 0
    error: Optional[BaseException] = None
    _done: threading.Event = field(default_factory=threading.Event, repr=False)

    def wait(self, timeout: Optional[float] = None) -> "CheckpointHandle":
        """Block until the pipeline finishes (or raise on failure/timeout)."""
        if not self._done.wait(timeout):
            raise TimeoutError(
                f"checkpoint {self.checkpoint_id} still in flight after {timeout}s"
            )
        if self.error is not None:
            raise CheckpointError(
                f"checkpoint {self.checkpoint_id} failed: {self.error}"
            ) from self.error
        return self

    @property
    def done(self) -> bool:
        return self._done.is_set()


class AsyncCheckpointer:
    """Staged async checkpoint writer bound to one (run, worker).

    Args:
        client: a connected ``RuntimeClient``.
        run_id / worker_id: identifiers from registration.
        staging_dir: local fast disk (NVMe in production) for stage 1.
        dest: remote destination prefix — ``s3://bucket/prefix`` or a
            ``file://`` / plain directory path.  ``None`` keeps artifacts
            local-only (the staged file itself is the artifact URI).
        uploader_bin: path to runtime-uploader; auto-detected when ``None``.
    """

    def __init__(
        self,
        client: Any,
        run_id: str,
        worker_id: str,
        staging_dir: str | Path,
        dest: Optional[str] = None,
        uploader_bin: Optional[str | Path] = None,
        weights_only: bool = True,
    ) -> None:
        self._client = client
        self.run_id = run_id
        self.worker_id = worker_id
        self.staging_dir = Path(staging_dir)
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self.dest = dest.rstrip("/") if dest else None
        self._uploader_bin = (
            Path(uploader_bin) if uploader_bin else _find_uploader_bin()
        )
        # Restores use torch.load(weights_only=True) by default; only disable
        # for state dicts that contain non-tensor Python objects you trust.
        self.weights_only = weights_only
        self._queue: "queue.Queue[Any]" = queue.Queue()
        self._worker = threading.Thread(
            target=self._run, name=f"dtr-ckpt-{worker_id}", daemon=True
        )
        self._worker.start()

    # ── public API ──────────────────────────────────────────────────────────

    def save(
        self, state: Any, step: int, checkpoint_id: Optional[str] = None
    ) -> CheckpointHandle:
        """Snapshot *state* (blocking) and stage everything else async.

        All workers of a run may call this with the same *step*; they join
        the same coordinator checkpoint and each contributes one artifact
        plus one 2PC commit vote.
        """
        ckpt_id = checkpoint_id or f"ckpt-step-{step:010d}"
        artifact_name = f"{ckpt_id}-{self.worker_id}.ckpt"
        handle = CheckpointHandle(
            checkpoint_id=ckpt_id,
            step=step,
            artifact_name=artifact_name,
            staging_path=self.staging_dir / artifact_name,
        )
        t0 = time.perf_counter()
        snapshot = _snapshot(state)
        handle.t_stall_s = time.perf_counter() - t0
        self._queue.put((handle, snapshot, t0))
        return handle

    def restore_latest(self) -> tuple[Any, dict]:
        """Load this worker's artifact from the latest committed checkpoint.

        Fast path: the local staged copy, when its SHA-256 matches the
        committed manifest.  Fallback: download/copy from the artifact URI.
        Returns ``(state, info)`` where info carries checkpoint_id/step/uri.
        """
        resp = self._client.restore_latest(run_id=self.run_id)
        manifest = resp.manifest
        suffix = f"-{self.worker_id}.ckpt"
        artifact = next(
            (a for a in manifest.artifacts if a.artifact_name.endswith(suffix)),
            None,
        )
        if artifact is None:
            raise CheckpointError(
                f"latest checkpoint {manifest.checkpoint_id.value} has no "
                f"artifact for worker {self.worker_id}"
            )
        info = {
            "checkpoint_id": manifest.checkpoint_id.value,
            "step": manifest.step,
            "artifact_uri": artifact.artifact_uri,
            "restored_from": "remote",
        }
        local = self.staging_dir / artifact.artifact_name
        if (
            local.is_file()
            and artifact.checksum
            and _sha256_file(local) == artifact.checksum
        ):
            info["restored_from"] = "local-staging"
            return self._load(local), info

        fetched = self._fetch(artifact.artifact_uri, artifact.artifact_name)
        if artifact.checksum:
            actual = _sha256_file(fetched)
            if actual != artifact.checksum:
                raise CheckpointError(
                    f"checksum mismatch for {artifact.artifact_uri}: "
                    f"manifest {artifact.checksum} != downloaded {actual}"
                )
        return self._load(fetched), info

    def close(self, timeout: Optional[float] = 60.0) -> None:
        """Drain the pipeline and stop the background thread."""
        self._queue.put(_SENTINEL)
        self._worker.join(timeout)

    def __enter__(self) -> "AsyncCheckpointer":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ── background pipeline ─────────────────────────────────────────────────

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            if item is _SENTINEL:
                return
            handle, snapshot, t0 = item
            try:
                self._process(handle, snapshot, t0)
            except BaseException as exc:  # surfaced via handle.wait()
                handle.error = exc
            finally:
                handle._done.set()

    def _process(self, handle: CheckpointHandle, snapshot: Any, t0: float) -> None:
        # Join (or open) the coordinator checkpoint.  Every worker may begin
        # the same checkpoint_id at the same step — the coordinator returns
        # the in-progress record for late joiners.
        self._client.begin_checkpoint(
            run_id=self.run_id,
            worker_id=self.worker_id,
            step=handle.step,
            checkpoint_id=handle.checkpoint_id,
            idempotency_key=f"begin-{handle.checkpoint_id}-{self.worker_id}",
        )

        # Stage 1: local durability.
        _serialize_to(handle.staging_path, snapshot)
        del snapshot
        handle.sha256 = _sha256_file(handle.staging_path)
        handle.size_bytes = handle.staging_path.stat().st_size
        handle.t_local_s = time.perf_counter() - t0

        # Stage 2: remote durability.
        handle.uri = self._upload(handle)

        # Stage 3: metadata + 2PC vote.
        self._client.report_checkpoint_artifact(
            run_id=self.run_id,
            worker_id=self.worker_id,
            checkpoint_id=handle.checkpoint_id,
            artifact_name=handle.artifact_name,
            artifact_uri=handle.uri,
            size_bytes=handle.size_bytes,
            checksum=handle.sha256,
            checksum_algorithm="sha256",
            idempotency_key=f"artifact-{handle.checkpoint_id}-{self.worker_id}",
        )
        commit = self._client.commit_checkpoint(
            run_id=self.run_id,
            checkpoint_id=handle.checkpoint_id,
            idempotency_key=f"commit-{handle.checkpoint_id}-{self.worker_id}",
            worker_id=self.worker_id,
        )
        handle.globally_committed = bool(
            getattr(commit, "globally_committed", False)
        )
        handle.t_committed_s = time.perf_counter() - t0

    # ── stage-2 transport ───────────────────────────────────────────────────

    def _upload(self, handle: CheckpointHandle) -> str:
        if self.dest is None:
            return handle.staging_path.resolve().as_uri()
        dest_uri = f"{self.dest}/{handle.artifact_name}"
        if self._uploader_bin is not None:
            self._upload_via_rust(handle.staging_path, dest_uri)
        elif dest_uri.startswith("s3://"):
            self._upload_via_boto3(handle.staging_path, dest_uri)
        else:
            target = _file_uri_to_path(dest_uri)
            if target is None:
                raise CheckpointError(f"unsupported checkpoint destination: {dest_uri}")
            target = target.resolve()
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(handle.staging_path, target)
        return dest_uri

    def _upload_via_rust(self, source: Path, dest_uri: str) -> None:
        self._run_rust_mover(
            ["--source", str(source), "--dest", dest_uri],
        )

    def _download_via_rust(self, source_uri: str, target: Path) -> None:
        self._run_rust_mover(
            ["--download", "--source", source_uri, "--dest", str(target)],
        )

    def _run_rust_mover(self, args: list[str]) -> None:
        cmd = [str(self._uploader_bin), *args]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise CheckpointError(
                f"runtime-uploader failed ({result.returncode}): {result.stderr.strip()}"
            )
        json.loads(result.stdout.strip().splitlines()[-1])  # sanity: valid JSON

    def _upload_via_boto3(self, source: Path, dest_uri: str) -> None:
        bucket, key = _parse_s3_uri(dest_uri)
        self._s3_client().upload_file(str(source), bucket, key)

    def _fetch(self, uri: str, artifact_name: str) -> Path:
        if uri.startswith("s3://"):
            target = self.staging_dir / artifact_name
            if self._uploader_bin is not None:
                self._download_via_rust(uri, target)
                return target
            bucket, key = _parse_s3_uri(uri)
            self._s3_client().download_file(bucket, key, str(target))
            return target
        local = _file_uri_to_path(uri)
        if local is None:
            raise CheckpointError(f"unsupported artifact uri scheme: {uri}")
        path = local.resolve()
        if path == (self.staging_dir / artifact_name).resolve():
            return path
        target = self.staging_dir / artifact_name
        if path.is_file():
            shutil.copyfile(path, target)
            return target
        raise CheckpointError(f"artifact not found at {uri}")

    def _s3_client(self) -> Any:
        try:
            return _make_s3_client()
        except ImportError as exc:
            raise CheckpointError(
                "s3:// checkpoint destination needs either the runtime-uploader "
                "binary (cargo build --release -p runtime-uploader) or boto3 "
                "(pip install 'dtruntime[s3]')"
            ) from exc

    def _load(self, path: Path) -> Any:
        torch = _torch()
        if torch is not None:
            return torch.load(path, map_location="cpu", weights_only=self.weights_only)
        with open(path, "rb") as f:
            return pickle.load(f)
