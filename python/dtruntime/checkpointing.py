"""Distributed checkpointing on top of ``torch.distributed.checkpoint`` (DCP).

:class:`DistributedCheckpointer` saves model + optimizer state through DCP's
sharded state-dict format and stores the resulting files as coordinator
checkpoint artifacts under two-phase commit.  Because the on-disk format is
DCP's, restores **reshard automatically across world sizes and parallelism
strategies** — save from 4 FSDP ranks and restore into 2 DDP ranks, or vice
versa.

Guarantees and behavior:

- **Atomicity** — a checkpoint is restorable only after every rank's artifacts
  are uploaded and every rank has voted to commit (coordinator 2PC).  A crash
  mid-upload leaves a partial checkpoint that is never selected for restore
  and is garbage-collected by retention.
- **Partial-upload recovery** — ``save`` retries failed artifact uploads;
  re-streaming an artifact is idempotent (same name, same bytes).
- **Corruption handling** — every artifact download is checksum-verified
  against the committed manifest.  On mismatch (or missing artifact) the
  checkpoint is reported corrupt; the coordinator marks it FAILED, the latest
  pointer falls back to the previous committed checkpoint, and the restore
  retries automatically.
- **Async save** — ``save_async`` stages state dicts and returns a handle;
  upload and 2PC run in a background thread while training continues.

Typical elastic training loop::

    ctx = torch_dist.init()
    checkpointer = DistributedCheckpointer(ctx)

    def train(c):
        model = c.wrap_model(build_model(), strategy="ddp")
        optimizer = make_optimizer(model)
        restored = checkpointer.restore(model, optimizer)
        start = restored.step + 1 if restored else 0
        for step in range(start, total_steps):
            c.raise_if_membership_changed()
            train_step(model, optimizer)
            if step % 100 == 0:
                checkpointer.save(model, optimizer, step=step)

    ctx.run_elastic(train)
"""

from __future__ import annotations

import hashlib
import logging
import shutil
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from dtruntime.torch_dist import TrainingContext

logger = logging.getLogger(__name__)

_UPLOAD_ATTEMPTS = 3
_CORRUPT_FALLBACK_ATTEMPTS = 3


class CheckpointError(Exception):
    """Raised when a checkpoint operation fails permanently.

    Deliberately NOT a ``RuntimeError`` subclass: elastic training loops treat
    unexpected ``RuntimeError``s as potential collective failures and wait for
    membership confirmation, which must not happen for checkpoint failures.
    """


@dataclass(frozen=True)
class RestoreResult:
    checkpoint_id: str
    step: int


class AsyncSaveHandle:
    """Handle for an in-flight :meth:`DistributedCheckpointer.save_async`."""

    def __init__(self) -> None:
        self._done = threading.Event()
        self._error: Optional[BaseException] = None
        self.checkpoint_id: Optional[str] = None

    def done(self) -> bool:
        return self._done.is_set()

    def result(self, timeout: Optional[float] = None) -> str:
        """Block until the save is committed; returns the checkpoint id."""
        if not self._done.wait(timeout):
            raise TimeoutError("async checkpoint save did not finish in time")
        if self._error is not None:
            raise CheckpointError("async checkpoint save failed") from self._error
        assert self.checkpoint_id is not None
        return self.checkpoint_id


class DistributedCheckpointer:
    """Coordinator-backed distributed checkpoint save/restore for a worker.

    All ranks of the process group must call :meth:`save` (or
    :meth:`save_async`) and :meth:`restore` collectively — DCP exchanges
    plans across the group.
    """

    def __init__(self, ctx: "TrainingContext", prefix: str = "dcp") -> None:
        self._ctx = ctx
        self._prefix = prefix

    # ── save ─────────────────────────────────────────────────────────────────

    def save(
        self,
        model: Any,
        optimizer: Any = None,
        *,
        step: int,
        extra_state: Optional[dict] = None,
    ) -> str:
        """Collectively save model/optimizer state as checkpoint ``step``.

        Returns the checkpoint id once it is globally committed.
        """
        state_dict = self._build_state_dict(model, optimizer, step, extra_state)
        checkpoint_id = f"{self._prefix}-step-{step}"
        local_dir = Path(tempfile.mkdtemp(prefix=f"dtr-ckpt-{step}-"))
        try:
            import torch.distributed.checkpoint as dcp

            dcp.save(state_dict, checkpoint_id=str(local_dir))
            self._upload_and_commit(checkpoint_id, step, local_dir)
        finally:
            shutil.rmtree(local_dir, ignore_errors=True)
        return checkpoint_id

    def save_async(
        self,
        model: Any,
        optimizer: Any = None,
        *,
        step: int,
        extra_state: Optional[dict] = None,
    ) -> AsyncSaveHandle:
        """Stage state and persist/upload/commit in the background.

        Staging (CPU copy + DCP plan exchange) happens on the calling thread —
        all ranks must call this at the same step.  The returned handle
        resolves once the checkpoint is globally committed.  At most one async
        save should be in flight at a time.
        """
        state_dict = self._build_state_dict(model, optimizer, step, extra_state)
        checkpoint_id = f"{self._prefix}-step-{step}"
        local_dir = Path(tempfile.mkdtemp(prefix=f"dtr-ckpt-{step}-"))

        import torch.distributed.checkpoint as dcp

        # DCP's own async_save stages tensors and writes in the background
        # where available; fall back to a synchronous write with async upload.
        try:
            write_future = dcp.async_save(state_dict, checkpoint_id=str(local_dir))
        except Exception:  # noqa: BLE001 - version-dependent surface
            dcp.save(state_dict, checkpoint_id=str(local_dir))
            write_future = None

        handle = AsyncSaveHandle()

        def _finish() -> None:
            try:
                if write_future is not None:
                    write_future.result()
                self._upload_and_commit(checkpoint_id, step, local_dir)
                handle.checkpoint_id = checkpoint_id
            except BaseException as exc:  # noqa: BLE001 - reported via handle
                handle._error = exc
            finally:
                shutil.rmtree(local_dir, ignore_errors=True)
                handle._done.set()

        threading.Thread(
            target=_finish, name=f"dtruntime-ckpt-{checkpoint_id}", daemon=True
        ).start()
        return handle

    _STATE_COMMITTED = 4  # CHECKPOINT_STATE_COMMITTED

    def _upload_and_commit(self, checkpoint_id: str, step: int, local_dir: Path) -> None:
        ctx = self._ctx
        # Every rank issues Begin with the same idempotency key: the first one
        # creates the record, the rest get the idempotent replay.
        begun = ctx.client.begin_checkpoint(
            run_id=ctx.run_id,
            worker_id=ctx.worker_id,
            checkpoint_id=checkpoint_id,
            step=step,
            idempotency_key=f"begin-{checkpoint_id}",
        )
        if int(begun.state) == self._STATE_COMMITTED:
            # A previous save of this step already committed globally (e.g. a
            # periodic save and a final save landing on the same step).
            # Artifacts are immutable; re-uploading fresh bytes would conflict.
            logger.info("checkpoint %s already committed; skipping save", checkpoint_id)
            return

        for path in sorted(local_dir.rglob("*")):
            if not path.is_file():
                continue
            artifact_name = path.relative_to(local_dir).as_posix()
            data = path.read_bytes()
            checksum = hashlib.sha256(data).hexdigest()
            self._upload_artifact_with_retry(checkpoint_id, artifact_name, data, checksum)

        response = ctx.client.commit_checkpoint(
            run_id=ctx.run_id,
            checkpoint_id=checkpoint_id,
            worker_id=ctx.worker_id,
            idempotency_key=f"commit-{checkpoint_id}-{ctx.worker_id}",
        )
        # 2PC: wait for the global commit (other ranks may still be uploading).
        deadline = time.monotonic() + 600
        while not response.globally_committed:
            if time.monotonic() > deadline:
                raise CheckpointError(
                    f"checkpoint {checkpoint_id} was not globally committed within 600s"
                )
            time.sleep(0.5)
            response = ctx.client.commit_checkpoint(
                run_id=ctx.run_id,
                checkpoint_id=checkpoint_id,
                worker_id=ctx.worker_id,
                idempotency_key=f"commit-{checkpoint_id}-{ctx.worker_id}",
            )
        logger.info("checkpoint %s committed at step %d", checkpoint_id, step)

    def _upload_artifact_with_retry(
        self, checkpoint_id: str, artifact_name: str, data: bytes, checksum: str
    ) -> None:
        ctx = self._ctx
        last_error: Optional[Exception] = None
        for attempt in range(1, _UPLOAD_ATTEMPTS + 1):
            try:
                response = ctx.client.stream_checkpoint_artifact(
                    run_id=ctx.run_id,
                    worker_id=ctx.worker_id,
                    checkpoint_id=checkpoint_id,
                    artifact_name=artifact_name,
                    data=data,
                    idempotency_key=f"stream-{checkpoint_id}-{artifact_name}",
                )
                if response.checksum and response.checksum != checksum:
                    raise CheckpointError(
                        f"coordinator stored a different checksum for {artifact_name}: "
                        f"{response.checksum} != {checksum}"
                    )
                ctx.client.report_checkpoint_artifact(
                    run_id=ctx.run_id,
                    worker_id=ctx.worker_id,
                    checkpoint_id=checkpoint_id,
                    artifact_name=artifact_name,
                    artifact_uri=response.artifact_uri,
                    size_bytes=len(data),
                    checksum=checksum,
                    checksum_algorithm="sha256",
                    idempotency_key=f"report-{checkpoint_id}-{artifact_name}",
                )
                return
            except Exception as exc:  # noqa: BLE001 - retried, then surfaced
                if _is_permanent_rpc_error(exc):
                    raise CheckpointError(
                        f"artifact {artifact_name} upload rejected permanently"
                    ) from exc
                last_error = exc
                logger.warning(
                    "artifact %s upload attempt %d/%d failed: %s",
                    artifact_name,
                    attempt,
                    _UPLOAD_ATTEMPTS,
                    exc,
                )
                time.sleep(min(2.0 * attempt, 5.0))
        raise CheckpointError(
            f"artifact {artifact_name} upload failed after {_UPLOAD_ATTEMPTS} attempts"
        ) from last_error

    # ── restore ──────────────────────────────────────────────────────────────

    def restore(
        self,
        model: Any,
        optimizer: Any = None,
    ) -> Optional[RestoreResult]:
        """Collectively restore the latest committed checkpoint.

        Returns ``None`` when the run has no committed checkpoint.  Corrupt
        checkpoints are reported and skipped, falling back to older committed
        checkpoints (up to 3 generations).
        """
        for _ in range(_CORRUPT_FALLBACK_ATTEMPTS):
            manifest = self._latest_manifest()
            if manifest is None:
                return None
            checkpoint_id = manifest.checkpoint_id.value

            local_dir = Path(tempfile.mkdtemp(prefix="dtr-restore-"))
            try:
                corruption = self._download_all(manifest, local_dir)
                if corruption is not None:
                    self._report_corrupt(checkpoint_id, corruption)
                    continue

                state_dict = self._build_state_dict(model, optimizer, 0, None)
                import torch.distributed.checkpoint as dcp

                dcp.load(state_dict, checkpoint_id=str(local_dir))
                self._apply_state_dict(model, optimizer, state_dict)
                step = int(state_dict.get("extra", {}).get("step", manifest.step))
                logger.info("restored checkpoint %s (step %d)", checkpoint_id, step)
                return RestoreResult(checkpoint_id=checkpoint_id, step=step)
            finally:
                shutil.rmtree(local_dir, ignore_errors=True)
        raise CheckpointError(
            f"no restorable checkpoint after {_CORRUPT_FALLBACK_ATTEMPTS} corruption fallbacks"
        )

    def _latest_manifest(self):
        import grpc

        try:
            return self._ctx.client.restore_latest(run_id=self._ctx.run_id).manifest
        except grpc.RpcError as exc:
            if exc.code() == grpc.StatusCode.NOT_FOUND:
                return None
            raise

    def _download_all(self, manifest, local_dir: Path) -> Optional[str]:
        """Download and verify every artifact.  Returns a corruption reason on
        failure, None on success."""
        ctx = self._ctx
        for artifact in manifest.artifacts:
            owner = _artifact_owner(artifact.artifact_uri)
            destination = local_dir / artifact.artifact_name
            try:
                size, checksum = ctx.client.fetch_checkpoint_artifact(
                    run_id=ctx.run_id,
                    worker_id=ctx.worker_id,
                    checkpoint_id=manifest.checkpoint_id.value,
                    artifact_name=artifact.artifact_name,
                    owner_worker_id=owner,
                    destination=destination,
                )
            except Exception as exc:  # noqa: BLE001 - treated as corruption
                return f"artifact {artifact.artifact_name} unreadable: {exc}"
            if artifact.checksum and checksum and checksum != artifact.checksum:
                return (
                    f"artifact {artifact.artifact_name} checksum mismatch: "
                    f"stored {checksum} != manifest {artifact.checksum}"
                )
            if artifact.size_bytes and size != artifact.size_bytes:
                return (
                    f"artifact {artifact.artifact_name} size mismatch: "
                    f"{size} != {artifact.size_bytes}"
                )
        return None

    def _report_corrupt(self, checkpoint_id: str, reason: str) -> None:
        logger.warning("checkpoint %s corrupt (%s); falling back", checkpoint_id, reason)
        try:
            self._ctx.client.report_checkpoint_corrupt(
                run_id=self._ctx.run_id,
                worker_id=self._ctx.worker_id,
                checkpoint_id=checkpoint_id,
                reason=reason,
                idempotency_key=f"corrupt-{checkpoint_id}-{self._ctx.worker_id}",
            )
        except Exception as exc:  # noqa: BLE001 - another rank may have reported first
            logger.info("corrupt report for %s not accepted (%s)", checkpoint_id, exc)

    # ── state-dict plumbing ──────────────────────────────────────────────────

    def _build_state_dict(
        self, model: Any, optimizer: Any, step: int, extra_state: Optional[dict]
    ) -> dict:
        from torch.distributed.checkpoint.state_dict import get_state_dict

        extra = {"step": step}
        if extra_state:
            extra.update(extra_state)
        if optimizer is not None:
            model_sd, optim_sd = get_state_dict(model, optimizers=optimizer)
            return {"model": model_sd, "optim": optim_sd, "extra": extra}
        model_sd, _ = get_state_dict(model, optimizers=[])
        return {"model": model_sd, "extra": extra}

    def _apply_state_dict(self, model: Any, optimizer: Any, state_dict: dict) -> None:
        from torch.distributed.checkpoint.state_dict import set_state_dict

        if optimizer is not None:
            set_state_dict(
                model,
                optimizers=optimizer,
                model_state_dict=state_dict["model"],
                optim_state_dict=state_dict["optim"],
            )
        else:
            set_state_dict(
                model,
                optimizers=[],
                model_state_dict=state_dict["model"],
                optim_state_dict={},
            )


def _is_permanent_rpc_error(exc: Exception) -> bool:
    """Application-level rejections that will not succeed on retry."""
    try:
        import grpc
    except ImportError:  # pragma: no cover
        return False
    return isinstance(exc, grpc.RpcError) and exc.code() in (
        grpc.StatusCode.INVALID_ARGUMENT,
        grpc.StatusCode.FAILED_PRECONDITION,
        grpc.StatusCode.PERMISSION_DENIED,
        grpc.StatusCode.NOT_FOUND,
    )


def _artifact_owner(artifact_uri: str) -> str:
    """Extract the uploading worker id from the storage layout
    ``.../checkpoints/<ckpt>/workers/<worker_id>/<artifact...>``."""
    parts = artifact_uri.replace("\\", "/").split("/")
    try:
        index = len(parts) - 1 - parts[::-1].index("workers")
        return parts[index + 1]
    except ValueError as exc:
        raise CheckpointError(
            f"artifact uri {artifact_uri!r} does not follow the workers/<id>/ layout"
        ) from exc
