"""Real torch.distributed integration backed by the coordinator.

A training process calls :func:`init` (usually with configuration injected by
``python -m dtruntime.launch`` through environment variables).  ``init``:

1. connects to the coordinator and registers the worker,
2. starts a background heartbeat thread,
3. joins the coordinator rendezvous to receive a rank, world size, and c10d
   TCPStore endpoint, and
4. calls ``torch.distributed.init_process_group``.

The returned :class:`TrainingContext` exposes the rank/world size, wraps
models for DDP/FSDP/DeepSpeed via :meth:`TrainingContext.wrap_model`, and
signals elastic membership changes through
:attr:`TrainingContext.membership_changed` (set when the coordinator bumps
the rendezvous round because a worker died or joined).  Call
:meth:`TrainingContext.rejoin` to re-rendezvous and rebuild the process group
with the new membership.
"""

from __future__ import annotations

import datetime
import logging
import os
import threading
from typing import Callable, Optional

from dtruntime.client import RuntimeClient
from dtruntime.rendezvous import RendezvousResult, default_host, rendezvous
from dtruntime.strategies import wrap_model as _wrap_model

logger = logging.getLogger(__name__)

ENV_COORDINATOR_ADDR = "DTRUNTIME_COORDINATOR_ADDR"
ENV_RUN_ID = "DTRUNTIME_RUN_ID"
ENV_WORKER_ID = "DTRUNTIME_WORKER_ID"
ENV_WORKER_HOST = "DTRUNTIME_WORKER_HOST"
ENV_LOCAL_RANK = "DTRUNTIME_LOCAL_RANK"
ENV_BACKEND = "DTRUNTIME_TORCH_BACKEND"
ENV_HEARTBEAT_INTERVAL = "DTRUNTIME_HEARTBEAT_INTERVAL"

DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 10.0
DEFAULT_PROCESS_GROUP_TIMEOUT_SECONDS = 300.0


class MembershipChanged(RuntimeError):
    """Raised by :meth:`TrainingContext.raise_if_membership_changed` when the
    coordinator has started a new rendezvous round (a worker died or joined).

    Training loops catch this, optionally restore from the latest checkpoint,
    and call :meth:`TrainingContext.rejoin`.
    """


class RunTerminated(RuntimeError):
    """Raised when the run reached a terminal lifecycle state (cancelled or
    failed) — the worker should stop training and exit."""

    def __init__(self, run_state: int, message: str) -> None:
        super().__init__(message)
        self.run_state = run_state


# RunState values mirrored from proto (numeric so this module never imports
# generated code at import time).
RUN_STATE_FAILED = 5
RUN_STATE_CANCELLED = 6
_TERMINAL_RUN_STATES = {4, 5, 6}  # completed, failed, cancelled

# DeregisterReason values mirrored from proto.
DEREGISTER_COMPLETED = 1
DEREGISTER_PREEMPTED = 2
DEREGISTER_ERROR = 3


class _HeartbeatThread(threading.Thread):
    """Sends worker heartbeats and watches the rendezvous round.

    When the coordinator reports a rendezvous round newer than the one this
    worker joined, ``membership_changed`` is set so the training loop can
    re-rendezvous at the next step boundary.
    """

    def __init__(
        self,
        client: RuntimeClient,
        run_id: str,
        worker_id: str,
        interval_seconds: float,
        membership_changed: threading.Event,
        run_terminated: threading.Event,
    ) -> None:
        super().__init__(name=f"dtruntime-heartbeat-{worker_id}", daemon=True)
        self._client = client
        self._run_id = run_id
        self._worker_id = worker_id
        self._interval = interval_seconds
        self._membership_changed = membership_changed
        self._run_terminated = run_terminated
        self.terminal_run_state = 0
        self._stop = threading.Event()
        self._joined_round_lock = threading.Lock()
        self._joined_round = 0

    def set_joined_round(self, round_number: int) -> None:
        with self._joined_round_lock:
            self._joined_round = round_number
        self._membership_changed.clear()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:  # pragma: no cover - exercised via integration test
        while not self._stop.wait(self._interval):
            try:
                response = self._client.heartbeat(
                    run_id=self._run_id, worker_id=self._worker_id
                )
            except Exception as exc:  # noqa: BLE001 - keep heartbeating through transient errors
                logger.warning("heartbeat failed (will retry): %s", exc)
                continue
            run_state = int(getattr(response, "run_state", 0))
            if run_state in _TERMINAL_RUN_STATES:
                if not self._run_terminated.is_set():
                    logger.info("run reached terminal state %d; signalling exit", run_state)
                self.terminal_run_state = run_state
                self._run_terminated.set()
                return
            with self._joined_round_lock:
                joined = self._joined_round
            if joined and response.rendezvous_round > joined:
                if not self._membership_changed.is_set():
                    logger.info(
                        "rendezvous round advanced (%d -> %d); membership changed",
                        joined,
                        response.rendezvous_round,
                    )
                self._membership_changed.set()


class TrainingContext:
    """Handle to a live coordinator-backed torch.distributed process group."""

    def __init__(
        self,
        client: RuntimeClient,
        run_id: str,
        worker_id: str,
        host: str,
        backend: Optional[str],
        process_group_timeout: float,
        heartbeat_interval: float,
    ) -> None:
        self.client = client
        self.run_id = run_id
        self.worker_id = worker_id
        self.host = host
        self.local_rank = int(os.environ.get(ENV_LOCAL_RANK, "0"))
        self._backend = backend
        self._pg_timeout = process_group_timeout
        self.membership_changed = threading.Event()
        self.run_terminated = threading.Event()
        self._heartbeat = _HeartbeatThread(
            client,
            run_id,
            worker_id,
            heartbeat_interval,
            self.membership_changed,
            self.run_terminated,
        )
        self._rendezvous_result: Optional[RendezvousResult] = None
        self._closed = False

    # ── lifecycle ────────────────────────────────────────────────────────────

    def start(self, rendezvous_timeout: float = 600.0) -> "TrainingContext":
        """Register the heartbeat, rendezvous, and init the process group."""
        self._heartbeat.start()
        self._rendezvous_and_init(rendezvous_timeout)
        return self

    def rejoin(self, rendezvous_timeout: float = 600.0) -> "TrainingContext":
        """Tear down the process group and re-rendezvous after a membership change.

        The new round may have a different world size (elastic) and this worker
        may receive a different rank; training loops should re-shard data and
        restore model state from the latest committed checkpoint afterwards.
        """
        self._destroy_process_group()
        self._rendezvous_and_init(rendezvous_timeout)
        return self

    def shutdown(self, reason: int = DEREGISTER_COMPLETED) -> None:
        """Deregister from the coordinator, stop heartbeating, and leave the
        process group.

        ``reason`` defaults to graceful completion; pass
        :data:`DEREGISTER_PREEMPTED` when exiting due to eviction or
        :data:`DEREGISTER_ERROR` on unrecoverable local failure.
        """
        if self._closed:
            return
        self._closed = True
        self._heartbeat.stop()
        try:
            self.client.deregister_worker(
                run_id=self.run_id,
                worker_id=self.worker_id,
                reason=reason,
                idempotency_key=f"deregister-{self.worker_id}",
            )
        except Exception as exc:  # noqa: BLE001 - departure must not mask the training result
            logger.warning("deregister_worker failed (coordinator will age us out): %s", exc)
        self._destroy_process_group()

    def __enter__(self) -> "TrainingContext":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.shutdown(DEREGISTER_COMPLETED if exc_type is None else DEREGISTER_ERROR)

    # ── properties ───────────────────────────────────────────────────────────

    @property
    def rank(self) -> int:
        return self._require_result().rank

    @property
    def world_size(self) -> int:
        return self._require_result().world_size

    @property
    def rendezvous_round(self) -> int:
        return self._require_result().round

    @property
    def backend(self) -> str:
        return self._backend or _default_backend()

    @property
    def is_rank_zero(self) -> bool:
        return self.rank == 0

    # ── training helpers ─────────────────────────────────────────────────────

    def wrap_model(self, model: object, strategy: str = "ddp", **kwargs: object) -> object:
        """Wrap a model for the requested distributed strategy (ddp/fsdp/deepspeed)."""
        return _wrap_model(model, strategy=strategy, local_rank=self.local_rank, **kwargs)

    def raise_if_membership_changed(self) -> None:
        """Raise :class:`RunTerminated` when the run was cancelled or failed,
        or :class:`MembershipChanged` when the round advanced.

        Call once per training step (cheap: two Event checks).
        """
        if self.run_terminated.is_set():
            raise RunTerminated(
                self._heartbeat.terminal_run_state,
                f"run {self.run_id} reached terminal state "
                f"{self._heartbeat.terminal_run_state}; stopping",
            )
        if self.membership_changed.is_set():
            raise MembershipChanged(
                f"rendezvous round advanced past {self.rendezvous_round}; "
                "call TrainingContext.rejoin() to re-form the process group"
            )

    def run_elastic(
        self,
        train_fn: "Callable[[TrainingContext], None]",
        max_rejoin: int = 10,
        rendezvous_timeout: float = 600.0,
        collective_error_grace: float = 150.0,
    ) -> None:
        """Run ``train_fn`` under elastic supervision.

        ``train_fn`` receives this context and should call
        :meth:`raise_if_membership_changed` at step boundaries.  When
        membership changes, the process group is re-formed and ``train_fn`` is
        invoked again — construct (or restore from the latest checkpoint) the
        model, wrappers, and optimizer inside ``train_fn`` so they bind to the
        fresh process group.

        A ``RuntimeError`` from a collective (a peer died mid-all-reduce) is
        treated as a membership change only when the coordinator confirms a
        new rendezvous round within ``collective_error_grace`` seconds —
        covering the heartbeat-based failure-detection window — and re-raised
        otherwise, so genuine training bugs still propagate.
        """
        rejoins = 0
        while True:
            try:
                train_fn(self)
                return
            except RunTerminated:
                # Cancellation / run failure is not recoverable membership
                # churn — surface it to the caller.
                raise
            except MembershipChanged:
                pass
            except RuntimeError:
                logger.warning(
                    "collective failed; waiting up to %.0fs for the coordinator "
                    "to confirm a membership change",
                    collective_error_grace,
                )
                if not self.membership_changed.wait(timeout=collective_error_grace):
                    raise
            rejoins += 1
            if rejoins > max_rejoin:
                raise MembershipChanged(
                    f"membership changed more than max_rejoin={max_rejoin} times"
                )
            logger.info("membership changed; re-rendezvousing (attempt %d)", rejoins)
            self.rejoin(rendezvous_timeout)

    # ── internals ────────────────────────────────────────────────────────────

    def _require_result(self) -> RendezvousResult:
        if self._rendezvous_result is None:
            raise RuntimeError("TrainingContext is not started; call start() first")
        return self._rendezvous_result

    def _rendezvous_and_init(self, timeout: float) -> None:
        result = rendezvous(
            self.client,
            run_id=self.run_id,
            worker_id=self.worker_id,
            host=self.host,
            timeout=timeout,
        )
        self._rendezvous_result = result
        self._heartbeat.set_joined_round(result.round)
        self._init_process_group(result)
        logger.info(
            "process group ready: rank %d/%d (round %d, store %s)",
            result.rank,
            result.world_size,
            result.round,
            result.init_method,
        )

    def _init_process_group(self, result: RendezvousResult) -> None:
        import torch.distributed as dist

        dist.init_process_group(
            backend=self.backend,
            init_method=result.init_method,
            rank=result.rank,
            world_size=result.world_size,
            timeout=datetime.timedelta(seconds=self._pg_timeout),
        )

    def _destroy_process_group(self) -> None:
        try:
            import torch.distributed as dist
        except ImportError:  # pragma: no cover
            return
        if dist.is_available() and dist.is_initialized():
            try:
                dist.destroy_process_group()
            except Exception as exc:  # noqa: BLE001 - the group may already be broken
                logger.warning("destroy_process_group failed (continuing): %s", exc)


def _default_backend() -> str:
    env_backend = os.environ.get(ENV_BACKEND)
    if env_backend:
        return env_backend
    try:
        import torch

        return "nccl" if torch.cuda.is_available() else "gloo"
    except ImportError:
        return "gloo"


def init(
    coordinator_addr: str | None = None,
    run_id: str | None = None,
    worker_id: str | None = None,
    host: str | None = None,
    backend: str | None = None,
    rendezvous_timeout: float = 600.0,
    process_group_timeout: float = DEFAULT_PROCESS_GROUP_TIMEOUT_SECONDS,
    heartbeat_interval: float | None = None,
    client: RuntimeClient | None = None,
) -> TrainingContext:
    """Initialise coordinator-backed distributed training for this process.

    Arguments default to the ``DTRUNTIME_*`` environment variables injected by
    ``python -m dtruntime.launch``; direct calls can pass them explicitly.
    Registers the worker, starts heartbeats, performs the rendezvous, and
    calls ``torch.distributed.init_process_group``.
    """
    coordinator_addr = coordinator_addr or os.environ.get(ENV_COORDINATOR_ADDR)
    run_id = run_id or os.environ.get(ENV_RUN_ID)
    worker_id = worker_id or os.environ.get(ENV_WORKER_ID)
    host = host or os.environ.get(ENV_WORKER_HOST) or default_host()
    if heartbeat_interval is None:
        heartbeat_interval = float(
            os.environ.get(ENV_HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_INTERVAL_SECONDS)
        )

    if client is None:
        if not coordinator_addr:
            raise ValueError(
                f"coordinator_addr is required (set {ENV_COORDINATOR_ADDR} or pass explicitly)"
            )
        client = RuntimeClient(coordinator_addr)
    if not run_id:
        raise ValueError(f"run_id is required (set {ENV_RUN_ID} or pass explicitly)")
    if not worker_id:
        raise ValueError(f"worker_id is required (set {ENV_WORKER_ID} or pass explicitly)")

    client.register_worker(
        run_id=run_id,
        worker_id=worker_id,
        host=host,
        idempotency_key=f"register-{worker_id}",
    )

    context = TrainingContext(
        client=client,
        run_id=run_id,
        worker_id=worker_id,
        host=host,
        backend=backend,
        process_group_timeout=process_group_timeout,
        heartbeat_interval=heartbeat_interval,
    )
    return context.start(rendezvous_timeout)
