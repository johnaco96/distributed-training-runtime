"""Unit tests for run-lifecycle handling in the Python SDK (no coordinator)."""

from __future__ import annotations

import threading
import types
import time
from pathlib import Path

import pytest

from dtruntime import torch_dist
from dtruntime.launch import LaunchConfig, WorkerSupervisor


class FakeHeartbeatClient:
    """Returns scripted heartbeat responses."""

    def __init__(self, responses: list) -> None:
        self._responses = list(responses)
        self.calls = 0

    def heartbeat(self, run_id: str, worker_id: str):
        self.calls += 1
        if len(self._responses) > 1:
            return self._responses.pop(0)
        return self._responses[0]


def _hb_response(rendezvous_round: int = 0, run_state: int = 0):
    return types.SimpleNamespace(
        state=2, rendezvous_round=rendezvous_round, run_state=run_state
    )


def _wait_for(event: threading.Event, timeout: float = 5.0) -> bool:
    return event.wait(timeout)


def test_heartbeat_thread_signals_run_termination_on_cancelled_state() -> None:
    membership = threading.Event()
    terminated = threading.Event()
    client = FakeHeartbeatClient([_hb_response(run_state=torch_dist.RUN_STATE_CANCELLED)])
    thread = torch_dist._HeartbeatThread(
        client, "run-1", "w0", 0.01, membership, terminated
    )
    thread.start()
    assert _wait_for(terminated), "cancellation must set run_terminated"
    assert thread.terminal_run_state == torch_dist.RUN_STATE_CANCELLED
    assert not membership.is_set()
    thread.join(timeout=5)
    assert not thread.is_alive(), "thread exits once the run is terminal"


def test_heartbeat_thread_signals_membership_change_on_round_advance() -> None:
    membership = threading.Event()
    terminated = threading.Event()
    client = FakeHeartbeatClient(
        [_hb_response(rendezvous_round=5), _hb_response(rendezvous_round=6)]
    )
    thread = torch_dist._HeartbeatThread(
        client, "run-1", "w0", 0.01, membership, terminated
    )
    thread.set_joined_round(5)
    thread.start()
    try:
        assert _wait_for(membership), "round advance must set membership_changed"
        assert not terminated.is_set()
    finally:
        thread.stop()
        thread.join(timeout=5)


def test_heartbeat_thread_survives_transient_rpc_errors() -> None:
    membership = threading.Event()
    terminated = threading.Event()

    class FlakyClient:
        def __init__(self) -> None:
            self.calls = 0

        def heartbeat(self, run_id: str, worker_id: str):
            self.calls += 1
            if self.calls < 3:
                raise RuntimeError("transient")
            return _hb_response(run_state=torch_dist.RUN_STATE_CANCELLED)

    thread = torch_dist._HeartbeatThread(
        FlakyClient(), "run-1", "w0", 0.01, membership, terminated
    )
    thread.start()
    assert _wait_for(terminated), "thread must keep heartbeating through errors"
    thread.join(timeout=5)


class _NoopHeartbeatClient:
    def heartbeat(self, run_id: str, worker_id: str):
        return _hb_response()

    def deregister_worker(self, **kwargs):
        self.deregistered = kwargs
        return types.SimpleNamespace(state=7, run_state=4)


def _context() -> torch_dist.TrainingContext:
    return torch_dist.TrainingContext(
        client=_NoopHeartbeatClient(),
        run_id="run-1",
        worker_id="w0",
        host="h",
        backend="gloo",
        process_group_timeout=1.0,
        heartbeat_interval=3600.0,
    )


def test_raise_prefers_run_termination_over_membership_change() -> None:
    ctx = _context()
    ctx.membership_changed.set()
    ctx.run_terminated.set()
    ctx._heartbeat.terminal_run_state = torch_dist.RUN_STATE_CANCELLED
    with pytest.raises(torch_dist.RunTerminated) as excinfo:
        ctx.raise_if_membership_changed()
    assert excinfo.value.run_state == torch_dist.RUN_STATE_CANCELLED


def test_run_elastic_does_not_retry_run_termination() -> None:
    ctx = _context()
    attempts = []

    def train(_ctx):
        attempts.append(1)
        raise torch_dist.RunTerminated(torch_dist.RUN_STATE_CANCELLED, "cancelled")

    with pytest.raises(torch_dist.RunTerminated):
        ctx.run_elastic(train, max_rejoin=5)
    assert len(attempts) == 1, "termination must not trigger re-rendezvous"


def test_shutdown_deregisters_with_reason() -> None:
    ctx = _context()
    ctx.shutdown(torch_dist.DEREGISTER_COMPLETED)
    assert ctx.client.deregistered["reason"] == torch_dist.DEREGISTER_COMPLETED
    # Idempotent: a second shutdown does not re-deregister.
    ctx.client.deregistered = None
    ctx.shutdown()
    assert ctx.client.deregistered is None


def test_supervisor_stops_without_restart_when_run_is_cancelled(tmp_path: Path) -> None:
    script = tmp_path / "worker.py"
    script.write_text("import sys; sys.exit(75)\n", encoding="utf-8")
    config = LaunchConfig(
        coordinator="unused:1",
        run_name="t",
        script=str(script),
        nproc_per_node=1,
        max_restarts=5,
        node_id="n",
    )
    supervisor = WorkerSupervisor(config, "run-1")
    supervisor._run_state = lambda: 6  # cancelled
    supervisor.start()
    assert supervisor.supervise() == 3
    assert supervisor._restarts_used == 0


def test_supervisor_treats_completed_run_with_failing_worker_as_success(
    tmp_path: Path,
) -> None:
    script = tmp_path / "worker.py"
    script.write_text("import sys; sys.exit(1)\n", encoding="utf-8")
    config = LaunchConfig(
        coordinator="unused:1",
        run_name="t",
        script=str(script),
        nproc_per_node=1,
        max_restarts=5,
        node_id="n",
    )
    supervisor = WorkerSupervisor(config, "run-1")
    supervisor._run_state = lambda: 4  # completed
    supervisor.start()
    assert supervisor.supervise() == 0


def test_supervisor_restarts_when_run_state_is_unknown(tmp_path: Path) -> None:
    marker = tmp_path / "attempted"
    script = tmp_path / "worker.py"
    script.write_text(
        "import os, sys\n"
        f"m = r'{marker}'\n"
        "if not os.path.exists(m):\n"
        "    open(m, 'w').close(); sys.exit(1)\n"
        "sys.exit(0)\n",
        encoding="utf-8",
    )
    config = LaunchConfig(
        coordinator="unused:1",
        run_name="t",
        script=str(script),
        nproc_per_node=1,
        max_restarts=3,
        node_id="n",
    )
    supervisor = WorkerSupervisor(config, "run-1")
    supervisor._run_state = lambda: 0  # unknown / live
    supervisor.start()
    assert supervisor.supervise() == 0
    assert supervisor._restarts_used == 1
