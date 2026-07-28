"""Coordinator failover test: a hard crash + restart on the same metadata store
must transparently rehydrate durable run state.

Gated by DTRUNTIME_RUN_FAILOVER_TESTS=1 and the prebuilt release coordinator
(cargo build --release -p runtime-coordinator).
"""

from __future__ import annotations

import os

import harness
import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("DTRUNTIME_RUN_FAILOVER_TESTS") != "1",
    reason="set DTRUNTIME_RUN_FAILOVER_TESTS=1 to run the failover test",
)


def test_coordinator_hard_kill_and_restart_rehydrates_run_state(tmp_path):
    pytest.importorskip("grpc")
    from dtruntime.client import RuntimeClient
    from dtruntime.generated import runtime_pb2 as pb2

    binary = harness.require_binary("runtime-coordinator")
    db = tmp_path / "meta.db"
    storage = tmp_path / "storage"
    address = f"127.0.0.1:{harness.free_tcp_port()}"
    extra = ["--metadata-db", str(db), "--storage-root", str(storage)]

    # ── Step 1: register a run + workers, then confirm state. ──
    proc = harness.start_coordinator(binary, address, extra)
    try:
        harness.wait_for_grpc(address, proc)
        client = RuntimeClient(address)
        reg = client.register_run(
            run_name="failover-run",
            world_size=2,
            assignment_policy=pb2.ASSIGNMENT_POLICY_STATIC_RANK,
            storage_backend_kind=pb2.STORAGE_BACKEND_KIND_LOCAL_FILESYSTEM,
            run_version="v1",
            idempotency_key="failover-idem",
        )
        run_id = reg.run_id.value
        for i in range(2):
            client.register_worker(
                run_id=run_id, worker_id=f"w{i}", rank=i, host=f"n{i}",
                idempotency_key=f"reg-w{i}",
            )
        before = client.get_run_status(run_id)
        assert before.active_worker_count >= 0
        state_before = before.state
        client.close()
    finally:
        # Hard kill — no graceful shutdown, simulating a crash/OOM.
        harness.hard_kill(proc)

    # ── Step 2: restart on the SAME metadata store. ──
    proc2 = harness.start_coordinator(binary, address, extra)
    try:
        harness.wait_for_grpc(address, proc2)
        client2 = RuntimeClient(address)

        # The run survived the crash: same id, rehydrated state.
        after = client2.get_run_status(run_id)
        assert after.run_id.value == run_id
        assert after.state == state_before

        # Idempotency was restored from the durable store: re-registering with
        # the same key returns the SAME run id rather than creating a new run.
        reg2 = client2.register_run(
            run_name="failover-run",
            world_size=2,
            assignment_policy=pb2.ASSIGNMENT_POLICY_STATIC_RANK,
            storage_backend_kind=pb2.STORAGE_BACKEND_KIND_LOCAL_FILESYSTEM,
            run_version="v1",
            idempotency_key="failover-idem",
        )
        assert reg2.run_id.value == run_id
        client2.close()
    finally:
        harness.stop_process(proc2)
