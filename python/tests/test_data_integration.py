"""End-to-end data pipeline test against a real coordinator.

Gated behind ``DTRUNTIME_RUN_LOCAL_INTEGRATION=1``.  Registers a versioned
dataset, then two workers (rank 0 and rank 1) each stream their coordinator-
assigned shards through a ``DistributedShardDataset`` and assert that:

- every record is delivered exactly once across the two ranks (no overlap, no
  loss) — real multi-rank data locality / sharding;
- each rank's shard set is stable across epochs (cache stays warm);
- the content-addressed dataset version round-trips.
"""

from __future__ import annotations

import os
import socket
import subprocess
import time
import uuid
from collections import Counter
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def _gate():
    if os.environ.get("DTRUNTIME_RUN_LOCAL_INTEGRATION") != "1":
        pytest.skip("set DTRUNTIME_RUN_LOCAL_INTEGRATION=1 to run coordinator integration test")
    pytest.importorskip("grpc")
    pytest.importorskip("numpy")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _start_coordinator(log_dir: Path):
    import grpc

    port = _free_port()
    address = f"127.0.0.1:{port}"
    out = (log_dir / "coord.log").open("w", encoding="utf-8")
    proc = subprocess.Popen(
        ["cargo", "run", "-p", "runtime-coordinator", "--", "--addr", address],
        cwd=REPO_ROOT, stdout=out, stderr=subprocess.STDOUT,
    )
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            pytest.fail("coordinator exited early")
        ch = grpc.insecure_channel(address)
        try:
            grpc.channel_ready_future(ch).result(timeout=1)
            return proc, address
        except Exception:
            pass
        finally:
            ch.close()
    pytest.fail("coordinator not reachable")


def _stop(proc):
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def test_two_rank_streaming_covers_dataset_once(tmp_path: Path) -> None:
    _gate()
    import numpy as np

    from dtruntime import connect
    from dtruntime.data import DistributedShardDataset, compute_dataset_version
    from dtruntime.data.versioning import register_versioned_dataset
    from dtruntime.generated import runtime_pb2
    from dtruntime.shard_loader import build_and_write_shards

    proc, address = _start_coordinator(tmp_path)
    client = None
    try:
        client = connect(address)
        suffix = uuid.uuid4().hex[:8]
        run = client.register_run(
            run_name=f"data-{suffix}",
            world_size=2,
            assignment_policy=runtime_pb2.ASSIGNMENT_POLICY_STATIC_RANK,
            storage_backend_kind=runtime_pb2.STORAGE_BACKEND_KIND_LOCAL_FILESYSTEM,
            idempotency_key=f"run-{suffix}",
        )
        run_id = run.run_id.value

        # 8 shards, 5 records each = 40 unique labels.
        n, feat = 40, 4
        features = np.arange(n * feat, dtype=np.float32).reshape(n, feat)
        labels = np.arange(n, dtype=np.int64)
        shards = build_and_write_shards(tmp_path / "shards", features, labels, shard_count=8)
        expected_version = compute_dataset_version(shards)

        _, version = register_versioned_dataset(
            client, run_id, "d", f"file://{tmp_path / 'shards'}", "npz", shards,
            dataset_id="dataset", idempotency_key=f"ds-{suffix}",
        )
        assert version == expected_version

        # Register two workers at ranks 0 and 1.
        for rank in (0, 1):
            client.register_worker(
                run_id=run_id, worker_id=f"w{rank}", rank=rank,
                idempotency_key=f"w{rank}-{suffix}",
            )

        def stream_rank(rank: int, epoch: int) -> list[int]:
            ds = DistributedShardDataset(
                client=client, run_id=run_id, worker_id=f"w{rank}",
                dataset_id="dataset", dataset_format="npz", shuffle=True, seed=99,
                cache_dir=str(tmp_path / f"cache-{rank}"),
                transform=lambda r: int(r["label"]),
            )
            ds.set_epoch(epoch)
            return list(ds)

        r0_e0 = stream_rank(0, 0)
        r1_e0 = stream_rank(1, 0)

        # Union covers the whole dataset exactly once (multi-rank locality).
        assert Counter(r0_e0 + r1_e0) == Counter(range(n))
        assert set(r0_e0).isdisjoint(r1_e0), "ranks must not share records"

        # Epoch 1: same per-rank shard set (cache warm), reordered.
        r0_e1 = stream_rank(0, 1)
        assert Counter(r0_e0) == Counter(r0_e1), "rank 0 keeps its shard set across epochs"
    finally:
        if client is not None:
            client.close()
        _stop(proc)
