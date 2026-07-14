"""Tests for the staged async checkpoint pipeline (AsyncCheckpointer).

Spawns the pre-built release coordinator (built by CI before pytest, and by
`cargo build --release -p runtime-coordinator` locally) and exercises the
full pipeline: snapshot → local staging → upload → artifact report → 2PC
commit vote → restore.
"""

from __future__ import annotations

import os
import socket
import subprocess
import time
import uuid
from pathlib import Path

import pytest

grpc = pytest.importorskip("grpc")
torch = pytest.importorskip("torch")

from dtruntime import AsyncCheckpointer, CheckpointError, connect
from dtruntime.generated import runtime_pb2

REPO_ROOT = Path(__file__).resolve().parents[2]
EXE = ".exe" if os.name == "nt" else ""
COORDINATOR_BIN = REPO_ROOT / "target" / "release" / f"runtime-coordinator{EXE}"


def _free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture(scope="module")
def coordinator_addr(tmp_path_factory: pytest.TempPathFactory) -> str:
    if not COORDINATOR_BIN.is_file():
        pytest.skip(
            f"release coordinator not built at {COORDINATOR_BIN}; "
            "run: cargo build --release -p runtime-coordinator"
        )
    addr = f"127.0.0.1:{_free_tcp_port()}"
    storage = tmp_path_factory.mktemp("coordinator-storage")
    proc = subprocess.Popen(
        [str(COORDINATOR_BIN), "--addr", addr, "--storage-root", str(storage)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.time() + 30
    with connect(addr) as probe:
        while time.time() < deadline:
            try:
                probe.restore_latest("warmup")
                break
            except grpc.RpcError as exc:
                if exc.code() is not grpc.StatusCode.UNAVAILABLE:
                    break  # server answered — ready
                if proc.poll() is not None:
                    raise RuntimeError("coordinator exited during startup")
                time.sleep(0.2)
        else:
            proc.terminate()
            raise RuntimeError(f"coordinator not ready at {addr}")
    yield addr
    proc.terminate()
    proc.wait(timeout=10)


def _register_run_and_workers(client, world_size: int) -> tuple[str, list[str]]:
    suffix = uuid.uuid4().hex[:8]
    run = client.register_run(
        run_name=f"async-ckpt-{suffix}",
        world_size=world_size,
        assignment_policy=runtime_pb2.ASSIGNMENT_POLICY_STATIC_RANK,
        storage_backend_kind=runtime_pb2.STORAGE_BACKEND_KIND_LOCAL_FILESYSTEM,
        idempotency_key=f"run-{suffix}",
    )
    run_id = run.run_id.value
    worker_ids = []
    for rank in range(world_size):
        resp = client.register_worker(
            run_id=run_id,
            worker_id=f"worker-{suffix}-{rank}",
            rank=rank,
            host="localhost",
            idempotency_key=f"worker-{suffix}-{rank}",
        )
        worker_ids.append(resp.worker_id.value)
    return run_id, worker_ids


def _make_state(numel: int = 1000) -> dict:
    return {
        "weights": torch.arange(numel, dtype=torch.float32),
        "bias": torch.ones(8),
        "step_count": 7,
    }


def test_staged_save_commits_and_restores_locally(coordinator_addr, tmp_path):
    with connect(coordinator_addr) as client:
        run_id, (worker_id,) = _register_run_and_workers(client, world_size=1)
        with AsyncCheckpointer(client, run_id, worker_id, tmp_path / "staging") as ckptr:
            state = _make_state()
            handle = ckptr.save(state, step=100)

            # save() must return before the pipeline completes its RPCs —
            # the stall is only the snapshot.
            assert handle.t_stall_s < 5.0

            handle.wait(timeout=30)
            assert handle.globally_committed  # world_size 1 → single vote wins
            assert handle.t_local_s is not None
            assert handle.t_committed_s is not None
            assert handle.t_stall_s < handle.t_committed_s
            assert handle.staging_path.is_file()
            assert handle.size_bytes == handle.staging_path.stat().st_size
            assert handle.uri.startswith("file://")

            restored, info = ckptr.restore_latest()
            assert info["restored_from"] == "local-staging"
            assert info["step"] == 100
            assert torch.equal(restored["weights"], state["weights"])
            assert restored["step_count"] == 7


def test_training_can_mutate_state_after_save_returns(coordinator_addr, tmp_path):
    with connect(coordinator_addr) as client:
        run_id, (worker_id,) = _register_run_and_workers(client, world_size=1)
        with AsyncCheckpointer(client, run_id, worker_id, tmp_path / "staging") as ckptr:
            state = _make_state()
            expected = state["weights"].clone()
            handle = ckptr.save(state, step=1)
            # Simulate the next training step mutating live parameters
            # immediately — the snapshot must be isolated from this.
            state["weights"].add_(999.0)
            handle.wait(timeout=30)
            restored, _ = ckptr.restore_latest()
            assert torch.equal(restored["weights"], expected)


def test_two_worker_2pc_votes_via_async_checkpointer(coordinator_addr, tmp_path):
    with connect(coordinator_addr) as client:
        run_id, worker_ids = _register_run_and_workers(client, world_size=2)
        ckptrs = [
            AsyncCheckpointer(client, run_id, wid, tmp_path / f"staging-{i}")
            for i, wid in enumerate(worker_ids)
        ]
        try:
            handles = [c.save(_make_state(), step=5) for c in ckptrs]
            for h in handles:
                h.wait(timeout=30)
            # Exactly one worker's vote completed the global commit; the
            # other's arrived while the checkpoint was still pending.
            flags = sorted(h.globally_committed for h in handles)
            assert flags == [False, True]
            # After both votes, the checkpoint is the durable restore target.
            restored, info = ckptrs[0].restore_latest()
            assert info["checkpoint_id"] == handles[0].checkpoint_id
            assert torch.equal(restored["bias"], torch.ones(8))
        finally:
            for c in ckptrs:
                c.close()


def test_restore_falls_back_to_remote_copy(coordinator_addr, tmp_path):
    remote = tmp_path / "remote"
    with connect(coordinator_addr) as client:
        run_id, (worker_id,) = _register_run_and_workers(client, world_size=1)
        with AsyncCheckpointer(
            client, run_id, worker_id, tmp_path / "staging", dest=str(remote)
        ) as ckptr:
            state = _make_state()
            handle = ckptr.save(state, step=3)
            handle.wait(timeout=30)
            assert (remote / handle.artifact_name).is_file()

            # Corrupt the local staged copy — checksum mismatch must force
            # the remote fetch, and the fetched artifact must verify.
            handle.staging_path.write_bytes(b"garbage")
            restored, info = ckptr.restore_latest()
            assert info["restored_from"] == "remote"
            assert torch.equal(restored["weights"], state["weights"])


# ── S3 (MinIO) integration ──────────────────────────────────────────────────
# Gated: DTRUNTIME_RUN_MINIO_TESTS=1 plus DTRUNTIME_S3_ENDPOINT /
# DTRUNTIME_S3_ACCESS_KEY / DTRUNTIME_S3_SECRET_KEY, with MinIO running and
# the runtime-uploader binary built (cargo build --release -p runtime-uploader).

UPLOADER_BIN = REPO_ROOT / "target" / "release" / f"runtime-uploader{EXE}"

minio_gated = pytest.mark.skipif(
    os.environ.get("DTRUNTIME_RUN_MINIO_TESTS") != "1",
    reason="set DTRUNTIME_RUN_MINIO_TESTS=1 with MinIO running to enable",
)


@pytest.fixture()
def s3_bucket() -> str:
    boto3 = pytest.importorskip("boto3", reason="boto3 needed to create the test bucket")
    endpoint = os.environ.get("DTRUNTIME_S3_ENDPOINT")
    if not endpoint:
        pytest.skip("DTRUNTIME_S3_ENDPOINT not set")
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=os.environ.get("DTRUNTIME_S3_REGION", "us-east-1"),
        aws_access_key_id=os.environ.get("DTRUNTIME_S3_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("DTRUNTIME_S3_SECRET_KEY"),
    )
    bucket = f"dtr-ckpt-test-{uuid.uuid4().hex[:12]}"
    client.create_bucket(Bucket=bucket)
    yield bucket
    objects = client.list_objects_v2(Bucket=bucket).get("Contents", [])
    for obj in objects:
        client.delete_object(Bucket=bucket, Key=obj["Key"])
    client.delete_bucket(Bucket=bucket)


@minio_gated
def test_s3_restore_survives_total_loss_of_local_staging(
    coordinator_addr, tmp_path, s3_bucket
):
    """The durability claim, end to end: a worker checkpoints to S3 via the
    Rust uploader, its node "dies" (staging dir wiped), and a fresh restore
    pulls the artifact back from S3 — through the Rust downloader — with the
    checksum verified against the committed manifest."""
    if not UPLOADER_BIN.is_file():
        pytest.skip(f"runtime-uploader not built at {UPLOADER_BIN}")
    import shutil

    with connect(coordinator_addr) as client:
        run_id, (worker_id,) = _register_run_and_workers(client, world_size=1)
        staging = tmp_path / "staging"
        dest = f"s3://{s3_bucket}/runs/{run_id}"
        with AsyncCheckpointer(
            client, run_id, worker_id, staging, dest=dest
        ) as ckptr:
            assert ckptr._uploader_bin is not None
            state = _make_state()
            handle = ckptr.save(state, step=42)
            handle.wait(timeout=60)
            assert handle.globally_committed
            assert handle.uri.startswith("s3://")

        # The node dies: every local trace of the checkpoint is gone.
        shutil.rmtree(staging)

        with AsyncCheckpointer(
            client, run_id, worker_id, staging, dest=dest
        ) as fresh:
            # Force the Rust path: if the code fell back to boto3 the test
            # should fail loudly, not silently pass another way.
            fresh._s3_client = lambda: pytest.fail(
                "restore used the boto3 fallback instead of runtime-uploader"
            )
            restored, info = fresh.restore_latest()
            assert info["restored_from"] == "remote"
            assert info["step"] == 42
            assert info["artifact_uri"].startswith("s3://")
            assert torch.equal(restored["weights"], state["weights"])
            assert torch.equal(restored["bias"], state["bias"])
            assert restored["step_count"] == state["step_count"]


@minio_gated
def test_s3_restore_detects_tampered_artifact(coordinator_addr, tmp_path, s3_bucket):
    """Corrupt the object in S3 after commit: restore must fail the manifest
    checksum check, not hand back bad state."""
    if not UPLOADER_BIN.is_file():
        pytest.skip(f"runtime-uploader not built at {UPLOADER_BIN}")
    import shutil

    boto3 = pytest.importorskip("boto3")
    with connect(coordinator_addr) as client:
        run_id, (worker_id,) = _register_run_and_workers(client, world_size=1)
        staging = tmp_path / "staging"
        dest = f"s3://{s3_bucket}/runs/{run_id}"
        with AsyncCheckpointer(client, run_id, worker_id, staging, dest=dest) as ckptr:
            handle = ckptr.save(_make_state(), step=7)
            handle.wait(timeout=60)

        shutil.rmtree(staging)
        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ["DTRUNTIME_S3_ENDPOINT"],
            region_name=os.environ.get("DTRUNTIME_S3_REGION", "us-east-1"),
            aws_access_key_id=os.environ.get("DTRUNTIME_S3_ACCESS_KEY"),
            aws_secret_access_key=os.environ.get("DTRUNTIME_S3_SECRET_KEY"),
        )
        key = handle.uri[len(f"s3://{s3_bucket}/") :]
        s3.put_object(Bucket=s3_bucket, Key=key, Body=b"tampered bytes")

        with AsyncCheckpointer(client, run_id, worker_id, staging, dest=dest) as fresh:
            with pytest.raises(CheckpointError, match="checksum mismatch"):
                fresh.restore_latest()


def test_upload_failure_surfaces_as_checkpoint_error(coordinator_addr, tmp_path):
    with connect(coordinator_addr) as client:
        run_id, (worker_id,) = _register_run_and_workers(client, world_size=1)
        with AsyncCheckpointer(
            client,
            run_id,
            worker_id,
            tmp_path / "staging",
            dest="s3://nonexistent-bucket/prefix",
            uploader_bin=tmp_path / "missing-uploader-binary",
        ) as ckptr:
            handle = ckptr.save(_make_state(), step=9)
            with pytest.raises(CheckpointError):
                handle.wait(timeout=30)
            assert not handle.globally_committed
