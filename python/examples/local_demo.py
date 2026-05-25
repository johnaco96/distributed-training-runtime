from __future__ import annotations

import argparse
import hashlib
import socket
import subprocess
import time
import uuid
from pathlib import Path

from dtruntime import connect
from dtruntime.generated import runtime_pb2


def _free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_grpc_server(address: str, process: subprocess.Popen[str], timeout_seconds: int = 60) -> None:
    import grpc

    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=1)
            raise RuntimeError(
                f"runtime-coordinator exited before it became reachable at {address}\n"
                f"stdout:\n{stdout}\n"
                f"stderr:\n{stderr}"
            )

        channel = grpc.insecure_channel(address)
        try:
            grpc.channel_ready_future(channel).result(timeout=1)
            return
        except Exception as exc:
            last_error = exc
        finally:
            channel.close()

    raise RuntimeError(f"runtime-coordinator was not reachable at {address}: {last_error}")


def _path_to_file_uri(path: Path) -> str:
    return path.resolve().as_uri()


def run_demo(address: str) -> None:
    with connect(address) as client:
        suffix = uuid.uuid4().hex[:8]
        run_response = client.stub.RegisterRun(
            runtime_pb2.RegisterRunRequest(
                idempotency_key=f"demo-register-run-{suffix}",
                run_name=f"phase10-local-demo-{suffix}",
                assignment_policy=runtime_pb2.ASSIGNMENT_POLICY_STATIC_RANK,
                storage_backend_kind=runtime_pb2.STORAGE_BACKEND_KIND_LOCAL_FILESYSTEM,
                world_size=2,
                run_version="phase-10-demo",
            )
        )
        run_id = run_response.run_id.value
        print(f"Run registered: run_id={run_id}")

        worker_ids = ["worker-0", "worker-1"]
        for rank, worker_id in enumerate(worker_ids):
            client.register_worker(
                run_id=run_id,
                worker_id=worker_id,
                rank=rank,
                host="localhost",
                idempotency_key=f"demo-register-worker-{worker_id}-{suffix}",
            )
            client.heartbeat(
                run_id=run_id,
                worker_id=worker_id,
                idempotency_key=f"demo-heartbeat-{worker_id}-{suffix}",
            )
        print(f"Workers registered and heartbeated: {worker_ids}")

        dataset_id = f"dataset-demo-{suffix}"
        dataset_root = Path(".dtruntime-demo") / "datasets"
        dataset_root.mkdir(parents=True, exist_ok=True)
        shards = []
        for i in range(4):
            shard_path = dataset_root / f"shard-{i}.jsonl"
            shard_path.write_text(f'{{"shard": {i}, "run_id": "{run_id}"}}\n', encoding="utf-8")
            shards.append(
                {
                    "shard_id": f"{dataset_id}-shard-{i}",
                    "shard_uri": _path_to_file_uri(shard_path),
                    "size_bytes": shard_path.stat().st_size,
                }
            )

        dataset_response = client.register_dataset(
            run_id=run_id,
            dataset_id=dataset_id,
            name="phase10-demo-dataset",
            uri=_path_to_file_uri(dataset_root),
            format="jsonl",
            dataset_version="v1",
            idempotency_key=f"demo-register-dataset-{suffix}",
            shards=shards,
        )
        print(f"Dataset registered: dataset_id={dataset_response.dataset_id.value}")

        assignments: dict[str, list[str]] = {}
        for worker_id in worker_ids:
            response = client.request_shard(
                run_id=run_id,
                worker_id=worker_id,
                dataset_id=dataset_id,
                idempotency_key=f"demo-request-shard-{worker_id}-{suffix}",
            )
            assignments[worker_id] = [a.shard_id.value for a in response.assignments]
            print(f"Shard assignment for {worker_id}: {assignments[worker_id]}")

        assert assignments["worker-0"], "worker-0 should receive at least one shard"
        assert assignments["worker-1"], "worker-1 should receive at least one shard"
        assert any(assignments.values()), "assigned shard list should be non-empty"

        checkpoint_id = "checkpoint-demo-1"
        begin_response = client.begin_checkpoint(
            run_id=run_id,
            worker_id="worker-0",
            checkpoint_id=checkpoint_id,
            step=1,
            idempotency_key=f"demo-begin-checkpoint-{suffix}",
        )
        checkpoint_id = begin_response.checkpoint_id.value
        print(f"Checkpoint started: checkpoint_id={checkpoint_id}")

        artifacts_dir = Path(".dtruntime-demo") / "checkpoints" / checkpoint_id
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        artifact_uris: dict[str, str] = {}

        for worker_id in worker_ids:
            artifact_path = artifacts_dir / f"{worker_id}-model.bin"
            content = (
                f"run_id={run_id}\n"
                f"worker_id={worker_id}\n"
                f"checkpoint_id={checkpoint_id}\n"
                f"assigned_shards={','.join(assignments[worker_id])}\n"
            ).encode("utf-8")
            artifact_path.write_bytes(content)
            checksum = hashlib.sha256(content).hexdigest()
            artifact_uri = _path_to_file_uri(artifact_path)
            artifact_uris[worker_id] = artifact_uri

            report_response = client.report_checkpoint_artifact(
                run_id=run_id,
                worker_id=worker_id,
                checkpoint_id=checkpoint_id,
                artifact_name=f"{worker_id}-model.bin",
                artifact_uri=artifact_uri,
                size_bytes=len(content),
                checksum=checksum,
                checksum_algorithm="sha256",
                idempotency_key=f"demo-report-artifact-{worker_id}-{suffix}",
            )
            assert report_response.accepted, f"artifact report for {worker_id} should be accepted"

        print(f"Artifact URIs: {artifact_uris}")

        commit_response = client.commit_checkpoint(
            run_id=run_id,
            checkpoint_id=checkpoint_id,
            idempotency_key=f"demo-commit-checkpoint-{suffix}",
        )
        print(f"Checkpoint commit state: {commit_response.state}")

        # Full stale/failed timeout transitions are covered by Rust tests. This demo only
        # simulates worker restart/rejoin without waiting for production timeout windows.
        print("Simulating worker-1 stop (heartbeats paused briefly).")
        time.sleep(1)
        print("Not waiting for production stale/failure timeout windows in this demo.")
        client.register_worker(
            run_id=run_id,
            worker_id="worker-1",
            rank=1,
            host="localhost",
            idempotency_key=f"demo-reregister-worker-1-{suffix}",
        )
        client.heartbeat(
            run_id=run_id,
            worker_id="worker-1",
            idempotency_key=f"demo-heartbeat-rejoin-worker-1-{suffix}",
        )
        print("worker-1 has rejoined/reactivated.")

        latest_response = client.restore_latest(run_id=run_id)
        manifest = latest_response.manifest
        assert manifest.checkpoint_id.value == checkpoint_id
        assert manifest.state == runtime_pb2.CHECKPOINT_STATE_COMMITTED
        assert len(manifest.artifacts) >= 2
        print(f"Restored latest checkpoint id: {manifest.checkpoint_id.value}")
        print("Local demo completed successfully")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run local end-to-end demo")
    parser.add_argument("--addr", default="", help="Use an existing coordinator address instead of starting one")
    args = parser.parse_args()

    if args.addr:
        print(f"Using externally managed coordinator at {args.addr}")
        run_demo(args.addr)
        return

    repo_root = Path(__file__).resolve().parents[2]
    port = _free_tcp_port()
    address = f"127.0.0.1:{port}"
    print(f"Starting runtime-coordinator at {address}")
    process = subprocess.Popen(
        ["cargo", "run", "-p", "runtime-coordinator", "--", "--addr", address],
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        _wait_for_grpc_server(address, process)
        print(f"Coordinator reachable: {address}")
        run_demo(address)
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)


if __name__ == "__main__":
    main()
