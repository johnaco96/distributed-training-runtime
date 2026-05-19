from __future__ import annotations

import os
import socket
import subprocess
import time
import uuid
from pathlib import Path

import pytest


def test_rust_coordinator_and_python_sdk_communicate_over_grpc() -> None:
    if os.environ.get("DTRUNTIME_RUN_LOCAL_INTEGRATION") != "1":
        pytest.skip("set DTRUNTIME_RUN_LOCAL_INTEGRATION=1 to run local gRPC integration test")

    grpc = pytest.importorskip("grpc", reason="grpcio is required for local integration test")
    pytest.importorskip(
        "google.protobuf", reason="protobuf is required for local integration test"
    )
    from dtruntime import connect
    from dtruntime.generated import runtime_pb2

    repo_root = Path(__file__).resolve().parents[2]
    port = _free_tcp_port()
    address = f"127.0.0.1:{port}"
    process = subprocess.Popen(
        ["cargo", "run", "-p", "runtime-coordinator", "--", "--addr", address],
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    client = None
    try:
        _wait_for_grpc_server(grpc, address, process)
        client = connect(address)

        suffix = uuid.uuid4().hex
        run_response = client.stub.RegisterRun(
            runtime_pb2.RegisterRunRequest(
                idempotency_key=f"register-run-{suffix}",
                run_name=f"local-integration-{suffix}",
                assignment_policy=runtime_pb2.ASSIGNMENT_POLICY_STATIC_RANK,
                storage_backend_kind=runtime_pb2.STORAGE_BACKEND_KIND_LOCAL_FILESYSTEM,
                world_size=1,
                run_version="integration-test",
            )
        )
        run_id = run_response.run_id.value
        assert run_id

        worker_id = f"worker-{suffix}"
        worker_response = client.register_worker(
            run_id=run_id,
            worker_id=worker_id,
            rank=0,
            host="localhost",
            idempotency_key=f"register-worker-{suffix}",
        )
        assert worker_response.worker_id.value == worker_id

        heartbeat_response = client.heartbeat(
            run_id=run_id,
            worker_id=worker_id,
            idempotency_key=f"heartbeat-{suffix}",
        )
        assert heartbeat_response.server_time is not None

        dataset_id = f"dataset-{suffix}"
        dataset_response = client.register_dataset(
            run_id=run_id,
            dataset_id=dataset_id,
            name="local-integration-dataset",
            uri="file:///tmp/dtruntime-local-integration",
            format="jsonl",
            dataset_version="v1",
            idempotency_key=f"register-dataset-{suffix}",
            shards=[
                {
                    "shard_id": f"{dataset_id}-shard-0",
                    "shard_uri": "file:///tmp/dtruntime-local-integration/shard-0.jsonl",
                    "byte_range_start": 0,
                    "byte_range_end": 99,
                    "size_bytes": 100,
                },
                {
                    "shard_id": f"{dataset_id}-shard-1",
                    "shard_uri": "file:///tmp/dtruntime-local-integration/shard-1.jsonl",
                    "byte_range_start": 100,
                    "byte_range_end": 199,
                    "size_bytes": 100,
                },
            ],
        )
        assert dataset_response.dataset_id.value == dataset_id
        assert dataset_response.accepted_shard_count == 2

        shard_response = client.request_shard(
            run_id=run_id,
            worker_id=worker_id,
            dataset_id=dataset_id,
            idempotency_key=f"request-shard-{suffix}",
        )
        assert shard_response.assignments
        assignment = shard_response.assignments[0]
        assert assignment.run_id.value == run_id
        assert assignment.dataset_id.value == dataset_id
        assert assignment.worker_id.value == worker_id
        assert assignment.shard_id.value
        assert assignment.shard_uri
    finally:
        if client is not None:
            client.close()
        _stop_process(process)


def _free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_grpc_server(grpc: object, address: str, process: subprocess.Popen[str]) -> None:
    deadline = time.monotonic() + 60
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=1)
            pytest.fail(
                "runtime-coordinator exited before it became reachable\n"
                f"stdout:\n{stdout}\n"
                f"stderr:\n{stderr}"
            )

        channel = grpc.insecure_channel(address)
        try:
            grpc.channel_ready_future(channel).result(timeout=1)
            return
        except Exception as exc:  # grpc raises FutureTimeoutError while the server starts.
            last_error = exc
        finally:
            channel.close()

    pytest.fail(f"runtime-coordinator was not reachable at {address}: {last_error}")


def _stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return

    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)
