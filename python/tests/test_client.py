from __future__ import annotations

import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dataclasses import dataclass, field
from typing import Optional
from unittest.mock import Mock

from dtruntime.client import RuntimeClient


@dataclass
class RunId:
    value: str


@dataclass
class WorkerId:
    value: str


@dataclass
class ShardId:
    value: str


@dataclass
class DatasetId:
    value: str


@dataclass
class DatasetMetadata:
    dataset_id: Optional[DatasetId] = None
    name: str = ""
    dataset_uri: str = ""
    format: str = ""
    shard_count: int = 0
    dataset_version: str = ""
    labels: dict[str, str] = field(default_factory=dict)


@dataclass
class ShardMetadata:
    shard_id: Optional[ShardId] = None
    dataset_id: Optional[DatasetId] = None
    shard_uri: str = ""
    byte_range_start: int = 0
    byte_range_end: int = 0
    size_bytes: int = 0
    checksum: str = ""
    checksum_algorithm: str = ""


@dataclass
class RegisterDatasetRequest:
    idempotency_key: str = ""
    run_id: Optional[RunId] = None
    dataset: Optional[DatasetMetadata] = None
    shards: list[ShardMetadata] = field(default_factory=list)


@dataclass
class RegisterDatasetResponse:
    dataset_id: Optional[DatasetId] = None
    accepted_shard_count: int = 0


@dataclass
class RegisterWorkerRequest:
    idempotency_key: str = ""
    run_id: Optional[RunId] = None
    worker_id: Optional[WorkerId] = None
    rank: int = 0
    host: str = ""
    sdk_version: str = ""
    protocol_version: str = ""


@dataclass
class RegisterWorkerResponse:
    worker_id: Optional[WorkerId] = None


@dataclass
class HeartbeatRequest:
    run_id: Optional[RunId] = None
    worker_id: Optional[WorkerId] = None
    idempotency_key: str = ""
    active_shard_ids: list[ShardId] = field(default_factory=list)


@dataclass
class HeartbeatResponse:
    state: int = 0


def install_fake_generated_modules(monkeypatch):
    channel = object()
    grpc_module = types.SimpleNamespace(insecure_channel=Mock(return_value=channel))
    runtime_pb2 = types.SimpleNamespace(
        RunId=RunId,
        WorkerId=WorkerId,
        ShardId=ShardId,
        DatasetId=DatasetId,
        DatasetMetadata=DatasetMetadata,
        ShardMetadata=ShardMetadata,
        RegisterDatasetRequest=RegisterDatasetRequest,
        RegisterDatasetResponse=RegisterDatasetResponse,
        RegisterWorkerRequest=RegisterWorkerRequest,
        RegisterWorkerResponse=RegisterWorkerResponse,
        HeartbeatRequest=HeartbeatRequest,
        HeartbeatResponse=HeartbeatResponse,
    )
    stub = Mock()
    runtime_pb2_grpc = types.SimpleNamespace(
        CoordinatorServiceStub=Mock(return_value=stub),
    )

    monkeypatch.setitem(sys.modules, "grpc", grpc_module)
    monkeypatch.setitem(sys.modules, "dtruntime.generated.runtime_pb2", runtime_pb2)
    monkeypatch.setitem(sys.modules, "dtruntime.generated.runtime_pb2_grpc", runtime_pb2_grpc)

    return grpc_module, runtime_pb2_grpc, stub, channel


def test_register_worker_builds_request_and_calls_stub(monkeypatch):
    _, runtime_pb2_grpc, stub, channel = install_fake_generated_modules(monkeypatch)
    response = RegisterWorkerResponse(worker_id=WorkerId(value="worker-1"))
    stub.RegisterWorker.return_value = response

    client = RuntimeClient("127.0.0.1:50051")
    result = client.register_worker(
        run_id="run-1",
        worker_id="worker-1",
        rank=3,
        host="host-a",
        sdk_version="0.1.0-test",
        protocol_version="runtime.v1-test",
        idempotency_key="idem-1",
    )

    runtime_pb2_grpc.CoordinatorServiceStub.assert_called_once_with(channel)
    stub.RegisterWorker.assert_called_once_with(
        RegisterWorkerRequest(
            idempotency_key="idem-1",
            run_id=RunId(value="run-1"),
            worker_id=WorkerId(value="worker-1"),
            rank=3,
            host="host-a",
            sdk_version="0.1.0-test",
            protocol_version="runtime.v1-test",
        )
    )
    assert result == response


def test_register_dataset_builds_request_and_calls_stub(monkeypatch):
    _, _, stub, _ = install_fake_generated_modules(monkeypatch)
    response = RegisterDatasetResponse(
        dataset_id=DatasetId(value="dataset-1"), accepted_shard_count=2
    )
    stub.RegisterDataset.return_value = response

    client = RuntimeClient("127.0.0.1:50051")
    result = client.register_dataset(
        run_id="run-1",
        name="training",
        uri="s3://bucket/datasets/training",
        format="bin",
        shards=[
            {
                "shard_id": "shard-0",
                "uri": "file:///data/shard-0.bin",
                "byte_range_start": 0,
                "byte_range_end": 1024,
                "size_bytes": 1024,
                "checksum": "abc",
                "checksum_algorithm": "sha256",
            },
            {
                "shard_id": "shard-1",
                "shard_uri": "file:///data/shard-1.bin",
                "byte_range_start": 1024,
                "byte_range_end": 2048,
                "size_bytes": 1024,
                "checksum": "def",
                "checksum_algorithm": "sha256",
            },
        ],
        dataset_id="dataset-1",
        dataset_version="v1",
        labels={"split": "train", "source": "unit-test"},
        idempotency_key="idem-dataset-1",
    )

    stub.RegisterDataset.assert_called_once_with(
        RegisterDatasetRequest(
            idempotency_key="idem-dataset-1",
            run_id=RunId(value="run-1"),
            dataset=DatasetMetadata(
                dataset_id=DatasetId(value="dataset-1"),
                name="training",
                dataset_uri="s3://bucket/datasets/training",
                format="bin",
                shard_count=2,
                dataset_version="v1",
                labels={"split": "train", "source": "unit-test"},
            ),
            shards=[
                ShardMetadata(
                    shard_id=ShardId(value="shard-0"),
                    dataset_id=DatasetId(value="dataset-1"),
                    shard_uri="file:///data/shard-0.bin",
                    byte_range_start=0,
                    byte_range_end=1024,
                    size_bytes=1024,
                    checksum="abc",
                    checksum_algorithm="sha256",
                ),
                ShardMetadata(
                    shard_id=ShardId(value="shard-1"),
                    dataset_id=DatasetId(value="dataset-1"),
                    shard_uri="file:///data/shard-1.bin",
                    byte_range_start=1024,
                    byte_range_end=2048,
                    size_bytes=1024,
                    checksum="def",
                    checksum_algorithm="sha256",
                ),
            ],
        )
    )
    assert result == response


def test_heartbeat_builds_request_and_converts_active_shard_ids(monkeypatch):
    _, _, stub, _ = install_fake_generated_modules(monkeypatch)
    response = HeartbeatResponse(state=2)
    stub.Heartbeat.return_value = response

    client = RuntimeClient("127.0.0.1:50051")
    result = client.heartbeat(
        run_id="run-1",
        worker_id="worker-1",
        active_shard_ids=["shard-a", "shard-b"],
        idempotency_key="idem-2",
    )

    stub.Heartbeat.assert_called_once_with(
        HeartbeatRequest(
            run_id=RunId(value="run-1"),
            worker_id=WorkerId(value="worker-1"),
            idempotency_key="idem-2",
            active_shard_ids=[ShardId(value="shard-a"), ShardId(value="shard-b")],
        )
    )
    assert result == response
