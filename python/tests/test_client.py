from __future__ import annotations

import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dataclasses import dataclass, field
from typing import Optional
from unittest.mock import Mock

from dtruntime.client import RuntimeClient
import pytest


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
class CheckpointId:
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


@dataclass
class GetShardAssignmentRequest:
    run_id: Optional[RunId] = None
    worker_id: Optional[WorkerId] = None
    dataset_id: Optional[DatasetId] = None
    idempotency_key: str = ""


@dataclass
class GetShardAssignmentResponse:
    assignments: list[object] = field(default_factory=list)


@dataclass
class BeginCheckpointRequest:
    run_id: Optional[RunId] = None
    worker_id: Optional[WorkerId] = None
    checkpoint_id: Optional[CheckpointId] = None
    step: int = 0
    idempotency_key: str = ""


@dataclass
class BeginCheckpointResponse:
    checkpoint_id: Optional[CheckpointId] = None




@dataclass
class ArtifactReport:
    artifact_name: str = ""
    artifact_uri: str = ""
    size_bytes: int = 0
    checksum: str = ""
    checksum_algorithm: str = ""


@dataclass
class ReportCheckpointArtifactRequest:
    run_id: Optional[RunId] = None
    worker_id: Optional[WorkerId] = None
    checkpoint_id: Optional[CheckpointId] = None
    artifact_report: Optional[ArtifactReport] = None
    idempotency_key: str = ""


@dataclass
class ReportCheckpointArtifactResponse:
    accepted: bool = False
@dataclass
class CommitCheckpointRequest:
    run_id: Optional[RunId] = None
    checkpoint_id: Optional[CheckpointId] = None
    idempotency_key: str = ""


@dataclass
class CommitCheckpointResponse:
    checkpoint_id: Optional[CheckpointId] = None


@dataclass
class GetLatestCheckpointRequest:
    run_id: Optional[RunId] = None


@dataclass
class GetLatestCheckpointResponse:
    manifest: object | None = None


def install_fake_generated_modules(monkeypatch):
    channel = object()
    creds = object()
    grpc_module = types.SimpleNamespace(
        insecure_channel=Mock(return_value=channel),
        ssl_channel_credentials=Mock(return_value=creds),
        secure_channel=Mock(return_value=channel),
    )
    runtime_pb2 = types.SimpleNamespace(
        RunId=RunId,
        WorkerId=WorkerId,
        ShardId=ShardId,
        DatasetId=DatasetId,
        CheckpointId=CheckpointId,
        DatasetMetadata=DatasetMetadata,
        ShardMetadata=ShardMetadata,
        RegisterDatasetRequest=RegisterDatasetRequest,
        RegisterDatasetResponse=RegisterDatasetResponse,
        RegisterWorkerRequest=RegisterWorkerRequest,
        RegisterWorkerResponse=RegisterWorkerResponse,
        HeartbeatRequest=HeartbeatRequest,
        HeartbeatResponse=HeartbeatResponse,
        GetShardAssignmentRequest=GetShardAssignmentRequest,
        GetShardAssignmentResponse=GetShardAssignmentResponse,
        BeginCheckpointRequest=BeginCheckpointRequest,
        BeginCheckpointResponse=BeginCheckpointResponse,
        ArtifactReport=ArtifactReport,
        ReportCheckpointArtifactRequest=ReportCheckpointArtifactRequest,
        ReportCheckpointArtifactResponse=ReportCheckpointArtifactResponse,
        CommitCheckpointRequest=CommitCheckpointRequest,
        CommitCheckpointResponse=CommitCheckpointResponse,
        GetLatestCheckpointRequest=GetLatestCheckpointRequest,
        GetLatestCheckpointResponse=GetLatestCheckpointResponse,
    )
    stub = Mock()
    runtime_pb2_grpc = types.SimpleNamespace(
        CoordinatorServiceStub=Mock(return_value=stub),
    )

    monkeypatch.setitem(sys.modules, "grpc", grpc_module)
    monkeypatch.setitem(sys.modules, "dtruntime.generated.runtime_pb2", runtime_pb2)
    monkeypatch.setitem(sys.modules, "dtruntime.generated.runtime_pb2_grpc", runtime_pb2_grpc)

    return grpc_module, runtime_pb2_grpc, stub, channel, creds



def test_tls_ca_uses_secure_channel(monkeypatch, tmp_path):
    grpc_module, _, _, channel, creds = install_fake_generated_modules(monkeypatch)
    ca = tmp_path / "ca.crt"
    ca.write_text("-----BEGIN CERTIFICATE-----\nreplace-me\n-----END CERTIFICATE-----\n")

    client = RuntimeClient("127.0.0.1:50051", tls_ca_cert=ca)

    grpc_module.ssl_channel_credentials.assert_called_once()
    grpc_module.secure_channel.assert_called_once_with("127.0.0.1:50051", creds)
    grpc_module.insecure_channel.assert_not_called()
    assert client._channel is channel


def test_tls_client_cert_without_key_errors(monkeypatch, tmp_path):
    install_fake_generated_modules(monkeypatch)
    cert = tmp_path / "client.crt"
    cert.write_text("dummy")
    with pytest.raises(ValueError, match="tls_client_cert and tls_client_key"):
        RuntimeClient("127.0.0.1:50051", tls_client_cert=cert)


def test_tls_client_key_without_cert_errors(monkeypatch, tmp_path):
    install_fake_generated_modules(monkeypatch)
    key = tmp_path / "client.key"
    key.write_text("dummy")
    with pytest.raises(ValueError, match="tls_client_cert and tls_client_key"):
        RuntimeClient("127.0.0.1:50051", tls_client_key=key)


def test_tls_paths_from_environment(monkeypatch, tmp_path):
    grpc_module, _, _, _, _ = install_fake_generated_modules(monkeypatch)
    ca = tmp_path / "ca.crt"
    cert = tmp_path / "client.crt"
    key = tmp_path / "client.key"
    ca.write_text("ca")
    cert.write_text("cert")
    key.write_text("key")
    monkeypatch.setenv("DTRUNTIME_TLS_CA_CERT", str(ca))
    monkeypatch.setenv("DTRUNTIME_TLS_CLIENT_CERT", str(cert))
    monkeypatch.setenv("DTRUNTIME_TLS_CLIENT_KEY", str(key))

    RuntimeClient("127.0.0.1:50051")

    grpc_module.secure_channel.assert_called_once()


def test_missing_tls_file_errors(monkeypatch):
    install_fake_generated_modules(monkeypatch)
    with pytest.raises(ValueError, match="failed to read tls_ca_cert"):
        RuntimeClient("127.0.0.1:50051", tls_ca_cert="/does/not/exist/ca.crt")

def test_register_worker_builds_request_and_calls_stub(monkeypatch):
    _, runtime_pb2_grpc, stub, channel, _ = install_fake_generated_modules(monkeypatch)
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
        ),
        metadata=[]
    )
    assert result == response


def test_register_dataset_builds_request_and_calls_stub(monkeypatch):
    _, _, stub, _, _ = install_fake_generated_modules(monkeypatch)
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
        ),
        metadata=[]
    )
    assert result == response


def test_heartbeat_builds_request_and_converts_active_shard_ids(monkeypatch):
    _, _, stub, _, _ = install_fake_generated_modules(monkeypatch)
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
        ),
        metadata=[]
    )
    assert result == response


def test_request_shard_builds_request_and_calls_stub(monkeypatch):
    _, _, stub, _, _ = install_fake_generated_modules(monkeypatch)
    response = GetShardAssignmentResponse(assignments=[object()])
    stub.GetShardAssignment.return_value = response

    client = RuntimeClient("127.0.0.1:50051")
    result = client.request_shard(
        run_id="run-1",
        worker_id="worker-1",
        dataset_id="dataset-1",
        idempotency_key="idem-shard-1",
    )

    stub.GetShardAssignment.assert_called_once_with(
        GetShardAssignmentRequest(
            run_id=RunId(value="run-1"),
            worker_id=WorkerId(value="worker-1"),
            dataset_id=DatasetId(value="dataset-1"),
            idempotency_key="idem-shard-1",
        ),
        metadata=[]
    )
    assert result == response


def test_request_shard_includes_cached_shard_metadata_when_provided(monkeypatch):
    _, _, stub, _, _ = install_fake_generated_modules(monkeypatch)
    response = GetShardAssignmentResponse(assignments=[object()])
    stub.GetShardAssignment.return_value = response
    client = RuntimeClient("127.0.0.1:50051")

    client.request_shard(
        run_id="run-1",
        worker_id="worker-1",
        dataset_id="dataset-1",
        cached_shard_ids=["shard-a", "shard-b"],
    )

    assert stub.GetShardAssignment.call_args.kwargs["metadata"] == [
        ("x-dtruntime-cached-shards", "shard-a,shard-b")
    ]


def test_begin_checkpoint_builds_request_and_calls_stub(monkeypatch):
    _, _, stub, _, _ = install_fake_generated_modules(monkeypatch)
    response = BeginCheckpointResponse(checkpoint_id=CheckpointId(value="ckpt-1"))
    stub.BeginCheckpoint.return_value = response

    client = RuntimeClient("127.0.0.1:50051")
    result = client.begin_checkpoint(
        run_id="run-1",
        worker_id="worker-1",
        step=42,
        checkpoint_id="ckpt-1",
        idempotency_key="idem-checkpoint-1",
    )

    stub.BeginCheckpoint.assert_called_once_with(
        BeginCheckpointRequest(
            run_id=RunId(value="run-1"),
            worker_id=WorkerId(value="worker-1"),
            checkpoint_id=CheckpointId(value="ckpt-1"),
            step=42,
            idempotency_key="idem-checkpoint-1",
        ),
        metadata=[]
    )
    assert result == response




def test_report_checkpoint_artifact_builds_request_and_calls_stub(monkeypatch):
    _, _, stub, _, _ = install_fake_generated_modules(monkeypatch)
    response = ReportCheckpointArtifactResponse(accepted=True)
    stub.ReportCheckpointArtifact.return_value = response

    client = RuntimeClient("127.0.0.1:50051")
    result = client.report_checkpoint_artifact(
        run_id="run-1",
        worker_id="worker-1",
        checkpoint_id="ckpt-1",
        artifact_name="model-state",
        artifact_uri="s3://bucket/checkpoints/ckpt-1/model.pt",
        size_bytes=1024,
        checksum="abc123",
        checksum_algorithm="sha256",
        idempotency_key="idem-artifact-1",
    )

    stub.ReportCheckpointArtifact.assert_called_once_with(
        ReportCheckpointArtifactRequest(
            run_id=RunId(value="run-1"),
            worker_id=WorkerId(value="worker-1"),
            checkpoint_id=CheckpointId(value="ckpt-1"),
            artifact_report=ArtifactReport(
                artifact_name="model-state",
                artifact_uri="s3://bucket/checkpoints/ckpt-1/model.pt",
                size_bytes=1024,
                checksum="abc123",
                checksum_algorithm="sha256",
            ),
            idempotency_key="idem-artifact-1",
        ),
        metadata=[]
    )
    assert result == response


def test_commit_checkpoint_builds_request_and_calls_stub(monkeypatch):
    _, _, stub, _, _ = install_fake_generated_modules(monkeypatch)
    response = CommitCheckpointResponse(checkpoint_id=CheckpointId(value="ckpt-1"))
    stub.CommitCheckpoint.return_value = response

    client = RuntimeClient("127.0.0.1:50051")
    result = client.commit_checkpoint(
        run_id="run-1",
        checkpoint_id="ckpt-1",
        idempotency_key="idem-commit-1",
    )

    stub.CommitCheckpoint.assert_called_once_with(
        CommitCheckpointRequest(
            run_id=RunId(value="run-1"),
            checkpoint_id=CheckpointId(value="ckpt-1"),
            idempotency_key="idem-commit-1",
        ),
        metadata=[]
    )
    assert result == response


def test_restore_latest_builds_request_and_calls_stub(monkeypatch):
    _, _, stub, _, _ = install_fake_generated_modules(monkeypatch)
    manifest = object()
    response = GetLatestCheckpointResponse(manifest=manifest)
    stub.GetLatestCheckpoint.return_value = response

    client = RuntimeClient("127.0.0.1:50051")
    result = client.restore_latest(run_id="run-1")

    stub.GetLatestCheckpoint.assert_called_once_with(
        GetLatestCheckpointRequest(run_id=RunId(value="run-1")),
        metadata=[]
    )
    assert result == response


def test_auth_metadata_from_args(monkeypatch):
    _, _, stub, _, _ = install_fake_generated_modules(monkeypatch)
    client = RuntimeClient("127.0.0.1:50051", auth_subject="worker-0", auth_role="worker", auth_worker_id="worker-0")
    client.heartbeat(run_id="r", worker_id="worker-0")
    stub.Heartbeat.assert_called_once()
    assert stub.Heartbeat.call_args.kwargs["metadata"] == [("x-dtruntime-subject", "worker-0"), ("x-dtruntime-role", "worker"), ("x-dtruntime-worker-id", "worker-0")]

def test_auth_metadata_from_env(monkeypatch):
    _, _, stub, _, _ = install_fake_generated_modules(monkeypatch)
    monkeypatch.setenv("DTRUNTIME_AUTH_SUBJECT", "admin")
    monkeypatch.setenv("DTRUNTIME_AUTH_ROLE", "admin")
    client = RuntimeClient("127.0.0.1:50051")
    client.restore_latest("r")
    assert stub.GetLatestCheckpoint.call_args.kwargs["metadata"] == [("x-dtruntime-subject", "admin"), ("x-dtruntime-role", "admin")]
