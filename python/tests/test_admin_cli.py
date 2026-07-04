"""Unit tests for the dtruntime-admin CLI (no coordinator required).

Drives :func:`dtruntime.admin.cli.main` against an in-memory fake client that
returns real ``runtime_pb2`` messages, so field access, enum rendering, JSON
output, and structured error mapping are all exercised end to end.
"""

from __future__ import annotations

import json

import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from dtruntime.admin import cli, errors
from dtruntime.generated import runtime_pb2 as pb2


def _ts(seconds: int) -> Timestamp:
    return Timestamp(seconds=seconds)


class FakeRpcError(Exception):
    """Mimics grpc.RpcError's code()/details() interface."""

    def __init__(self, code_name: str, details: str) -> None:
        super().__init__(details)
        self._code_name = code_name
        self._details = details

    def code(self):
        class _Code:
            name = self._code_name

        return _Code()

    def details(self):
        return self._details


class FakeClient:
    """Records calls and returns canned protobuf responses."""

    def __init__(self, **canned):
        self.canned = canned
        self.calls: list[tuple[str, dict]] = []
        self.closed = False

    def _record(self, name, **kwargs):
        self.calls.append((name, kwargs))
        value = self.canned.get(name)
        if isinstance(value, Exception):
            raise value
        if callable(value):
            return value(**kwargs)
        return value

    def list_runs(self, **kw):
        return self._record("list_runs", **kw)

    def list_workers(self, **kw):
        return self._record("list_workers", **kw)

    def list_shard_progress(self, **kw):
        return self._record("list_shard_progress", **kw)

    def list_checkpoints(self, **kw):
        return self._record("list_checkpoints", **kw)

    def cancel_run(self, **kw):
        return self._record("cancel_run", **kw)

    def pause_run(self, **kw):
        return self._record("pause_run", **kw)

    def resume_run(self, **kw):
        return self._record("resume_run", **kw)

    def retry_run(self, **kw):
        return self._record("retry_run", **kw)

    def drain_run(self, **kw):
        return self._record("drain_run", **kw)

    def get_runtime_info(self, **kw):
        return self._record("get_runtime_info", **kw)

    def fetch_checkpoint_artifact(self, **kw):
        return self._record("fetch_checkpoint_artifact", **kw)

    def report_checkpoint_corrupt(self, **kw):
        return self._record("report_checkpoint_corrupt", **kw)

    def close(self):
        self.closed = True


@pytest.fixture
def run_cli(monkeypatch):
    """Return a helper that runs the CLI against a supplied FakeClient."""

    def _run(argv, client):
        monkeypatch.setattr(cli, "_build_client", lambda address, token=None: client)
        return cli.main(argv)

    return _run


def _run_summary(run_id="run-1", state=pb2.RUN_STATE_RUNNING, **kw):
    defaults = dict(
        run_id=pb2.RunId(value=run_id),
        run_name="job",
        state=state,
        state_reason="",
        world_size=4,
        active_worker_count=4,
        retry_count=0,
        registered_at=_ts(1_700_000_000),
    )
    defaults.update(kw)
    return pb2.RunSummary(**defaults)


# ── list / describe ─────────────────────────────────────────────────────────


def test_runs_list_human_output(run_cli, capsys):
    resp = pb2.ListRunsResponse(runs=[_run_summary()], server_time=_ts(1_700_000_100))
    client = FakeClient(list_runs=resp)
    code = run_cli(["runs", "list"], client)
    out = capsys.readouterr().out
    assert code == errors.EXIT_OK
    assert "run-1" in out and "running" in out
    # No state filter -> RUN_STATE_UNSPECIFIED (0).
    assert client.calls[0][1]["state_filter"] == 0
    assert client.closed


def test_runs_list_json_and_state_filter(run_cli, capsys):
    resp = pb2.ListRunsResponse(runs=[_run_summary(state=pb2.RUN_STATE_FAILED)])
    client = FakeClient(list_runs=resp)
    code = run_cli(["--json", "runs", "list", "--state", "failed"], client)
    out = capsys.readouterr().out
    assert code == errors.EXIT_OK
    payload = json.loads(out)
    assert payload[0]["state"] == "failed"
    assert client.calls[0][1]["state_filter"] == pb2.RUN_STATE_FAILED


def test_runs_list_bad_state_is_usage_error(run_cli, capsys):
    client = FakeClient(list_runs=pb2.ListRunsResponse())
    code = run_cli(["runs", "list", "--state", "nonsense"], client)
    err = capsys.readouterr().err
    assert code == errors.EXIT_USAGE
    assert "unknown run state" in err


def test_runs_describe(run_cli, capsys):
    runs = pb2.ListRunsResponse(runs=[_run_summary(state_reason="all good")])
    workers = pb2.ListWorkersResponse(
        workers=[
            pb2.WorkerInfo(
                worker_id=pb2.WorkerId(value="w0"),
                rank=0,
                host="node-a",
                state=pb2.WORKER_STATE_ACTIVE,
            )
        ],
        server_time=_ts(1_700_000_100),
    )
    client = FakeClient(list_runs=runs, list_workers=workers)
    code = run_cli(["runs", "describe", "run-1"], client)
    out = capsys.readouterr().out
    assert code == errors.EXIT_OK
    assert "all good" in out and "w0" in out and "node-a" in out


def test_describe_unknown_run_is_not_found(run_cli, capsys):
    client = FakeClient(list_runs=pb2.ListRunsResponse())
    code = run_cli(["runs", "describe", "ghost"], client)
    err = capsys.readouterr().err
    assert code == errors.EXIT_NOT_FOUND
    assert "ghost" in err


# ── run actions ─────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "action,method,state",
    [
        ("pause", "pause_run", pb2.RUN_STATE_PAUSED),
        ("resume", "resume_run", pb2.RUN_STATE_RUNNING),
        ("retry", "retry_run", pb2.RUN_STATE_PENDING),
        ("drain", "drain_run", pb2.RUN_STATE_DRAINING),
    ],
)
def test_run_actions(run_cli, capsys, action, method, state):
    resp = pb2.RunActionResponse(state=state, state_reason="by admin")
    client = FakeClient(**{method: resp})
    code = run_cli(["run", action, "run-1", "--reason", "maint"], client)
    out = capsys.readouterr().out
    assert code == errors.EXIT_OK
    assert method in client.calls[0][0]
    assert client.calls[0][1]["reason"] == "maint"
    assert cli._run_state_label(pb2, state) in out


def test_run_cancel(run_cli, capsys):
    resp = pb2.CancelRunResponse(state=pb2.RUN_STATE_CANCELLED)
    client = FakeClient(cancel_run=resp)
    code = run_cli(["run", "cancel", "run-1"], client)
    out = capsys.readouterr().out
    assert code == errors.EXIT_OK
    assert "cancelled" in out


def test_pause_invalid_state_maps_to_failed_precondition(run_cli, capsys):
    client = FakeClient(
        pause_run=FakeRpcError("FAILED_PRECONDITION", "cannot pause run run-1 while it is completed")
    )
    code = run_cli(["run", "pause", "run-1"], client)
    err = capsys.readouterr().err
    assert code == errors.EXIT_INVALID_STATE
    assert "cannot pause" in err
    assert "hint:" in err


def test_unavailable_maps_to_connection_error(run_cli, capsys):
    client = FakeClient(list_runs=FakeRpcError("UNAVAILABLE", "failed to connect"))
    code = run_cli(["--json", "runs", "list"], client)
    err = capsys.readouterr().err
    assert code == errors.EXIT_UNAVAILABLE
    payload = json.loads(err)
    assert payload["status"] == "UNAVAILABLE"
    assert "remediation" in payload


# ── workers / shards / checkpoints ──────────────────────────────────────────


def test_workers_list_with_deadlines(run_cli, capsys):
    resp = pb2.ListWorkersResponse(
        workers=[
            pb2.WorkerInfo(
                worker_id=pb2.WorkerId(value="w0"),
                rank=0,
                host="node-a",
                state=pb2.WORKER_STATE_ACTIVE,
                sdk_version="0.1.0",
                last_heartbeat_at=_ts(1_700_000_050),
                next_heartbeat_deadline=_ts(1_700_000_110),
                failure_deadline=_ts(1_700_000_130),
            )
        ],
        server_time=_ts(1_700_000_100),
    )
    client = FakeClient(list_workers=resp)
    code = run_cli(["workers", "list", "run-1"], client)
    out = capsys.readouterr().out
    assert code == errors.EXIT_OK
    assert "w0" in out and "active" in out
    assert "+10s" in out  # heartbeat deadline 10s in the future


def test_shards_list_json(run_cli, capsys):
    resp = pb2.ListShardProgressResponse(
        shards=[
            pb2.ShardProgressInfo(
                shard_id=pb2.ShardId(value="s0"),
                dataset_id=pb2.DatasetId(value="d0"),
                worker_id=pb2.WorkerId(value="w0"),
                lease_epoch=3,
                progress_offset=128,
                is_complete=False,
                updated_at=_ts(1_700_000_090),
            )
        ]
    )
    client = FakeClient(list_shard_progress=resp)
    code = run_cli(["--json", "shards", "list", "run-1", "--dataset", "d0"], client)
    out = capsys.readouterr().out
    assert code == errors.EXIT_OK
    payload = json.loads(out)
    assert payload[0]["shard_id"] == "s0"
    assert payload[0]["progress_offset"] == 128
    assert client.calls[0][1]["dataset_id"] == "d0"


def _manifest(cid="ckpt-1", state=pb2.CHECKPOINT_STATE_COMMITTED, artifacts=None):
    return pb2.CheckpointManifest(
        run_id=pb2.RunId(value="run-1"),
        checkpoint_id=pb2.CheckpointId(value=cid),
        step=10,
        state=state,
        artifacts=artifacts or [],
        committed_at=_ts(1_700_000_080),
    )


def test_checkpoints_list(run_cli, capsys):
    art = pb2.ArtifactReport(
        artifact_name="shard.pt",
        artifact_uri="s3://b/run-1/checkpoints/ckpt-1/workers/w0/shard.pt",
        size_bytes=1024,
        checksum="abc",
    )
    resp = pb2.ListCheckpointsResponse(checkpoints=[_manifest(artifacts=[art])])
    client = FakeClient(list_checkpoints=resp)
    code = run_cli(["checkpoints", "list", "run-1"], client)
    out = capsys.readouterr().out
    assert code == errors.EXIT_OK
    assert "ckpt-1" in out and "committed" in out and "1024" in out


# ── checkpoint verify ───────────────────────────────────────────────────────


def test_checkpoint_verify_ok(run_cli, capsys):
    art = pb2.ArtifactReport(
        artifact_name="shard.pt",
        artifact_uri="s3://b/run-1/checkpoints/ckpt-1/workers/w0/shard.pt",
        size_bytes=8,
        checksum="deadbeef",
    )
    resp = pb2.ListCheckpointsResponse(checkpoints=[_manifest(artifacts=[art])])

    def fetch(**kw):
        # Owner correctly parsed from the uri.
        assert kw["owner_worker_id"] == "w0"
        return (8, "deadbeef")

    client = FakeClient(list_checkpoints=resp, fetch_checkpoint_artifact=fetch)
    code = run_cli(["checkpoint", "verify", "run-1", "ckpt-1"], client)
    out = capsys.readouterr().out
    assert code == errors.EXIT_OK
    assert "OK" in out


def test_checkpoint_verify_checksum_mismatch_reports_corrupt(run_cli, capsys):
    art = pb2.ArtifactReport(
        artifact_name="shard.pt",
        artifact_uri="s3://b/run-1/checkpoints/ckpt-1/workers/w0/shard.pt",
        size_bytes=8,
        checksum="expected",
    )
    resp = pb2.ListCheckpointsResponse(checkpoints=[_manifest(artifacts=[art])])
    client = FakeClient(
        list_checkpoints=resp,
        fetch_checkpoint_artifact=lambda **kw: (8, "actual"),
        report_checkpoint_corrupt=pb2.ReportCheckpointCorruptResponse(),
    )
    code = run_cli(
        ["checkpoint", "verify", "run-1", "ckpt-1", "--report-corrupt"], client
    )
    out = capsys.readouterr().out
    assert code == errors.EXIT_INVALID_STATE
    assert "checksum mismatch" in out
    assert any(name == "report_checkpoint_corrupt" for name, _ in client.calls)


def test_checkpoint_verify_unknown_checkpoint(run_cli, capsys):
    resp = pb2.ListCheckpointsResponse(checkpoints=[])
    client = FakeClient(list_checkpoints=resp)
    code = run_cli(["checkpoint", "verify", "run-1", "ghost"], client)
    err = capsys.readouterr().err
    assert code == errors.EXIT_NOT_FOUND


# ── doctor ──────────────────────────────────────────────────────────────────


def test_doctor_coordinator_probe(run_cli, capsys):
    info = pb2.GetRuntimeInfoResponse(
        protocol_version=pb2.ProtocolVersion(major=1, minor=2, patch=3),
        capabilities=pb2.RuntimeCapabilities(features=["checkpoint-cleanup-library"]),
    )
    client = FakeClient(get_runtime_info=info)
    code = run_cli(["doctor"], client)
    out = capsys.readouterr().out
    assert code == errors.EXIT_OK
    assert "protocol 1.2.3" in out


def test_doctor_run_flags_stuck_worker_and_no_checkpoint(run_cli, capsys):
    info = pb2.GetRuntimeInfoResponse(
        protocol_version=pb2.ProtocolVersion(major=1, minor=0, patch=0)
    )
    runs = pb2.ListRunsResponse(
        runs=[_run_summary(state=pb2.RUN_STATE_RUNNING, active_worker_count=1)]
    )
    workers = pb2.ListWorkersResponse(
        workers=[
            pb2.WorkerInfo(
                worker_id=pb2.WorkerId(value="w0"),
                rank=0,
                state=pb2.WORKER_STATE_FAILED,
                failure_deadline=_ts(1_700_000_050),
            )
        ],
        server_time=_ts(1_700_000_100),
    )
    checkpoints = pb2.ListCheckpointsResponse(checkpoints=[])
    client = FakeClient(
        get_runtime_info=info,
        list_runs=runs,
        list_workers=workers,
        list_checkpoints=checkpoints,
    )
    code = run_cli(["doctor", "run-1"], client)
    out = capsys.readouterr().out
    assert code == errors.EXIT_INVALID_STATE
    assert "w0" in out
    assert "no committed checkpoint" in out
