from __future__ import annotations

import importlib.util
import json
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace

import pytest


def _load_module():
    if importlib.util.find_spec("torch") is None:
        pytest.skip("torch not installed")
    module_path = Path(__file__).resolve().parents[1] / "examples" / "pytorch_e2e_training_benchmark.py"
    spec = importlib.util.spec_from_file_location("pytorch_e2e_training_benchmark", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_stall_percent_calculation() -> None:
    bench = _load_module()
    assert bench.calculate_stall_percent(2.0, 10.0) == 20.0


def test_checkpoint_writer_writes_actual_bytes(tmp_path: Path) -> None:
    bench = _load_module()
    p = tmp_path / "a.bin"
    size = bench.write_checkpoint_bytes(p, 4096)
    assert size == 4096


def test_summary_schema_generation(tmp_path: Path) -> None:
    bench = _load_module()
    result = bench.BenchmarkResult(
        total_duration_seconds=1,
        total_steps=1,
        samples_per_second=1,
        step_time_p50_ms=1,
        step_time_p95_ms=1,
        step_time_p99_ms=1,
        checkpoint_count=1,
        checkpoint_bytes_total=1,
        checkpoint_write_seconds_total=1,
        checkpoint_write_p50_ms=1,
        checkpoint_write_p95_ms=1,
        checkpoint_write_p99_ms=1,
        training_stall_seconds_total=1,
        stall_percent=1,
        checkpoint_throughput_mb_s=1,
        restore_latest_seconds=None,
        worker_id="w",
        shard_count=1,
        backend="localfs",
        mode="baseline-blocking",
    )
    p = tmp_path / "summary.json"
    p.write_text(json.dumps(result.__dict__))
    payload = json.loads(p.read_text())
    assert payload["mode"] == "baseline-blocking"


def test_script_contains_s3_env_and_no_real_secret() -> None:
    text = Path("k8s/benchmark/training-jobs.yaml").read_text()
    assert "DTRUNTIME_S3_ENDPOINT" in text
    assert "DTRUNTIME_S3_SECRET_ACCESS_KEY" in text
    assert "AKIA" not in text


def test_unavailable_metrics_recorded() -> None:
    text = Path("scripts/run_e2e_training_profile.sh").read_text()
    assert "unavailable:" in text


def test_report_sections_present() -> None:
    text = Path("scripts/run_e2e_training_benchmark_k8s.sh").read_text()
    for section in ["## environment", "## Kubernetes context", "## checkpoint stall comparison", "## what this does not prove"]:
        assert section in text


def _benchmark_args(mode: str, output_dir: Path) -> Namespace:
    return Namespace(
        mode=mode,
        backend="localfs",
        samples=32,
        steps=2,
        batch_size=8,
        checkpoint_every_steps=1,
        checkpoint_bytes=1024,
        input_dim=8,
        num_classes=4,
        seed=7,
        run_id="test-run",
        worker_id="worker-0",
        shard_count=2,
        output_dir=str(output_dir),
        json_out=None,
    )


def test_dtruntime_managed_uses_runtime_client_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    bench = _load_module()

    calls: list[str] = []

    class FakeClient:
        def __init__(self, address: str) -> None:
            assert address == "http://127.0.0.1:50051"
            self._runtime_pb2 = SimpleNamespace(
                ASSIGNMENT_POLICY_STATIC_RANK=1,
                STORAGE_BACKEND_KIND_LOCAL_FILESYSTEM=1,
                STORAGE_BACKEND_KIND_S3_COMPATIBLE=2,
            )

        def register_run(self, **_: object) -> SimpleNamespace:
            calls.append("register_run")
            return SimpleNamespace(run_id=SimpleNamespace(value="run-1"))

        def register_worker(self, **_: object) -> None:
            calls.append("register_worker")

        def register_dataset(self, **_: object) -> SimpleNamespace:
            calls.append("register_dataset")
            return SimpleNamespace(dataset_id=SimpleNamespace(value="dataset-1"))

        def request_shard(self, **_: object) -> None:
            calls.append("request_shard")

        def begin_checkpoint(self, **_: object) -> SimpleNamespace:
            calls.append("begin_checkpoint")
            return SimpleNamespace(checkpoint_id=SimpleNamespace(value="ckpt-1"))

        def report_checkpoint_artifact(self, **_: object) -> None:
            calls.append("report_checkpoint_artifact")

        def commit_checkpoint(self, **_: object) -> None:
            calls.append("commit_checkpoint")

        def restore_latest(self, **_: object) -> None:
            calls.append("restore_latest")

        def close(self) -> None:
            calls.append("close")

    monkeypatch.setattr(bench, "RuntimeClient", FakeClient)
    result = bench.run_training(_benchmark_args("dtruntime-managed", tmp_path))
    assert result.restore_latest_seconds is not None
    assert "register_run" in calls
    assert "begin_checkpoint" in calls
    assert "report_checkpoint_artifact" in calls
    assert "commit_checkpoint" in calls
    assert "restore_latest" in calls


def test_dtruntime_managed_does_not_fallback_when_coordinator_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    bench = _load_module()

    class FailingClient:
        def __init__(self, _: str) -> None:
            raise RuntimeError("coordinator unreachable")

    monkeypatch.setattr(bench, "RuntimeClient", FailingClient)

    with pytest.raises(RuntimeError, match="coordinator unreachable"):
        bench.run_training(_benchmark_args("dtruntime-managed", tmp_path))
