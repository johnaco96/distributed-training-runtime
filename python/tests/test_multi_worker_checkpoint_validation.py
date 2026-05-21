from __future__ import annotations

import importlib.util
from argparse import Namespace
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "examples" / "multi_worker_checkpoint_validation.py"
spec = importlib.util.spec_from_file_location("multi_worker_checkpoint_validation", MODULE_PATH)
validation = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(validation)


class _Obj:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class FakeClient:
    def __init__(self, addr: str):
        self.addr = addr
        self._runtime_pb2 = _Obj(
            ASSIGNMENT_POLICY_STATIC_RANK=1,
            STORAGE_BACKEND_KIND_LOCAL_FILESYSTEM=1,
        )

    def register_run(self, **kwargs):
        return _Obj(run_id=_Obj(value="run-123"))

    def register_dataset(self, **kwargs):
        return _Obj(dataset_id=_Obj(value="dataset-123"))

    def register_worker(self, **kwargs):
        return _Obj()

    def request_shard(self, **kwargs):
        rank = int(kwargs["worker_id"].split("-")[-1])
        return _Obj(assignments=[_Obj(shard_id=_Obj(value=f"shard-{rank}"), lease_epoch=1, lease_state=2)])

    def begin_checkpoint(self, **kwargs):
        return _Obj(checkpoint_id=_Obj(value=kwargs["checkpoint_id"]))

    def report_checkpoint_artifact(self, **kwargs):
        return _Obj()

    def commit_checkpoint(self, **kwargs):
        return _Obj(state=4)

    def restore_latest(self, run_id: str):
        artifacts = [
            _Obj(artifact_name=f"rank-{i}.bin", artifact_uri=f"u{i}", size_bytes=123, checksum="abc")
            for i in range(4)
        ]
        return _Obj(manifest=_Obj(state=4, checkpoint_id=_Obj(value="global-ckpt"), step=4, artifacts=artifacts))

    def close(self):
        return None


def test_multi_worker_validation_writes_all_artifacts(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(validation, "RuntimeClient", FakeClient)
    args = Namespace(
        coordinator_addr="127.0.0.1:50051",
        run_id="integration-run",
        checkpoint_id="global-ckpt",
        world_size=4,
        shard_count=4,
        step=4,
        output_dir=str(tmp_path),
        json_out=str(tmp_path / "result.json"),
    )

    result = validation.run_validation(args)

    assert result["runtime_run_id"] == "run-123"
    assert result["checkpoint_id"] == "global-ckpt"
    assert len(result["artifacts"]) == 4
    assert all(a["exists"] for a in result["artifacts"])
    assert result["restore_latest"]["artifact_count"] == 4
