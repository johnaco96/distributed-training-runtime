"""Tests for DistributedShardDataset + ShardPrefetcher with a fake client."""

from __future__ import annotations

import hashlib
import types
from collections import Counter
from pathlib import Path

import numpy as np
import pytest
from dtruntime.data.cache import ShardCache
from dtruntime.data.dataset import DistributedShardDataset
from dtruntime.data.formats import NpzFormat
from dtruntime.data.streaming import ShardPrefetcher, ShardSpec


def _write_npz(path: Path, labels: list[int]) -> str:
    features = np.zeros((len(labels), 2), dtype=np.float32)
    for i, lab in enumerate(labels):
        features[i] = [lab, lab]
    np.savez(path, features=features, labels=np.array(labels, dtype=np.int64))
    return hashlib.sha256(path.read_bytes()).hexdigest()


class FakeAssignment:
    def __init__(self, shard_id, uri, checksum):
        self.shard_id = types.SimpleNamespace(value=shard_id)
        self.shard_uri = uri
        self.checksum = checksum
        self.checksum_algorithm = "sha256"
        self.lease_epoch = 1
        self.progress_offset = 0


class FakeClient:
    """Serves a fixed shard set and records progress reports."""

    def __init__(self, assignments):
        self._assignments = assignments
        self.progress_reports = []
        self.cache_hints_seen = []

    def request_shard(self, run_id, worker_id, dataset_id, cached_shard_ids=None, **_kw):
        self.cache_hints_seen.append(cached_shard_ids)
        return types.SimpleNamespace(assignments=self._assignments)

    def report_shard_progress(self, **kwargs):
        self.progress_reports.append(kwargs)
        return types.SimpleNamespace(accepted=True)


def _dataset(tmp_path, client, **kwargs):
    kwargs.setdefault("dataset_format", "npz")
    kwargs.setdefault("cache_dir", str(tmp_path / "cache"))
    return DistributedShardDataset(
        client=client,
        run_id="run-1",
        worker_id="w0",
        dataset_id="dataset",
        **kwargs,
    )


# ── prefetcher ───────────────────────────────────────────────────────────────


def test_prefetcher_streams_all_records_in_order(tmp_path: Path) -> None:
    specs = []
    for s in range(3):
        p = tmp_path / f"shard-{s}.npz"
        _write_npz(p, [s * 10 + i for i in range(4)])
        specs.append(ShardSpec(shard_id=f"shard-{s}", uri=p.resolve().as_uri()))

    cache = ShardCache(tmp_path / "cache")
    done = []
    with ShardPrefetcher(
        specs, NpzFormat(), cache, prefetch_shards=2, prefetch_records=8,
        on_shard_done=lambda spec, n: done.append((spec.shard_id, n)),
    ) as pf:
        labels = [sr.record["label"] for sr in pf]

    assert labels == list(range(0, 4)) + list(range(10, 14)) + list(range(20, 24))
    assert done == [("shard-0", 4), ("shard-1", 4), ("shard-2", 4)]


def test_prefetcher_reraises_producer_errors(tmp_path: Path) -> None:
    spec = ShardSpec(shard_id="bad", uri=(tmp_path / "missing.npz").resolve().as_uri())
    cache = ShardCache(tmp_path / "cache")
    with pytest.raises(Exception):
        with ShardPrefetcher([spec], NpzFormat(), cache) as pf:
            list(pf)


def test_prefetcher_verifies_checksums(tmp_path: Path) -> None:
    p = tmp_path / "shard.npz"
    _write_npz(p, [1, 2, 3])
    spec = ShardSpec(shard_id="s", uri=p.resolve().as_uri(), checksum="deadbeef")
    cache = ShardCache(tmp_path / "cache")
    with pytest.raises(Exception):
        with ShardPrefetcher([spec], NpzFormat(), cache) as pf:
            list(pf)


# ── dataset ──────────────────────────────────────────────────────────────────


def test_dataset_iterates_all_records_and_reports_progress(tmp_path: Path) -> None:
    assignments = []
    for s in range(2):
        p = tmp_path / f"shard-{s}.npz"
        ck = _write_npz(p, [s * 100 + i for i in range(5)])
        assignments.append(FakeAssignment(f"shard-{s}", p.resolve().as_uri(), ck))
    client = FakeClient(assignments)

    ds = _dataset(tmp_path, client, shuffle=False, transform=lambda r: r["label"])
    labels = list(ds)
    assert sorted(labels) == [i for s in range(2) for i in (s * 100 + np.arange(5))]
    # Both shards reported complete.
    assert len(client.progress_reports) == 2
    assert all(r["is_complete"] for r in client.progress_reports)


def test_dataset_epoch_shuffle_is_deterministic_and_reorders(tmp_path: Path) -> None:
    assignments = []
    for s in range(3):
        p = tmp_path / f"shard-{s}.npz"
        ck = _write_npz(p, list(range(s * 10, s * 10 + 6)))
        assignments.append(FakeAssignment(f"shard-{s}", p.resolve().as_uri(), ck))
    client = FakeClient(assignments)

    ds = _dataset(
        tmp_path, client, shuffle=True, seed=123, shuffle_buffer=8,
        transform=lambda r: r["label"],
    )
    ds.set_epoch(0)
    e0a = list(ds)
    ds.set_epoch(0)
    e0b = list(ds)
    ds.set_epoch(1)
    e1 = list(ds)

    assert e0a == e0b, "same epoch reproduces order"
    assert e0a != e1, "different epoch reorders"
    assert Counter(e0a) == Counter(e1), "same multiset every epoch (no data lost)"


def test_dataset_sends_cache_hints_on_second_pass(tmp_path: Path) -> None:
    p = tmp_path / "shard-0.npz"
    ck = _write_npz(p, [1, 2, 3])
    client = FakeClient([FakeAssignment("shard-0", p.resolve().as_uri(), ck)])
    ds = _dataset(tmp_path, client, shuffle=False, transform=lambda r: r["label"])
    list(ds)  # first pass, no hint
    list(ds)  # second pass sends the warm set
    assert client.cache_hints_seen[0] is None
    assert client.cache_hints_seen[1] == ["shard-0"]


def test_dataset_empty_assignment_yields_nothing(tmp_path: Path) -> None:
    ds = _dataset(tmp_path, FakeClient([]), transform=lambda r: r["label"])
    assert list(ds) == []


def test_dataset_splits_shards_across_dataloader_workers(tmp_path, monkeypatch) -> None:
    """Simulate a 3-worker DataLoader: each worker reads a disjoint shard
    slice and their union covers every record exactly once."""
    import dtruntime.data.dataset as dataset_mod

    assignments = []
    for s in range(6):
        p = tmp_path / f"shard-{s}.npz"
        ck = _write_npz(p, list(range(s * 10, s * 10 + 10)))
        assignments.append(FakeAssignment(f"shard-{s}", p.resolve().as_uri(), ck))

    seen: list[int] = []
    num_workers = 3
    for wi in range(num_workers):
        monkeypatch.setattr(
            dataset_mod, "current_dataloader_worker", lambda wi=wi: (wi, num_workers)
        )
        ds = _dataset(
            tmp_path,
            FakeClient(assignments),
            shuffle=True,
            seed=5,
            shuffle_buffer=4,
            transform=lambda r: int(r["label"]),
        )
        ds.set_epoch(0)
        seen.extend(ds)

    assert Counter(seen) == Counter(range(60)), "each record read exactly once across workers"


def test_dataset_works_through_torch_dataloader(tmp_path: Path) -> None:
    torch = pytest.importorskip("torch")
    from dtruntime.data.formats import features_label_transform
    from torch.utils.data import DataLoader

    assignments = []
    for s in range(4):
        p = tmp_path / f"shard-{s}.npz"
        ck = _write_npz(p, list(range(s * 8, s * 8 + 8)))
        assignments.append(FakeAssignment(f"shard-{s}", p.resolve().as_uri(), ck))
    client = FakeClient(assignments)

    ds = _dataset(
        tmp_path, client, shuffle=True, seed=7, shuffle_buffer=4,
        transform=features_label_transform, report_progress=False,
    )
    ds.set_epoch(0)
    loader = DataLoader(ds, batch_size=8)
    seen = 0
    for features, labels in loader:
        assert features.shape[1] == 2
        assert features.dtype == torch.float32
        assert labels.dtype == torch.long
        seen += len(labels)
    assert seen == 32, "all records delivered exactly once"
