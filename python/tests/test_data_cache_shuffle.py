"""Unit tests for the shard cache, shuffling, versioning, and locality."""

from __future__ import annotations

import hashlib
from collections import Counter
from pathlib import Path

import pytest

from dtruntime.data.cache import ChecksumMismatch, ShardCache
from dtruntime.data.locality import split_for_dataloader_worker
from dtruntime.data.shuffle import derive_seed, reservoir_shuffle, shuffled_order
from dtruntime.data.versioning import (
    compute_dataset_version,
    verify_dataset_version,
)


# ── cache ────────────────────────────────────────────────────────────────────


def _fake_fetch(payloads: dict[str, bytes]):
    def fetch(uri: str, **_kw) -> bytes:
        return payloads[uri]

    return fetch


def test_cache_downloads_once_then_hits(tmp_path: Path) -> None:
    data = b"shard-bytes"
    checksum = hashlib.sha256(data).hexdigest()
    calls = {"n": 0}

    def fetch(uri, **_kw):
        calls["n"] += 1
        return data

    cache = ShardCache(tmp_path, fetch=fetch)
    p1 = cache.get("s3://b/x", checksum)
    p2 = cache.get("s3://b/x", checksum)
    assert p1 == p2
    assert p1.read_bytes() == data
    assert calls["n"] == 1
    assert cache.hits == 1 and cache.misses == 1


def test_cache_rejects_checksum_mismatch(tmp_path: Path) -> None:
    cache = ShardCache(tmp_path, fetch=lambda uri, **_k: b"bad")
    with pytest.raises(ChecksumMismatch):
        cache.get("s3://b/x", hashlib.sha256(b"good").hexdigest())


def test_cache_evicts_lru_when_over_budget(tmp_path: Path) -> None:
    payloads = {f"u{i}": bytes([i]) * 100 for i in range(4)}
    cache = ShardCache(tmp_path, max_bytes=250, fetch=_fake_fetch(payloads))
    # Each shard is 100 bytes; budget 250 holds at most 2.
    for i in range(3):
        cache.get(f"u{i}")
        cache.unpin(f"u{i}")
    assert cache.total_bytes <= 250
    assert cache.evictions >= 1
    # The oldest (u0) should have been evicted first.
    assert not cache.contains("u0")
    assert cache.contains("u2")


def test_cache_does_not_evict_pinned(tmp_path: Path) -> None:
    payloads = {f"u{i}": bytes([i]) * 100 for i in range(3)}
    cache = ShardCache(tmp_path, max_bytes=150, fetch=_fake_fetch(payloads))
    cache.get("u0", pin=True)  # pinned, must survive
    cache.get("u1")
    cache.unpin("u1")
    cache.get("u2")
    cache.unpin("u2")
    assert cache.contains("u0"), "pinned shard must not be evicted"


def test_cache_adopts_existing_files_on_restart(tmp_path: Path) -> None:
    data = b"warm"
    checksum = hashlib.sha256(data).hexdigest()
    c1 = ShardCache(tmp_path, fetch=lambda uri, **_k: data)
    c1.get("u", checksum)
    # New cache over the same dir: the shard is already warm (no fetch).
    c2 = ShardCache(tmp_path, fetch=lambda uri, **_k: (_ for _ in ()).throw(AssertionError("refetched")))
    assert c2.contains("u")
    assert c2.get("u", checksum).read_bytes() == data


# ── shuffle ──────────────────────────────────────────────────────────────────


def test_derive_seed_is_deterministic_and_epoch_sensitive() -> None:
    assert derive_seed(7, 0) == derive_seed(7, 0)
    assert derive_seed(7, 0) != derive_seed(7, 1)
    assert derive_seed(7, 0, "shards") != derive_seed(7, 0, "records")


def test_shuffled_order_is_deterministic_per_epoch_and_permutes() -> None:
    items = list(range(20))
    e0a = shuffled_order(items, 42, 0)
    e0b = shuffled_order(items, 42, 0)
    e1 = shuffled_order(items, 42, 1)
    assert e0a == e0b, "same (seed, epoch) must reproduce"
    assert e0a != e1, "different epoch must reorder"
    assert sorted(e0a) == items, "must be a permutation"


def test_reservoir_shuffle_preserves_multiset_and_reorders() -> None:
    items = list(range(200))
    out = list(reservoir_shuffle(iter(items), buffer_size=32, base_seed=1, epoch=0))
    assert Counter(out) == Counter(items), "no records lost or duplicated"
    assert out != items, "buffer shuffle should reorder"
    # Deterministic given (seed, epoch).
    out2 = list(reservoir_shuffle(iter(items), buffer_size=32, base_seed=1, epoch=0))
    assert out == out2


def test_reservoir_shuffle_buffer_one_is_passthrough() -> None:
    items = list(range(10))
    assert list(reservoir_shuffle(iter(items), 1, 0, 0)) == items


# ── locality ─────────────────────────────────────────────────────────────────


def test_split_for_dataloader_worker_partitions_without_overlap() -> None:
    shards = list(range(10))
    parts = [split_for_dataloader_worker(shards, w, 3) for w in range(3)]
    # Disjoint and complete.
    flat = sorted(x for p in parts for x in p)
    assert flat == shards
    assert all(set(a).isdisjoint(b) for i, a in enumerate(parts) for b in parts[i + 1 :])


def test_split_single_worker_gets_everything() -> None:
    shards = list(range(5))
    assert split_for_dataloader_worker(shards, 0, 1) == shards


# ── versioning ───────────────────────────────────────────────────────────────


def _manifest(*pairs):
    return [{"shard_id": sid, "checksum": ck} for sid, ck in pairs]


def test_dataset_version_is_content_addressed_and_order_independent() -> None:
    a = _manifest(("s0", "aa"), ("s1", "bb"))
    b = _manifest(("s1", "bb"), ("s0", "aa"))  # reordered
    assert compute_dataset_version(a) == compute_dataset_version(b)
    assert compute_dataset_version(a).startswith("sha256:")


def test_dataset_version_changes_when_content_changes() -> None:
    a = _manifest(("s0", "aa"), ("s1", "bb"))
    changed = _manifest(("s0", "aa"), ("s1", "CHANGED"))
    added = _manifest(("s0", "aa"), ("s1", "bb"), ("s2", "cc"))
    assert compute_dataset_version(a) != compute_dataset_version(changed)
    assert compute_dataset_version(a) != compute_dataset_version(added)


def test_verify_dataset_version_detects_drift() -> None:
    manifest = _manifest(("s0", "aa"))
    version = compute_dataset_version(manifest)
    verify_dataset_version(version, manifest)  # ok
    with pytest.raises(ValueError, match="version mismatch"):
        verify_dataset_version(version, _manifest(("s0", "DRIFT")))
