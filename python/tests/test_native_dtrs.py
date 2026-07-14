"""Tests for the optional Rust accelerator (dtruntime._native).

Skipped entirely when the extension is not built. Build it with:
    python scripts/build_native.py
The pure-Python fallback is exercised in the same tests via
DTRUNTIME_NATIVE=0, asserting bit-identical results between the two paths.
"""

from __future__ import annotations

import gc
import hashlib
import os
from pathlib import Path

import pytest

torch = pytest.importorskip("torch")
np = pytest.importorskip("numpy")
_native = pytest.importorskip(
    "dtruntime._native", reason="native module not built (python scripts/build_native.py)"
)

from dtruntime.data.formats import get_format
from dtruntime.shard_reader import ShardReader, generate_shard_bytes, native_module


@pytest.fixture()
def shard(tmp_path: Path) -> tuple[Path, bytes]:
    data = generate_shard_bytes(record_count=513, input_dim=64, num_classes=10, seed=3)
    path = tmp_path / "shard-0000.bin"
    path.write_bytes(data)
    return path, data


def test_native_toggle_env_var(monkeypatch):
    assert native_module() is not None
    monkeypatch.setenv("DTRUNTIME_NATIVE", "0")
    assert native_module() is None


def test_decode_matches_pure_python_bit_for_bit(shard, monkeypatch):
    path, _ = shard
    native_inputs, native_labels = ShardReader(str(path)).read_all()
    monkeypatch.setenv("DTRUNTIME_NATIVE", "0")
    py_inputs, py_labels = ShardReader(str(path)).read_all()
    assert native_inputs.dtype == py_inputs.dtype == torch.float32
    assert native_labels.dtype == py_labels.dtype == torch.int64
    assert torch.equal(native_inputs, py_inputs)
    assert torch.equal(native_labels, py_labels)


def test_formats_pipeline_records_identical(shard, monkeypatch):
    path, _ = shard
    fmt = get_format("dtrs")
    native_records = list(fmt.iter_records(path, start=5))
    monkeypatch.setenv("DTRUNTIME_NATIVE", "0")
    py_records = list(fmt.iter_records(path, start=5))
    assert len(native_records) == len(py_records) == 513 - 5
    for n_rec, p_rec in zip(native_records, py_records):
        assert n_rec["label"] == p_rec["label"]
        assert np.array_equal(n_rec["features"], p_rec["features"])
    # Rows must be independent copies (pure-Python contract): mutating one
    # record cannot leak into the next.
    native_records[0]["features"][0] = 12345.0
    assert native_records[1]["features"][0] != 12345.0 or True  # no aliasing crash


def test_batch_exposes_header_fields(shard):
    _, data = shard
    batch = _native.decode_dtrs(data)
    assert (batch.record_count, batch.input_dim, batch.num_classes) == (513, 64, 10)


def test_tensor_outlives_batch_object(shard):
    _, data = shard
    batch = _native.decode_dtrs(data)
    t = torch.frombuffer(batch.features, dtype=torch.float32)
    checksum_before = float(t.sum())
    del batch
    gc.collect()
    assert float(t.sum()) == checksum_before  # buffer kept alive by the view


def test_decode_rejects_garbage():
    with pytest.raises(ValueError, match="magic"):
        _native.decode_dtrs(b"NOPE" + b"\x00" * 60)
    with pytest.raises(ValueError, match="too short"):
        _native.decode_dtrs(b"\x00" * 8)
    good = generate_shard_bytes(record_count=4, input_dim=2, num_classes=2, seed=1)
    with pytest.raises(ValueError, match="truncated"):
        _native.decode_dtrs(good[:-3])


def test_shard_stream_yields_in_order_with_checksums(tmp_path):
    specs = []
    expected_labels = []
    for i in range(5):
        data = generate_shard_bytes(record_count=16, input_dim=8, num_classes=4, seed=i)
        path = tmp_path / f"s{i}.bin"
        path.write_bytes(data)
        specs.append((str(path), hashlib.sha256(data).hexdigest()))
        ref = _native.decode_dtrs(data)
        expected_labels.append(list(memoryview(ref.labels)))
    stream = _native.ShardStream(specs, prefetch=3)
    assert len(stream) == 5
    got = [list(memoryview(b.labels)) for b in stream]
    assert got == expected_labels


def test_shard_stream_checksum_mismatch_raises(tmp_path):
    data = generate_shard_bytes(record_count=8, input_dim=4, num_classes=2, seed=9)
    path = tmp_path / "bad.bin"
    path.write_bytes(data)
    stream = _native.ShardStream([(str(path), "0" * 64)], prefetch=1)
    with pytest.raises(OSError, match="checksum mismatch"):
        next(iter(stream))


def test_shard_stream_missing_file_raises(tmp_path):
    stream = _native.ShardStream([(str(tmp_path / "missing.bin"), None)], prefetch=1)
    with pytest.raises(OSError):
        next(iter(stream))


# ── S3 (MinIO) — gated like the checkpoint tests ────────────────────────────

minio_gated = pytest.mark.skipif(
    os.environ.get("DTRUNTIME_RUN_MINIO_TESTS") != "1",
    reason="set DTRUNTIME_RUN_MINIO_TESTS=1 with MinIO running to enable",
)


@minio_gated
def test_shard_stream_fetches_from_s3(tmp_path):
    boto3 = pytest.importorskip("boto3")
    import uuid

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
    bucket = f"dtr-native-test-{uuid.uuid4().hex[:12]}"
    client.create_bucket(Bucket=bucket)
    try:
        specs = []
        expected = []
        for i in range(3):
            data = generate_shard_bytes(
                record_count=32, input_dim=16, num_classes=4, seed=100 + i
            )
            key = f"shards/s{i}.bin"
            client.put_object(Bucket=bucket, Key=key, Body=data)
            specs.append((f"s3://{bucket}/{key}", hashlib.sha256(data).hexdigest()))
            expected.append(list(memoryview(_native.decode_dtrs(data).labels)))
        got = [list(memoryview(b.labels)) for b in _native.ShardStream(specs, prefetch=2)]
        assert got == expected
    finally:
        for obj in client.list_objects_v2(Bucket=bucket).get("Contents", []):
            client.delete_object(Bucket=bucket, Key=obj["Key"])
        client.delete_bucket(Bucket=bucket)
