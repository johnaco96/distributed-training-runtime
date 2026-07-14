"""Unit tests for dtruntime.shard_reader — no live coordinator or S3 required."""

from __future__ import annotations

import io
import struct

import pytest

torch = pytest.importorskip("torch")  # this module exercises tensor I/O

from dtruntime.shard_reader import (
    HEADER_SIZE,
    ShardBatch,
    ShardHeader,
    ShardReader,
    generate_shard_bytes,
    read_shard_header,
    write_shard_header,
)


# ── Header round-trip ─────────────────────────────────────────────────────────

def test_header_write_read_roundtrip():
    h = ShardHeader(record_count=100, input_dim=32, num_classes=10)
    buf = io.BytesIO()
    write_shard_header(buf, h)
    data = buf.getvalue()
    assert len(data) == HEADER_SIZE
    h2 = read_shard_header(data)
    assert h2.record_count == 100
    assert h2.input_dim == 32
    assert h2.num_classes == 10


def test_header_bad_magic_raises():
    h = ShardHeader(record_count=1, input_dim=4, num_classes=2)
    buf = io.BytesIO()
    write_shard_header(buf, h)
    bad = bytearray(buf.getvalue())
    bad[0:4] = b"XXXX"
    with pytest.raises(ValueError, match="bad magic"):
        read_shard_header(bytes(bad))


def test_header_bad_version_raises():
    h = ShardHeader(record_count=1, input_dim=4, num_classes=2)
    buf = io.BytesIO()
    write_shard_header(buf, h)
    bad = bytearray(buf.getvalue())
    struct.pack_into("<I", bad, 4, 99)  # version = 99
    with pytest.raises(ValueError, match="unsupported shard version"):
        read_shard_header(bytes(bad))


def test_header_too_short_raises():
    with pytest.raises(ValueError, match="too short"):
        read_shard_header(b"\x00" * 8)


def test_header_record_size():
    h = ShardHeader(record_count=10, input_dim=128, num_classes=5)
    assert h.record_size == 128 * 4 + 4
    assert h.data_size == 10 * (128 * 4 + 4)


# ── Shard generation ──────────────────────────────────────────────────────────

def test_generate_shard_bytes_structure():
    data = generate_shard_bytes(record_count=50, input_dim=16, num_classes=4, seed=1)
    h = read_shard_header(data)
    assert h.record_count == 50
    assert h.input_dim == 16
    assert h.num_classes == 4
    expected_size = HEADER_SIZE + 50 * (16 * 4 + 4)
    assert len(data) == expected_size


def test_generate_shard_bytes_deterministic():
    a = generate_shard_bytes(record_count=10, input_dim=8, num_classes=3, seed=42)
    b = generate_shard_bytes(record_count=10, input_dim=8, num_classes=3, seed=42)
    assert a == b


def test_generate_shard_bytes_different_seeds():
    a = generate_shard_bytes(record_count=10, input_dim=8, num_classes=3, seed=1)
    b = generate_shard_bytes(record_count=10, input_dim=8, num_classes=3, seed=2)
    assert a != b


def test_generate_shard_no_seed():
    # Should not raise, even without a seed.
    data = generate_shard_bytes(record_count=5, input_dim=4, num_classes=2, seed=None)
    h = read_shard_header(data)
    assert h.record_count == 5


# ── ShardReader from file:// URI ──────────────────────────────────────────────

def _write_tmp_shard(tmp_path, record_count=20, input_dim=8, num_classes=4, seed=7):
    data = generate_shard_bytes(record_count, input_dim, num_classes, seed)
    p = tmp_path / "shard-0000.bin"
    p.write_bytes(data)
    return p, data


def test_shard_reader_file_uri_header(tmp_path):
    p, _ = _write_tmp_shard(tmp_path)
    reader = ShardReader(p.as_uri(), batch_size=5)
    assert reader.record_count == 20
    assert reader.input_dim == 8
    assert reader.num_classes == 4


def test_shard_reader_local_path(tmp_path):
    p, _ = _write_tmp_shard(tmp_path)
    # bare path string (no scheme) should also work
    reader = ShardReader(str(p), batch_size=5)
    assert reader.record_count == 20


def test_shard_reader_yields_correct_batch_shapes(tmp_path):
    p, _ = _write_tmp_shard(tmp_path, record_count=20, input_dim=8, num_classes=4)
    reader = ShardReader(p.as_uri(), batch_size=5)
    batches = list(reader)
    assert len(batches) == 4  # 20 records / 5 per batch
    for batch in batches:
        assert isinstance(batch, ShardBatch)
        assert batch.inputs.shape == (5, 8)
        assert batch.labels.shape == (5,)
        assert batch.inputs.dtype == torch.float32
        assert batch.labels.dtype == torch.int64


def test_shard_reader_partial_last_batch(tmp_path):
    p, _ = _write_tmp_shard(tmp_path, record_count=7, input_dim=4, num_classes=2)
    reader = ShardReader(p.as_uri(), batch_size=3)
    batches = list(reader)
    assert len(batches) == 3  # [3, 3, 1]
    assert batches[-1].inputs.shape[0] == 1


def test_shard_reader_progress_offset_monotone(tmp_path):
    p, _ = _write_tmp_shard(tmp_path, record_count=10, input_dim=4, num_classes=2)
    reader = ShardReader(p.as_uri(), batch_size=3)
    offsets = [b.progress_offset for b in reader]
    assert offsets == sorted(offsets)
    assert offsets[-1] == 10  # consumed all records


def test_shard_reader_label_range(tmp_path):
    p, _ = _write_tmp_shard(tmp_path, record_count=50, input_dim=4, num_classes=5, seed=3)
    reader = ShardReader(p.as_uri(), batch_size=50)
    batch = next(iter(reader))
    assert batch.labels.min().item() >= 0
    assert batch.labels.max().item() < 5


def test_shard_reader_read_all(tmp_path):
    p, _ = _write_tmp_shard(tmp_path, record_count=30, input_dim=8, num_classes=3)
    reader = ShardReader(p.as_uri(), batch_size=7)
    inputs, labels = reader.read_all()
    assert inputs.shape == (30, 8)
    assert labels.shape == (30,)


def test_shard_reader_caches_bytes(tmp_path):
    p, _ = _write_tmp_shard(tmp_path, record_count=10, input_dim=4, num_classes=2)
    reader = ShardReader(p.as_uri(), batch_size=5)
    _ = list(reader)   # first pass — reads from disk
    _ = list(reader)   # second pass — uses cached _raw; no re-read
    assert reader._raw is not None  # bytes are cached


def test_shard_reader_unsupported_scheme_raises():
    reader = ShardReader("gcs://bucket/key", batch_size=32)
    with pytest.raises(ValueError, match="unsupported shard URI scheme"):
        list(reader)  # triggers _load() → _fetch_shard_bytes → raises


# ── Data correctness (read back == generated) ─────────────────────────────────

def test_shard_reader_values_match_generation(tmp_path):
    """The floats read back from disk must equal the ones generate_shard_bytes produced."""
    data = generate_shard_bytes(record_count=4, input_dim=3, num_classes=2, seed=99)
    p = tmp_path / "check.bin"
    p.write_bytes(data)

    reader = ShardReader(str(p), batch_size=4)
    inputs, labels = reader.read_all()

    # Re-generate the same data independently and compare.
    rng = torch.Generator()
    rng.manual_seed(99)
    expected_inputs = torch.randn(4, 3, generator=rng).to(torch.float32)
    expected_labels = torch.randint(0, 2, (4,), generator=rng).to(torch.int32)

    assert torch.allclose(inputs, expected_inputs, atol=1e-6)
    assert torch.equal(labels.to(torch.int32), expected_labels)
