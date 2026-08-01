"""Unit tests for dtruntime.data.formats (no coordinator)."""

from __future__ import annotations

import io
import json
import struct
import tarfile
from pathlib import Path

import numpy as np
import pytest
from dtruntime.data.formats import (
    DtrsFormat,
    JsonlFormat,
    NpzFormat,
    ParquetFormat,
    WebdatasetFormat,
    get_format,
    register_format,
    supported_formats,
)


def test_registry_lists_builtin_formats() -> None:
    assert set(supported_formats()) >= {"npz", "dtrs", "jsonl", "parquet", "webdataset"}


def test_get_format_is_case_insensitive_and_errors_clearly() -> None:
    assert get_format("NPZ").name == "npz"
    with pytest.raises(ValueError, match="unknown dataset format"):
        get_format("nonesuch")


def test_register_custom_format_roundtrips() -> None:
    class Custom:
        name = "custom-x"

        def iter_records(self, path, *, start=0):
            yield {"v": 1}

        def count_records(self, path):
            return 1

    register_format(Custom())
    assert get_format("custom-x").count_records(Path(".")) == 1


def test_npz_format_streams_rows_and_counts(tmp_path: Path) -> None:
    features = np.arange(12, dtype=np.float32).reshape(6, 2)
    labels = np.array([0, 1, 2, 3, 4, 5], dtype=np.int64)
    path = tmp_path / "shard.npz"
    np.savez(path, features=features, labels=labels)

    fmt = NpzFormat()
    assert fmt.count_records(path) == 6
    records = list(fmt.iter_records(path))
    assert len(records) == 6
    assert records[0]["label"] == 0
    np.testing.assert_array_equal(records[3]["features"], features[3])

    # start= skips leading records (resume).
    resumed = list(fmt.iter_records(path, start=4))
    assert [r["label"] for r in resumed] == [4, 5]


def test_dtrs_format_streams_and_resumes(tmp_path: Path) -> None:
    input_dim, num_classes, count = 3, 4, 5
    path = tmp_path / "shard.bin"
    with open(path, "wb") as f:
        f.write(struct.pack("<4sIIII3I", b"DTRS", 1, count, input_dim, num_classes, 0, 0, 0))
        for i in range(count):
            f.write(np.full(input_dim, float(i), dtype="<f4").tobytes())
            f.write(struct.pack("<i", i % num_classes))

    fmt = DtrsFormat()
    assert fmt.count_records(path) == count
    records = list(fmt.iter_records(path))
    assert len(records) == count
    np.testing.assert_allclose(records[2]["features"], [2.0, 2.0, 2.0])
    assert records[2]["label"] == 2

    resumed = list(fmt.iter_records(path, start=3))
    assert [r["label"] for r in resumed] == [3, 0]


def test_jsonl_format_streams_and_counts(tmp_path: Path) -> None:
    path = tmp_path / "shard.jsonl"
    path.write_text("".join(json.dumps({"i": i}) + "\n" for i in range(4)), encoding="utf-8")
    fmt = JsonlFormat()
    assert fmt.count_records(path) == 4
    assert [r["i"] for r in fmt.iter_records(path, start=2)] == [2, 3]


def test_parquet_format_streams_by_row_group(tmp_path: Path) -> None:
    pq = pytest.importorskip("pyarrow.parquet")
    pa = pytest.importorskip("pyarrow")
    path = tmp_path / "shard.parquet"
    table = pa.table({"x": list(range(10)), "y": [f"r{i}" for i in range(10)]})
    pq.write_table(table, path, row_group_size=3)

    fmt = ParquetFormat()
    assert fmt.count_records(path) == 10
    records = list(fmt.iter_records(path))
    assert len(records) == 10
    assert records[0] == {"x": 0, "y": "r0"}
    assert [r["x"] for r in fmt.iter_records(path, start=8)] == [8, 9]


def test_webdataset_format_groups_by_key(tmp_path: Path) -> None:
    path = tmp_path / "shard.tar"
    with tarfile.open(path, "w") as tar:
        for key in ("000", "001"):
            for ext, payload in ((b"cls", f"{key}-cls"), (b"txt", f"{key}-txt")):
                data = payload.encode() if isinstance(payload, str) else payload
                info = tarfile.TarInfo(name=f"{key}.{ext.decode()}")
                info.size = len(data)
                tar.addfile(info, io.BytesIO(data))

    fmt = WebdatasetFormat()
    records = list(fmt.iter_records(path))
    assert len(records) == 2
    assert records[0]["__key__"] == "000"
    assert records[0]["cls"] == b"000-cls"
    assert records[1]["txt"] == b"001-txt"
    # start= skips whole records.
    assert [r["__key__"] for r in fmt.iter_records(path, start=1)] == ["001"]
