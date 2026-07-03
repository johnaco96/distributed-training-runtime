"""Pluggable shard formats for the data pipeline.

A *shard* is the unit of caching, locality, and coordinator assignment; a
shard file contains many *records*.  A :class:`ShardFormat` knows how to
stream records out of one cached shard file and (when cheap) how to count
them without a full scan.

Records are plain ``dict[str, Any]`` so formats stay general (features/labels,
tokenized text, images, ...).  A user-supplied ``transform`` turns a record
dict into whatever the training loop wants (usually tensors); a default
``features``/``label`` transform is provided for the common tabular case.

Built-in formats
----------------
* ``npz``        — numpy ``.npz`` with ``features`` (2-D) + ``labels`` (1-D);
                   the format written by :func:`dtruntime.shard_loader.write_shard_file`.
* ``dtrs``       — the DTRS v1 binary format from :mod:`dtruntime.shard_reader`.
* ``jsonl``      — one JSON object per line.
* ``parquet``    — Apache Parquet (requires ``pyarrow``); streamed by row group.
* ``webdataset`` — POSIX tar of grouped files (``key.cls``, ``key.jpg`` ...),
                   the WebDataset convention; streamed sequentially.

Register a custom format with :func:`register_format`.
"""

from __future__ import annotations

import io
import json
import struct
import tarfile
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, Optional, Protocol, runtime_checkable

Record = Dict[str, Any]
Transform = Callable[[Record], Any]


@runtime_checkable
class ShardFormat(Protocol):
    """Streams records out of a single cached shard file."""

    #: Format name used in ``DatasetMetadata.format`` and the registry.
    name: str

    def iter_records(self, path: Path, *, start: int = 0) -> Iterator[Record]:
        """Yield records from ``path``, skipping the first ``start`` records.

        ``start`` supports resume-from-mid-shard: it is a record count, not a
        byte offset, so it is stable regardless of per-record size.
        """
        ...

    def count_records(self, path: Path) -> Optional[int]:
        """Number of records in ``path``, or ``None`` if counting requires a
        full scan the caller should avoid."""
        ...


_REGISTRY: Dict[str, ShardFormat] = {}


def register_format(fmt: ShardFormat) -> None:
    """Register (or replace) a shard format by its ``name``."""
    _REGISTRY[fmt.name.lower()] = fmt


def get_format(name: str) -> ShardFormat:
    """Look up a registered format (case-insensitive)."""
    key = (name or "").lower()
    if key not in _REGISTRY:
        raise ValueError(
            f"unknown dataset format {name!r}; registered: "
            f"{', '.join(sorted(_REGISTRY)) or '(none)'}"
        )
    return _REGISTRY[key]


def supported_formats() -> list[str]:
    return sorted(_REGISTRY)


# ── npz ─────────────────────────────────────────────────────────────────────


class NpzFormat:
    name = "npz"

    def iter_records(self, path: Path, *, start: int = 0) -> Iterator[Record]:
        import numpy as np

        with np.load(path) as npz:
            if "features" not in npz or "labels" not in npz:
                raise ValueError(
                    f"npz shard {path} is missing 'features'/'labels' arrays"
                )
            features = npz["features"]
            labels = npz["labels"]
        n = len(features)
        for i in range(max(0, start), n):
            yield {"features": features[i], "label": int(labels[i])}

    def count_records(self, path: Path) -> Optional[int]:
        import numpy as np

        with np.load(path) as npz:
            return int(len(npz["labels"]))


# ── dtrs binary ─────────────────────────────────────────────────────────────


class DtrsFormat:
    name = "dtrs"
    _HEADER_SIZE = 32
    _MAGIC = b"DTRS"

    def _header(self, path: Path) -> tuple[int, int, int]:
        with open(path, "rb") as f:
            head = f.read(self._HEADER_SIZE)
        if len(head) < self._HEADER_SIZE:
            raise ValueError(f"dtrs shard {path} too short for header")
        magic, version, record_count, input_dim, num_classes = struct.unpack_from(
            "<4sIIII", head, 0
        )
        if magic != self._MAGIC:
            raise ValueError(f"dtrs shard {path}: bad magic {magic!r}")
        if version != 1:
            raise ValueError(f"dtrs shard {path}: unsupported version {version}")
        return record_count, input_dim, num_classes

    def iter_records(self, path: Path, *, start: int = 0) -> Iterator[Record]:
        import numpy as np

        record_count, input_dim, _ = self._header(path)
        record_size = input_dim * 4 + 4
        with open(path, "rb") as f:
            f.seek(self._HEADER_SIZE + max(0, start) * record_size)
            for _ in range(max(0, start), record_count):
                buf = f.read(record_size)
                if len(buf) < record_size:
                    break
                features = np.frombuffer(buf[: input_dim * 4], dtype="<f4").copy()
                label = struct.unpack_from("<i", buf, input_dim * 4)[0]
                yield {"features": features, "label": int(label)}

    def count_records(self, path: Path) -> Optional[int]:
        return self._header(path)[0]


# ── jsonl ───────────────────────────────────────────────────────────────────


class JsonlFormat:
    name = "jsonl"

    def iter_records(self, path: Path, *, start: int = 0) -> Iterator[Record]:
        with open(path, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i < start:
                    continue
                line = line.strip()
                if line:
                    yield json.loads(line)

    def count_records(self, path: Path) -> Optional[int]:
        with open(path, "r", encoding="utf-8") as f:
            return sum(1 for line in f if line.strip())


# ── parquet ─────────────────────────────────────────────────────────────────


class ParquetFormat:
    name = "parquet"

    def _pq(self):
        try:
            import pyarrow.parquet as pq
        except ImportError as exc:  # pragma: no cover - optional dep
            raise RuntimeError(
                "the 'parquet' dataset format requires pyarrow: pip install pyarrow"
            ) from exc
        return pq

    def iter_records(self, path: Path, *, start: int = 0) -> Iterator[Record]:
        pq = self._pq()
        pf = pq.ParquetFile(path)
        seen = 0
        # Stream by row group so large shards never fully materialise.
        for rg in range(pf.num_row_groups):
            table = pf.read_row_group(rg)
            columns = {name: table.column(name).to_pylist() for name in table.column_names}
            rows = table.num_rows
            for i in range(rows):
                if seen >= start:
                    yield {name: values[i] for name, values in columns.items()}
                seen += 1

    def count_records(self, path: Path) -> Optional[int]:
        pq = self._pq()
        return int(pq.ParquetFile(path).metadata.num_rows)


# ── webdataset (tar) ─────────────────────────────────────────────────────────


class WebdatasetFormat:
    """WebDataset tar shards.

    Files sharing a basename form one record; the extension becomes the field
    name (``00001.jpg`` + ``00001.cls`` → ``{"jpg": b"...", "cls": b"..."}``).
    Streamed strictly sequentially, honouring tar member order.
    """

    name = "webdataset"

    def iter_records(self, path: Path, *, start: int = 0) -> Iterator[Record]:
        emitted = 0
        with tarfile.open(path, "r") as tar:
            current_key: Optional[str] = None
            record: Record = {}
            for member in tar:
                if not member.isfile():
                    continue
                key, _, ext = member.name.rpartition(".")
                if not key:
                    key, ext = member.name, ""
                if current_key is not None and key != current_key:
                    if emitted >= start:
                        yield record
                    emitted += 1
                    record = {}
                current_key = key
                extracted = tar.extractfile(member)
                record[ext or "bin"] = extracted.read() if extracted else b""
                record["__key__"] = key
            if record and emitted >= start:
                yield record

    def count_records(self, path: Path) -> Optional[int]:
        # Counting needs a full scan of the tar directory; let the caller
        # decide whether that is worthwhile.
        return None


# ── default transform ───────────────────────────────────────────────────────


def features_label_transform(record: Record) -> tuple[Any, Any]:
    """Default transform for tabular records: ``(features_tensor, label)``.

    Works with the ``npz``/``dtrs`` records (``features`` array + ``label``
    int).  Requires torch.
    """
    import torch

    features = record["features"]
    label = record["label"]
    features_t = torch.as_tensor(features, dtype=torch.float32)
    label_t = torch.as_tensor(label, dtype=torch.long)
    return features_t, label_t


for _fmt in (NpzFormat(), DtrsFormat(), JsonlFormat(), ParquetFormat(), WebdatasetFormat()):
    register_format(_fmt)
