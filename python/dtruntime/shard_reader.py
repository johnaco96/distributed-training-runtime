"""
Binary shard I/O for the Distributed Training Runtime.

Wire format (DTRS v1)
---------------------
Header (32 bytes):
  [0:4]   magic        = b'DTRS'
  [4:8]   version      = uint32 LE = 1
  [8:12]  record_count = uint32 LE   (number of (input, label) pairs)
  [12:16] input_dim    = uint32 LE   (number of float32 values per sample)
  [16:20] num_classes  = uint32 LE   (exclusive upper bound for labels)
  [20:32] reserved     = 12 zero bytes

Records (each = input_dim × 4 + 4 bytes):
  inputs : input_dim × float32 LE
  label  : int32 LE                  (0 ≤ label < num_classes)

URI schemes understood by ShardReader
--------------------------------------
  s3://bucket/key         — downloads via boto3 from an S3-compatible endpoint
  file:///abs/path        — reads from local filesystem
  /abs/path  or  ./rel    — reads from local filesystem (no scheme)
"""

from __future__ import annotations

import io
import os
import struct
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Generator, Iterator, Optional, Tuple
from urllib.parse import urlparse

import torch

MAGIC = b"DTRS"
VERSION = 1
HEADER_SIZE = 32
HEADER_STRUCT = struct.Struct("<4sIIII3I")  # magic, ver, records, input_dim, num_classes, 3×reserved uint32


# ── Header ────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ShardHeader:
    record_count: int
    input_dim: int
    num_classes: int

    @property
    def record_size(self) -> int:
        """Bytes per record: input_dim floats + 1 int32 label."""
        return self.input_dim * 4 + 4

    @property
    def data_size(self) -> int:
        return self.record_count * self.record_size


def write_shard_header(f: io.RawIOBase, header: ShardHeader) -> None:
    packed = struct.pack(
        "<4sIIII3I",
        MAGIC,
        VERSION,
        header.record_count,
        header.input_dim,
        header.num_classes,
        0, 0, 0,  # reserved
    )
    assert len(packed) == HEADER_SIZE
    f.write(packed)


def read_shard_header(data: bytes) -> ShardHeader:
    if len(data) < HEADER_SIZE:
        raise ValueError(f"shard data too short for header ({len(data)} < {HEADER_SIZE})")
    fields = struct.unpack_from("<4sIIII3I", data, 0)
    magic, version, record_count, input_dim, num_classes = fields[:5]
    if magic != MAGIC:
        raise ValueError(f"bad magic: expected {MAGIC!r}, got {magic!r}")
    if version != VERSION:
        raise ValueError(f"unsupported shard version {version} (expected {VERSION})")
    return ShardHeader(record_count=record_count, input_dim=input_dim, num_classes=num_classes)


# ── Shard generation ───────────────────────────────────────────────────────────

def generate_shard_bytes(
    record_count: int,
    input_dim: int,
    num_classes: int,
    seed: Optional[int] = None,
) -> bytes:
    """Produce a complete DTRS shard as a bytes object.

    Uses a seeded RNG so the same (seed, record_count, input_dim) triple always
    produces the same bytes — useful for reproducible benchmarks.
    """
    rng = torch.Generator()
    if seed is not None:
        rng.manual_seed(seed)

    header = ShardHeader(record_count=record_count, input_dim=input_dim, num_classes=num_classes)
    buf = io.BytesIO()
    write_shard_header(buf, header)

    inputs = torch.randn(record_count, input_dim, generator=rng).to(torch.float32)
    labels = torch.randint(0, num_classes, (record_count,), generator=rng).to(torch.int32)

    # Interleave: input_row (input_dim × f32) || label (i32) for each record.
    for i in range(record_count):
        buf.write(inputs[i].numpy().tobytes())
        buf.write(struct.pack("<i", labels[i].item()))

    return buf.getvalue()


# ── Raw data fetch ─────────────────────────────────────────────────────────────

def _is_local_path(uri: str) -> bool:
    """Return True if *uri* should be treated as a local filesystem path.

    Handles:
      - ``file:///abs/path``
      - POSIX absolute paths (``/abs/path``)
      - Relative paths (``./rel`` or ``rel``)
      - Windows absolute paths (``C:\\...`` or ``C:/...``) — urlparse sees the
        drive letter as a single-character scheme, so we special-case it.
    """
    parsed = urlparse(uri)
    # file:// scheme
    if parsed.scheme == "file":
        return True
    # No scheme at all (empty string)
    if parsed.scheme == "":
        return True
    # Windows drive letter: single alpha char followed by colon, e.g. 'c' from 'C:\...'
    if len(parsed.scheme) == 1 and parsed.scheme.isalpha():
        return True
    return False


def _fetch_shard_bytes(
    shard_uri: str,
    s3_endpoint: Optional[str] = None,
    s3_access_key: Optional[str] = None,
    s3_secret_key: Optional[str] = None,
    s3_region: str = "us-east-1",
) -> bytes:
    """Download or read raw shard bytes from the given URI."""
    parsed = urlparse(shard_uri)

    if parsed.scheme == "s3":
        import boto3
        from botocore.config import Config

        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        endpoint = s3_endpoint or os.environ.get("DTRUNTIME_S3_ENDPOINT")
        access_key = s3_access_key or os.environ.get("DTRUNTIME_S3_ACCESS_KEY")
        secret_key = s3_secret_key or os.environ.get("DTRUNTIME_S3_SECRET_KEY")

        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=s3_region,
            config=Config(signature_version="s3v4"),
        )
        response = client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()

    # Local filesystem: file:///path, /abs/path, ./rel, C:\..., etc.
    if _is_local_path(shard_uri):
        if parsed.scheme == "file":
            # url2pathname handles Windows drive letters in file:// URIs
            # correctly on all platforms (e.g. /C:/path → C:\path on Windows).
            from urllib.request import url2pathname
            path = Path(url2pathname(parsed.path))
        else:
            path = Path(shard_uri)
        return path.read_bytes()

    raise ValueError(f"unsupported shard URI scheme: {parsed.scheme!r} in {shard_uri!r}")


# ── ShardReader ────────────────────────────────────────────────────────────────

@dataclass
class ShardBatch:
    inputs: torch.Tensor   # shape (batch_size, input_dim), dtype=float32
    labels: torch.Tensor   # shape (batch_size,), dtype=int64
    progress_offset: int   # records consumed so far (cumulative)


class ShardReader:
    """Reads a DTRS binary shard and iterates it as batches of tensors.

    Example::

        reader = ShardReader("s3://my-bucket/datasets/v1/shard-0000.bin",
                             batch_size=32,
                             s3_endpoint="http://localhost:9100",
                             s3_access_key="minioadmin",
                             s3_secret_key="minioadmin")
        for batch in reader:
            loss = model(batch.inputs).cross_entropy(batch.labels)
            client.report_shard_progress(..., progress_offset=batch.progress_offset)

    The first call to ``__iter__`` downloads the shard bytes (one HTTP GET for
    S3) and then iterates purely in-memory.  Subsequent iterations re-use the
    cached bytes without hitting the network.
    """

    def __init__(
        self,
        shard_uri: str,
        batch_size: int = 32,
        s3_endpoint: Optional[str] = None,
        s3_access_key: Optional[str] = None,
        s3_secret_key: Optional[str] = None,
        s3_region: str = "us-east-1",
    ) -> None:
        self.shard_uri = shard_uri
        self.batch_size = batch_size
        self._s3_kwargs = dict(
            s3_endpoint=s3_endpoint,
            s3_access_key=s3_access_key,
            s3_secret_key=s3_secret_key,
            s3_region=s3_region,
        )
        self._raw: Optional[bytes] = None
        self._header: Optional[ShardHeader] = None

    def _load(self) -> None:
        if self._raw is None:
            self._raw = _fetch_shard_bytes(self.shard_uri, **self._s3_kwargs)
            self._header = read_shard_header(self._raw)

    @property
    def header(self) -> ShardHeader:
        self._load()
        assert self._header is not None
        return self._header

    @property
    def record_count(self) -> int:
        return self.header.record_count

    @property
    def input_dim(self) -> int:
        return self.header.input_dim

    @property
    def num_classes(self) -> int:
        return self.header.num_classes

    def __iter__(self) -> Iterator[ShardBatch]:
        self._load()
        assert self._raw is not None
        h = self._header
        assert h is not None
        record_size = h.record_size
        offset = HEADER_SIZE
        records_read = 0

        while records_read < h.record_count:
            n = min(self.batch_size, h.record_count - records_read)
            inputs_list = []
            labels_list = []
            for _ in range(n):
                row = self._raw[offset : offset + h.input_dim * 4]
                label_bytes = self._raw[offset + h.input_dim * 4 : offset + record_size]
                # bytearray is writable so torch.frombuffer doesn't warn;
                # clone() detaches the tensor from the buffer before it's GC'd.
                inputs_list.append(torch.frombuffer(bytearray(row), dtype=torch.float32).clone())
                labels_list.append(struct.unpack_from("<i", label_bytes)[0])
                offset += record_size
            records_read += n

            yield ShardBatch(
                inputs=torch.stack(inputs_list),
                labels=torch.tensor(labels_list, dtype=torch.int64),
                progress_offset=records_read,
            )

    def read_all(self) -> Tuple[torch.Tensor, torch.Tensor]:
        """Convenience: return (inputs, labels) tensors for the full shard."""
        all_inputs = []
        all_labels = []
        for batch in self:
            all_inputs.append(batch.inputs)
            all_labels.append(batch.labels)
        return torch.cat(all_inputs), torch.cat(all_labels)


# ── Local filesystem write helper ─────────────────────────────────────────────

def write_shard_to_local_file(data: bytes, path) -> str:
    """Write *data* to *path* on the local filesystem.

    Creates parent directories as needed and returns a ``file://`` URI
    pointing at the resolved absolute path.  This is the local-backend
    counterpart of :func:`upload_shard_to_s3`.

    Args:
        data: Raw shard bytes (DTRS v1 format).
        path: :class:`pathlib.Path` or ``str`` destination path.

    Returns:
        A ``file://`` URI suitable for passing to :class:`ShardReader`.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)
    return p.resolve().as_uri()


# ── S3 upload helper (used by generate_shard_dataset.py) ──────────────────────

def upload_shard_to_s3(
    data: bytes,
    bucket: str,
    key: str,
    s3_endpoint: Optional[str] = None,
    s3_access_key: Optional[str] = None,
    s3_secret_key: Optional[str] = None,
    s3_region: str = "us-east-1",
) -> str:
    """Upload *data* to S3 and return the canonical ``s3://bucket/key`` URI."""
    import boto3
    from botocore.config import Config

    client = boto3.client(
        "s3",
        endpoint_url=s3_endpoint or os.environ.get("DTRUNTIME_S3_ENDPOINT"),
        aws_access_key_id=s3_access_key or os.environ.get("DTRUNTIME_S3_ACCESS_KEY"),
        aws_secret_access_key=s3_secret_key or os.environ.get("DTRUNTIME_S3_SECRET_KEY"),
        region_name=s3_region,
        config=Config(signature_version="s3v4"),
    )
    client.put_object(Bucket=bucket, Key=key, Body=data)
    return f"s3://{bucket}/{key}"


def ensure_s3_bucket(
    bucket: str,
    s3_endpoint: Optional[str] = None,
    s3_access_key: Optional[str] = None,
    s3_secret_key: Optional[str] = None,
    s3_region: str = "us-east-1",
) -> None:
    """Create *bucket* if it does not already exist."""
    import boto3
    from botocore.config import Config

    client = boto3.client(
        "s3",
        endpoint_url=s3_endpoint or os.environ.get("DTRUNTIME_S3_ENDPOINT"),
        aws_access_key_id=s3_access_key or os.environ.get("DTRUNTIME_S3_ACCESS_KEY"),
        aws_secret_access_key=s3_secret_key or os.environ.get("DTRUNTIME_S3_SECRET_KEY"),
        region_name=s3_region,
        config=Config(signature_version="s3v4"),
    )
    existing = [b["Name"] for b in client.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)
