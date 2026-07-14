"""Shared internal helpers: file hashing, URI handling, S3 client construction.

One implementation for the idioms that used to be copy-pasted across
``async_checkpoint``, ``data.cache``, and ``shard_reader`` — the kind of
subtly-divergent duplication (Windows drive letters, env-var fallbacks) that
breeds cross-platform bugs.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any, Optional


def sha256_file(path: Path) -> str:
    """Streaming SHA-256 hex digest of a file (1 MiB chunks)."""
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_uri_to_path(uri: str) -> Optional[Path]:
    """Resolve a local ``file://`` URI (or bare path) to a filesystem path.

    Handles the several conventions in circulation:
    - ``file:///home/user/x`` (POSIX, RFC form),
    - ``file:///C:/Users/x``  (Windows, ``Path.as_uri()`` form),
    - ``file://C:\\Users\\x``   (Windows, ``build_and_write_shards`` form),
    - a bare absolute/relative path with no scheme.

    Returns ``None`` for non-file schemes (e.g. ``s3://``).
    """
    if uri.startswith("file://"):
        rest = uri[len("file://") :]
        # ``/C:/...`` → ``C:/...`` (strip the leading slash before a drive).
        if len(rest) >= 3 and rest[0] == "/" and rest[2] == ":":
            rest = rest[1:]
        return Path(rest)
    scheme = uri.split("://", 1)[0] if "://" in uri else ""
    if scheme == "" or (len(scheme) == 1 and scheme.isalpha()):
        # No scheme, or a Windows drive letter mistaken for one (``C:\\...``).
        return Path(uri)
    return None


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split ``s3://bucket/key/parts`` into ``(bucket, key)``."""
    if not uri.startswith("s3://"):
        raise ValueError(f"not an s3 uri: {uri}")
    rest = uri[len("s3://") :]
    bucket, _, key = rest.partition("/")
    if not bucket or not key:
        raise ValueError(f"invalid s3 uri: {uri}")
    return bucket, key


def make_s3_client(
    endpoint: Optional[str] = None,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    region: Optional[str] = None,
) -> Any:
    """Build a boto3 S3 client from explicit arguments, falling back to the
    ``DTRUNTIME_S3_ENDPOINT`` / ``DTRUNTIME_S3_ACCESS_KEY`` /
    ``DTRUNTIME_S3_SECRET_KEY`` / ``DTRUNTIME_S3_REGION`` environment —
    the same variables the Rust data movers read.

    Raises ImportError when boto3 is not installed
    (``pip install 'dtruntime[s3]'``).
    """
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=endpoint or os.environ.get("DTRUNTIME_S3_ENDPOINT"),
        aws_access_key_id=access_key or os.environ.get("DTRUNTIME_S3_ACCESS_KEY"),
        aws_secret_access_key=secret_key or os.environ.get("DTRUNTIME_S3_SECRET_KEY"),
        region_name=region or os.environ.get("DTRUNTIME_S3_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )
