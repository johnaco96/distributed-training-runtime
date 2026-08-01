"""Content-addressed dataset versioning.

A dataset *version* is derived deterministically from the content of its
shards: hash the ordered ``(shard_id, checksum)`` pairs.  Two registrations of
the same bytes get the same version; changing, adding, or reordering any shard
produces a new version.  This makes versions **reproducible** (no manual
version bumping) and **verifiable** (a worker can confirm it is reading the
data it expects).

The version string is ``"sha256:<hex>"``; it is stored in
``DatasetMetadata.dataset_version`` at registration time.
"""

from __future__ import annotations

import hashlib
from typing import Mapping, Sequence


def compute_dataset_version(shards: Sequence[Mapping[str, object]]) -> str:
    """Compute the content-addressed version for a shard manifest.

    ``shards`` is the list of dicts returned by
    :func:`dtruntime.data.shard_writer.build_and_write_shards` (each with at least
    ``shard_id`` and ``checksum``).  Shards are sorted by id first so the
    version is independent of manifest ordering.
    """
    h = hashlib.sha256()
    for shard in sorted(shards, key=lambda s: str(s.get("shard_id", ""))):
        shard_id = str(shard.get("shard_id", ""))
        checksum = str(shard.get("checksum", ""))
        h.update(shard_id.encode("utf-8"))
        h.update(b"\x00")
        h.update(checksum.encode("utf-8"))
        h.update(b"\x00")
    return f"sha256:{h.hexdigest()}"


def register_versioned_dataset(
    client,
    run_id: str,
    name: str,
    uri: str,
    fmt: str,
    shards: Sequence[Mapping[str, object]],
    *,
    dataset_id: str = "",
    labels: "dict[str, str] | None" = None,
    idempotency_key: str = "",
    version: "str | None" = None,
):
    """Register a dataset with a content-addressed version.

    When ``version`` is omitted it is computed from ``shards`` via
    :func:`compute_dataset_version`.  Returns
    ``(RegisterDatasetResponse, version)``.
    """
    resolved_version = version or compute_dataset_version(shards)
    response = client.register_dataset(
        run_id=run_id,
        name=name,
        uri=uri,
        format=fmt,
        shards=list(shards),
        dataset_id=dataset_id,
        dataset_version=resolved_version,
        labels=labels,
        idempotency_key=idempotency_key,
    )
    return response, resolved_version


def verify_dataset_version(
    expected: str, shards: Sequence[Mapping[str, object]]
) -> None:
    """Raise ``ValueError`` if ``shards`` do not hash to ``expected``.

    Used to pin a training run to an exact dataset version — catches silent
    data drift (a shard rebuilt or swapped underneath a pinned version).
    """
    actual = compute_dataset_version(shards)
    if actual != expected:
        raise ValueError(
            f"dataset version mismatch: manifest hashes to {actual}, "
            f"but {expected} was expected"
        )
