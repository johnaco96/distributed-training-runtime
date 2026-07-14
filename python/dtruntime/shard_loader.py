"""Shard authoring helpers: write datasets as per-shard ``.npz`` files.

``build_and_write_shards()`` — called once (by rank-0 or a prep job) —
serialises a dataset into per-shard ``.npz`` files (``features`` float32 +
``labels`` int64 arrays) and returns the shard metadata list that
``client.register_dataset()`` expects. Workers then *consume* those shards
through :class:`dtruntime.data.DistributedShardDataset` (coordinator-assigned,
cached, prefetched — with the Rust fast path when ``dtruntime._native`` is
built); this module only creates them.
"""

from __future__ import annotations

import hashlib
import io
from pathlib import Path

import numpy as np

# Bump when the on-disk format changes so old files are rejected early.
SHARD_FORMAT_VERSION: int = 1


# ---------------------------------------------------------------------------
# Low-level read / write
# ---------------------------------------------------------------------------

def write_shard_file(
    path: Path,
    features: "np.ndarray",
    labels: "np.ndarray",
) -> str:
    """Serialise one shard to *path* as a ``.npz`` file.

    Returns the sha256 hex digest of the written bytes so the caller can
    register it with the coordinator for integrity checking.

    Args:
        path: Destination file path (parent directory must exist).
        features: 2-D float32 array of shape ``(n_samples, n_features)``.
        labels: 1-D int64 array of shape ``(n_samples,)``.

    Returns:
        SHA-256 hex digest string (64 hex chars).
    """
    features = np.asarray(features, dtype=np.float32)
    labels = np.asarray(labels, dtype=np.int64)
    buf = io.BytesIO()
    np.savez(
        buf,
        features=features,
        labels=labels,
        format_version=np.array(SHARD_FORMAT_VERSION, dtype=np.int32),
    )
    data = buf.getvalue()
    path.write_bytes(data)
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# Dataset preparation
# ---------------------------------------------------------------------------

def build_and_write_shards(
    shard_dir: Path,
    features_all: "np.ndarray",
    labels_all: "np.ndarray",
    shard_count: int,
    dataset_id: str = "dataset",
    shard_id_prefix: str = "shard",
) -> list[dict]:
    """Split the full dataset into *shard_count* ``.npz`` files on disk.

    Shards are written to ``shard_dir/<shard_id_prefix>-NNNN.npz``.  The
    returned list is ready to be passed directly to
    ``RuntimeClient.register_dataset(shards=...)``.

    Args:
        shard_dir: Directory where shard files are written (created if absent).
        features_all: Full feature matrix, shape ``(N, F)``.
        labels_all: Full label vector, shape ``(N,)``.
        shard_count: Number of shards to produce.
        dataset_id: The dataset ID string used in shard registration metadata.
        shard_id_prefix: Prefix for generated shard IDs (default: ``"shard"``).

    Returns:
        List of dicts with keys ``shard_id``, ``shard_uri``, ``size_bytes``,
        ``checksum``, ``checksum_algorithm`` — pass as ``shards`` to
        :py:meth:`dtruntime.client.RuntimeClient.register_dataset`.
    """
    shard_dir = Path(shard_dir)
    shard_dir.mkdir(parents=True, exist_ok=True)

    n_samples = len(features_all)
    shard_count = max(1, shard_count)
    indices_per_shard: list[list[int]] = [[] for _ in range(shard_count)]
    for i in range(n_samples):
        indices_per_shard[i % shard_count].append(i)

    shard_metadata: list[dict] = []
    for shard_idx, indices in enumerate(indices_per_shard):
        shard_id = f"{shard_id_prefix}-{shard_idx:04d}"
        path = shard_dir / f"{shard_id}.npz"

        if indices:
            feat_slice = features_all[indices]
            lab_slice = labels_all[indices]
        else:
            # Empty shard — should not happen in practice but be defensive.
            feat_slice = np.empty((0, features_all.shape[1]), dtype=np.float32)
            lab_slice = np.empty((0,), dtype=np.int64)

        checksum = write_shard_file(path, feat_slice, lab_slice)
        size_bytes = path.stat().st_size

        shard_metadata.append({
            "shard_id": shard_id,
            "shard_uri": f"file://{path.resolve()}",
            "size_bytes": size_bytes,
            "byte_range_start": 0,
            "byte_range_end": size_bytes,
            "checksum": checksum,
            "checksum_algorithm": "sha256",
        })

    return shard_metadata
