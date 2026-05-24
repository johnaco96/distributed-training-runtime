"""Real shard data I/O for the distributed training runtime.

This module bridges the coordinator's shard-assignment protocol with actual
data on disk (or object storage).  Every shard is a numpy ``.npz`` file
containing ``features`` (float32) and ``labels`` (int64) arrays.

Typical worker flow
-------------------
1. ``build_and_write_shards()`` — called once (by rank-0 or a prep job)
   to serialise the dataset into per-shard ``.npz`` files and return the
   shard metadata list needed by ``client.register_dataset()``.

2. ``ShardLoader`` — per-worker object that calls
   ``client.request_shard()`` to get the coordinator-assigned URIs, reads
   the actual bytes from disk, reports completion back to the coordinator,
   and returns ``torch.Tensor`` pairs ready for the training loop.

URI schemes supported
---------------------
* ``file:///abs/path/to/shard.npz``  — local filesystem (default for
  single-node and Docker Desktop k8s).
* ``s3://bucket/key``                — S3-compatible object storage
  (requires ``boto3``; endpoint overridable via ``DTRUNTIME_S3_ENDPOINT``).
"""

from __future__ import annotations

import hashlib
import io
import os
from pathlib import Path
from typing import TYPE_CHECKING, Sequence

import numpy as np

if TYPE_CHECKING:
    import torch
    from dtruntime.client import RuntimeClient

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


def read_shard_file(path: Path) -> tuple["np.ndarray", "np.ndarray"]:
    """Load a shard ``.npz`` file written by :func:`write_shard_file`.

    Returns:
        ``(features, labels)`` — float32 and int64 numpy arrays.

    Raises:
        FileNotFoundError: if *path* does not exist.
        ValueError: if the file does not contain the expected keys.
    """
    npz = np.load(path)
    if "features" not in npz or "labels" not in npz:
        raise ValueError(
            f"Shard file {path} is missing 'features' or 'labels' arrays. "
            "Was it written by shard_loader.write_shard_file()?"
        )
    return npz["features"].astype(np.float32), npz["labels"].astype(np.int64)


def read_shard_uri(uri: str) -> tuple["np.ndarray", "np.ndarray"]:
    """Load shard data from a coordinator-assigned URI.

    Supported schemes:
    * ``file://``  — reads from the local filesystem.
    * ``s3://``    — reads via boto3 (requires ``DTRUNTIME_S3_ENDPOINT`` env
                     var for MinIO / non-AWS endpoints).

    Returns:
        ``(features, labels)`` numpy arrays.
    """
    if uri.startswith("file://"):
        return read_shard_file(Path(uri.removeprefix("file://")))

    if uri.startswith("s3://"):
        return _read_shard_s3(uri)

    raise ValueError(
        f"Unsupported shard URI scheme: {uri!r}. "
        "Supported schemes: file://, s3://"
    )


def _read_shard_s3(uri: str) -> tuple["np.ndarray", "np.ndarray"]:
    """Read a shard from S3-compatible object storage."""
    try:
        import boto3  # type: ignore[import-untyped]
    except ImportError as exc:
        raise RuntimeError(
            "boto3 is required for s3:// shard URIs. "
            "Install it with: pip install boto3"
        ) from exc

    # Strip leading "s3://"
    rest = uri.removeprefix("s3://")
    bucket, _, key = rest.partition("/")
    if not bucket or not key:
        raise ValueError(f"Malformed S3 URI: {uri!r}")

    endpoint = os.getenv("DTRUNTIME_S3_ENDPOINT")
    region = os.getenv("DTRUNTIME_S3_REGION", "us-east-1")
    kwargs: dict = {"region_name": region}
    if endpoint:
        kwargs["endpoint_url"] = endpoint

    s3 = boto3.client("s3", **kwargs)
    resp = s3.get_object(Bucket=bucket, Key=key)
    data = resp["Body"].read()
    npz = np.load(io.BytesIO(data))
    return npz["features"].astype(np.float32), npz["labels"].astype(np.int64)


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


# ---------------------------------------------------------------------------
# ShardLoader — per-worker high-level interface
# ---------------------------------------------------------------------------

class ShardLoader:
    """Requests coordinator-assigned shards and loads the actual data bytes.

    Each call to :meth:`load_next_shard_as_tensors` performs three real
    operations:

    1. **GetShardAssignment** gRPC — coordinator returns the shard(s)
       assigned to this worker.
    2. **File read** — the shard bytes are read from the URI the coordinator
       returned (``file://`` or ``s3://``).
    3. **ReportShardProgress** gRPC — notifies the coordinator that this
       worker has fully consumed the shard.

    Args:
        client: An authenticated :class:`~dtruntime.client.RuntimeClient`.
        run_id: The run's ID string (from ``register_run`` response).
        worker_id: This worker's ID string.
        dataset_id: The registered dataset ID.
    """

    def __init__(
        self,
        client: "RuntimeClient",
        run_id: str,
        worker_id: str,
        dataset_id: str,
    ) -> None:
        self.client = client
        self.run_id = run_id
        self.worker_id = worker_id
        self.dataset_id = dataset_id

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load_next_shard(
        self,
        idempotency_key: str = "",
    ) -> tuple[list[str], list[tuple["np.ndarray", "np.ndarray"]]]:
        """Request shard assignment, load bytes, report completion.

        Returns:
            ``(shard_ids, [(features, labels), ...])`` — one entry per
            assigned shard.  ``shard_ids`` is empty when the coordinator
            has no more shards to assign to this worker.
        """
        resp = self.client.request_shard(
            run_id=self.run_id,
            worker_id=self.worker_id,
            dataset_id=self.dataset_id,
            idempotency_key=idempotency_key,
        )

        shard_ids: list[str] = []
        shard_data: list[tuple[np.ndarray, np.ndarray]] = []

        for shard in resp.assignments:
            shard_id = shard.shard_id.value
            shard_uri = shard.shard_uri

            features, labels = read_shard_uri(shard_uri)
            shard_ids.append(shard_id)
            shard_data.append((features, labels))

            # Report full consumption back to the coordinator so it can
            # track data-plane progress and re-assign on worker failure.
            # Use the lease_epoch the coordinator included in the assignment
            # so the progress report is not rejected as stale.
            lease_epoch = int(getattr(shard, "lease_epoch", 0))
            bytes_read = features.nbytes + labels.nbytes
            prog_key = (
                f"{idempotency_key}-prog-{shard_id}" if idempotency_key else ""
            )
            self.client.report_shard_progress(
                run_id=self.run_id,
                worker_id=self.worker_id,
                dataset_id=self.dataset_id,
                shard_id=shard_id,
                lease_epoch=lease_epoch,
                progress_offset=bytes_read,
                is_complete=True,
                idempotency_key=prog_key,
            )

        return shard_ids, shard_data

    def load_next_shard_as_tensors(
        self,
        idempotency_key: str = "",
    ) -> tuple[list[str], "torch.Tensor", "torch.Tensor"]:
        """Like :meth:`load_next_shard` but concatenates into torch tensors.

        Returns:
            ``(shard_ids, features_tensor, labels_tensor)`` — features is
            ``float32`` and labels is ``int64``.  Both tensors have zero
            rows when no shards are assigned.
        """
        import torch

        shard_ids, shard_data = self.load_next_shard(idempotency_key=idempotency_key)
        if not shard_data:
            return shard_ids, torch.empty(0), torch.empty(0, dtype=torch.long)

        all_features = np.concatenate([f for f, _ in shard_data], axis=0)
        all_labels = np.concatenate([lb for _, lb in shard_data], axis=0)
        return (
            shard_ids,
            torch.from_numpy(all_features),
            torch.from_numpy(all_labels).long(),
        )
