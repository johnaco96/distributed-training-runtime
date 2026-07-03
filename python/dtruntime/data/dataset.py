"""``DistributedShardDataset`` — a coordinator-backed ``IterableDataset``.

This is the top-level entry point for the data pipeline.  It composes the
format registry, disk cache, deterministic epoch shuffling, prefetching, and
locality splitting into a single ``torch.utils.data.IterableDataset`` that
plugs straight into ``DataLoader``.

Usage
-----
::

    from torch.utils.data import DataLoader
    from dtruntime.data import DistributedShardDataset
    from dtruntime.data.formats import features_label_transform

    ds = DistributedShardDataset(
        client=client, run_id=run_id, worker_id=worker_id,
        dataset_id="dataset", dataset_format="npz",
        transform=features_label_transform,
        shuffle=True, seed=1234,
        cache_dir="/var/cache/dtruntime", cache_max_bytes=8 << 30,
    )
    loader = DataLoader(ds, batch_size=32, num_workers=4)

    for epoch in range(epochs):
        ds.set_epoch(epoch)          # re-shuffles deterministically
        for features, labels in loader:
            train_step(features, labels)

What it does each ``__iter__`` (i.e. each ``DataLoader`` pass):

1. asks the coordinator for this rank's shards (stable per-rank subset),
   sending the warm-cache hint for sticky placement;
2. splits those shards across the DataLoader worker processes;
3. shuffles the shard order for the current epoch (deterministic);
4. streams records with prefetch + backpressure, optionally shuffling records
   within a shard via a reservoir buffer;
5. reports per-shard progress to the coordinator (resume-from-offset) and
   applies the user ``transform``.

Determinism is keyed on ``(seed, epoch)`` so all ranks/workers agree without
extra coordination and a resumed worker reproduces its order exactly.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Iterator, List, Optional

from dtruntime.data.cache import ShardCache
from dtruntime.data.formats import Record, get_format
from dtruntime.data.locality import (
    cache_hint_shard_ids,
    current_dataloader_worker,
    split_for_dataloader_worker,
)
from dtruntime.data.shuffle import reservoir_shuffle, shuffled_order
from dtruntime.data.streaming import ShardPrefetcher, ShardSpec, StreamedRecord

logger = logging.getLogger(__name__)

try:  # torch is an optional import at module load time
    from torch.utils.data import IterableDataset as _IterableDataset
except Exception:  # noqa: BLE001 - allow import without torch

    class _IterableDataset:  # type: ignore[no-redef]
        pass


class DistributedShardDataset(_IterableDataset):
    """Coordinator-backed, streaming, shufflable IterableDataset."""

    def __init__(
        self,
        client: Any,
        run_id: str,
        worker_id: str,
        dataset_id: str,
        *,
        dataset_format: str = "npz",
        dataset_version: Optional[str] = None,
        transform: Optional[Callable[[Record], Any]] = None,
        shuffle: bool = True,
        seed: int = 0,
        shuffle_buffer: int = 1024,
        cache_dir: Optional[str] = None,
        cache_max_bytes: int = 8 * 1024**3,
        verify_checksums: bool = True,
        prefetch_shards: int = 2,
        prefetch_records: int = 512,
        report_progress: bool = True,
        fetch_kwargs: Optional[dict] = None,
    ) -> None:
        self.client = client
        self.run_id = run_id
        self.worker_id = worker_id
        self.dataset_id = dataset_id
        self.dataset_format = dataset_format
        self.dataset_version = dataset_version
        self.transform = transform
        self.shuffle = shuffle
        self.seed = seed
        self.shuffle_buffer = shuffle_buffer
        self.verify_checksums = verify_checksums
        self.prefetch_shards = prefetch_shards
        self.prefetch_records = prefetch_records
        self.report_progress = report_progress
        self.fetch_kwargs = fetch_kwargs or {}
        self._epoch = 0
        self._last_assigned: List["_Assigned"] = []

        self._fmt = get_format(dataset_format)
        import tempfile

        self._cache = ShardCache(
            cache_dir or tempfile.mkdtemp(prefix="dtruntime-data-"),
            max_bytes=cache_max_bytes,
            verify_on_hit=False,  # verified on download; hits are trusted
        )

    # ── epoch control ────────────────────────────────────────────────────────

    def set_epoch(self, epoch: int) -> None:
        """Set the epoch that seeds this pass's shuffle (call before each pass)."""
        self._epoch = int(epoch)

    @property
    def epoch(self) -> int:
        return self._epoch

    @property
    def cache(self) -> ShardCache:
        return self._cache

    # ── iteration ────────────────────────────────────────────────────────────

    def __iter__(self) -> Iterator[Any]:
        specs = self._plan_specs()
        if not specs:
            return

        prefetcher = ShardPrefetcher(
            specs,
            self._fmt,
            self._cache,
            prefetch_shards=self.prefetch_shards,
            prefetch_records=self.prefetch_records,
            on_shard_done=self._report_shard_done if self.report_progress else None,
            fetch_kwargs=self.fetch_kwargs,
        )

        def _records() -> Iterator[StreamedRecord]:
            with prefetcher:
                yield from prefetcher

        stream = _records()
        if self.shuffle and self.shuffle_buffer > 1:
            stream = reservoir_shuffle(
                stream,
                self.shuffle_buffer,
                self.seed,
                self._epoch,
                salt="records",
            )

        for streamed in stream:
            record = streamed.record
            yield self.transform(record) if self.transform is not None else record

    # ── planning ─────────────────────────────────────────────────────────────

    def _plan_specs(self) -> List[ShardSpec]:
        """Resolve this rank + DataLoader-worker's ordered shard specs."""
        assignments = self._assigned_shards()
        if not assignments:
            return []

        # DataLoader-worker split (within this rank / process).
        wi, nw = current_dataloader_worker()
        my_shards = split_for_dataloader_worker(assignments, wi, nw)
        if not my_shards:
            return []

        # Per-epoch deterministic shard-order shuffle (keeps whole-shard cache
        # locality; only order changes).
        if self.shuffle:
            my_shards = shuffled_order(my_shards, self.seed, self._epoch, salt="shards")

        specs: List[ShardSpec] = []
        for a in my_shards:
            checksum = a.checksum if self.verify_checksums else None
            specs.append(
                ShardSpec(
                    shard_id=a.shard_id,
                    uri=a.shard_uri,
                    checksum=checksum,
                    checksum_algorithm=a.checksum_algorithm or "sha256",
                    # Resume only applies to the first epoch of a resumed run;
                    # later epochs re-read the whole shard.
                    start_record=a.progress_offset if self._epoch == 0 else 0,
                )
            )
        return specs

    def _assigned_shards(self) -> List["_Assigned"]:
        """Call the coordinator for this worker's shards, with cache hints."""
        cached = cache_hint_shard_ids(self._last_assigned) if self._last_assigned else None
        response = self.client.request_shard(
            run_id=self.run_id,
            worker_id=self.worker_id,
            dataset_id=self.dataset_id,
            cached_shard_ids=cached,
        )
        assigned = [
            _Assigned(
                shard_id=a.shard_id.value,
                shard_uri=a.shard_uri,
                checksum=getattr(a, "checksum", "") or "",
                checksum_algorithm=getattr(a, "checksum_algorithm", "") or "sha256",
                lease_epoch=int(getattr(a, "lease_epoch", 0)),
                progress_offset=int(getattr(a, "progress_offset", 0)),
            )
            for a in response.assignments
        ]
        self._last_assigned = assigned
        return assigned

    def _report_shard_done(self, spec: ShardSpec, total_records: int) -> None:
        lease_epoch = 0
        for a in self._last_assigned:
            if a.shard_id == spec.shard_id:
                lease_epoch = a.lease_epoch
                break
        try:
            self.client.report_shard_progress(
                run_id=self.run_id,
                worker_id=self.worker_id,
                dataset_id=self.dataset_id,
                shard_id=spec.shard_id,
                lease_epoch=lease_epoch,
                progress_offset=total_records,
                is_complete=True,
            )
        except Exception as exc:  # noqa: BLE001 - progress is best-effort
            logger.warning("report_shard_progress failed for %s: %s", spec.shard_id, exc)


class _Assigned:
    __slots__ = (
        "shard_id",
        "shard_uri",
        "checksum",
        "checksum_algorithm",
        "lease_epoch",
        "progress_offset",
    )

    def __init__(
        self,
        shard_id: str,
        shard_uri: str,
        checksum: str,
        checksum_algorithm: str,
        lease_epoch: int,
        progress_offset: int,
    ) -> None:
        self.shard_id = shard_id
        self.shard_uri = shard_uri
        self.checksum = checksum
        self.checksum_algorithm = checksum_algorithm
        self.lease_epoch = lease_epoch
        self.progress_offset = progress_offset
