"""Shard prefetching with bounded-queue backpressure.

Given an ordered list of shard specs, :class:`ShardPrefetcher` downloads the
next few shards to the cache *ahead* of consumption (so I/O overlaps compute)
while streaming decoded records through a bounded queue.  The queue bound is
the backpressure mechanism: when the training loop is slower than I/O, the
prefetch threads block on a full queue instead of reading the whole dataset
into memory.

Design
------
- A small thread pool warms the cache (``prefetch_shards`` shards ahead).
- A single producer thread streams records from cached shards in order and
  puts them on a ``queue.Queue(maxsize=prefetch_records)``.
- The consumer iterates the prefetcher; exceptions raised in the producer are
  re-raised in the consumer thread (no silent data loss).
- ``close()`` / context-manager exit stops threads promptly even if iteration
  was abandoned mid-stream.
"""

from __future__ import annotations

import queue
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator, List, Optional

from dtruntime.data.cache import ShardCache
from dtruntime.data.formats import Record, ShardFormat


@dataclass
class ShardSpec:
    """One shard to stream: where it is, how to verify it, and where to resume."""

    shard_id: str
    uri: str
    checksum: Optional[str] = None
    checksum_algorithm: str = "sha256"
    #: Records already consumed (resume-from-mid-shard); skipped on read.
    start_record: int = 0


@dataclass
class StreamedRecord:
    """A decoded record plus enough context to report progress."""

    shard_id: str
    record: Record
    #: 1-based count of records emitted from this shard so far.
    shard_record_index: int


_SENTINEL = object()


class ShardPrefetcher:
    """Streams records from an ordered list of shards with prefetch + backpressure."""

    def __init__(
        self,
        specs: List[ShardSpec],
        fmt: ShardFormat,
        cache: ShardCache,
        *,
        prefetch_shards: int = 2,
        prefetch_records: int = 512,
        record_transform: Optional[Callable[[Record], Record]] = None,
        on_shard_start: Optional[Callable[[ShardSpec], None]] = None,
        on_shard_done: Optional[Callable[[ShardSpec, int], None]] = None,
        fetch_kwargs: Optional[dict] = None,
    ) -> None:
        self._specs = specs
        self._fmt = fmt
        self._cache = cache
        self._prefetch_shards = max(1, prefetch_shards)
        self._transform = record_transform
        self._on_shard_start = on_shard_start
        self._on_shard_done = on_shard_done
        self._fetch_kwargs = fetch_kwargs or {}

        self._queue: "queue.Queue[Any]" = queue.Queue(maxsize=max(1, prefetch_records))
        self._stop = threading.Event()
        self._error: Optional[BaseException] = None
        self._producer: Optional[threading.Thread] = None
        self._warm_pool: List[threading.Thread] = []
        self._warm_lock = threading.Lock()
        self._next_to_warm = 0
        self._started = False

    # ── iteration ────────────────────────────────────────────────────────────

    def __iter__(self) -> Iterator[StreamedRecord]:
        self._start()
        while True:
            item = self._queue.get()
            if item is _SENTINEL:
                break
            yield item
        if self._error is not None:
            raise self._error

    def __enter__(self) -> "ShardPrefetcher":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def close(self) -> None:
        self._stop.set()
        # Drain so a blocked producer can make progress and exit.
        try:
            while True:
                self._queue.get_nowait()
        except queue.Empty:
            pass
        if self._producer is not None:
            self._producer.join(timeout=5)
        for t in self._warm_pool:
            t.join(timeout=5)

    # ── internals ────────────────────────────────────────────────────────────

    def _start(self) -> None:
        if self._started:
            return
        self._started = True
        # Warm the cache ahead of the producer with a small pool.
        pool_size = min(self._prefetch_shards, max(1, len(self._specs)))
        for _ in range(pool_size):
            t = threading.Thread(target=self._warm_loop, daemon=True)
            t.start()
            self._warm_pool.append(t)
        self._producer = threading.Thread(target=self._produce, daemon=True)
        self._producer.start()

    def _claim_warm_index(self) -> Optional[int]:
        with self._warm_lock:
            if self._next_to_warm >= len(self._specs):
                return None
            idx = self._next_to_warm
            self._next_to_warm += 1
            return idx

    def _warm_loop(self) -> None:
        while not self._stop.is_set():
            idx = self._claim_warm_index()
            if idx is None:
                return
            spec = self._specs[idx]
            try:
                self._cache.get(
                    spec.uri, spec.checksum, pin=True, **self._fetch_kwargs
                )
            except BaseException:  # noqa: BLE001 - surfaced by the producer on read
                # Leave it for the producer to retry/raise so ordering and
                # error reporting stay in one place.
                pass

    def _produce(self) -> None:
        try:
            for spec in self._specs:
                if self._stop.is_set():
                    break
                path = self._cache.get(
                    spec.uri, spec.checksum, pin=True, **self._fetch_kwargs
                )
                if self._on_shard_start is not None:
                    self._on_shard_start(spec)
                emitted = self._stream_shard(spec, path)
                self._cache.unpin(spec.uri)
                if self._on_shard_done is not None:
                    self._on_shard_done(spec, spec.start_record + emitted)
        except BaseException as exc:  # noqa: BLE001 - re-raised in consumer
            self._error = exc
        finally:
            self._put(_SENTINEL)

    def _stream_shard(self, spec: ShardSpec, path: Path) -> int:
        emitted = 0
        for record in self._fmt.iter_records(path, start=spec.start_record):
            if self._stop.is_set():
                break
            if self._transform is not None:
                record = self._transform(record)
            emitted += 1
            self._put(
                StreamedRecord(
                    shard_id=spec.shard_id,
                    record=record,
                    shard_record_index=spec.start_record + emitted,
                )
            )
        return emitted

    def _put(self, item: Any) -> None:
        # Block on a full queue (backpressure) but stay responsive to close().
        while not self._stop.is_set():
            try:
                self._queue.put(item, timeout=0.25)
                return
            except queue.Full:
                continue
