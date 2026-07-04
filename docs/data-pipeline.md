# Data pipeline

`dtruntime.data` is a streaming, shufflable, cache-backed dataset layer that
plugs into PyTorch `DataLoader`. It turns the coordinator's shard-assignment
protocol into a production data path: workers stream their assigned shards
from a warm local cache, records are shuffled deterministically per epoch, and
progress is reported for resume. It complements the low-level control-plane
model in [dataset-sharding.md](dataset-sharding.md).

## Quick start

```python
from torch.utils.data import DataLoader
from dtruntime.data import DistributedShardDataset
from dtruntime.data.formats import features_label_transform

ds = DistributedShardDataset(
    client=client, run_id=run_id, worker_id=worker_id,
    dataset_id="dataset", dataset_format="npz",
    transform=features_label_transform,
    shuffle=True, seed=1234,
    cache_dir="/var/cache/dtruntime", cache_max_bytes=8 << 30,
    prefetch_shards=2, prefetch_records=512,
)
loader = DataLoader(ds, batch_size=32, num_workers=4)

for epoch in range(epochs):
    ds.set_epoch(epoch)                # re-shuffles deterministically
    for features, labels in loader:
        train_step(features, labels)
```

## How the requirements map

| Requirement | Implementation |
|---|---|
| **Streaming** | Shards are the unit of caching/locality; records stream out of a cached shard file lazily (`ShardFormat.iter_records`), so a shard never fully materialises. Parquet streams by row group, WebDataset by tar member, jsonl by line. |
| **Epoch shuffling** | Two composed, deterministic shuffles keyed on `(seed, epoch)`: shard visiting order (`shuffle.shuffled_order`) and within-shard records (a bounded reservoir shuffle, `shuffle.reservoir_shuffle`). Same `(seed, epoch)` reproduces the exact order — required for correct resume; different epoch reorders. |
| **Versioning** | Content-addressed: `compute_dataset_version(shards)` hashes the ordered `(shard_id, checksum)` pairs into `sha256:...`. `register_versioned_dataset` sets it at registration; `verify_dataset_version` pins a run to exact data and catches drift. |
| **Cache management** | `ShardCache`: size-bounded LRU disk cache with atomic downloads, per-key download dedup, checksum verification, pinning during streaming, and adoption of shards left on disk from a previous run (warm restart). |
| **Prefetching / backpressure** | `ShardPrefetcher` warms the next `prefetch_shards` shards with a thread pool and streams decoded records through a bounded `queue.Queue(maxsize=prefetch_records)`. A slow training loop blocks the producers (backpressure) instead of reading the whole dataset into memory. |
| **Formats beyond .npz** | Pluggable registry: `npz`, `dtrs` (the binary format), `jsonl`, `parquet` (pyarrow), `webdataset` (tar). Register custom formats with `register_format`. |
| **DataLoader / IterableDataset** | `DistributedShardDataset` **is** a `torch.utils.data.IterableDataset`; pass it straight to `DataLoader`. `num_workers>0` is handled: each worker process reads a disjoint round-robin slice of the rank's shards, so records are delivered exactly once. |
| **Multi-node data locality** | The coordinator gives each rank a **stable** shard subset (`shard_index % world_size == rank`). Between epochs only the order changes, never which node owns a shard — so the local disk cache stays warm and no data moves between nodes. The dataset reports its warm shard set as a cache hint for sticky placement. |

## Determinism and resume

All shuffling is a pure function of `(seed, epoch)`, so:

- every rank and DataLoader worker computes a consistent view with **no extra
  coordination** (they derive the same permutations from the shared seed);
- a worker that crashes and resumes at the same epoch reproduces its exact
  order, so the coordinator's per-shard `progress_offset` (records already
  consumed) correctly skips finished records on the first resumed epoch.

`set_epoch(epoch)` must be called before each pass (as with PyTorch's
`DistributedSampler`).

## Formats

```python
from dtruntime.data import get_format, supported_formats, register_format
# ['dtrs', 'jsonl', 'npz', 'parquet', 'webdataset']

class MyFormat:
    name = "myfmt"
    def iter_records(self, path, *, start=0): ...   # yield dict records
    def count_records(self, path): ...              # or return None
register_format(MyFormat())
```

Records are `dict[str, Any]`; a `transform` maps a record to whatever the
training loop consumes. `features_label_transform` handles the common tabular
case (`npz`/`dtrs`) → `(features_tensor, label_tensor)`.

Parquet requires the optional dependency: `pip install "dtruntime[parquet]"`.

## Cache

```python
from dtruntime.data import ShardCache
cache = ShardCache("/var/cache/dtruntime", max_bytes=8 << 30, verify_on_hit=False)
path = cache.get(uri, checksum, pin=True)   # downloads once, verifies, pins
cache.unpin(uri)
```

Downloads are atomic (`temp + os.replace`), so a crash never leaves a
half-written shard that looks cached. Concurrent gets of the same shard fetch
once. Set `verify_on_hit=True` to re-checksum on every read (catches bit-rot at
the cost of I/O); by default hits are trusted after the download-time check.

## Validation

- `python/tests/test_data_formats.py`, `test_data_cache_shuffle.py`,
  `test_data_dataset.py` — unit tests (formats incl. parquet/webdataset,
  cache LRU/pin/dedup/checksum, deterministic shuffle, versioning, DataLoader
  worker split, torch `DataLoader` integration). No coordinator required.
- `python/tests/test_data_integration.py` — two ranks stream a coordinator-
  registered versioned dataset and cover it exactly once with no overlap,
  keeping each rank's shard set stable across epochs. Gate:
  `DTRUNTIME_RUN_LOCAL_INTEGRATION=1`.
- Working example:
  [python/examples/data_pipeline_demo.py](../python/examples/data_pipeline_demo.py).
