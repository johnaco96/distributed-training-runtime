# Rust data plane throughput (measured)

**What changed:** DTRS shard decode and shard fetch moved from Python into
`dtruntime._native` (PyO3, `crates/dtruntime-native`). Feature tensors are
handed to PyTorch **zero-copy** through the buffer protocol — the tensor
borrows the Rust-decoded memory, no serialization boundary. The pure-Python
path remains the fallback (`DTRUNTIME_NATIVE=0` forces it), and both paths
are asserted bit-identical in `python/tests/test_native_dtrs.py`.

**Environment:** Windows 11, local NVMe, Python 3.13, single process; the
s3:// leg targets MinIO running in WSL2 over loopback. Numbers are
best-of-3; reproduce with:

```bash
python scripts/build_native.py
python benchmarks/dataloading/bench_dtrs.py --s3-bucket <bucket>
```

## Results (8 shards × 4096 records × dim 512 = 67.2 MB)

| Path | Pure Python | Rust (`_native`) | Speedup |
|---|---|---|---|
| `ShardReader.read_all()` (batch load) | 171,615 rec/s (352 MB/s) | **998,540 rec/s (2,049 MB/s)** | **5.8×** |
| Data-pipeline record iteration | 373,367 rec/s | 561,903 rec/s | 1.5× |
| `ShardStream` file:// (prefetch=3 + SHA-256 verify) | — | 1,230,418 rec/s (2,525 MB/s) | — |
| `ShardStream` s3:// MinIO loopback (prefetch=3 + SHA-256 verify) | — | 386,146 rec/s (792 MB/s) | — |

Interpretation:

- The **batch path** is where zero-copy pays: one GIL-released decode pass,
  one `torch.frombuffer` view. 5.8× is the headline and it is real.
- The **record-iteration path** improves only 1.5× because its contract
  (each record is an independent writable numpy row) forces a per-row copy
  after the Rust decode; the remaining time is Python-side row handling,
  not decode.
- `ShardStream` numbers **include** SHA-256 integrity verification of every
  shard. The s3:// figure is bounded by the loopback object store, not the
  decoder — it exists to show the fetch+verify+decode pipeline works against
  real object storage, not as a network benchmark.
- Single process, local machine, synthetic shards. No claim is made about
  multi-node throughput here.
