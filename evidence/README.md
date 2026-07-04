# Evidence bundles

Reproducible proof that the ⚠️ ("implemented, locally validated") items in
[docs/support-matrix.md](../docs/support-matrix.md) actually work against the
**real** external software — not simulated. Each subdirectory is one item; each
run drops a dated bundle:

```
evidence/<item>/<YYYY-MM-DD>/
  ├── result.json        # machine-readable pass/fail + key metrics
  ├── <component>.log     # component logs / query output
  └── notes.md            # what was run + how to reproduce
```

The harnesses that produce them live under [`tools/evidence/`](../tools/evidence/)
and are runnable locally (docker compose / kind). Tier-2 (real GPU/cloud) proof
is captured the same way once run on that infra.

| Item | Harness | Proves |
|---|---|---|
| SIEM ingestion | `tools/evidence/siem` | Audit NDJSON actually ingests into a real log store (Loki) via Vector |
| SPIFFE/SPIRE | `tools/evidence/spiffe` | A real SPIRE-issued SVID authenticates to the coordinator |
| Volcano + Kueue | `tools/evidence/schedulers` | A `TrainingRun` is really gang-scheduled / quota-admitted in a live cluster |
| Multi-node | `tools/evidence/multinode` | Rendezvous + checkpoint restore + data locality across separate nodes |
