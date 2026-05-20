# Status and Limitations (Phase 19D)

## Completion status

- Phases **10 through 18** are complete and locally validated.
- Phase **19A** (README + quickstart), **19B** (architecture diagrams/overview), and **19C** (runbooks/examples indexing) are complete.
- Phase **19D** completes documentation polish by adding troubleshooting guidance plus this final status/limitations pass.

## Current capabilities

Implemented capabilities today include:

- Rust coordinator gRPC control plane.
- Python SDK for coordinator interactions.
- Dataset registration and deterministic shard assignment flows.
- Checkpoint lifecycle support including integrity-oriented commit validation and cleanup helper paths.
- Storage abstraction with local filesystem support and optional S3/MinIO-compatible backend paths.
- Benchmark harness for local RPC and scaling-style smoke runs.
- Observability foundation (structured logs + in-process metrics).
- PyTorch single-worker example.
- PyTorch simulated distributed-style example.

## Current limitations and non-goals

This project currently does **not** claim:

- Production scheduler integration.
- Kubernetes deployment/orchestration support.
- Prometheus/Grafana exporter integration.
- Real `torch.distributed` execution.
- Real multi-node ML orchestration.
- Production-ready cloud deployment posture.
- Proven production fault tolerance.
- Large-scale benchmark claims without measured evidence.

Important boundary: local simulated worker scaling is useful for smoke/perf exploration, but it is **not** proof of real cluster scale.

## Validation status

Locally validated baseline commands generally include:

- `cargo fmt`
- `cargo check --workspace`
- `cargo test --workspace`
- `PYTHONPATH=python python -m pytest python/tests`
- `PYTHONPATH=python python python/examples/local_demo.py`
- `DTRUNTIME_RUN_LOCAL_INTEGRATION=1 PYTHONPATH=python python -m pytest python/tests/test_local_integration.py -v`

Optional or environment-gated validation surfaces:

- MinIO tests (require env gate and local MinIO service setup).
- PyTorch example runs (require torch installed).
- Any network-dependent dependency fetches (may fail in restricted environments).

Before external demo/release checkpoints, re-run the full local validation set in a clean environment and include any optional paths relevant to your deployment goals.

## Suggested next phase boundary

Phase 19 ends with docs polish and conservative status framing.

**Phase 20 should be selected explicitly after this point.** It is intentionally not started in Phase 19D.
