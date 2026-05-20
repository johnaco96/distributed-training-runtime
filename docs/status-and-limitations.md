# Status and Limitations (Phase 20E)

## Completion status

- Phase **20D** (project packaging/documentation polish) is complete.
- Phase **20E** (local/dev Kubernetes deployment path) is complete.
- Demo/release checklist: [`docs/demo-checklist.md`](demo-checklist.md).
- Packaging/positioning guide: [`docs/project-packaging.md`](project-packaging.md).

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
- Production Kubernetes deployment/orchestration support (beyond local/dev manifests).
- Prometheus/Grafana exporter integration.
- Real `torch.distributed` execution.
- Real multi-node ML orchestration.
- Production-ready cloud deployment posture.
- Proven production fault tolerance.
- Large-scale benchmark claims without measured evidence.

Important boundary: local simulated worker scaling is useful for smoke/perf exploration, but it is **not** proof of real cluster scale.

## Phase 20+ direction

Phase 20 roadmap and sequencing are documented in [Product direction](product-direction.md). This document remains the conservative source for current capabilities and limitations until those future phases are implemented and validated.

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


Local Kubernetes deployment manifests exist in `k8s/local/` and are documented in `docs/kubernetes-local.md`. This is intentionally not a production Kubernetes platform.

## Phase 20F-1 status update

- Phase **20F-1** documentation foundation is complete.
- Phase **20F-2** through **20F-5** remain pending.

Security-related references:
- [Security architecture](security-architecture.md)
- [Threat model](threat-model.md)
- [Production readiness](production-readiness.md)
- [Security auth plan](security-auth.md)
- [High availability plan](high-availability.md)
- [Dashboard plan](dashboard-plan.md)
- [Production S3 plan](production-s3.md)

Local Kubernetes remains local/dev only and is not a production security posture.
\n\n## Phase 20F-2 TLS/mTLS foundation\nPhase 20F-2 adds optional transport security for coordinator gRPC and Python RuntimeClient. Plaintext remains the default local/dev behavior when TLS options are omitted.\n\nCoordinator TLS options:\n- --tls-cert /path/to/server.crt\n- --tls-key /path/to/server.key\n- --tls-client-ca /path/to/ca.crt (optional; enables client cert verification/mTLS)\n\nExample server commands:\n- runtime-coordinator --addr 0.0.0.0:50051 --tls-cert /path/to/server.crt --tls-key /path/to/server.key\n- runtime-coordinator --addr 0.0.0.0:50051 --tls-cert /path/to/server.crt --tls-key /path/to/server.key --tls-client-ca /path/to/ca.crt\n\nPython RuntimeClient supports TLS args and env vars: tls_ca_cert, tls_client_cert, tls_client_key and DTRUNTIME_TLS_CA_CERT, DTRUNTIME_TLS_CLIENT_CERT, DTRUNTIME_TLS_CLIENT_KEY.\n\nNo certificates/keys are committed in-repo; use placeholders such as k8s/local/tls-secret.example.yaml.\nNo workload authorization policy is implemented in 20F-2; that is planned for Phase 20F-3.\nNo production PKI issuance/rotation is implemented in 20F-2.\n
