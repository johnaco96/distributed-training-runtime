# Runbooks

This hub points to copy-ready validation and demo paths without duplicating full manuals.

## Recommended order (new developers)

1. [Quickstart](quickstart.md)
2. Full local validation
3. Local demo
4. Benchmarks
5. PyTorch examples
6. Optional S3/MinIO validation

## 1) Full local validation runbook

```bash
cargo fmt
cargo check --workspace
cargo test --workspace
PYTHONPATH=python python -m pytest python/tests
```

## 2) Local demo runbook

```bash
PYTHONPATH=python python python/examples/local_demo.py
```

Cleanup:

```powershell
Remove-Item -Recurse -Force .dtruntime-demo -ErrorAction SilentlyContinue
```

## 3) Benchmark runbook

Smoke/local scaling commands:

```bash
cargo run -p runtime-bench -- worker-rpc --workers 2 --heartbeats 2
cargo run -p runtime-bench -- scaling-workers --workers-list 1,2 --heartbeats 2
cargo run -p runtime-bench -- scaling-workers --simulate-large --workers-list 32,64 --heartbeats 1
```

More benchmark guidance: [Benchmarks](benchmarks.md), [Scaling tests](scaling-tests.md).

## 4) ML/PyTorch runbook (optional)

Single-worker fresh/resume/no-checkpoint:

```bash
PYTHONPATH=python python python/examples/pytorch_train.py --epochs 1 --samples 32 --batch-size 8 --out-dir .dtruntime-pytorch-smoke
PYTHONPATH=python python python/examples/pytorch_train.py --epochs 2 --samples 32 --batch-size 8 --out-dir .dtruntime-pytorch-smoke --resume
PYTHONPATH=python python python/examples/pytorch_train.py --epochs 1 --samples 32 --batch-size 8 --out-dir .dtruntime-pytorch-no-checkpoint --no-checkpoint
```

Simulated distributed-style fresh/resume/no-checkpoint:

```bash
PYTHONPATH=python python python/examples/pytorch_simulated_distributed.py --workers 2 --epochs 1 --samples 32 --batch-size 8 --shards 4 --out-dir .dtruntime-pytorch-dist-smoke
PYTHONPATH=python python python/examples/pytorch_simulated_distributed.py --workers 2 --epochs 2 --samples 32 --batch-size 8 --shards 4 --out-dir .dtruntime-pytorch-dist-smoke --resume
PYTHONPATH=python python python/examples/pytorch_simulated_distributed.py --workers 2 --epochs 1 --samples 32 --batch-size 8 --shards 4 --out-dir .dtruntime-pytorch-dist-no-checkpoint --no-checkpoint
```

Detailed notes: [ML Integration Runbook](ml-runbook.md).

## 5) Checkpoint/storage runbook (optional S3/MinIO)

```bash
cargo check -p runtime-storage --features s3
cargo test -p runtime-storage --features s3
cargo check -p runtime-checkpoint --features s3
cargo test -p runtime-checkpoint --features s3
```

Run MinIO-backed tests only when MinIO is configured and env-gated.

Details: [Object storage](object-storage.md), [Checkpoint cleanup](checkpoint-cleanup.md).

## 6) Cleanup runbook

```powershell
Remove-Item -Recurse -Force .dtruntime-demo -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .dtruntime-pytorch-smoke -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .dtruntime-pytorch-no-checkpoint -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .dtruntime-pytorch-dist-smoke -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .dtruntime-pytorch-dist-no-checkpoint -ErrorAction SilentlyContinue
```

## Related docs

- [Quickstart](quickstart.md)
- [Example commands](examples.md)
- [Troubleshooting](troubleshooting.md)
- [Status and limitations](status-and-limitations.md)


## Phase 20D packaging/demo references

- Demo/release checklist: [`docs/demo-checklist.md`](demo-checklist.md)
- Project packaging guide: [`docs/project-packaging.md`](project-packaging.md)
- Coordinator-driven PyTorch example: `python/examples/pytorch_coordinator_train.py`
- Orchestration benchmark: `python/examples/benchmark_pytorch_orchestration.py`


## 7) Local Kubernetes runbook (Phase 20E)

For local cluster demo deployment steps, see [`docs/kubernetes-local.md`](kubernetes-local.md).

Scope is intentionally local/dev only (single coordinator replica, no HA/auth/dashboard).

## Security runbook references (Phase 20F-1)

- [Security architecture](security-architecture.md)
- [Threat model](threat-model.md)
- [Production readiness](production-readiness.md)
- [Security auth plan](security-auth.md)
- [High availability plan](high-availability.md)
- [Dashboard plan](dashboard-plan.md)
- [Production S3 plan](production-s3.md)

Next implementation step is 20F-2 (optional TLS/mTLS foundation).
\n\n## Phase 20F-2 TLS/mTLS foundation\nPhase 20F-2 adds optional transport security for coordinator gRPC and Python RuntimeClient. Plaintext remains the default local/dev behavior when TLS options are omitted.\n\nCoordinator TLS options:\n- --tls-cert /path/to/server.crt\n- --tls-key /path/to/server.key\n- --tls-client-ca /path/to/ca.crt (optional; enables client cert verification/mTLS)\n\nExample server commands:\n- runtime-coordinator --addr 0.0.0.0:50051 --tls-cert /path/to/server.crt --tls-key /path/to/server.key\n- runtime-coordinator --addr 0.0.0.0:50051 --tls-cert /path/to/server.crt --tls-key /path/to/server.key --tls-client-ca /path/to/ca.crt\n\nPython RuntimeClient supports TLS args and env vars: tls_ca_cert, tls_client_cert, tls_client_key and DTRUNTIME_TLS_CA_CERT, DTRUNTIME_TLS_CLIENT_CERT, DTRUNTIME_TLS_CLIENT_KEY.\n\nNo certificates/keys are committed in-repo; use placeholders such as k8s/local/tls-secret.example.yaml.\nNo workload authorization policy is implemented in 20F-2; that is planned for Phase 20F-3.\nNo production PKI issuance/rotation is implemented in 20F-2.\n


## Phase 20F-3 authorization scaffolding
Phase 20F-3 adds an internal authorization model with optional metadata-based caller identity for local/dev testing. Default mode remains disabled for compatibility. Enable with `runtime-coordinator --addr 0.0.0.0:50051 --auth-mode metadata` or `DTRUNTIME_AUTH_MODE=metadata`. Python client can attach `x-dtruntime-subject`, `x-dtruntime-role`, and optional `x-dtruntime-worker-id` via constructor args or env (`DTRUNTIME_AUTH_SUBJECT`, `DTRUNTIME_AUTH_ROLE`, `DTRUNTIME_AUTH_WORKER_ID`). This is policy scaffolding only (not full SPIFFE/SPIRE or production RBAC). Audit/security events remain planned for Phase 20F-4; dashboard/HA/production S3 hardening remain Phase 20F-5.


Phase 20F-4 adds structured audit/security events; dashboard/SIEM/durable audit storage remain future work (20F-5+). Audit logs may include logical identity and run/checkpoint identifiers and should be protected.

## Phase 20F-5 runbook direction

Runbooks currently cover local/dev flows. Future runbooks should implement the production gates in [production-gates.md](production-gates.md), including dashboard auth, HA failover drills, and production S3 restore/retention validation.


## Local TLS/mTLS smoke

Generate local development certificates (PowerShell):

```powershell
pwsh -File scripts/generate_dev_certs.ps1
```

TLS coordinator:

```bash
cargo run -p runtime-coordinator -- --addr 127.0.0.1:50051 --tls-cert .dtruntime-certs/server.crt --tls-key .dtruntime-certs/server.key
```

mTLS coordinator:

```bash
cargo run -p runtime-coordinator -- --addr 127.0.0.1:50051 --tls-cert .dtruntime-certs/server.crt --tls-key .dtruntime-certs/server.key --tls-client-ca .dtruntime-certs/ca.crt
```


## Phase 20 hardening update (in progress)

- Added stricter env-gated MinIO/S3 integration-test behavior with explicit skip messaging.
- Expanded benchmark/runbook documentation labels and caveats for local/dev versus secure/minio optional surfaces.
- Secure Kubernetes runbook depth expanded for local certificate/secret creation flow.
- Production readiness is still not claimed; remaining gaps are tracked in status/production docs.
