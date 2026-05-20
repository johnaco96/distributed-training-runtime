# Local Kubernetes Runbook (Phase 20E)

## Scope and non-goals

This runbook provides a **local/dev-first** Kubernetes demonstration path for distributed-training-runtime.

What this covers:
- Deploying a single runtime coordinator pod.
- Running a coordinator-driven PyTorch demo job against that service.

What this does **not** cover:
- Production Kubernetes readiness.
- HA/replicated coordinators.
- Authentication/authorization.
- Dashboard/UX.
- Production scheduler integrations and autoscaling.
- `torch.distributed` multi-node execution.
- Production S3 hardening.

## Prerequisites

- Docker
- kubectl
- kind or minikube

## Build local image

```bash
docker build -t distributed-training-runtime:local .
```

## Create local cluster (kind example)

```bash
kind create cluster --name dtruntime
kind load docker-image distributed-training-runtime:local --name dtruntime
```

## Apply manifests

```bash
kubectl apply -f k8s/local/
```

## Check coordinator

```bash
kubectl get pods -n dtruntime
kubectl logs deployment/runtime-coordinator -n dtruntime
```

## Run/check PyTorch coordinator demo job

```bash
kubectl get jobs -n dtruntime
kubectl logs job/pytorch-coordinator-demo -n dtruntime
```

## Cleanup

```bash
kubectl delete -f k8s/local/
kind delete cluster --name dtruntime
```

## Caveats

- Local/dev only.
- No HA.
- No auth.
- No dashboard.
- No production scheduler.
- Not `torch.distributed`.
- Not proof of multi-node benchmark scaling.
- Benchmark results measured in local Kubernetes should be tracked separately from local subprocess benchmarks.

## Security note (Phase 20F-1)

The manifests under `k8s/local/` are local/dev only. They do not yet implement production authn/authz or mTLS.

A placeholder secret example exists at `k8s/local/auth-secret.example.yaml` for future local/dev experiments only; it is not wired into current deployment flow.

See:
- [Security architecture](security-architecture.md)
- [Threat model](threat-model.md)
- [Production readiness](production-readiness.md)
\n\n## Phase 20F-2 TLS/mTLS foundation\nPhase 20F-2 adds optional transport security for coordinator gRPC and Python RuntimeClient. Plaintext remains the default local/dev behavior when TLS options are omitted.\n\nCoordinator TLS options:\n- --tls-cert /path/to/server.crt\n- --tls-key /path/to/server.key\n- --tls-client-ca /path/to/ca.crt (optional; enables client cert verification/mTLS)\n\nExample server commands:\n- runtime-coordinator --addr 0.0.0.0:50051 --tls-cert /path/to/server.crt --tls-key /path/to/server.key\n- runtime-coordinator --addr 0.0.0.0:50051 --tls-cert /path/to/server.crt --tls-key /path/to/server.key --tls-client-ca /path/to/ca.crt\n\nPython RuntimeClient supports TLS args and env vars: tls_ca_cert, tls_client_cert, tls_client_key and DTRUNTIME_TLS_CA_CERT, DTRUNTIME_TLS_CLIENT_CERT, DTRUNTIME_TLS_CLIENT_KEY.\n\nNo certificates/keys are committed in-repo; use placeholders such as k8s/local/tls-secret.example.yaml.\nNo workload authorization policy is implemented in 20F-2; that is planned for Phase 20F-3.\nNo production PKI issuance/rotation is implemented in 20F-2.\n


## Phase 20F-3 authorization scaffolding
Phase 20F-3 adds an internal authorization model with optional metadata-based caller identity for local/dev testing. Default mode remains disabled for compatibility. Enable with `runtime-coordinator --addr 0.0.0.0:50051 --auth-mode metadata` or `DTRUNTIME_AUTH_MODE=metadata`. Python client can attach `x-dtruntime-subject`, `x-dtruntime-role`, and optional `x-dtruntime-worker-id` via constructor args or env (`DTRUNTIME_AUTH_SUBJECT`, `DTRUNTIME_AUTH_ROLE`, `DTRUNTIME_AUTH_WORKER_ID`). This is policy scaffolding only (not full SPIFFE/SPIRE or production RBAC). Audit/security events remain planned for Phase 20F-4; dashboard/HA/production S3 hardening remain Phase 20F-5.
