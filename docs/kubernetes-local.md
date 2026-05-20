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
docker build -t distributed-training-runtime:local-secure .
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

## Security posture reminder

Local Kubernetes manifests are local/dev only. Default path is plaintext and no-auth unless explicitly configured using optional placeholders. No public exposure is configured by default.



## Optional secure local/dev path (`k8s/local-secure`)

This path is local/dev only and not a production claim.

1. Generate local dev certs (ignored output under `.dtruntime-certs/`):
   - `pwsh -File scripts/generate_dev_certs.ps1`
   - The generated server certificate includes SANs for `localhost`, `127.0.0.1`, `runtime-coordinator`, `runtime-coordinator.dtruntime`, `runtime-coordinator.dtruntime.svc`, and `runtime-coordinator.dtruntime.svc.cluster.local`.
2. Create namespace: `kubectl apply -f k8s/local-secure/namespace.yaml`
3. Create TLS Secret from generated local files (example names):
   - `kubectl -n dtruntime create secret tls dtruntime-tls --cert=.dtruntime-certs/server.crt --key=.dtruntime-certs/server.key --dry-run=client -o yaml | kubectl apply -f -`
4. Create auth secret placeholder/environment inputs (do not commit real values):
   - `kubectl apply -f k8s/local-secure/auth-secret.example.yaml`
5. Apply secure manifests: `kubectl apply -f k8s/local-secure/`
6. Wait for coordinator: `kubectl -n dtruntime rollout status deployment/runtime-coordinator`
7. Run secure PyTorch job if configured: `kubectl apply -f k8s/local-secure/pytorch-coordinator-job.yaml`
8. Inspect logs: `kubectl -n dtruntime logs deployment/runtime-coordinator`
9. Cleanup: `kubectl delete -f k8s/local-secure/`

No cert/key/token material is committed in-repo.


Observability update: Step 1 Prometheus/Grafana is implemented. See `docs/observability.md`, `observability/prometheus/prometheus.yml`, and `observability/grafana/dtruntime-dashboard.json`.

## Step 2 readiness/liveness/startup probes

`k8s/local/` and `k8s/local-secure/` now configure coordinator `startupProbe`, `livenessProbe`, and `readinessProbe` against the metrics port (`:9090`):
- `/healthz` is used for liveness/startup.
- `/readyz` is used for readiness gating.

This prevents routing traffic before readiness and allows Kubernetes to restart unhealthy pods.

## Step 3 secure deployment validation
For secure local validation, follow `k8s/local-secure/README.md` and apply manifests in order (service, deployment, job). Do not use placeholder YAML secrets as runtime inputs; create real Secrets with `kubectl create secret tls` / `kubectl create secret generic` from `.dtruntime-certs/`.
For secure local validation, the job endpoint is `runtime-coordinator.dtruntime.svc.cluster.local:50051` and TLS hostname verification remains enabled (no `ssl_target_name_override`). Never commit generated cert/key/CA files from `.dtruntime-certs/`.

