# Local Kubernetes Runbook

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

## Security note

The manifests under `k8s/local/` are local/dev only. They do not enforce production authn/authz or mTLS by default.

A placeholder secret example exists at `k8s/local/auth-secret.example.yaml` for future local/dev experiments only; it is not wired into current deployment flow.

For production-grade local TLS/mTLS, use the `k8s/local-secure/` path described below.

See:
- [Security architecture](security-architecture.md)
- [Threat model](threat-model.md)
- [Production readiness](production-readiness.md)

## TLS/mTLS configuration

The coordinator supports optional TLS via:
- `--tls-cert /path/to/server.crt`
- `--tls-key /path/to/server.key`
- `--tls-client-ca /path/to/ca.crt` (optional; enables client cert verification/mTLS)

Python RuntimeClient supports TLS args and env vars: `tls_ca_cert`, `tls_client_cert`, `tls_client_key` and `DTRUNTIME_TLS_CA_CERT`, `DTRUNTIME_TLS_CLIENT_CERT`, `DTRUNTIME_TLS_CLIENT_KEY`.

No certificates/keys are committed in-repo; use placeholders such as `k8s/local/tls-secret.example.yaml`.

## Authorization mode

Enable metadata-based authorization with `--auth-mode metadata` or `DTRUNTIME_AUTH_MODE=metadata`.

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

## Observability

Step 1 Prometheus/Grafana is implemented. See `docs/observability.md`, `observability/prometheus/prometheus.yml`, and `observability/grafana/dtruntime-dashboard.json`.

## Readiness/liveness/startup probes

`k8s/local/` and `k8s/local-secure/` configure coordinator `startupProbe`, `livenessProbe`, and `readinessProbe` against the metrics port (`:9090`):
- `/healthz` is used for liveness/startup.
- `/readyz` is used for readiness gating.

This prevents routing traffic before readiness and allows Kubernetes to restart unhealthy pods.

## Secure deployment validation

For secure local validation, follow `k8s/local-secure/README.md` and apply manifests in order (service, deployment, job). Do not use placeholder YAML secrets as runtime inputs; create real Secrets with `kubectl create secret tls` / `kubectl create secret generic` from `.dtruntime-certs/`. The job endpoint is `runtime-coordinator.dtruntime.svc.cluster.local:50051` and TLS hostname verification remains enabled. Never commit generated cert/key/CA files from `.dtruntime-certs/`.

## Benchmark workflow (secure runtime)

Run `scripts/run_k8s_secure_validation.ps1` to execute an end-to-end secure Kubernetes validation and collect machine-readable artifacts under a timestamped output directory. Use unique image tags (or digests in production) and ensure deployment + job use the same exact image.
