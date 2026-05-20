# Local secure Kubernetes validation (TLS + mTLS)

This directory is the validated secure local/dev path for Step 3 production readiness.
It deploys the coordinator with TLS and mTLS enabled from Kubernetes Secrets and runs
a secure PyTorch smoke job against `runtime-coordinator.dtruntime.svc.cluster.local:50051`.
The secure job connects to `runtime-coordinator.dtruntime.svc.cluster.local:50051`, and TLS hostname verification remains enabled (no override).
The job also supplies `DTRUNTIME_AUTH_SUBJECT` and `DTRUNTIME_AUTH_ROLE` from `dtruntime-auth`; `python/examples/pytorch_coordinator_train.py` forwards these metadata headers via `RuntimeClient` for coordinator RPCs.
Do not commit generated cert/key/CA material under `.dtruntime-certs/`.


## Prerequisites

- Docker image available in your cluster nodes (for local clusters, build/tag locally).
- `kubectl` access to a development cluster.
- `openssl` and PowerShell available to run `scripts/generate_dev_certs.ps1` (Git for Windows often installs OpenSSL at `C:\Program Files\Git\usr\bin\openssl.exe`).

## Deterministic runbook

1) Clean namespace:

```bash
kubectl delete namespace dtruntime --ignore-not-found
```

2) Generate local/dev certificates (writes to ignored `.dtruntime-certs/`; server cert SANs include localhost and Kubernetes service DNS names used by the secure job):

```bash
pwsh ./scripts/generate_dev_certs.ps1
```

3) Create namespace:

```bash
kubectl create namespace dtruntime
```

4) Create TLS/mTLS secrets from generated certs:

```bash
kubectl create secret tls dtruntime-tls \
  --cert=.dtruntime-certs/server.crt \
  --key=.dtruntime-certs/server.key \
  -n dtruntime

kubectl create secret generic dtruntime-client-ca \
  --from-file=ca.crt=.dtruntime-certs/ca.crt \
  -n dtruntime

kubectl create secret tls dtruntime-client-tls \
  --cert=.dtruntime-certs/client.crt \
  --key=.dtruntime-certs/client.key \
  -n dtruntime
```

5) Create metadata auth secret:

```bash
kubectl create secret generic dtruntime-auth \
  --from-literal=DTRUNTIME_AUTH_SUBJECT=admin-local \
  --from-literal=DTRUNTIME_AUTH_ROLE=admin \
  -n dtruntime
```

6) Apply secure service + deployment (ordered; do not bulk apply directory):

```bash
kubectl apply -f k8s/local-secure/coordinator-service.yaml
kubectl apply -f k8s/local-secure/coordinator-deployment.yaml
```

7) If needed, pin image tag built for this step:

```bash
kubectl -n dtruntime set image deployment/runtime-coordinator coordinator=distributed-training-runtime:local-secure
```

8) Wait for coordinator readiness and verify probes:

```bash
kubectl -n dtruntime rollout status deploy/runtime-coordinator --timeout=180s
kubectl -n dtruntime get pods -l app=runtime-coordinator -o wide
kubectl -n dtruntime describe pod -l app=runtime-coordinator
```

9) Run secure PyTorch smoke job:

```bash
kubectl apply -f k8s/local-secure/pytorch-coordinator-job.yaml
kubectl -n dtruntime wait --for=condition=complete --timeout=300s job/pytorch-coordinator-demo
```

10) Show job logs:

```bash
kubectl -n dtruntime logs job/pytorch-coordinator-demo
```

11) Optional metrics/health verification:

```bash
kubectl -n dtruntime port-forward deploy/runtime-coordinator 9090:9090
curl -fsS http://127.0.0.1:9090/healthz
curl -fsS http://127.0.0.1:9090/readyz
curl -fsS http://127.0.0.1:9090/metrics | head
```

12) Cleanup:

```bash
kubectl delete namespace dtruntime --ignore-not-found
```


Secure job authorization model: separate admin and worker identities are required. `DTRUNTIME_AUTH_SUBJECT`/`DTRUNTIME_AUTH_ROLE` from `dtruntime-auth` are used only for admin/control-plane RPCs. Worker-scoped RPCs use per-worker identity (`role=worker`, subject defaults to `worker-N`, and `x-dtruntime-worker-id=worker-N`). Admin identity cannot impersonate worker RPCs under metadata auth mode.
