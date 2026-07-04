# Production deployment

This guide covers deploying the coordinator control plane to a production
Kubernetes cluster. For the local/dev path see
[kubernetes-local.md](kubernetes-local.md); for the operator see
[kubernetes-operator.md](kubernetes-operator.md).

Two equivalent install paths are provided:

- **Helm chart** — `deploy/helm/dtruntime` (recommended; most configurable).
- **Raw manifests** — `k8s/prod` (kustomize; for GitOps / non-Helm teams).

## Prerequisites

- Kubernetes ≥ 1.24.
- A **PostgreSQL** database for HA + durable metadata. Create a Secret with the
  DSN:
  ```bash
  kubectl -n dtruntime create secret generic postgres-credentials \
    --from-literal=DATABASE_URL='postgres://user:pass@host:5432/dtruntime'
  ```
- **cert-manager** (for TLS), or provide a TLS Secret yourself.
- Optional: the **Prometheus Operator** CRDs (for `ServiceMonitor` /
  `PrometheusRule`).
- A published coordinator image (see [RELEASING.md](RELEASING.md)) or your own
  registry mirror.

## Install with Helm

```bash
helm install dtruntime deploy/helm/dtruntime \
  -n dtruntime --create-namespace \
  -f deploy/helm/dtruntime/values-production.yaml \
  --set image.tag=0.1.0

kubectl -n dtruntime rollout status deploy/dtruntime
```

Key production values (see `values.yaml` for the full list):

| Value | Purpose |
|---|---|
| `metadataStore.backend=postgres` | HA + durable metadata (required for >1 replica) |
| `coordinator.replicas=2` | Leader + hot standby (advisory-lock HA) |
| `auth.mode=metadata` + `auth.disableHeaderIdentity=true` | Enforce authz; reject caller-asserted headers |
| `auth.oidc.*` / `auth.spiffe.*` | OIDC/JWT and/or SPIFFE identity ([security-auth.md](security-auth.md)) |
| `tls.enabled=true` + `certManager.enabled=true` | TLS via cert-manager |
| `monitoring.serviceMonitor.enabled` / `prometheusRule.enabled` | Scrape + alerts ([slos.md](slos.md)) |
| `networkPolicy.enabled=true`, `podDisruptionBudget.enabled=true` | Network isolation + disruption safety |
| `operator.enabled=true` | Also deploy the TrainingRun operator |

## Install with raw manifests

```bash
kubectl apply -f k8s/prod/cert-manager/     # ClusterIssuer + Certificate
kubectl apply -k k8s/prod                   # namespace, SA, Service, Deployment, PDB, NetworkPolicy
```

Set the image via the kustomization's `images:` field (defaults to
`ghcr.io/johnaco96/distributed-training-runtime:0.1.0`).

## High availability

The coordinator runs **leader + standby**: both replicas start, but only the one
holding the PostgreSQL advisory lock binds the gRPC port and serves traffic. The
standby's TCP readiness probe fails (keeping it out of the Service endpoints)
until the leader exits and it acquires the lock (~5 s), then it starts serving.
`topologySpreadConstraints` place the replicas on different nodes; the
`PodDisruptionBudget` keeps at least the leader available during voluntary
disruptions. See [high-availability.md](high-availability.md).

## Hardening posture

The production deployment runs the coordinator:

- as **non-root** (`runAsNonRoot`, uid/gid 65532) with a **read-only root
  filesystem** (only the checkpoint staging dir is writable),
- with **all Linux capabilities dropped** and `allowPrivilegeEscalation: false`,
  under the `RuntimeDefault` seccomp profile,
- in a namespace labeled for the **restricted** Pod Security Standard,
- behind a **NetworkPolicy** limiting ingress to the gRPC/metrics ports,
- with **auth enforced** and dev header identity disabled,
- with **TLS** terminated at the coordinator (optionally mTLS).

See [security-auth.md](security-auth.md), [threat-model.md](threat-model.md),
and [secret-rotation.md](secret-rotation.md).

## Images and provenance

Release images are multi-arch (amd64/arm64), **cosign-signed**, and carry SLSA
provenance + an SBOM. Verify before deploying:

```bash
cosign verify \
  --certificate-identity-regexp 'https://github.com/johnaco96/distributed-training-runtime/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  ghcr.io/johnaco96/distributed-training-runtime:0.1.0
```

Enforce signature verification cluster-side with a policy controller (e.g.
Sigstore policy-controller or Kyverno) admitting only signed images.

## Verify the deployment

```bash
# Readiness (only the leader is Ready)
kubectl -n dtruntime get pods -l app.kubernetes.io/component=coordinator

# From a client pod (or port-forward):
dtruntime-admin -a dtruntime.dtruntime:50051 --token "$JWT" doctor
```

## Compatibility / support matrix

| Component | Supported |
|---|---|
| Kubernetes | ≥ 1.24 |
| Architectures | linux/amd64, linux/arm64 |
| Metadata store | PostgreSQL (HA), SQLite (single-node/dev) |
| Python SDK | 3.9 – 3.12, OS-independent |
| Coordinator host builds (CI) | Linux, macOS, Windows |

The coordinator/SDK build and unit-test on Linux, macOS, and Windows (see
`.github/workflows/compat.yml`); container images target Linux. Production
Kubernetes deployment is validated locally against kind and documented here —
it has not been run on a managed cloud cluster at scale.
