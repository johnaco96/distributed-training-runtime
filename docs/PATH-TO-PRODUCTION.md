# Path to production

This branch (`main`) is a **clean, focused prototype**: the Rust coordinator, the
Python SDK, and a one-command failure-recovery demo. It carries the pieces that
make the control plane itself trustworthy — layered OIDC/SPIFFE authentication,
RBAC, TLS/mTLS, durable SQLite/PostgreSQL metadata with advisory-lock HA,
hardened S3 storage, Prometheus metrics, and an audit log — and deliberately
leaves out the *deployment* layers (Kubernetes, Helm, operator, alerting rules,
release signing) so the core stays easy to read and obviously works.

A **full, production-hardened implementation of everything below already exists**
on the [`production-hardening`](https://github.com/johnaco96/distributed-training-runtime/tree/production-hardening) branch — this
document is the map from the prototype to that, in the order you'd actually
tackle it.

## The core is designed for this

The prototype already makes the choices that make scaling straightforward:

- **Control plane separated from the data plane** — the coordinator holds state
  and makes decisions; workers do the compute. You scale them independently.
- **Durable metadata** — run/worker state persists to SQLite *or* PostgreSQL and
  rehydrates on restart. Point it at Postgres and the coordinator is HA-ready.
- **A versioned gRPC contract** — backward/forward compatibility is enforceable
  at the wire, so clients and coordinator can be rolled independently.
- **Pluggable object storage** — local filesystem for dev, S3-compatible for
  production, behind one interface.

## The scaling steps (and where each is implemented on `production-hardening`)

| # | Concern | What you add | On the full branch |
|---|---|---|---|
| 1 | **Containerize** | Build coordinator + SDK images | `Dockerfile*`, multi-arch build |
| 2 | **Durable, HA control plane** | Multi-replica rollout + failover validation (PostgreSQL metadata and advisory-lock leader election already ship on `main` via `--metadata-db-url`) | `k8s/prod` replica topology |
| 3 | **Run it on Kubernetes** | Production manifests + a Helm chart (non-root, NetworkPolicy, PDB, resources) | `deploy/helm/dtruntime`, `k8s/prod` |
| 4 | **Declarative jobs** | A `TrainingRun` CRD + operator that renders worker Jobs and projects status | `crates/runtime-operator`, `k8s/operator` |
| 5 | **Cluster scheduling** | Gang scheduling / quota via Volcano or Kueue; Slurm/Ray/SageMaker submitters | operator scheduler backends, `docs/schedulers.md` |
| 6 | **Real object storage** | Bucket lifecycle policy and cloud-scale load validation (multipart, retry/jitter, SSE, and error classification already ship on `main`) | `runtime-storage` (S3), load tests |
| 7 | **Security** | Audit → SIEM pipeline, secret-rotation runbooks, threat-model docs (the OIDC/JWT + SPIFFE auth layers, RBAC, and TLS/mTLS already live on `main` in the coordinator's `security/` + `auth` modules) | `docs/security-*`, SIEM evidence harness |
| 8 | **Observability & SLOs** | Recording/alert rules, SLOs, on-call runbooks, Grafana (the Prometheus `/metrics`, `/healthz`, `/readyz` endpoints and the audit log already ship on `main`) | `observability/`, `docs/slos.md`, `docs/runbooks-oncall.md` |
| 9 | **Release engineering** | SemVer releases, signed images (cosign) + SLSA provenance, dependency scanning | `.github/workflows/release.yml`, `security-scan.yml`, `deny.toml` |
| 10 | **Scale validation** | Soak / chaos / failover tests, load tests, real benchmarks | `evidence/`, `tools/evidence/`, ops tests |

## Recommended order

1. **Containerize + Postgres HA** (#1–2) — the smallest step to a durable,
   restartable production control plane.
2. **Kubernetes + operator** (#3–4) — declarative `TrainingRun` jobs.
3. **Security + observability** (#7–8) — before real workloads run on it.
4. **Scheduling, release, scale-validation** (#5, #9–10) — as scale demands.

## The honest gap

Even the `production-hardening` branch is validated **locally and in `kind`**,
not on a real multi-node GPU cluster or managed cloud at scale. The rendezvous
and checkpoint paths are the same ones a real cluster would use, but multi-node
NCCL training, AWS S3 at load, and live cloud IAM/SIEM integration are the next
milestone. See that branch's `docs/support-matrix.md` and
`docs/status-and-limitations.md` for the precise, honest boundaries.
