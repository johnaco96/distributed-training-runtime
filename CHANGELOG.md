# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- **Packaging & deployment**: production Helm chart (`deploy/helm/dtruntime`) and
  a hardened raw-manifest bundle (`k8s/prod`, kustomize) with HA coordinator,
  PodDisruptionBudget, NetworkPolicy, non-root securityContext, ServiceMonitor,
  and PrometheusRule. Tag-triggered release workflow producing a Python
  sdist/wheel, multi-arch container images (cosign-signed, with SLSA provenance
  + SBOM), and a packaged Helm chart. Cross-platform CI matrix (Linux/macOS/
  Windows + container build). Distribution metadata + `docs/RELEASING.md`,
  `docs/production-deployment.md`.
- **Observability & ops**: Prometheus recording/alerting rules, SLOs with
  multiwindow burn-rate alerts, on-call runbooks, and gated failover/chaos/soak
  tests with measured benchmarks.
- **Security**: layered OIDC/JWT + SPIFFE authentication, role/permission RBAC,
  hot JWT key rotation, NDJSON/SIEM audit sink, and `cargo-deny` + `pip-audit`
  supply-chain scanning.
- **Admin & operator experience**: `dtruntime-admin` CLI and admin/inspection
  RPCs (pause/resume/retry/drain, list runs/workers/shards/checkpoints, doctor,
  checkpoint verify).

Earlier phases (real torch.distributed execution, the Kubernetes operator,
failure semantics, distributed checkpointing, the streaming data pipeline, and
S3/persistence hardening) predate this changelog; see the git history and
`docs/status-and-limitations.md`.

## [0.1.0] - unreleased

Initial development baseline.
