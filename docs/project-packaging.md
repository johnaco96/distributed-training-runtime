# Project Packaging Guide

## Project identity / positioning

Distributed Training Runtime is packaged as a **local-first ML runtime control-plane prototype** focused on coordination semantics and recovery workflows, not as a production cluster scheduler.

## Intended audience

- ML systems reviewers
- Infrastructure engineers
- Research/prototype evaluators
- Portfolio/interview reviewers

## Core capabilities to emphasize

- Rust coordinator + Python SDK control-plane workflow
- Deterministic dataset registration and shard assignment
- Worker lifecycle tracking (registration, heartbeat, liveness semantics)
- Checkpoint metadata + artifact lifecycle experiments
- Local recovery/resume-oriented ML example paths
- Local benchmark comparison of standalone vs coordinator-driven training
- Optional mTLS and role-based authorization
- Structured audit logging
- PostgreSQL HA via advisory lock

## Claims to avoid

Do **not** claim:
- production readiness
- Kubernetes-native training operator capabilities
- real multi-node distributed training proofs
- `torch.distributed` integration
- production HA/auth/dashboard completeness
- benchmark-based superiority claims

## Demo narrative

1. Show Rust coordinator + Python SDK local demo.
2. Show coordinator-driven PyTorch training flow.
3. Show checkpoint/resume artifacts and metrics.
4. Show benchmark overhead summary report (JSON/CSV).
5. Show optional TLS/mTLS and auth mode with local-secure Kubernetes path.

## Suggested repository tour

- `crates/runtime-coordinator`
- `crates/runtime-proto`
- `crates/runtime-checkpoint`
- `crates/runtime-storage`
- `crates/runtime-bench`
- `python/dtruntime`
- `python/examples`
- `docs`

## README badges guidance

Only include badges/placeholders that are already backed by real automation/status signals. Do not add fake or speculative badges.

## Release-readiness checklist

- tests/checks green in target environment
- generated artifacts cleaned from working tree
- docs updated (README + runbooks + status pages)
- benchmark results labeled local/environment-specific
- limitations prominently visible and conservative

## Local Kubernetes packaging

A local/dev-first deployment path is available:
- Build image from `Dockerfile`.
- Apply manifests from `k8s/local/`.
- Run the `pytorch-coordinator-demo` Job against the in-cluster coordinator Service.

For secure local Kubernetes with TLS/mTLS, use `k8s/local-secure/`.

This extends packaging demonstration coverage without claiming production orchestration maturity.

## Security/readiness packaging note

For reviewer-facing clarity, include links to the security foundation docs and avoid production-security claims:
- [Security architecture](security-architecture.md)
- [Threat model](threat-model.md)
- [Production readiness](production-readiness.md)

Do not claim full auth/dashboard/HA/production S3 readiness. Packaging outputs remain local/dev oriented.
