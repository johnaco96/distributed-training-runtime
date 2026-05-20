# Dashboard Plan (Phase 20F-5 Secure Design Track)

## Current status

- Dashboard is **not implemented**.
- No dashboard server currently exists.
- No dashboard authentication/authorization flow exists yet.
- Structured audit/security event foundation from Phase 20F-4 exists and can be used as a source for future dashboard security visibility.
- Metrics/log visibility currently exists only through local/runtime foundations (service metrics snapshots, logs, and docs), not a dedicated dashboard product.

## Target secure dashboard model

Target direction is staged and conservative:

1. **Read-only local dashboard first.**
2. **Authentication required before any non-local exposure.**
3. OIDC/human identity provider integration comes later (post-MVP).
4. Role separation:
   - read-only viewer
   - operator
   - admin
5. Dashboard must never display:
   - secrets
   - raw auth metadata maps
   - private key/certificate/token material
   - object-store credentials
6. Dashboard service is **not exposed publicly by default**.
7. Default bind/exposure should remain localhost/local-cluster only until explicit hardening gates are complete.

## Proposed dashboard views (future)

- Active runs
- Run status
- Active/stale/failed workers
- Shard assignment overview
- Checkpoint lifecycle state
- Latest checkpoint summary
- Audit/security event summary
- Benchmark summary ingestion
- Coordinator health
- Storage/checkpoint cleanup status

## Implementation phases

- **Dashboard-1:** Read-only CLI/status endpoint or local-only API summary.
- **Dashboard-2:** Local read-only web UI with no public exposure.
- **Dashboard-3:** Authentication and role-based access.
- **Dashboard-4:** Audit/security-event view.
- **Dashboard-5:** Optional Prometheus/Grafana integration.
- **Dashboard-6:** Production deployment hardening.

## Acceptance criteria

A dashboard track is only considered secure-ready when:

- No secrets are displayed in UI/API payloads.
- Auth is required for non-local use.
- Dashboard logs exclude secrets and sensitive auth material.
- Security event visibility exists (at least summary-level from audit events).
- Safe defaults are enforced (local bind/no public exposure by default).
- Documentation and tests cover security behavior and non-claims.

## Risk register

- Accidental public exposure of dashboard endpoint.
- Secret leakage in views, APIs, or logs.
- Confusing local metrics behavior with production-scale observability guarantees.
- Dashboard becoming an unintended control-plane mutation path.
- Missing/incomplete authorization checks for sensitive data.

## Explicit non-claims

This design track does **not** implement:
- A production dashboard server.
- OIDC authentication.
- Production-ready dashboard authorization.
- Public dashboard deployment defaults.
