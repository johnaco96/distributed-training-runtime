# Audit Logging (Phase 20F-4)

Phase 20F-4 adds structured coordinator audit/security events for startup, authn/authz decisions, and key state-changing RPCs.

## Implemented categories
- Coordinator startup and security configuration mode events.
- Auth metadata acceptance/rejection.
- Authorization allowed/denied decisions.
- Key mutation and checkpoint lifecycle events.

## Not logged by design
- Secrets/tokens.
- Private keys or certificate contents.
- Raw metadata maps.
- Object-store credentials.

Audit fields may include logical identities (`subject`, `role`, `worker_id`) and run/checkpoint identifiers; protect logs accordingly.

This is application-level structured audit logging only, not full SIEM integration, durable retention, or dashboard analytics.

Future work (20F-5+): durable export, security counters, dashboard views, SIEM pipelines, retention/compliance policy.
