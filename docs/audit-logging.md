# Audit Logging

The coordinator emits structured audit events for startup, authn/authz decisions, and key state-changing RPCs.

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

Future work: durable export, security counters, dashboard views, SIEM pipelines, retention/compliance policy.
