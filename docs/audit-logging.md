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

Audit records also carry the authenticated **credential source** (`jwt`,
`spiffe`, or `metadata`) in the reason field of accept events, and every
authorization decision increments the `auth_allowed_total` / `auth_denied_total`
Prometheus counters.

## SIEM export (NDJSON)

Pass `--audit-log-file <PATH>` (or `DTRUNTIME_AUDIT_LOG_FILE`) to additionally
write every audit event as a newline-delimited JSON (NDJSON) record. The sink
composes with the durable DB sink — both receive every event. The schema is
stable and flat, tagged `"schema": "dtruntime.audit.v1"`, for direct ingestion
by a log shipper (Fluent Bit, Vector, Filebeat) into a SIEM (Splunk, Elastic,
Loki, Chronicle):

```json
{"schema":"dtruntime.audit.v1","occurred_at_millis":1751590000000,"kind":"AuthorizationDenied","outcome":"Rejected","subject":"alice","role":"viewer","reason":"permission_denied","request":"cancel_run"}
```

Point the file at a shared volume and let your agent tail it; rotate it with the
platform's log-rotation tooling (the sink re-opens on the next process start).

This remains application-level structured audit logging; durable retention and
compliance policy are handled by the downstream SIEM.

Future work: dashboard analytics views and a native push/webhook forwarder.
