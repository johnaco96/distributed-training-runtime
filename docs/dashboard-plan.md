# Dashboard Plan (Phase 20F-1)

## Current dashboard status

- Dashboard is not implemented.

## Current observability foundation

- Structured logs are available.
- In-process runtime metrics exist.
- Benchmark outputs exist as JSON/CSV artifacts.

## Future dashboard views

Planned read-oriented views include:
- Active runs.
- Worker status (active/stale/failed).
- Checkpoint lifecycle state.
- Latest checkpoint metadata.
- Shard assignment overview.
- Benchmark summaries.
- RPC error/security event summaries.

## Security requirements

- Authentication required.
- No public default exposure.
- No secrets displayed.
- Role separation to be added in later phases.

## Implementation path

1. Metrics endpoint/exporter hardening.
2. Local read-only dashboard prototype.
3. Auth integration.
4. Prometheus/Grafana integration later if needed.

## Non-claims

This document does not claim a dashboard implementation exists in the current codebase.
