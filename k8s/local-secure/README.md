# Optional secure local mode
Uses TLS secret placeholders and metadata auth mode for local experiments only.

## Coordinator health probes

The secure local coordinator deployment also enables `--metrics-addr 0.0.0.0:9090` and configures:
- `startupProbe` -> `GET /healthz`
- `livenessProbe` -> `GET /healthz`
- `readinessProbe` -> `GET /readyz`

Probe endpoints are plain-text health responses and do not expose secret material.
