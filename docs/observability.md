# Observability (Step 1: Prometheus/Grafana)

Start coordinator metrics endpoint:

```bash
cargo run -p runtime-coordinator -- --addr 127.0.0.1:50051 --metrics-addr 127.0.0.1:9090
curl http://127.0.0.1:9090/metrics
```

Prometheus config: `observability/prometheus/prometheus.yml`.
Grafana dashboard JSON: `observability/grafana/dtruntime-dashboard.json`.

Metrics are aggregate-only (no run_id/worker_id/dataset_id/checkpoint_id labels) for production-safety and low-cardinality.
No secrets, auth metadata values, cert/key contents, or object-store credentials are exported in metrics.

This completes Step 1 of the final production-readiness roadmap and replaces the need for a custom production dashboard UI.

## Step 2: Coordinator health probes

The coordinator metrics HTTP listener (enabled with `--metrics-addr`) now also serves:
- `GET /healthz` for liveness (returns `200 OK` when process/responding).
- `GET /readyz` for readiness (returns `200 OK` when coordinator is ready for gRPC traffic, otherwise `503`).

Probe responses are low-cost plain text and do not include secrets or workload identifiers.

## Step 4 metrics collection note

`run_k8s_secure_validation.ps1` always captures raw coordinator `/metrics` into `metrics.prom` for pass/fail evidence. Prometheus API queries are optional and should not gate baseline secure validation success.

## Alerting, SLOs, and runbooks

- **Recording rules (SLIs)**: `observability/prometheus/recording.rules.yml`.
- **Alerting rules**: `observability/prometheus/alerts.rules.yml` — availability
  (incl. multiwindow SLO burn), checkpoint durability, worker-fleet health, and
  security (auth-denial rate). Each alert links to its runbook.
- **Prometheus Operator**: `k8s/observability/prometheus-rules.yaml`
  (`PrometheusRule`).
- **SLOs & error budgets**: [slos.md](slos.md).
- **On-call response**: [runbooks-oncall.md](runbooks-oncall.md).
- **Resilience tests** (failover / chaos / soak) and measured benchmark
  results: [observability-ops-tests.md](observability-ops-tests.md),
  [benchmarks.md](benchmarks.md).

Validate the rules locally with promtool:

```bash
docker run --rm -v "$PWD/observability/prometheus:/rules" --entrypoint promtool \
  prom/prometheus:latest check rules /rules/recording.rules.yml /rules/alerts.rules.yml
```
