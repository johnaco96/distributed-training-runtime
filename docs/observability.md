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
