# SIEM ingestion — evidence (2026-07-04)

**Verdict: PASS.** The coordinator's NDJSON audit stream is ingested end-to-end
into a real log store (Grafana Loki) via a Vector log shipper, and is queryable.
This upgrades the SIEM row from "NDJSON sink implemented, not run vs a live SIEM"
to "proven against a real log pipeline."

## What ran

1. `docker compose up -d` in `tools/evidence/siem/` → **Loki 3.0.0** + **Vector 0.39.0**.
2. Coordinator started with `--auth-mode metadata --audit-log-file .../tools/evidence/siem/audit/audit.ndjson`.
3. Generated events: an admin `register_run` + `runs list` (allow), and a
   `viewer` attempting `run cancel`/`pause` (deny).
4. Vector tails the file, parses each JSON line, ships to Loki labeled `job=dtruntime-audit`.
5. Queried Loki: `{job="dtruntime-audit"}`.

## Result

**17/17** emitted audit records ingested and queryable, including
`AuthorizationDenied` ×3 and `AuthorizationAllowed` ×2. See `result.json` and the
raw `loki-query-response.json`.

## Reproduce

```bash
cd tools/evidence/siem && docker compose up -d
# start a coordinator with --audit-log-file $PWD/audit/audit.ndjson and generate events
curl -sG http://localhost:3100/loki/api/v1/query_range \
  --data-urlencode 'query={job="dtruntime-audit"}' \
  --data-urlencode "start=$(( ($(date +%s)-3600)*1000000000 ))" \
  --data-urlencode "end=$(( $(date +%s)*1000000000 ))"
docker compose down
```

Swap Loki for Elasticsearch/Splunk by changing the Vector `sink` — the coordinator
side is unchanged (see `docs/audit-logging.md`).
