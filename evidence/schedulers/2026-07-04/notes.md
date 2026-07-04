# Scheduler integration (Kueue) — evidence (2026-07-04)

**Verdict: PASS.** Upgrades the Kueue row from "rendered to contract" to
"a **real Kueue controller** admits and gates the operator's rendered Jobs by
quota."

## What ran

1. Installed **Kueue v0.8.1** into the `kind-dtr-e2e` cluster. (Worked around the
   upstream `gcr.io/kubebuilder/kube-rbac-proxy:v0.8.0` image-not-found issue by
   dropping that metrics-only sidecar so the controller pod goes Ready.)
2. `ClusterQueue` with **nominalQuota cpu=1** + a `LocalQueue`.
3. Two Jobs **rendered exactly as the operator's `KueueBackend`**: label
   `kueue.x-k8s.io/queue-name` + `suspend: true` + a 1-CPU request.

## Result

- **job-b** → Workload `Admitted=True`, Job unsuspended and running; **job-a** →
  Workload not admitted, Job stays `suspend: true` (the 1-CPU quota is exhausted).
- Deleted job-b (frees the CPU) → Kueue **admitted job-a** (unsuspended, active).

Real quota-driven admission control, end to end. See `result.json` and
`kueue-workloads-final.yaml`.

## Volcano

Volcano gang-scheduling is only meaningful on a **multi-node** cluster (a
`minMember` gang is trivially satisfiable on one node). It is covered by the
multi-node harness (`tools/evidence/multinode/`) / the Tier-2 Linode campaign.

## Reproduce

```bash
kubectl apply --server-side -f https://github.com/kubernetes-sigs/kueue/releases/download/v0.8.1/manifests.yaml
# (drop the broken kube-rbac-proxy sidecar so the pod is Ready)
kubectl create ns sched-demo
kubectl apply -f tools/evidence/schedulers/kueue/queues.yaml
kubectl apply -f tools/evidence/schedulers/kueue/jobs.yaml
kubectl -n sched-demo get jobs,workloads
```
