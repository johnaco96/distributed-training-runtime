# Kubernetes operator

`runtime-operator` (Rust, kube-rs) reconciles `TrainingRun` custom resources
into running distributed training workloads:

- optionally a **managed coordinator** (Deployment + Service) per run, or an
  external coordinator by address;
- an **Indexed Job** of worker pods — one launcher
  (`python -m dtruntime.launch`) per pod, `nprocPerNode` training processes
  each — with GPU resources, placement constraints, and scheduler
  integration from the spec;
- **status projection**: `kubectl get trainingruns` shows
  Pending → Running → Succeeded/Failed derived from the worker Job.

## Install

```bash
kubectl apply -f k8s/operator/crd-trainingrun.yaml
kubectl apply -f k8s/operator/operator.yaml   # namespace, RBAC, Deployment
```

Build the operator image with `docker build -f Dockerfile.operator -t <tag> .`
and point the Deployment at it. During development you can skip the in-cluster
Deployment entirely and run the operator locally against your kubeconfig:

```bash
cargo run -p runtime-operator
```

## Submitting a run

```yaml
apiVersion: dtruntime.io/v1alpha1
kind: TrainingRun
metadata:
  name: mnist-ddp
spec:
  image: ghcr.io/example/trainer:latest   # dtruntime + your train.py
  script: ["train.py", "--epochs", "10"]
  nodes: 2
  nprocPerNode: 2
  coordinator:
    manage:
      image: ghcr.io/example/runtime-coordinator:latest
  backend: nccl
  resources:
    limits:
      nvidia.com/gpu: "2"
```

See [k8s/operator/example-trainingrun.yaml](../k8s/operator/example-trainingrun.yaml)
for the full surface (elastic bounds, Volcano/Kueue, node selectors, spread
constraints) and [docs/schedulers.md](schedulers.md) for scheduler semantics.

How the pieces connect:

- Every worker pod gets a stable node identity
  (`<run>-node-$(JOB_COMPLETION_INDEX)`) and advertises its pod IP
  (`--worker-host`, downward API) so peers can reach its c10d TCPStore.
- The launcher registers the run idempotently, so all pods converge on one
  run; the coordinator assigns global ranks through the rendezvous — no rank
  arithmetic in manifests.
- Worker pods run with `restartPolicy: Never`; in-pod process restarts are
  the launcher's job (`maxRestarts`), pod-level failures count against the
  Job `backoffLimit` (`spec.backoffLimit`, default 6). Replacement pods
  re-join the rendezvous and training recovers by rank replacement
  ([docs/torch-distributed.md](torch-distributed.md)).
- Deleting the TrainingRun cascades to everything it created
  (ownerReferences).

## Status

```
$ kubectl get trun
NAME        PHASE     NODES   AGE
mnist-ddp   Running   2       3m
```

`status.message` carries detail (active/completed node counts, Job failure
reasons, or spec-validation errors for specs the operator rejected).

## Validation

- Rendering, scheduler adaptation, and status projection are unit-tested
  without a cluster (`cargo test -p runtime-operator`).
- The end-to-end path (CRD + operator + managed coordinator + 2-node gloo
  DDP inside kind) is validated by
  [scripts/kind_operator_e2e.ps1](../scripts/kind_operator_e2e.ps1); see
  [status-and-limitations.md](status-and-limitations.md) for what remains
  unproven (real GPU clusters, NCCL fabric).
