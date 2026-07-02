# Scheduler integrations

The operator separates *what* a training job is (the `TrainingRun` spec) from
*how* it is scheduled. In-cluster scheduling systems plug in as
`SchedulerBackend` implementations that adapt the rendered Kubernetes
objects; out-of-cluster systems integrate one level higher, as workload
submitters that run the launcher on nodes they allocate.

## In-cluster backends (implemented)

Select with `spec.scheduling.backend`:

### `kubernetes` (default)

Plain kube-scheduler. The worker Job is an Indexed Job with
`parallelism = completions = nodes`; each pod runs one
`python -m dtruntime.launch` instance. No adaptation.

Caveat: kube-scheduler places pods one at a time. A fixed-size run on a
saturated cluster can deadlock waiting for pods that never fit — use elastic
bounds, or gang scheduling via Volcano.

### `volcano`

Gang scheduling. The backend sets `schedulerName: volcano` on worker pods,
creates a `PodGroup` with `minMember = nodes` (and `spec.queue` when
`scheduling.queue` is set), and annotates pods into the group. All worker
pods are scheduled together or not at all, which matches fixed-size
rendezvous semantics. Requires Volcano installed in the cluster.

### `kueue`

Quota and queueing. The backend labels the worker Job with
`kueue.x-k8s.io/queue-name: <scheduling.queue>` and creates it **suspended**;
Kueue admits and unsuspends it when the LocalQueue has quota. Requires Kueue
installed and a LocalQueue in the namespace. `scheduling.queue` is mandatory.

## Resource management

- **Accelerators** — request through `spec.resources`
  (`limits: {"nvidia.com/gpu": "8"}` or any extended resource:
  `google.com/tpu`, `amd.com/gpu`, `habana.ai/gaudi`). The device plugin on
  the node advertises the resource; the operator passes it through verbatim.
- **Placement** — `spec.scheduling.nodeSelector`, `tolerations`, and
  `affinity` flow directly into the pod spec (e.g. pin to
  `cloud.google.com/gke-accelerator: nvidia-h100-80gb` nodes, tolerate
  dedicated-GPU taints).
- **Topology awareness** — `spec.scheduling.topologySpreadConstraints`
  spreads workers across fault domains, or podAffinity in
  `spec.scheduling.affinity` packs them into one zone/rack to keep NCCL
  traffic local.
- **Quotas** — namespace `ResourceQuota` bounds aggregate GPU/CPU use per
  team namespace with the default backend; Kueue provides queue-level quota
  with borrowing; Volcano queues provide weighted capacity sharing.
- **Priority/preemption** — `spec.scheduling.priorityClassName`.

## Out-of-cluster systems (extension points)

Slurm, Ray, SageMaker, and AWS Batch do not schedule Kubernetes pods, so
they are not `SchedulerBackend`s. They integrate as **submitters**: anything
that (1) allocates N nodes, (2) runs
`python -m dtruntime.launch --coordinator <addr> --run-name <name> ...` on
each, and (3) tears the allocation down when the run finishes. The launcher
handles registration, rendezvous, rank assignment, and supervision
identically in every environment — no Kubernetes required.

Contract per node:

```
python -m dtruntime.launch \
    --coordinator <reachable coordinator addr> \
    --run-name <shared name>            # idempotent across nodes \
    --nproc-per-node <procs> \
    --world-size <total>                # or --min-workers/--max-workers \
    --node-id <stable node identity> \
    --worker-host <address other nodes can reach this node on> \
    train.py [args]
```

| System | Sketch |
|---|---|
| **Slurm** | `sbatch --nodes=N` script whose step is `srun python -m dtruntime.launch ... --node-id "$SLURMD_NODENAME" --worker-host "$(hostname -I \| cut -d' ' -f1)"`. The coordinator runs as a service reachable from compute nodes. |
| **Ray** | A Ray job starts N actors (one per node via `ScalingConfig`/placement groups); each actor `subprocess.run`s the launcher or calls `dtruntime.torch_dist.init()` directly in-process. |
| **SageMaker** | Custom training container whose entrypoint execs the launcher; `--node-id` from `SM_CURRENT_HOST`, world size from `SM_HOSTS`; the coordinator address arrives as a hyperparameter/env. |
| **AWS Batch** | Multi-node parallel job; the container command is the launcher with `--node-id "$AWS_BATCH_JOB_NODE_INDEX"`. |

A first-class `WorkloadSubmitter` abstraction (submit/describe/cancel against
these systems from the CLI/operator) is planned alongside the admin CLI —
the launcher contract above is stable and is what such submitters build on.

## Adding a new in-cluster backend

Implement `SchedulerBackend` in
[crates/runtime-operator/src/scheduler.rs](../crates/runtime-operator/src/scheduler.rs):

```rust
pub trait SchedulerBackend: Send + Sync {
    fn name(&self) -> &'static str;
    fn adapt(&self, workload: &mut RenderedWorkload, tr: &TrainingRun)
        -> Result<(), SchedulerError>;
}
```

`adapt` may mutate the rendered Job/Deployment (labels, schedulerName,
suspend) and append scheduler-specific CRs to `extra_objects` (applied as
dynamic objects, so no compiled types are needed). Register the backend in
`backend_for`, add it to `KNOWN_BACKENDS`, and grant the operator RBAC for
any new resource types in `k8s/operator/operator.yaml`.
