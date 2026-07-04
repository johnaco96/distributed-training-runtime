# Multi-node — harness ready, execution pending (Tier-2)

**Status: harness delivered, not yet executed here.** The local evidence run
used a **single-node** kind cluster (`kind-dtr-e2e`), and this Windows /
Docker-Desktop box does not have the `kind` binary installed to spin up a fresh
multi-node cluster. Rather than produce a weak single-node "multi-node" result,
this is folded into the **Tier-2 Linode campaign** (real multi-node Linux
nodes), where it is clean and meaningful.

The harness (`tools/evidence/multinode/`) is complete and runnable:

- `kind-multinode.yaml` — a control-plane + 3 worker nodes cluster.
- `run.sh` — creates the cluster, builds/loads the coordinator + operator
  images, applies the CRD/operator and a 3-worker `TrainingRun`, and captures:
  - `nodes.txt` / `pods-by-node.txt` — workers scheduled on **distinct nodes**,
  - `trainingrun-status.txt` — the run reaching RUNNING/Succeeded (rendezvous
    formed **across nodes**),
  - checkpoint restore + data-locality assertions from worker logs / metrics.

This mirrors the **Phase 2** kind e2e that already validated cross-pod gloo DDP →
Succeeded (single-host kind). The remaining delta this proves is *separate
physical nodes* — which the Linode 2-node campaign also covers alongside NCCL
multi-node training and cross-machine checkpoint restore (one campaign closes
several ⚠️ rows at once).

## To run

On a Linux host (or the Linode cluster) with kind + docker:

```bash
bash tools/evidence/multinode/run.sh
```
