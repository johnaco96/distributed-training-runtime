# Product Direction (Phase 20A)

## A. Executive summary

Distributed Training Runtime is now a **serious local-first ML runtime prototype** with meaningful coordinator, checkpoint, and ML example foundations.

Phase 20 is about turning those existing runtime pieces into a coherent product direction instead of adding disconnected features. Future capability claims should be benchmark-backed and scoped conservatively.

Kubernetes deployment, HA/replication, authentication/authorization, and dashboard UX are important, but they should follow a working **coordinator-driven ML training path** so the project proves runtime value before deployment polish.

Selected Phase 20 path:

1. **Phase 20A** — Product direction doc.
2. **Phase 20B** — Coordinator-driven PyTorch orchestration.
3. **Phase 20C** — Benchmarks: standalone vs coordinator-driven training.
4. **Phase 20D** — Package as ML infra project.
5. **Phase 20E** — Kubernetes deployment.
6. **Phase 20F** — Authentication / dashboard / HA / production S3.

## B. Current foundation

The repository already includes a substantial local-first baseline:

- Rust coordinator gRPC control plane.
- Python SDK.
- Dataset registration and deterministic shard assignment.
- Worker registration, heartbeat, leases, and liveness behavior.
- Checkpoint lifecycle, metadata, integrity validation, latest restore pointer, and cleanup helper paths.
- Storage abstraction with local filesystem and optional S3/MinIO-compatible paths.
- Benchmark harness for control-plane and storage-adjacent benchmark flows.
- Observability foundation (structured logs + in-process metrics).
- PyTorch single-worker example with checkpoint save/restore.
- PyTorch simulated distributed-style example.
- Documentation foundations across quickstart, architecture, runbooks, examples, troubleshooting, and limitations.

## C. Why not Kubernetes first?

Kubernetes is valuable for packaging and reproducible deployment, but adding Kubernetes first would mostly prove deployability—not ML runtime semantics.

The core product proof should be a PyTorch training loop that actually uses the runtime end-to-end:

- register run
- register logical workers
- register dataset
- use coordinator shard assignment
- train on assigned shards
- save checkpoint artifacts
- report checkpoint metadata
- commit checkpoint
- restore latest checkpoint through coordinator

After that orchestration path exists and is benchmarked, Kubernetes becomes a force multiplier that deploys and scales a real product behavior.

This sequencing improves credibility and makes benchmark interpretation more honest and actionable.

## D. Candidate product directions

### 1) Coordinator-driven PyTorch orchestration
- **What it means:** Implement an ML example that uses coordinator APIs directly during training/recovery flow.
- **Why it matters:** Converts runtime primitives into a coherent ML control-plane story.
- **What would need to be implemented:** Coordinator-integrated run/worker/dataset/shard/checkpoint flow in Python example code.
- **Dependencies/prerequisites:** Existing SDK + checkpoint flows (already present).
- **Risks:** Added orchestration overhead; poor API ergonomics if integration surface is awkward.
- **Benchmark/validation evidence required:** Successful fresh/resume flows; latency/overhead measurements versus standalone training.
- **Priority:** **High**.
- **Recommended phase placement:** **Phase 20B**.

### 2) Benchmark expansion: standalone vs coordinator-driven
- **What it means:** Compare local standalone PyTorch example against coordinator-driven version.
- **Why it matters:** Quantifies overhead and validates product claims.
- **What would need to be implemented:** Controlled benchmark scenarios, measurement output format, repeatable runbook.
- **Dependencies/prerequisites:** Coordinator-driven PyTorch path available first.
- **Risks:** Misleading numbers without environment disclosure or consistent workloads.
- **Benchmark/validation evidence required:** Runtime/latency/checkpoint/restore/failure-retry comparisons with caveats.
- **Priority:** **High**.
- **Recommended phase placement:** **Phase 20C**.

### 3) Packaging/release polish as ML infra project
- **What it means:** Present project as a coherent ML infra prototype with clear onboarding and demo/release artifacts.
- **Why it matters:** Improves reviewer adoption and reduces ambiguity about scope.
- **What would need to be implemented:** README/product story refresh, docs coherence pass, release/demo checklist.
- **Dependencies/prerequisites:** Coordinator-driven path + benchmark narrative.
- **Risks:** Overstating readiness if packaging gets ahead of evidence.
- **Benchmark/validation evidence required:** Include measured benchmark summary and reproducible command references.
- **Priority:** **High**.
- **Recommended phase placement:** **Phase 20D**.

### 4) Kubernetes deployment
- **What it means:** Containerized coordinator/worker example plus local cluster deployment docs/manifests.
- **Why it matters:** Improves operational realism and dev-environment reproducibility.
- **What would need to be implemented:** Images, manifests/Helm, kind/minikube runbooks, deployment validation.
- **Dependencies/prerequisites:** Real coordinator-driven ML orchestration behavior.
- **Risks:** Shipping deployment scaffolding for an unproven runtime story.
- **Benchmark/validation evidence required:** Kubernetes-specific benchmark/operability validation separate from local baseline.
- **Priority:** **Medium**.
- **Recommended phase placement:** **Phase 20E**.

### 5) Authentication / authorization
- **What it means:** Add identity and access controls for coordinator/API usage.
- **Why it matters:** Mandatory before serious multi-user or remote deployment.
- **What would need to be implemented:** Auth model, token/session plumbing, policy boundaries, operational key handling.
- **Dependencies/prerequisites:** Stable API semantics and remote deployment posture.
- **Risks:** Security theater if semantics or threat model are unclear.
- **Benchmark/validation evidence required:** Access-control tests, failure-path tests, and measured auth overhead.
- **Priority:** **Medium**.
- **Recommended phase placement:** **Phase 20F** (early in that phase for remote usage).

### 6) Dashboard / UI
- **What it means:** Visual monitoring/inspection interface for runs, workers, checkpoints, and dataset/shard state.
- **Why it matters:** Improves usability and demo quality.
- **What would need to be implemented:** API surface for UI queries, frontend shell, state views, pagination/filtering.
- **Dependencies/prerequisites:** Stable metrics/event/export surfaces and product semantics.
- **Risks:** UI churn if backend semantics are still moving.
- **Benchmark/validation evidence required:** Functional demos plus latency/usability checks on core views.
- **Priority:** **Low (later/dependent)**.
- **Recommended phase placement:** **Phase 20F** (after core semantics stabilize).

### 7) Coordinator replication / HA
- **What it means:** Add failover-ready coordinator state replication and high-availability model.
- **Why it matters:** Required for resilient production-style control-plane behavior.
- **What would need to be implemented:** Replication model, leader election/consensus strategy, recovery semantics.
- **Dependencies/prerequisites:** Stable coordinator semantics and checkpoint/run consistency contracts.
- **Risks:** High complexity; correctness bugs can invalidate recovery guarantees.
- **Benchmark/validation evidence required:** Fault-injection and failover recovery benchmarks.
- **Priority:** **Low (later/dependent)**.
- **Recommended phase placement:** **Phase 20F**.

### 8) Cache-aware dataset placement
- **What it means:** Prefer worker-shard assignment that improves cache locality.
- **Why it matters:** Potential throughput/latency gains for repeated epochs.
- **What would need to be implemented:** Cache metadata signals and assignment policy updates.
- **Dependencies/prerequisites:** Coordinator-driven orchestration and baseline shard benchmarks.
- **Risks:** Policy complexity and uneven fairness across workers.
- **Benchmark/validation evidence required:** Before/after throughput and epoch runtime comparison on repeated passes.
- **Priority:** **Medium**.
- **Recommended phase placement:** Post-20C/20D optimization track (likely 20F sub-track).

### 9) Advanced prefetching
- **What it means:** Pipeline data/checkpoint fetch ahead of immediate consumption.
- **Why it matters:** Can reduce idle time in training loops.
- **What would need to be implemented:** Prefetch scheduler, bounded queues, backpressure controls.
- **Dependencies/prerequisites:** Coordinator-driven training path and baseline IO benchmarks.
- **Risks:** Memory pressure and unstable performance under variable workloads.
- **Benchmark/validation evidence required:** End-to-end runtime and stall-time comparisons with controlled configs.
- **Priority:** **Medium**.
- **Recommended phase placement:** Optimization follow-on after baseline benchmark evidence.

### 10) Production S3 tuning
- **What it means:** Improve cloud-object-store behavior for checkpoint and data artifacts.
- **Why it matters:** Storage path performance/reliability is critical in realistic ML workflows.
- **What would need to be implemented:** Multipart/concurrency tuning, retry/backoff hardening, operational defaults.
- **Dependencies/prerequisites:** Real coordinator-driven checkpoint path exercised at meaningful volume.
- **Risks:** Environment-specific tuning may not generalize; cost/performance tradeoffs.
- **Benchmark/validation evidence required:** S3/MinIO performance/retry measurements with explicit environment details.
- **Priority:** **Medium**.
- **Recommended phase placement:** **Phase 20F** after core semantics are validated.

## E. Selected Phase 20 path

### Phase 20A — Product direction doc
- This document.
- No runtime behavior changes.

### Phase 20B — Coordinator-driven PyTorch orchestration
- Add an example that uses Python SDK/coordinator in the training flow.
- Register run/workers/dataset.
- Use coordinator shard assignment.
- Save/report/commit checkpoint metadata.
- Restore latest checkpoint through coordinator path.

### Phase 20C — Benchmarks: standalone vs coordinator-driven training
- Compare current standalone PyTorch example vs coordinator-driven example.
- Measure overhead honestly.
- Report latency, runtime, checkpoint time, restore time, and failure/retry behavior.

### Phase 20D — Package as ML infra project
- Polish README/product story.
- Optionally add a clearer example project layout.
- Ensure docs tell one coherent reviewer-facing story.
- Add release/demo checklist.

### Phase 20E — Kubernetes deployment
- Containerize coordinator and worker example.
- Add local kind/minikube deployment docs.
- Add manifests/Helm only once product behavior exists.
- Keep this local/dev-first and avoid production claims.

### Phase 20F — Authentication / dashboard / HA / production S3
- Choose sub-track sequence based on benchmark evidence and product needs.
- Authentication before remote multi-user deployment.
- Dashboard after metrics/export surfaces stabilize.
- HA/replication after coordinator semantics stabilize.
- Production S3 tuning after real ML checkpoint paths are exercised at scale.

## F. Honest benchmark plan

Phase 20C should capture at minimum:

- End-to-end training runtime.
- Coordinator RPC count per epoch.
- Coordinator RPC latency p50/p90/p99.
- Dataset registration latency.
- Shard assignment latency.
- Checkpoint write throughput.
- Checkpoint commit latency.
- Restore latest latency.
- Standalone vs coordinator-driven overhead percentage.
- Failure rate.
- Retry count.
- Optional S3 throughput.
- Worker scaling smoke for 1, 2, 4, 8, 16 local workers.
- Simulated 32/64/128 logical-worker runs with caveats.

Explicit reporting constraints:

- Do not claim hundreds/thousands of real workers without measured distributed tests.
- Simulated worker tests are not proof of real cluster scale.
- Local benchmark numbers are environment-specific.
- Kubernetes benchmarks should be reported separately from local benchmarks.

## G. Product positioning

Current positioning:

- **“A local-first ML runtime control-plane prototype.”**
- **“A Rust/Python runtime for worker coordination, dataset shard assignment, checkpoint metadata, and training recovery experiments.”**
- **“A serious ML infrastructure research/prototype project.”**

Do **not** position it yet as:

- A production replacement for Ray, Kubeflow, TorchElastic, or Kubernetes-native training operators.
- A production distributed training scheduler.
- A production fault-tolerant cloud ML platform.

## H. What would make this compelling to reviewers

- Coordinator-driven PyTorch example.
- Clear benchmark comparison against standalone path.
- Honest limitations and conservative claims.
- Reproducible runbooks.
- Clean architecture docs.
- Small but real checkpoint/recovery story.
- Kubernetes follow-up path that deploys something real.

## I. Phase 20A acceptance criteria

- `docs/product-direction.md` exists.
- `README.md` links to this document.
- `docs/status-and-limitations.md` links to this document.
- `docs/architecture.md` may link to this document for forward architecture evolution.
- No runtime behavior changed in Phase 20A.
- Product path is explicitly selected.
- Claims remain conservative.


## Phase 20B status

Phase 20B is now implemented with a coordinator-driven PyTorch orchestration example (`python/examples/pytorch_coordinator_train.py`) that bridges local training loops to runtime coordinator APIs in a deterministic, local-first, single-process flow.


## Phase 20C: Local PyTorch orchestration benchmark

Use `python/examples/benchmark_pytorch_orchestration.py` to compare local standalone training (`pytorch_train.py`) versus local coordinator-driven training (`pytorch_coordinator_train.py`). It records wall-clock timings, overhead (seconds and percent), final loss/accuracy, checkpoint/resume status, and coordinator RPC summary when available. Results are emitted as JSON and CSV for reproducibility and automation.

Example:

`PYTHONPATH=python python python/examples/benchmark_pytorch_orchestration.py --runs 3 --epochs 2 --samples 128 --batch-size 16 --workers 2 --shards 4 --out-dir .dtruntime-pytorch-bench`

Smoke:

`PYTHONPATH=python python python/examples/benchmark_pytorch_orchestration.py --runs 1 --epochs 1 --samples 32 --batch-size 8 --workers 2 --shards 4 --out-dir .dtruntime-pytorch-bench-smoke`

Caveats: this is local-only and environment-specific. Coordinator timings include coordinator startup in the current implementation (no warmed/external coordinator benchmark mode yet). These numbers do **not** prove distributed multi-node scaling, Kubernetes performance, or production throughput.


## Phase 20D status

Phase 20D is now complete: packaging/documentation polish aligns README, runbooks, benchmark framing, and demo/release checklists so the repository presents a coherent ML infrastructure prototype story before starting Kubernetes work.

Phase 20E (Kubernetes local/dev deployment) is now complete. Phase 20F (auth/dashboard/HA/production S3) remains pending by design.
