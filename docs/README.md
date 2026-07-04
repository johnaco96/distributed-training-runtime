# Documentation index

Start here. Docs are grouped by what you're trying to do.

## Get started

- [Quickstart](quickstart.md) — run a coordinator and a first job locally.
- [Local demo](local-demo.md) · [Examples](examples.md)
- [Architecture overview](architecture.md)

## Integrate your training

- [Integrate an existing PyTorch script](pytorch-integration.md) — the essential guide.
- [torch.distributed execution & rendezvous](torch-distributed.md)
- [Distributed checkpointing](checkpointing-distributed.md) · [Checkpoint protocol](checkpoint-protocol.md) · [Cleanup/GC](checkpoint-cleanup.md)
- [Data pipeline](data-pipeline.md) · [Dataset sharding](dataset-sharding.md) · [Leases](leases.md)
- [ML integration examples](ml-integration.md)

## Deploy to production

- [Production deployment](production-deployment.md) — Helm + raw manifests, HA, hardening.
- [Release process & image signing](RELEASING.md)
- [Kubernetes operator](kubernetes-operator.md) · [Local Kubernetes](kubernetes-local.md)
- [Scheduler integrations](schedulers.md) · [High availability](high-availability.md)
- [Metadata persistence](metadata-persistence.md) · [Object storage](object-storage.md) · [S3 hardening](storage-hardening.md) · [Schema migrations](schema-migrations.md)

## Operate

- [Failure recovery & operator actions](failure-recovery-operations.md) — what recovers automatically and what you can do.
- [Admin & operator CLI](admin-cli.md) · [Run lifecycle](run-lifecycle.md) · [Failure recovery mechanics](failure-recovery.md)
- [On-call runbooks](runbooks-oncall.md) · [SLOs & error budgets](slos.md) · [Observability](observability.md)
- [Ops resilience tests (failover/chaos/soak)](observability-ops-tests.md) · [Troubleshooting](troubleshooting.md)

## Secure

- [Authentication & authorization](security-auth.md) · [Security architecture](security-architecture.md)
- [Secret management & rotation](secret-rotation.md) · [Audit logging](audit-logging.md) · [Threat model](threat-model.md)

## Reference

- [**Support matrix**](support-matrix.md) — frameworks, storage, schedulers, platforms, limits.
- [Status & limitations](status-and-limitations.md) · [Protocol compatibility](protocol-compatibility.md)
- [Benchmarks](benchmarks.md) · [Retry & idempotency](retry-idempotency.md)
