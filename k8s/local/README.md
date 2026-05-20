# Local Kubernetes Manifests (Phase 20E)

These manifests are intentionally local/dev focused.

Apply all manifests:

```bash
kubectl apply -f k8s/local/
```

Included resources:
- `namespace.yaml`: `dtruntime` namespace.
- `coordinator-deployment.yaml`: single-replica coordinator Deployment.
- `coordinator-service.yaml`: ClusterIP service exposing gRPC on 50051.
- `pytorch-coordinator-job.yaml`: one-shot demo training Job that uses the coordinator service.


Security planning references:
- `docs/security-architecture.md`
- `docs/threat-model.md`
- `docs/production-readiness.md`

`auth-secret.example.yaml` is a placeholder-only local/dev example and is not required by current manifests.

## Security notes

- Local demo defaults to plaintext/no-auth unless explicitly configured.
- TLS/auth secret examples are placeholders only; do not commit real credentials or key material.
- No production exposure is configured by default (no Ingress/LoadBalancer).

