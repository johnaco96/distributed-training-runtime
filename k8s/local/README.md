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
