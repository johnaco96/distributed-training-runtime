# E2E Training Benchmark (Kubernetes + MinIO/S3)

Use `scripts/run_e2e_training_benchmark_k8s.sh` to run the full benchmark.

## MinIO for local benchmark

- Apply `k8s/benchmark/minio.yaml` for local S3-compatible storage.
- Dev credentials are stored in `dtruntime-minio-credentials` Secret.
- Replace with production AWS S3 by setting:
  - `DTRUNTIME_STORAGE_BACKEND=s3`
  - `DTRUNTIME_S3_ENDPOINT` (omit for AWS S3)
  - `DTRUNTIME_S3_BUCKET`
  - `DTRUNTIME_S3_REGION`
  - `DTRUNTIME_S3_ACCESS_KEY_ID`
  - `DTRUNTIME_S3_SECRET_ACCESS_KEY`
  - `DTRUNTIME_S3_FORCE_PATH_STYLE=true` for MinIO

Never commit real credentials.
