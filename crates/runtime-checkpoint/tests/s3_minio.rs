#![forbid(unsafe_code)]

#[cfg(feature = "s3")]
mod tests {
    use runtime_checkpoint::{
        upload_checkpoint_artifacts_concurrently, CheckpointArtifactUpload, CheckpointId,
        CheckpointRecord, CheckpointStore, ChecksumAlgorithm, RunId, WorkerId,
    };
    use runtime_storage::{S3Storage, S3StorageConfig, StorageKey, StorageOperationPolicy};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn env_required(name: &str) -> String {
        std::env::var(name).unwrap_or_else(|_| panic!("missing required env var: {name}"))
    }

    fn unique_suffix() -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time monotonic")
            .as_nanos();
        format!("{nanos}")
    }

    #[test]
    fn minio_checkpoint_roundtrip_env_gated() {
        if std::env::var("DTRUNTIME_RUN_MINIO_TESTS").as_deref() != Ok("1") {
            eprintln!("skipping MinIO test; set DTRUNTIME_RUN_MINIO_TESTS=1 to enable");
            return;
        }

        let configured_prefix = std::env::var("DTRUNTIME_S3_KEY_PREFIX").ok();
        let key_prefix = Some(match configured_prefix {
            Some(prefix) if !prefix.trim().is_empty() => {
                format!("{}/test-runs/{}", prefix.trim_matches('/'), unique_suffix())
            }
            _ => format!("test-runs/{}", unique_suffix()),
        });

        let storage = S3Storage::new(S3StorageConfig {
            endpoint_url: Some(env_required("DTRUNTIME_S3_ENDPOINT_URL")),
            region: env_required("DTRUNTIME_S3_REGION"),
            bucket: env_required("DTRUNTIME_S3_BUCKET"),
            access_key_id: env_required("DTRUNTIME_S3_ACCESS_KEY_ID"),
            secret_access_key: env_required("DTRUNTIME_S3_SECRET_ACCESS_KEY"),
            force_path_style: std::env::var("DTRUNTIME_S3_FORCE_PATH_STYLE")
                .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
                .unwrap_or(true),
            key_prefix,
        })
        .expect("create s3 storage");

        let store = CheckpointStore::new(storage.clone());
        let run_id = RunId("run-minio".into());
        let checkpoint_id = CheckpointId("ckpt-minio".into());
        let worker_id = WorkerId("worker-0".into());
        let mut record = CheckpointRecord::new(run_id.clone(), checkpoint_id.clone(), 42, 1000);

        store
            .write_pending_manifest(&record)
            .expect("write pending manifest");
        let artifacts = upload_checkpoint_artifacts_concurrently(
            &storage,
            vec![
                CheckpointArtifactUpload {
                    worker_id: worker_id.clone(),
                    artifact_name: "model.bin".into(),
                    storage_key: StorageKey::new(format!(
                        "runs/{}/checkpoints/{}/workers/{}/model.bin",
                        run_id.0, checkpoint_id.0, worker_id.0
                    ))
                    .unwrap(),
                    bytes: b"minio-bytes".to_vec(),
                    checksum_algorithm: ChecksumAlgorithm::Sha256,
                },
                CheckpointArtifactUpload {
                    worker_id: worker_id.clone(),
                    artifact_name: "optim.bin".into(),
                    storage_key: StorageKey::new(format!(
                        "runs/{}/checkpoints/{}/workers/{}/optim.bin",
                        run_id.0, checkpoint_id.0, worker_id.0
                    ))
                    .unwrap(),
                    bytes: b"minio-optim".to_vec(),
                    checksum_algorithm: ChecksumAlgorithm::Sha256,
                },
            ],
            &StorageOperationPolicy {
                max_concurrent_operations: 2,
                ..Default::default()
            },
        )
        .expect("upload artifacts");
        for a in artifacts.clone() {
            record.add_artifact(a, 1001).expect("add artifact");
        }
        let committed = store
            .commit_checkpoint(&mut record, 2000)
            .expect("commit checkpoint");
        let latest = store.load_latest_manifest(&run_id).expect("load latest");

        assert_eq!(latest.checkpoint_id, committed.checkpoint_id);
        assert_eq!(latest.checkpoint_id, checkpoint_id.0);
        assert_eq!(latest.artifacts.len(), 2);
    }
}
