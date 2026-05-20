#![forbid(unsafe_code)]

#[cfg(feature = "s3")]
mod tests {
    use runtime_checkpoint::{
        upload_checkpoint_artifacts_concurrently, CheckpointArtifactUpload, CheckpointId,
        CheckpointRecord, CheckpointStore, ChecksumAlgorithm, RunId, WorkerId,
    };
    use runtime_storage::{S3Storage, S3StorageConfig, StorageKey, StorageOperationPolicy};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn env_optional(name: &str) -> Option<String> {
        std::env::var(name).ok().filter(|v| !v.trim().is_empty())
    }

    fn env_required_or_skip(name: &str) -> Option<String> {
        match env_optional(name) {
            Some(v) => Some(v),
            None => {
                eprintln!("skipping MinIO test; missing required env var: {name}");
                None
            }
        }
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

        let configured_prefix = env_optional("DTRUNTIME_S3_KEY_PREFIX");
        let key_prefix = Some(match configured_prefix {
            Some(prefix) if !prefix.trim().is_empty() => {
                format!("{}/test-runs/{}", prefix.trim_matches('/'), unique_suffix())
            }
            _ => format!("test-runs/{}", unique_suffix()),
        });

        let Some(endpoint_url) = env_required_or_skip("DTRUNTIME_S3_ENDPOINT_URL") else {
            return;
        };
        let Some(region) = env_required_or_skip("DTRUNTIME_S3_REGION") else {
            return;
        };
        let Some(bucket) = env_required_or_skip("DTRUNTIME_S3_BUCKET") else {
            return;
        };
        let Some(access_key_id) = env_required_or_skip("DTRUNTIME_S3_ACCESS_KEY_ID") else {
            return;
        };
        let Some(secret_access_key) = env_required_or_skip("DTRUNTIME_S3_SECRET_ACCESS_KEY") else {
            return;
        };

        let storage = match S3Storage::new(S3StorageConfig {
            endpoint_url: Some(endpoint_url),
            region,
            bucket,
            access_key_id,
            secret_access_key,
            force_path_style: std::env::var("DTRUNTIME_S3_FORCE_PATH_STYLE")
                .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
                .unwrap_or(true),
            key_prefix,
        }) {
            Ok(storage) => storage,
            Err(err) => {
                eprintln!("skipping MinIO test; unable to create S3 storage client: {err}");
                return;
            }
        };

        let store = CheckpointStore::new(storage.clone());
        let run_id = RunId("run-minio".into());
        let checkpoint_id = CheckpointId("ckpt-minio".into());
        let worker_id = WorkerId("worker-0".into());
        let mut record = CheckpointRecord::new(run_id.clone(), checkpoint_id.clone(), 42, 1000);

        store
            .write_pending_manifest(&record)
            .expect("write pending manifest");
        let artifacts = match upload_checkpoint_artifacts_concurrently(
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
        ) {
            Ok(v) => v,
            Err(err) => {
                eprintln!("skipping MinIO test; artifact upload failed (MinIO unavailable/misconfigured): {err}");
                return;
            }
        };
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

        let cleanup_result = store.cleanup_checkpoints(&run_id, 1, true);
        assert!(
            cleanup_result.is_ok(),
            "cleanup should complete for MinIO-backed run"
        );
    }
}
