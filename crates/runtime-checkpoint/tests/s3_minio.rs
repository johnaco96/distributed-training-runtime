#![forbid(unsafe_code)]

#[cfg(feature = "s3")]
mod tests {
    use runtime_checkpoint::{
        upload_checkpoint_artifacts_concurrently, CheckpointArtifactUpload,
        CheckpointCleanupPolicy, CheckpointId, CheckpointRecord, CheckpointStore,
        ChecksumAlgorithm, RunId, WorkerId,
    };
    use runtime_storage::{
        S3Storage, S3StorageConfig, StorageBackend, StorageKey, StorageOperationPolicy,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    fn env_optional(name: &str) -> Option<String> {
        std::env::var(name).ok().filter(|v| !v.trim().is_empty())
    }
    fn unique_suffix() -> String {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
            .to_string()
    }

    fn maybe_storage() -> Option<S3Storage> {
        if std::env::var("DTRUNTIME_RUN_MINIO_TESTS").as_deref() != Ok("1") {
            eprintln!("skipping MinIO test; set DTRUNTIME_RUN_MINIO_TESTS=1");
            return None;
        }
        let endpoint_url = env_optional("DTRUNTIME_S3_ENDPOINT_URL")?;
        let region = env_optional("DTRUNTIME_S3_REGION")?;
        let bucket = env_optional("DTRUNTIME_S3_BUCKET")?;
        let access_key_id = env_optional("DTRUNTIME_S3_ACCESS_KEY_ID")?;
        let secret_access_key = env_optional("DTRUNTIME_S3_SECRET_ACCESS_KEY")?;
        let key_prefix = Some(format!(
            "{}/test-runs/{}",
            env_optional("DTRUNTIME_S3_KEY_PREFIX")
                .unwrap_or_else(|| "dtruntime".into())
                .trim_matches('/'),
            unique_suffix()
        ));
        S3Storage::new(S3StorageConfig {
            endpoint_url: Some(endpoint_url),
            region,
            bucket,
            access_key_id,
            secret_access_key,
            force_path_style: true,
            key_prefix,
            ..Default::default()
        })
        .ok()
    }

    fn commit_one(
        store: &CheckpointStore<S3Storage>,
        storage: &S3Storage,
        run_id: &RunId,
        checkpoint_id: &CheckpointId,
        step: u64,
    ) {
        let worker_id = WorkerId("worker-0".into());
        let mut record =
            CheckpointRecord::new(run_id.clone(), checkpoint_id.clone(), step, 1000 + step);
        store.write_pending_manifest(&record).expect("pending");
        let artifacts = upload_checkpoint_artifacts_concurrently(
            storage,
            vec![CheckpointArtifactUpload {
                worker_id: worker_id.clone(),
                artifact_name: "model.bin".into(),
                storage_key: StorageKey::new(format!(
                    "runs/{}/checkpoints/{}/workers/{}/model.bin",
                    run_id.0, checkpoint_id.0, worker_id.0
                ))
                .unwrap(),
                bytes: format!("payload-{step}").into_bytes(),
                checksum_algorithm: ChecksumAlgorithm::Sha256,
            }],
            &StorageOperationPolicy {
                max_concurrent_operations: 1,
                ..Default::default()
            },
        )
        .expect("upload");
        for a in artifacts {
            record.add_artifact(a, 2000 + step).expect("add");
        }
        store
            .commit_checkpoint(&mut record, 3000 + step)
            .expect("commit");
    }

    #[test]
    fn minio_cleanup_preserves_latest_and_latest_loads() {
        let Some(storage) = maybe_storage() else {
            return;
        };
        let store = CheckpointStore::new(storage.clone());
        let run_id = RunId("run-minio-cleanup".into());
        commit_one(&store, &storage, &run_id, &CheckpointId("ckpt-1".into()), 1);
        commit_one(&store, &storage, &run_id, &CheckpointId("ckpt-2".into()), 2);
        let before = store.load_latest_manifest(&run_id).expect("latest before");
        assert_eq!(before.checkpoint_id, "ckpt-2");

        store
            .cleanup_checkpoints(
                &run_id,
                CheckpointCleanupPolicy {
                    keep_latest: true,
                    keep_last_n_committed: 1,
                    remove_pending: true,
                    remove_orphan_artifacts: true,
                },
            )
            .expect("cleanup");
        let after = store.load_latest_manifest(&run_id).expect("latest after");
        assert_eq!(after.checkpoint_id, "ckpt-2");
    }

    #[test]
    fn minio_commit_rejects_checksum_mismatch_after_tamper() {
        let Some(storage) = maybe_storage() else {
            return;
        };
        let store = CheckpointStore::new(storage.clone());
        let run_id = RunId("run-minio-mismatch".into());
        let checkpoint_id = CheckpointId("ckpt-bad".into());
        let worker_id = WorkerId("worker-0".into());
        let mut record = CheckpointRecord::new(run_id.clone(), checkpoint_id.clone(), 1, 1);
        store.write_pending_manifest(&record).expect("pending");
        let key = StorageKey::new(format!(
            "runs/{}/checkpoints/{}/workers/{}/model.bin",
            run_id.0, checkpoint_id.0, worker_id.0
        ))
        .unwrap();
        let mut artifacts = upload_checkpoint_artifacts_concurrently(
            &storage,
            vec![CheckpointArtifactUpload {
                worker_id: worker_id.clone(),
                artifact_name: "model.bin".into(),
                storage_key: key.clone(),
                bytes: b"original".to_vec(),
                checksum_algorithm: ChecksumAlgorithm::Sha256,
            }],
            &StorageOperationPolicy::default(),
        )
        .expect("upload");
        storage.put(&key, b"tampered").expect("tamper object");
        record.add_artifact(artifacts.remove(0), 2).expect("add");
        assert!(store.commit_checkpoint(&mut record, 3).is_err());
    }
}
