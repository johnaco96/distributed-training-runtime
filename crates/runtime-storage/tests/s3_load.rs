#![forbid(unsafe_code)]

//! Object-store load + integrity tests against a real S3-compatible backend
//! (MinIO).  Gated behind `DTRUNTIME_RUN_MINIO_TESTS=1` plus the standard
//! `DTRUNTIME_S3_*` connection env vars, like the checkpoint MinIO tests.
//!
//! Coverage:
//! - concurrent put/get/delete under contention, verifying every object's
//!   bytes round-trip intact (integrity under load);
//! - multipart upload of an object larger than the multipart threshold,
//!   verifying it reassembles correctly;
//! - throughput is printed for visibility (not asserted — it is machine- and
//!   network-dependent).

#[cfg(feature = "s3")]
mod tests {
    use runtime_storage::{S3Storage, S3StorageConfig, StorageBackend, StorageKey};
    use std::sync::Arc;
    use std::time::{Instant, SystemTime, UNIX_EPOCH};

    fn env_opt(name: &str) -> Option<String> {
        std::env::var(name).ok().filter(|v| !v.trim().is_empty())
    }

    fn unique() -> String {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .to_string()
    }

    fn maybe_storage(part_size: Option<usize>, threshold: Option<usize>) -> Option<S3Storage> {
        if std::env::var("DTRUNTIME_RUN_MINIO_TESTS").as_deref() != Ok("1") {
            eprintln!("skipping S3 load test; set DTRUNTIME_RUN_MINIO_TESTS=1");
            return None;
        }
        let prefix = format!(
            "{}/load/{}",
            env_opt("DTRUNTIME_S3_KEY_PREFIX")
                .unwrap_or_else(|| "dtruntime".into())
                .trim_matches('/'),
            unique()
        );
        S3Storage::new(S3StorageConfig {
            endpoint_url: Some(env_opt("DTRUNTIME_S3_ENDPOINT_URL")?),
            region: env_opt("DTRUNTIME_S3_REGION")?,
            bucket: env_opt("DTRUNTIME_S3_BUCKET")?,
            access_key_id: env_opt("DTRUNTIME_S3_ACCESS_KEY_ID")?,
            secret_access_key: env_opt("DTRUNTIME_S3_SECRET_ACCESS_KEY")?,
            force_path_style: true,
            key_prefix: Some(prefix),
            max_concurrent_operations: Some(32),
            multipart_part_size_bytes: part_size,
            multipart_threshold_bytes: threshold,
            ..Default::default()
        })
        .ok()
    }

    /// Deterministic pseudo-random payload for a given object index so we can
    /// verify integrity without storing every payload.
    fn payload(idx: usize, len: usize) -> Vec<u8> {
        let mut state = 0x9e37_79b9_u32.wrapping_mul(idx as u32 + 1);
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                (state & 0xff) as u8
            })
            .collect()
    }

    #[test]
    fn concurrent_put_get_delete_preserves_integrity() {
        let Some(storage) = maybe_storage(None, None) else {
            return;
        };
        let storage = Arc::new(storage);

        let objects = 200_usize;
        let size = 32 * 1024; // 32 KiB each
        let concurrency = 16;

        let started = Instant::now();
        let mut handles = Vec::new();
        for lane in 0..concurrency {
            let storage = Arc::clone(&storage);
            handles.push(std::thread::spawn(move || {
                for idx in (lane..objects).step_by(concurrency) {
                    let key = StorageKey::new(format!("obj-{idx}.bin")).unwrap();
                    let data = payload(idx, size);
                    storage.put(&key, &data).expect("put");
                    let got = storage.get(&key).expect("get");
                    assert_eq!(got, data, "object {idx} round-trip mismatch");
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let elapsed = started.elapsed();
        let mb = (objects * size) as f64 / 1e6;
        eprintln!(
            "s3 load: {objects} objects x {size}B put+get in {:.2}s ({:.1} MB/s round-trip, {concurrency} lanes)",
            elapsed.as_secs_f64(),
            2.0 * mb / elapsed.as_secs_f64(),
        );

        // Spot-check a few, then clean up.
        for idx in [0, objects / 2, objects - 1] {
            let key = StorageKey::new(format!("obj-{idx}.bin")).unwrap();
            assert!(storage.exists(&key).unwrap());
            storage.delete(&key).expect("delete");
            assert!(
                !storage.exists(&key).unwrap(),
                "delete did not remove {idx}"
            );
        }
    }

    #[test]
    fn multipart_upload_reassembles_large_object() {
        // Force multipart: 5 MiB parts, 5 MiB threshold, a 17 MiB object → 4
        // parts (three 5 MiB + one 2 MiB).
        let five_mib = 5 * 1024 * 1024;
        let Some(storage) = maybe_storage(Some(five_mib), Some(five_mib)) else {
            return;
        };
        let size = 17 * 1024 * 1024;
        let data = payload(42, size);
        let key = StorageKey::new("multipart/large.bin").unwrap();

        let started = Instant::now();
        storage.put(&key, &data).expect("multipart put");
        let got = storage.get(&key).expect("get");
        assert_eq!(got.len(), size, "multipart object size mismatch");
        assert_eq!(got, data, "multipart object bytes mismatch");
        eprintln!(
            "s3 multipart: {} MiB uploaded+downloaded in {:.2}s",
            size / (1024 * 1024),
            started.elapsed().as_secs_f64()
        );
        storage.delete(&key).expect("delete");
    }

    #[test]
    fn file_streaming_roundtrip_put_file_then_get_to_file() {
        // The restore path the checkpoint pipeline relies on: stream a file
        // up with put_file_async, stream it back with get_to_file_async, and
        // verify the bytes survived plus no .part remnant was left behind.
        let Some(storage) = maybe_storage(None, None) else {
            return;
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let dir = std::env::temp_dir().join(format!("dtr-s3-file-{}", unique()));
        std::fs::create_dir_all(&dir).unwrap();
        let src = dir.join("artifact.bin");
        let data = payload(7, 3 * 1024 * 1024 + 137); // deliberately not chunk-aligned
        std::fs::write(&src, &data).unwrap();

        let key = StorageKey::new("file-stream/artifact.bin").unwrap();
        rt.block_on(async {
            let put_size = storage.put_file_async(&key, &src).await.expect("put_file");
            assert_eq!(put_size, data.len() as u64);

            let out = dir.join("restored.bin");
            let got_size = storage
                .get_to_file_async(&key, &out)
                .await
                .expect("get_to_file");
            assert_eq!(got_size, data.len() as u64);
            assert_eq!(std::fs::read(&out).unwrap(), data, "bytes mismatch");
            assert!(
                !out.with_extension("part").exists(),
                ".part file left behind after successful download"
            );

            let missing = StorageKey::new("file-stream/does-not-exist.bin").unwrap();
            let err = storage
                .get_to_file_async(&missing, &dir.join("nope.bin"))
                .await
                .expect_err("missing key must error");
            assert!(
                matches!(err, runtime_storage::StorageError::NotFound(_)),
                "expected NotFound, got: {err:?}"
            );
        });
        storage.delete(&key).expect("delete");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
