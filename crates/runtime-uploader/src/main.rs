#![forbid(unsafe_code)]

//! runtime-uploader — stage-2 mover for the staged checkpoint pipeline.
//!
//! Streams a locally-staged checkpoint artifact to its durable destination,
//! or (with `--download`) streams an artifact back from that destination for
//! restore. Called by the Python SDK's `AsyncCheckpointer` as a subprocess so
//! the heavy I/O path stays in Rust (streaming body, connection pooling,
//! concurrency semaphore) while the training loop never blocks on it.
//!
//! Upload:   --source <FILE> --dest <s3://bucket/key | file:///path | PATH>
//!   s3://bucket/key           — streamed via `S3Storage::put_file_async`
//!   file:///abs/path or path  — copied with `std::fs::copy`
//! Download: --download --source <s3://bucket/key | file:///path | PATH> --dest <FILE>
//!   s3://bucket/key           — streamed via `S3Storage::get_to_file_async`
//!                               (written to `<FILE>.part`, renamed on success)
//!
//! Credentials/config come from flags or the same environment variables the
//! coordinator uses (`DTRUNTIME_S3_ACCESS_KEY`, `DTRUNTIME_S3_SECRET_KEY`,
//! `DTRUNTIME_S3_ENDPOINT`, `DTRUNTIME_S3_REGION`).
//!
//! On success prints one JSON line to stdout:
//!   {"uri":"s3://…","size_bytes":N,"elapsed_ms":M}
//!
//! Unlike `runtime-coordinator` and `runtime-bench`, this binary parses its own
//! arguments instead of using `clap`. That is deliberate: it is spawned once
//! per checkpoint artifact by the SDK, always with the same six machine-
//! generated flags, so it trades a dependency and the startup cost of building
//! a clap `Command` for forty lines of `match`. Flags meant for humans belong
//! in the two CLIs above.

use std::path::{Path, PathBuf};
use std::time::Instant;

use runtime_storage::uri::{parse_s3_uri, uri_to_local_path};
use runtime_storage::{S3Storage, S3StorageConfig, StorageKey};

struct Args {
    /// Upload: local file. Download: remote URI (s3:// | file:// | path).
    source: String,
    /// Upload: remote URI. Download: local file path.
    dest: String,
    download: bool,
    /// CLI overrides for the `DTRUNTIME_S3_*` environment configuration.
    endpoint_url: Option<String>,
    region: Option<String>,
    force_path_style: bool,
}

fn print_usage() {
    eprintln!(
        "Usage: runtime-uploader --source <FILE> --dest <s3://bucket/key | file:///path | PATH> \
         [--download] [--endpoint-url URL] [--region REGION] [--force-path-style]\n\
         With --download the direction reverses: --source is the remote URI, --dest the local file.\n\
         Env: DTRUNTIME_S3_ACCESS_KEY, DTRUNTIME_S3_SECRET_KEY, DTRUNTIME_S3_ENDPOINT, DTRUNTIME_S3_REGION"
    );
}

fn parse_args() -> Result<Args, String> {
    let mut source: Option<String> = None;
    let mut dest: Option<String> = None;
    let mut download = false;
    let mut endpoint_url: Option<String> = None;
    let mut region: Option<String> = None;
    let mut force_path_style = false;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--source" => source = Some(args.next().ok_or("--source needs a value")?),
            "--dest" => dest = Some(args.next().ok_or("--dest needs a value")?),
            "--download" => download = true,
            "--endpoint-url" => {
                endpoint_url = Some(args.next().ok_or("--endpoint-url needs a value")?)
            }
            "--region" => region = Some(args.next().ok_or("--region needs a value")?),
            "--force-path-style" => force_path_style = true,
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    Ok(Args {
        source: source.ok_or("--source is required")?,
        dest: dest.ok_or("--dest is required")?,
        download,
        endpoint_url,
        region,
        force_path_style,
    })
}

async fn s3_storage(args: &Args, bucket: &str) -> Result<S3Storage, String> {
    // Env (`DTRUNTIME_S3_*`) provides the base config; CLI flags override.
    let mut config = S3StorageConfig::from_env(bucket)?;
    if let Some(endpoint) = &args.endpoint_url {
        config.endpoint_url = Some(endpoint.clone());
        // MinIO and most self-hosted S3 endpoints need path-style addressing.
        config.force_path_style = true;
    }
    if let Some(region) = &args.region {
        config.region = region.clone();
    }
    config.force_path_style |= args.force_path_style;
    S3Storage::new_async(config)
        .await
        .map_err(|e| e.to_string())
}

async fn upload_s3(args: &Args, bucket: &str, key: &str) -> Result<u64, String> {
    let storage = s3_storage(args, bucket).await?;
    let storage_key = StorageKey::new(key).map_err(|e| e.to_string())?;
    storage
        .put_file_async(&storage_key, Path::new(&args.source))
        .await
        .map_err(|e| e.to_string())
}

async fn download_s3(args: &Args, bucket: &str, key: &str, out: &Path) -> Result<u64, String> {
    let storage = s3_storage(args, bucket).await?;
    let storage_key = StorageKey::new(key).map_err(|e| e.to_string())?;
    storage
        .get_to_file_async(&storage_key, out)
        .await
        .map_err(|e| e.to_string())
}

fn copy_file(source: &Path, dest: &Path) -> Result<u64, String> {
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    std::fs::copy(source, dest).map_err(|e| e.to_string())
}

#[tokio::main]
async fn main() {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("runtime-uploader: {e}");
            print_usage();
            std::process::exit(2);
        }
    };
    let start = Instant::now();
    let (result, uri) = if args.download {
        // --source is the remote URI, --dest the local file.
        let out_path = PathBuf::from(&args.dest);
        let result = if args.source.starts_with("s3://") {
            match parse_s3_uri(&args.source) {
                Ok((bucket, key)) => download_s3(&args, &bucket, &key, &out_path).await,
                Err(e) => Err(e),
            }
        } else {
            copy_file(&uri_to_local_path(&args.source), &out_path)
        };
        (result, args.source.clone())
    } else {
        let source_path = PathBuf::from(&args.source);
        if !source_path.is_file() {
            eprintln!(
                "runtime-uploader: source is not a file: {}",
                source_path.display()
            );
            std::process::exit(2);
        }
        let result = if args.dest.starts_with("s3://") {
            match parse_s3_uri(&args.dest) {
                Ok((bucket, key)) => upload_s3(&args, &bucket, &key).await,
                Err(e) => Err(e),
            }
        } else {
            copy_file(&source_path, &uri_to_local_path(&args.dest))
        };
        (result, args.dest.clone())
    };

    match result {
        Ok(size_bytes) => {
            let out = serde_json::json!({
                "uri": uri,
                "size_bytes": size_bytes,
                "elapsed_ms": start.elapsed().as_millis() as u64,
            });
            println!("{out}");
        }
        Err(e) => {
            let verb = if args.download { "download" } else { "upload" };
            eprintln!("runtime-uploader: {verb} failed: {e}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // URI parsing/conversion is covered by `runtime_storage::uri`'s own tests.

    #[test]
    fn copy_file_roundtrip() {
        let dir = std::env::temp_dir().join(format!("dtr-uploader-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let src = dir.join("src.bin");
        std::fs::write(&src, b"checkpoint-bytes").unwrap();
        let dest = dir.join("nested").join("dest.bin");
        let n = copy_file(&src, &dest).unwrap();
        assert_eq!(n, 16);
        assert_eq!(std::fs::read(&dest).unwrap(), b"checkpoint-bytes");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
