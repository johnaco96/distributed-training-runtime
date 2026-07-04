use crate::object_key::{normalize_key_prefix, storage_key_to_object_key};
use crate::{
    run_with_timeout_retry, StorageBackend, StorageError, StorageKey, StorageOperationPolicy,
};
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, ServerSideEncryption};
use aws_sdk_s3::Client;
use tokio::sync::Semaphore;

/// Default multipart threshold: objects larger than this are uploaded in
/// parts.  Below this a single `PutObject` is cheaper (one request).
pub const DEFAULT_MULTIPART_THRESHOLD_BYTES: usize = 16 * 1024 * 1024;
/// Default part size for multipart uploads.  S3 requires parts >= 5 MiB
/// (except the last) and allows at most 10 000 parts.
pub const DEFAULT_MULTIPART_PART_SIZE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct S3StorageConfig {
    pub endpoint_url: Option<String>,
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub force_path_style: bool,
    pub key_prefix: Option<String>,
    /// Override the number of concurrent S3 operations allowed by the semaphore.
    /// `None` uses [`StorageOperationPolicy::default`] (currently 64).
    /// Pass `Some(N)` to tune for your worker count — a value of `world_size`
    /// or higher eliminates semaphore stalls at checkpoint time.
    pub max_concurrent_operations: Option<usize>,
    /// Server-side encryption for uploads: `Some("AES256")` for SSE-S3 or
    /// `Some("aws:kms")` for SSE-KMS.  `None` uses the bucket default.
    pub server_side_encryption: Option<String>,
    /// KMS key id/ARN for SSE-KMS (used only when `server_side_encryption` is
    /// `aws:kms`).
    pub sse_kms_key_id: Option<String>,
    /// Objects at or above this size are uploaded via multipart.  `None` uses
    /// [`DEFAULT_MULTIPART_THRESHOLD_BYTES`].
    pub multipart_threshold_bytes: Option<usize>,
    /// Part size for multipart uploads.  `None` uses
    /// [`DEFAULT_MULTIPART_PART_SIZE_BYTES`].  Clamped to the S3 minimum of
    /// 5 MiB.
    pub multipart_part_size_bytes: Option<usize>,
}

impl Default for S3StorageConfig {
    fn default() -> Self {
        Self {
            endpoint_url: None,
            region: "us-east-1".to_string(),
            bucket: String::new(),
            access_key_id: String::new(),
            secret_access_key: String::new(),
            force_path_style: false,
            key_prefix: None,
            max_concurrent_operations: None,
            server_side_encryption: None,
            sse_kms_key_id: None,
            multipart_threshold_bytes: None,
            multipart_part_size_bytes: None,
        }
    }
}

/// Whether an S3 error code denotes a transient failure worth retrying.
///
/// Throttling and server-side 5xx codes are retryable; everything else
/// (auth, validation, missing bucket/key) is permanent.  Unknown service
/// codes default to permanent so an unexpected auth/validation error is not
/// retried into the ground.
pub(crate) fn is_retryable_s3_code(code: &str) -> bool {
    matches!(
        code,
        "SlowDown"
            | "RequestTimeout"
            | "RequestTimeTooSkewed"
            | "ServiceUnavailable"
            | "InternalError"
            | "ThrottlingException"
            | "Throttling"
            | "TooManyRequests"
            | "503 SlowDown"
    )
}

/// Classify an AWS SDK error into a [`StorageError`], deciding whether the
/// retry loop should re-attempt it.
///
/// - transport failures (timeout, dispatch, connection) → retryable;
/// - service errors are classified by their S3 error code: throttling and
///   server-side 5xx codes are retryable, everything else (auth, validation,
///   missing bucket) is permanent so it is not retried into oblivion;
/// - `NoSuchKey` maps to [`StorageError::NotFound`] when a key is supplied.
pub(crate) fn classify_s3_error<E, R>(
    operation: &'static str,
    key: Option<&StorageKey>,
    err: &SdkError<E, R>,
) -> StorageError
where
    E: ProvideErrorMetadata,
{
    match err.as_service_error() {
        Some(service) => {
            let code = service.code().unwrap_or("").to_string();
            if code == "NoSuchKey" || code == "NotFound" {
                if let Some(key) = key {
                    return StorageError::NotFound(key.clone());
                }
            }
            let retryable = is_retryable_s3_code(&code);
            StorageError::Backend {
                operation,
                retryable,
                code: Some(code),
                message: service.message().unwrap_or("").to_string(),
            }
        }
        // No modelled service error → transport/dispatch/timeout: retryable.
        None => StorageError::Backend {
            operation,
            retryable: true,
            code: None,
            message: err.to_string(),
        },
    }
}

impl S3StorageConfig {
    pub fn validate(&self) -> Result<(), StorageError> {
        if self.region.trim().is_empty() {
            return Err(StorageError::InvalidKey(
                "s3 region cannot be empty".to_string(),
            ));
        }
        if self.bucket.trim().is_empty() {
            return Err(StorageError::InvalidKey(
                "s3 bucket cannot be empty".to_string(),
            ));
        }
        Ok(())
    }

    pub fn normalized_key_prefix(&self) -> Result<Option<String>, StorageError> {
        normalize_key_prefix(self.key_prefix.as_deref())
    }
}

#[derive(Clone)]
pub struct S3Storage {
    client: Client,
    bucket: String,
    key_prefix: Option<String>,
    /// Present when constructed via [`S3Storage::new`] (sync / blocking path).
    /// Absent when constructed via [`S3Storage::new_async`] (native async path);
    /// in that case the sync [`StorageBackend`] methods are unavailable and
    /// only [`S3Storage::put_async`] should be used.
    rt: Option<std::sync::Arc<tokio::runtime::Runtime>>,
    policy: StorageOperationPolicy,
    limiter: std::sync::Arc<Semaphore>,
    sse: Option<ServerSideEncryption>,
    sse_kms_key_id: Option<String>,
    multipart_threshold: usize,
    multipart_part_size: usize,
}

impl S3Storage {
    /// Synchronous constructor — creates its own Tokio runtime internally.
    ///
    /// **Must not be called from within an async context** (panics with
    /// "Cannot start a runtime from within a runtime"). Use
    /// [`S3Storage::new_async`] instead when you are already inside a Tokio
    /// runtime (e.g. from `#[tokio::main]`).
    pub fn new(config: S3StorageConfig) -> Result<Self, StorageError> {
        let policy = Self::policy_from_config(&config);
        policy.validate()?;
        config.validate()?;
        let key_prefix = config.normalized_key_prefix()?;
        let rt = tokio::runtime::Runtime::new().map_err(StorageError::Io)?;
        let client = rt.block_on(Self::build_client(config.clone()));
        Ok(Self::assemble(
            client,
            config,
            key_prefix,
            Some(std::sync::Arc::new(rt)),
            policy,
        ))
    }

    /// Async constructor — builds the AWS SDK client in the caller's Tokio
    /// runtime, so the connection pool is driven by that same runtime.
    ///
    /// Use this from `async` contexts (e.g. inside `#[tokio::main]`). The
    /// returned [`S3Storage`] only supports [`S3Storage::put_async`] and the
    /// other `*_async` helpers; calling the synchronous [`StorageBackend`]
    /// methods will return an error.
    pub async fn new_async(config: S3StorageConfig) -> Result<Self, StorageError> {
        let policy = Self::policy_from_config(&config);
        policy.validate()?;
        config.validate()?;
        let key_prefix = config.normalized_key_prefix()?;
        let client = Self::build_client(config.clone()).await;
        Ok(Self::assemble(client, config, key_prefix, None, policy))
    }

    /// Assemble an [`S3Storage`] from its built client and resolved config,
    /// shared by both constructors.
    fn assemble(
        client: Client,
        config: S3StorageConfig,
        key_prefix: Option<String>,
        rt: Option<std::sync::Arc<tokio::runtime::Runtime>>,
        policy: StorageOperationPolicy,
    ) -> Self {
        let sse = config
            .server_side_encryption
            .as_deref()
            .map(ServerSideEncryption::from);
        let multipart_threshold = config
            .multipart_threshold_bytes
            .unwrap_or(DEFAULT_MULTIPART_THRESHOLD_BYTES);
        // Enforce the S3 minimum part size (5 MiB) so multipart never fails
        // on a too-small non-final part.
        let multipart_part_size = config
            .multipart_part_size_bytes
            .unwrap_or(DEFAULT_MULTIPART_PART_SIZE_BYTES)
            .max(5 * 1024 * 1024);
        Self {
            client,
            bucket: config.bucket,
            key_prefix,
            rt,
            limiter: std::sync::Arc::new(Semaphore::new(policy.max_concurrent_operations)),
            policy,
            sse,
            sse_kms_key_id: config.sse_kms_key_id,
            multipart_threshold,
            multipart_part_size,
        }
    }

    /// Resolve the effective [`StorageOperationPolicy`] from a config.
    ///
    /// Starts from the global default and applies any per-config override for
    /// `max_concurrent_operations` (set via `--max-s3-concurrent-ops`).
    fn policy_from_config(config: &S3StorageConfig) -> StorageOperationPolicy {
        let mut policy = StorageOperationPolicy::default();
        if let Some(n) = config.max_concurrent_operations {
            policy.max_concurrent_operations = n;
        }
        policy
    }

    /// Build the AWS SDK S3 client from a config. Used by both constructors.
    ///
    /// Builds the S3-specific config directly rather than inheriting from a
    /// shared `SdkConfig` so that custom endpoint URLs (e.g. MinIO) and
    /// `force_path_style` are guaranteed to take effect on the S3 client.
    async fn build_client(config: S3StorageConfig) -> Client {
        let credentials = Credentials::new(
            config.access_key_id,
            config.secret_access_key,
            None,
            None,
            "dtruntime-s3-storage",
        );
        let mut s3_builder = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(config.region))
            .credentials_provider(credentials)
            .force_path_style(config.force_path_style);
        if let Some(endpoint) = config.endpoint_url {
            s3_builder = s3_builder.endpoint_url(endpoint);
        }
        Client::from_conf(s3_builder.build())
    }

    fn object_key_for(&self, key: &StorageKey) -> Result<String, StorageError> {
        storage_key_to_object_key(self.key_prefix.as_deref(), key)
    }

    /// Upload bytes to S3 natively async.
    ///
    /// Works whether the instance was created via [`S3Storage::new`] or
    /// [`S3Storage::new_async`]. The HTTP request is driven by the current
    /// Tokio runtime (the one awaiting this future), so the client connection
    /// pool and the runtime are always co-located.
    pub async fn put_async(&self, key: &StorageKey, bytes: Vec<u8>) -> Result<(), StorageError> {
        if bytes.len() >= self.multipart_threshold {
            return self.multipart_put_async(key, bytes).await;
        }
        let object_key = self.object_key_for(key)?;
        run_with_timeout_retry(&self.policy, "s3-put", || {
            let object_key = object_key.clone();
            let bytes = bytes.clone();
            async move {
                let _permit = self.acquire().await?;
                self.apply_sse(
                    self.client
                        .put_object()
                        .bucket(&self.bucket)
                        .key(object_key)
                        .body(ByteStream::from(bytes)),
                )
                .send()
                .await
                .map_err(|err| classify_s3_error("s3-put", Some(key), &err))?;
                Ok(())
            }
        })
        .await
    }

    /// Upload a large object in parts.
    ///
    /// Parts are uploaded with bounded concurrency (via the shared
    /// semaphore); on any failure the multipart upload is aborted so no
    /// orphaned parts accrue storage cost.  Each part upload is retried
    /// independently by the SDK's built-in retry layer.
    async fn multipart_put_async(
        &self,
        key: &StorageKey,
        bytes: Vec<u8>,
    ) -> Result<(), StorageError> {
        let object_key = self.object_key_for(key)?;
        let create = self
            .apply_sse_multipart(
                self.client
                    .create_multipart_upload()
                    .bucket(&self.bucket)
                    .key(&object_key),
            )
            .send()
            .await
            .map_err(|err| classify_s3_error("s3-multipart-create", Some(key), &err))?;
        let upload_id = create.upload_id().unwrap_or_default().to_string();

        let result = self
            .upload_all_parts(&object_key, &upload_id, &bytes, key)
            .await;
        match result {
            Ok(parts) => {
                self.client
                    .complete_multipart_upload()
                    .bucket(&self.bucket)
                    .key(&object_key)
                    .upload_id(&upload_id)
                    .multipart_upload(
                        CompletedMultipartUpload::builder()
                            .set_parts(Some(parts))
                            .build(),
                    )
                    .send()
                    .await
                    .map_err(|err| classify_s3_error("s3-multipart-complete", Some(key), &err))?;
                Ok(())
            }
            Err(err) => {
                // Best-effort abort so partial parts do not linger.
                let _ = self
                    .client
                    .abort_multipart_upload()
                    .bucket(&self.bucket)
                    .key(&object_key)
                    .upload_id(&upload_id)
                    .send()
                    .await;
                Err(err)
            }
        }
    }

    async fn upload_all_parts(
        &self,
        object_key: &str,
        upload_id: &str,
        bytes: &[u8],
        key: &StorageKey,
    ) -> Result<Vec<CompletedPart>, StorageError> {
        let part_size = self.multipart_part_size;
        let mut tasks = tokio::task::JoinSet::new();
        for (idx, chunk) in bytes.chunks(part_size).enumerate() {
            let part_number = idx as i32 + 1; // S3 part numbers are 1-based.
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let object_key = object_key.to_string();
            let upload_id = upload_id.to_string();
            let limiter = self.limiter.clone();
            let policy = self.policy;
            let body = chunk.to_vec();
            let key = key.clone();
            tasks.spawn(async move {
                run_with_timeout_retry(&policy, "s3-upload-part", || {
                    let client = client.clone();
                    let bucket = bucket.clone();
                    let object_key = object_key.clone();
                    let upload_id = upload_id.clone();
                    let limiter = limiter.clone();
                    let body = body.clone();
                    let key = key.clone();
                    async move {
                        let _permit = limiter
                            .acquire()
                            .await
                            .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?;
                        let out = client
                            .upload_part()
                            .bucket(&bucket)
                            .key(&object_key)
                            .upload_id(&upload_id)
                            .part_number(part_number)
                            .body(ByteStream::from(body))
                            .send()
                            .await
                            .map_err(|err| classify_s3_error("s3-upload-part", Some(&key), &err))?;
                        Ok(CompletedPart::builder()
                            .part_number(part_number)
                            .set_e_tag(out.e_tag().map(String::from))
                            .build())
                    }
                })
                .await
            });
        }

        let mut parts = Vec::new();
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(Ok(part)) => parts.push(part),
                Ok(Err(err)) => return Err(err),
                Err(join_err) => {
                    return Err(StorageError::Io(std::io::Error::other(
                        join_err.to_string(),
                    )))
                }
            }
        }
        // Completion requires parts in ascending part-number order.
        parts.sort_by_key(|p| p.part_number());
        Ok(parts)
    }

    async fn acquire(&self) -> Result<tokio::sync::SemaphorePermit<'_>, StorageError> {
        self.limiter
            .acquire()
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))
    }

    fn apply_sse(
        &self,
        builder: aws_sdk_s3::operation::put_object::builders::PutObjectFluentBuilder,
    ) -> aws_sdk_s3::operation::put_object::builders::PutObjectFluentBuilder {
        let mut builder = builder;
        if let Some(sse) = &self.sse {
            builder = builder.server_side_encryption(sse.clone());
        }
        if let Some(kms) = &self.sse_kms_key_id {
            builder = builder.ssekms_key_id(kms);
        }
        builder
    }

    fn apply_sse_multipart(
        &self,
        builder: aws_sdk_s3::operation::create_multipart_upload::builders::CreateMultipartUploadFluentBuilder,
    ) -> aws_sdk_s3::operation::create_multipart_upload::builders::CreateMultipartUploadFluentBuilder
    {
        let mut builder = builder;
        if let Some(sse) = &self.sse {
            builder = builder.server_side_encryption(sse.clone());
        }
        if let Some(kms) = &self.sse_kms_key_id {
            builder = builder.ssekms_key_id(kms);
        }
        builder
    }

    fn sync_rt(&self) -> Result<&tokio::runtime::Runtime, StorageError> {
        self.rt.as_deref().ok_or_else(|| {
            StorageError::Io(std::io::Error::other(
                "S3Storage was created with new_async(); use put_async() instead of the \
                 synchronous StorageBackend methods",
            ))
        })
    }
}

impl StorageBackend for S3Storage {
    fn put(&self, key: &StorageKey, bytes: &[u8]) -> Result<(), StorageError> {
        let rt = self.sync_rt()?;
        rt.block_on(self.put_async(key, bytes.to_vec()))
    }

    fn get(&self, key: &StorageKey) -> Result<Vec<u8>, StorageError> {
        let rt = self.sync_rt()?;
        let object_key = self.object_key_for(key)?;
        rt.block_on(async {
            run_with_timeout_retry(&self.policy, "s3-get", || async {
                let _permit = self
                    .limiter
                    .acquire()
                    .await
                    .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?;
                match self
                    .client
                    .get_object()
                    .bucket(&self.bucket)
                    .key(object_key.clone())
                    .send()
                    .await
                {
                    Ok(out) => out
                        .body
                        .collect()
                        .await
                        .map(|b| b.into_bytes().to_vec())
                        .map_err(|err| StorageError::Io(std::io::Error::other(err.to_string()))),
                    Err(err) => Err(classify_s3_error("s3-get", Some(key), &err)),
                }
            })
            .await
        })
    }

    fn exists(&self, key: &StorageKey) -> Result<bool, StorageError> {
        let rt = self.sync_rt()?;
        let object_key = self.object_key_for(key)?;
        rt.block_on(async {
            run_with_timeout_retry(&self.policy, "s3-exists", || async {
                let _permit = self
                    .limiter
                    .acquire()
                    .await
                    .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?;
                match self
                    .client
                    .head_object()
                    .bucket(&self.bucket)
                    .key(object_key.clone())
                    .send()
                    .await
                {
                    Ok(_) => Ok(true),
                    Err(err) => match classify_s3_error("s3-exists", Some(key), &err) {
                        StorageError::NotFound(_) => Ok(false),
                        other => Err(other),
                    },
                }
            })
            .await
        })
    }

    fn list_prefix(&self, prefix: &StorageKey) -> Result<Vec<StorageKey>, StorageError> {
        let rt = self.sync_rt()?;
        let object_prefix = self.object_key_for(prefix)?;
        rt.block_on(async {
            let result = run_with_timeout_retry(&self.policy, "s3-list-prefix", || async {
                let _permit = self
                    .limiter
                    .acquire()
                    .await
                    .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?;
                self.client
                    .list_objects_v2()
                    .bucket(&self.bucket)
                    .prefix(&object_prefix)
                    .send()
                    .await
                    .map_err(|err| classify_s3_error("s3-list-prefix", None, &err))
            })
            .await?;

            let mut out = Vec::new();
            for object in result.contents() {
                if let Some(key) = object.key() {
                    let relative = if let Some(prefix) = &self.key_prefix {
                        key.strip_prefix(&format!("{prefix}/")).unwrap_or(key)
                    } else {
                        key
                    };
                    out.push(StorageKey::new(relative)?);
                }
            }
            out.sort();
            Ok(out)
        })
    }

    fn delete(&self, key: &StorageKey) -> Result<(), StorageError> {
        let rt = self.sync_rt()?;
        let object_key = self.object_key_for(key)?;
        rt.block_on(async {
            run_with_timeout_retry(&self.policy, "s3-delete", || async {
                let _permit = self
                    .limiter
                    .acquire()
                    .await
                    .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?;
                self.client
                    .delete_object()
                    .bucket(&self.bucket)
                    .key(object_key.clone())
                    .send()
                    .await
                    .map_err(|err| classify_s3_error("s3-delete", Some(key), &err))?;
                Ok(())
            })
            .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_rejects_empty_bucket() {
        let cfg = S3StorageConfig {
            endpoint_url: None,
            region: "us-east-1".into(),
            bucket: " ".into(),
            access_key_id: "x".into(),
            secret_access_key: "y".into(),
            force_path_style: true,
            key_prefix: None,
            max_concurrent_operations: None,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn config_rejects_empty_region() {
        let cfg = S3StorageConfig {
            endpoint_url: None,
            region: " ".into(),
            bucket: "b".into(),
            access_key_id: "x".into(),
            secret_access_key: "y".into(),
            force_path_style: true,
            key_prefix: None,
            max_concurrent_operations: None,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn retryable_codes_are_transient_only() {
        for code in [
            "SlowDown",
            "ServiceUnavailable",
            "InternalError",
            "ThrottlingException",
            "TooManyRequests",
            "RequestTimeout",
        ] {
            assert!(is_retryable_s3_code(code), "{code} should be retryable");
        }
        for code in [
            "AccessDenied",
            "InvalidAccessKeyId",
            "SignatureDoesNotMatch",
            "NoSuchBucket",
            "InvalidArgument",
            "",
            "SomethingUnknown",
        ] {
            assert!(!is_retryable_s3_code(code), "{code} must not be retried");
        }
    }
}
