use crate::object_key::{normalize_key_prefix, storage_key_to_object_key};
use crate::{StorageBackend, StorageError, StorageKey};
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

#[derive(Debug, Clone)]
pub struct S3StorageConfig {
    pub endpoint_url: Option<String>,
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub force_path_style: bool,
    pub key_prefix: Option<String>,
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
    rt: std::sync::Arc<tokio::runtime::Runtime>,
}

impl S3Storage {
    pub fn new(config: S3StorageConfig) -> Result<Self, StorageError> {
        config.validate()?;
        let key_prefix = config.normalized_key_prefix()?;
        let rt = tokio::runtime::Runtime::new().map_err(StorageError::Io)?;
        let client = rt.block_on(async move {
            let credentials = Credentials::new(
                config.access_key_id,
                config.secret_access_key,
                None,
                None,
                "dtruntime-s3-storage",
            );
            let mut loader = aws_config::defaults(BehaviorVersion::latest())
                .region(Region::new(config.region))
                .credentials_provider(credentials);
            if let Some(endpoint) = config.endpoint_url {
                loader = loader.endpoint_url(endpoint);
            }
            let shared = loader.load().await;
            let s3_config = aws_sdk_s3::config::Builder::from(&shared)
                .force_path_style(config.force_path_style)
                .build();
            Client::from_conf(s3_config)
        });
        Ok(Self {
            client,
            bucket: config.bucket,
            key_prefix,
            rt: std::sync::Arc::new(rt),
        })
    }

    fn object_key_for(&self, key: &StorageKey) -> Result<String, StorageError> {
        storage_key_to_object_key(self.key_prefix.as_deref(), key)
    }
}

impl StorageBackend for S3Storage {
    fn put(&self, key: &StorageKey, bytes: &[u8]) -> Result<(), StorageError> {
        let object_key = self.object_key_for(key)?;
        self.rt.block_on(async {
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(object_key)
                .body(ByteStream::from(bytes.to_vec()))
                .send()
                .await
                .map_err(|err| StorageError::Io(std::io::Error::other(err.to_string())))?;
            Ok(())
        })
    }

    fn get(&self, key: &StorageKey) -> Result<Vec<u8>, StorageError> {
        let object_key = self.object_key_for(key)?;
        self.rt.block_on(async {
            match self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(object_key)
                .send()
                .await
            {
                Ok(out) => out
                    .body
                    .collect()
                    .await
                    .map(|b| b.into_bytes().to_vec())
                    .map_err(|err| StorageError::Io(std::io::Error::other(err.to_string()))),
                Err(err) => {
                    let msg = err.to_string();
                    if msg.contains("NoSuchKey") {
                        Err(StorageError::NotFound(key.clone()))
                    } else {
                        Err(StorageError::Io(std::io::Error::other(msg)))
                    }
                }
            }
        })
    }

    fn exists(&self, key: &StorageKey) -> Result<bool, StorageError> {
        let object_key = self.object_key_for(key)?;
        self.rt.block_on(async {
            match self
                .client
                .head_object()
                .bucket(&self.bucket)
                .key(object_key)
                .send()
                .await
            {
                Ok(_) => Ok(true),
                Err(err) => {
                    let msg = err.to_string();
                    if msg.contains("NotFound") || msg.contains("404") {
                        Ok(false)
                    } else {
                        Err(StorageError::Io(std::io::Error::other(msg)))
                    }
                }
            }
        })
    }

    fn list_prefix(&self, prefix: &StorageKey) -> Result<Vec<StorageKey>, StorageError> {
        let object_prefix = self.object_key_for(prefix)?;
        self.rt.block_on(async {
            let result = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&object_prefix)
                .send()
                .await
                .map_err(|err| StorageError::Io(std::io::Error::other(err.to_string())))?;

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
        let object_key = self.object_key_for(key)?;
        self.rt.block_on(async {
            self.client
                .delete_object()
                .bucket(&self.bucket)
                .key(object_key)
                .send()
                .await
                .map_err(|err| StorageError::Io(std::io::Error::other(err.to_string())))?;
            Ok(())
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
        };
        assert!(cfg.validate().is_err());
    }
}
