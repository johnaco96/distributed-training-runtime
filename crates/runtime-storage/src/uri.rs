//! URI helpers shared by every consumer that moves bytes to/from storage
//! (`runtime-uploader`, `dtruntime-native`). One implementation of the
//! `s3://bucket/key` split and the `file://` → path conversion — including
//! the Windows drive-letter edge case — instead of a copy per crate.

use std::path::PathBuf;

/// Split `s3://bucket/key/parts` into `(bucket, key)`.
pub fn parse_s3_uri(uri: &str) -> Result<(String, String), String> {
    let rest = uri
        .strip_prefix("s3://")
        .ok_or_else(|| format!("not an s3 uri: {uri}"))?;
    let (bucket, key) = rest
        .split_once('/')
        .ok_or_else(|| format!("s3 uri missing object key: {uri}"))?;
    if bucket.is_empty() || key.is_empty() {
        return Err(format!("s3 uri missing bucket or key: {uri}"));
    }
    Ok((bucket.to_string(), key.to_string()))
}

/// Resolve a `file://` URI or plain path to a filesystem path.
pub fn uri_to_local_path(uri: &str) -> PathBuf {
    let stripped = uri.strip_prefix("file://").unwrap_or(uri);
    // Windows: file:///C:/x arrives as /C:/x after the strip.
    let stripped =
        if stripped.len() >= 3 && stripped.starts_with('/') && stripped.as_bytes()[2] == b':' {
            &stripped[1..]
        } else {
            stripped
        };
    PathBuf::from(stripped)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_s3_uri_splits_bucket_and_key() {
        assert_eq!(
            parse_s3_uri("s3://bucket/a/b/c.bin").unwrap(),
            ("bucket".to_string(), "a/b/c.bin".to_string())
        );
    }

    #[test]
    fn parse_s3_uri_rejects_malformed() {
        assert!(parse_s3_uri("s3://bucket-only").is_err());
        assert!(parse_s3_uri("s3:///key-only").is_err());
        assert!(parse_s3_uri("http://not-s3/key").is_err());
    }

    #[test]
    fn uri_to_local_path_handles_plain_and_file_uris() {
        assert_eq!(uri_to_local_path("/tmp/x.bin"), PathBuf::from("/tmp/x.bin"));
        assert_eq!(
            uri_to_local_path("file:///tmp/x.bin"),
            PathBuf::from("/tmp/x.bin")
        );
    }

    #[test]
    fn uri_to_local_path_handles_windows_drive_letters() {
        assert_eq!(
            uri_to_local_path("file:///C:/data/x.bin"),
            PathBuf::from("C:/data/x.bin")
        );
    }
}
