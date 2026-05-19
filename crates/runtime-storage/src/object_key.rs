use crate::{StorageError, StorageKey};

pub fn normalize_key_prefix(prefix: Option<&str>) -> Result<Option<String>, StorageError> {
    match prefix {
        None => Ok(None),
        Some(raw) => {
            let trimmed = raw.trim().trim_matches('/');
            if trimmed.is_empty() {
                return Ok(None);
            }
            let key = StorageKey::new(trimmed)?;
            Ok(Some(key.as_str().to_string()))
        }
    }
}

pub fn storage_key_to_object_key(
    prefix: Option<&str>,
    key: &StorageKey,
) -> Result<String, StorageError> {
    let normalized_prefix = normalize_key_prefix(prefix)?;
    Ok(match normalized_prefix {
        Some(prefix) => format!("{prefix}/{}", key.as_str()),
        None => key.as_str().to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_normalization() {
        assert_eq!(normalize_key_prefix(None).expect("none"), None);
        assert_eq!(normalize_key_prefix(Some("/")).expect("empty slash"), None);
        assert_eq!(
            normalize_key_prefix(Some(" runs/a/ ")).expect("normalize"),
            Some("runs/a".to_string())
        );
    }

    #[test]
    fn key_mapping() {
        let key = StorageKey::new("runs/r1/LATEST").expect("valid");
        assert_eq!(
            storage_key_to_object_key(Some("prefix"), &key).expect("map"),
            "prefix/runs/r1/LATEST"
        );
        assert_eq!(
            storage_key_to_object_key(None, &key).expect("map"),
            "runs/r1/LATEST"
        );
    }
}
