#![forbid(unsafe_code)]

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

pub struct CoordinatorLockGuard {
    lock_path: PathBuf,
}

impl CoordinatorLockGuard {
    pub fn acquire(metadata_db_path: &Path) -> Result<Self, String> {
        let lock_path = metadata_db_path.with_extension("lock");
        let parent = lock_path.parent().ok_or("invalid metadata-db lock path")?;
        fs::create_dir_all(parent).map_err(|e| format!("failed to create lock directory: {e}"))?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&lock_path)
            .map_err(|e| {
                format!(
                    "metadata-db lock already held or unavailable at {}: {e}",
                    lock_path.display()
                )
            })?;
        let pid = std::process::id();
        writeln!(file, "pid={pid}").map_err(|e| format!("failed to write lock file: {e}"))?;
        Ok(Self { lock_path })
    }
}

impl Drop for CoordinatorLockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.lock_path);
    }
}
