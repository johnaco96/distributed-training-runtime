#![forbid(unsafe_code)]

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use fs4::fs_std::FileExt;

/// Single-writer guard for the SQLite metadata store.
///
/// Uses an OS advisory lock (`LockFileEx` / `flock`) on a sidecar `.lock`
/// file rather than create-new sentinel semantics: the kernel releases the
/// lock when the process dies, so a hard-killed coordinator can always be
/// restarted on the same metadata DB without manual lock cleanup, while a
/// concurrently *running* coordinator is still refused.
pub struct CoordinatorLockGuard {
    lock_path: PathBuf,
    // Held open for the process lifetime; the advisory lock lives on this
    // handle.
    _file: File,
}

impl CoordinatorLockGuard {
    pub fn acquire(metadata_db_path: &Path) -> Result<Self, String> {
        let lock_path = metadata_db_path.with_extension("lock");
        let parent = lock_path.parent().ok_or("invalid metadata-db lock path")?;
        fs::create_dir_all(parent).map_err(|e| format!("failed to create lock directory: {e}"))?;
        let mut file = OpenOptions::new()
            .create(true)
            // Never truncate before the lock is held: a stale file's contents
            // are only overwritten by the process that wins the lock below.
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .map_err(|e| {
                format!(
                    "failed to open metadata-db lock file at {}: {e}",
                    lock_path.display()
                )
            })?;
        let locked = file.try_lock_exclusive().map_err(|e| {
            format!(
                "failed to lock metadata-db lock file at {}: {e}",
                lock_path.display()
            )
        })?;
        if !locked {
            return Err(format!(
                "metadata-db lock already held by a running coordinator at {}",
                lock_path.display()
            ));
        }
        let pid = std::process::id();
        let _ = writeln!(file, "pid={pid}");
        Ok(Self {
            lock_path,
            _file: file,
        })
    }
}

impl Drop for CoordinatorLockGuard {
    fn drop(&mut self) {
        // Best-effort tidy-up; correctness never depends on this because the
        // OS releases the advisory lock with the process.
        let _ = fs::remove_file(&self.lock_path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn second_acquire_fails_while_guard_lives_and_succeeds_after_drop() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("meta.db");

        let guard = CoordinatorLockGuard::acquire(&db).expect("first acquire");
        let second = CoordinatorLockGuard::acquire(&db);
        assert!(second.is_err(), "concurrent coordinator must be refused");

        drop(guard);
        CoordinatorLockGuard::acquire(&db).expect("acquire after release");
    }

    #[test]
    fn stale_lock_file_from_a_dead_process_does_not_block_acquisition() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("meta.db");
        let lock_path = db.with_extension("lock");

        // Simulate a hard-killed coordinator: the lock file exists but no
        // process holds the advisory lock.
        fs::write(&lock_path, "pid=99999\n").unwrap();

        CoordinatorLockGuard::acquire(&db)
            .expect("stale lock file must not block a restarted coordinator");
    }
}
