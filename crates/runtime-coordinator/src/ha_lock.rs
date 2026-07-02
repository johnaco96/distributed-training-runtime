#![forbid(unsafe_code)]

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

/// Single-node coordinator lock for the SQLite metadata store.
///
/// Holds an OS advisory lock (`File::try_lock`) on `<metadata-db>.lock` for
/// the lifetime of the guard. The OS releases the lock when the process
/// exits — including crashes and SIGKILL — so a crashed coordinator never
/// leaves a stale lock behind and can always restart immediately.
///
/// (The previous implementation created the file with `create_new` and
/// removed it in `Drop`; any hard kill left the file in place and blocked
/// every subsequent restart, which defeats crash recovery.)
pub struct CoordinatorLockGuard {
    /// Keeps the OS lock alive; dropping the handle releases it.
    _file: File,
    lock_path: PathBuf,
}

impl CoordinatorLockGuard {
    pub fn acquire(metadata_db_path: &Path) -> Result<Self, String> {
        let lock_path = metadata_db_path.with_extension("lock");
        let parent = lock_path.parent().ok_or("invalid metadata-db lock path")?;
        fs::create_dir_all(parent).map_err(|e| format!("failed to create lock directory: {e}"))?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .map_err(|e| {
                format!(
                    "failed to open metadata-db lock file at {}: {e}",
                    lock_path.display()
                )
            })?;
        file.try_lock().map_err(|e| {
            format!(
                "metadata-db lock already held by a live coordinator at {}: {e}",
                lock_path.display()
            )
        })?;
        // Best-effort breadcrumb for operators inspecting the lock file.
        let pid = std::process::id();
        let _ = writeln!(file, "pid={pid}");
        Ok(Self {
            _file: file,
            lock_path,
        })
    }

    pub fn path(&self) -> &Path {
        &self.lock_path
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
        // process holds the OS advisory lock.
        fs::write(&lock_path, "pid=99999\n").unwrap();

        CoordinatorLockGuard::acquire(&db)
            .expect("stale lock file must not block a restarted coordinator");
    }
}
