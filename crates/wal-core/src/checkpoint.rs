//! Durable checkpoint file.
//!
//! The checkpoint records the highest sequence number whose corresponding
//! entry has been *applied* to stable state (e.g. flushed to a database).
//! Entries at or below the checkpoint are safe to truncate; entries above
//! it must be replayed on the next startup.
//!
//! # On-disk format
//!
//! A single little-endian `u64` stored in `<dir>/checkpoint`.
//! A missing or empty file is treated as checkpoint 0 (nothing applied yet).

use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::error::Result;

const CHECKPOINT_FILE: &str = "checkpoint";

/// Manages the durable checkpoint value for a WAL directory.
pub struct Checkpoint {
    path: PathBuf,
    sequence: u64,
}

impl Checkpoint {
    /// Load from disk, or initialise at 0 if the file does not exist yet.
    pub fn open(dir: &Path) -> Result<Self> {
        let path = dir.join(CHECKPOINT_FILE);
        let sequence = if path.exists() {
            let bytes = fs::read(&path)?;
            // Tolerate a truncated file (treat as 0)
            if bytes.len() >= 8 {
                u64::from_le_bytes(bytes[..8].try_into().unwrap())
            } else {
                0
            }
        } else {
            0
        };
        Ok(Self { path, sequence })
    }

    /// The last persisted checkpoint sequence (0 means nothing checkpointed).
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Advance the checkpoint to `sequence` and persist atomically.
    ///
    /// Silently ignores calls where `sequence ≤ current`, so it is safe to
    /// call with a value that was already recorded.
    pub fn advance(&mut self, sequence: u64) -> Result<()> {
        if sequence > self.sequence {
            // Write to a temp file then rename for crash-atomicity
            let tmp = self.path.with_extension("tmp");
            fs::write(&tmp, sequence.to_le_bytes())?;
            fs::rename(&tmp, &self.path)?;
            self.sequence = sequence;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn starts_at_zero_on_fresh_dir() {
        let dir = TempDir::new().unwrap();
        let cp = Checkpoint::open(dir.path()).unwrap();
        assert_eq!(cp.sequence(), 0);
    }

    #[test]
    fn persists_across_reopen() {
        let dir = TempDir::new().unwrap();
        {
            let mut cp = Checkpoint::open(dir.path()).unwrap();
            cp.advance(100).unwrap();
        }
        let cp = Checkpoint::open(dir.path()).unwrap();
        assert_eq!(cp.sequence(), 100);
    }

    #[test]
    fn advance_is_monotonic() {
        let dir = TempDir::new().unwrap();
        let mut cp = Checkpoint::open(dir.path()).unwrap();
        cp.advance(50).unwrap();
        cp.advance(30).unwrap(); // should be a no-op
        assert_eq!(cp.sequence(), 50);
    }

    #[test]
    fn advance_same_value_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let mut cp = Checkpoint::open(dir.path()).unwrap();
        cp.advance(7).unwrap();
        cp.advance(7).unwrap();
        assert_eq!(cp.sequence(), 7);
    }
}
