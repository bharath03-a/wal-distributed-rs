//! Durable Raft state: `current_term` and `voted_for`.
//!
//! Raft requires these two values to survive crashes. We store them in a
//! single binary file (`raft_state.bin`) using an atomic write (temp + rename)
//! identical to the checkpoint approach in `wal-core`.
//!
//! # Format
//!
//! ```text
//! [ term: u64 LE ][ voted_for_len: u32 LE ][ voted_for: utf-8 bytes (N) ]
//! ```
//!
//! `voted_for_len == 0` means "not voted in this term".

use std::{fs, path::{Path, PathBuf}};

use crate::{config::NodeId, error::Result};

const STATE_FILE: &str = "raft_state.bin";

pub struct PersistentState {
    path: PathBuf,
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
}

impl PersistentState {
    pub fn open(dir: &Path) -> Result<Self> {
        let path = dir.join(STATE_FILE);
        let (term, voted_for) = if path.exists() {
            let bytes = fs::read(&path)?;
            decode(&bytes)
        } else {
            (0, None)
        };
        Ok(Self { path, current_term: term, voted_for })
    }

    /// Flush the current term and vote to disk atomically.
    pub fn persist(&self) -> Result<()> {
        let tmp = self.path.with_extension("tmp");
        fs::write(&tmp, encode(self.current_term, self.voted_for.as_deref()))?;
        fs::rename(&tmp, &self.path)?;
        Ok(())
    }

    /// Advance to a higher term and clear the vote. Persists immediately.
    pub fn advance_term(&mut self, new_term: u64) -> Result<()> {
        self.current_term = new_term;
        self.voted_for = None;
        self.persist()
    }

    /// Record a vote for `candidate` in the current term. Persists immediately.
    pub fn record_vote(&mut self, candidate: NodeId) -> Result<()> {
        self.voted_for = Some(candidate);
        self.persist()
    }
}

fn encode(term: u64, voted_for: Option<&str>) -> Vec<u8> {
    let s = voted_for.unwrap_or("");
    let mut buf = Vec::with_capacity(8 + 4 + s.len());
    buf.extend_from_slice(&term.to_le_bytes());
    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
    buf
}

fn decode(bytes: &[u8]) -> (u64, Option<NodeId>) {
    if bytes.len() < 12 {
        return (0, None);
    }
    let term = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    let len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let voted_for = if len == 0 || bytes.len() < 12 + len {
        None
    } else {
        String::from_utf8(bytes[12..12 + len].to_vec()).ok()
    };
    (term, voted_for)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn fresh_dir_is_term_zero_no_vote() {
        let dir = TempDir::new().unwrap();
        let s = PersistentState::open(dir.path()).unwrap();
        assert_eq!(s.current_term, 0);
        assert!(s.voted_for.is_none());
    }

    #[test]
    fn persists_and_reloads() {
        let dir = TempDir::new().unwrap();
        {
            let mut s = PersistentState::open(dir.path()).unwrap();
            s.advance_term(5).unwrap();
            s.record_vote("node-2".into()).unwrap();
        }
        let s = PersistentState::open(dir.path()).unwrap();
        assert_eq!(s.current_term, 5);
        assert_eq!(s.voted_for.as_deref(), Some("node-2"));
    }

    #[test]
    fn advance_term_clears_vote() {
        let dir = TempDir::new().unwrap();
        let mut s = PersistentState::open(dir.path()).unwrap();
        s.advance_term(3).unwrap();
        s.record_vote("node-1".into()).unwrap();
        s.advance_term(4).unwrap();
        assert!(s.voted_for.is_none());
    }
}
