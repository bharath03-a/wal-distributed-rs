//! The replicated Raft log.
//!
//! Each log entry carries a `term` (the leader's term when it was appended)
//! plus the application payload.  The term is what lets Raft detect and
//! resolve divergence between nodes after a leader change.
//!
//! # Persistence
//!
//! Entries are kept in an in-memory `Vec` for fast indexed access.
//! Every mutation (append / truncate) is also written to the underlying
//! `wal-core` WAL so the log survives crashes.
//!
//! ## WAL payload encoding
//!
//! | kind | payload |
//! |------|---------|
//! | `0x01` APPEND   | `[index: u64 LE][term: u64 LE][data: bytes]` |
//! | `0x02` TRUNCATE | `[from_index: u64 LE]` |
//!
//! On recovery we replay the WAL and apply each record, so truncations are
//! crash-safe.

use wal_core::{Wal, WalConfig};

use crate::error::Result;

// ── On-disk record kinds ─────────────────────────────────────────────────────

const KIND_APPEND: u8 = 0x01;
const KIND_TRUNCATE: u8 = 0x02;

// ── Public types ─────────────────────────────────────────────────────────────

/// One entry in the replicated log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub term: u64,
    /// 1-based position in the log (Raft convention).
    pub index: u64,
    pub data: Vec<u8>,
}

// ── RaftLog ───────────────────────────────────────────────────────────────────

/// In-memory replicated log backed by a `wal-core` WAL for crash recovery.
///
/// Log indices are **1-based** throughout, matching the Raft paper.
/// `entries[0]` holds the entry with `index == 1`.
pub struct RaftLog {
    entries: Vec<LogEntry>,
    wal: Wal,
}

impl RaftLog {
    /// Open the log, replaying the WAL to rebuild in-memory state.
    pub fn open(wal_config: WalConfig) -> Result<Self> {
        let wal = Wal::open(wal_config)?;
        let all = wal.read_from(1)?;

        let mut entries: Vec<LogEntry> = Vec::new();

        for wal_entry in all {
            let payload = &wal_entry.data;
            if payload.is_empty() {
                continue;
            }
            match payload[0] {
                KIND_APPEND => {
                    if let Some(e) = decode_append(&payload[1..]) {
                        // Overwrite if we already have this index (leader change)
                        let idx = e.index;
                        if idx as usize <= entries.len() {
                            entries[(idx - 1) as usize] = e;
                        } else {
                            entries.push(e);
                        }
                    }
                }
                KIND_TRUNCATE => {
                    if payload.len() >= 9 {
                        let from = u64::from_le_bytes(payload[1..9].try_into().unwrap());
                        if from > 0 {
                            entries.truncate((from - 1) as usize);
                        }
                    }
                }
                _ => {} // unknown kind — skip
            }
        }

        Ok(Self { entries, wal })
    }

    // ── Write path ────────────────────────────────────────────────────────────

    /// Append a new entry at the end of the log. Returns the assigned index.
    pub fn append(&mut self, term: u64, data: &[u8]) -> Result<u64> {
        let index = self.last_index() + 1;
        let entry = LogEntry { term, index, data: data.to_vec() };
        let payload = encode_append(&entry);
        self.wal.append(&payload)?;
        self.entries.push(entry);
        Ok(index)
    }

    /// Remove all entries with `index >= from_index`. Crash-safe.
    pub fn truncate_from(&mut self, from_index: u64) -> Result<()> {
        if from_index == 0 || from_index > self.last_index() {
            return Ok(());
        }
        let payload = encode_truncate(from_index);
        self.wal.append(&payload)?;
        self.entries.truncate((from_index - 1) as usize);
        Ok(())
    }

    // ── Read path ─────────────────────────────────────────────────────────────

    pub fn last_index(&self) -> u64 {
        self.entries.len() as u64
    }

    pub fn last_term(&self) -> u64 {
        self.entries.last().map(|e| e.term).unwrap_or(0)
    }

    /// Term of the entry at `index`, or `None` if the index is out of range.
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 || index > self.last_index() {
            return None;
        }
        Some(self.entries[(index - 1) as usize].term)
    }

    /// Return a slice of entries starting at `from_index` (inclusive).
    pub fn entries_from(&self, from_index: u64) -> Vec<LogEntry> {
        if from_index == 0 || from_index > self.last_index() {
            return vec![];
        }
        self.entries[(from_index - 1) as usize..].to_vec()
    }

    /// Return the first index whose entry has `term == t`, or `None`.
    pub fn first_index_of_term(&self, term: u64) -> Option<u64> {
        self.entries.iter().find(|e| e.term == term).map(|e| e.index)
    }
}

// ── Encoding helpers ──────────────────────────────────────────────────────────

fn encode_append(e: &LogEntry) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 8 + 8 + e.data.len());
    buf.push(KIND_APPEND);
    buf.extend_from_slice(&e.index.to_le_bytes());
    buf.extend_from_slice(&e.term.to_le_bytes());
    buf.extend_from_slice(&e.data);
    buf
}

fn decode_append(payload: &[u8]) -> Option<LogEntry> {
    if payload.len() < 16 {
        return None;
    }
    let index = u64::from_le_bytes(payload[0..8].try_into().ok()?);
    let term = u64::from_le_bytes(payload[8..16].try_into().ok()?);
    let data = payload[16..].to_vec();
    Some(LogEntry { term, index, data })
}

fn encode_truncate(from_index: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(9);
    buf.push(KIND_TRUNCATE);
    buf.extend_from_slice(&from_index.to_le_bytes());
    buf
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn open_log(dir: &TempDir) -> RaftLog {
        RaftLog::open(WalConfig {
            dir: dir.path().to_path_buf(),
            max_segment_bytes: 1024 * 1024,
            sync_writes: false,
        })
        .unwrap()
    }

    #[test]
    fn append_assigns_sequential_indices() {
        let dir = TempDir::new().unwrap();
        let mut log = open_log(&dir);
        assert_eq!(log.append(1, b"a").unwrap(), 1);
        assert_eq!(log.append(1, b"b").unwrap(), 2);
        assert_eq!(log.append(2, b"c").unwrap(), 3);
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
    }

    #[test]
    fn term_at_returns_correct_term() {
        let dir = TempDir::new().unwrap();
        let mut log = open_log(&dir);
        log.append(1, b"entry in term 1").unwrap();
        log.append(2, b"entry in term 2").unwrap();
        assert_eq!(log.term_at(1), Some(1));
        assert_eq!(log.term_at(2), Some(2));
        assert_eq!(log.term_at(3), None);
    }

    #[test]
    fn truncate_removes_trailing_entries() {
        let dir = TempDir::new().unwrap();
        let mut log = open_log(&dir);
        for t in 1u64..=5 {
            log.append(1, format!("e{t}").as_bytes()).unwrap();
        }
        log.truncate_from(3).unwrap();
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.entries_from(1).len(), 2);
    }

    #[test]
    fn survives_crash_and_recovery() {
        let dir = TempDir::new().unwrap();
        {
            let mut log = open_log(&dir);
            log.append(1, b"x").unwrap();
            log.append(1, b"y").unwrap();
            log.append(2, b"z").unwrap();
        }
        let log = open_log(&dir);
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.term_at(3), Some(2));
        assert_eq!(log.entries_from(2)[0].data, b"y");
    }

    #[test]
    fn truncation_survives_crash() {
        let dir = TempDir::new().unwrap();
        {
            let mut log = open_log(&dir);
            log.append(1, b"keep").unwrap();
            log.append(1, b"discard").unwrap();
            log.truncate_from(2).unwrap();
        }
        let log = open_log(&dir);
        assert_eq!(log.last_index(), 1);
        assert_eq!(log.entries_from(1)[0].data, b"keep");
    }
}
