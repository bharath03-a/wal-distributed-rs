//! The replicated Raft log.
//!
//! Each log entry carries a `term` (the leader's term when it was appended)
//! plus the application payload.  The term is what lets Raft detect and
//! resolve divergence between nodes after a leader change.
//!
//! # Persistence
//!
//! Entries are kept in an in-memory `Vec` for fast indexed access.
//! Every mutation (append / truncate / compact) is also written to the
//! underlying `wal-core` WAL so the log survives crashes.
//!
//! ## WAL payload encoding
//!
//! | kind | payload |
//! |------|---------|
//! | `0x01` APPEND   | `[index: u64 LE][term: u64 LE][data: bytes]` |
//! | `0x02` TRUNCATE | `[from_index: u64 LE]` |
//! | `0x03` SNAPSHOT | `[last_index: u64 LE][last_term: u64 LE]` |
//!
//! On recovery we replay the WAL and apply each record.  A SNAPSHOT record
//! marks that all entries before `last_index` have been compacted away; the
//! in-memory log restarts from that point.

use wal_core::{Wal, WalConfig};

use crate::error::Result;

// ── On-disk record kinds ─────────────────────────────────────────────────────

const KIND_APPEND: u8 = 0x01;
const KIND_TRUNCATE: u8 = 0x02;
const KIND_SNAPSHOT: u8 = 0x03;

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
///
/// After a snapshot is taken at `snapshot_index`, entries before that point
/// are discarded.  `entries[0]` holds the entry with
/// `index == snapshot_index + 1`.
pub struct RaftLog {
    /// In-memory log entries *after* the snapshot point.
    entries: Vec<LogEntry>,
    wal: Wal,
    /// Index of the last entry included in the most recent snapshot.
    /// 0 = no snapshot taken yet.
    snapshot_index: u64,
    /// Term of the entry at `snapshot_index`.
    snapshot_term: u64,
}

impl RaftLog {
    /// Open the log, replaying the WAL to rebuild in-memory state.
    pub fn open(wal_config: WalConfig) -> Result<Self> {
        let wal = Wal::open(wal_config)?;
        let all = wal.read_from(1)?;

        let mut entries: Vec<LogEntry> = Vec::new();
        let mut snapshot_index: u64 = 0;
        let mut snapshot_term: u64 = 0;

        for wal_entry in all {
            let payload = &wal_entry.data;
            if payload.is_empty() {
                continue;
            }
            match payload[0] {
                KIND_APPEND => {
                    if let Some(e) = decode_append(&payload[1..]) {
                        let idx = e.index;
                        // The entry's offset relative to snapshot_index
                        let offset = idx.saturating_sub(snapshot_index + 1) as usize;
                        if idx > snapshot_index {
                            if offset < entries.len() {
                                entries[offset] = e;
                            } else {
                                entries.push(e);
                            }
                        }
                        // Entries at or before snapshot_index are ignored
                    }
                }
                KIND_TRUNCATE => {
                    if payload.len() >= 9 {
                        let from = u64::from_le_bytes(payload[1..9].try_into().unwrap());
                        if from > snapshot_index + 1 {
                            let trim_to = (from - snapshot_index - 1) as usize;
                            entries.truncate(trim_to);
                        } else if from == snapshot_index + 1 {
                            entries.clear();
                        }
                    }
                }
                KIND_SNAPSHOT => {
                    if payload.len() >= 17 {
                        snapshot_index = u64::from_le_bytes(payload[1..9].try_into().unwrap());
                        snapshot_term = u64::from_le_bytes(payload[9..17].try_into().unwrap());
                        // All in-memory entries before or at snapshot_index are invalid
                        entries.retain(|e| e.index > snapshot_index);
                    }
                }
                _ => {} // unknown kind — skip
            }
        }

        Ok(Self { entries, wal, snapshot_index, snapshot_term })
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
        if from_index <= self.snapshot_index || from_index > self.last_index() {
            return Ok(());
        }
        let payload = encode_truncate(from_index);
        self.wal.append(&payload)?;
        let trim_to = (from_index - self.snapshot_index - 1) as usize;
        self.entries.truncate(trim_to);
        Ok(())
    }

    /// Compact the log up to and including `last_included_index`.
    ///
    /// Writes a SNAPSHOT marker to the WAL, checkpoints it, truncates old
    /// WAL segments, and removes compacted entries from memory.
    pub fn compact(&mut self, last_included_index: u64, last_included_term: u64) -> Result<()> {
        if last_included_index <= self.snapshot_index {
            return Ok(()); // already compacted past this point
        }

        let payload = encode_snapshot(last_included_index, last_included_term);
        let snap_seq = self.wal.append(&payload)?;

        // Checkpoint at this sequence and free old WAL segments
        self.wal.checkpoint(snap_seq)?;
        self.wal.truncate_before(snap_seq)?;

        // Drop in-memory entries that are now in the snapshot
        let trim = (last_included_index.min(self.last_index()) - self.snapshot_index) as usize;
        if trim >= self.entries.len() {
            self.entries.clear();
        } else {
            self.entries.drain(0..trim);
        }

        self.snapshot_index = last_included_index;
        self.snapshot_term = last_included_term;

        Ok(())
    }

    /// Install a snapshot received from the leader.
    ///
    /// Resets the log to start after `last_included_index`.  Entries the
    /// follower already had *after* `last_included_index` are preserved.
    pub fn install_snapshot(
        &mut self,
        last_included_index: u64,
        last_included_term: u64,
    ) -> Result<()> {
        if last_included_index <= self.snapshot_index {
            return Ok(()); // we're already ahead of this snapshot
        }

        let payload = encode_snapshot(last_included_index, last_included_term);
        let snap_seq = self.wal.append(&payload)?;
        self.wal.checkpoint(snap_seq)?;
        self.wal.truncate_before(snap_seq)?;

        // Keep any entries we have that come after the snapshot
        self.entries.retain(|e| e.index > last_included_index);

        self.snapshot_index = last_included_index;
        self.snapshot_term = last_included_term;

        Ok(())
    }

    // ── Read path ─────────────────────────────────────────────────────────────

    pub fn last_index(&self) -> u64 {
        self.snapshot_index + self.entries.len() as u64
    }

    pub fn last_term(&self) -> u64 {
        self.entries.last().map(|e| e.term).unwrap_or(self.snapshot_term)
    }

    /// Term of the entry at `index`, or `None` if out of range / compacted.
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return None;
        }
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        if index < self.snapshot_index {
            return None; // compacted
        }
        let offset = (index - self.snapshot_index - 1) as usize;
        self.entries.get(offset).map(|e| e.term)
    }

    /// Return a slice of entries starting at `from_index` (inclusive).
    ///
    /// Returns an empty vec if all entries at or after `from_index` have
    /// been compacted into a snapshot.
    pub fn entries_from(&self, from_index: u64) -> Vec<LogEntry> {
        let start = from_index.max(self.snapshot_index + 1);
        if start == 0 || start > self.last_index() {
            return vec![];
        }
        let offset = (start - self.snapshot_index - 1) as usize;
        self.entries[offset..].to_vec()
    }

    /// Return the first index whose entry has `term == t`, or `None`.
    pub fn first_index_of_term(&self, term: u64) -> Option<u64> {
        if term == self.snapshot_term && self.snapshot_index > 0 {
            return Some(self.snapshot_index);
        }
        self.entries.iter().find(|e| e.term == term).map(|e| e.index)
    }

    /// All entries currently in memory (after the snapshot point).
    pub fn all_entries(&self) -> &[LogEntry] {
        &self.entries
    }

    /// Index of the last entry included in the current snapshot (0 = none).
    pub fn snapshot_index(&self) -> u64 {
        self.snapshot_index
    }

    /// Term of the entry at `snapshot_index`.
    pub fn snapshot_term(&self) -> u64 {
        self.snapshot_term
    }
}

// ── Encoding helpers ──────────────────────────────────────────────────────────

pub(crate) fn encode_append(e: &LogEntry) -> Vec<u8> {
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

fn encode_snapshot(last_index: u64, last_term: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(17);
    buf.push(KIND_SNAPSHOT);
    buf.extend_from_slice(&last_index.to_le_bytes());
    buf.extend_from_slice(&last_term.to_le_bytes());
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

    #[test]
    fn compact_removes_entries_and_updates_snapshot_index() {
        let dir = TempDir::new().unwrap();
        let mut log = open_log(&dir);
        for i in 1u64..=5 {
            log.append(1, format!("e{i}").as_bytes()).unwrap();
        }
        // Compact up to index 3
        log.compact(3, 1).unwrap();
        assert_eq!(log.snapshot_index(), 3);
        assert_eq!(log.snapshot_term(), 1);
        assert_eq!(log.last_index(), 5);
        // Entries 1–3 are gone
        assert!(log.entries_from(1).is_empty() || log.entries_from(1)[0].index > 3);
        // Entry 4 and 5 remain
        let tail = log.entries_from(4);
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].index, 4);
    }

    #[test]
    fn compact_survives_crash() {
        let dir = TempDir::new().unwrap();
        {
            let mut log = open_log(&dir);
            for i in 1u64..=5 {
                log.append(1, format!("e{i}").as_bytes()).unwrap();
            }
            log.compact(3, 1).unwrap();
        }
        let log = open_log(&dir);
        assert_eq!(log.snapshot_index(), 3);
        assert_eq!(log.last_index(), 5);
        let tail = log.entries_from(4);
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[1].data, b"e5");
    }

    #[test]
    fn install_snapshot_resets_log() {
        let dir = TempDir::new().unwrap();
        let mut log = open_log(&dir);
        for i in 1u64..=3 {
            log.append(1, format!("e{i}").as_bytes()).unwrap();
        }
        log.install_snapshot(5, 2).unwrap();
        assert_eq!(log.snapshot_index(), 5);
        assert_eq!(log.last_index(), 5);
        assert_eq!(log.last_term(), 2);
        // No entries remain
        assert!(log.entries_from(1).is_empty());
    }

    #[test]
    fn term_at_snapshot_index_returns_snapshot_term() {
        let dir = TempDir::new().unwrap();
        let mut log = open_log(&dir);
        log.append(1, b"a").unwrap();
        log.append(1, b"b").unwrap();
        log.compact(2, 1).unwrap();
        assert_eq!(log.term_at(2), Some(1)); // snapshot_term
        assert_eq!(log.term_at(1), None);    // compacted
    }
}
