//! Snapshot persistence for Raft log compaction (§7).
//!
//! A snapshot captures the state of the replicated log up to
//! `last_included_index`, allowing earlier WAL segments to be deleted.
//!
//! # File format
//!
//! ```text
//! ┌──────────────────────┬───────────────────────┬──────────────────┬──────────────────┐
//! │ last_index (u64 LE)  │  last_term (u64 LE)   │  data_len (u32)  │  data (N bytes)  │
//! └──────────────────────┴───────────────────────┴──────────────────┴──────────────────┘
//! ```
//!
//! Written atomically: data goes to `snapshot.tmp`, then renamed to `snapshot`.

use std::{
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

/// A point-in-time snapshot of the replicated log.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Index of the last log entry included in this snapshot.
    pub last_included_index: u64,
    /// Term of the last log entry included in this snapshot.
    pub last_included_term: u64,
    /// Serialised application state — for this WAL service, the committed
    /// log entries encoded as `[count: u32][index: u64, term: u64, len: u32, data: bytes...]`.
    pub data: Vec<u8>,
}

impl Snapshot {
    /// Persist the snapshot atomically.
    ///
    /// Writes to `<dir>/snapshot.tmp` then renames to `<dir>/snapshot`
    /// so a crash during the write never leaves a partial file.
    pub fn save(&self, dir: &Path) -> io::Result<()> {
        let tmp = dir.join("snapshot.tmp");
        let dest = snapshot_path(dir);

        let mut buf: Vec<u8> = Vec::with_capacity(20 + self.data.len());
        buf.extend_from_slice(&self.last_included_index.to_le_bytes());
        buf.extend_from_slice(&self.last_included_term.to_le_bytes());
        buf.extend_from_slice(&(self.data.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.data);

        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(&buf)?;
        f.sync_all()?;
        drop(f);

        std::fs::rename(&tmp, &dest)
    }

    /// Load the snapshot from `<dir>/snapshot`, returning `None` if it does
    /// not exist yet.
    pub fn load(dir: &Path) -> io::Result<Option<Self>> {
        let path = snapshot_path(dir);
        match std::fs::File::open(&path) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
            Ok(mut f) => {
                let mut buf = Vec::new();
                f.read_to_end(&mut buf)?;

                if buf.len() < 20 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "snapshot file too short",
                    ));
                }

                let last_included_index = u64::from_le_bytes(buf[0..8].try_into().unwrap());
                let last_included_term = u64::from_le_bytes(buf[8..16].try_into().unwrap());
                let data_len = u32::from_le_bytes(buf[16..20].try_into().unwrap()) as usize;

                if buf.len() < 20 + data_len {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "snapshot data truncated",
                    ));
                }

                let data = buf[20..20 + data_len].to_vec();
                Ok(Some(Self {
                    last_included_index,
                    last_included_term,
                    data,
                }))
            }
        }
    }
}

fn snapshot_path(dir: &Path) -> PathBuf {
    dir.join("snapshot")
}

/// Serialise a list of log entries into snapshot payload bytes.
pub fn encode_entries(entries: &[crate::raft::log::LogEntry]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for e in entries {
        buf.extend_from_slice(&e.index.to_le_bytes());
        buf.extend_from_slice(&e.term.to_le_bytes());
        buf.extend_from_slice(&(e.data.len() as u32).to_le_bytes());
        buf.extend_from_slice(&e.data);
    }
    buf
}

/// Deserialise snapshot payload bytes back into log entries.
pub fn decode_entries(data: &[u8]) -> Option<Vec<crate::raft::log::LogEntry>> {
    use crate::raft::log::LogEntry;

    if data.len() < 4 {
        return Some(vec![]);
    }
    let count = u32::from_le_bytes(data[0..4].try_into().ok()?) as usize;
    let mut entries = Vec::with_capacity(count);
    let mut pos = 4;

    for _ in 0..count {
        if pos + 20 > data.len() {
            return None;
        }
        let index = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
        let term = u64::from_le_bytes(data[pos + 8..pos + 16].try_into().ok()?);
        let len = u32::from_le_bytes(data[pos + 16..pos + 20].try_into().ok()?) as usize;
        pos += 20;
        if pos + len > data.len() {
            return None;
        }
        let entry_data = data[pos..pos + len].to_vec();
        pos += len;
        entries.push(LogEntry {
            index,
            term,
            data: entry_data,
        });
    }

    Some(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::log::LogEntry;
    use tempfile::TempDir;

    #[test]
    fn save_and_load_roundtrip() {
        let dir = TempDir::new().unwrap();
        let snap = Snapshot {
            last_included_index: 42,
            last_included_term: 3,
            data: b"hello snapshot".to_vec(),
        };
        snap.save(dir.path()).unwrap();
        let loaded = Snapshot::load(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.last_included_index, 42);
        assert_eq!(loaded.last_included_term, 3);
        assert_eq!(loaded.data, b"hello snapshot");
    }

    #[test]
    fn load_returns_none_when_missing() {
        let dir = TempDir::new().unwrap();
        assert!(Snapshot::load(dir.path()).unwrap().is_none());
    }

    #[test]
    fn encode_decode_entries_roundtrip() {
        let entries = vec![
            LogEntry {
                index: 1,
                term: 1,
                data: b"a".to_vec(),
            },
            LogEntry {
                index: 2,
                term: 1,
                data: b"bb".to_vec(),
            },
            LogEntry {
                index: 3,
                term: 2,
                data: b"ccc".to_vec(),
            },
        ];
        let bytes = encode_entries(&entries);
        let decoded = decode_entries(&bytes).unwrap();
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[2].data, b"ccc");
    }
}
