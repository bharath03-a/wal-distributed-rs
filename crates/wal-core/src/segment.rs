//! Segment file management.
//!
//! A segment is an append-only file that stores a contiguous range of WAL
//! entries. When a segment reaches its configured maximum size, the WAL
//! *rotates* — the current segment becomes read-only ("sealed") and a new
//! active segment is created.
//!
//! # File naming
//!
//! Segments are named `wal-{id:020}.seg`, e.g.:
//! ```text
//! wal-00000000000000000001.seg   (first segment)
//! wal-00000000000000000002.seg   (after first rotation)
//! ```
//! The zero-padded 20-digit ID keeps lexicographic and numeric order aligned.

use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use crate::{
    entry::{self, Entry},
    error::{Result, WalError},
};

/// Default maximum segment size: 64 MiB.
pub const DEFAULT_MAX_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

const PREFIX: &str = "wal-";
const SUFFIX: &str = ".seg";

// ─── Segment ────────────────────────────────────────────────────────────────

/// An append-only segment file.
///
/// Internally wraps a `BufWriter<File>` so that small appends are coalesced
/// before hitting the OS.  Call [`Segment::sync`] (or [`Segment::flush`]) to
/// ensure data is visible to subsequent readers.
pub struct Segment {
    id: u64,
    path: PathBuf,
    writer: BufWriter<File>,
    size: u64,
    max_bytes: u64,
}

impl Segment {
    /// Create a new, empty segment file. Fails if the file already exists.
    pub fn create(dir: &Path, id: u64, max_bytes: u64) -> Result<Self> {
        let path = segment_path(dir, id);
        let file = OpenOptions::new()
            .write(true)
            .create_new(true) // atomic: fail if file exists
            .open(&path)?;
        Ok(Self {
            id,
            path,
            writer: BufWriter::new(file),
            size: 0,
            max_bytes,
        })
    }

    /// Open an existing segment for appending.
    pub fn open_for_append(path: PathBuf, max_bytes: u64) -> Result<Self> {
        let id = segment_id_from_path(&path)?;
        let size = path.metadata()?.len();
        let file = OpenOptions::new().append(true).open(&path)?;
        Ok(Self {
            id,
            path,
            writer: BufWriter::new(file),
            size,
            max_bytes,
        })
    }

    /// Append a pre-encoded entry buffer.
    ///
    /// Returns `Err(SegmentFull)` when the write would push `size` past
    /// `max_bytes`. The caller (the WAL) is responsible for rotating first.
    pub fn append(&mut self, encoded: &[u8]) -> Result<()> {
        let new_size = self.size + encoded.len() as u64;
        if new_size > self.max_bytes {
            return Err(WalError::SegmentFull {
                size: self.size,
                max: self.max_bytes,
                entry: encoded.len() as u64,
            });
        }
        self.writer.write_all(encoded)?;
        self.size = new_size;
        Ok(())
    }

    /// Flush the `BufWriter` buffer **and** call `fsync`.
    ///
    /// After this returns `Ok(())` the data is durable on the storage device.
    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    /// Flush the `BufWriter` buffer to the kernel (no `fsync`).
    ///
    /// After this call the data is in the OS page cache and will be visible
    /// to new read handles on the same file.
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush().map_err(Into::into)
    }

    /// `true` when further appends would exceed the size cap.
    pub fn would_overflow(&self, encoded_len: usize) -> bool {
        self.size + encoded_len as u64 > self.max_bytes
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

// ─── Standalone read helper ──────────────────────────────────────────────────

/// Read all entries from a segment file at `path`.
///
/// Opens a separate read-only file handle so this can be called while the
/// same segment is also open for writing.  Checksum validation is performed
/// on every entry.
pub fn read_entries(path: &Path) -> Result<Vec<Entry>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    let mut raw = Vec::new();
    reader.read_to_end(&mut raw)?;

    let mut entries = Vec::new();
    let mut offset = 0usize;

    while offset < raw.len() {
        let (entry, consumed) = entry::decode(&raw[offset..], offset)?;
        entries.push(entry);
        offset += consumed;
    }

    Ok(entries)
}

// ─── Path helpers ────────────────────────────────────────────────────────────

/// Build the canonical file path for segment `id` inside `dir`.
pub fn segment_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{}{:020}{}", PREFIX, id, SUFFIX))
}

/// Parse the numeric segment ID from a path like `wal-00000000000000000001.seg`.
pub fn segment_id_from_path(path: &Path) -> Result<u64> {
    let filename = path.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
        WalError::InvalidSegmentFilename {
            filename: path.display().to_string(),
        }
    })?;

    let numeric = filename
        .strip_prefix(PREFIX)
        .and_then(|s| s.strip_suffix(SUFFIX))
        .ok_or_else(|| WalError::InvalidSegmentFilename {
            filename: filename.to_owned(),
        })?;

    numeric
        .parse::<u64>()
        .map_err(|_| WalError::InvalidSegmentFilename {
            filename: filename.to_owned(),
        })
}

/// Return all segment paths in `dir`, sorted ascending by segment ID.
pub fn list_segments(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(|r| r.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension().and_then(|e| e.to_str()) == Some("seg")
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with(PREFIX))
                    .unwrap_or(false)
        })
        .collect();

    paths.sort_by_key(|p| segment_id_from_path(p).unwrap_or(u64::MAX));
    Ok(paths)
}

// ─── Unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry;
    use tempfile::TempDir;

    fn tmp() -> TempDir {
        tempfile::tempdir().unwrap()
    }

    #[test]
    fn create_segment_is_empty() {
        let dir = tmp();
        let seg = Segment::create(dir.path(), 1, DEFAULT_MAX_SEGMENT_BYTES).unwrap();
        assert_eq!(seg.id(), 1);
        assert_eq!(seg.size(), 0);
        assert!(seg.path().exists());
    }

    #[test]
    fn append_increases_size() {
        let dir = tmp();
        let mut seg = Segment::create(dir.path(), 1, DEFAULT_MAX_SEGMENT_BYTES).unwrap();
        let encoded = entry::encode(1, b"hello");
        seg.append(&encoded).unwrap();
        assert_eq!(seg.size() as usize, encoded.len());
    }

    #[test]
    fn read_entries_roundtrip() {
        let dir = tmp();
        let mut seg = Segment::create(dir.path(), 1, DEFAULT_MAX_SEGMENT_BYTES).unwrap();
        for i in 1u64..=5 {
            seg.append(&entry::encode(i, format!("entry {i}").as_bytes()))
                .unwrap();
        }
        seg.flush().unwrap();

        let entries = read_entries(seg.path()).unwrap();
        assert_eq!(entries.len(), 5);
        for (i, e) in entries.iter().enumerate() {
            assert_eq!(e.sequence, (i + 1) as u64);
            assert_eq!(e.data, format!("entry {}", i + 1).as_bytes());
        }
    }

    #[test]
    fn segment_full_error() {
        let dir = tmp();
        // Tiny cap: smaller than a single encoded entry
        let mut seg = Segment::create(dir.path(), 1, 10).unwrap();
        let encoded = entry::encode(1, b"this is way too big");
        let result = seg.append(&encoded);
        assert!(matches!(result, Err(WalError::SegmentFull { .. })));
    }

    #[test]
    fn segment_path_roundtrip() {
        let dir = PathBuf::from("/tmp");
        let path = segment_path(&dir, 999);
        assert_eq!(segment_id_from_path(&path).unwrap(), 999);
    }

    #[test]
    fn list_segments_returns_sorted_order() {
        let dir = tmp();
        for id in [3u64, 1, 5, 2, 4] {
            Segment::create(dir.path(), id, DEFAULT_MAX_SEGMENT_BYTES).unwrap();
        }
        let paths = list_segments(dir.path()).unwrap();
        let ids: Vec<u64> = paths
            .iter()
            .map(|p| segment_id_from_path(p).unwrap())
            .collect();
        assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn invalid_filename_returns_error() {
        let result = segment_id_from_path(Path::new("/tmp/notasegment.txt"));
        assert!(matches!(
            result,
            Err(WalError::InvalidSegmentFilename { .. })
        ));
    }
}
