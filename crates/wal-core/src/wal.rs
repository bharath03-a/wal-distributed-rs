//! Write-Ahead Log — the main public API.
//!
//! # Concepts
//!
//! **Sequence number (LSN)**: Every appended entry receives a unique,
//! monotonically increasing 64-bit integer starting at 1. This is the log's
//! primary addressing mechanism.
//!
//! **Segment**: Entries are stored in one or more segment files on disk. The
//! *active* segment accepts new writes; once it reaches `max_segment_bytes`
//! the WAL rotates to a fresh segment. Previous segments become *sealed*
//! (read-only).
//!
//! **Checkpoint**: A durably-persisted sequence number marking the highest
//! entry that has been applied to stable state. [`Wal::recover`] replays
//! everything *after* the checkpoint so that partially-applied entries can be
//! re-executed after a crash.
//!
//! # Example
//!
//! ```no_run
//! use wal_core::{Wal, WalConfig};
//!
//! let config = WalConfig::new("/tmp/my-wal");
//! let mut wal = Wal::open(config).unwrap();
//!
//! // Write some entries
//! let seq = wal.append(b"begin transaction").unwrap();
//! wal.append(b"update row 42").unwrap();
//! wal.append(b"commit").unwrap();
//!
//! // Mark everything up to `seq` as safely applied
//! wal.checkpoint(seq).unwrap();
//!
//! // On the next process start, recover() will only replay the un-checkpointed tail
//! let pending = wal.recover().unwrap();
//! ```

use std::path::{Path, PathBuf};

use crate::{
    checkpoint::Checkpoint,
    entry::Entry,
    error::Result,
    segment::{self, Segment, DEFAULT_MAX_SEGMENT_BYTES},
};

// Metric names — documented here so users know what to scrape.
//
//  wal_entries_appended_total  counter  Total entries written to this WAL.
//  wal_bytes_appended_total    counter  Total payload bytes written.
//  wal_segment_rotations_total counter  Number of segment rotations.
//  wal_active_segment_bytes    gauge    Current size of the active segment.
//
// These are no-ops unless a `metrics` recorder is installed by the binary
// (e.g. via `metrics-exporter-prometheus`).
const METRIC_ENTRIES: &str = "wal_entries_appended_total";
const METRIC_BYTES: &str = "wal_bytes_appended_total";
const METRIC_ROTATIONS: &str = "wal_segment_rotations_total";
const METRIC_SEG_BYTES: &str = "wal_active_segment_bytes";

// ─── Configuration ───────────────────────────────────────────────────────────

/// Configuration for a [`Wal`] instance.
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Directory that will hold segment files and the checkpoint file.
    pub dir: PathBuf,
    /// Hard cap on individual segment file size in bytes.
    /// Defaults to 64 MiB ([`DEFAULT_MAX_SEGMENT_BYTES`]).
    pub max_segment_bytes: u64,
    /// When `true`, every [`Wal::append`] calls `fsync` before returning.
    /// Set to `false` for higher throughput (e.g., in benchmarks) at the
    /// cost of potential data loss on unexpected power loss.
    pub sync_writes: bool,
}

impl WalConfig {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            max_segment_bytes: DEFAULT_MAX_SEGMENT_BYTES,
            sync_writes: true,
        }
    }
}

// ─── Wal ─────────────────────────────────────────────────────────────────────

/// A crash-safe, segment-based Write-Ahead Log.
///
/// Open with [`Wal::open`]; it automatically recovers from an interrupted
/// previous run.
pub struct Wal {
    config: WalConfig,
    /// The segment currently accepting writes.
    active: Segment,
    /// IDs of older, read-only segments (ascending order).
    sealed: Vec<u64>,
    checkpoint: Checkpoint,
    /// Sequence number to assign to the *next* append.
    next_sequence: u64,
}

impl Wal {
    /// Open (or create) a WAL rooted at the directory in `config`.
    ///
    /// - If the directory is empty a fresh WAL is created starting at
    ///   sequence 1.
    /// - If segment files already exist the WAL resumes from where it left
    ///   off — recovering the last written sequence so new appends continue
    ///   the sequence without gaps or reuse.
    pub fn open(config: WalConfig) -> Result<Self> {
        std::fs::create_dir_all(&config.dir)?;

        let checkpoint = Checkpoint::open(&config.dir)?;
        let existing = segment::list_segments(&config.dir)?;

        let (active, sealed, next_sequence) = if existing.is_empty() {
            // ── Brand new WAL ──
            let seg = Segment::create(&config.dir, 1, config.max_segment_bytes)?;
            (seg, Vec::new(), 1u64)
        } else {
            // ── Recovery: re-open the last segment for appending ──
            let sealed_ids: Vec<u64> = existing[..existing.len() - 1]
                .iter()
                .map(|p| segment::segment_id_from_path(p).unwrap())
                .collect();

            let active_path = existing.last().unwrap().clone();
            let active_seg =
                Segment::open_for_append(active_path.clone(), config.max_segment_bytes)?;

            // Determine next_sequence from the highest LSN on disk
            let next_seq = highest_sequence_on_disk(&active_path, &sealed_ids, &config.dir)?
                .map(|s| s + 1)
                .unwrap_or(1);

            (active_seg, sealed_ids, next_seq)
        };

        Ok(Self {
            config,
            active,
            sealed,
            checkpoint,
            next_sequence,
        })
    }

    /// Append raw bytes to the WAL and return the assigned sequence number.
    ///
    /// If `sync_writes` is enabled the data is fsynced before returning,
    /// guaranteeing durability.  The WAL rotates to a fresh segment
    /// automatically when needed.
    pub fn append(&mut self, data: &[u8]) -> Result<u64> {
        let seq = self.next_sequence;
        let encoded = crate::entry::encode(seq, data);

        if self.active.would_overflow(encoded.len()) {
            self.rotate()?;
        }

        self.active.append(&encoded)?;

        if self.config.sync_writes {
            self.active.sync()?;
        } else {
            self.active.flush()?;
        }

        // Emit metrics (no-ops when no recorder is installed)
        metrics::counter!(METRIC_ENTRIES).increment(1);
        metrics::counter!(METRIC_BYTES).increment(data.len() as u64);
        metrics::gauge!(METRIC_SEG_BYTES).set(self.active.size() as f64);

        self.next_sequence = seq + 1;
        Ok(seq)
    }

    /// Return all entries whose sequence number is `>= from_sequence`.
    ///
    /// Both sealed and active segments are scanned in order.
    pub fn read_from(&self, from_sequence: u64) -> Result<Vec<Entry>> {
        let mut result = Vec::new();

        for &sid in &self.sealed {
            let path = segment::segment_path(&self.config.dir, sid);
            for entry in segment::read_entries(&path)? {
                if entry.sequence >= from_sequence {
                    result.push(entry);
                }
            }
        }

        for entry in segment::read_entries(self.active.path())? {
            if entry.sequence >= from_sequence {
                result.push(entry);
            }
        }

        Ok(result)
    }

    /// Durably record `sequence` as the checkpoint.
    ///
    /// All entries with `sequence <= checkpoint` are now candidates for
    /// truncation via [`Wal::truncate_before`].
    pub fn checkpoint(&mut self, sequence: u64) -> Result<()> {
        self.checkpoint.advance(sequence)
    }

    /// The current checkpoint sequence (0 if none has been set).
    pub fn checkpoint_sequence(&self) -> u64 {
        self.checkpoint.sequence()
    }

    /// Return all entries that need to be re-applied after a crash.
    ///
    /// Specifically, returns every entry with `sequence > checkpoint`.
    /// Call this immediately after [`Wal::open`] to replay any work that was
    /// in-flight when the process last terminated.
    pub fn recover(&self) -> Result<Vec<Entry>> {
        self.read_from(self.checkpoint.sequence() + 1)
    }

    /// Delete sealed segments whose highest sequence is `< before_sequence`.
    ///
    /// The active segment is never touched.  This is how you reclaim disk
    /// space after a successful checkpoint.
    pub fn truncate_before(&mut self, before_sequence: u64) -> Result<()> {
        let dir = self.config.dir.clone();
        let mut remaining = Vec::new();

        for &sid in &self.sealed {
            let path = segment::segment_path(&dir, sid);
            let entries = segment::read_entries(&path)?;
            let max_seq = entries.iter().map(|e| e.sequence).max().unwrap_or(0);

            if max_seq < before_sequence {
                std::fs::remove_file(&path)?;
            } else {
                remaining.push(sid);
            }
        }

        self.sealed = remaining;
        Ok(())
    }

    /// Force an fsync of the active segment regardless of `sync_writes`.
    pub fn sync(&mut self) -> Result<()> {
        self.active.sync()
    }

    /// The sequence number that will be assigned to the next append.
    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /// Seal the active segment and open a fresh one.
    fn rotate(&mut self) -> Result<()> {
        self.active.sync()?;
        let old_id = self.active.id();
        self.sealed.push(old_id);

        let new_id = old_id + 1;
        let new_seg = Segment::create(&self.config.dir, new_id, self.config.max_segment_bytes)?;
        self.active = new_seg;

        metrics::counter!(METRIC_ROTATIONS).increment(1);
        metrics::gauge!(METRIC_SEG_BYTES).set(0.0);

        Ok(())
    }
}

// ── Free functions ────────────────────────────────────────────────────────────

/// Find the highest sequence number stored across all segments on disk.
///
/// Only the active (last) segment is fully scanned; if it is empty we fall
/// back to the most recent sealed segment.
fn highest_sequence_on_disk(
    active_path: &Path,
    sealed_ids: &[u64],
    dir: &Path,
) -> Result<Option<u64>> {
    // Try the active segment first (most recent entries live here)
    let active_entries = segment::read_entries(active_path)?;
    if let Some(last) = active_entries.last() {
        return Ok(Some(last.sequence));
    }

    // Active is empty — fall back to the most recent sealed segment
    for &sid in sealed_ids.iter().rev() {
        let path = segment::segment_path(dir, sid);
        let entries = segment::read_entries(&path)?;
        if let Some(last) = entries.last() {
            return Ok(Some(last.sequence));
        }
    }

    Ok(None)
}
