//! Integration tests for `wal-core`.
//!
//! These tests exercise the full stack — [`Wal::open`], append, read, checkpoint,
//! recovery, rotation, and truncation — using real temporary directories.

use tempfile::TempDir;
use wal_core::{Wal, WalConfig};

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Open a WAL with a small segment size (easy to trigger rotation in tests).
fn open_wal(dir: &TempDir) -> Wal {
    Wal::open(WalConfig {
        dir: dir.path().to_path_buf(),
        max_segment_bytes: 512,
        sync_writes: false,
    })
    .unwrap()
}

// ── Basic append & read ───────────────────────────────────────────────────────

#[test]
fn append_returns_monotonic_sequence_numbers() {
    let dir = TempDir::new().unwrap();
    let mut wal = open_wal(&dir);

    let seq1 = wal.append(b"first").unwrap();
    let seq2 = wal.append(b"second").unwrap();
    let seq3 = wal.append(b"third").unwrap();

    assert_eq!(seq1, 1);
    assert_eq!(seq2, 2);
    assert_eq!(seq3, 3);
}

#[test]
fn read_from_start_returns_all_entries() {
    let dir = TempDir::new().unwrap();
    let mut wal = open_wal(&dir);

    wal.append(b"alpha").unwrap();
    wal.append(b"beta").unwrap();
    wal.append(b"gamma").unwrap();

    let entries = wal.read_from(1).unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].data, b"alpha");
    assert_eq!(entries[1].data, b"beta");
    assert_eq!(entries[2].data, b"gamma");
}

#[test]
fn read_from_filters_earlier_sequences() {
    let dir = TempDir::new().unwrap();
    let mut wal = open_wal(&dir);

    for i in 1u64..=10 {
        wal.append(format!("entry {i}").as_bytes()).unwrap();
    }

    let entries = wal.read_from(6).unwrap();
    assert_eq!(entries.len(), 5);
    assert_eq!(entries[0].sequence, 6);
    assert_eq!(entries[4].sequence, 10);
}

// ── Segment rotation ──────────────────────────────────────────────────────────

#[test]
fn data_survives_rotation_across_multiple_segments() {
    let dir = TempDir::new().unwrap();
    // Very small cap to force many rotations
    let config = WalConfig {
        dir: dir.path().to_path_buf(),
        max_segment_bytes: 80,
        sync_writes: false,
    };
    let mut wal = Wal::open(config).unwrap();

    for i in 1u64..=30 {
        wal.append(format!("record-{i:04}").as_bytes()).unwrap();
    }

    let entries = wal.read_from(1).unwrap();
    assert_eq!(entries.len(), 30);
    for (i, e) in entries.iter().enumerate() {
        assert_eq!(e.sequence, (i + 1) as u64);
        assert_eq!(e.data, format!("record-{:04}", i + 1).as_bytes());
    }
}

// ── Checkpoint & recovery ─────────────────────────────────────────────────────

#[test]
fn recover_replays_only_entries_after_checkpoint() {
    let dir = TempDir::new().unwrap();
    let config = WalConfig {
        dir: dir.path().to_path_buf(),
        max_segment_bytes: 4096,
        sync_writes: true,
    };

    // --- First run: write 3 entries, checkpoint after 2 ---
    {
        let mut wal = Wal::open(config.clone()).unwrap();
        wal.append(b"tx-1 committed").unwrap();
        wal.append(b"tx-2 committed").unwrap();
        wal.checkpoint(2).unwrap();
        wal.append(b"tx-3 in-flight").unwrap(); // not checkpointed
                                                // Process "crashes" here (WAL is dropped)
    }

    // --- Recovery run ---
    {
        let wal = Wal::open(config).unwrap();
        let pending = wal.recover().unwrap();
        assert_eq!(pending.len(), 1, "only tx-3 should need re-applying");
        assert_eq!(pending[0].sequence, 3);
        assert_eq!(pending[0].data, b"tx-3 in-flight");
    }
}

#[test]
fn recover_returns_nothing_when_all_entries_are_checkpointed() {
    let dir = TempDir::new().unwrap();
    let config = WalConfig {
        dir: dir.path().to_path_buf(),
        max_segment_bytes: 4096,
        sync_writes: true,
    };

    {
        let mut wal = Wal::open(config.clone()).unwrap();
        wal.append(b"a").unwrap();
        wal.append(b"b").unwrap();
        wal.checkpoint(2).unwrap();
    }

    let wal = Wal::open(config).unwrap();
    assert!(wal.recover().unwrap().is_empty());
}

// ── Reopen / sequence continuity ─────────────────────────────────────────────

#[test]
fn sequence_continues_after_reopen() {
    let dir = TempDir::new().unwrap();
    let config = WalConfig {
        dir: dir.path().to_path_buf(),
        max_segment_bytes: 4096,
        sync_writes: false,
    };

    let last_seq = {
        let mut wal = Wal::open(config.clone()).unwrap();
        wal.append(b"entry A").unwrap();
        wal.append(b"entry B").unwrap()
    };

    let mut wal = Wal::open(config).unwrap();
    let next = wal.append(b"entry C").unwrap();
    assert_eq!(
        next,
        last_seq + 1,
        "sequence must be gapless across reopens"
    );
}

#[test]
fn all_entries_visible_after_reopen() {
    let dir = TempDir::new().unwrap();
    let config = WalConfig {
        dir: dir.path().to_path_buf(),
        max_segment_bytes: 4096,
        sync_writes: false,
    };

    {
        let mut wal = Wal::open(config.clone()).unwrap();
        wal.append(b"persisted-1").unwrap();
        wal.append(b"persisted-2").unwrap();
    }

    let mut wal = Wal::open(config).unwrap();
    wal.append(b"new-3").unwrap();

    let entries = wal.read_from(1).unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].data, b"persisted-1");
    assert_eq!(entries[2].data, b"new-3");
}

// ── Truncation ────────────────────────────────────────────────────────────────

#[test]
fn truncate_removes_sealed_segments_before_threshold() {
    let dir = TempDir::new().unwrap();
    let config = WalConfig {
        dir: dir.path().to_path_buf(),
        max_segment_bytes: 80, // small to generate several segments
        sync_writes: false,
    };
    let mut wal = Wal::open(config).unwrap();

    for i in 1u64..=20 {
        wal.append(format!("data-{i:03}").as_bytes()).unwrap();
    }

    // Entries 1–9 will be removed; entries 10+ stay
    wal.truncate_before(10).unwrap();

    let entries = wal.read_from(1).unwrap();
    assert!(
        entries.iter().all(|e| e.sequence >= 10),
        "no entry before threshold should remain"
    );
    assert!(
        !entries.is_empty(),
        "entries at and above threshold must survive"
    );
}

// ── Edge cases ────────────────────────────────────────────────────────────────

#[test]
fn empty_payload_is_valid() {
    let dir = TempDir::new().unwrap();
    let mut wal = open_wal(&dir);
    let seq = wal.append(&[]).unwrap();
    let entries = wal.read_from(seq).unwrap();
    assert_eq!(entries.len(), 1);
    assert!(entries[0].data.is_empty());
}

#[test]
fn large_single_entry() {
    let dir = TempDir::new().unwrap();
    let config = WalConfig {
        dir: dir.path().to_path_buf(),
        max_segment_bytes: 1024 * 1024, // 1 MiB
        sync_writes: false,
    };
    let mut wal = Wal::open(config).unwrap();
    let big_payload = vec![0xAB_u8; 512 * 1024]; // 512 KiB
    let seq = wal.append(&big_payload).unwrap();
    let entries = wal.read_from(seq).unwrap();
    assert_eq!(entries[0].data, big_payload);
}

#[test]
fn fresh_wal_has_no_pending_recovery_entries() {
    let dir = TempDir::new().unwrap();
    let wal = open_wal(&dir);
    assert!(wal.recover().unwrap().is_empty());
}
