//! Benchmarks for `wal-core`.
//!
//! Run with:
//! ```
//! cargo bench -p wal-core
//! ```
//! HTML reports are written to `target/criterion/`.
//!
//! # What we measure
//!
//! | Group              | What it tells you                                 |
//! |--------------------|---------------------------------------------------|
//! | `entry_codec`      | Pure encode/decode cost (no I/O)                 |
//! | `append_small`     | Throughput: many tiny writes (64 B payloads)      |
//! | `append_large`     | Throughput: fewer large writes (4 KiB payloads)   |
//! | `append_bulk`      | Batch write: 1 000 entries back-to-back           |
//! | `recover`          | Replay cost: read 1 000 entries from segments     |

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tempfile::TempDir;
use wal_core::{Wal, WalConfig};

// ── Helpers ────────────────────────────────────────────────────────────────────

fn open_wal(dir: &TempDir, sync: bool) -> Wal {
    Wal::open(WalConfig {
        dir: dir.path().to_path_buf(),
        max_segment_bytes: 256 * 1024 * 1024, // 256 MiB — won't rotate during bench
        sync_writes: sync,
    })
    .unwrap()
}

// ── Codec micro-benchmark ─────────────────────────────────────────────────────

fn bench_entry_codec(c: &mut Criterion) {
    let payload = vec![0xAB_u8; 256];

    let mut g = c.benchmark_group("entry_codec");

    g.bench_function("encode_256b", |b| {
        b.iter(|| wal_core::entry::encode(black_box(42), black_box(&payload)))
    });

    let encoded = wal_core::entry::encode(1, &payload);
    g.bench_function("decode_256b", |b| {
        b.iter(|| wal_core::entry::decode(black_box(&encoded), 0).unwrap())
    });

    g.finish();
}

// ── Append throughput ─────────────────────────────────────────────────────────

fn bench_append_payload_sizes(c: &mut Criterion) {
    let sizes: &[usize] = &[64, 256, 1024, 4096, 16384];

    let mut g = c.benchmark_group("append_no_sync");
    for &size in sizes {
        let payload = vec![0u8; size];
        g.throughput(Throughput::Bytes(size as u64));
        g.bench_with_input(BenchmarkId::from_parameter(size), &payload, |b, p| {
            let dir = TempDir::new().unwrap();
            let mut wal = open_wal(&dir, false);
            b.iter(|| wal.append(black_box(p)).unwrap());
        });
    }
    g.finish();
}

fn bench_append_with_sync(c: &mut Criterion) {
    // Only bench with fsync on a realistic payload — fsync dominates latency
    let payload = vec![0u8; 256];

    let mut g = c.benchmark_group("append_with_fsync");
    g.throughput(Throughput::Bytes(payload.len() as u64));
    g.bench_function("256b_fsync", |b| {
        let dir = TempDir::new().unwrap();
        let mut wal = open_wal(&dir, true);
        b.iter(|| wal.append(black_box(&payload)).unwrap());
    });
    g.finish();
}

// ── Bulk append (1 000 entries per iteration) ──────────────────────────────────

fn bench_bulk_append(c: &mut Criterion) {
    const N: usize = 1_000;
    let payload = vec![0u8; 128];

    let mut g = c.benchmark_group("append_bulk");
    g.throughput(Throughput::Elements(N as u64));

    g.bench_function(format!("{N}_entries_no_sync"), |b| {
        b.iter_with_setup(
            || {
                let dir = TempDir::new().unwrap();
                let wal = open_wal(&dir, false);
                (dir, wal)
            },
            |(_dir, mut wal)| {
                for _ in 0..N {
                    wal.append(black_box(&payload)).unwrap();
                }
            },
        );
    });

    g.finish();
}

// ── Recovery (read all entries back) ──────────────────────────────────────────

fn bench_recover(c: &mut Criterion) {
    const N: usize = 1_000;
    let payload = vec![0u8; 128];

    // Pre-write N entries into a fixed dir
    let dir = TempDir::new().unwrap();
    {
        let mut wal = open_wal(&dir, false);
        for _ in 0..N {
            wal.append(&payload).unwrap();
        }
        wal.sync().unwrap();
    }

    let mut g = c.benchmark_group("recover");
    g.throughput(Throughput::Elements(N as u64));

    g.bench_function(format!("{N}_entries"), |b| {
        b.iter(|| {
            let wal = open_wal(&dir, false);
            black_box(wal.recover().unwrap())
        });
    });

    g.finish();
}

// ── Segment rotation overhead ─────────────────────────────────────────────────

fn bench_rotation(c: &mut Criterion) {
    // Force a rotation every ~10 entries (100-byte segment cap + 16-byte header)
    let payload = vec![0u8; 80]; // each entry ≈ 96 bytes, segment cap = 1 KiB

    let mut g = c.benchmark_group("segment_rotation");
    g.throughput(Throughput::Elements(100));

    g.bench_function("100_entries_with_rotation", |b| {
        b.iter_with_setup(
            || TempDir::new().unwrap(),
            |dir| {
                let mut wal = Wal::open(WalConfig {
                    dir: dir.path().to_path_buf(),
                    max_segment_bytes: 1_024, // 1 KiB → rotates every ~10 entries
                    sync_writes: false,
                })
                .unwrap();
                for _ in 0..100 {
                    wal.append(black_box(&payload)).unwrap();
                }
                dir // keep alive until after the closure
            },
        );
    });

    g.finish();
}

// ── Registration ──────────────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_entry_codec,
    bench_append_payload_sizes,
    bench_append_with_sync,
    bench_bulk_append,
    bench_recover,
    bench_rotation,
);
criterion_main!(benches);
