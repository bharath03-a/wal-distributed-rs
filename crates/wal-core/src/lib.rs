//! `wal-core` — a crash-safe, segment-based Write-Ahead Log engine.
//!
//! # Quick start
//!
//! ```no_run
//! use wal_core::{Wal, WalConfig};
//!
//! // Open (or create) a WAL in the given directory
//! let mut wal = Wal::open(WalConfig::new("/tmp/my-wal")).unwrap();
//!
//! // Append entries; each returns its sequence number
//! let seq1 = wal.append(b"begin tx").unwrap();
//! let seq2 = wal.append(b"update key=foo value=bar").unwrap();
//! wal.append(b"commit").unwrap();
//!
//! // Once state has been applied, advance the checkpoint
//! wal.checkpoint(seq2).unwrap();
//!
//! // On the next startup, recover() returns only un-checkpointed entries
//! let to_replay = wal.recover().unwrap();
//! ```

pub mod checkpoint;
pub mod entry;
pub mod error;
pub mod segment;
pub mod wal;

// Re-export the primary public surface
pub use entry::Entry;
pub use error::{Result, WalError};
pub use wal::{Wal, WalConfig};
