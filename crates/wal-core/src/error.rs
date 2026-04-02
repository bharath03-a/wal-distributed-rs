use thiserror::Error;

/// All errors that the WAL engine can produce.
#[derive(Debug, Error)]
pub enum WalError {
    /// Underlying OS / file-system error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// A stored CRC32 checksum did not match the recomputed one.
    /// This indicates either bit-rot or a partial / corrupted write.
    #[error("checksum mismatch at byte offset {offset}: stored {stored:#010x}, computed {computed:#010x}")]
    ChecksumMismatch {
        offset: usize,
        stored: u32,
        computed: u32,
    },

    /// The buffer ended before a complete entry could be decoded.
    #[error("unexpected end of data while decoding entry at byte offset {offset}")]
    UnexpectedEof { offset: usize },

    /// A file path did not match the expected segment naming convention.
    #[error("invalid segment filename: {filename}")]
    InvalidSegmentFilename { filename: String },

    /// An append was rejected because it would exceed the segment size cap.
    #[error("segment is full (current {size} B, max {max} B, entry {entry} B)")]
    SegmentFull { size: u64, max: u64, entry: u64 },
}

/// Convenience alias used throughout the crate.
pub type Result<T> = std::result::Result<T, WalError>;
