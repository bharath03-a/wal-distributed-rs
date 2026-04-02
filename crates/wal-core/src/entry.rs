//! On-disk entry format for WAL segments.
//!
//! Every entry is written as a fixed 16-byte header followed by the
//! application payload:
//!
//! ```text
//! ┌──────────────┬─────────────┬──────────────┬─────────────────┐
//! │ sequence (8) │  len (4)    │  crc32 (4)   │   data (N)      │
//! │  u64 LE      │  u32 LE     │  u32 LE      │   raw bytes     │
//! └──────────────┴─────────────┴──────────────┴─────────────────┘
//! ```
//!
//! The CRC32 covers only the `data` field. Sequence numbers start at 1
//! and increase monotonically across the entire WAL (not per-segment).

use crate::error::{Result, WalError};

/// Byte size of the fixed entry header.
pub const HEADER_SIZE: usize = 16;

/// An in-memory representation of a single WAL entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Monotonically increasing log sequence number (1-based, global).
    pub sequence: u64,
    /// The application-level payload stored in this entry.
    pub data: Vec<u8>,
}

/// Encode `(sequence, data)` into its on-disk byte representation.
///
/// The returned `Vec<u8>` is `HEADER_SIZE + data.len()` bytes long and is
/// ready to be written directly to a segment file.
pub fn encode(sequence: u64, data: &[u8]) -> Vec<u8> {
    let len = data.len() as u32;
    let crc = crc32fast::hash(data);

    let mut buf = Vec::with_capacity(HEADER_SIZE + data.len());
    buf.extend_from_slice(&sequence.to_le_bytes());
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(&crc.to_le_bytes());
    buf.extend_from_slice(data);
    buf
}

/// Decode one entry from the beginning of `buf`.
///
/// `buf_offset` is the position of `buf[0]` within the segment file; it is
/// only used for diagnostic error messages.
///
/// On success returns `(Entry, bytes_consumed)`.
/// Returns `Err(UnexpectedEof)` if `buf` is shorter than the encoded entry.
/// Returns `Err(ChecksumMismatch)` if the stored CRC32 does not match.
pub fn decode(buf: &[u8], buf_offset: usize) -> Result<(Entry, usize)> {
    if buf.len() < HEADER_SIZE {
        return Err(WalError::UnexpectedEof { offset: buf_offset });
    }

    let sequence = u64::from_le_bytes(buf[0..8].try_into().unwrap());
    let data_len = u32::from_le_bytes(buf[8..12].try_into().unwrap()) as usize;
    let stored_crc = u32::from_le_bytes(buf[12..16].try_into().unwrap());

    let total = HEADER_SIZE + data_len;
    if buf.len() < total {
        return Err(WalError::UnexpectedEof { offset: buf_offset });
    }

    let data = &buf[HEADER_SIZE..total];
    let computed_crc = crc32fast::hash(data);

    if stored_crc != computed_crc {
        return Err(WalError::ChecksumMismatch {
            offset: buf_offset,
            stored: stored_crc,
            computed: computed_crc,
        });
    }

    Ok((Entry { sequence, data: data.to_vec() }, total))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_empty_payload() {
        let encoded = encode(1, &[]);
        let (entry, consumed) = decode(&encoded, 0).unwrap();
        assert_eq!(entry.sequence, 1);
        assert!(entry.data.is_empty());
        assert_eq!(consumed, HEADER_SIZE);
    }

    #[test]
    fn roundtrip_with_data() {
        let payload = b"hello, WAL!";
        let encoded = encode(42, payload);
        let (entry, consumed) = decode(&encoded, 0).unwrap();
        assert_eq!(entry.sequence, 42);
        assert_eq!(entry.data, payload);
        assert_eq!(consumed, HEADER_SIZE + payload.len());
    }

    #[test]
    fn detects_truncated_header() {
        // Only 4 bytes — not enough for the 16-byte header
        let encoded = encode(1, b"data");
        let result = decode(&encoded[..4], 0);
        assert!(matches!(result, Err(WalError::UnexpectedEof { .. })));
    }

    #[test]
    fn detects_truncated_data() {
        let encoded = encode(1, b"hello");
        // Header is intact but data is cut short
        let truncated = &encoded[..HEADER_SIZE + 2];
        let result = decode(truncated, 0);
        assert!(matches!(result, Err(WalError::UnexpectedEof { .. })));
    }

    #[test]
    fn detects_corrupted_payload() {
        let mut encoded = encode(1, b"important data");
        // Flip bits in the last byte of the data section
        let last = encoded.len() - 1;
        encoded[last] ^= 0xFF;
        let result = decode(&encoded, 0);
        assert!(matches!(result, Err(WalError::ChecksumMismatch { .. })));
    }

    #[test]
    fn multiple_entries_in_stream() {
        let mut stream = Vec::new();
        stream.extend_from_slice(&encode(1, b"first"));
        stream.extend_from_slice(&encode(2, b"second"));
        stream.extend_from_slice(&encode(3, b"third"));

        let mut offset = 0;
        let mut entries = Vec::new();
        while offset < stream.len() {
            let (entry, consumed) = decode(&stream[offset..], offset).unwrap();
            entries.push(entry);
            offset += consumed;
        }

        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].data, b"first");
        assert_eq!(entries[2].sequence, 3);
    }
}
