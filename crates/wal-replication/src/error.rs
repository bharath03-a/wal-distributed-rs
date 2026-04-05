//! Error types for the replication layer.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum RaftError {
    #[error("not the leader (try {hint:?})")]
    NotLeader { hint: Option<String> },

    #[error("quorum was not reached — entry may not be committed")]
    QuorumNotReached,

    #[error("node is shutting down")]
    Shutdown,

    #[error("WAL error: {0}")]
    Wal(#[from] wal_core::WalError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("gRPC transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("gRPC status: {0}")]
    Status(#[from] Box<tonic::Status>),
}

pub type Result<T> = std::result::Result<T, RaftError>;
