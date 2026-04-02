//! `wal-replication` — Raft-based distributed replication for `wal-core`.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────┐
//! │  Client                                                      │
//! │    │  gRPC Write / ReadFrom                                  │
//! │    ▼                                                         │
//! │  WalService ──► RaftHandle ──channel──► RaftNode (actor)     │
//! │                                              │               │
//! │  RaftService ──► RaftHandle ──channel──►     │               │
//! │  (peer RPCs)                            ┌────┴────┐          │
//! │                                         │ RaftLog │          │
//! │                                         │  (WAL)  │          │
//! │                                         └─────────┘          │
//! └──────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Quick start
//!
//! ```no_run
//! use wal_replication::{ClusterConfig, NodeInfo, RaftNode, start_server};
//! use std::net::SocketAddr;
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = ClusterConfig::new(
//!         NodeInfo { id: "node-1".into(), addr: "http://127.0.0.1:7001".into() },
//!         vec![
//!             NodeInfo { id: "node-2".into(), addr: "http://127.0.0.1:7002".into() },
//!             NodeInfo { id: "node-3".into(), addr: "http://127.0.0.1:7003".into() },
//!         ],
//!         "/tmp/wal-node-1",
//!     );
//!
//!     let handle = RaftNode::start(config.clone()).unwrap();
//!     start_server(handle, "0.0.0.0:7001".parse().unwrap()).await.unwrap();
//! }
//! ```

pub mod config;
pub mod error;
pub mod persistent_state;
pub mod proto;
pub mod raft;
pub mod server;

pub use config::{ClusterConfig, NodeId, NodeInfo};
pub use error::{RaftError, Result};
pub use raft::{LogEntry, RaftHandle, RaftNode};

use std::net::SocketAddr;

use tonic::transport::Server;

use crate::{
    proto::wal::{
        raft_service_server::RaftServiceServer,
        wal_service_server::WalServiceServer,
    },
    server::{RaftServiceImpl, WalServiceImpl},
};

/// Start the gRPC server for both `RaftService` and `WalService`.
///
/// This function runs until the server is shut down (it never returns `Ok`
/// in normal operation). Typically called from `tokio::spawn`.
pub async fn start_server(handle: RaftHandle, addr: SocketAddr) -> Result<()> {
    let raft_svc = RaftServiceServer::new(RaftServiceImpl::new(handle.clone()));
    let wal_svc = WalServiceServer::new(WalServiceImpl::new(handle));

    Server::builder()
        .add_service(raft_svc)
        .add_service(wal_svc)
        .serve(addr)
        .await?;

    Ok(())
}
