//! Cluster and node configuration.

use std::{path::PathBuf, time::Duration};

/// A unique name for a node within the cluster (e.g. `"node-1"`).
pub type NodeId = String;

/// Address + identity of a single cluster member.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub id: NodeId,
    /// gRPC endpoint, e.g. `"http://127.0.0.1:7001"`.
    pub addr: String,
}

/// Full configuration for one node and the cluster it belongs to.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// This node's identity and address.
    pub this_node: NodeInfo,
    /// All *other* members of the cluster.
    pub peers: Vec<NodeInfo>,
    /// Directory for this node's WAL segments and persistent Raft state.
    pub data_dir: PathBuf,
    /// Minimum election timeout (Raft recommends 150–300 ms).
    pub election_timeout_min: Duration,
    /// Maximum election timeout.
    pub election_timeout_max: Duration,
    /// How often the leader sends heartbeats (must be << election timeout).
    pub heartbeat_interval: Duration,
}

impl ClusterConfig {
    /// Sensible defaults. Override fields as needed.
    pub fn new(this_node: NodeInfo, peers: Vec<NodeInfo>, data_dir: impl Into<PathBuf>) -> Self {
        Self {
            this_node,
            peers,
            data_dir: data_dir.into(),
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
        }
    }

    /// Total cluster size (this node + peers).
    pub fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }

    /// Minimum votes needed to win an election or commit an entry.
    pub fn quorum(&self) -> usize {
        self.cluster_size() / 2 + 1
    }
}
