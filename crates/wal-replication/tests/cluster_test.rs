//! Integration tests for a 3-node Raft cluster.
//!
//! Each test spins up 3 in-process nodes on localhost ports and exercises
//! the full consensus path over real gRPC connections.

use std::{net::SocketAddr, time::Duration};

use tempfile::TempDir;
use tokio::time::sleep;
use wal_replication::{ClusterConfig, NodeInfo, RaftNode, start_server};

// ── Helpers ────────────────────────────────────────────────────────────────────

/// Reserve N free TCP ports from the OS, release them, and return their numbers.
/// Using port 0 lets the kernel pick, avoiding any port-reuse races between tests.
async fn free_ports(n: usize) -> Vec<u16> {
    let mut listeners = Vec::new();
    let mut ports = Vec::new();
    for _ in 0..n {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        ports.push(l.local_addr().unwrap().port());
        listeners.push(l);
    }
    drop(listeners); // release before servers bind
    ports
}

fn node_addr(port: u16) -> String {
    format!("http://127.0.0.1:{}", port)
}

struct Cluster {
    handles: Vec<wal_replication::RaftHandle>,
    _dirs: Vec<TempDir>,
}

impl Cluster {
    /// Start a 3-node cluster on dynamically allocated ports.
    /// A brief startup delay lets the gRPC servers bind before the first write.
    async fn start() -> Self {
        let ports = free_ports(3).await;
        let infos: Vec<NodeInfo> = ports
            .iter()
            .enumerate()
            .map(|(i, &p)| NodeInfo {
                id: format!("node-{}", i + 1),
                addr: node_addr(p),
            })
            .collect();

        let mut handles = Vec::new();
        let mut dirs = Vec::new();

        for (i, info) in infos.iter().enumerate() {
            let peers: Vec<NodeInfo> = infos
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(_, n)| n.clone())
                .collect();

            let dir = TempDir::new().unwrap();
            let cfg = ClusterConfig {
                this_node: info.clone(),
                peers,
                data_dir: dir.path().to_path_buf(),
                election_timeout_min: Duration::from_millis(150),
                election_timeout_max: Duration::from_millis(300),
                heartbeat_interval: Duration::from_millis(50),
            };

            let handle = RaftNode::start(cfg.clone()).unwrap();

            let addr: SocketAddr = format!("0.0.0.0:{}", ports[i]).parse().unwrap();
            let h2 = handle.clone();
            tokio::spawn(async move {
                let _ = start_server(h2, addr).await;
            });

            handles.push(handle);
            dirs.push(dir);
        }

        // Give gRPC servers a moment to bind before the first election round
        sleep(Duration::from_millis(50)).await;

        Cluster { handles, _dirs: dirs }
    }

    /// Poll each node for `write()` success to discover the leader.
    /// Returns the handle of the leader node and its index within `handles`.
    async fn wait_for_leader(&self, timeout: Duration) -> Option<(usize, &wal_replication::RaftHandle)> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            for (i, h) in self.handles.iter().enumerate() {
                // A tiny probe write to see if this node is the leader
                if h.write(b"probe".to_vec()).await.is_ok() {
                    return Some((i, h));
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return None;
            }
            sleep(Duration::from_millis(20)).await;
        }
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_leader_is_elected() {
    let cluster = Cluster::start().await;
    // Give nodes time to elect a leader
    let leader = cluster.wait_for_leader(Duration::from_secs(3)).await;
    assert!(leader.is_some(), "a leader should be elected within 3 seconds");
}

#[tokio::test]
async fn test_write_to_leader_succeeds() {
    let cluster = Cluster::start().await;
    let (_, leader) = cluster
        .wait_for_leader(Duration::from_secs(3))
        .await
        .expect("no leader elected");

    let idx = leader.write(b"hello distributed WAL".to_vec()).await.unwrap();
    // The probe write in wait_for_leader is index 1, so our write is 2
    assert!(idx >= 1, "write should return a positive index");
}

#[tokio::test]
async fn test_entries_replicated_to_all_nodes() {
    let cluster = Cluster::start().await;
    let (leader_i, leader) = cluster
        .wait_for_leader(Duration::from_secs(3))
        .await
        .expect("no leader elected");

    let idx = leader.write(b"replicated entry".to_vec()).await.unwrap();

    // Give followers a moment to receive the committed entry
    sleep(Duration::from_millis(200)).await;

    // Every node (including the leader) should see this entry
    for (i, handle) in cluster.handles.iter().enumerate() {
        let entries = handle.read_from(1).await.unwrap();
        let found = entries.iter().any(|e| e.index == idx && e.data == b"replicated entry");
        assert!(
            found,
            "node {} (leader={}) should have entry at index {}",
            i, i == leader_i, idx
        );
    }
}

#[tokio::test]
async fn test_multiple_writes_maintain_order() {
    let cluster = Cluster::start().await;
    let (_, leader) = cluster
        .wait_for_leader(Duration::from_secs(3))
        .await
        .expect("no leader elected");

    let payloads = [b"tx-1".as_ref(), b"tx-2".as_ref(), b"tx-3".as_ref()];
    let mut indices = Vec::new();
    for p in &payloads {
        indices.push(leader.write(p.to_vec()).await.unwrap());
    }

    // Indices must be strictly increasing
    for w in indices.windows(2) {
        assert!(w[1] > w[0], "log indices must be monotonically increasing");
    }
}

#[tokio::test]
async fn test_follower_rejects_write() {
    let cluster = Cluster::start().await;
    let (leader_i, _) = cluster
        .wait_for_leader(Duration::from_secs(3))
        .await
        .expect("no leader elected");

    // Pick a follower
    let follower_i = if leader_i == 0 { 1 } else { 0 };
    let result = cluster.handles[follower_i].write(b"should fail".to_vec()).await;
    assert!(
        matches!(result, Err(wal_replication::RaftError::NotLeader { .. })),
        "follower must reject writes with NotLeader"
    );
}
