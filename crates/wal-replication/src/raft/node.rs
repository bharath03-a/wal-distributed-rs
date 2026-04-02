//! The Raft consensus node — an async actor that drives the Raft state machine.
//!
//! # Actor model
//!
//! `RaftNode` owns all mutable state and runs inside a single tokio task.
//! External callers (gRPC handlers, tests) communicate exclusively through a
//! `RaftHandle`, which wraps a channel.  This keeps the state machine
//! single-threaded with no locks.
//!
//! # Raft roles
//!
//! ```text
//!  ┌─────────────────────────────────────────────────────────────┐
//!  │  Follower  ──(timeout)──►  Candidate  ──(majority)──►  Leader  │
//!  │     ▲                          │                          │     │
//!  │     └──────(higher term)───────┘◄────(higher term)────────┘     │
//!  └─────────────────────────────────────────────────────────────┘
//! ```

use std::{collections::HashMap, sync::Arc, time::Instant};

// Metric names
//
//  raft_elections_started_total  counter  How many elections this node started.
//  raft_votes_granted_total      counter  How many votes this node granted.
//  raft_entries_committed_total  counter  Entries committed by the leader.
//  raft_current_term             gauge    Raft term (rises monotonically).
//  raft_commit_index             gauge    Highest committed log index.
//  raft_role                     gauge    0=follower, 1=candidate, 2=leader.
const M_ELECTIONS:  &str = "raft_elections_started_total";
const M_VOTES:      &str = "raft_votes_granted_total";
const M_COMMITTED:  &str = "raft_entries_committed_total";
const M_TERM:       &str = "raft_current_term";
const M_COMMIT_IDX: &str = "raft_commit_index";
const M_ROLE:       &str = "raft_role";

use futures::future::join_all;
use rand::Rng;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, Duration};

use crate::{
    config::{ClusterConfig, NodeId},
    error::{RaftError, Result},
    proto::wal::{
        AppendEntriesRequest, AppendEntriesResponse, LogEntry as ProtoEntry,
        RequestVoteRequest, RequestVoteResponse,
    },
};

use super::log::{LogEntry, RaftLog};
use crate::persistent_state::PersistentState;
use wal_core::WalConfig;

// ── Peer client ───────────────────────────────────────────────────────────────

/// Thin async wrapper around a gRPC channel to one peer.
#[derive(Clone)]
pub struct PeerClient {
    pub id: NodeId,
    addr: String,
}

impl PeerClient {
    pub fn new(id: NodeId, addr: String) -> Self {
        Self { id, addr }
    }

    /// RPC timeout applied to every peer call. Prevents the actor from blocking
    /// indefinitely when a peer is mid-election or temporarily unresponsive,
    /// which would otherwise cause a deadlock between two concurrently-blocked actors.
    const RPC_TIMEOUT: Duration = Duration::from_millis(300);

    pub async fn append_entries(
        &self,
        req: AppendEntriesRequest,
    ) -> std::result::Result<AppendEntriesResponse, tonic::Status> {
        use crate::proto::wal::raft_service_client::RaftServiceClient;
        let addr = self.addr.clone();
        let call = async move {
            let mut client = RaftServiceClient::connect(addr)
                .await
                .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
            client.append_entries(req).await.map(|r| r.into_inner())
        };
        tokio::time::timeout(Self::RPC_TIMEOUT, call)
            .await
            .unwrap_or_else(|_| Err(tonic::Status::deadline_exceeded("AppendEntries timed out")))
    }

    pub async fn request_vote(
        &self,
        req: RequestVoteRequest,
    ) -> std::result::Result<RequestVoteResponse, tonic::Status> {
        use crate::proto::wal::raft_service_client::RaftServiceClient;
        let addr = self.addr.clone();
        let call = async move {
            let mut client = RaftServiceClient::connect(addr)
                .await
                .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
            client.request_vote(req).await.map(|r| r.into_inner())
        };
        tokio::time::timeout(Self::RPC_TIMEOUT, call)
            .await
            .unwrap_or_else(|_| Err(tonic::Status::deadline_exceeded("RequestVote timed out")))
    }
}

// ── Role ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

// ── Messages the actor handles ────────────────────────────────────────────────

pub enum RaftMsg {
    AppendEntries {
        req: AppendEntriesRequest,
        reply: oneshot::Sender<AppendEntriesResponse>,
    },
    RequestVote {
        req: RequestVoteRequest,
        reply: oneshot::Sender<RequestVoteResponse>,
    },
    ClientWrite {
        data: Vec<u8>,
        reply: oneshot::Sender<std::result::Result<u64, RaftError>>,
    },
    ClientRead {
        from_index: u64,
        reply: oneshot::Sender<Vec<LogEntry>>,
    },
}

// ── RaftHandle (public API) ───────────────────────────────────────────────────

/// A cheaply clone-able handle to the running [`RaftNode`] actor.
#[derive(Clone)]
pub struct RaftHandle {
    tx: mpsc::Sender<RaftMsg>,
}

impl RaftHandle {
    pub async fn append_entries(
        &self,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(RaftMsg::AppendEntries { req, reply: tx })
            .await
            .map_err(|_| RaftError::Shutdown)?;
        rx.await.map_err(|_| RaftError::Shutdown)
    }

    pub async fn request_vote(&self, req: RequestVoteRequest) -> Result<RequestVoteResponse> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(RaftMsg::RequestVote { req, reply: tx })
            .await
            .map_err(|_| RaftError::Shutdown)?;
        rx.await.map_err(|_| RaftError::Shutdown)
    }

    pub async fn write(&self, data: Vec<u8>) -> Result<u64> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(RaftMsg::ClientWrite { data, reply: tx })
            .await
            .map_err(|_| RaftError::Shutdown)?;
        rx.await.map_err(|_| RaftError::Shutdown)?
    }

    pub async fn read_from(&self, from_index: u64) -> Result<Vec<LogEntry>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(RaftMsg::ClientRead { from_index, reply: tx })
            .await
            .map_err(|_| RaftError::Shutdown)?;
        rx.await.map_err(|_| RaftError::Shutdown)
    }
}

// ── RaftNode ──────────────────────────────────────────────────────────────────

pub struct RaftNode {
    id: NodeId,
    config: Arc<ClusterConfig>,
    peers: Vec<PeerClient>,

    // Raft persistent state
    ps: PersistentState,

    // Replicated log (WAL-backed)
    log: RaftLog,

    // Volatile state (all nodes)
    role: Role,
    commit_index: u64,
    current_leader: Option<NodeId>,

    // Leader volatile state
    next_index: HashMap<NodeId, u64>,
    match_index: HashMap<NodeId, u64>,

    // Election timer
    last_heartbeat: Instant,
    election_timeout: Duration,
}

impl RaftNode {
    /// Construct and immediately spawn the actor. Returns a [`RaftHandle`].
    pub fn start(config: ClusterConfig) -> Result<RaftHandle> {
        std::fs::create_dir_all(&config.data_dir)?;

        let ps = PersistentState::open(&config.data_dir)?;
        let wal_config = WalConfig {
            dir: config.data_dir.join("wal"),
            max_segment_bytes: 64 * 1024 * 1024,
            sync_writes: true,
        };
        let log = RaftLog::open(wal_config)?;

        let peers: Vec<PeerClient> = config
            .peers
            .iter()
            .map(|p| PeerClient::new(p.id.clone(), p.addr.clone()))
            .collect();

        let election_timeout = random_election_timeout(&config);

        let node = RaftNode {
            id: config.this_node.id.clone(),
            config: Arc::new(config),
            peers,
            ps,
            log,
            role: Role::Follower,
            commit_index: 0,
            current_leader: None,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            last_heartbeat: Instant::now(),
            election_timeout,
        };

        let (tx, rx) = mpsc::channel(256);
        tokio::spawn(node.run(rx));

        Ok(RaftHandle { tx })
    }

    // ── Main event loop ───────────────────────────────────────────────────────

    async fn run(mut self, mut rx: mpsc::Receiver<RaftMsg>) {
        loop {
            let until_timeout = self.time_until_timeout();

            tokio::select! {
                Some(msg) = rx.recv() => {
                    self.handle_msg(msg).await;
                }
                _ = sleep(until_timeout) => {
                    self.handle_timeout().await;
                }
            }
        }
    }

    fn time_until_timeout(&self) -> Duration {
        match self.role {
            Role::Leader => self.config.heartbeat_interval,
            _ => {
                let elapsed = self.last_heartbeat.elapsed();
                self.election_timeout.saturating_sub(elapsed)
            }
        }
    }

    async fn handle_timeout(&mut self) {
        match self.role {
            Role::Leader => self.send_heartbeats().await,
            _ => {
                // Election timeout: start an election
                tracing::info!("{}: election timeout, starting election", self.id);
                self.start_election().await;
            }
        }
    }

    async fn handle_msg(&mut self, msg: RaftMsg) {
        match msg {
            RaftMsg::AppendEntries { req, reply } => {
                let resp = self.on_append_entries(req);
                let _ = reply.send(resp);
            }
            RaftMsg::RequestVote { req, reply } => {
                let resp = self.on_request_vote(req);
                let _ = reply.send(resp);
            }
            RaftMsg::ClientWrite { data, reply } => {
                let result = self.on_client_write(data).await;
                let _ = reply.send(result);
            }
            RaftMsg::ClientRead { from_index, reply } => {
                let entries = self.log.entries_from(from_index);
                let _ = reply.send(entries);
            }
        }
    }

    // ── AppendEntries RPC (§5.3) ──────────────────────────────────────────────

    fn on_append_entries(&mut self, req: AppendEntriesRequest) -> AppendEntriesResponse {
        // Rule 1: reject stale leaders
        if req.term < self.ps.current_term {
            return self.ae_reject(0, 0);
        }

        // Rule 2: a valid leader → become / stay follower
        if req.term > self.ps.current_term {
            let _ = self.ps.advance_term(req.term);
        }
        self.become_follower_with_leader(req.term, Some(req.leader_id.clone()));

        // Rule 3: check log consistency at prev_log_index / prev_log_term
        if req.prev_log_index > 0 {
            match self.log.term_at(req.prev_log_index) {
                None => {
                    // We don't have that entry yet
                    return self.ae_reject(self.log.last_index() + 1, 0);
                }
                Some(t) if t != req.prev_log_term => {
                    // Conflicting term: give back the first index of that term
                    // so the leader can skip the whole term in one round trip
                    let conflict_index = self
                        .log
                        .first_index_of_term(t)
                        .unwrap_or(req.prev_log_index);
                    return self.ae_reject(conflict_index, t);
                }
                _ => {}
            }
        }

        // Rule 4: append new entries (truncating on conflict)
        for proto_entry in &req.entries {
            let idx = proto_entry.index;
            match self.log.term_at(idx) {
                Some(t) if t == proto_entry.term => {
                    // Already have a matching entry — no-op
                }
                Some(_) => {
                    // Conflict: trim our log and fall through to append
                    let _ = self.log.truncate_from(idx);
                    let _ = self.log.append(proto_entry.term, &proto_entry.data);
                }
                None => {
                    let _ = self.log.append(proto_entry.term, &proto_entry.data);
                }
            }
        }

        // Rule 5: advance commit index
        if req.leader_commit > self.commit_index {
            self.commit_index = req.leader_commit.min(self.log.last_index());
        }

        AppendEntriesResponse {
            term: self.ps.current_term,
            success: true,
            conflict_index: 0,
            conflict_term: 0,
        }
    }

    fn ae_reject(&self, conflict_index: u64, conflict_term: u64) -> AppendEntriesResponse {
        AppendEntriesResponse {
            term: self.ps.current_term,
            success: false,
            conflict_index,
            conflict_term,
        }
    }

    // ── RequestVote RPC (§5.2) ────────────────────────────────────────────────

    fn on_request_vote(&mut self, req: RequestVoteRequest) -> RequestVoteResponse {
        // Always update term if we see a higher one
        if req.term > self.ps.current_term {
            let _ = self.ps.advance_term(req.term);
            self.role = Role::Follower;
        }

        if req.term < self.ps.current_term {
            return RequestVoteResponse { term: self.ps.current_term, vote_granted: false };
        }

        let already_voted_for_other = self
            .ps
            .voted_for
            .as_ref()
            .map(|v| v != &req.candidate_id)
            .unwrap_or(false);

        if already_voted_for_other {
            return RequestVoteResponse { term: self.ps.current_term, vote_granted: false };
        }

        // Log up-to-date check (§5.4.1)
        let log_ok = req.last_log_term > self.log.last_term()
            || (req.last_log_term == self.log.last_term()
                && req.last_log_index >= self.log.last_index());

        if !log_ok {
            return RequestVoteResponse { term: self.ps.current_term, vote_granted: false };
        }

        let _ = self.ps.record_vote(req.candidate_id.clone());
        self.last_heartbeat = Instant::now(); // reset timer on granting a vote
        metrics::counter!(M_VOTES).increment(1);
        tracing::debug!("{}: voted for {} in term {}", self.id, req.candidate_id, req.term);

        RequestVoteResponse { term: self.ps.current_term, vote_granted: true }
    }

    // ── Leader election (§5.2) ────────────────────────────────────────────────

    async fn start_election(&mut self) {
        let new_term = self.ps.current_term + 1;
        let _ = self.ps.advance_term(new_term);
        let _ = self.ps.record_vote(self.id.clone());
        self.role = Role::Candidate;
        self.last_heartbeat = Instant::now();
        self.election_timeout = random_election_timeout(&self.config);

        metrics::counter!(M_ELECTIONS).increment(1);
        metrics::gauge!(M_TERM).set(new_term as f64);
        metrics::gauge!(M_ROLE).set(1.0); // candidate
        tracing::info!("{}: starting election for term {}", self.id, new_term);

        let req = RequestVoteRequest {
            term: new_term,
            candidate_id: self.id.clone(),
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
        };

        let futures: Vec<_> = self
            .peers
            .iter()
            .map(|p| {
                let p = p.clone();
                let r = req.clone();
                async move { (p.id.clone(), p.request_vote(r).await) }
            })
            .collect();

        let results = join_all(futures).await;

        let mut votes = 1usize; // self-vote
        for (_peer_id, result) in results {
            match result {
                Ok(resp) => {
                    if resp.term > self.ps.current_term {
                        let _ = self.ps.advance_term(resp.term);
                        self.become_follower_with_leader(resp.term, None);
                        return;
                    }
                    if resp.vote_granted {
                        votes += 1;
                    }
                }
                Err(e) => tracing::warn!("{}: vote RPC failed: {}", self.id, e),
            }
        }

        if self.role != Role::Candidate {
            return; // stepped down during election
        }

        if votes >= self.config.quorum() {
            tracing::info!(
                "{}: won election for term {} with {}/{} votes",
                self.id, new_term, votes, self.config.cluster_size()
            );
            self.become_leader();
        } else {
            tracing::debug!("{}: lost election ({} votes)", self.id, votes);
            self.role = Role::Follower;
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.current_leader = Some(self.id.clone());

        // Initialize leader replication state
        let next = self.log.last_index() + 1;
        for peer in &self.peers {
            self.next_index.insert(peer.id.clone(), next);
            self.match_index.insert(peer.id.clone(), 0);
        }
        metrics::gauge!(M_ROLE).set(2.0); // leader
        tracing::info!("{}: became leader for term {}", self.id, self.ps.current_term);
    }

    fn become_follower_with_leader(&mut self, term: u64, leader: Option<String>) {
        self.role = Role::Follower;
        if let Some(l) = leader {
            self.current_leader = Some(l);
        }
        self.last_heartbeat = Instant::now();
        self.election_timeout = random_election_timeout(&self.config);
        metrics::gauge!(M_ROLE).set(0.0); // follower
        metrics::gauge!(M_TERM).set(self.ps.current_term as f64);
        let _ = term; // term already advanced by caller if needed
    }

    // ── Client write (leader path) ────────────────────────────────────────────

    async fn on_client_write(&mut self, data: Vec<u8>) -> Result<u64> {
        if self.role != Role::Leader {
            return Err(RaftError::NotLeader {
                hint: self.current_leader.clone(),
            });
        }

        let index = self.log.append(self.ps.current_term, &data)?;
        let quorum = self.config.quorum();

        // Replicate to peers and count acknowledgments (includes self = 1)
        let acks = self.replicate_to_peers(index).await;
        if acks < quorum {
            return Err(RaftError::QuorumNotReached);
        }

        self.commit_index = index;
        metrics::counter!(M_COMMITTED).increment(1);
        metrics::gauge!(M_COMMIT_IDX).set(index as f64);
        tracing::debug!("{}: committed index {}", self.id, index);
        Ok(index)
    }

    /// Send AppendEntries to all peers, return total acks (self + peers).
    async fn replicate_to_peers(&mut self, up_to_index: u64) -> usize {
        let futures: Vec<_> = self
            .peers
            .iter()
            .map(|peer| {
                let peer = peer.clone();
                let req = self.build_append_entries_for(peer.id.clone(), up_to_index);
                async move {
                    match peer.append_entries(req).await {
                        Ok(resp) => (peer.id, resp),
                        Err(e) => {
                            tracing::warn!("AppendEntries to {} failed: {}", peer.id, e);
                            (
                                peer.id,
                                AppendEntriesResponse {
                                    term: 0,
                                    success: false,
                                    conflict_index: 0,
                                    conflict_term: 0,
                                },
                            )
                        }
                    }
                }
            })
            .collect();

        let results = join_all(futures).await;
        let mut acks = 1usize; // self

        for (peer_id, resp) in results {
            if resp.term > self.ps.current_term {
                // Discovered a higher term: step down
                let _ = self.ps.advance_term(resp.term);
                self.become_follower_with_leader(resp.term, None);
                return acks;
            }
            if resp.success {
                acks += 1;
                self.match_index.insert(peer_id.clone(), up_to_index);
                self.next_index.insert(peer_id, up_to_index + 1);
            } else if resp.conflict_index > 0 {
                self.next_index.insert(peer_id, resp.conflict_index);
            }
        }

        acks
    }

    fn build_append_entries_for(&self, peer_id: NodeId, up_to_index: u64) -> AppendEntriesRequest {
        let next = *self.next_index.get(&peer_id).unwrap_or(&(up_to_index + 1));
        let prev_log_index = next.saturating_sub(1);
        let prev_log_term = self.log.term_at(prev_log_index).unwrap_or(0);

        let entries: Vec<ProtoEntry> = self
            .log
            .entries_from(next)
            .into_iter()
            .filter(|e| e.index <= up_to_index)
            .map(|e| ProtoEntry { term: e.term, index: e.index, data: e.data })
            .collect();

        AppendEntriesRequest {
            term: self.ps.current_term,
            leader_id: self.id.clone(),
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.commit_index,
        }
    }

    // ── Heartbeat (leader) ────────────────────────────────────────────────────

    async fn send_heartbeats(&mut self) {
        let futures: Vec<_> = self
            .peers
            .iter()
            .map(|peer| {
                let peer = peer.clone();
                let req = AppendEntriesRequest {
                    term: self.ps.current_term,
                    leader_id: self.id.clone(),
                    prev_log_index: self.log.last_index(),
                    prev_log_term: self.log.last_term(),
                    entries: vec![],
                    leader_commit: self.commit_index,
                };
                async move { peer.append_entries(req).await }
            })
            .collect();

        let results = join_all(futures).await;
        for result in results {
            if let Ok(resp) = result {
                if resp.term > self.ps.current_term {
                    let _ = self.ps.advance_term(resp.term);
                    self.become_follower_with_leader(resp.term, None);
                    return;
                }
            }
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn random_election_timeout(config: &ClusterConfig) -> Duration {
    let min = config.election_timeout_min.as_millis() as u64;
    let max = config.election_timeout_max.as_millis() as u64;
    let ms = rand::rng().random_range(min..=max);
    Duration::from_millis(ms)
}
