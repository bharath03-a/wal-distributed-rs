//! gRPC service implementations.
//!
//! Two services are exposed:
//! - [`RaftServiceImpl`] — handles `AppendEntries` and `RequestVote` from peers.
//! - [`WalServiceImpl`]  — handles `Write` and `ReadFrom` from clients.
//!
//! Both forward requests to the [`RaftHandle`] actor and return its response.

use tonic::{Request, Response, Status};

use crate::{
    proto::wal::{
        raft_service_server::RaftService,
        wal_service_server::WalService,
        AppendEntriesRequest, AppendEntriesResponse,
        ReadFromRequest, ReadFromResponse,
        RequestVoteRequest, RequestVoteResponse,
        WriteRequest, WriteResponse,
        LogEntry as ProtoEntry,
    },
    raft::RaftHandle,
};

// ── RaftService ───────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct RaftServiceImpl {
    handle: RaftHandle,
}

impl RaftServiceImpl {
    pub fn new(handle: RaftHandle) -> Self {
        Self { handle }
    }
}

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let resp = self
            .handle
            .append_entries(request.into_inner())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(resp))
    }

    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        let resp = self
            .handle
            .request_vote(request.into_inner())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(resp))
    }
}

// ── WalService ────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct WalServiceImpl {
    handle: RaftHandle,
}

impl WalServiceImpl {
    pub fn new(handle: RaftHandle) -> Self {
        Self { handle }
    }
}

#[tonic::async_trait]
impl WalService for WalServiceImpl {
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let data = request.into_inner().data;
        match self.handle.write(data).await {
            Ok(index) => Ok(Response::new(WriteResponse {
                success: true,
                index,
                leader_hint: String::new(),
            })),
            Err(crate::error::RaftError::NotLeader { hint }) => {
                Ok(Response::new(WriteResponse {
                    success: false,
                    index: 0,
                    leader_hint: hint.unwrap_or_default(),
                }))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn read_from(
        &self,
        request: Request<ReadFromRequest>,
    ) -> Result<Response<ReadFromResponse>, Status> {
        let from_index = request.into_inner().from_index;
        let entries = self
            .handle
            .read_from(from_index)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let proto_entries: Vec<ProtoEntry> = entries
            .into_iter()
            .map(|e| ProtoEntry { term: e.term, index: e.index, data: e.data })
            .collect();

        Ok(Response::new(ReadFromResponse { entries: proto_entries }))
    }
}
