use super::context::Context;
use super::state::{self, State};
use super::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply, RaftServer,
    RequestVoteArgs, RequestVoteReply,
};
use super::{Raft, RaftClient};
use crate::conf::Config;
use anyhow::Result;
use log::{info, trace};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct RaftService {
    id: String,
    listen_addr: String,
    context: Arc<RwLock<Context>>,
    state: Arc<Mutex<Arc<Box<dyn State>>>>,
}

impl RaftService {
    pub fn new(cfg: Config) -> Self {
        let Config {
            id, listen_addr, ..
        } = cfg.clone();
        let (context, state) = state::init(cfg);
        RaftService {
            id,
            listen_addr,
            context,
            state,
        }
    }

    pub fn context(&self) -> Arc<RwLock<Context>> {
        self.context.clone()
    }

    pub fn state(&self) -> Arc<Mutex<Arc<Box<dyn State>>>> {
        self.state.clone()
    }

    pub async fn serve(&self) -> Result<()> {
        let addr = self.listen_addr.parse()?;
        info!(target: "raft::service", id = self.id; "raft gRPC server listening on {addr}");
        Server::builder()
            .add_service(RaftServer::new(self.clone()))
            .serve(addr)
            .await?;
        Ok(())
    }

    pub async fn serve_with_shutdown<F: std::future::Future<Output = ()>>(
        &self,
        f: F,
    ) -> Result<()> {
        let addr = self.listen_addr.parse()?;
        info!(target: "raft::service", id = self.id; "raft gRPC server listening on {addr}");
        Server::builder()
            .add_service(RaftServer::new(self.clone()))
            .serve_with_shutdown(addr, f)
            .await?;
        Ok(())
    }
}

#[tonic::async_trait]
impl Raft for RaftService {
    async fn request_vote(
        &self,
        request: Request<RequestVoteArgs>,
    ) -> Result<Response<RequestVoteReply>, Status> {
        let request = request.into_inner();
        trace!(
            target: "raft::rpc",
            term = request.term,
            candidate_id = request.candidate_id,
            last_log_index = request.last_log_index,
            last_log_term = request.last_log_term;
            "received RequestVote request",
        );
        let state = self.state.lock().await;
        let (resp, new_state) = state
            .handle_request_vote(self.context.clone(), request)
            .await;
        state::transition(state, new_state, self.context.clone()).await;
        Ok(Response::new(resp))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesArgs>,
    ) -> Result<Response<AppendEntriesReply>, Status> {
        let request = request.into_inner();
        trace!(
            target: "raft::rpc",
            term = request.term,
            leader_id = request.leader_id,
            prev_log_index = request.prev_log_index,
            prev_log_term = request.prev_log_term,
            entries = request.entries.len();
            "received AppendEntries request",
        );
        let state = self.state.lock().await;
        let (resp, new_state) = state
            .handle_append_entries(self.context.clone(), request)
            .await;
        state::transition(state, new_state, self.context.clone()).await;
        Ok(Response::new(resp))
    }

    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotArgs>,
    ) -> Result<Response<InstallSnapshotReply>, Status> {
        let request = request.into_inner();
        trace!(
            target: "raft::rpc",
            term = request.term,
            leader_id = request.leader_id,
            last_included_index = request.last_included_index,
            last_included_term = request.last_included_term,
            snapshot_size = request.snapshot.len();
            "received InstallSnapshot request",
        );
        let state = self.state.lock().await;
        let (resp, new_state) = state
            .handle_install_snapshot(self.context.clone(), request)
            .await;
        state::transition(state, new_state, self.context.clone()).await;
        Ok(Response::new(resp))
    }
}

pub struct PeerClient {
    endpoint: Endpoint,
    cli: Option<RaftClient<Channel>>,
}

impl PeerClient {
    pub fn new(addr: &str, timeout: Duration) -> Self {
        PeerClient {
            endpoint: Channel::builder(addr.parse().unwrap()).timeout(timeout),
            cli: None,
        }
    }

    pub fn url(&self) -> String {
        self.endpoint.uri().to_string()
    }

    async fn connect(&mut self) -> Result<&mut RaftClient<Channel>, Status> {
        if self.cli.is_none() {
            let channel = self
                .endpoint
                .connect()
                .await
                .map_err(|e| Status::unavailable(e.to_string()))?;
            // Once connect() returns successfully, the connection
            // reliability is handled by the underlying gRPC library.
            info!(target: "raft::rpc",
                peer_addr = self.endpoint.uri().to_string();
                "connected to peer"
            );
            self.cli = Some(RaftClient::new(channel));
        }

        Ok(self.cli.as_mut().unwrap())
    }

    pub async fn request_vote(
        &mut self,
        args: RequestVoteArgs,
    ) -> Result<RequestVoteReply, Status> {
        let resp = self
            .connect()
            .await?
            .request_vote(Request::new(args))
            .await?;
        Ok(resp.into_inner())
    }

    pub async fn append_entries(
        &mut self,
        args: AppendEntriesArgs,
    ) -> Result<AppendEntriesReply, Status> {
        let resp = self
            .connect()
            .await?
            .append_entries(Request::new(args))
            .await?;
        Ok(resp.into_inner())
    }

    pub async fn install_snapshot(
        &mut self,
        args: InstallSnapshotArgs,
    ) -> Result<InstallSnapshotReply, Status> {
        let resp = self
            .connect()
            .await?
            .install_snapshot(Request::new(args))
            .await?;
        Ok(resp.into_inner())
    }
}
