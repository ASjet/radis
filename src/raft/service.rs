use super::context::Context;
use super::state::{State, TodoState};
use super::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply,
    RequestVoteArgs, RequestVoteReply,
};
use super::{Raft, RaftClient};
use crate::conf::Config;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

pub struct RaftService {
    context: Arc<RwLock<Context>>,
    state: Arc<Mutex<Box<dyn State>>>,
}

impl RaftService {
    pub fn new(cfg: Config) -> Self {
        RaftService {
            context: Arc::new(RwLock::new(Context::new(cfg))),
            state: Arc::new(Mutex::new(Box::new(TodoState {}))),
        }
    }

    pub fn context(&self) -> Arc<RwLock<Context>> {
        self.context.clone()
    }
}

#[tonic::async_trait]
impl Raft for RaftService {
    async fn request_vote(
        &self,
        _request: Request<RequestVoteArgs>,
    ) -> Result<Response<RequestVoteReply>, Status> {
        let mut state = self.state.lock().await;
        let (resp, transition) = state
            .handle_request_vote(self.context.clone(), _request.into_inner())
            .await;

        if let Some(new_state) = transition {
            *state = new_state;
        }

        Ok(Response::new(resp))
    }

    async fn append_entries(
        &self,
        _request: Request<AppendEntriesArgs>,
    ) -> Result<Response<AppendEntriesReply>, Status> {
        let mut state = self.state.lock().await;
        let (resp, transition) = state
            .handle_append_entries(self.context.clone(), _request.into_inner())
            .await;

        if let Some(new_state) = transition {
            *state = new_state;
        }

        Ok(Response::new(resp))
    }

    async fn install_snapshot(
        &self,
        _request: Request<InstallSnapshotArgs>,
    ) -> Result<Response<InstallSnapshotReply>, Status> {
        let mut state = self.state.lock().await;
        let (resp, transition) = state
            .handle_install_snapshot(self.context.clone(), _request.into_inner())
            .await;

        if let Some(new_state) = transition {
            *state = new_state;
        }

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

    async fn connect(&mut self) -> Result<&mut RaftClient<Channel>, Status> {
        if self.cli.is_none() {
            let channel = self
                .endpoint
                .connect()
                .await
                .map_err(|e| Status::unavailable(e.to_string()))?;
            // Once connect() returns successfully, the connection
            // reliability is handled by the underlying gRPC library.
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
