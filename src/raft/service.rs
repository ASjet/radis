use super::context::Context;
use super::state::{self, FollowerState, State};
use super::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply, RaftServer,
    RequestVoteArgs, RequestVoteReply,
};
use super::{Raft, RaftClient};
use crate::conf::Config;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, RwLock};
use tonic::transport::{self, Channel, Endpoint, Server};
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct RaftService {
    context: Arc<RwLock<Context>>,
    state: Arc<Mutex<Arc<Box<dyn State>>>>,
}

impl RaftService {
    pub fn new(cfg: Config) -> Self {
        let (timeout_tx, timeout_rx) = mpsc::channel(1);
        let (tick_tx, tick_rx) = mpsc::channel(1);
        let context = Arc::new(RwLock::new(Context::new(cfg, timeout_tx, tick_tx)));
        let state = Arc::new(Mutex::new(FollowerState::new(0, None)));
        state::handle_timer(state.clone(), context.clone(), timeout_rx, tick_rx);
        RaftService { context, state }
    }

    pub fn context(&self) -> Arc<RwLock<Context>> {
        self.context.clone()
    }

    pub fn state(&self) -> Arc<Mutex<Arc<Box<dyn State>>>> {
        self.state.clone()
    }

    pub async fn serve(&self, addr: SocketAddr) -> Result<(), transport::Error> {
        Server::builder()
            .add_service(RaftServer::new(self.clone()))
            .serve(addr)
            .await
    }
}

#[tonic::async_trait]
impl Raft for RaftService {
    async fn request_vote(
        &self,
        request: Request<RequestVoteArgs>,
    ) -> Result<Response<RequestVoteReply>, Status> {
        let state = self.state.lock().await;
        let (resp, new_state) = state
            .handle_request_vote(self.context.clone(), request.into_inner())
            .await;
        state::transition(state, new_state, self.context.clone()).await;
        Ok(Response::new(resp))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesArgs>,
    ) -> Result<Response<AppendEntriesReply>, Status> {
        let state = self.state.lock().await;
        let (resp, new_state) = state
            .handle_append_entries(self.context.clone(), request.into_inner())
            .await;
        state::transition(state, new_state, self.context.clone()).await;
        Ok(Response::new(resp))
    }

    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotArgs>,
    ) -> Result<Response<InstallSnapshotReply>, Status> {
        let state = self.state.lock().await;
        let (resp, new_state) = state
            .handle_install_snapshot(self.context.clone(), request.into_inner())
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
