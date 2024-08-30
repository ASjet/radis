use super::config::Config;
use super::context::Context;
use super::state::{self, State};
use super::Persister;
use super::{
    AppendCommandArgs, AppendCommandReply, AppendEntriesArgs, AppendEntriesReply,
    InstallSnapshotArgs, InstallSnapshotReply, NodeInfoArgs, NodeInfoReply, Raft, RaftClient,
    RaftServer, RequestVoteArgs, RequestVoteReply,
};
use anyhow::Result;
use async_trait::async_trait;
use log::{info, trace};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, RwLock};
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct RaftService {
    id: String,
    listen_addr: String,
    context: Arc<RwLock<Context>>,
    state: Arc<Mutex<Arc<Box<dyn State>>>>,

    close_ch: Arc<Mutex<Option<mpsc::Sender<()>>>>,
}

impl RaftService {
    pub fn new(cfg: Config, commit_ch: mpsc::Sender<Arc<Vec<u8>>>) -> Self {
        let Config {
            id,
            rpc_host: listen_addr,
            ..
        } = cfg.clone();
        let (context, state) = state::init(cfg, commit_ch);
        RaftService {
            id,
            listen_addr,
            context,
            state,
            close_ch: Arc::new(Mutex::new(None)),
        }
    }

    pub fn context(&self) -> Arc<RwLock<Context>> {
        self.context.clone()
    }

    pub fn state(&self) -> Arc<Mutex<Arc<Box<dyn State>>>> {
        self.state.clone()
    }

    /// Setup persister for wal and snapshot.
    /// This method must be called before `RaftService::serve()`
    pub async fn setup_persister(&self, persister: Box<dyn Persister>) -> Result<()> {
        self.context.write().await.setup_persister(persister).await
    }

    pub async fn serve(&self) -> Result<()> {
        let (close_tx, mut close_rx) = mpsc::channel::<()>(1);
        *self.close_ch.lock().await = Some(close_tx);
        let addr = self.listen_addr.parse()?;
        info!(target: "raft::service", id = self.id; "raft gRPC server listening on {addr}");
        Server::builder()
            .add_service(RaftServer::new(self.clone()))
            .serve_with_shutdown(addr, async move {
                close_rx.recv().await;
            })
            .await?;
        Ok(())
    }

    pub async fn append_command(&self, cmd: Vec<u8>) -> Result<()> {
        info!(target: "raft::service", id = self.id; "append command");
        let state = self.state.lock().await;
        match state.role() {
            state::Role::Follower => {
                if let Some(leader_id) = state.following() {
                    let leader = self
                        .context
                        .read()
                        .await
                        .get_peer_by_id(&leader_id)
                        .ok_or(anyhow::anyhow!("unknown leader id"))?;
                    drop(state);
                    let resp = leader
                        .lock()
                        .await
                        .append_command(AppendCommandArgs { command: cmd })
                        .await?;
                    if resp.success {
                        return Ok(());
                    } else {
                        return Err(anyhow::anyhow!(resp.error));
                    }
                }
            }
            state::Role::Candidate => {}
            state::Role::Leader => {
                let new_state = state.on_command(self.context.clone(), cmd).await?;
                if state::transition(state, new_state, self.context.clone()).await {
                    return Err(anyhow::anyhow!("not leader"));
                } else {
                    return Ok(());
                }
            }
        }
        return Err(anyhow::anyhow!("still in election"));
    }

    pub async fn close(&self) {
        if let Some(ch) = self.close_ch.lock().await.take() {
            ch.send(()).await.unwrap();
        }
        let ctx = self.context.read().await;
        ctx.cancel_timeout().await;
        ctx.stop_tick().await;
    }
}

#[async_trait]
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

    async fn node_info(&self, _: Request<NodeInfoArgs>) -> Result<Response<NodeInfoReply>, Status> {
        let state = self.state.lock().await;
        Ok(Response::new(NodeInfoReply {
            id: self.id.clone(),
            term: state.term(),
            role: state.role().to_string(),
        }))
    }

    async fn append_command(
        &self,
        request: Request<AppendCommandArgs>,
    ) -> Result<Response<AppendCommandReply>, Status> {
        let request = request.into_inner();
        trace!(
            target: "raft::rpc",
            "received AppendCommand request",
        );
        self.append_command(request.command)
            .await
            .map(|_| {
                Ok(Response::new(AppendCommandReply {
                    success: true,
                    error: "".to_string(),
                }))
            })
            .map_err(|e| Status::aborted(e.to_string()))?
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

    pub async fn node_info(&mut self, args: NodeInfoArgs) -> Result<NodeInfoReply, Status> {
        let resp = self.connect().await?.node_info(Request::new(args)).await?;
        Ok(resp.into_inner())
    }

    pub async fn append_command(
        &mut self,
        args: AppendCommandArgs,
    ) -> Result<AppendCommandReply, Status> {
        let resp = self
            .connect()
            .await?
            .append_command(Request::new(args))
            .await?
            .into_inner();
        if resp.success {
            Ok(resp)
        } else {
            Err(Status::failed_precondition(resp.error))
        }
    }
}
