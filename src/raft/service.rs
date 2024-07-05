use super::context::Context;
pub use super::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply,
    RequestVoteArgs, RequestVoteReply,
};
pub use super::{Raft, RaftClient};
use crate::conf::Config;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

pub struct RaftService {
    context: Arc<RwLock<Context>>,
}

impl RaftService {
    pub fn new(cfg: Config) -> Self {
        RaftService {
            context: Arc::new(RwLock::new(Context::new(cfg))),
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
        todo!()
    }

    async fn append_entries(
        &self,
        _request: Request<AppendEntriesArgs>,
    ) -> Result<Response<AppendEntriesReply>, Status> {
        todo!()
    }

    async fn install_snapshot(
        &self,
        _request: Request<InstallSnapshotArgs>,
    ) -> Result<Response<InstallSnapshotReply>, Status> {
        todo!()
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

    async fn connect(&mut self) {
        if self.cli.is_some() {
            return;
        }

        let channel = loop {
            match self.endpoint.connect().await {
                Ok(channel) => break channel,
                Err(e) => {
                    eprintln!("failed to connect to peer {}: {}", self.endpoint.uri(), e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        };

        self.cli = Some(RaftClient::new(channel));
    }

    pub async fn request_vote(
        &mut self,
        args: RequestVoteArgs,
    ) -> Result<RequestVoteReply, Status> {
        self.connect().await;
        let resp = self
            .cli
            .as_mut()
            .unwrap()
            .request_vote(Request::new(args))
            .await?;
        Ok(resp.into_inner())
    }

    pub async fn append_entries(
        &mut self,
        args: AppendEntriesArgs,
    ) -> Result<AppendEntriesReply, Status> {
        self.connect().await;
        let resp = self
            .cli
            .as_mut()
            .unwrap()
            .append_entries(Request::new(args))
            .await?;
        Ok(resp.into_inner())
    }

    pub async fn install_snapshot(
        &mut self,
        args: InstallSnapshotArgs,
    ) -> Result<InstallSnapshotReply, Status> {
        self.connect().await;
        let resp = self
            .cli
            .as_mut()
            .unwrap()
            .install_snapshot(Request::new(args))
            .await?;
        Ok(resp.into_inner())
    }
}
