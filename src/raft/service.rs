#![allow(dead_code, unused)]
pub use super::Raft;
pub use super::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply,
    RequestVoteArgs, RequestVoteReply,
};
use crate::conf::Config;
use tonic::{Request, Response, Status};

pub struct RaftService {
    me: String,
    peers: Vec<String>,
}

impl RaftService {
    pub fn new(cfg: Config) -> Self {
        let Config {
            id,
            listen_addr: _,
            peer_addrs,
        } = cfg;

        RaftService {
            me: id,
            peers: peer_addrs,
        }
    }
}

#[tonic::async_trait]
impl Raft for RaftService {
    async fn request_vote(
        &self,
        request: Request<RequestVoteArgs>,
    ) -> Result<Response<RequestVoteReply>, Status> {
        unimplemented!()
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesArgs>,
    ) -> Result<Response<AppendEntriesReply>, Status> {
        unimplemented!()
    }

    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotArgs>,
    ) -> Result<Response<InstallSnapshotReply>, Status> {
        unimplemented!()
    }
}
