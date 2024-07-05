use super::context::Context;
pub use super::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply,
    RequestVoteArgs, RequestVoteReply,
};
use std::sync::Arc;
use tokio::sync::RwLock;

type RaftContext = Arc<RwLock<Context>>;

#[tonic::async_trait]
pub trait State: Sync + Send {
    async fn handle_request_vote(
        &self,
        _ctx: RaftContext,
        _args: RequestVoteArgs,
    ) -> (RequestVoteReply, Option<Box<dyn State>>) {
        (RequestVoteReply::default(), None)
    }

    async fn handle_append_entries(
        &self,
        _ctx: RaftContext,
        _args: AppendEntriesArgs,
    ) -> (AppendEntriesReply, Option<Box<dyn State>>) {
        (AppendEntriesReply::default(), None)
    }

    async fn handle_install_snapshot(
        &self,
        _ctx: RaftContext,
        _args: InstallSnapshotArgs,
    ) -> (InstallSnapshotReply, Option<Box<dyn State>>) {
        (InstallSnapshotReply::default(), None)
    }
}

pub struct TodoState {}

impl State for TodoState {}
