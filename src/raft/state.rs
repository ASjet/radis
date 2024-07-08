use super::context::Context;
use super::context::{LogIndex, PeerID};
use super::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply,
    RequestVoteArgs, RequestVoteReply,
};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, MutexGuard, RwLock};

mod candidate;
mod follower;
mod leader;

pub use follower::FollowerState;

type RaftContext = Arc<RwLock<Context>>;
pub type Term = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[tonic::async_trait]
pub trait State: Sync + Send + Debug {
    fn term(&self) -> Term;
    fn role(&self) -> Role;
    async fn setup_timer(&self, ctx: RaftContext);
    /// Call with holding the lock of state
    async fn on_timeout(&self, ctx: RaftContext) -> Option<Arc<Box<dyn State>>>;
    /// Call without holding the lock of state
    async fn on_tick(&self, ctx: RaftContext) -> Option<Arc<Box<dyn State>>>;

    async fn request_vote_logic(
        &self,
        ctx: RaftContext,
        args: RequestVoteArgs,
    ) -> (RequestVoteReply, Option<Arc<Box<dyn State>>>);

    async fn append_entries_logic(
        &self,
        ctx: RaftContext,
        args: AppendEntriesArgs,
    ) -> (AppendEntriesReply, Option<Arc<Box<dyn State>>>);

    async fn install_snapshot_logic(
        &self,
        ctx: RaftContext,
        args: InstallSnapshotArgs,
    ) -> (InstallSnapshotReply, Option<Arc<Box<dyn State>>>);

    async fn handle_request_vote(
        &self,
        ctx: RaftContext,
        args: RequestVoteArgs,
    ) -> (RequestVoteReply, Option<Arc<Box<dyn State>>>) {
        if args.term < self.term() {
            // Reply false if term < currentTerm (§5.1)
            return (
                RequestVoteReply {
                    term: self.term(),
                    granted: false,
                },
                None,
            );
        }

        if args.term > self.term() {
            // If RPC request or response contains term T > currentTerm:
            // set currentTerm = T, convert to follower (§5.1)
            let state = follower::FollowerState::new(args.term, Some(args.candidate_id.clone()));
            let (reply, new_state) = state.request_vote_logic(ctx, args).await;
            return (reply, new_state.or(Some(state)));
        }

        self.request_vote_logic(ctx, args).await
    }

    async fn handle_append_entries(
        &self,
        ctx: RaftContext,
        args: AppendEntriesArgs,
    ) -> (AppendEntriesReply, Option<Arc<Box<dyn State>>>) {
        if args.term < self.term() {
            // Reply false if term < currentTerm (§5.1)
            return (
                AppendEntriesReply {
                    term: self.term(),
                    success: false,
                    conflict_index: 0,
                    conflict_term: 0,
                },
                None,
            );
        }

        if args.term > self.term() {
            // If RPC request or response contains term T > currentTerm:
            // set currentTerm = T, convert to follower (§5.1)
            let state = follower::FollowerState::new(args.term, Some(args.leader_id.clone()));
            let (reply, new_state) = state.append_entries_logic(ctx, args).await;
            return (reply, new_state.or(Some(state)));
        }

        self.append_entries_logic(ctx, args).await
    }

    async fn handle_install_snapshot(
        &self,
        ctx: RaftContext,
        args: InstallSnapshotArgs,
    ) -> (InstallSnapshotReply, Option<Arc<Box<dyn State>>>) {
        if args.term < self.term() {
            // Reply immediately if term < currentTerm
            return (InstallSnapshotReply { term: self.term() }, None);
        }

        if args.term > self.term() {
            // If RPC request or response contains term T > currentTerm:
            // set currentTerm = T, convert to follower (§5.1)
            let state = follower::FollowerState::new(args.term, Some(args.leader_id.clone()));
            let (reply, new_state) = state.install_snapshot_logic(ctx, args).await;
            return (reply, new_state.or(Some(state)));
        }

        self.install_snapshot_logic(ctx, args).await
    }
}

pub fn handle_timer(
    state: Arc<Mutex<Arc<Box<dyn State>>>>,
    ctx: Arc<RwLock<Context>>,
    mut timeout_rx: mpsc::Receiver<()>,
    mut tick_rx: mpsc::Receiver<()>,
) {
    tokio::spawn(async move {
        ctx.read().await.init_timer().await;
        state.lock().await.setup_timer(ctx.clone()).await;
        loop {
            tokio::select! {
                _ = timeout_rx.recv() => {
                    let ctx = ctx.clone();
                    let state = state.clone();
                    tokio::spawn(async move {
                        let state = state.lock().await;
                        let new_state = state.on_timeout(ctx.clone()).await;
                        transition(state, new_state, ctx.clone()).await;
                    });
                }
                _ = tick_rx.recv() => {
                    let ctx = ctx.clone();
                    let state = state.clone();
                    tokio::spawn(async move {
                        let s = state.lock().await.clone();
                        let new_state = s.on_tick(ctx.clone()).await;
                        let guard = state.lock().await;
                        if s.term() == guard.term() && s.role() == guard.role(){
                            transition(guard, new_state, ctx.clone()).await;
                        }
                    });
                }
            }
        }
    });
}

pub async fn transition<'a>(
    mut state: MutexGuard<'a, Arc<Box<dyn State>>>,
    new_state: Option<Arc<Box<dyn State>>>,
    ctx: Arc<RwLock<Context>>,
) {
    if let Some(new_state) = new_state {
        println!("from {:?} transition to {:?}", *state, new_state);
        *state = new_state;
        state.setup_timer(ctx).await;
    }
}
