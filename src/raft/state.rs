use super::context::Context;
use super::context::{LogIndex, PeerID};
use super::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply,
    RequestVoteArgs, RequestVoteReply,
};
use crate::conf::Config;
use log::{debug, info};
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, MutexGuard, RwLock};

mod candidate;
mod follower;
mod leader;

pub use follower::FollowerState;

type RaftContext = Arc<RwLock<Context>>;
pub type Term = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[tonic::async_trait]
pub trait State: Sync + Send + Debug {
    fn term(&self) -> Term;
    fn role(&self) -> Role;
    fn following(&self) -> Option<PeerID>;
    async fn setup(&self, ctx: RaftContext);
    /// Call with holding the lock of state
    async fn on_timeout(&self, ctx: RaftContext) -> Option<Arc<Box<dyn State>>>;
    /// Call without holding the lock of state
    async fn on_tick(&self, ctx: RaftContext) -> Option<Arc<Box<dyn State>>>;
    /// Append new command
    async fn on_command(
        &self,
        ctx: RaftContext,
        cmd: Vec<u8>,
    ) -> anyhow::Result<Option<Arc<Box<dyn State>>>>;

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
            debug!(target: "raft::state",
                rpc = "RequestVote",
                candidate_id = args.candidate_id,
                candidate_term = args.term,
                current_term = self.term(),
                reason = "invalid candidate term";
                "reject rpc request"
            );
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
            info!(target: "raft::state",
                rpc = "RequestVote",
                candidate_term = args.term,
                current_term = self.term();
                "meet higher term, convert to follower"
            );
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
            debug!(target: "raft::state",
                rpc = "AppendEntries",
                leader_id = args.leader_id,
                leader_term = args.term,
                current_term = self.term(),
                reason = "invalid leader term";
                "reject rpc request"
            );
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
            info!(target: "raft::state",
                rpc = "AppendEntries",
                leader_term = args.term,
                current_term = self.term();
                "meet higher term, convert to follower"
            );
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
            debug!(target: "raft::state",
                rpc = "InstallSnapshot",
                leader_id = args.leader_id,
                leader_term = args.term,
                current_term = self.term(),
                reason = "invalid leader term";
                "reject rpc request"
            );
            return (InstallSnapshotReply { term: self.term() }, None);
        }

        if args.term > self.term() {
            // If RPC request or response contains term T > currentTerm:
            // set currentTerm = T, convert to follower (§5.1)
            info!(target: "raft::state",
                rpc = "InstallSnapshot",
                leader_term = args.term,
                current_term = self.term();
                "meet higher term, convert to follower"
            );
            let state = follower::FollowerState::new(args.term, Some(args.leader_id.clone()));
            let (reply, new_state) = state.install_snapshot_logic(ctx, args).await;
            return (reply, new_state.or(Some(state)));
        }

        self.install_snapshot_logic(ctx, args).await
    }
}

impl Serialize for Box<dyn State> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("term", &self.term())?;
        map.serialize_entry("role", &self.role())?;
        map.serialize_entry("following", &self.following())?;
        map.end()
    }
}

pub fn init(cfg: Config) -> (Arc<RwLock<Context>>, Arc<Mutex<Arc<Box<dyn State>>>>) {
    let (timeout_tx, timeout_rx) = mpsc::channel(1);
    let (tick_tx, tick_rx) = mpsc::channel(1);
    let context = Arc::new(RwLock::new(Context::new(cfg, timeout_tx, tick_tx)));

    let init_state = FollowerState::new(0, None);
    info!(target: "raft::state",
        state:serde = (&init_state as &Box<dyn State>);
        "init raft state"
    );
    let state = Arc::new(Mutex::new(init_state));
    handle_timer(state.clone(), context.clone(), timeout_rx, tick_rx);
    (context, state)
}

pub async fn transition<'a>(
    mut state: MutexGuard<'a, Arc<Box<dyn State>>>,
    new_state: Option<Arc<Box<dyn State>>>,
    ctx: Arc<RwLock<Context>>,
) -> bool {
    if let Some(new_state) = new_state {
        info!(target: "raft::state",
            old_state:serde = (&*state as &Box<dyn State>),
            new_state:serde = (&new_state as &Box<dyn State>);
            "state transition occurred"
        );
        *state = new_state;
        state.setup(ctx).await;
        return true;
    }
    false
}

fn handle_timer(
    state: Arc<Mutex<Arc<Box<dyn State>>>>,
    ctx: Arc<RwLock<Context>>,
    mut timeout_rx: mpsc::Receiver<()>,
    mut tick_rx: mpsc::Receiver<()>,
) {
    tokio::spawn(async move {
        ctx.read().await.init_timer().await;
        state.lock().await.setup(ctx.clone()).await;
        loop {
            tokio::select! {
                _ = timeout_rx.recv() => {
                    let ctx = ctx.clone();
                    let state = state.clone();
                    tokio::spawn(async move {
                        let state = state.lock().await;
                        debug!(target: "raft::timer",
                            event = "timeout",
                            state:serde = (&*state as &Box<dyn State>);
                            "timer event occurred"
                        );
                        let new_state = state.on_timeout(ctx.clone()).await;
                        transition(state, new_state, ctx.clone()).await;
                    });
                }
                _ = tick_rx.recv() => {
                    let ctx = ctx.clone();
                    let state = state.clone();
                    tokio::spawn(async move {
                        let s = state.lock().await.clone();
                        debug!(target: "raft::timer",
                            event = "timeout",
                            state:serde = (&s as &Box<dyn State>);
                            "timer event occurred"
                        );
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

#[tokio::test]
async fn reject_lower_term() {
    let (ctx, _) = init(Config::builder().peers(1).build().pop().unwrap());
    let state = FollowerState::new(2, None);

    assert_eq!(2, state.term());

    let peer = "test".to_string();

    let (reply, new_state) = state
        .handle_request_vote(
            ctx.clone(),
            RequestVoteArgs {
                term: 1,
                candidate_id: peer.clone(),
                last_log_index: 0,
                last_log_term: 0,
            },
        )
        .await;
    assert_eq!(
        RequestVoteReply {
            term: 2,
            granted: false
        },
        reply
    );
    assert!(new_state.is_none());

    let (reply, new_state) = state
        .handle_append_entries(
            ctx.clone(),
            AppendEntriesArgs {
                term: 1,
                leader_id: peer.clone(),
                prev_log_index: 0,
                prev_log_term: 0,
                leader_commit: 0,
                entries: vec![],
            },
        )
        .await;
    assert_eq!(
        AppendEntriesReply {
            term: 2,
            success: false,
            conflict_index: 0,
            conflict_term: 0,
        },
        reply
    );
    assert!(new_state.is_none());

    let (reply, new_state) = state
        .handle_install_snapshot(
            ctx.clone(),
            InstallSnapshotArgs {
                term: 1,
                leader_id: peer.clone(),
                last_included_index: 0,
                last_included_term: 0,
                snapshot: vec![],
            },
        )
        .await;
    assert_eq!(InstallSnapshotReply { term: 2 }, reply);
    assert!(new_state.is_none());
}
