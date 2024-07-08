use super::candidate::CandidateState;
use super::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply,
    RequestVoteArgs, RequestVoteReply,
};
use super::{LogIndex, PeerID, RaftContext, Role, State, Term};
use crate::raft::config;
use log::{debug, info};
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Serialize)]
pub struct FollowerState {
    term: Term,
    follow: Option<PeerID>,
}

impl FollowerState {
    pub fn new(term: Term, follow: Option<PeerID>) -> Arc<Box<dyn State>> {
        Arc::new(Box::new(Self { term, follow }))
    }

    /// Check if the candidate's log is more up-to-date
    fn is_request_valid(&self, _ctx: RaftContext, _args: &RequestVoteArgs) -> bool {
        // TODO: implement this
        true
    }

    /// Handle entries from leader, return if any conflict appears
    fn handle_entries(
        &self,
        _ctx: RaftContext,
        _args: AppendEntriesArgs,
    ) -> Option<(Term, LogIndex)> {
        // TODO: append logs and commit
        None
    }
}

#[tonic::async_trait]
impl State for FollowerState {
    fn term(&self) -> Term {
        self.term
    }

    fn role(&self) -> Role {
        Role::Follower
    }

    fn following(&self) -> Option<PeerID> {
        self.follow.clone()
    }

    async fn setup_timer(&self, ctx: RaftContext) {
        let timeout = config::follower_timeout();
        let ctx = ctx.read().await;
        ctx.reset_timeout(timeout).await;
        ctx.stop_tick().await;
        debug!(target: "raft::state",
            state:serde = self,
            timeout:serde = timeout,
            tick = "stop";
            "setup timer"
        );
    }

    async fn request_vote_logic(
        &self,
        ctx: RaftContext,
        args: RequestVoteArgs,
    ) -> (RequestVoteReply, Option<Arc<Box<dyn State>>>) {
        let (grant, new_state) = match &self.follow {
            Some(l) if *l == args.candidate_id => {
                // Already voted for this candidate
                (true, None)
            }
            Some(_) => {
                // Already voted for other candidate, reject
                (false, None)
            }
            None => {
                // Not voted for any candidate yet
                if self.is_request_valid(ctx.clone(), &args) {
                    // Vote for this candidate
                    (
                        true,
                        Some(FollowerState::new(args.term, Some(args.candidate_id))),
                    )
                } else {
                    // Reject this candidate
                    (false, None)
                }
            }
        };

        if grant {
            let timeout = config::follower_timeout();
            ctx.read().await.reset_timeout(timeout).await;
            debug!(target: "raft::timer",
                state:serde = self,
                timeout:serde = timeout;
                "reset timeout timer"
            );
        }

        (
            RequestVoteReply {
                term: self.term,
                granted: grant,
            },
            new_state,
        )
    }

    async fn append_entries_logic(
        &self,
        ctx: RaftContext,
        args: AppendEntriesArgs,
    ) -> (AppendEntriesReply, Option<Arc<Box<dyn State>>>) {
        let (success, new_state) = match &self.follow {
            Some(l) if *l == args.leader_id => {
                // Following this leader
                // TODO: append entries to context
                debug!(target: "raft::rpc",
                    state:serde = self,
                    term = args.term,
                    leader = args.leader_id;
                    "recv heartbeat"
                );
                (true, None)
            }
            Some(_) => {
                // Following other leader, reject
                (false, None)
            }
            None => {
                // Not following any leader
                (
                    true,
                    Some(FollowerState::new(args.term, Some(args.leader_id.clone()))),
                )
            }
        };

        let reply = if success {
            let timeout = config::follower_timeout();
            ctx.read().await.reset_timeout(timeout).await;
            debug!(target: "raft::timer",
                state:serde = self,
                timeout:serde = timeout;
                "reset timeout timer"
            );
            if let Some((conflict_term, conflict_index)) = self.handle_entries(ctx, args) {
                AppendEntriesReply {
                    term: self.term,
                    success: false,
                    conflict_term,
                    conflict_index,
                }
            } else {
                AppendEntriesReply {
                    term: self.term,
                    success: true,
                    conflict_term: 0,
                    conflict_index: 0,
                }
            }
        } else {
            // TODO: set latest log' index and term
            AppendEntriesReply {
                term: self.term,
                success: false,
                conflict_term: 0,
                conflict_index: 0,
            }
        };
        (reply, new_state)
    }

    async fn install_snapshot_logic(
        &self,
        _ctx: RaftContext,
        _args: InstallSnapshotArgs,
    ) -> (InstallSnapshotReply, Option<Arc<Box<dyn State>>>) {
        (InstallSnapshotReply::default(), None)
    }

    async fn on_timeout(&self, _ctx: RaftContext) -> Option<Arc<Box<dyn State>>> {
        info!(target: "raft::state",
            state:serde = self;
            "follower timeout, become candidate"
        );
        Some(CandidateState::new(self.term + 1))
    }

    async fn on_tick(&self, _ctx: RaftContext) -> Option<Arc<Box<dyn State>>> {
        None
    }
}
