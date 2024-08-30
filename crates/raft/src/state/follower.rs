use super::candidate::CandidateState;
use super::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply,
    RequestVoteArgs, RequestVoteReply,
};
use super::{LogIndex, PeerID, RaftContext, Role, State, Term};
use crate::config;
use async_trait::async_trait;
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
    async fn is_request_valid(&self, ctx: RaftContext, args: &RequestVoteArgs) -> bool {
        let ctx = ctx.read().await;
        let log = ctx.log();
        let (latest_index, latest_term) = match log.latest() {
            Some(log) => log,
            None => return true,
        };

        // From 5.4.1:
        // If the logs have last entries with different terms, then
        // the log with the later term is more up-to-date.
        if args.last_log_term < latest_term {
            return false;
        }
        if args.last_log_term > latest_term {
            return true;
        }

        // From 5.4.1:
        // If the logs end with the same term, then
        // whichever log is longer is more up-to-date.
        return args.last_log_index as LogIndex >= latest_index;
    }

    /// Handle entries from leader, return if any conflict appears
    async fn handle_entries(
        &self,
        ctx: RaftContext,
        args: AppendEntriesArgs,
    ) -> Option<(Term, LogIndex)> {
        let mut ctx = ctx.write().await;
        let log = ctx.log_mut();
        let (latest_index, latest_term) = match log.latest() {
            Some(log) => log,
            None => (0, 0),
        };

        // Reply false if log doesn’t contain an entry at prevLogIndex
        // whose term matches prevLogTerm (§5.3)
        if args.prev_log_index as LogIndex > latest_index {
            info!(target: "raft::log",
                state:serde = self,
                prev_index = args.prev_log_index,
                latest_index = latest_index;
                "reject append logs: prev index is larger than last log index"
            );
            return Some((latest_term, latest_index));
        }

        let prev_term = match log.term(args.prev_log_index as LogIndex) {
            Some(term) => term,
            None => {
                info!(target: "raft::log",
                    state:serde = self,
                    prev_index = args.prev_log_index;
                    "reject append logs: prev index is already trimmed"
                );
                return Some((latest_term, latest_index));
            }
        };

        // If an existing entry conflicts with a new one (same index but different terms),
        // delete the existing entry and all that follow it (§5.3)
        if prev_term != args.prev_log_term {
            info!(target: "raft::log",
                state:serde = self,
                prev_term = prev_term,
                prev_index = args.prev_log_index;
                "reject append logs: prev term is conflict with last log term"
            );
            // Fallback the whole term once a time
            let term_start = log.first_log_at_term(prev_term).unwrap();
            log.delete_since(term_start);
            return Some((prev_term, args.prev_log_index as LogIndex));
        }

        log.delete_since(args.prev_log_index as LogIndex + 1);

        // Append any new entries not already in the log
        let entries = args.entries.len();
        if entries > 0 {
            args.entries.into_iter().for_each(|entry| {
                log.append(entry.term, entry.command);
            });
            info!(target: "raft::log",
                state:serde = self,
                prev_index = args.prev_log_index ,
                prev_term = args.prev_log_term,
                entries = entries;
                "append new log [{}..{}]",
                args.prev_log_index + 1,
                args.prev_log_index + 1 + entries as u64
            );
        }
        None
    }
}

#[async_trait]
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

    async fn setup(&self, ctx: RaftContext) {
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

    async fn on_command(
        &self,
        _ctx: RaftContext,
        _cmd: Vec<u8>,
    ) -> anyhow::Result<Option<Arc<Box<dyn State>>>> {
        unimplemented!();
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
                if self.is_request_valid(ctx.clone(), &args).await {
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
            let commit_index = args.leader_commit;
            if let Some((conflict_term, conflict_index)) =
                self.handle_entries(ctx.clone(), args).await
            {
                AppendEntriesReply {
                    term: self.term,
                    success: false,
                    last_log_index: conflict_index as u64,
                    last_log_term: conflict_term,
                }
            } else {
                let (last_log_index, last_log_term) = match ctx.read().await.log().latest() {
                    Some(log) => log,
                    None => (0, 0),
                };
                ctx.write().await.commit_log(commit_index as LogIndex).await;
                AppendEntriesReply {
                    term: self.term,
                    success: true,
                    last_log_index: last_log_index as u64,
                    last_log_term,
                }
            }
        } else {
            let (last_log_index, last_log_term) = match ctx.read().await.log().latest() {
                Some(log) => log,
                None => (0, 0),
            };
            AppendEntriesReply {
                term: self.term,
                success: false,
                last_log_index: last_log_index as u64,
                last_log_term,
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

#[tokio::test]
async fn vote_request() {
    let (commit_tx, _) = tokio::sync::mpsc::channel(1);
    let (ctx, state) = super::init(
        super::Config::builder().peers(1).build().pop().unwrap(),
        commit_tx,
    );
    let state = state.lock().await;

    assert_eq!(state.term(), 0);

    // RequestVote from a candidate with higher term
    let candidate1 = "update_candidate".to_string();
    let (reply, new_state) = state
        .handle_request_vote(
            ctx.clone(),
            RequestVoteArgs {
                term: 2,
                candidate_id: candidate1.clone(),
                last_log_index: 0,
                last_log_term: 0,
            },
        )
        .await;
    assert_eq!(
        RequestVoteReply {
            term: 2,
            granted: true
        },
        reply
    );
    let new_state = new_state.unwrap();
    assert_eq!(Role::Follower, new_state.role());
    assert_eq!(2 as Term, new_state.term());
    assert_eq!(Some(candidate1), new_state.following());
}

#[tokio::test]
async fn append_entries() {
    let (commit_tx, _) = tokio::sync::mpsc::channel(1);
    let (ctx, state) = super::init(
        super::Config::builder().peers(1).build().pop().unwrap(),
        commit_tx,
    );
    let state = state.lock().await;

    assert_eq!(state.term(), 0);

    // AppendEntries from a candidate with higher term
    let leader1 = "update_leader".to_string();
    let (reply, new_state) = state
        .handle_append_entries(
            ctx.clone(),
            AppendEntriesArgs {
                term: 2,
                leader_id: leader1.clone(),
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
            success: true,
            last_log_index: 0,
            last_log_term: 0,
        },
        reply
    );
    let new_state = new_state.unwrap();
    assert_eq!(Role::Follower, new_state.role());
    assert_eq!(2 as Term, new_state.term());
    assert_eq!(Some(leader1), new_state.following());
}
