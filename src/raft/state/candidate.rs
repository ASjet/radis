use super::follower::FollowerState;
use super::leader::LeaderState;
use super::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply,
    RequestVoteArgs, RequestVoteReply,
};
use super::{PeerID, RaftContext, Role, State, Term};
use crate::raft::config;
use futures::future;
use log::{debug, error, info};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};

#[derive(Debug, Serialize)]
pub struct CandidateState {
    term: Term,
}

impl CandidateState {
    pub fn new(term: Term) -> Arc<Box<dyn State>> {
        Arc::new(Box::new(Self { term }))
    }
}

#[tonic::async_trait]
impl State for CandidateState {
    fn term(&self) -> Term {
        self.term
    }

    fn role(&self) -> Role {
        Role::Candidate
    }

    fn following(&self) -> Option<PeerID> {
        None
    }

    async fn setup(&self, ctx: RaftContext) {
        let timeout = config::candidate_timeout();
        let tick = Duration::from_millis(config::REQUEST_VOTE_INTERVAL as u64);
        let ctx = ctx.read().await;
        ctx.reset_timeout(timeout).await;
        ctx.reset_tick(tick).await;
        debug!(target: "raft::state",
            state:serde = self,
            timeout:serde = timeout,
            tick:serde = tick;
            "setup timer"
        );
    }

    async fn on_command(
        &self,
        _ctx: RaftContext,
        _cmd: Vec<u8>,
    ) -> anyhow::Result<Option<Arc<Box<dyn State>>>> {
        Err(anyhow::anyhow!("not leader"))
    }

    async fn request_vote_logic(
        &self,
        _ctx: RaftContext,
        _args: RequestVoteArgs,
    ) -> (RequestVoteReply, Option<Arc<Box<dyn State>>>) {
        (
            RequestVoteReply {
                term: self.term,
                granted: false,
            },
            None,
        )
    }

    async fn append_entries_logic(
        &self,
        ctx: RaftContext,
        args: AppendEntriesArgs,
    ) -> (AppendEntriesReply, Option<Arc<Box<dyn State>>>) {
        info!(target: "raft::state",
            state:serde = self,
            leader = args.leader_id;
            "other peer won current election, revert to follower"
        );
        let new_state = FollowerState::new(args.term.clone(), Some(args.leader_id.clone()));
        (
            new_state.handle_append_entries(ctx, args).await.0,
            Some(new_state),
        )
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
            "election timeout, start new election"
        );
        Some(CandidateState::new(self.term + 1))
    }

    async fn on_tick(&self, ctx: RaftContext) -> Option<Arc<Box<dyn State>>> {
        let (peers, majority, me) = {
            let ctx = ctx.read().await;
            (ctx.peers(), ctx.majority(), ctx.me().clone())
        };
        let higher_term = Arc::new(Mutex::new(self.term));
        let votes = Arc::new(Mutex::new(vec![false; peers]));
        let notify = Arc::new((Mutex::new(0), Notify::new()));
        let mut requests = Vec::with_capacity(peers);
        let args = RequestVoteArgs {
            term: self.term,
            candidate_id: me,
            last_log_index: 0,
            last_log_term: 0,
        };

        let term = self.term;

        for peer in 0..peers {
            let peer_cli = ctx.read().await.get_peer(peer);
            let votes = votes.clone();
            let win_notify = notify.clone();
            let args = args.clone();
            let higher_term = higher_term.clone();

            requests.push(tokio::spawn(async move {
                let resp = peer_cli.lock().await.request_vote(args).await;
                let peer_url = peer_cli.lock().await.url();
                match resp {
                    Ok(resp) => {
                        // TODO: prefetch peer next index
                        debug!(target: "raft::rpc",
                            term = resp.term,
                            peer = peer_url,
                            granted = resp.granted;
                            "request vote from peer"
                        );

                        if resp.granted {
                            let mut votes = votes.lock().await;
                            if votes[peer] {
                                return;
                            }
                            votes[peer] = true;
                        } else {
                            let mut higher_term = higher_term.lock().await;
                            if resp.term > *higher_term {
                                *higher_term = resp.term;
                                let (_, notify) = &*win_notify;
                                notify.notify_one();
                            }
                            return;
                        }

                        let (lock, notify) = &*win_notify;
                        let mut cnt = lock.lock().await;
                        *cnt += 1;
                        if *cnt >= majority {
                            // Wake up the main task if win this election
                            notify.notify_one();
                        }
                    }
                    Err(e) => {
                        error!(target: "raft::rpc",
                            error:err = e,
                            peer = peer_url,
                            term = term;
                            "call peer rpc RequestVote error"
                        );
                    }
                }
            }));
        }

        let finish_notify = notify.clone();
        tokio::spawn(async move {
            future::join_all(requests).await;

            let (_, notify) = &*finish_notify;
            // Wake up the main task if all requests are done
            notify.notify_one();
        });

        let (lock, notify) = &*notify;
        notify.notified().await;

        let vote = *lock.lock().await;
        if vote >= majority {
            info!(target: "raft::rpc",
                term = self.term,
                vote = vote,
                all = peers;
                "won current election"
            );
            Some(LeaderState::new(self.term))
        } else {
            let higher_term = *higher_term.lock().await;
            if higher_term > self.term {
                info!(target: "raft::rpc",
                    term = self.term,
                    new_term = higher_term;
                    "meet higher term, revert to follower"
                );
                Some(FollowerState::new(higher_term, None))
            } else {
                None
            }
        }
    }
}
