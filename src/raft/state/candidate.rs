use super::follower::FollowerState;
use super::leader::LeaderState;
use super::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply,
    RequestVoteArgs, RequestVoteReply,
};
use super::{RaftContext, Role, State, Term};
use crate::raft::config;
use futures::future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};

#[derive(Debug)]
pub struct CandidateState {
    term: Term,
}

impl CandidateState {
    pub fn new(term: Term) -> Arc<Box<dyn State>> {
        println!("[{}] become candidate", term);
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

    async fn setup_timer(&self, ctx: RaftContext) {
        let ctx = ctx.read().await;
        let dura = config::candidate_timeout();
        ctx.reset_timeout(dura).await;
        ctx.reset_tick(Duration::from_millis(config::REQUEST_VOTE_INTERVAL as u64))
            .await;
        println!("[{}] After {:?} start next election...", self.term, dura);
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
        _ctx: RaftContext,
        args: AppendEntriesArgs,
    ) -> (AppendEntriesReply, Option<Arc<Box<dyn State>>>) {
        println!(
            "[{}] peer {} won this election, revert to follower",
            self.term, args.leader_id
        );
        (
            AppendEntriesReply {
                term: self.term,
                success: true,
                conflict_term: 0,
                conflict_index: 0,
            },
            Some(FollowerState::new(args.term, Some(args.leader_id))),
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
                match resp {
                    Ok(resp) => {
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
                    Err(e) => println!(
                        "[{}] call peer {} request_vote failed: {:?}",
                        term,
                        peer_cli.lock().await.url(),
                        e
                    ),
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

        if *lock.lock().await >= majority {
            Some(LeaderState::new(self.term))
        } else {
            let higher_term = *higher_term.lock().await;
            if higher_term > self.term {
                Some(FollowerState::new(higher_term, None))
            } else {
                None
            }
        }
    }
}
