use super::{
    AppendEntriesArgs, AppendEntriesReply, FollowerState, InstallSnapshotArgs,
    InstallSnapshotReply, RequestVoteArgs, RequestVoteReply,
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
pub struct LeaderState {
    term: Term,
}

impl LeaderState {
    pub fn new(term: Term) -> Arc<Box<dyn State>> {
        Arc::new(Box::new(Self { term }))
    }
}

#[tonic::async_trait]
impl State for LeaderState {
    fn term(&self) -> Term {
        self.term
    }

    fn role(&self) -> Role {
        Role::Leader
    }

    fn following(&self) -> Option<PeerID> {
        None
    }

    async fn setup_timer(&self, ctx: RaftContext) {
        let tick = Duration::from_millis(config::HEARTBEAT_INTERVAL as u64);
        let ctx = ctx.read().await;
        ctx.cancel_timeout().await;
        ctx.reset_tick(tick).await;
        debug!(target: "raft::state",
            state:serde = self,
            timeout = "stop",
            tick:serde = tick;
            "setup timer"
        );
    }

    async fn request_vote_logic(
        &self,
        _ctx: RaftContext,
        _args: RequestVoteArgs,
    ) -> (RequestVoteReply, Option<Arc<Box<dyn State>>>) {
        (RequestVoteReply::default(), None)
    }

    async fn append_entries_logic(
        &self,
        _ctx: RaftContext,
        _args: AppendEntriesArgs,
    ) -> (AppendEntriesReply, Option<Arc<Box<dyn State>>>) {
        (AppendEntriesReply::default(), None)
    }

    async fn install_snapshot_logic(
        &self,
        _ctx: RaftContext,
        _args: InstallSnapshotArgs,
    ) -> (InstallSnapshotReply, Option<Arc<Box<dyn State>>>) {
        (InstallSnapshotReply::default(), None)
    }

    async fn on_timeout(&self, _ctx: RaftContext) -> Option<Arc<Box<dyn State>>> {
        None
    }

    async fn on_tick(&self, ctx: RaftContext) -> Option<Arc<Box<dyn State>>> {
        let (peers, me) = {
            let ctx = ctx.read().await;
            (ctx.peers(), ctx.me().clone())
        };
        let higher_term = Arc::new(Mutex::new(self.term));
        let notify = Arc::new(Notify::new());
        let mut requests = Vec::with_capacity(peers);
        let args = AppendEntriesArgs {
            term: self.term,
            leader_id: me,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        for peer in 0..peers {
            let peer_cli = ctx.read().await.get_peer(peer);
            let notify = notify.clone();
            let args = args.clone();
            let higher_term = higher_term.clone();

            requests.push(tokio::spawn(async move {
                let resp = peer_cli.lock().await.append_entries(args).await;
                let peer_url = peer_cli.lock().await.url();
                match resp {
                    Ok(resp) => {
                        if !resp.success {
                            let mut higher_term = higher_term.lock().await;
                            if resp.term > *higher_term {
                                *higher_term = resp.term;
                                notify.notify_one();
                                return;
                            }
                        }
                        debug!(target: "raft::rpc",
                            term = resp.term,
                            peer = peer_url;
                            "send heartbeat to peer"
                        );
                    }
                    Err(e) => {
                        error!(target: "raft::rpc",
                            error:err = e,
                            peer = peer_url;
                            "call peer rpc AppendEntries(hb) error"
                        );
                    }
                }
            }));
        }

        let finish_notify = notify.clone();
        tokio::spawn(async move {
            future::join_all(requests).await;

            // Wake up the main task if all requests are done
            finish_notify.notify_one();
        });

        notify.notified().await;

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
