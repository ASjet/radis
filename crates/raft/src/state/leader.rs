use super::{
    AppendEntriesArgs, AppendEntriesReply, FollowerState, InstallSnapshotArgs,
    InstallSnapshotReply, RequestVoteArgs, RequestVoteReply,
};
use super::{PeerID, RaftContext, Role, State, Term};
use crate::config;
use async_trait::async_trait;
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

    async fn sync_peers(&self, ctx: RaftContext) -> Option<Arc<Box<dyn State>>> {
        let peers = ctx.read().await.peers();
        let higher_term = Arc::new(Mutex::new(self.term));
        let notify = Arc::new(Notify::new());
        let mut requests = Vec::with_capacity(peers);

        for peer in 0..peers {
            let ctx = ctx.clone();
            let (peer_cli, peer_next_index, mut args) = {
                let ctx = ctx.read().await;
                let args = AppendEntriesArgs {
                    term: self.term,
                    leader_id: ctx.me().clone(),
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: vec![],
                    leader_commit: ctx.log().commit_index(),
                };

                (ctx.get_peer(peer), ctx.peer_next_index(peer), args)
            };
            let notify = notify.clone();
            let higher_term = higher_term.clone();

            requests.push(tokio::spawn(async move {
                // Lock peer_next_index until the end of the request,
                // so next rpc can send accumulated new entries during this rpc
                let mut peer_next_index = peer_next_index.lock().await;
                {
                    let ctx = ctx.read().await;
                    let log = ctx.log();
                    let prev_index = *peer_next_index - 1;
                    match log.term(prev_index) {
                        Some(term) => {
                            args.prev_log_term = term;
                            args.prev_log_index = prev_index;
                        }
                        None => {
                            args.prev_log_term = args.term;
                            args.prev_log_index = log.snapshot_index();
                        }
                    }
                    args.entries = log.since(*peer_next_index as usize);
                }
                let resp = peer_cli.lock().await.append_entries(args).await;
                let peer_url = peer_cli.lock().await.url();
                match resp {
                    Ok(resp) => {
                        debug!(target: "raft::rpc",
                            term = resp.term,
                            peer = peer_url;
                            "call peer rpc AppendEntries"
                        );
                        if !resp.success {
                            let mut higher_term = higher_term.lock().await;
                            if resp.term > *higher_term {
                                *higher_term = resp.term;
                                notify.notify_one();
                                return;
                            } else {
                                // There must be a conflict
                                error!(target: "raft::rpc",
                                    term = resp.term,
                                    peer = peer_url,
                                    conflict_index = resp.last_log_index,
                                    conflict_term = resp.last_log_term;
                                    "meet peer log conflict"
                                );
                            }
                        } else {
                            // Update next index
                            debug!(target: "raft::rpc",
                                term = resp.term,
                                peer = peer_url,
                                last_log_index = resp.last_log_index,
                                last_log_term = resp.last_log_term;
                                "sync peer log"
                            );
                        }
                        *peer_next_index = resp.last_log_index + 1;
                        ctx.write()
                            .await
                            .update_peer_index(peer, resp.last_log_index)
                            .await;
                    }
                    Err(e) => {
                        error!(target: "raft::rpc",
                            error:err = e,
                            peer = peer_url;
                            "peer rpc AppendEntries error"
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

#[async_trait]
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

    async fn setup(&self, ctx: RaftContext) {
        let tick = Duration::from_millis(config::HEARTBEAT_INTERVAL as u64);
        let ctx = ctx.read().await;
        let init_next_index = ctx.log().latest().unwrap().0 + 1;
        for peer in 0..ctx.peers() {
            *ctx.peer_next_index(peer).lock().await = init_next_index;
        }
        ctx.cancel_timeout().await;
        ctx.reset_tick(tick).await;
        debug!(target: "raft::state",
            state:serde = self,
            timeout = "stop",
            tick:serde = tick;
            "setup timer"
        );
    }

    async fn on_command(
        &self,
        ctx: RaftContext,
        cmd: Vec<u8>,
    ) -> anyhow::Result<Option<Arc<Box<dyn State>>>> {
        let index = ctx.write().await.log_mut().append(self.term(), cmd);
        info!(target: "raft::state",
            state:serde = self,
            index = index;
            "command appended, wait sync to peers"
        );
        self.sync_peers(ctx).await;
        Ok(None)
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
        self.sync_peers(ctx).await
    }
}
