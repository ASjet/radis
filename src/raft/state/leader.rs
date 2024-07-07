use super::{
    AppendEntriesArgs, AppendEntriesReply, FollowerState, InstallSnapshotArgs,
    InstallSnapshotReply, RequestVoteArgs, RequestVoteReply,
};
use super::{RaftContext, Role, State, Term};
use crate::raft::config;
use futures::future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};

#[derive(Debug)]
pub struct LeaderState {
    term: Term,
}

impl LeaderState {
    pub fn new(term: Term) -> Arc<Box<dyn State>> {
        println!("[{}] won this election", term);
        Arc::new(Box::new(Self { term }))
    }
}

#[tonic::async_trait]
impl State for LeaderState {
    fn term(&self) -> Term {
        self.term
    }

    fn role(&self) -> Role {
        Role::Candidate
    }

    async fn setup_timer(&self, ctx: RaftContext) {
        let ctx = ctx.read().await;
        ctx.cancel_timeout().await;
        ctx.reset_tick(Duration::from_millis(config::HEARTBEAT_INTERVAL as u64))
            .await;
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
                            }
                        }
                        println!("send heartbeat to peer {}: {:?}", peer_url, resp);
                    }
                    Err(e) => println!("call peer {} append_entries(hb) failed: {:?}", peer_url, e),
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
            Some(FollowerState::new(higher_term, None))
        } else {
            None
        }
    }
}
