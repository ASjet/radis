use super::config::REQUEST_TIMEOUT;
use super::log::LogManager;
use super::service::PeerClient;
use crate::conf::Config;
use crate::timer::{OneshotTimer, PeriodicTimer};
use log::debug;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;

pub type PeerID = String;
pub type Peer = usize;
pub type LogIndex = u64;

pub struct Context {
    id: String,
    peers: Vec<Arc<Mutex<PeerClient>>>,

    log: LogManager,
    peer_next_index: Vec<AtomicU64>,
    peer_commit_index: Vec<AtomicU64>,

    timeout: Arc<OneshotTimer>,
    tick: Arc<PeriodicTimer>,
}

impl Context {
    pub fn new(cfg: Config, timeout_event: Sender<()>, tick_event: Sender<()>) -> Self {
        let timeout = Duration::from_millis(REQUEST_TIMEOUT);
        let Config {
            id,
            listen_addr: _,
            peer_addrs,
        } = cfg;
        let peers = peer_addrs.len();

        Self {
            id,
            peers: peer_addrs
                .iter()
                .enumerate()
                .map(|(i, addr)| {
                    debug!(target: "raft::context",
                        peer_index = i,
                        peer_addr = addr,
                        timeout:serde = timeout;
                        "init peer client"
                    );
                    Arc::new(Mutex::new(PeerClient::new(addr, timeout)))
                })
                .collect(),

            log: LogManager::new(),
            peer_next_index: (0..peers).map(|_| AtomicU64::new(0)).collect(),
            peer_commit_index: (0..peers).map(|_| AtomicU64::new(0)).collect(),

            timeout: Arc::new(OneshotTimer::new(timeout_event)),
            tick: Arc::new(PeriodicTimer::new(tick_event)),
        }
    }

    pub async fn init_timer(&self) {
        let timeout = self.timeout.clone();
        let tick = self.tick.clone();
        tokio::spawn(async move {
            debug!(target: "raft::context", timer = "timeout"; "start timer");
            timeout.start().await;
        });
        tokio::spawn(async move {
            debug!(target: "raft::context", timer = "tick"; "start timer");
            tick.start().await;
        });
    }

    pub fn me(&self) -> &PeerID {
        &self.id
    }

    pub fn get_peer(&self, peer: Peer) -> Arc<Mutex<PeerClient>> {
        self.peers[peer].clone()
    }

    pub fn peers(&self) -> usize {
        self.peers.len()
    }

    pub fn majority(&self) -> usize {
        self.peers.len() / 2 + 1
    }

    pub async fn reset_timeout(&self, timeout: Duration) {
        self.timeout.reset(timeout).await;
    }

    pub async fn cancel_timeout(&self) {
        self.timeout.cancel().await;
    }

    pub async fn reset_tick(&self, interval: Duration) {
        self.tick.reset(interval).await;
    }

    pub async fn stop_tick(&self) {
        self.tick.stop().await;
    }

    pub fn log(&self) -> &LogManager {
        &self.log
    }

    pub fn log_mut(&mut self) -> &mut LogManager {
        &mut self.log
    }

    pub fn peer_next_index(&self, peer: Peer) -> &AtomicU64 {
        &self.peer_next_index[peer]
    }

    pub fn peer_commit_index(&self, peer: Peer) -> &AtomicU64 {
        &self.peer_commit_index[peer]
    }
}
