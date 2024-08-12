use super::config::REQUEST_TIMEOUT;
use super::log::LogManager;
use super::service::PeerClient;
use crate::conf::Config;
use crate::timer::{OneshotTimer, PeriodicTimer};
use log::debug;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::{Mutex, RwLock};

pub type PeerID = String;
pub type Peer = usize;
pub type LogIndex = u64;

pub struct Context {
    id: String,
    peers: Vec<Arc<Mutex<PeerClient>>>,
    id_map: Arc<HashMap<String, Peer>>,

    log: LogManager,
    peer_next_index: Vec<Arc<Mutex<LogIndex>>>,
    peer_sync_index: Vec<Arc<RwLock<LogIndex>>>,
    commit_ch: mpsc::Sender<Arc<Vec<u8>>>,

    timeout: Arc<OneshotTimer>,
    tick: Arc<PeriodicTimer>,
}

impl Context {
    pub fn new(
        cfg: Config,
        commit_ch: mpsc::Sender<Arc<Vec<u8>>>,
        timeout_event: Sender<()>,
        tick_event: Sender<()>,
    ) -> Self {
        let timeout = Duration::from_millis(REQUEST_TIMEOUT);
        let Config {
            id,
            redis_addr: _,
            raft_rpc_addr: _,
            raft_peers: peer_addrs,
        } = cfg;
        let n_peer = peer_addrs.len();

        let mut id_map = HashMap::with_capacity(n_peer);
        let mut peers = Vec::with_capacity(n_peer);
        for (i, (id, addr)) in peer_addrs.into_iter().enumerate() {
            debug!(target: "raft::context",
                peer_index = i,
                peer_id = id,
                peer_addr = addr,
                timeout:serde = timeout;
                "init peer client"
            );
            id_map.insert(id, i);
            peers.push(Arc::new(Mutex::new(PeerClient::new(&addr, timeout))));
        }

        Self {
            id,
            peers,
            id_map: Arc::new(id_map),
            log: LogManager::new(),
            peer_next_index: (0..n_peer).map(|_| Arc::new(Mutex::new(0))).collect(),
            peer_sync_index: (0..n_peer).map(|_| Arc::new(RwLock::new(0))).collect(),
            commit_ch,

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

    pub fn get_peer_by_id(&self, id: &str) -> Option<Arc<Mutex<PeerClient>>> {
        self.id_map.get(id).map(|&peer| self.peers[peer].clone())
    }

    pub fn peers(&self) -> usize {
        self.peers.len()
    }

    pub fn majority(&self) -> usize {
        self.peers.len() / 2
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

    pub async fn commit_log(&mut self, index: LogIndex) {
        self.log.commit(index, &self.commit_ch).await;
    }

    pub fn peer_next_index(&self, peer: Peer) -> Arc<Mutex<LogIndex>> {
        self.peer_next_index[peer].clone()
    }

    pub async fn update_peer_index(&mut self, peer: Peer, index: LogIndex) {
        *self.peer_sync_index[peer].write().await = index;

        let mut sync_indexes = vec![0; self.peers()];
        for (i, index) in self.peer_sync_index.iter().enumerate() {
            sync_indexes[i] = *index.read().await;
        }
        debug!("peer_sync_index: {:?}", sync_indexes);
        self.commit_log(majority_index(sync_indexes)).await;
    }
}

fn majority_index(mut indexes: Vec<LogIndex>) -> LogIndex {
    indexes.sort_unstable();
    let majority_index = indexes.len() / 2 - 1;
    indexes[majority_index]
}
