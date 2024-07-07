use super::config::REQUEST_TIMEOUT;
use super::service::PeerClient;
use crate::conf::Config;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub type PeerID = String;
pub type Peer = usize;
pub type LogIndex = u64;

pub struct Context {
    id: String,
    peers: Vec<Arc<Mutex<PeerClient>>>,
}

impl Context {
    pub fn new(cfg: Config) -> Self {
        let timeout = Duration::from_millis(REQUEST_TIMEOUT);
        let Config {
            id,
            listen_addr: _,
            peer_addrs,
        } = cfg;

        Self {
            id,
            peers: peer_addrs
                .iter()
                .map(|addr| Arc::new(Mutex::new(PeerClient::new(addr, timeout))))
                .collect(),
        }
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
}
