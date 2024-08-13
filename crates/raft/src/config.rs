#![allow(dead_code)]
use rand::{self, Rng};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::{ops::Add, time::Duration};

pub const REQUEST_TIMEOUT: u64 = 1000;
pub const HEARTBEAT_INTERVAL: i64 = 100;
pub const REQUEST_VOTE_INTERVAL: i64 = 150;
pub const ELECTION_TIMEOUT: i64 = 150;
pub const ELECTION_TIMEOUT_DELTA: i64 = 75;
pub const HEARTBEAT_TIMEOUT: i64 = 150;
pub const HEARTBEAT_TIMEOUT_DELTA: i64 = 75;

pub fn random_backoff(base: i64, delta: i64) -> Duration {
    let mut rng = rand::thread_rng();
    Duration::from_millis(base.add(rng.gen_range(-delta / 2..delta / 2)) as u64)
}

pub fn follower_timeout() -> Duration {
    random_backoff(ELECTION_TIMEOUT, ELECTION_TIMEOUT_DELTA)
}

pub fn candidate_timeout() -> Duration {
    random_backoff(ELECTION_TIMEOUT, ELECTION_TIMEOUT_DELTA)
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Config {
    pub id: String,
    pub rpc_host: String,
    pub peers: HashMap<String, String>,
}

impl Config {
    pub fn builder() -> Builder {
        Builder::default()
    }
}

pub struct Builder {
    rpc_host: String,
    peer_host: String,
    base_port: u16,
    name_prefix: String,
    peers: i32,
}

impl Builder {
    pub fn default() -> Builder {
        Builder {
            rpc_host: "0.0.0.0".to_string(),
            peer_host: "http://localhost".to_string(),
            base_port: 50000,
            name_prefix: "node".to_string(),
            peers: 3,
        }
    }

    pub fn listen_host(mut self, host: &str) -> Builder {
        self.rpc_host = host.to_string();
        self
    }

    pub fn peer_host(mut self, host: &str) -> Builder {
        self.peer_host = host.to_string();
        self
    }

    pub fn base_port(mut self, port: u16) -> Builder {
        self.base_port = port;
        self
    }

    pub fn name_prefix(mut self, prefix: &str) -> Builder {
        self.name_prefix = prefix.to_string();
        self
    }

    pub fn peers(mut self, n: i32) -> Builder {
        self.peers = n;
        self
    }

    pub fn build(self) -> Vec<Config> {
        struct Node {
            id: String,
            rpc_listen_host: String,
            rpc_peer_host: String,
        }
        let nodes: Vec<Node> = (0..self.peers)
            .map(|id| Node {
                id: format!("{}{}", self.name_prefix, id),
                rpc_listen_host: join_host_port(&self.rpc_host, self.base_port + id as u16),
                rpc_peer_host: join_host_port(&self.peer_host, self.base_port + id as u16),
            })
            .collect();

        Vec::from_iter(nodes.iter().map(|node| {
            Config {
                id: node.id.clone(),
                rpc_host: node.rpc_listen_host.clone(),
                peers: HashMap::from_iter(
                    nodes
                        .iter()
                        .filter(|peer| peer.id != node.id)
                        .map(|peer| (peer.id.clone(), peer.rpc_peer_host.clone())),
                ),
            }
        }))
    }
}

fn join_host_port(host: &str, port: u16) -> String {
    format!("{}:{}", host, port)
}
