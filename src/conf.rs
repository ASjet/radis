use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Config {
    pub id: String,
    pub redis_addr: String,
    pub raft_rpc_addr: String,
    pub raft_peers: HashMap<String, String>,
}

impl Config {
    pub fn from_path(path: &str) -> Result<Config> {
        let file = std::fs::read_to_string(path)?;
        let cfg = toml::from_str(&file)?;
        Ok(cfg)
    }

    pub fn builder() -> Builder {
        Builder::default()
    }
}

pub struct Builder {
    raft_rpc_host: String,
    raft_peer_host: String,
    raft_base_port: u16,
    name_prefix: String,
    peers: i32,
}

impl Builder {
    pub fn default() -> Builder {
        Builder {
            raft_rpc_host: "0.0.0.0".to_string(),
            raft_peer_host: "http://localhost".to_string(),
            raft_base_port: 50000,
            name_prefix: "node".to_string(),
            peers: 3,
        }
    }

    pub fn listen_host(mut self, host: &str) -> Builder {
        self.raft_rpc_host = host.to_string();
        self
    }

    pub fn peer_host(mut self, host: &str) -> Builder {
        self.raft_peer_host = host.to_string();
        self
    }

    pub fn base_port(mut self, port: u16) -> Builder {
        self.raft_base_port = port;
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
            redis_addr: String,
            rpc_listen_host: String,
            rpc_peer_host: String,
        }
        let nodes: Vec<Node> = (0..self.peers)
            .map(|id| Node {
                id: format!("{}{}", self.name_prefix, id),
                redis_addr: join_host_port(&self.raft_rpc_host, 63790 + id as u16),
                rpc_listen_host: join_host_port(
                    &self.raft_rpc_host,
                    self.raft_base_port + id as u16,
                ),
                rpc_peer_host: join_host_port(
                    &self.raft_peer_host,
                    self.raft_base_port + id as u16,
                ),
            })
            .collect();

        Vec::from_iter(nodes.iter().map(|node| {
            Config {
                id: node.id.clone(),
                redis_addr: node.redis_addr.clone(),
                raft_rpc_addr: node.rpc_listen_host.clone(),
                raft_peers: HashMap::from_iter(
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
