use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Config {
    pub id: String,
    pub listen_addr: String,
    pub peer_addrs: Vec<String>,
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
    listen_host: String,
    peer_host: String,
    base_port: u16,
    name_prefix: String,
    peers: i32,
}

impl Builder {
    pub fn default() -> Builder {
        Builder {
            listen_host: "0.0.0.0".to_string(),
            peer_host: "http://localhost".to_string(),
            base_port: 50000,
            name_prefix: "node".to_string(),
            peers: 3,
        }
    }

    pub fn listen_host(mut self, host: &str) -> Builder {
        self.listen_host = host.to_string();
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
        let mut cfgs = Vec::new();
        for id in 0..self.peers {
            let cfg = Config {
                id: format!("{}{}", self.name_prefix, id),
                listen_addr: join_host_port(&self.listen_host, self.base_port + id as u16),
                peer_addrs: make_peer_addrs(&self.peer_host, self.base_port, self.peers, id),
            };
            cfgs.push(cfg);
        }
        cfgs
    }
}

fn join_host_port(host: &str, port: u16) -> String {
    format!("{}:{}", host, port)
}

fn make_peer_addrs(host: &str, base_port: u16, n_peer: i32, id: i32) -> Vec<String> {
    (0..n_peer)
        .filter(|peer| *peer != id)
        .map(|peer| join_host_port(host, base_port + peer as u16))
        .collect()
}
