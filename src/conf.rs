use anyhow::Result;
use raft::config::Config as RaftConfig;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Config {
    pub listen_addr: String,
    pub raft: RaftConfig,
}

impl Config {
    pub fn from_path(path: &str) -> Result<Config> {
        let file = std::fs::read_to_string(path)?;
        let cfg = toml::from_str(&file)?;
        Ok(cfg)
    }
}
