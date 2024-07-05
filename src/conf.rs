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
}
