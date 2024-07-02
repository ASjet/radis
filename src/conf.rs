use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Config {
    pub id: String,
    pub listen_addr: String,
    pub peer_addrs: Vec<String>,
}
