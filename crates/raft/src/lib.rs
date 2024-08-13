tonic::include_proto!("raft");
pub use raft_client::RaftClient;
pub use raft_server::{Raft, RaftServer};

pub mod config;
mod context;
mod log;
mod service;
pub mod state;

pub use service::RaftService;
