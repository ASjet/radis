tonic::include_proto!("raft");

pub mod config;
mod context;
mod log;
mod service;
pub mod state;

pub use raft_client::RaftClient;
pub use raft_server::{Raft, RaftServer};

pub use context::LogIndex;
pub use log::Persister;
pub use service::RaftService;
pub use state::Term;
