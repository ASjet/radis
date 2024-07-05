pub mod conf;

pub mod raft {
    tonic::include_proto!("raft");
    pub use raft_client::RaftClient;
    pub use raft_server::{Raft, RaftServer};

    mod service;
    pub use service::RaftService;
}
