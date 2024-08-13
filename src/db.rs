use crate::conf::Config;
use crate::raft::RaftService;
use anyhow::Result;
use bytes::Bytes;
use log::{debug, info};
use mini_redis::{Command, Connection, Frame};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, RwLock};

type Db = Arc<RwLock<HashMap<String, Bytes>>>;

#[derive(Debug, Serialize, Deserialize)]
struct Record {
    key: String,
    value: Bytes,
}

pub struct RedisServer {
    listen_addr: String,
    raft_server: RaftService,

    db: Db,
    commit_rx: mpsc::Receiver<Arc<Vec<u8>>>,
}

impl RedisServer {
    pub fn new(cfg: Config) -> Self {
        let (commit_tx, commit_rx) = mpsc::channel(1);
        let listen_addr = cfg.listen_addr.clone();
        let raft_server = RaftService::new(cfg.raft, commit_tx);
        Self {
            listen_addr,
            raft_server,
            db: Arc::new(RwLock::new(HashMap::new())),
            commit_rx,
        }
    }

    pub async fn serve(&mut self) -> Result<()> {
        let raft_srv = self.raft_server.clone();
        tokio::spawn(async move {
            raft_srv.serve().await.unwrap();
        });
        info!(target: "db::service", "radis server listening on {}", self.listen_addr);
        let listener = TcpListener::bind(&self.listen_addr).await?;
        loop {
            tokio::select! {
                accept = listener.accept() => {
                    if let Ok((socket, _)) = accept {
                        let db = self.db.clone();
                        let raft_srv = self.raft_server.clone();
                        tokio::spawn(async move {
                            handle_request(raft_srv, socket, db).await;
                        });
                    }
                }
                commit = self.commit_rx.recv() => {
                    if let Some(commit) = commit {
                        self.handle_commit(commit).await;
                    }
                }
            }
        }
    }

    async fn handle_commit(&self, commit: Arc<Vec<u8>>) {
        let record: Record = serde_json::from_slice(commit.as_slice()).unwrap();
        let mut db = self.db.write().await;
        let rc = db.insert(record.key, record.value);
        debug!("commit: {:?}", rc);
    }
}

async fn handle_request(raft_srv: RaftService, socket: TcpStream, db: Db) {
    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Command::Get(cmd) => {
                let db = db.read().await;
                if let Some(value) = db.get(cmd.key()) {
                    Frame::Bulk(value.clone().into())
                } else {
                    Frame::Null
                }
            }
            Command::Set(cmd) => {
                let record = Record {
                    key: cmd.key().to_string(),
                    value: cmd.value().clone(),
                };
                let command = serde_json::to_vec(&record).unwrap();
                debug!("append command: {:?}", command);
                match raft_srv.append_command(command).await {
                    Ok(_) => Frame::Simple("OK".to_string()),
                    Err(e) => Frame::Error(e.to_string()),
                }
            }
            Command::Unknown(_) => Frame::Error("unknown command".to_string()),
            cmd => Frame::Error(format!("unimplemented {:?}", cmd)),
        };

        connection.write_frame(&response).await.unwrap();
    }
}
