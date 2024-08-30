use super::context::LogIndex;
use super::state::Term;
use super::Log;
use anyhow::Result;
use async_trait::async_trait;
use log::{debug, error, info};
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::mpsc;

#[async_trait]
pub trait Persister: Sync + Send {
    /// Reading WAL since the last snapshot
    /// Read a single log each time, return `Ok(None)` on EOF
    async fn read_wal(&mut self) -> Result<Option<(Term, Vec<u8>)>>;
    async fn write_wal(&mut self, term: Term, data: &[u8]) -> Result<()>;

    async fn read_snapshot(&self) -> Result<Option<(LogIndex, Vec<u8>)>>;
    async fn write_snapshot(&mut self, last_index: LogIndex, data: &[u8]) -> Result<()>;
}

#[derive(Debug)]
pub struct InnerLog {
    term: Term,
    data: Arc<Vec<u8>>,
}

impl InnerLog {
    pub fn new(term: Term, data: Vec<u8>) -> Self {
        Self {
            term,
            data: Arc::new(data),
        }
    }
}

pub struct LogManager {
    commit_ch: mpsc::Sender<Arc<Vec<u8>>>,
    commit_index: LogIndex,
    snapshot_index: LogIndex,
    logs: Vec<InnerLog>,
    #[allow(dead_code)]
    snapshot: Option<Vec<u8>>,

    persister: Option<Box<dyn Persister>>,
}

impl LogManager {
    pub fn new(commit_ch: mpsc::Sender<Arc<Vec<u8>>>) -> Self {
        Self {
            commit_ch,
            commit_index: 0,
            snapshot_index: 0,
            logs: vec![InnerLog::new(0, vec![])],
            snapshot: None,
            persister: None,
        }
    }

    pub async fn setup_persister(&mut self, persister: Box<dyn Persister>) -> Result<()> {
        info!(target: "raft::persist", "setup persister");
        self.persister = Some(persister);
        self.replay_wal().await
    }

    async fn replay_wal(&mut self) -> Result<()> {
        let p = match self.persister.as_mut() {
            Some(p) => p,
            None => return Ok(()),
        };

        if let Some((index, data)) = p.read_snapshot().await? {
            info!(target: "raft::persist",
                size = data.len(),
                last_index = index;
                "restore snapshot"
            );
            self.snapshot_index = index;
            self.snapshot = Some(data);
        }

        while let Some((term, command)) = p.read_wal().await? {
            self.logs.push(InnerLog::new(term, command));
        }
        info!(target: "raft::persist",
            logs = self.logs.len();
            "replay wal"
        );
        Ok(())
    }

    pub fn append(&mut self, term: Term, log: Vec<u8>) -> LogIndex {
        self.logs.push(InnerLog::new(term, log));
        self.snapshot_index + self.logs.len()
    }

    pub fn since(&self, start: usize) -> Vec<Log> {
        let offset = start + self.snapshot_index as usize;
        self.logs[start - self.snapshot_index as usize..]
            .iter()
            .enumerate()
            .map(|(i, log)| Log {
                index: (i + offset) as u64,
                term: log.term,
                command: log.data.deref().clone(),
            })
            .collect()
    }

    pub fn latest(&self) -> Option<(LogIndex, Term)> {
        self.logs
            .last()
            .map(|log| (self.snapshot_index + self.logs.len() - 1, log.term))
    }

    pub fn term(&self, index: LogIndex) -> Option<Term> {
        if index < self.snapshot_index {
            return None;
        }
        let index = (index - self.snapshot_index) as usize;
        if index >= self.logs.len() {
            return None;
        }
        Some(self.logs[index].term)
    }

    pub fn first_log_at_term(&self, term: Term) -> Option<LogIndex> {
        self.logs
            .iter()
            .position(|log| log.term == term)
            .map(|index| index + self.snapshot_index)
    }

    pub fn delete_since(&mut self, index: LogIndex) -> usize {
        if index < self.snapshot_index {
            return 0;
        }
        let index = (index - self.snapshot_index) as usize;
        let deleted = self.logs.drain(index..).count();
        debug!(target: "raft::log",
            deleted = deleted;
            "delete logs[{}..]", index
        );
        deleted
    }

    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }

    pub fn snapshot_index(&self) -> LogIndex {
        self.snapshot_index
    }

    pub async fn commit(&mut self, index: LogIndex) {
        if index <= self.commit_index {
            return;
        }

        info!(target: "raft::log",
            "commit logs[{}..{}]",
            self.commit_index, index
        );
        // Since commit_index is the index that already committed,
        // we need to start from commit_index + 1
        let start = (self.commit_index - self.snapshot_index + 1) as usize;
        let end = (index - self.snapshot_index + 1) as usize;
        for (i, log) in if end >= self.logs.len() {
            &self.logs[start..]
        } else {
            &self.logs[start..end]
        }
        .iter()
        .enumerate()
        {
            if let Some(persister) = self.persister.as_mut() {
                if let Err(e) = persister.write_wal(log.term, &log.data).await {
                    self.commit_index += i;
                    error!(target: "raft::log",
                        // error:err = e.into();
                        "write wal at log[{}] error: {}",
                        self.commit_index+1, e
                    );
                    return;
                }
            }
            if let Err(e) = self.commit_ch.send(log.data.clone()).await {
                self.commit_index += i;
                error!(target: "raft::log",
                    // error:err = e.into();
                    "commit log[{}] error: {}",
                    self.commit_index+1, e
                );
                return;
            }
        }
        info!(target: "raft::log",
            before = self.commit_index,
            after = index;
            "update commit index"
        );
        self.commit_index = index;
    }
}
