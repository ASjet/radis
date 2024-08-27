use super::context::LogIndex;
use super::state::Term;
use super::Log;
use log::{debug, info};
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::mpsc;

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
}

impl LogManager {
    pub fn new(commit_ch: mpsc::Sender<Arc<Vec<u8>>>) -> Self {
        Self {
            commit_ch,
            commit_index: 0,
            snapshot_index: 0,
            logs: vec![InnerLog::new(0, vec![])],
            snapshot: None,
        }
    }

    pub fn append(&mut self, term: Term, log: Vec<u8>) -> LogIndex {
        self.logs.push(InnerLog::new(term, log));
        self.snapshot_index + self.logs.len() as u64
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
            .map(|log| (self.snapshot_index + self.logs.len() as u64 - 1, log.term))
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
            .map(|index| index as u64 + self.snapshot_index)
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
        for log in if end >= self.logs.len() {
            &self.logs[start..]
        } else {
            &self.logs[start..end]
        }
        .iter()
        {
            self.commit_ch.send(log.data.clone()).await.unwrap();
        }
        self.commit_index = index;
    }
}
