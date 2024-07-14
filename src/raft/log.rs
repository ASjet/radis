use super::context::LogIndex;
use super::state::Term;
use super::Log;
use std::ops::Deref;
use std::sync::Arc;

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
    commit_index: LogIndex,
    snapshot_index: LogIndex,
    logs: Vec<InnerLog>,
    #[allow(dead_code)]
    snapshot: Option<Vec<u8>>,
}

impl LogManager {
    pub fn new() -> Self {
        Self {
            commit_index: 0,
            snapshot_index: 0,
            logs: Vec::new(),
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
            .map(|log| (self.snapshot_index + self.logs.len() as u64, log.term))
    }

    pub fn term(&self, index: LogIndex) -> Option<Term> {
        let index = index as usize;
        if index < self.snapshot_index as usize {
            return None;
        }
        let index = index - self.snapshot_index as usize;
        if index >= self.logs.len() {
            return None;
        }
        Some(self.logs[index].term)
    }

    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }

    pub fn snapshot_index(&self) -> LogIndex {
        self.snapshot_index
    }
}
