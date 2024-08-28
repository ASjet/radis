use super::{Persister, Term};
use anyhow::Result;
use async_trait::async_trait;

pub struct FilePersister {
    dir: String,
}

impl FilePersister {
    pub fn new(dir: &str) -> Self {
        Self {
            dir: String::from(dir),
        }
    }
}

#[async_trait]
impl Persister for FilePersister {
    async fn read_wal(&mut self) -> Result<Option<(Term, Vec<u8>)>> {
        todo!()
    }

    async fn write_wal(&mut self, term: Term, data: &[u8]) -> Result<()> {
        todo!()
    }

    async fn read_snapshot(&self) -> Result<Option<(usize, Vec<u8>)>> {
        todo!()
    }

    async fn write_snapshot(&mut self, last_index: usize, data: &[u8]) -> Result<()> {
        todo!()
    }
}
