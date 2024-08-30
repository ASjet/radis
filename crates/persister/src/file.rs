use super::{Persister, Term};
use anyhow::Result;
use async_trait::async_trait;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};

pub struct FilePersister {
    dir: String,

    wal_w: Option<File>,
    wal_r: Option<File>,
}

impl FilePersister {
    pub fn new(dir: &str) -> Self {
        Self {
            dir: String::from(dir),
            wal_w: None,
            wal_r: None,
        }
    }

    async fn wal_write_handle(&mut self) -> &mut File {
        if self.wal_w.is_none() {
            std::fs::create_dir_all(format!("{}/wal", self.dir)).unwrap();
            let path = format!("{}/wal/log", self.dir);
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .open(path)
                .await
                .unwrap();
            self.wal_w = Some(file);
        }
        self.wal_w.as_mut().unwrap()
    }

    async fn wal_read_handle(&mut self) -> std::io::Result<&mut File> {
        if self.wal_r.is_none() {
            let path = format!("{}/wal/log", self.dir);
            let file = File::open(path).await?;
            self.wal_r = Some(file);
        }
        Ok(self.wal_r.as_mut().unwrap())
    }
}

#[async_trait]
impl Persister for FilePersister {
    async fn read_wal(&mut self) -> Result<Option<(Term, Vec<u8>)>> {
        let wal = match self.wal_read_handle().await {
            Ok(wal) => wal,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    return Ok(None);
                } else {
                    return Err(e.into());
                }
            }
        };

        // Read the length of the entry
        let mut length_bytes = [0u8; size_of::<usize>()];
        match wal.read_exact(&mut length_bytes).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Close current wal file so next call can read it again
                self.wal_r = None;
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        }
        let length = usize::from_be_bytes(length_bytes);

        // Read the entry
        let mut entry_bytes = vec![0u8; length];
        wal.read_exact(&mut entry_bytes).await?;

        let (term_bytes, data) = entry_bytes.split_at(size_of::<Term>());
        let term = Term::from_be_bytes(term_bytes.try_into().unwrap());

        Ok(Some((term, data.to_vec())))
    }

    async fn write_wal(&mut self, term: Term, data: &[u8]) -> Result<()> {
        let wal = self.wal_write_handle().await;
        let length = size_of::<Term>() + data.len();
        wal.write(&length.to_be_bytes()).await?;
        wal.write(&term.to_be_bytes()).await?;
        wal.write(data).await?;
        Ok(())
    }

    async fn read_snapshot(&self) -> Result<Option<(usize, Vec<u8>)>> {
        // TODO: implement me
        Ok(None)
    }
    async fn write_snapshot(&mut self, _last_index: usize, _data: &[u8]) -> Result<()> {
        // TODO: implement me
        Ok(())
    }
}
