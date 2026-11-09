#![allow(unused)]
use bytes::{BufMut, Bytes, BytesMut};
use std::{io, sync::LazyLock};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::mpsc,
};

const MIN_CHUNK: usize = 1 << 18; // 256 KiB
const INITIAL_CHUNK_CAPACITY: usize = 1 << 20; // 1 MiB
const MAX_CHUNK: usize = 4 << 20; // 4 MiB

const READ_BUFFER_CAPACITY: usize = 1 << 18; // 256 KiB

const AVG_BITS: usize = 20;
const MASK: u64 = (1u64 << AVG_BITS) - 1;

fn generate_gear_table() -> [u64; 256] {
    use blake3::Hasher;
    std::array::from_fn(|byte| {
        let mut hasher = Hasher::new();

        hasher.update(b"residium-gear-v1");
        hasher.update(&[byte as u8]);

        let digest = hasher.finalize();

        u64::from_be_bytes(
            digest.as_bytes()[..8]
                .try_into()
                .expect("BLAKE3 digest contains at least 8 bytes"),
        )
    })
}

static GEAR_TABLE: LazyLock<[u64; 256]> = LazyLock::new(generate_gear_table);

pub struct Chunker {
    fingerprint: u64,
    tx: mpsc::Sender<Bytes>,
}

impl Chunker {
    pub fn new(tx: mpsc::Sender<Bytes>) -> Self {
        Self { fingerprint: 0, tx }
    }

    fn update_gear_hash(&mut self, byte: u8) -> u64 {
        self.fingerprint
            .wrapping_shl(1)
            .wrapping_add(GEAR_TABLE[byte as usize])
    }

    fn is_chunk_boundary(&self) -> bool {
        self.fingerprint & MASK == 0
    }

    pub async fn chunk<R>(&mut self, reader: &mut R) -> io::Result<()>
    where
        R: AsyncRead + Unpin,
    {
        let mut chunk = BytesMut::with_capacity(INITIAL_CHUNK_CAPACITY);
        let mut read_buf = BytesMut::with_capacity(READ_BUFFER_CAPACITY);

        loop {
            let n = read_bytes(&mut read_buf, reader).await?;

            if n == 0 {
                break;
            }

            for i in 0..n {
                let byte = read_buf[i];

                chunk.put_u8(byte);

                self.fingerprint = self.update_gear_hash(byte);

                let boundary = (chunk.len() >= MIN_CHUNK && self.is_chunk_boundary())
                    || chunk.len() >= MAX_CHUNK;

                if boundary {
                    let completed = chunk.split().freeze();

                    self.fingerprint = 0;

                    self.tx.send(completed).await.map_err(|_| {
                        io::Error::new(io::ErrorKind::BrokenPipe, "chunk consumer closed")
                    })?;
                }
            }

            read_buf.clear();
        }

        if !chunk.is_empty() {
            let completed = chunk.freeze();

            self.tx
                .send(completed)
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "chunk consumer closed"))?;
        }

        Ok(())
    }
}

pub async fn read_bytes<R>(buffer: &mut BytesMut, reader: &mut R) -> io::Result<usize>
where
    R: AsyncRead + Unpin,
{
    reader.read_buf(buffer).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk() { 
    }
}
