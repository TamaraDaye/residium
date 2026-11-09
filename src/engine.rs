#![allow(unused)]
use crate::chunker::Chunker;
use crate::storage::{
    Chunk, ChunkKey, DiskPointer, Object, ObjectKey, ObjectMetadata, Tombstone,
};
use crate::writer::WriterCommand;
use std::{
    io,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::{mpsc, oneshot},
};

//TODO: seperate object writer from chunk writer with queue for object manifests and introduce proper replies

const CHUNK_QUEUE_CAPACITY: usize = 5;

pub struct Engine {
    writers: Vec<WriteHandle>,
}

pub struct ObjectUpload<R> {
    pub key: ObjectKey,
    pub metadata: ObjectMetadata,
    pub expected_size: Option<u64>,
    pub body: R,
}

impl Engine {
    pub async fn store_object<R>(&mut self, upload: ObjectUpload<R>) -> io::Result<()>
    where
        R: AsyncRead + Unpin,
    {
        if self.writers.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "no storage writers configured",
            ));
        }

        let ObjectUpload {
            key,
            metadata,
            expected_size,
            mut body,
        } = upload;

        let (tx, mut rx) = mpsc::channel(CHUNK_QUEUE_CAPACITY);
        let chunker = Chunker::new(tx);

        let chunking = async move {
            let mut chunker = chunker;
            if let Some(expected_size) = expected_size {
                let mut bounded_body = (&mut body).take(expected_size);
                chunker.chunk(&mut bounded_body).await?;
                if bounded_body.limit() != 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "object body ended before its declared size",
                    ));
                }
                Ok(())
            } else {
                chunker.chunk(&mut body).await
            }
        };

        let writing = async {
            let mut chunk_keys = Vec::new();
            let mut object_size = 0u64;
            let mut checksum = blake3::Hasher::new();
            let mut touched_writers = vec![false; self.writers.len()];

            while let Some(bytes) = rx.recv().await {
                object_size = object_size.checked_add(bytes.len() as u64).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "object too large")
                })?;
                checksum.update(&bytes);

                let chunk = Chunk::new(bytes);
                let chunk_key = chunk.key;
                let writer_index = self.writer_for_hash(&chunk_key.hash);

                self.writers[writer_index].send_chunk(chunk).await?;
                touched_writers[writer_index] = true;
                chunk_keys.push(chunk_key);
            }

            Ok::<_, io::Error>((
                chunk_keys,
                object_size,
                *checksum.finalize().as_bytes(),
                touched_writers,
            ))
        };

        let (chunking_result, writing_result) = tokio::join!(chunking, writing);
        let (chunk_keys, object_size, checksum, touched_writers) = writing_result?;
        chunking_result?;

        if let Some(expected_size) = expected_size {
            if object_size != expected_size {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "object size did not match its declared size",
                ));
            }
        }

        for (index, touched) in touched_writers.into_iter().enumerate() {
            if touched {
                self.writers[index].sync().await?;
            }
        }

        let creation_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?
            .as_millis() as u64;
        let object = Object::from_upload(
            key.clone(),
            chunk_keys,
            metadata,
            object_size,
            checksum,
            creation_time,
        );

        let object_writer = self.writer_for_hash(blake3::hash(key.as_str().as_bytes()).as_bytes());
        self.writers[object_writer].put_manifest(object).await?;
        self.writers[object_writer].sync().await?;

        Ok(())
    }

    async fn get_object() {}
    async fn delete_object() {}
    async fn head_object() {}
}

impl Engine {
    fn writer_for_hash(&self, hash: &[u8; 32]) -> usize {
        let selector = u64::from_be_bytes(hash[..8].try_into().expect("hash prefix is 8 bytes"));
        (selector % self.writers.len() as u64) as usize
    }
}

#[derive(Clone)]
pub struct WriteHandle {
    tx: mpsc::Sender<WriterCommand>,
}

impl WriteHandle {
    pub async fn send_chunk(&self, chunk: Chunk) -> io::Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(WriterCommand::PutChunk {
                chunk,
                reply: reply_tx,
            })
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "writer is closed"))?;
        reply_rx
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "writer dropped chunk reply"))?
    }

    pub async fn put_manifest(&self, object: Object) -> io::Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(WriterCommand::PutManifest {
                object,
                reply: reply_tx,
            })
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "writer is closed"))?;
        Ok(())
    }

    pub async fn sync(&self) -> io::Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(WriterCommand::Sync { reply: reply_tx })
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "writer is closed"))?;
        reply_rx
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "writer dropped sync reply"))?
    }
}

