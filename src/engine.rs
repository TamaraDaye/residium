#![allow(unused)]
use crate::storage::{Chunk, ChunkKey, DiskPointer, MemTable, Object, Tombstone};
use crate::chunker::Chunker;
use bytes::Bytes;
use tokio::{net::TcpStream, sync::{mpsc, oneshot}};

pub struct Engine {
    writers: Vec<WriteHandle>,
}

impl Engine {
    async fn store_object(&mut self, stream: TcpStream) { 
        let (tx, mut rx) = mpsc::channel(5);
        let mut chunker = Chunker::new(tx);

        chunker.chunk(stream).await;

        while let Some(bytes) = rx.recv().await {
            let chunk = Chunk::new(bytes);
            let writer = usize::from(chunk.key.hash[0] & 1);
            self.writers[writer].send_chunk(chunk).await;
        }

    }
    async fn get_object() {}
    async fn delete_object() {}
    async fn head_object() {}
}

#[derive(Clone)]
pub struct WriteHandle {
    tx: mpsc::Sender<WriterCommand>,
}


impl WriteHandle {
    pub async fn send_chunk(&mut self, chunk: Chunk) -> Result<(), std::io::Error> {
        // let (tx, rx) = oneshot::channel();

        // self.tx.send(WriterCommand::PutChunk{ chunk: chunk, reply:tx }).await;


        todo!()
    }
}

pub struct WriterActor {
    id: u32,
    rx: mpsc::Receiver<WriterCommand>,
    indexes: MemTable,
    state: WriterState,
}

pub enum WriterCommand {
    PutChunk {
        chunk: Chunk,
        reply: tokio::sync::oneshot::Sender<(ChunkKey, DiskPointer)>,
    },
    PutManifest {
        object: Object,
        reply: tokio::sync::oneshot::Sender<DiskPointer>,
    },
    PutTombstone {
        tombstone: Tombstone,
        reply: tokio::sync::oneshot::Sender<bool>,
    },
    Sync{reply: oneshot::Sender<bool>},
    Shutdown{reply: oneshot::Sender<bool>},
}

impl WriteHandle {
    fn put_chunk() {}
    fn put_manifest() {}
    fn put_tombstone() {}
}

pub enum WriterState {
    Ready,
    Recovering,
    Rotating,
    Failed,
    Stopped,
}
