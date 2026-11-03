#![allow(unused)]
use tokio::sync::mpsc;
use bytes::Bytes;
use crate::storage::{Chunk, MemTable};

pub struct Engine {
    writers: Vec<WriteHandle>
}

impl Engine {
    async fn put_object(){}
    async fn get_object(){}
    async fn delete_object(){}
    async fn head_object(){}
}

#[derive(Clone)]
pub struct WriteHandle {
    tx: mpsc::Sender<Chunk>, 
}

pub struct WriterActor {
    id: u32, 
    rx: mpsc::Receiver<WriterCommand>,
    indexes: MemTable,
    state: WriterState
}


pub enum WriterCommand {
    PutChunk, 
    PutManifest,
    PutTombstone,
    Sync,
    Shutdown
}



impl WriteHandle {
    fn put_chunk () {}
    fn put_manifest () {}
    fn put_tombstone () {}
}

pub enum WriterState {
    Ready, 
    Recovering, 
    Rotating, 
    Failed, 
    Stopped, 
}


