#![allow(unused)]
use tokio::{io::{AsyncWrite, AsyncWriteExt}, sync::{mpsc, oneshot}};
use crate::storage::{Chunk, DiskPointer, MemTable, Object, PackFile, RecordHeader, RecordType, Tombstone};
use std::{io, marker::PhantomData, path::PathBuf};
use postcard;


const RECORD_HEADER_LEN: usize = 42;
const FORMAT_VERSION: u8 = 1;
const SEGMENT_SIZE: usize = 512 << 20;

pub enum WriterCommand {
    PutChunk {
        chunk: Chunk,
        reply: oneshot::Sender<io::Result<()>>,
    },
    PutManifest {
        object: Object,
        reply: oneshot::Sender<io::Result<DiskPointer>>,
    },
    PutTombstone {
        tombstone: Tombstone,
        reply: oneshot::Sender<io::Result<()>>,
    },
    Sync {
        reply: oneshot::Sender<io::Result<()>>,
    },
    Shutdown {
        reply: oneshot::Sender<io::Result<()>>,
    },
}

pub enum WriterState {
    Ready,
    Recovering,
    Rotating,
    Failed,
    Stopped,
}


pub struct ChunkWriter; 
pub struct ObjectWriter;



pub struct WriterActor <S, F: AsyncWrite +  Unpin> {
    id: u32,
    rx: mpsc::Receiver<WriterCommand>,
    index: MemTable,
    state: WriterState,
    file: PackFile<F>,
    _state:  PhantomData<S>
}


impl <F: AsyncWrite + AsyncWriteExt + Unpin > WriterActor<ChunkWriter, F> {

}


impl <F: AsyncWrite + Unpin > WriterActor<ObjectWriter, F> {
    async fn receive_manifest(&mut self) {
        while let Some(command) = self.rx.recv().await {
            match command {
                WriterCommand::PutManifest { object, reply } => {
                    let object_hash = &object.manifest.checksum;
                    let data = postcard::to_stdvec(&object).unwrap();
                    let header = encode_mainfest_header(data.len() as u64, object_hash);
                } 
                WriterCommand::Sync { reply } => {

                }

                WriterCommand::PutTombstone { tombstone, reply } => {

                }
                _ => {}
            }

        }
    }


    async fn write(&mut self, header: &[u8], data: &[u8]) {
        if self.file.size + data.len() + header.len() > SEGMENT_SIZE {

        }
    }

    fn rotate_pack(&mut self) {

    }


}

fn encode_mainfest_header(payload_length: u64, hash: &[u8]) -> [u8; RECORD_HEADER_LEN] {
    let mut header = [0u8; RECORD_HEADER_LEN];
    header[0] = FORMAT_VERSION;
    header[1] = RecordType::ObjectManifest as u8;
    header[2..10].copy_from_slice(&payload_length.to_le_bytes()); 
    header[10..].copy_from_slice(hash);
    header
}

impl <S, F: AsyncWrite + Unpin > WriterActor <S, F> {
    fn new(id: u32, rx: mpsc::Receiver<WriterCommand>, data_dir: Option<PathBuf>) -> Self {
        let index = MemTable::new();
        Self { 
            id, 
            rx, 
            index, 
            state: WriterState::Ready, 
            file: pack_file,
            _state: PhantomData
        }
    }


    async fn receive_chunk(&mut self) {
        while let Some(command) = self.rx.recv().await {
            match command {
                WriterCommand::PutChunk { chunk, reply } => {
                }

                WriterCommand::Shutdown { reply } => {

                }
                _ => {}
            }
        }
    }
}
