#![allow(unused)]
use crate::storage::{
    Chunk, DiskPointer, MemTable, Object, PackFile, RecordHeader, RecordType, Tombstone,
};
use postcard;
use std::path::Path;
use std::{io, marker::PhantomData, path::PathBuf};
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::{mpsc, oneshot},
};

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
fn encode_mainfest_header(payload_length: u64, hash: &[u8]) -> [u8; RECORD_HEADER_LEN] {
    let mut header = [0u8; RECORD_HEADER_LEN];

    header[0] = FORMAT_VERSION;

    header[1] = RecordType::ObjectManifest as u8;

    header[2..10].copy_from_slice(&payload_length.to_le_bytes());

    header[10..].copy_from_slice(hash);

    header
}

pub struct ChunkWriter;
pub struct ObjectWriter;

pub trait SegmentIo: AsyncWrite + Unpin + Sized {
    async fn open(data_dir: Option<&Path>, writer_id: u32, segment_id: u32) -> io::Result<Self>;
    async fn sync(&mut self) -> io::Result<()>;
}

pub struct WriterActor<S, F: AsyncWrite + Unpin> {
    id: u32,
    rx: mpsc::Receiver<WriterCommand>,
    index: MemTable,
    state: WriterState,
    data_dir: Option<PathBuf>,
    file: PackFile<F>,
    _state: PhantomData<S>,
}

impl<S, F: SegmentIo> WriterActor<S, F> {
    pub async fn new(
        id: u32,
        rx: mpsc::Receiver<WriterCommand>,
        data_dir: Option<PathBuf>,
    ) -> io::Result<Self> {
        let writer = F::open(data_dir.as_deref(), id, 0).await?;
        Ok(Self {
            id,
            rx,
            index: MemTable::new(),
            state: WriterState::Ready,
            data_dir,
            file: PackFile::new(writer, id),
            _state: PhantomData,
        })
    }

    async fn write(&mut self, header: &[u8], data: &[u8]) -> io::Result<DiskPointer> {
        if self.file.size + header.len() + data.len() > SEGMENT_SIZE {
            self.rotate_pack().await?;
        }

        let start_dp = self.file.write_data(header).await?;
        self.file.write_data(data).await?;

        Ok(DiskPointer {
            record_size: (header.len() + data.len()) as u64,
            ..start_dp
        })
    }

    async fn rotate_pack(&mut self) -> io::Result<()> {
        self.state = WriterState::Rotating;

        if let Err(e) = self.file.pack.flush().await.and_then(|_| Ok(())) {
            self.state = WriterState::Failed;
            return Err(e);
        }
        self.file.pack.sync().await?;

        let next_segment_id = self.file.segment_id + 1;
        match F::open(self.data_dir.as_deref(), self.id, next_segment_id).await {
            Ok(next_writer) => {
                self.file.reset_segment(next_writer, next_segment_id);
                self.state = WriterState::Ready;
                Ok(())
            }
            Err(e) => {
                self.state = WriterState::Failed;
                Err(e)
            }
        }
    }
}

impl<F: SegmentIo> WriterActor<ChunkWriter, F> {
    pub async fn receive_chunk(&mut self) {
        while let Some(command) = self.rx.recv().await {
            match command {
                WriterCommand::PutChunk { chunk, reply } => {
                    // Calls shared `self.write(&header, &chunk.data).await`
                }
                WriterCommand::Shutdown { reply } => break,
                _ => {}
            }
        }
    }
}

impl<F: SegmentIo> WriterActor<ObjectWriter, F> {
    pub async fn receive_manifest(&mut self) {
        while let Some(command) = self.rx.recv().await {
            match command {
                WriterCommand::PutManifest { object, reply } => {
                    let object_hash = &object.manifest.checksum;
                    let data = postcard::to_stdvec(&object).unwrap();
                    let header = encode_mainfest_header(data.len() as u64, object_hash);
                    let res = self.write(&header, &data).await;
                    let _ = reply.send(res);
                }
                WriterCommand::Sync { reply } => {
                    let _ = reply.send(self.file.pack.sync().await);
                }
                WriterCommand::PutTombstone { tombstone, reply } => {}
                _ => {}
            }
        }
    }
}
