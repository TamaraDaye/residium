#![allow(unused)]
use crate::storage::{
    Chunk, DiskPointer, Entry, MemTable, Object, PackFile, RecordHeader, RecordType, Tombstone,
};
use postcard;
use std::path::Path;
use std::{future::Future, io, marker::PhantomData, path::PathBuf};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

//TODO: implement chunk writer functionality and create a status variable to decalre success on install and should reply object manifest for creation
//of objects

const RECORD_HEADER_LEN: usize = 42;
const FORMAT_VERSION: u8 = 1;
const SEGMENT_SIZE: usize = 512 << 20; //512mb per chunk file

impl SegmentIo for File {
    async fn open(data_dir: Option<&Path>, writer_id: u32, segment_id: u32) -> io::Result<Self> {
        let dir = data_dir.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "missing data_dir for disk file",
            )
        })?;
        let path = dir.join(format!("writer_{writer_id}_{segment_id}.pack"));

        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(path)
            .await
    }

    async fn sync(&mut self) -> io::Result<()> {
        self.sync_data().await
    }
}

// In-memory mock for unit tests
impl SegmentIo for io::Cursor<Vec<u8>> {
    async fn open(_dir: Option<&Path>, _writer_id: u32, _segment_id: u32) -> io::Result<Self> {
        Ok(io::Cursor::new(Vec::new()))
    }

    async fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub enum WriterCommand {
    PutChunk {
        chunk: Chunk,
        reply: oneshot::Sender<io::Result<()>>,
    },
    PutManifest {
        object: Object,
        reply: oneshot::Sender<io::Result<()>>,
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
fn encode_mainfest_header(
    payload_length: u64,
    hash: &[u8],
    record: RecordType,
) -> [u8; RECORD_HEADER_LEN] {
    let mut header = [0u8; RECORD_HEADER_LEN];

    header[0] = FORMAT_VERSION;

    header[1] = record as u8;

    header[2..10].copy_from_slice(&payload_length.to_le_bytes());

    header[10..].copy_from_slice(hash);

    header
}

pub struct ChunkWriter;
pub struct ObjectWriter;

pub trait SegmentIo: AsyncWrite + Unpin + Sized + Send {
    fn open(
        data_dir: Option<&Path>,
        writer_id: u32,
        segment_id: u32,
    ) -> impl Future<Output = io::Result<Self>> + Send;
    fn sync(&mut self) -> impl Future<Output = io::Result<()>> + Send;
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
                    let header = encode_mainfest_header(
                        chunk.key.length as u64,
                        &chunk.key.hash,
                        RecordType::Chunk,
                    );
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
                    let header = encode_mainfest_header(
                        data.len() as u64,
                        object_hash,
                        RecordType::ObjectManifest,
                    );
                    let res = self.write(&header, &data).await;
                    self.index
                        .object_index
                        .insert(object.key, Entry::Ptr(res.unwrap()))
                        .unwrap();
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
