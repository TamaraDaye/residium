#![allow(unused)]

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::RwLock;
use std::{collections::BTreeMap, io};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

pub enum Entry {
    Tombstone(Tombstone),
    Ptr(DiskPointer),
}

const SEGMENT_SIZE: usize = 512 << 20;

pub struct MemTable {
    pub chunk_index: BTreeMap<ChunkKey, DiskPointer>,
    pub object_index: BTreeMap<ObjectKey, Entry>,
    pub chunk_index_size: usize,
    pub object_index_size: usize,
}

impl MemTable {
    pub fn new() -> Self {
        let chunk_index: BTreeMap<ChunkKey, DiskPointer> = BTreeMap::new();
        let object_index: BTreeMap<ObjectKey, Entry> = BTreeMap::new();
        Self {
            chunk_index,
            object_index,
            chunk_index_size: 0,
            object_index_size: 0,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordHeader {
    pub format_version: u8,
    pub record_type: RecordType,
    pub payload_length: u64,
    pub hash: [u8; 32],
}

impl RecordHeader {}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ObjectKey(String);

impl ObjectKey {
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Serialize, Deserialize)]
pub struct Object {
    pub key: ObjectKey,
    pub chunks: Vec<ChunkKey>,
    pub manifest: ObjectManifest,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ObjectMetadata {
    pub content_type: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ObjectManifest {
    pub content_type: Option<String>,
    pub creation_time: u64,
    pub object_size: u64,
    pub checksum: [u8; 32],
}

impl Object {
    pub fn from_upload(
        key: ObjectKey,
        chunks: Vec<ChunkKey>,
        metadata: ObjectMetadata,
        object_size: u64,
        checksum: [u8; 32],
        creation_time: u64,
    ) -> Self {
        Self {
            key,
            chunks,
            manifest: ObjectManifest {
                content_type: metadata.content_type,
                creation_time,
                object_size,
                checksum,
            },
        }
    }
}

pub struct DiskPointer {
    pub segment_id: u32,
    pub offset: u64,
    pub record_size: u64,
    pub writer_id: u32,
}

pub struct PackFile<F: AsyncWrite + Unpin> {
    pub pack: F,
    pub segment_id: u32,
    pub offset: usize,
    pub size: usize,
    pub writer_id: u32,
}

impl<F: AsyncWrite + Unpin> PackFile<F> {
    fn new(writer: F, writer_id: u32) -> Self {
        Self {
            pack: writer,
            segment_id: 0,
            offset: 0,
            size: 0,
            writer_id,
        }
    }

    async fn write_data(&mut self, data: &[u8]) -> io::Result<DiskPointer> {
        let offset = self.offset;

        let dp = DiskPointer {
            segment_id: self.segment_id,
            offset: offset as u64,
            record_size: data.len() as u64,
            writer_id: self.writer_id,
        };

        self.pack.write_all(data).await;
        self.offset += data.len();
        self.size += data.len();

        Ok(dp)
    }
}

#[derive(Serialize, Deserialize)]
pub struct Tombstone {
    pub key: ObjectKey,
    pub deletion_time: u64,
    pub version: u64,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ChunkKey {
    pub hash: [u8; 32],
    pub length: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum RecordType {
    Chunk = 1,
    ObjectManifest = 2,
    Tombstone = 3,
}

pub struct Chunk {
    pub key: ChunkKey,
    pub chunk: Bytes,
}

impl Chunk {
    pub fn new(bytes: Bytes) -> Self {
        let key = ChunkKey::new(&bytes);
        Self { key, chunk: bytes }
    }
}

impl ChunkKey {
    pub fn new(chunk: &Bytes) -> Self {
        let hash = *blake3::hash(&chunk).as_bytes();
        let length = chunk.len();
        Self { hash, length }
    }
}
