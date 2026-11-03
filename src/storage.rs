#![allow(unused)]

use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::RwLock;
use tokio::fs::File;


pub enum Entry {
    Tombstone(Tombstone),
    Ptr(DiskPointer),
}

/// The packfiles are the durable source of truth for this local storage layer.
/// In-memory indexes derived from the append-only packfiles.
pub struct MemTable {
    pub chunk_index: RwLock<BTreeMap<ChunkKey, DiskPointer>>,
    pub object_index: RwLock<BTreeMap<ObjectKey, Entry>>,
    pub active_pack: PackFile,
}

impl MemTable {
    fn new() -> Self {
        todo!()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordFrame {
    magic_number: [u8; 4],
    format_version: u8,
    record_type: RecordType,
    flags: u16,
    payload_length: u32,
    hash: [u8; 32],
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ObjectKey(String);

impl ObjectKey {
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

struct ObjectChunks {
    chunks: Vec<ChunkKey>,
}


struct Object {
    key: ObjectKey,
    chunks: ObjectChunks,
    manifest: ObjectManifest,
}


struct ObjectManifest {
    mime_type: String,
    creation_time: u64,
    object_size: u64,
    checksum: [u8; 32],
}

pub struct DiskPointer {
    segment_id: u32,
    offset: u64,
    record_size: u64,
    writer_id: u32
}

pub struct PackFile {
    pack: File,
    segment_id: u32,
    /// Bytes written in the active segment; the next append position.
    offset: u64,
    /// Current total size of this packfile. Keep it at or below `segment_size`.
    size: u64,
    /// Configured maximum size for this packfile segment.
    segment_size: u64,
}

pub struct Tombstone {
    key: ObjectKey,
    deletion_time: u64,
    version: u64,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ChunkKey {
    hash: [u8; 32],
    length: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum RecordType {
    Chunk = 1,
    ObjectManifest = 2,
    Tombstone = 3,
}

pub struct Chunk {
    key: ChunkKey,
    chunk: Bytes,
}

impl ChunkKey {
    pub fn new(chunk: Bytes) -> Self {
        let hash = *blake3::hash(&chunk).as_bytes();
        let length = chunk.len();
        Self {
            hash,
            length,
        }
    }
}
