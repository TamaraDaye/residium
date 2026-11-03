#![allow(unused)]

use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::RwLock;
use tokio::fs::File;

struct Engine {
    memtable: MemTable,
}

pub enum Entry {
    Tombstone(Tombstone),
    Ptr(DiskPointer),
}

/// In-memory indexes derived from the append-only packfiles.
/// The packfiles are the durable source of truth for this local storage layer.
struct MemTable {
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
    chunks: Vec<Chunk>,
}


struct Object {
    key: ObjectKey,
    chunks: ObjectChunks,
    manifest: ObjectManifest,
    object_size: u64,
    hash: [u8; 32],
}


struct ObjectManifest {
    mime_type: String,
    creation_time: u64,
    version: u64,
}

pub struct DiskPointer {
    segment_id: u32,
    offset: u64,
    record_size: u64,
}

pub struct PackFile {
    pack: File,
    segment_id: u32,
    offset: u64,
    size: u32,
    segment_size: u32,
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
