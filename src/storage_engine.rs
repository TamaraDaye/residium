#![allow(unused)]

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use std::collections::BTreeMap;
use std::sync::RwLock;
use tokio::fs::File;

struct Engine {
    memtable: MemTable,
    current_segment_id: String,
    current_write_offset: u64,
}


pub struct Wal {
    size: usize,
    enabled: bool,
    version: usize,
    manager: Option<WalManager>,
}

pub struct WalManager {
    file: File,
    blocks: u32,
    active_block: u32,
    block_size: usize,
}

pub enum Entry {
    Tombstone(Tombstone), 
    Ptr(DiskPointer)
}

struct MemTable {
    pub chunk_index: RwLock<BTreeMap<ChunkKey, DiskPointer>>,
    pub object_index: RwLock<BTreeMap<ObjectKey, Entry>>, 
    pub wal: File,
    pub active_pack: PackFile,
}

impl MemTable {
    fn new() -> Self {
        todo!()
    }
}

pub struct RecordFrame {
    magic_number: Vec<u8>,
    format_version: u8,
    record_type: u8,
    flags: u8,
    payload_length: u32,
    hash: Vec<u8>,
}

struct ObjectKey {
    key_length: u32,
    version: u8,
    object_size: u64,
    hash: Vec<u8>,
}

struct ObjectChunks {
    chunk_count: u32,
    chunks: Option<Vec<ChunkKey>>,
} 


struct Object {
    key: ObjectKey ,
    chunk: ObjectChunks, 
    manifest: ObjectManifest
}


struct ObjectManifest {
    mime_type: String,
    creation_time: u64,
}

pub struct DiskPointer {
    segment_id: u32, 
    offset: u64, 
    record_size: u64
}

pub struct PackFile {
    pack: File,
    segment_id: u32,
    offset: u64,
    size: u32,
    segment_size: u32,
}

pub struct Tombstone {
    key_length: u32, 
    key: Vec<u8>, 
    deletion_time: u64
}

pub struct ChunkKey {
    hash: [u8; 32],
    length: usize,
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
