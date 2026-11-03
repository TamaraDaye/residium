#![allow(unused)]

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use std::collections::BTreeMap;
use std::sync::RwLock;
use tokio::fs::File;

const SEGMENT_ROLLOVER: u64 = 64 << 20;

struct Engine {
    memtable: MemTable,
    current_segment_id: String, 
    current_write_offset: u64
}

struct MemTable {
    pub index: RwLock<BTreeMap<ChunkID, DiskPointer>>,
    pub wal: File,
    pub active_pack: File,
}

impl MemTable {
    fn new() -> Self {
        todo!()
    }


}

struct RecordFrame {
    magic_number: Vec<u8>,
    record_type: u8,
    flags: u8,
    payload_length: u32,
    hash: Vec<u8>,
}

struct Inode {
    version: u8,
    object_size: u64,
    chunk_count: u32,
    chunk_id: Vec<ChunkID>,
    mime_type: String,
    creation_time: DateTime<Utc>,
    hash: Vec<u8>,
}

struct DiskPointer {
    segment_id: u32,
    offset: u64,
    size: u32,
}

pub struct ChunkID {
    hash: [u8; 32],
    length: usize,
    chunk: Bytes,
}


impl ChunkID {
    pub fn new(chunk: Bytes) -> Self {
        let hash = *blake3::hash(&chunk).as_bytes();
        let length = chunk.len();
        Self {
            hash, 
            length, 
            chunk
        }
    }
}

struct Slicer {
    ring_buffer: RingBuffer,
}

struct RingBuffer {}

struct Pipeline {
    buffer: Vec<u8>,
}
