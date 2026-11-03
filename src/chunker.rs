#![allow(unused)]
const MIN_CHUNK: usize = 1 << 18; // 256KB 
const AVG_CHUNK: usize = 1 << 20; // 1 MiB
const MAX_CHUNK: usize = 4 << 20; // 4MiB
const AVG_BITS: usize = 20;

use blake3::Hasher;

use bytes::BytesMut;
use tokio::io::AsyncRead;
use std::sync::LazyLock;


fn generate_gear_table() -> [u64; 256] {
    std::array::from_fn(|byte| {
        let mut hasher = Hasher::new();

        hasher.update(b"residium-gear-v1");
        hasher.update(&[byte as u8]);

        let digest = hasher.finalize();

        u64::from_be_bytes(
            digest.as_bytes()[..8]
                .try_into()
                .expect("SHA-256 digest contains at least 8 bytes"),
        )
    })
}

static GEAR_TABLE: LazyLock<[u64; 256]> = LazyLock::new(generate_gear_table);

fn update_gear_hash(hash: u64, byte: u8) -> u64 {
    hash.wrapping_shl(1).wrapping_add(GEAR_TABLE[byte as usize])
}

fn is_chunk_boundary(fp: u64, mask: u64) -> bool {
    fp & mask == 0
}


pub fn chunk() {
    todo!()
}
