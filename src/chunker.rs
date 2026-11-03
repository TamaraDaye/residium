#![allow(unused)]
use blake3::Hasher;
use bytes::{BufMut, Bytes, BytesMut};
use std::{io, sync::LazyLock};
use tokio::{
    io::AsyncReadExt,
    net::TcpStream,
    sync::mpsc,
};

const MIN_CHUNK: usize = 1 << 18; // 256 KiB
const INITIAL_CHUNK_CAPACITY: usize = 1 << 20; // 1 MiB
const MAX_CHUNK: usize = 4 << 20; // 4 MiB

const READ_BUFFER_CAPACITY: usize = 1 << 18; // 256 KiB

const AVG_BITS: usize = 20;
const MASK: u64 = (1u64 << AVG_BITS) - 1;

fn generate_gear_table() -> [u64; 256] {
    std::array::from_fn(|byte| {
        let mut hasher = Hasher::new();

        hasher.update(b"residium-gear-v1");
        hasher.update(&[byte as u8]);

        let digest = hasher.finalize();

        u64::from_be_bytes(
            digest.as_bytes()[..8]
                .try_into()
                .expect("BLAKE3 digest contains at least 8 bytes"),
        )
    })
}

static GEAR_TABLE: LazyLock<[u64; 256]> = LazyLock::new(generate_gear_table);

fn update_gear_hash(hash: u64, byte: u8) -> u64 {
    hash.wrapping_shl(1)
        .wrapping_add(GEAR_TABLE[byte as usize])
}

fn is_chunk_boundary(fp: u64) -> bool {
    fp & MASK == 0
}

pub async fn chunk(
    mut conn: TcpStream,
    tx: mpsc::Sender<Bytes>,
) -> io::Result<()> {
    let mut fingerprint: u64 = 0;

    let mut chunk = BytesMut::with_capacity(INITIAL_CHUNK_CAPACITY);
    let mut read_buf = BytesMut::with_capacity(READ_BUFFER_CAPACITY);

    loop {
        let n = read_bytes(&mut read_buf, &mut conn).await?;

        if n == 0 {
            break;
        }

        for i in 0..n {
            let byte = read_buf[i];

            chunk.put_u8(byte);
            fingerprint = update_gear_hash(fingerprint, byte);

            let chunk_len = chunk.len();

            let boundary =
                (chunk_len >= MIN_CHUNK && is_chunk_boundary(fingerprint))
                || chunk_len >= MAX_CHUNK;

            if boundary {
                let completed = chunk.split().freeze();

                fingerprint = 0;

                tx.send(completed)
                    .await
                    .map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "chunk consumer closed",
                        )
                    })?;
            }
        }

        read_buf.clear();
    }

    if !chunk.is_empty() {
        let completed = chunk.freeze();

        tx.send(completed)
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "chunk consumer closed",
                )
            })?;
    }

    Ok(())
}

pub async fn read_bytes(
    buffer: &mut BytesMut,
    stream: &mut TcpStream,
) -> io::Result<usize> {
    stream.read_buf(buffer).await
}
