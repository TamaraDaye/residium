# Storage engine: next implementation steps

This checklist continues from the chunker and the generic `WriterActor<S, F>` / `SegmentIo` design. The append-only packfiles are durable; `MemTable` is an in-memory index rebuilt during recovery, so it does not need an LSM-style flush.

## 1. Stabilize the record and segment boundary

- [ ] Make `RecordHeader::encode` the only header encoder. Align its byte layout with `storage-format.md`: magic bytes, version, type, flags, length, and payload BLAKE3 digest.
- [ ] Add header decoding and validation before implementing reads or recovery.
- [ ] Hash the serialized manifest payload for a manifest record. `ObjectManifest.checksum` is the checksum of the original object bytes and is different.
- [ ] Decide and document one integer width and byte order. The current code uses a 42-byte, little-endian header while `storage-format.md` specifies 44 bytes and big-endian fields.
- [ ] Give `SegmentIo` an associated context type so `File` can use `PathBuf` and test writers can use `()` or mock state. This removes `Option<PathBuf>` from `WriterActor`.
- [ ] Open newly created disk segments with `create_new(true)`. Do not reopen an existing segment with an offset of zero; recovery must determine the active segment and its real append offset.

## 2. Complete the chunk writer

- [ ] Implement `WriterActor<ChunkWriter>::receive_chunk`.
- [ ] Check `chunk_index` first. On a hit, reply successfully without another append.
- [ ] On a miss, encode a chunk record, call `write`, then insert the returned `DiskPointer` into `chunk_index`.
- [ ] Send exactly one result through every command reply channel. A missing reply currently makes `Engine::send_chunk` wait forever.
- [ ] On a write or sync error, return the error to the command sender and move the actor to `Failed`; do not `unwrap` in the actor loop.
- [ ] Rotate before writing a record that would exceed the segment target when the active segment already contains data. Do not split a record across segments. An oversized single record may occupy an empty segment.

## 3. Complete the manifest writer and engine wiring

- [ ] Implement `PutManifest` and `PutTombstone` using the same append/reply/error pattern.
- [ ] Update `object_index` only after its record append succeeds. Replacing an object key is normal; do not call `unwrap()` on `BTreeMap::insert`.
- [ ] Make `WriteHandle::put_manifest` wait for its oneshot reply, as `send_chunk` already does.
- [ ] Keep chunk writers and manifest writers as separate handle collections in `Engine`. A `ChunkWriter` should never silently receive and ignore a manifest command.
- [ ] Put chunk segments and manifest segments in separate paths, or add a pack kind to `DiskPointer`. `writer_id + segment_id` alone cannot identify a file when both actor kinds exist.
- [ ] Preserve commit order: append missing chunks, sync all touched chunk writers, append the manifest, sync its writer, then report success.

## 4. Add focused tests while implementing

- [ ] Header encode/decode tests: expected bytes, invalid magic/version/type, invalid length.
- [ ] `PackFile<Cursor<Vec<u8>>>` test: sequential writes advance offset and return the expected `DiskPointer`.
- [ ] Chunk writer test: append one chunk, receive success, and verify an index entry.
- [ ] Deduplication test: submitting the same chunk twice produces one stored record.
- [ ] Rotation test: a record that does not fit in a non-empty segment is written to the next segment and has the next segment ID.
- [ ] Manifest overwrite test: the latest manifest replaces the live object-index entry.
- [ ] Temporary-directory integration test: write an object, restart/recover, then read exactly the original bytes.

## 5. Build recovery and reads after writes work

- [ ] Scan segment files in order and rebuild chunk/object indexes from valid records.
- [ ] Treat an incomplete final record as an interrupted append; reject invalid interior records as corruption.
- [ ] Implement reading a chunk from a `DiskPointer`, validating its header and payload hash.
- [ ] Implement `get`, `head`, and delete from the rebuilt indexes.

