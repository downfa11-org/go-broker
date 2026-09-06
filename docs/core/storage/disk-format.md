# Disk Format

This document describes the Cursus partition-log files. The runtime accepts only the documented current metadata versions; an incompatible format requires an explicit offline converter or clean bootstrap.

## Layout

Each topic partition has independent segment and sparse offset-index files:

```text
{log_dir}/{topic}/
  partition_{partition}_segment_{baseOffset:020d}.log
  partition_{partition}_segment_{baseOffset:020d}.index
```

The segment number is the first logical record offset in that segment, not an ordinal file number. The default `log_segment_bytes` is 1 GiB and is configurable. A segment also rolls when its index would exceed `log_index_size_bytes` or when the configured time-based roll interval expires.

## Segment Record

A log segment is a sequence of length-prefixed records:

```text
uint32_be serializedLength
byte[serializedLength] serializedDiskMessage
```

The maximum accepted serialized record size is 64 MiB, including its checksum. New records use `CDM3`; the reader also accepts complete `CDM2` records without checksums for an in-place upgrade. The serialized message uses big-endian fixed-width integers and length-prefixed strings/bytes in this order:

| Field | Encoding |
|---|---|
| magic | four bytes `CDM3` |
| topic | `uint16` length + UTF-8 bytes |
| partition | `uint32` |
| logical offset | `uint64` |
| producer id | `uint16` length + bytes |
| producer sequence | `uint64` |
| producer epoch | `uint64` representation of non-negative `int64` |
| payload | `uint32` length + bytes |
| event type | `uint16` length + bytes |
| schema version | `uint32` |
| aggregate version | `uint64` |
| metadata | `uint16` length + bytes |
| key | `uint16` length + bytes |
| transactional id/state/marker | three `uint16` length-prefixed values |
| control batch type | `uint16` length + bytes |
| control batch version | `int16` |
| control coordinator epoch | `int64` |
| control key/value | two `uint16` length-prefixed byte arrays |
| checksum | `uint32` CRC32C (Castagnoli) of magic and all preceding record fields |

The transaction/control fields are empty for ordinary records. Control records remain in the raw log and are filtered by `read_committed`.

## Sparse Offset Index

Each `.index` file contains fixed 16-byte entries:

```text
uint64_be logicalOffset
uint64_be bytePosition
```

An entry is written after the configured byte interval (default 4096 bytes). Reads locate the segment and nearest indexed byte position, validate that entry against the log, then scan records. Active segments require contiguous logical offsets. Compacted closed segments retain original offsets in strictly increasing order and may contain holes. Segment data is opened through mmap for reads.

## Write And Sync Contract

Asynchronous writes enter a buffered `writeCh` (default capacity 1024). `flushLoop` serializes and writes a batch when any of these occurs:

- `disk_flush_batch_size` records are ready (default 50),
- `linger_ms` expires (default 50 ms),
- an explicit flush or shutdown drains pending records,
- a segment roll is required.

`WriteBatch` flushes the Go buffered data and index writers to the operating-system file descriptors. `syncLoop` calls `Sync` for data and index files at `disk_flush_interval_ms` (default 500 ms) and advances the partition durability callback after a successful sync. Explicit `Flush`, segment rotation, and shutdown also sync data.

A write being visible in the process page cache is different from surviving power loss. Client acknowledgements and replicated HWM advancement must be interpreted according to the selected publish/replication path and the synced committed-tail contract, not merely successful channel enqueue.

Standalone batches validate producer sequence transitions before enqueueing and share one explicit queue-draining sync boundary, instead of syncing each message. Segment rotations or background sync may add sync calls. HWM and producer state advance only after batch success; a storage failure remains an uncertain outcome, not an atomic batch rollback. Alternate storage implementations without allocating-batch support retain the single-message fallback.

Transaction updates snapshot only the affected ID. Journal compaction thresholds measure bytes/records added since the last compaction, so a large live transaction set does not force a full rewrite on every append. Journal records still contain a complete snapshot of that transaction; large individual transactions retain snapshot-write amplification.

## Recovery

Startup recovery scans the entire active segment, including records before the last sparse index entry, validates lengths/checksums and contiguous offsets, and removes only a trailing partial record. Complete malformed or checksum-invalid records fail startup without truncation. Closed segments are checksum-validated when read; this is not a full startup scrub of every closed segment. A detected decode failure fences further writes and makes disk readiness fail until restart and repair from a verified backup/replica. It rebuilds or opens the sparse index and clamps recovered committed high watermarks to the durable tail.

Take a consistent backup before upgrade. Existing `CDM2` records remain readable but gain no checksum until rewritten by normal compaction; do not claim retrospective corruption detection. Once `CDM3` records are written, downgrading the binary against that data directory is unsupported. The checksum detects accidental record corruption, not malicious tampering. The outer length prefix and torn-tail recovery are not protected by this checksum; an incomplete tail is still handled by the existing recovery policy.

Partition-owned side checkpoints include:

- synced high-watermark state,
- producer epoch/sequence state (also rebuildable from records),
- event-stream indexes and snapshots where enabled.

Transaction visibility indexes are rebuilt from durable transaction records and markers. The durable transaction coordinator decision remains a separate metadata authority.

## Standalone Topic Manifest

A standalone broker stores the authoritative topic registry in `{log_dir}/__topic_metadata.json`. The only supported shape is version 3:

```json
{
  "version": 3,
  "topics": [
    {
      "name": "orders",
      "revision": 3,
      "partitions": 4,
      "replication_factor": 3,
      "lifecycle_epoch": 1,
      "idempotent": true,
      "event_sourcing": false,
      "policy": {
        "cleanup_policy": "delete",
        "partitioner": "hash_key",
        "auth_policy": "acl",
        "read_acl": ["game-server"],
        "write_acl": ["game-server"],
        "retention_hours": 168,
        "retention_bytes": 0
      }
    }
  ]
}
```

The encoded manifest is limited to 16 MiB. Updates write a mode-`0600` same-directory temporary file, sync it, atomically replace the committed manifest, and sync the parent directory where supported. Startup reads only the committed path; abandoned `.tmp` files are not authoritative. Runtime parsing accepts only manifest version 3, disallows unknown fields, and rejects duplicate names, missing revision/replication-factor/lifecycle-epoch fields, invalid definitions, and trailing content. A missing manifest with persisted topic logs, or a valid manifest that omits a persisted topic directory, is an integrity error. The broker does not infer ACL or event-sourcing mode from segment filenames when a manifest is absent or invalid.

Backups of a standalone broker must keep this manifest with topic partition directories, `__transaction_state.journal`, and the consumer offset log. Restoring only segment files cannot reconstruct the full topic policy.

## Standalone Consumer Metadata

`__consumer_offsets` stores version-1 group registration, complete committed-next-offset snapshot, and group tombstone JSON payloads inside ordinary segment frames. Stable semantic keys support compaction; lifecycle epochs fence delete/re-create, and snapshot revisions make replay deterministic across physical internal partitions. Runtime replay rejects unversioned single/bulk offset JSON records.

The internal topic is forced to compact cleanup and unlimited time/size retention regardless of broker defaults, manifest input, or application `CREATE`. Registration/commit acknowledgement synchronously flushes and fsyncs its authoritative log. Corrupt, truncated, conflicting, regressing, unversioned, or key-mismatched records fail readiness instead of being skipped. See [Standalone Clean-Bootstrap Recovery](../../standalone-storage-recovery.md) for the supported reset boundary.

## Standalone Transaction Journal

A standalone broker stores coordinator snapshots in `{log_dir}/__transaction_state.journal`. This file is separate from partition segments. Each record uses the following versioned frame:

    uint32_be payloadLength
    byte[payloadLength] JSON {"version":1,"transaction":{...}}
    uint32_be crc32(payload)

The encoded payload is limited to 32 MiB. Every accepted transition is appended and fsynced. Before appending, the broker truncates bytes beyond the last validated record so a failed partial write cannot hide later acknowledged state. Startup repairs only a torn or checksum-corrupt final frame and rejects corruption before the tail. Every runtime record must use the version-1 envelope; bare transaction snapshots are rejected.

The journal is append-only and currently has no automatic compaction. Backups must keep it consistent with partition logs and the standalone consumer offset store.

## Retention And Compaction

`log_cleanup_policy` accepts `delete`, `compact`, or `delete,compact`. Time/size deletion removes complete eligible closed segments. Standalone compaction rewrites closed segments with same-directory `.compacting` files, fsyncs them, and atomically replaces the authoritative log followed by its rebuilt sparse index. Parent-directory metadata is synced where the platform supports it.

The active segment is preserved by both operations. Active readers defer maintenance. A compacted closed log has a versioned `.log.compacted-<size>` sidecar; offset holes are accepted only when the sidecar is valid and its size matches the log. Backups must preserve the log, its matching sidecar, and its index as one generation. A stale sparse index after an interrupted replacement is correctness-safe because every candidate entry is validated and invalid entries fall back to a scan. Startup removes abandoned compaction temporary files and stale sidecars.

Compaction preserves logical offsets and therefore permits holes only in closed segments. It is rejected for distributed and event-sourcing topics. The full selection and recovery contract is documented in [Log Compaction](log-compaction.md).

## Concurrency

`DiskHandler` separates metadata/file ownership (`mu`), serialized writer operations (`ioMu`), and index reader/writer state (`indexMu`). Write paths acquire metadata before I/O locks. Readers copy the segment list under a short lock and release it before mmap scanning.

## Platform Notes

Linux builds can apply sequential-access advice and zero-copy transfer where the selected path supports it. Windows uses portable file operations. The broker does not rely on `O_DIRECT` or per-batch `O_SYNC` as a universal format guarantee.

## Defaults

| Setting | Default |
|---|---:|
| Segment size | 1 GiB |
| Index size | 10 MiB |
| Sparse index interval | 4096 bytes |
| Async channel capacity | 1024 records |
| Flush batch | 50 records |
| Linger | 50 ms |
| File sync interval | 500 ms |
| Cleanup policy | `delete` |
| Compaction check interval | 300000 ms |
| Minimum cleanable dirty ratio | 0.5 |
