# Cursus Wire Protocol Specification

> Specification revision: 2.0
> Supported wire protocol versions: 2
> Target audience: SDK implementors (C++, Java, Python, Go)

---

## 1. Transport Layer

### TCP Connection

| Property | Value |
|----------|-------|
| Transport | TCP (IPv4/IPv6) |
| Default port | 9000 (configurable) |
| TLS | Optional, TLS 1.2+ required when enabled |
| Health checks | HTTP GET `/live` and `/ready` on port 9080 (configurable) |
| Max concurrent connections | 1000 |
| Idle timeout | 5 seconds (server re-reads on timeout, does NOT disconnect) |

### Connection Lifecycle

```
Client              Broker
  |--- TCP connect --->|
  |                    | (worker assigned)
  |--- NEGOTIATE ----->| (required Wire v2 binary frame)
  |<-- NEGOTIATE ------| (version + compression selected)
  |--- REQUEST -------->| (correlated Wire v2 frame)
  |<-- RESPONSE --------| (same request ID)
  |--- REQUEST -------->|
  |<-- STREAM ----------| (zero or more correlated frames)
  |    ...             |
  |--- EXIT ---------->| (or close socket)
```

A connection that does not begin with the Wire v2 negotiation frame is rejected. The Go client serializes request lifecycles on each connection; use multiple connections for parallel operations.

---

## 2. Framing

Every message is a self-delimiting Wire v2 frame with a fixed 32-byte big-endian header followed by `encoded_length` payload bytes.

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Magic `CRS2` (u32) | Version (u16) | Kind (u8) | Flags (u8)   |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Command (u16) | Status (u16) | Request ID (u64)               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Encoded length (u32) | Decoded length (u32) | CRC32C (u32)    |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Encoded payload (`encoded_length` bytes)                       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

- **Magic**: `0x43525332` (`CRS2`)
- **Version**: exactly `2`
- **Max encoded and decoded payload**: 64 MB (`67,108,864` bytes)
- **Byte order**: Big Endian (network order) throughout the protocol
- **Checksum**: CRC32C (Castagnoli) over the encoded payload
- Application requests and responses require a non-zero request ID. A response or stream frame uses the request's ID and command.

### Compression

Compression is selected by the required connection handshake. Negotiation frames are always uncompressed; every later frame explicitly identifies the selected algorithm in its flags.

| Algorithm | ID |
|-----------|----|
| none | `0` |
| gzip | `1` |
| snappy | `2` |
| lz4 | `3` |

```
Send: decoded payload → compress → header + encoded payload
Recv: header + encoded payload → CRC32C verify → decompress → decoded payload
```

---

## 3. Message Types

| Kind | Value | Purpose |
|------|-------|---------|
| Negotiation request | `1` | Client version range and ordered compression preferences |
| Negotiation response | `2` | Selected Wire version and compression |
| Request | `3` | Correlated application request |
| Response | `4` | Correlated success or structured error |
| Stream | `5` | Correlated stream data/control/end frame |

---

## 4. Negotiation and Application Payloads

### Required Binary Handshake

The first frame must be an uncompressed negotiation request with command `NEGOTIATE`. Its payload is `minimum_version (u16)`, `maximum_version (u16)`, `compression_count (u16)`, followed by that many compression IDs. The broker selects the first mutually supported compression and returns an uncompressed negotiation response containing `version (u16)` and `compression (u8)`. Version 2 must fall inside the requested range. There are no application-level `PROTOCOL_INFO`, feature flags, or text `NEGOTIATE` commands.

### Request Encoding

The frame header carries the command ID. Most request payloads use the deterministic command schema with magic `CRQ2` (`0x43525132`), schema version 2, ordered positional strings, and ordered key/value fields. `PUBLISH` may instead carry a Wire v2 batch with magic `CBV2` (`0x43425632`). Raw text commands and topic envelopes are rejected by the public listener.

### Parameter Parsing Rules

- Parameters use `key=value` syntax, separated by whitespace
- **Parameter order is flexible** — the broker parses key=value pairs regardless of order
- The `message=` parameter is special: it captures the entire remainder of the line
- Field names use lower-case ASCII letters and underscores; later characters may also contain upper-case ASCII letters or digits.

### Response Contract

Application response payloads are machine-readable.

- Success responses MUST be `OK` or `OK key=value ...` unless the command returns a documented JSON envelope with `"status":"OK"`.
- Failure responses use status `ERROR` and a binary error payload containing code, class, retryable flag, message, and deterministic fields.
- Error classes are `validation`, `authorization`, `routing`, `availability`, `conflict`, `fencing`, `not_found`, and `internal`.
- Clients branch on frame status and decoded schemas, never on natural-language text.

### Command Reference

#### Topic Management

**CREATE**
```
CREATE topic=<name> [partitions=<N>] [idempotent=<true|false>] [event_sourcing=<true|false>] [replication_factor=<N>] [min_in_sync_replicas=<N>] [cleanup_policy=<delete|compact|delete,compact>] [retention_hours=<N>] [retention_bytes=<N>] [partitioner=<hash_key|round_robin>] [auth_policy=<open|deny_write|deny_read|acl>] [read_acl=<principal[,principal]>] [write_acl=<principal[,principal]>]
```
| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| topic | Yes | - | Portable topic name: 1-249 ASCII bytes using letters, digits, `.`, `_`, `-`, or `=` |
| partitions | No | 4 for a new topic | Number of partitions; existing topics can only increase |
| idempotent | No | false for a new topic | Enable producer dedup; immutable after creation |
| event_sourcing | No | false for a new topic | Enable aggregate stream commands; immutable after creation and requires non-compacted history |
| cleanup_policy | No | broker default for a new topic | `delete`, `compact`, or canonical combined policy `delete,compact` |
| retention_hours | No | 0 | Per-topic retention hours override metadata; 0 means broker default |
| retention_bytes | No | 0 | Per-topic retention bytes override metadata; 0 means broker default |
| partitioner | No | hash_key | `hash_key` uses message key hash, `round_robin` ignores keys |
| auth_policy | No | open | `open`, `deny_write`, `deny_read`, or `acl` topic policy |
| read_acl | No | - | Comma-separated principals allowed to read when `auth_policy=acl`; `*` allows any authenticated principal |
| write_acl | No | - | Comma-separated principals allowed to write when `auth_policy=acl`; `*` allows any authenticated principal |
| replication_factor | No | 3 | Replica count (distributed mode) |
| min_in_sync_replicas | No | broker default | Optional durable topic override; must be between 1 and the topic replication factor |

Response: `OK topic=<name> partitions=<N> cleanup_policy=<policy> partitioner=<hash_key|round_robin> auth_policy=<open|deny_write|deny_read|acl> read_acl=<csv> write_acl=<csv> retention_hours=<N> retention_bytes=<N> revision=<N> replication_factor=<N> idempotent=<bool> event_sourcing=<bool> lifecycle_epoch=<N> min_in_sync_replicas=<N|default> effective_min_in_sync_replicas=<N>`.

Topic names are a portable on-disk identifier: 1-249 ASCII bytes containing only letters, digits, `.`, `_`, `-`, or `=`; `.` and `..` are reserved. Invalid names return `ERROR: invalid_topic_name ...`.

A missing topic is built from broker defaults and the supplied fields. For an existing topic, `CREATE` is a presence-aware patch: omitted fields retain their authoritative value, while explicit `0`, `false`, and empty `read_acl=`/`write_acl=` values are applied. Partition count can only increase. `replication_factor`, `idempotent`, and `event_sourcing` may be restated with the current value but cannot be changed. A no-op keeps the current `revision`; every effective definition change increments it.

A successful standalone `CREATE` means the revisioned topic definition has been atomically replaced and synced in `{log_dir}/__topic_metadata.json` before the new policy or partition count is exposed to publishers. Broker restart restores the optional `min_in_sync_replicas` override; omission uses the broker `min_insync_replicas` fallback. Manifest format 3 carries lifecycle epochs after truncate. Distributed state uses only Raft snapshot version 9 and requires every partition to carry `committed_hwm_version=1`, an explicit numeric `committed_hwm` (including zero), leader epoch, ISR, and lifecycle epoch.

**ALTER_TOPIC_CONFIG**
```
ALTER_TOPIC_CONFIG topic=<name> min_in_sync_replicas=<N|default>
```
This command atomically replaces only the topic override. `default` removes the override and restores broker fallback; it does not rewrite older metadata with an arbitrary value.

`compact` and `delete,compact` are accepted for non-event-sourcing application topics in standalone and distributed mode. Distributed creation or update requires every active broker to advertise lifecycle protocol version 2; mixed-version attempts return `ERROR: unsupported_topic_policy ...`. Cleaner passes remain gated until all configured replicas are active and in ISR, committed HWM is authoritative, and local/FSM lifecycle epoch, cleanup policy, LEO, and HWM agree. Event-sourcing topics return `ERROR: invalid_topic_policy ...`, and broker-owned internal metadata is never compacted by the distributed cleaner. Repeating `CREATE` for an existing event-sourcing topic cannot use a false `event_sourcing` argument to bypass validation. Repeating `CREATE` also preserves existing partition leader epochs and committed HWMs; only newly added partitions receive new assignments.

**DELETE**
```
DELETE topic=<name> [if_exists=<true|false>]
```
Responses:

- `OK topic=<name> deleted=true` when an existing topic is logically deleted.
- `OK topic=<name> deleted=false` when `if_exists=true` observes an already missing topic.
- Either response can include `cleanup_pending=true` when the durable logical state is committed but node-local storage cleanup must be retried by reconciliation.

`if_exists` defaults to `false`, so a missing topic returns `ERROR: topic_not_found topic=<name>`. Invalid boolean values return `ERROR: invalid_if_exists ...`. `DELETE` requires the admin permission; topic read/write permission is insufficient. Broker-owned `__consumer_offsets` returns `ERROR: internal_topic_delete_forbidden ...` even with `if_exists=true`.

Deletion fails closed with `ERROR: topic_delete_blocked ...` while a consumer group for the topic has active members or an open/committing transaction references it. Successful deletion writes lifecycle tombstones for inactive groups and removes their offsets, removes producer sequence state, and removes target-topic operations from terminal transactions. Event-sourcing indexes and snapshot handles are closed before topic storage is removed. Recreating the same name starts at definition revision 1 and must not restore old logs, offsets, producer state, transaction operations, or event-sourcing metadata.

Standalone deletion first preflights active group and transaction references, then commits removal from the durable manifest before mutating their durable metadata. A manifest or pre-commit event-state failure returns `ERROR: delete_topic_failed ...` and leaves the topic, group offsets, and transaction references live. After the manifest commit, storage or dependency cleanup failure returns success with `cleanup_pending=true` because the topic is no longer authoritative; `if_exists=true` retries dependency cleanup. Stale group/transaction references and the orphan-storage guard reject same-name recreation until cleanup succeeds or an operator remediates the old storage. Distributed deletion is routed to the current leader and serialized in Raft; topic, partition, group, transaction, and producer state remain absent after replay and snapshot restore. Node-local cleanup failures remain visible to the topic materialization reconciler and metrics.

`DELETE` followed by `CREATE` is not a truncate/reset operation and must not be generated implicitly by applications or reconcilers. Use the guarded `TRUNCATE` operation below when the topic definition and identity must be retained.

**TRUNCATE**
```
TRUNCATE topic=<name> expected_revision=<N>
```

`TRUNCATE` is admin-only. `expected_revision` is required and must match the authoritative definition revision. Success increments both `revision` and `lifecycle_epoch`, retains the topic definition, and resets every partition to `LEO=0` and `HWM=0`:

```
OK topic=<name> truncated=true revision=<N> lifecycle_epoch=<N> leo=0 hwm=0 [cleanup_pending=true]
```

The operation fails closed while a consumer group has active members or an open/committing transaction references the topic. On success it deletes inactive groups and offsets, producer sequence state, terminal transaction references, event-sourcing indexes/snapshots, records, and committed watermarks. The broker-owned `__consumer_offsets` topic cannot be truncated.

Standalone mode durably commits the new definition epoch before replacing storage. Until the matching local epoch marker is synced, all access to the topic is fenced and startup resumes cleanup instead of serving the old generation. Distributed mode commits one `TOPIC_TRUNCATE` Raft transition, advances partition leader epochs, and fences message replication, partition commits, and event snapshots whose lifecycle epoch is missing or stale. A node-local cleanup failure returns `cleanup_pending=true` and remains unavailable on that node until materialization converges.

Every active broker must advertise lifecycle protocol version 1 before a distributed truncate is accepted. A Raft directory is valid only when its `.cursus-raft-format` marker contains `9`; non-empty unmarked directories, older markers, older snapshots, and partition metadata without explicit committed-HWM provenance fail startup with `unsupported recovery protocol`. Recovery requires removing all Cursus persistent state and clean-bootstrapping the whole cluster; mixed-version rolling upgrades and downgrade are unsupported.

**LIST**
```
LIST
```
Response: `OK count=<N> topics=<comma-separated-topic-names>`. Empty brokers return `OK count=0 topics=`.

**HELP**
```
HELP
```
Response: `OK commands=<comma-separated-command-names>`.

**DESCRIBE**
```
DESCRIBE topic=<name>
```
Response (JSON):
```json
{
  "status": "OK",
  "topic": "mytopic",
  "definition": {
    "name": "mytopic",
    "revision": 2,
    "partitions": 1,
    "replication_factor": 3,
    "idempotent": false,
    "event_sourcing": false,
    "policy": {
      "cleanup_policy": "delete",
      "partitioner": "hash_key",
      "auth_policy": "open"
    }
  },
  "partitions": [
    {
      "id": 0,
      "leader": "broker-1:9000",
      "replicas": ["broker-1", "broker-2"],
      "isr": ["broker-1", "broker-2"],
      "leo": 1024,
      "hwm": 1020
    }
  ]
}
```

#### Publishing

**PUBLISH**
```
PUBLISH topic=<name> acks=<0|1|-1|all> producerId=<id> [partition=<N>] [seqNum=<N>] [epoch=<N>] [isIdempotent=<true|false>] message=<text>
```
| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| topic | Yes | - | Target topic |
| acks | No | 1 | Durability level |
| producerId | Yes | - | Unique producer ID |
| message | Yes | - | Message payload (captures rest of line) |
| partition | No | round-robin/key policy | Explicit target partition for text PUBLISH. Idempotent producers should use per-partition sequence numbers. |
| seqNum | No | 0 | Sequence number (for idempotent mode) |
| epoch | No | 0 | Producer epoch |
| isIdempotent | No | false | Enable dedup for this message |

Because `message=` captures the rest of the line, optional parameters such as `partition`, `seqNum`, `epoch`, and `isIdempotent` must appear before `message=` in text commands.

Transaction metadata fields such as `transactional_id`, `transaction_state`, and `transaction_marker` are broker-internal on `PUBLISH`. Clients must use `INIT_PRODUCER_ID`, `BEGIN_TXN`, `TXN_PUBLISH`, and `END_TXN`; direct metadata injection is rejected with `ERROR: transaction_metadata_forbidden command=PUBLISH`.

Response (JSON — `AckResponse`):
```json
{
  "status": "OK",
  "last_offset": 42,
  "producer_id": "producer-1",
  "producer_epoch": 0,
  "seq_start": 1,
  "seq_end": 1,
  "leader": "broker-1:9000"
}
```

**Acks Semantics**:

| Value | Behavior |
|-------|----------|
| `0` | Fire-and-forget. The external connection receives no response frame and replication continues asynchronously without a delivery guarantee. |
| `1` | Respond after the partition leader's durable local append. Ordered bounded replication continues after the response; a leader failure before commit can lose this record. |
| `-1` / `all` | Aliases. Reject before append when the current ISR is smaller than the effective minimum, then wait for every broker in the captured ISR and a fenced committed-HWM update. Non-ISR replicas are not part of completion. |

`acks` is a publisher/request setting and is never stored in topic metadata. The effective minimum is the topic `min_in_sync_replicas` override when present, otherwise broker `min_insync_replicas`. Read-committed consumers never read beyond committed HWM, so a leader-only `acks=1` tail remains invisible until asynchronous replication commits it. `enable_idempotence=true` requires `acks=all` or `acks=-1`; other combinations fail before append and producer sequence mutation.

In standalone mode the local broker is the sole replica. `acks=1`, `all`, and `-1` complete after the same durable local append when the effective minimum is 1; `all` and `-1` reject before append when it is greater than 1. `acks=0` still receives no response frame.

#### Cluster Discovery

**FIND_COORDINATOR**
```
FIND_COORDINATOR group=<name>
```
| Param | Required | Description |
|-------|----------|-------------|
| group | Yes | Consumer group name |

Response: `OK coordinator_id=<broker_id> host=<host> port=<port>`

Any broker can answer this command. The coordinator is determined by consistent hashing of the group name across active brokers.

> In cluster mode, group commands (`JOIN_GROUP`, `SYNC_GROUP`, `LEAVE_GROUP`, `HEARTBEAT`, `COMMIT_OFFSET`, `FETCH_OFFSET`) must be sent to the coordinator. If sent to a non-coordinator broker, the response will be:
> `ERROR: NOT_COORDINATOR host=<coordinator_host> port=<coordinator_port>`

**METADATA**
```
METADATA topic=<name>
```
| Param | Required | Description |
|-------|----------|-------------|
| topic | Yes | Topic name |

Response: `OK topic=<name> partitions=<N> leaders=<host:port>,<host:port>,... epochs=<csv> revision=<N> replication_factor=<N> idempotent=<bool> event_sourcing=<bool> cleanup_policy=<policy> partitioner=<policy> auth_policy=<policy> read_acl=<csv> write_acl=<csv> retention_hours=<N> retention_bytes=<N> lifecycle_epoch=<N>`

Returns partition leaders/epochs and the authoritative durable topic policy restored from the standalone manifest or cluster FSM. Leader addresses are in partition order (P0, P1, P2, ...).

Any broker can answer this command. Addresses are the advertised client addresses from the FSM broker registry.

> In cluster mode, `CONSUME` and `STREAM` should be sent to the partition leader. If sent to a non-leader broker, the response will be:
> `ERROR: NOT_LEADER leader=<host:port>`

**CLUSTER_STATUS**
```
CLUSTER_STATUS
```
Response: `OK cluster=<json>`. The JSON payload reports active and inactive brokers, the Raft leader, per-partition leader/epoch/HWM/replica/ISR state, and aggregate leaderless and under-replicated counts. Serialization failure returns `ERROR: marshal_cluster_status_failed reason="..."`.

**ELECT_LEADER**
```
ELECT_LEADER topic=<name> partition=<N> broker=<broker-id>
```
Response: `OK topic=<name> partition=<N> previous_leader=<broker-id> leader=<broker-id> leader_epoch=<N> changed=<true|false>`.

The target must be an active broker in both the replica set and ISR. The Raft FSM compares the expected current leader epoch before changing leaders, increments the epoch exactly once, and preserves the committed HWM and replica membership. A retry against the already selected leader is idempotent. `ELECT_LEADER` is not a partition reassignment or broker-drain command and never promotes an out-of-sync replica.

Missing targets return `ERROR: missing_broker command=ELECT_LEADER`; rejected state changes return `ERROR: leader_election_rejected ...`; an unavailable apply result returns `ERROR: leader_election_result_unavailable ...` and may be retried because election is idempotent.

#### Consumer Group Coordination

**JOIN_GROUP**
```
JOIN_GROUP topic=<name> group=<name> member=<id>
```
Response: `OK generation=<N> member=<actual-id> assignments=[0,1,2]`

> Broker appends a random 4-digit suffix to the member ID.
> e.g., `member=consumer-1` → actual ID `consumer-1-8374`
> A fresh join atomically registers a missing group against the supplied topic and
> its authoritative partition count. `REGISTER_GROUP` is optional explicit
> provisioning for a group that must exist before its first member.

**SYNC_GROUP**
```
SYNC_GROUP topic=<name> group=<name> member=<actual-id>
```
Response: `OK assignments=[0,1,2]`

**LEAVE_GROUP**
```
LEAVE_GROUP topic=<name> group=<name> member=<actual-id>
```
Response: `OK group=<name> member=<actual-id> left=true`

**HEARTBEAT**
```
HEARTBEAT topic=<name> group=<name> member=<actual-id> [generation=<N>]
```
Response: `OK member=<actual-id> generation=<N>`

| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| topic | Yes | - | Portable topic name: 1-249 ASCII bytes using letters, digits, `.`, `_`, `-`, or `=` |
| group | Yes | - | Consumer group name |
| member | Yes | - | Consumer member ID (with suffix) |
| generation | No | - | Current generation number; if supplied, stale generations return ERROR: GEN_MISMATCH ... |

Recommended interval: 3 seconds. Server session timeout: configurable (default ~30s).

**GROUP_STATUS**
```
GROUP_STATUS group=<name>
```
Response (JSON):
```json
{
  "group_name": "mygroup",
  "topic_name": "mytopic",
  "state": "Stable",
  "generation": 3,
  "member_count": 2,
  "partition_count": 4,
  "members": [
    {
      "member_id": "consumer-1-8374",
      "last_heartbeat": "2025-01-01T00:00:00Z",
      "assignments": [0, 1]
    }
  ],
  "last_rebalance": "2025-01-01T00:00:00Z"
}
```

#### Consuming

**CONSUME** (single poll)
```
CONSUME topic=<name> partition=<N> offset=<N> member=<id> group=<name> [autoOffsetReset=<earliest|latest>] [isolation=<read_committed|read_uncommitted>] [batch=<N>] [wait_ms=<N>]
```
| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| topic | Yes | - | Topic name (supports `*` and `?` wildcards) |
| partition | Yes | - | Partition ID |
| offset | Yes | - | Starting offset |
| member | Yes | - | Consumer member ID |
| group | No | default-group | Consumer group |
| autoOffsetReset | No | earliest | `earliest` (0) or `latest` (HWM) |
| isolation | No | read_committed | `read_committed` hides unresolved/aborted transactional records; `read_uncommitted` returns the raw committed log, including transaction metadata and control markers. |
| batch | No | 8192 | Max messages per poll |
| wait_ms | No | 0 | Long-poll timeout in ms |

Response: Binary batch frame (Section 5)

If the broker has a committed offset for `(topic, group, partition)`, `CONSUME`
starts from that committed offset even when the request includes a lower
`offset=` value. If no committed offset exists, the explicit `offset=` value is
used. If the command omits a usable explicit offset in a future protocol
revision, `autoOffsetReset=earliest` starts at `0` and `autoOffsetReset=latest`
starts at the partition high-water mark.

`CONSUME` is a stateless partition-leader read. The
partition leader does not validate consumer group ownership, member liveness, or
generation on the data path. Ownership and generation fencing are enforced by
coordinator commands such as `HEARTBEAT`, `COMMIT_OFFSET`, and `BATCH_COMMIT`.

**STREAM** (continuous push)
```
STREAM topic=<name> partition=<N> member=<id> group=<name> [isolation=<read_committed|read_uncommitted>] [batch=<N>]
```

Opens a continuous stream. Server pushes binary batches at ~100ms intervals. The connection stays open until the client disconnects, the broker removes the stream, the stream times out, or an unrecoverable stream error occurs. Like `CONSUME`, `STREAM` is a stateless partition-leader data path and does not validate group ownership or generation on every read. `STREAM` uses the same `isolation` contract as `CONSUME`; the default is `read_committed`.

Stream delivery never advances the consumer group's committed offset. Delivery to a TCP connection is not processing acknowledgement. After processing a batch, the client must explicitly send `COMMIT_OFFSET` or `BATCH_COMMIT` with its current member and generation.

Keepalive: the server sends a correlated Wire stream frame with status `OK` and an empty payload when no messages are available. Clients treat the empty payload as keepalive and continue reading.

Control frames: the broker may send a correlated Wire stream frame whose payload starts with `STREAM_CONTROL`. Clients inspect control payloads before binary batch decoding.

```text
STREAM_CONTROL type=CLOSE reason=<stopped|removed|timeout|error|offset_out_of_range> offset=<nextOffset>
```

`type=CLOSE` is a graceful stream terminator. `offset` is the broker's next stream offset at close time. `reason=offset_out_of_range` means the requested stream offset is older than the retained log. Clients SHOULD close the socket, keep or refresh their committed offset, and reconnect or rejoin according to the consumer group lifecycle. A broker crash, process kill, or network failure can still close the TCP connection without a terminator; clients MUST treat raw disconnect as retryable and resume from the broker committed offset.

#### Offset Management


**LIST_OFFSETS**

```text
LIST_OFFSETS topic=<name> [partition=<N>]
```

Success response:

```text
OK topic=<name> partitions=<N> offsets=P0:earliest=<N>:latest=<N>:leo=<N>:hwm=<N>,P1:earliest=<N>:latest=<N>:leo=<N>:hwm=<N>
```

`LIST_OFFSETS` returns the broker-side offset range for retained records. `earliest` is the first retained offset. `latest` is the next readable committed offset and is the value clients should use for `autoOffsetReset=latest`. `leo` is the partition log end offset, and `hwm` is the high-water mark before the broker caps reads to the flushed durable tail. A single `partition=` returns only that partition.

Errors:

```text
ERROR: missing_topic command=LIST_OFFSETS
ERROR: topic_not_found topic=<name>
ERROR: invalid_partition command=LIST_OFFSETS
ERROR: partition_not_found partition=<N>
```

**FETCH_OFFSET**
```
FETCH_OFFSET topic=<name> partition=<N> group=<name>
```
Response: `OK offset=<nextOffset>`. If no offset has been committed, the broker returns `OK offset=0` (earliest).

The key is `(topic, group, partition)`. Offsets are independent for every group
and every partition.

**COMMIT_OFFSET**
```
COMMIT_OFFSET topic=<name> partition=<N> group=<name> offset=<N> member=<actual-id> generation=<N>
```
Response: `OK`

The `offset` value is the next offset to read after the client has processed
records. Commits are durable and monotonic per `(topic, group, partition)`.
Committing the same offset is idempotent. Committing an offset lower than the
current committed offset returns an error and does not move the stored offset
backward. `member` and `generation` are required and must identify the current
partition owner; missing values are validation errors and stale values are fencing
errors.

**BATCH_COMMIT**
```
BATCH_COMMIT topic=<name> group=<name> member=<id> generation=<N> offsets=P<partition>:<offset>,P<partition>:<offset>,...
```
Example: `BATCH_COMMIT topic=t1 group=g1 member=m1 generation=3 offsets=P0:100,P1:200,P2:150`

`offsets` is one required Wire v2 field containing at most 1,024 unique partition pairs. Positional offset lists are not part of the current protocol.

Response: `OK batched=<N>`

> The `P` prefix before partition numbers is required.

Batch commits follow the same monotonic rule as `COMMIT_OFFSET` for each
included partition. The broker validates `member` and `generation` before applying
the batch. If any partition is not owned by that member in that generation, the
entire batch is rejected with `ERROR: NOT_OWNER ...`. Stale generations return
`ERROR: GEN_MISMATCH ...`, and unknown members return `ERROR: member_not_found ...`.
The whole batch is also rejected before any offset changes when `generation` is
missing, an entry is malformed, or the same partition appears more than once.


#### Transaction Coordinator

Cursus exposes a broker-managed transaction coordinator for consume-process-produce workflows. In distributed mode, transaction commands are routed by `transactional_id` using the coordinator key `txn:<transactional_id>`. Clients can discover the owner with `FIND_COORDINATOR transactional_id=<id>` and must retry on `ERROR: NOT_COORDINATOR host=<host> port=<port>`.

Standalone brokers append coordinator snapshots to `<log_dir>/__transaction_state.journal` and fsync each accepted transition. One encoded journal snapshot is limited to 32 MiB. Recovery truncates a torn or checksum-corrupt final journal record, rejects non-tail corruption, restores the latest state for each transactional id, and retries durable `committing` work before the client listener becomes ready. Distributed brokers replicate the same snapshots through the Raft FSM as `TXN_SYNC`. Snapshot version 9 requires explicit committed-HWM provenance for every partition. On restore, local data above the authoritative committed HWM is truncated before service, while an HWM above local LEO or missing provenance fails startup rather than guessing.

Clients should first call `INIT_PRODUCER_ID` for a `transactional_id`; the broker returns the authoritative `(producerId, epoch)` session and bumps `epoch` on re-initialization to fence older producers. The coordinator fences stale producers by `(transactional_id, producerId, epoch)`: lower epochs are rejected, and staged operations must use the same producer and epoch that opened the transaction. After `transactional_id_expiration_ms`, completed transactions discard staged message/offset payloads but retain a compact epoch tombstone. The tombstone participates in standalone journal and distributed metadata snapshots, preventing an older producer session from being revived. Active `open` and `committing` transactions are not expired by the cleanup path.

**INIT_PRODUCER_ID**

```text
INIT_PRODUCER_ID transactional_id=<id>
```

Success: `OK transactional_id=<id> producerId=<producer-id> epoch=<N>`. Re-initializing the same `transactional_id` returns the same broker-managed `producerId` with a higher `epoch`, aborts any open local staging for that id, and fences commands that still use the previous epoch. If the transaction is already `committing`, the broker rejects reinitialization so the prepared commit can be retried or recovered.

**BEGIN_TXN**

```text
BEGIN_TXN transactional_id=<id> producerId=<producer-id> epoch=<N>
```

Success: `OK transactional_id=<id> state=open producerId=<producer-id> epoch=<N>`. One initialized epoch may begin one transaction. After a successful commit or abort, another `BEGIN_TXN` with that epoch returns `ERROR: producer_reinitialization_required ...`; call `INIT_PRODUCER_ID` to obtain the next epoch. This does not prevent retrying an uncertain `END_TXN` with the original epoch.

**TXN_PUBLISH**

```text
TXN_PUBLISH transactional_id=<id> topic=<topic> [partition=<N>] producerId=<producer-id> seqNum=<N> epoch=<N> [key=<key>] message=<payload>
```

Success: `OK transactional_id=<id> staged_messages=1 topic=<topic> partition=<N>`.

The record is staged in the transaction coordinator and is not published until `END_TXN ... result=commit`. `seqNum` is required and must be greater than zero; the broker uses `(producerId, epoch, seqNum)` to make commit recovery idempotent even when the target topic is not globally idempotent. Commit publishes records through the normal partition-leader and replication path with `transaction_state=open`, then appends a hidden Cursus transaction marker to every touched partition. A matching marker resolves the partition log, but `read_committed` also requires the coordinator decision for the current transaction epoch to be `committed`; marker append alone cannot expose output while the transaction is still `committing`. Aborting an `open` transaction writes no output or control records. Once durable commit preparation begins, abort is fenced and recovery must finish the commit; readers still understand abort markers already present in older logs.

**SEND_OFFSETS_TO_TXN**

```text
SEND_OFFSETS_TO_TXN transactional_id=<id> producerId=<producer-id> epoch=<N> topic=<topic> group=<group> member=<member> generation=<N> offsets=P<partition>:<nextOffset>,P<partition>:<nextOffset>
```

Success: `OK transactional_id=<id> staged_offsets=<N>`.

The broker validates `member`, `generation`, partition ownership, and monotonic offsets before staging, then revalidates them before commit. Every offset in one transaction must share exactly one `(topic, group, member, generation)` scope. Repeating a partition replaces only an equal or higher staged `nextOffset`; a lower value is rejected. Commit applies the scope with one fenced `BATCH_COMMIT`, so either every partition offset in that scope advances or none does. Use separate transactions for different consumer scopes.

**END_TXN**

```text
END_TXN transactional_id=<id> producerId=<producer-id> epoch=<N> result=<commit|abort>
```

Success: `OK transactional_id=<id> state=<committed|aborted> messages=<N> offsets=<N>`. Retrying the same final result with the same `producerId` and `epoch` is idempotent; a lower epoch is rejected as fenced, and trying to abort a committed transaction or commit an aborted transaction returns an error. Once commit preparation has durably entered `committing`, abort is rejected because output markers or source offsets may already have been applied; clients must retry commit with the same producer session.

**TXN_STATUS**

```text
TXN_STATUS transactional_id=<id>
```

Success: `OK transactional_id=<id> state=<open|committing|committed|aborted> messages=<N> offsets=<N>`.

Current guarantee: a successful transaction commit has one durable coordinator decision and follows this order:

1. validate the staged output records and the single consumer offset scope while the transaction is still `open`,
2. persist the prepared `committing` state,
3. revalidate current topic, ownership, generation, and monotonic-offset fences,
4. publish output records idempotently through partition leaders and replication,
5. append hidden transaction commit markers to all touched partitions,
6. apply the staged offsets with one generation/ownership-fenced bulk commit,
7. persist the final `committed` coordinator decision.

`read_committed` exposes a transaction only when its partition commit marker and current-epoch coordinator decision agree. Aborted records and control markers are skipped; the earliest unresolved transaction defines the stable visibility boundary. Partitions maintain an in-memory transaction index rebuilt from durable logs. For historical producer epochs no longer retained by the coordinator, the durable partition marker remains authoritative; every currently tracked epoch requires marker/decision agreement. Coordinator state is restored from the standalone fsynced versioned journal or, in distributed mode, from `TXN_SYNC` and version-9 Raft metadata snapshots. A broker that restores `committing` state retries the prepared work; producer sequence state rebuilt from logs prevents duplicate records. Retried finalization with the same epoch is idempotent.

The marker uses Cursus control metadata (`control_batch_type=transaction`, `control_batch_version=2`, `control_batch_coordinator_epoch=<epoch>`) and control-record bytes (`key: int16 version, int16 markerType`; `value: int16 version, int32 coordinatorEpoch`). The surrounding segment and network protocol remain Cursus-owned. The transaction covers broker output records and one consumer offset scope; external database, HTTP, filesystem, or service effects remain outside it and require application-level idempotency or their own transaction.
#### Event Sourcing Commands

These commands are available only on topics created with `event_sourcing=true`.

Event-sourcing commands are partition-routed by aggregate `key`. In distributed mode, `APPEND_STREAM`, `READ_STREAM`, `STREAM_VERSION`, `SAVE_SNAPSHOT`, and `READ_SNAPSHOT` must execute on the leader for the aggregate partition. A non-leader broker returns `ERROR: NOT_LEADER leader=<host:port>`; clients should reconnect to that address and retry. Followers index replicated event-sourcing messages, apply quorum-replicated snapshots, and rebuild local stream indexes from the committed log on restart. Partitions persist their high watermark checkpoint and restore it on broker restart so committed reads resume from the last successful committed tail.

**APPEND_STREAM**
```
APPEND_STREAM topic=<name> key=<aggregate_key> version=<N> event_type=<type> [schema_version=<N>] [metadata=<json>] message=<payload>
```
| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| topic | Yes | - | Event-sourcing-enabled topic |
| key | Yes | - | Aggregate ID (determines partition routing) |
| version | Yes | - | Expected next version (must equal current version + 1) |
| event_type | No | "" | Event type name (e.g., `OrderCreated`) |
| schema_version | No | 1 | Schema version for upcasting |
| metadata | No | "" | Arbitrary JSON metadata |
| message | Yes | - | Event payload (captures rest of line) |

Success response: `OK version=<N> offset=<N> partition=<N>`

Internal broker catch-up commands: `LIST_SNAPSHOTS topic=<name> partition=<N>`, `FETCH_SNAPSHOT topic=<name> partition=<N> key=<aggregate_key>`, and `CATCHUP_SNAPSHOTS topic=<name> partition=<N> [leader=<host:port>]`. These commands are for broker-to-broker recovery only. In distributed mode, internal broker commands require `internal_token=<shared-token>` and brokers must be configured with `internal_auth_token`; clients and SDKs must not send these commands directly. Operators can additionally set `internal_broker_port` to move broker-to-broker command forwarding off the public client listener. When `internal_use_tls=true`, that internal listener requires mutual TLS using `internal_tls_cert_path`, `internal_tls_key_path`, and `internal_tls_ca_path`; the router dials peer brokers on the internal port with the same CA trust. The shared internal token remains a command-level guard, but mTLS/internal listener separation is the stronger network boundary.

Error responses:
- `ERROR: version_conflict current=<N> expected=<N>` — optimistic concurrency failure
- `ERROR: event_sourcing_not_enabled topic=<name>` - topic not created with `event_sourcing=true`
- `ERROR: invalid_schema_version` - `schema_version` was not an unsigned integer
- `ERROR: NOT_LEADER leader=<host:port>` - command reached a non-leader broker in distributed mode

**READ_STREAM**
```
READ_STREAM topic=<name> key=<aggregate_key> [from_version=<N>] [limit=<1..1024>] [through_version=<N>] [lifecycle_epoch=<N>] [snapshot=<true|false>]
```
| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| topic | Yes | - | Portable topic name: 1-249 ASCII bytes using letters, digits, `.`, `_`, `-`, or `=` |
| key | Yes | - | Aggregate ID |
| from_version | No | 1 | Positive starting version; a usable snapshot advances the event batch to snapshot version + 1 |

Response: Two correlated Wire stream frames sent sequentially.

Frame 1 — JSON envelope:
```json
{
  "status": "OK",
  "topic": "orders",
  "key": "order-123",
  "partition": 2,
  "count": 5,
  "snapshot": {"version": 500, "payload": "..."}
}
```

The `snapshot` field is included only when a usable snapshot exists at or after `from_version`, snapshot use is enabled, and the snapshot is within `through_version` when specified. `count` reflects the number of events in Frame 2 (not including the snapshot).

Paged responses include `stream_version`, `has_more`, `next_version`, and `lifecycle_epoch`. `limit` defaults to 256 and is capped at 1024; encoded batches are additionally capped at 64 MiB. Continue with `from_version=next_version`, the initial `through_version=stream_version`, the initial `lifecycle_epoch`, and `snapshot=false`. `has_more=false` uses `next_version=0`. Invalid limits/cursors, missing indexed records, and encoding failures return an error before either success frame. Without an explicit `limit`, a multi-page result returns `stream_page_required` instead of a partial result.

Frame 2 — Wire v2 `CBV2` batch containing the events. If no events exist after the snapshot, the batch has message count 0. A missing `from_version` defaults to `1`; zero or non-numeric values return a Wire v2 error response with code `invalid_from_version` and no batch frame.

**STREAM_VERSION**
```
STREAM_VERSION topic=<name> key=<aggregate_key>
```
| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| topic | Yes | - | Portable topic name: 1-249 ASCII bytes using letters, digits, `.`, `_`, `-`, or `=` |
| key | Yes | - | Aggregate ID |

Response: `OK version=<N>` (for example, `OK version=6`). Returns `OK version=0` if the aggregate does not exist.

**SAVE_SNAPSHOT**
```
SAVE_SNAPSHOT topic=<name> key=<aggregate_key> version=<N> message=<payload>
```
| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| topic | Yes | - | Portable topic name: 1-249 ASCII bytes using letters, digits, `.`, `_`, `-`, or `=` |
| key | Yes | - | Aggregate ID |
| version | Yes | - | Aggregate version this snapshot represents (must be <= current version) |
| message | Yes | - | Serialized aggregate state (captures rest of line) |

Success response: `OK version=<N> partition=<N>`

In distributed mode, success means the leader stored the snapshot and replicated it to the configured in-sync replica quorum through the internal `REPLICATE_SNAPSHOT internal_token=<shared-token> payload=<json>` broker-to-broker command. Clients should not send `REPLICATE_SNAPSHOT` directly. Brokers reject internal commands without the configured token with `ERROR: internal_command_unauthorized ...`, and reject distributed-mode internal commands when no token is configured with `ERROR: internal_auth_not_configured ...`.

Error responses:
- `ERROR: snapshot_version_exceeds_stream version=<N> current=<N>`
- `ERROR: snapshot_replicate_failed reason="..."`

**READ_SNAPSHOT**
```
READ_SNAPSHOT topic=<name> key=<aggregate_key>
```
| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| topic | Yes | - | Portable topic name: 1-249 ASCII bytes using letters, digits, `.`, `_`, `-`, or `=` |
| key | Yes | - | Aggregate ID |

Success response: `OK snapshot=<json>`
```text
OK snapshot={"version":500,"payload":"..."}
```

Not found response: `OK snapshot=null`.

---

## 5. Binary Batch Format

### Batch Schema

All integers are big-endian. Strings and byte arrays use a `uint32` length followed by that many bytes.

```
magic (u32 = 0x43425632 "CBV2")
version (u16 = 2)
flags (u16; bit 0 = idempotent)
topic (string)
partition (i32)
acks (string)
batch_start (u64)
batch_end (u64)
record_count (u32)
records (record_count × length-prefixed record bytes)
```

### Message Record

Each record starts with `version (u16 = 2)` and a `presence (u64)` bitmap, followed by required topic, partition, offset, and payload fields. Optional producer, timestamp, key, event-sourcing, transaction, and control-record fields appear in bitmap order. Decoders reject unknown presence bits, trailing bytes, routing fields that conflict with the enclosing batch, and more than 100,000 records.

### Batch Response

Server responds with the same binary batch format for CONSUME. PUBLISH with `acks=1`, `acks=all`, or `acks=-1` returns JSON `AckResponse` with `status="OK"`. External PUBLISH with `acks=0` emits no response frame; internal forwarding still receives a private response so routing can complete safely.

---

## 6. Consumer Group Protocol

### Lifecycle

```
1. JOIN_GROUP   → receive generation, member ID, initial assignments
2. SYNC_GROUP   → receive confirmed partition assignments
3. Loop:
   a. HEARTBEAT      (every 3s)
   b. CONSUME/STREAM (fetch messages)
   c. COMMIT_OFFSET  (periodically)
4. LEAVE_GROUP  → graceful exit
```

### Rebalancing

Rebalance is triggered when:
- A new consumer joins
- An existing consumer leaves or times out
- Partitions are added to the topic

Detection:
- `HEARTBEAT` returns `ERROR: GEN_MISMATCH ...`, `ERROR: member_not_found ...`, or another `ERROR:` response
- `COMMIT_OFFSET` or `BATCH_COMMIT` returns `ERROR: NOT_OWNER ...`, `ERROR: GEN_MISMATCH ...`, or `ERROR: member_not_found ...`

Client action:
1. Stop consuming.
2. Commit already processed offsets only while the member still owns those partitions.
3. Re-execute `JOIN_GROUP` then `SYNC_GROUP`.
4. Fetch broker committed offsets for the new assignments.
5. Resume consuming from those offsets.
### Offset Resolution Priority

```
1. Saved offset from coordinator for (topic, group, partition)
2. Explicit offset from request parameter
3. autoOffsetReset = "latest" → partition `LIST_OFFSETS latest` value
4. autoOffsetReset = "earliest" → 0
```

### SDK Offset and Group Contract

All SDKs should implement the same consumer group resume behavior:

- After `JOIN_GROUP` and `SYNC_GROUP`, fetch `FETCH_OFFSET` for every assigned partition before consuming.
- Treat the broker committed offset as the source of truth on reconnect, rejoin, and broker restart. SDK-local offset caches must not override a broker committed offset.
- Commit only after records have been processed. The committed value is `lastProcessedOffset + 1`.
- Send `member=<actual-id>` and `generation=<N>` on `COMMIT_OFFSET` when the SDK has group membership state.
- Send `BATCH_COMMIT` entries as `P<partition>:<nextOffset>` and include `member` plus `generation`.
- Treat `ERROR: offset_regression ...` as a failed commit; do not update local committed state from it.
- Treat `ERROR: GEN_MISMATCH ...`, `ERROR: NOT_OWNER ...`, and `ERROR: member_not_found ...` from `HEARTBEAT`, `COMMIT_OFFSET`, or `BATCH_COMMIT` as group membership failures that require stopping consumption and rejoining.
- On `ERROR: OFFSET_OUT_OF_RANGE ...`, apply the SDK `auto_offset_reset` policy: `earliest` resumes from the broker-reported earliest retained offset, `latest` resumes from the broker-reported latest offset, and `error` fails the consumer instead of silently skipping or replaying data.
### Offset Lifecycle and Delivery Guarantees

Consumer group registrations, tombstones, and committed next offsets are stored by the standalone broker in versioned records in `__consumer_offsets` and loaded again before readiness. A successful registration survives without a commit; `FETCH_OFFSET` returns `0` for its uncommitted partitions. Successful commits are synchronously persisted as monotonic complete snapshots, and the internal topic is compacted with unlimited retention rather than inheriting application delete retention. Replay corruption, inconsistency, or an unversioned record keeps readiness false and never falls back to an empty coordinator. The offline storage command reports unsupported record shapes but cannot convert or authorize them. See [Standalone Clean-Bootstrap Recovery](standalone-storage-recovery.md) for the reset procedure.

In distributed mode, offset updates are also applied through the Raft FSM and included in FSM snapshots.

Recommended client loop:

1. Fetch or join/sync assignments.
2. Consume from the broker-selected resume offset.
3. Process the returned records.
4. Commit `lastProcessedOffset + 1` using `COMMIT_OFFSET ... member=<id> generation=<N>` or `BATCH_COMMIT ... offsets=P<partition>:<nextOffset>`.

This gives at-least-once delivery when the client commits after processing. If a
client commits before processing and then crashes, it may skip unprocessed
records, which is at-most-once behavior for that client. Cursus provides broker-managed transaction commands for consume-process-produce workflows, including producer fencing, durable transaction state, idempotent finalization, hidden partition transaction markers with durable transaction control-record key/value bytes and Cursus control-batch metadata, startup recovery for prepared commits, and read-committed filtering with a stable visibility boundary for unresolved open transactions. It is still not exactly-once for external effects, so applications that need exactly-once external effects must still make their processors idempotent or transactional outside the broker.

Example for a game server such as wargame-IOCP:

```
JOIN_GROUP topic=match-events group=wargame-iocp member=game-01
SYNC_GROUP topic=match-events group=wargame-iocp member=game-01-1234
FETCH_OFFSET topic=match-events partition=0 group=wargame-iocp
# broker returns: OK offset=<nextOffset>
CONSUME topic=match-events partition=0 offset=<nextOffset> group=wargame-iocp member=game-01-1234 batch=128
COMMIT_OFFSET topic=match-events partition=0 group=wargame-iocp offset=<lastProcessedOffset+1>
```

The game server does not need its own Postgres table for Cursus offsets once it
uses this API. On reconnect or broker restart, the same group resumes from the
last successful broker commit.

---

## 7. Error Handling

### Error Response Format

Every Wire v2 error response has status `ERROR` and a typed binary payload. Its readable form is:

```text
ERROR: <code> class=<class> retryable=<true|false> [key=value ...]
```

`retryable=true` means the same logical operation may be attempted again after applying the response instructions, such as reconnecting to the advertised leader or waiting for coordinator availability. `retryable=false` means blindly repeating the same request is not valid; the client may still recover by changing state, rejoining a group, correcting input, or obtaining authorization.

| Class | Meaning | Typical client action |
|-------|---------|-----------------------|
| `routing` | Request reached the wrong broker | Follow redirect metadata and retry |
| `availability` | Required broker subsystem is temporarily unavailable | Back off and retry |
| `fencing` | Producer epoch, group generation, member, or ownership is stale | Recreate producer state or rejoin; do not repeat unchanged request |
| `conflict` | Request conflicts with authoritative state | Refresh state and decide whether to issue a changed request |
| `authorization` | Principal or topic policy denied the operation | Fail closed |
| `not_found` | Requested resource does not exist | Create or select a valid resource |
| `validation` | Command syntax or value is invalid | Correct the request |
| `internal` | Broker could not classify a safe recovery action | Fail closed and surface diagnostics |

SDKs should parse the code and fields first, use `class` for broad handling, and use `retryable` only as a retry eligibility signal. They must still apply bounded backoff and must never retry a non-idempotent operation unless its command contract makes the retry safe. The broker's authoritative code-to-class registry is implemented in `pkg/protocol`; a coverage test fails when a new static `ERROR:` emission is added without a registry entry.

### Error Codes

| Error | Cause | Client Action |
|-------|-------|---------------|
| `invalid_topic_name topic=<X>` | Topic name violates the portable storage contract | Use 1-249 ASCII bytes containing letters, digits, `.`, `_`, `-`, or `=` |
| `topic_not_found topic=<X>` | Topic not created | CREATE topic first |
| `PARTITION_NOT_FOUND <N>` | Invalid partition ID | Check DESCRIBE for valid IDs |
| `TOPIC_NOT_FOUND <X>` | Topic not found | CREATE topic first |
| `NOT_AUTHORIZED_FOR_PARTITION <T>:<P>` | Not partition leader | Redirect to correct leader |
| `NOT_LEADER leader=<addr>` | Not Raft leader | Reconnect to specified address |
| `group_not_found group=<X>` | Group not registered | JOIN_GROUP or REGISTER_GROUP |
| `topic_not_assigned_to_group` | Topic mismatch | Verify group registration |
| `GEN_MISMATCH current=N requested=N group=<G> member=<M>` | Stale generation | Re-join group |
| `NOT_OWNER partition=N member=<M> group=<G> generation=N` | Assignment changed | Re-join group |
| `member_not_found member=<M> group=<G>` | Member is no longer active | Re-join group |
| `offset_regression reason="..."` | Commit lower than current stored offset | Treat commit as failed; refetch offset |
| `stale_producer_epoch producer=<id> current=<N> got=<N>` | Idempotent producer request uses an older fenced epoch | Treat as fatal for that producer instance; create a new producer session |
| `producer_reinitialization_required transactional_id=<id> epoch=<N>` | A completed epoch attempted to begin another transaction | Call `INIT_PRODUCER_ID`, then begin with the returned higher epoch; retain the old epoch only for uncertain finalization retry |
| `OFFSET_OUT_OF_RANGE requested=N earliest=N latest=N` | Requested offset is older than retained log or beyond available range | Treat as data loss or reset according to policy |
| `no_valid_offsets` | BATCH_COMMIT contains no usable offsets | Supply at least one `P<N>:<offset>` entry |
| `missing_generation command=<command>` | Group command omitted its generation | Rejoin if needed and send the current generation |
| `invalid_batch_commit_entry reason=<reason>` | BATCH_COMMIT `offsets` is missing, malformed, oversized, or repeats a partition | Correct the named offsets field and retry |
| `duplicate_partition partition=N group=<G> topic=<T>` | BATCH_COMMIT repeats a partition | Send one next offset per partition; no offsets were committed |
| `NOT_PARTITION_LEADER leader=<id> requested_leader=<id>` | Replication reached a broker that is not the current partition leader | Refresh partition metadata and retry against the leader |
| `PARTITION_LEADER_FENCED` | An internal partition commit no longer matches the current leader or epoch | Do not acknowledge success; refresh partition metadata before retrying |
| `STALE_LEADER_EPOCH current=N requested=N` | Replication request carries a fenced leader epoch | Discard the stale leader session and refresh metadata |
| `cluster_metadata_unavailable command=REPLICATE_MESSAGE` | Cluster metadata subsystem is temporarily unavailable | Back off and retry |
| `partition_metadata_not_found topic=<T> partition=N` | Partition metadata does not exist | Refresh metadata and verify topic/partition |
| `missing_leader_fence command=REPLICATE_MESSAGE` | Internal replication omitted leader ID or epoch | Correct the broker request; do not retry unchanged |
| `invalid_commit_watermark reason="..."` | Commit watermark is ahead of local durable data or otherwise invalid | Repair/catch up the replica before retrying |
| `replica_index_prepare_failed reason="..."` | The follower could not prepare its committed event-stream index before HWM advancement | Repair the local index/storage error and retry the HWM commit |
| invalid_control_batch_bytes field=<key|value> | Broker-internal transaction control bytes are not valid base64 | Reject the internal publish and inspect the producing broker's Wire v2 encoding |
| `version_conflict current=N expected=N` | Optimistic concurrency failure | Reload aggregate and retry |
| `event_sourcing_not_enabled topic=<X>` | ES command on non-ES topic | CREATE topic with `event_sourcing=true` |
| `snapshot_version_exceeds_stream version=N current=N` | Invalid snapshot version | Use version <= current stream version |

### Leader Redirect

In distributed mode, if a request reaches a non-leader broker:

```
ERROR: NOT_LEADER leader=192.168.1.10:9000
```

Client should reconnect to the specified address and retry.

---

## 8. Topic Policy Contract

### Authorization

The wire protocol exposes per-topic `auth_policy` metadata: `open`, `deny_write`, `deny_read`, and `acl`. When `enable_sasl=true`, all protected admin, topic, group, and transaction commands require connection authentication with `AUTH principal=<principal> token=<token>` or inline `principal=<principal> auth_token=<token>`.

Configured users may receive `admin`, `topic.read`, `topic.write`, `group`, `transaction`, or wildcard `*` permissions. Commands that cross boundaries require every applicable permission: `CONSUME`/`STREAM` require `topic.read` plus `group`, `TXN_PUBLISH` requires `transaction` plus `topic.write`, and `SEND_OFFSETS_TO_TXN` requires `transaction` plus `group`. Missing authentication returns `ERROR: authentication_required command=<COMMAND>`; insufficient coarse permission returns `ERROR: NOT_AUTHORIZED_FOR_OPERATION command=<COMMAND> permission=<permission>`.

After coarse authorization, `auth_policy=acl` checks `read_acl` for topic reads and `write_acl` for topic writes, returning `ERROR: NOT_AUTHORIZED_FOR_TOPIC topic=<T> operation=<read|write>` on denial. Internal broker contexts bypass client permissions but remain subject to the separate internal-listener/token boundary. This is a token authentication contract, not a mechanism-specific SASL byte protocol; use TLS/mTLS and network controls across trust boundaries. Every configured principal requires an explicit non-empty permission list; invalid authentication configuration fails startup.

### Retention

Topics store `retention_hours` and `retention_bytes` policy metadata. A value of `0` means the broker-level default applies. Enforcement is still performed by the broker storage/retention loop, and retention-gap reads return `ERROR: OFFSET_OUT_OF_RANGE requested=<N> earliest=<N> latest=<N>` instead of an empty batch. Clients should treat this as data loss for that group and recover according to application policy, such as alerting, resetting to `earliest`, or rebuilding from another source.

### Partition Keys

`partitioner=hash_key` routes keyed messages with FNV-1a 64-bit hash modulo the topic partition count and routes unkeyed messages round-robin. `partitioner=round_robin` ignores message keys and routes every publish by round-robin. Increasing partition count can remap future records for an existing key.

---

## 9. Idempotent Producer

### Enable

Set `isIdempotent=true` on PUBLISH or in binary batch header.

### Requirements

- Idempotent publishing requires `acks=all` or `acks=-1`; weaker modes are rejected before append and sequence mutation
- Each idempotent message must have a unique `(producerId, epoch, seqNum)` tuple within a partition
- `seqNum` must be monotonically increasing within the current producer epoch for each partition
- A new `(producerId, epoch)` sequence starts at `seqNum=1`; starting above 1 is rejected as a gap
- A higher `epoch` fences the previous producer session and may restart `seqNum` from 1
- A lower `epoch` is rejected as stale producer state
- `seqNum` = 0 disables dedup for non-transactional publish messages; transactional `TXN_PUBLISH` requires `seqNum > 0`
- Broker rejects duplicates silently (returns OK)

### Sequence Tracking

- Broker tracks the last seen `(epoch, seqNum)` per `(producerId)` per partition
- Disk-backed partitions persist producer sequence checkpoints, rebuild producer state from partition logs on broker restart, and use that state to make transactional commit recovery idempotent
- Distributed FSM snapshots also include producer sequence state for replicated message commands
- Distributed recovery writes and accepts only FSM snapshot version 9. Every partition includes explicit committed-HWM provenance; older persistent state requires a full clean bootstrap.
- Producer state expires from memory after `producer_state_ttl_ms` of inactivity (default 30 minutes); durable checkpoints retain the last persisted sequence until the partition data is removed

---

## 10. SDK Implementation Checklist

### Required

- [ ] Required Wire v2 handshake and exact version enforcement
- [ ] CRS2 frame encoding/decoding, CRC32C, request correlation, and explicit compression
- [ ] CBV2 batch and CRQ2 command payload encoding/decoding
- [ ] JSON response parsing (AckResponse, DESCRIBE, GROUP_STATUS)
- [ ] Consumer group lifecycle (JOIN → SYNC → HEARTBEAT → CONSUME → COMMIT)
- [ ] Broker-owned offset resume, monotonic `nextOffset` commits, and `auto_offset_reset` gap handling
- [ ] Explicit `read_committed` / `read_uncommitted` on `CONSUME` and `STREAM`
- [ ] Typed binary error payload parsing
- [ ] Structured error class and retry eligibility handling
- [ ] Leader redirect handling

### Recommended

- [ ] TLS 1.2+ support
- [ ] Compression (gzip/snappy/lz4)
- [ ] Connection pooling
- [ ] Exponential backoff on reconnect
- [ ] Async offset commits (BATCH_COMMIT)
- [ ] Idempotent producer with sequence numbers
- [ ] Transaction lifecycle (`INIT_PRODUCER_ID`, one transaction per epoch, retryable finalization, and automatic reinitialization)
- [ ] Wildcard topic subscription
- [ ] Stream mode (continuous push)
- [ ] Read/write buffer tuning (2MB recommended)
- [ ] Event sourcing: APPEND_STREAM with optimistic concurrency
- [ ] Event sourcing: READ_STREAM two-frame response parsing
- [ ] Event sourcing: Snapshot save/read
- [ ] Event sourcing: Auto-snapshot after N events (recommended: 500)
