# Event Sourcing

## Overview

Event sourcing is a persistence pattern where state changes are captured as an immutable, append-only sequence of domain events. Instead of storing only the current state, the system stores every event that led to it. The current state is derived by replaying events from the beginning (or from a snapshot).

Cursus provides native event sourcing support at the broker level. Aggregates are modeled as key-based logical streams within existing topics. The broker handles stream indexing, optimistic concurrency control, and snapshot management. Projections and schema evolution remain the responsibility of client applications.

### Why Native Support Matters

Without broker-level support, applications must implement concurrency control, version tracking, and stream indexing on top of a general-purpose message log. This leads to duplicated effort across SDKs and introduces race conditions that are difficult to resolve without coordination at the storage layer. By moving these concerns into the broker, Cursus serializes optimistic writes for one aggregate on its current partition leader. In distributed mode, a successful append also follows the configured in-sync replica quorum and leader-epoch checks.

### Design Principles

- **Reuse existing primitives.** Events are stored as regular messages in the append-only partition log and use partition replication, HWM, and consumer groups. Event-sourcing topics preserve their complete history; the transaction command family does not automatically wrap `APPEND_STREAM`.
- **Opt-in per topic.** The `event_sourcing=true` flag on CREATE enables event sourcing behavior. Non-event-sourcing topics are completely unaffected.
- **Broker owns storage, SDK owns domain logic.** The broker handles versioning, indexing, and snapshots. Schema evolution (upcasting) is the SDK's responsibility.

---

## Quick Start

### 1. Create an Event-Sourcing Topic

```
CREATE topic=orders partitions=4 event_sourcing=true cleanup_policy=delete
```

Response:

```
OK topic=orders partitions=4
```

The `event_sourcing=true` flag tells the broker to maintain a per-partition stream index for aggregate-level version tracking.

Event-sourcing topics do not inherit broker retention limits. Positive topic retention limits and compaction are rejected because stream indexes require the complete event log. This applies before storage maintenance starts, including restart and partition growth. `cleanup_policy=delete` remains the metadata value but does not enable automatic event-history deletion.

Plan storage capacity and alerts for an indefinitely growing event log. Application snapshots accelerate replay but do not authorize log deletion or replace the broker's version index. Before upgrading, remove explicit retention limits from event-sourcing topic definitions. Upgrade every cluster broker before relying on this protection; older brokers may still delete history. If earlier retention already removed log prefixes, restore the complete log from backup. Recovery errors stop stream operations instead of resetting aggregate versions. Administrative topic deletion and truncation remain explicitly destructive operations.

### 2. Append Events

Append events to an aggregate stream using `APPEND_STREAM`. The `key` parameter is the aggregate ID, and `version` is the expected next version (optimistic concurrency).

```
APPEND_STREAM topic=orders key=order-123 version=1 event_type=OrderCreated message={"customer":"alice","total":49.98}
```

Response:

```
OK version=1 offset=0 partition=2
```

Append a second event:

```
APPEND_STREAM topic=orders key=order-123 version=2 event_type=ItemAdded message={"sku":"GADGET-7","qty":1}
```

Response:

```
OK version=2 offset=1 partition=2
```

### 3. Check Stream Version

```
STREAM_VERSION topic=orders key=order-123
```

Response:

```
OK version=2
```

Returns `0` if the aggregate does not exist.

### 4. Read a Stream

`READ_STREAM` returns all events for an aggregate. The response consists of two length-prefixed frames: a JSON envelope followed by a binary batch containing the events.

```
READ_STREAM topic=orders key=order-123
```

Frame 1 (JSON envelope):

```json
{
  "status": "OK",
  "topic": "orders",
  "key": "order-123",
  "partition": 2,
  "count": 2
}
```

Frame 2: Wire v2 `CBV2` batch containing the two events.

To read events starting from a specific version:

```
READ_STREAM topic=orders key=order-123 from_version=2
```

Large streams require pagination. Add `limit=256` (allowed range 1–1024). Each response is also bounded by the 64 MiB Wire v2 frame limit, so a page may contain fewer events than requested. The envelope includes `stream_version`, `has_more`, `next_version`, and `lifecycle_epoch`. While `has_more=true`, request the next page with `from_version=next_version`, the original `through_version=stream_version` and `lifecycle_epoch`, and `snapshot=false`. This pins replay to its initial boundary and detects topic truncation. Do not delete and recreate a topic while clients are replaying it: a new topic may reuse the initial lifecycle epoch. A request without `limit` fails with `stream_page_required` when its complete result cannot fit one default page; it never silently truncates history. Encoding and index/log consistency errors precede the success envelope.

In Go, `ReadStream`/`ReadStreamFrom` return `sdk.ErrStreamPageRequired` rather than partial data when the result spans pages. Use `ReadStreamPage` for an individual bounded page or `WalkStream` for incremental processing at a fixed version:

```go
err := store.WalkStream("order-123", 1, func(page *sdk.StreamData) error {
    for _, event := range page.Events {
        if err := applyEvent(event); err != nil {
            return err
        }
    }
    return nil
})
```

Version `0` allows an initial snapshot; positive versions request event history without snapshot skipping. `AggregateRepository.Load` and `sdk.Replay` use page visitors when the store supports them. Callbacks run after a page has been validated; a later error does not roll back callbacks from earlier pages. Do not retain all pages if bounded replay memory is required. Update brokers before using the new page APIs; clients reject responses without the page identity/boundary fields.

---

## Optimistic Concurrency

Every `APPEND_STREAM` call includes a `version` parameter that specifies the expected next version of the aggregate. The broker checks this against the current version before writing.

### Version Semantics

| version value | Meaning |
|---------------|---------|
| `1` | Expect this to be a new aggregate (current version must be 0) |
| `N` | Expect the current version to be `N-1` |

The broker rejects the write if the expected version does not match:

```
APPEND_STREAM topic=orders key=order-123 version=2 event_type=ItemAdded message={"sku":"X"}
```

If the current version is already 3:

```
ERROR: version_conflict current=3 expected=2
```

### Handling VERSION_CONFLICT

The standard pattern for handling conflicts is optimistic retry:

1. Reload the aggregate by reading the stream again.
2. Re-apply the domain command against the updated state.
3. Re-attempt the append with the correct version.
4. Repeat up to a maximum retry count (3 is recommended).

```
// Pseudocode
for retries := 0; retries < 3; retries++ {
    aggregate := loadFromStream("orders", "order-123")
    newEvent  := aggregate.handle(command)
    result    := appendStream("orders", "order-123", aggregate.version+1, newEvent)
    if result.ok {
        break
    }
    // VERSION_CONFLICT: loop retries with fresh state
}
```

Two concurrent writers for the same aggregate are serialized by the broker. Exactly one succeeds; the other receives VERSION_CONFLICT and must retry.

---

## Snapshots

As the number of events in a stream grows, replaying all events on every load becomes expensive. Snapshots solve this by persisting the aggregate state at a specific version. On subsequent reads, the broker returns the snapshot plus only the events that occurred after it.

### Saving a Snapshot

```
SAVE_SNAPSHOT topic=orders key=order-123 version=3 message={"customer":"alice","items":[...],"total":69.97,"status":"shipped"}
```

Response:

```
OK version=3 partition=2
```

Constraints:
- The `version` must be less than or equal to the current stream version.
- A new snapshot for the same aggregate overwrites the previous one.

### Reading a Snapshot

```
READ_SNAPSHOT topic=orders key=order-123
```

Response:

```text
OK snapshot={"version":3,"payload":"{\"customer\":\"alice\",\"items\":[...],\"total\":69.97,\"status\":\"shipped\"}"}
```

If no snapshot exists, the response is:

```text
OK snapshot=null
```

### Automatic Use in READ_STREAM

When a snapshot exists, `READ_STREAM` automatically uses it. The JSON envelope includes the snapshot, and the binary batch contains only events after the snapshot version:

```
READ_STREAM topic=orders key=order-123
```

Frame 1:

```json
{
  "status": "OK",
  "topic": "orders",
  "key": "order-123",
  "partition": 2,
  "count": 2,
  "snapshot": {
    "version": 3,
    "payload": "..."
  }
}
```

Frame 2: Binary batch with events at versions 4 and 5 only.

### Snapshot Frequency Recommendation

Choose a snapshot interval from measured replay cost, snapshot size, and aggregate write rate. Five hundred events can be a starting point, not a broker guarantee. SDKs may implement workload-specific auto-snapshot logic:

```
// Pseudocode
if aggregate.version % 500 == 0 {
    saveSnapshot(topic, key, aggregate.version, serialize(aggregate))
}
```

### Snapshot Replication

In distributed mode, `SAVE_SNAPSHOT` is routed to the aggregate partition leader. The leader validates that the snapshot version is not ahead of the current stream version, stores the snapshot locally, then replicates it to followers with an internal `REPLICATE_SNAPSHOT` command before returning success. A successful response means the snapshot reached the configured in-sync replica quorum.

Snapshots remain a replay optimization: correctness still comes from the committed event log. If a broker lacks a snapshot after failover or restore, it can rebuild stream state by replaying committed events.

---

## Distributed Event Sourcing

Event-sourcing topics use the same partition leadership and replication model as normal Cursus topics. The aggregate `key` determines the partition, and the partition leader is the authority for append ordering and optimistic concurrency.

### Write Path

In distributed mode, `APPEND_STREAM` is routed to the leader for the aggregate partition. The leader:

1. checks the current aggregate version in the stream index,
2. appends the event to the partition log with the next aggregate version,
3. replicates the assigned message to followers,
4. indexes the event only after the append/replication path succeeds, and
5. returns `OK version=<N> offset=<N> partition=<N>`.

Followers append replicated event-sourcing messages without treating the uncommitted tail as part of the stream index. Before a broker serves leader-side append, read, version, or snapshot validation, it advances its derived index only through the stable committed HWM. This preserves the leader-assigned offset and aggregate version without allowing an uncommitted tail to influence optimistic version checks, including after leadership changes. On restart, each broker rebuilds the derived stream index from the committed partition log before answering stream commands, discarding index entries beyond the recovered HWM.

### Read Path

`READ_STREAM`, `STREAM_VERSION`, `SAVE_SNAPSHOT`, and `READ_SNAPSHOT` are partition-routed commands. In distributed mode, clients should send them to the leader for the aggregate partition. A non-leader broker returns a leader redirect:

```text
ERROR: NOT_LEADER leader=<host:port>
```

SDKs should reconnect to the advertised leader and retry the command. `READ_STREAM` reads from the committed log, so it does not expose uncommitted leader-local events.

### Snapshots, Committed Tail, and Retention

Snapshots are quorum-replicated by the partition leader before `SAVE_SNAPSHOT` returns `OK`. Followers apply the replicated snapshot locally, so a promoted follower can serve snapshot-assisted reads for snapshots that reached quorum. Snapshot replication is quorum-gated on the write path. If a node was offline during the write, it can run the internal CATCHUP_SNAPSHOTS command for the partition to pull the latest snapshot catalog from the partition leader and apply missing snapshots locally. Correctness still comes from committed event replay if snapshot catch-up is delayed.

Each partition persists its committed tail as a high-watermark checkpoint. The checkpoint is written through a synced temporary file and restored with a durable-tail clamp on restart. Leader appends advance LEO first; HWM advances only after the replicated write path succeeds and is flushed. On broker restart, the partition restores the checkpointed HWM and `READ_STREAM`/`CONSUME` only expose records at or below the recovered committed tail. This prevents uncommitted leader-local records that happened to be flushed before a crash from becoming visible after restart.

Because event sourcing relies on replay, retention must be configured carefully. If old event records are deleted before a durable snapshot/export strategy exists for the aggregate, a broker can no longer rebuild the full stream history from version 1. For event-sourcing topics, prefer retention settings that preserve the event log for as long as the domain requires replay.

### Guarantees

- The current partition leader serializes successful appends per aggregate key; optimistic version checks define their logical order.
- Optimistic concurrency is enforced by requiring `version = currentVersion + 1`.
- Replicated followers maintain their own derived stream index, but advance it only through the stable committed HWM.
- Snapshot writes are quorum-replicated before success.
- Reads return committed events only, bounded by the recovered HWM after restart.
- An append error after a network or broker failure can leave the client uncertain whether the event reached the committed log. Retry the identical `(topic, key, version, payload)` command and keep projection handlers idempotent.
- Exactly-once external side effects are not provided by the broker.

---
## Schema Evolution

Events are immutable. Once written, their payload never changes. Schema evolution is handled at read time by the client application.

### Event Type and Schema Version

Each event carries two fields for schema management:

| Field | Description |
|-------|-------------|
| `event_type` | The event type name (e.g., `OrderCreated`, `ItemAdded`) |
| `schema_version` | An integer version for the event's schema (default: 1) |

Example:

```
APPEND_STREAM topic=orders key=order-123 version=4 event_type=OrderCreated schema_version=2 message={"customer":"alice","email":"alice@example.com","total":49.98}
```

### Upcasting in the SDK

When reading a stream, the SDK should check each event's `schema_version` and transform older schemas to the current version. This process is called upcasting.

```
// Pseudocode: upcaster chain
func upcast(eventType string, schemaVersion int, payload map[string]any) map[string]any {
    if eventType == "OrderCreated" && schemaVersion == 1 {
        // v1 -> v2: add email field with default
        payload["email"] = "unknown"
        schemaVersion = 2
    }
    return payload
}
```

Register upcasters per event type in the SDK. When `readStream` returns events, apply the upcaster chain before passing events to the domain model.

---

## Projections

Cursus does not provide a built-in projection engine. Instead, projections are built using the existing `CONSUME` and `STREAM` commands with consumer groups.

### Pattern

1. Create a consumer group on the event-sourcing topic.
2. Consume events using `CONSUME` or `STREAM` (continuous push mode).
3. For each event, update the read model (database, cache, search index).
4. Commit offsets after processing.

```
JOIN_GROUP topic=orders group=order-projections member=projector-1
SYNC_GROUP topic=orders group=order-projections member=<assigned-member-id> generation=<N>
FETCH_OFFSET topic=orders partition=0 group=order-projections
CONSUME topic=orders partition=0 offset=<nextOffset> member=<assigned-member-id> group=order-projections generation=<N> isolation=read_committed
COMMIT_OFFSET topic=orders partition=0 group=order-projections offset=<lastProcessedOffset+1> member=<assigned-member-id> generation=<N>
```

Because event-sourcing topics are regular Cursus topics, all consumer group features work: rebalancing, offset management, wildcard subscriptions, and batch commits.

### Multiple Projections

Different consumer groups can maintain independent projections of the same event stream:

- `order-summary-projection` -- updates a summary dashboard
- `order-search-projection` -- indexes orders into a search engine
- `order-analytics-projection` -- feeds an analytics pipeline

Each group tracks its own offsets and processes events independently.

---

## Command Reference

### APPEND_STREAM

Append an event to an aggregate stream with optimistic concurrency control.

```
APPEND_STREAM topic=<name> key=<aggregate_key> version=<N> event_type=<type> [schema_version=<N>] [metadata=<json>] message=<payload>
```

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| topic | Yes | - | Event-sourcing-enabled topic |
| key | Yes | - | Aggregate ID (used for partition routing) |
| version | Yes | - | Expected next version (must equal current + 1) |
| event_type | No | "" | Event type name |
| schema_version | No | 1 | Schema version for upcasting |
| metadata | No | "" | Arbitrary JSON metadata |
| message | Yes | - | Event payload (captures rest of line) |

Success response:

```
OK version=<N> offset=<N> partition=<N>
```

Error responses:

```
ERROR: version_conflict current=<N> expected=<N>
ERROR: topic_not_found topic=<name>
ERROR: event_sourcing_not_enabled topic=<name>
```

### READ_STREAM

Read events from an aggregate stream. Returns two length-prefixed frames.

```
READ_STREAM topic=<name> key=<aggregate_key> [from_version=<N>]
```

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| topic | Yes | - | Topic name |
| key | Yes | - | Aggregate ID |
| from_version | No | 1 | Positive starting version; a usable snapshot advances the returned event batch |

Frame 1 -- JSON envelope:

```json
{
  "status": "OK",
  "topic": "...",
  "key": "...",
  "partition": 0,
  "count": 5,
  "snapshot": {"version": 500, "payload": "..."}
}
```

The `snapshot` field is present only when a snapshot exists at or after `from_version`. Zero or non-numeric `from_version` values return a JSON `invalid_from_version` error envelope.

Frame 2 -- Binary batch (0xBA7C format) containing the events.

### STREAM_VERSION

Get the current version of an aggregate stream.

```
STREAM_VERSION topic=<name> key=<aggregate_key>
```

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| topic | Yes | - | Topic name |
| key | Yes | - | Aggregate ID |

Response: `OK version=<N>` (for example, `OK version=3`). Returns `OK version=0` if the aggregate does not exist.

### SAVE_SNAPSHOT

Save an aggregate snapshot at a specific version.

```
SAVE_SNAPSHOT topic=<name> key=<aggregate_key> version=<N> message=<payload>
```

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| topic | Yes | - | Topic name |
| key | Yes | - | Aggregate ID |
| version | Yes | - | Version this snapshot represents (must be <= current version) |
| message | Yes | - | Serialized aggregate state (captures rest of line) |

Success response:

```
OK version=<N> partition=<N>
```

Error responses:

```
ERROR: snapshot_version_exceeds_stream version=<N> current=<N>
ERROR: topic_not_found topic=<name>
```

### READ_SNAPSHOT

Read the latest snapshot for an aggregate.

```
READ_SNAPSHOT topic=<name> key=<aggregate_key>
```

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| topic | Yes | - | Topic name |
| key | Yes | - | Aggregate ID |

Success response:

```text
OK snapshot={"version":500,"payload":"..."}
```

Not found response:

```text
OK snapshot=null
```

---

## Best Practices

### Aggregate Design

- **One stream per aggregate instance.** Use the aggregate ID as the `key`. For example, each order gets its own stream keyed by `order-<uuid>`.
- **Keep events small.** Store only what changed, not the full state. State reconstruction is the job of the aggregate's `apply` method.
- **Use descriptive event type names.** `OrderCreated`, `ItemAdded`, `PaymentReceived` -- not `Update` or `Change`.
- **Include causation and correlation IDs in metadata.** This enables tracing command-to-event chains across aggregates.

### Snapshot Frequency

- Use 500 events only as an initial snapshot interval; measure replay latency and snapshot write cost for the aggregate.
- Adjust based on aggregate load time requirements. If aggregates rarely exceed 100 events, snapshots may not be necessary.
- Never skip snapshots entirely for high-traffic aggregates. Without snapshots, loading an aggregate with 100,000 events requires replaying all of them.

### Partition Key Choice

- The `key` field determines partition routing. All events for the same aggregate always land on the same partition.
- Choose keys with good cardinality. UUIDs work well. Sequential integers can cause hot partitions if the hash distribution is poor.
- If you need cross-aggregate ordering guarantees, use a shared key (e.g., `customer-<id>`). But note this means all aggregates with that key share a single partition, which limits write throughput.

### Concurrency and Retries

- Always use optimistic concurrency (the `version` parameter). Appending without version checks defeats the purpose of event sourcing.
- Limit retries to 3 attempts. If a write still fails after 3 retries, the aggregate is under heavy contention. Consider redesigning the aggregate boundaries.
- In systems with eventual consistency, prefer idempotent event handlers over exactly-once delivery guarantees.

### Projections

- Use separate consumer groups for each projection. This allows independent progress and failure isolation.
- Projections should be rebuildable. Because committed offsets are monotonic, rebuild with a new consumer group id (or a future explicit administrative reset workflow) rather than attempting to commit an older offset to the existing group.
- For time-sensitive projections, use `STREAM` (continuous push mode) instead of polling with `CONSUME`.
