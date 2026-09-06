# Configuration

This document provides a comprehensive guide to configuring cursus. 

It covers all available configuration parameters, their default values, configuration sources, and the precedence order when configuration is specified in multiple locations.

## Purpose and Scope

### Configuration in cursus controls:

- Network settings (ports, TLS)
- Performance tuning (buffer sizes, batch sizes, timeouts)
- Operational behavior (cleanup intervals, metrics exporters)
- Disk persistence parameters (flush batching, linger times)
- Message handling (compression, channel capacities)

Broker configuration is a flat `Config` value. YAML uses snake-case keys and JSON uses the dotted keys declared on `pkg/config.Config`.

## Configuration Sources and Precedence

cursus applies configuration in this order:

1. built-in defaults,
2. parsed command-line flags,
3. the YAML or JSON file selected by `--config` or `CONFIG_PATH`,
4. supported environment-variable overrides,
5. normalization and security validation.

The configuration file therefore overrides ordinary value flags, and supported
environment variables override both. `--raft-peers` is the one value flag
applied after file loading and before environment overrides. Use `--config`
to select a file and environment variables for deliberate deployment-time
overrides; do not assume that `--port` or another ordinary flag overrides a
value present in that file.

## Configuration File Format

Configuration files can be in either YAML or JSON format. The format is detected automatically based on the file extension.

### YAML Format

The standard configuration format used in cursus is YAML. Keys are top-level;
there is no `broker:` wrapper:

```
broker_port: 9000
health_check_port: 9080
enable_exporter: true
exporter_port: 9100
log_dir: "broker-logs"
compression_type: "lz4"
disk_flush_batch_size: 500
disk_write_timeout_ms: 200
linger_ms: 100
log_segment_roll_ms: 604800000
log_cleanup_policy: "delete"

static_consumer_groups:
  - name: "workers"
    consumer_count: 2
    topics: ["orders"]
    topic_partitions:
      orders: 6
```

### JSON Format

The same configuration can be expressed in JSON format:

```
{
  "broker.port": 9000,
  "health.check.port": 9080,
  "log.dir": "broker-logs",
  "enable.exporter": true,
  "exporter.port": 9100,
  "log.cleanup.interval": 60,
  "tls.enable": false,
  "tls.cert_path": "certs/server.crt",
  "tls.key_path": "certs/server.key",
  "compression.type": "lz4",
  "disk.flush.batch.size": 500,
  "linger.ms": 100,
  "channel.buffer.size": 10000,
  "partition.channel.buffer.size": 10000,
  "consumer.channel.buffer.size": 1000
}
```

## Configuration Structure

The configuration is represented by the Config struct in the codebase, which organizes parameters into logical categories.


### Request Admission

The broker reserves one in-flight slot and `32 + encoded_length + decoded_length` bytes after validating each incoming frame header, before allocating its body. Reservations cover negotiation and application requests and remain held through processing and response writes; idle connections do not reserve slots. Client and internal listeners use separate, process-local pools so client saturation does not consume the internal replication pool.

| YAML parameter | Environment override | Default |
|----------------|----------------------|---------|
| `max_inflight_requests` | `MAX_INFLIGHT_REQUESTS` | 32 |
| `max_request_bytes` | `MAX_REQUEST_BYTES` | 134217728 (128 MiB) |
| `max_internal_inflight_requests` | `MAX_INTERNAL_INFLIGHT_REQUESTS` | 32 |
| `max_internal_request_bytes` | `MAX_INTERNAL_REQUEST_BYTES` | 134217728 (128 MiB) |

Corresponding command-line flags replace underscores with hyphens. Non-positive broker values normalize to these bounded defaults; Helm rejects non-positive overrides. Limits are fixed at startup. Admission exhaustion closes the connection before reading the body or executing the command, without a success response. Transport failures do not prove whether an earlier request committed: only retry using the operation's idempotency/transaction contract, and back off under saturation. A frame allowed by the wire protocol can still exceed the remaining admission budget; reduce batch size or provision capacity for both encoded and decoded sizes plus the header.

These counters bound incoming frame reservations, **not process RSS**. Parsed command copies, compression scratch buffers, responses, stream buffers, queues, transaction state and event indexes need additional memory headroom. Long polls retain a slot until completion; leave capacity for client heartbeats and other control requests. Stream handoff releases the initial request reservation and is governed separately by stream limits.

Prometheus exposes `cursus_request_inflight`, `cursus_request_inflight_limit`, `cursus_request_reserved_bytes`, `cursus_request_byte_limit` and `cursus_request_rejected_total`, each labeled `listener="client"` or `listener="internal"`. Alert on sustained rejection increases and budget utilization together with process memory and latency; do not treat low reservation usage as evidence that total memory is safe.

### Common Parameters

| Parameter           | Type       | Default        | Description                                      |
|--------------------|------------|----------------|--------------------------------------------------|
| `broker_port`        | int        | 9000           | Main broker TCP port for client connections     |
| `health_check_port`  | int        | 9080           | HTTP port for `/live` and `/ready`                         |
| `log_dir`            | string     | "broker-logs"  | Directory path for persistent log segments      |
| `enable_exporter`    | bool       | true           | Enable Prometheus metrics exporter              |
| `exporter_port`      | int        | 9100           | HTTP port for the Prometheus `/metrics` endpoint       |
| `log_level`          | string     | "info"         | Broker log level: `debug`, `info`, `warn`, or `error` |
| `log_cleanup_interval` | int      | 300            | Legacy maintenance-loop interval (seconds)      |

In standalone mode, `log_dir` also contains `__topic_metadata.json`, the broker-owned `__consumer_offsets` topic, and `__transaction_state.journal`. The versioned topic manifest is atomically replaced before create/update success exposes a new topic definition and is loaded before internal-topic validation, durable group/offset replay, coordinator/static-group initialization, and readiness. Invalid or unsupported manifest/internal metadata fails closed in diagnostics-only mode; the broker does not fall back to guessed ACL, event-sourcing, retention, group, or offset state. A pre-manifest directory with persisted logs requires a complete [`clean bootstrap`](../standalone-storage-recovery.md); no offline import or migration command is supported.

The broker fsyncs the append-only transaction coordinator journal before acknowledging transaction state transitions and repairs only a torn or checksum-corrupt final record during startup. One encoded snapshot record is limited to 32 MiB, so transaction batches must remain bounded. Include the topic manifest, journal, consumer offset log, and partition directories in one backup and restore procedure.

The health and metrics listeners are unauthenticated operations endpoints. Restrict both ports to a trusted network. `/live` reports process liveness, while `/ready` includes topic metadata, consumer metadata, storage, and distributed leader checks. See [Broker Observability](../reference/observability.md).

# Security and Compression

| Parameter       | Type   | Default | Description                                  |
|----------------|--------|---------|----------------------------------------------|
| `use_tls`        | bool   | false   | Enable TLS for TCP connections               |
| `tls_cert_path`  | string | ""      | Path to TLS certificate file                 |
| `tls_key_path`  | string | ""      | Path to TLS private key file                 |
| `internal_broker_port` | int | 0 | Optional dedicated broker-to-broker command port |
| `internal_use_tls` | bool | false | Require mutual TLS on the internal broker listener |
| `internal_tls_cert_path` | string | "" | Broker certificate for internal mTLS |
| `internal_tls_key_path` | string | "" | Broker private key for internal mTLS |
| `internal_tls_ca_path` | string | "" | CA used to verify peer broker certificates |
| `internal_tls_server_name` | string | "" | Server name used by broker-to-broker mTLS clients |
| `enable_sasl` | bool | false | Enable SASL-PLAIN-style token authentication for text commands |
| `sasl_users` | list | [] | Principal/token/permissions entries accepted by `AUTH` and inline authentication |
| `compression_type` | string | "none" | Preferred codec: `none`, `gzip`, `snappy`, or `lz4` |

When `use_tls` is enabled and certificate paths are provided, the broker loads the certificate using `tls.LoadX509KeyPair()` during initialization. In distributed mode, `internal_broker_port` moves broker-to-broker text commands away from the public client listener. If `internal_use_tls` is enabled, the internal listener requires client certificates signed by `internal_tls_ca_path`, and peer routers dial the internal port with mTLS using `internal_tls_server_name` for certificate verification.

When `enable_sasl` is enabled, protected commands require `AUTH principal=<principal> token=<token>` or inline `principal=<principal> auth_token=<token>`. Every user must declare at least one permission from `admin`, `topic.read`, `topic.write`, `group`, `transaction`, and `*`; startup rejects missing, unknown, repeated, or duplicate-principal entries. `CONSUME`/`STREAM` require both `topic.read` and `group`; `TXN_PUBLISH` requires `transaction` and `topic.write`; `SEND_OFFSETS_TO_TXN` requires `transaction` and `group`. Topic `auth_policy=acl` is evaluated after the coarse permission check. The environment form is `SASL_USERS=principal:token:permission1|permission2`, with comma-separated users.

# DiskHandler Performance Tuning

These parameters directly affect the write path performance and batching behavior described in DiskHandler and Write Path.

| Parameter             | Type | Default | Description                                                |
|----------------------|------|---------|------------------------------------------------------------|
| `disk_flush_batch_size` | int  | 50      | Number of messages to batch before flushing to disk       |
| `linger_ms`             | int  | 50      | Maximum time to wait before flushing (milliseconds)       |
| `channel_buffer_size`   | int  | 1024    | Buffer size for DiskHandler's writeCh channel             |
| `disk_write_timeout_ms` | int  | 10      | Timeout while enqueueing an asynchronous write (ms)       |
| `disk_flush_interval_ms`| int  | 500     | Periodic fsync interval (milliseconds)                    |
| `log_segment_bytes`     | uint64 | 1073741824 | Maximum segment file size (1GB default)                |
| `log_index_size_bytes`  | uint64 | 10485760   | Maximum index file size (10MB default)                 |
| `log_index_interval_bytes` | int | 4096    | Index entry interval in bytes                            |
| `log_retention_hours`   | int  | 168     | Log retention period in hours (7 days default)            |
| `log_retention_bytes`   | int64 | -1     | Retained byte limit; `-1` means unlimited                 |
| `log_segment_roll_ms`   | int  | 604800000 | Time-based roll interval (7 days)                       |
| `log_cleanup_policy`    | string | "delete" | `delete`, `compact`, or `delete,compact`; distributed compaction is safety-gated |
| `log_retention_check_interval_ms` | int | 300000 | Delete-retention evaluation interval                  |
| `log_compaction_check_interval_ms` | int | 300000 | Closed-segment compaction evaluation interval         |
| `log_min_cleanable_dirty_ratio` | float64 | 0.5 | Minimum removable-byte ratio before compaction         |
| `compression_type`      | string | "none" | Compression type: "none", "gzip", "snappy", "lz4"       |


Trade-offs:

- Higher `disk_flush_batch_size`: Better throughput, higher queueing latency, and more records waiting for flush/sync
- Lower `linger_ms`: Lower latency, more frequent I/O operations, reduced throughput
- Larger `channel_buffer_size`: Better handling of burst traffic, higher memory usage

# Partition and Consumer Channel Tuning

These parameters control the in-memory channel buffer sizes for message distribution within the topic management system.

| Parameter                     | Type | Default | Description                                      |
|-------------------------------|------|---------|--------------------------------------------------|
| `partition_channel_buffer_size` | int  | 10000   | Buffer size for each Partition's input channel  |
| `consumer_channel_buffer_size`  | int  | 1000    | Buffer size for each Consumer's message channel |
| `broadcast_channel_buffer_size` | int  | 10000   | Buffer size for embedded topic broadcast channels |

# Broker And Cluster Parameters

These values participate in active broker behavior:

| Parameter | Default | Purpose |
|---|---:|---|
| `enabled_distribution` | false | Enable the Raft-backed cluster runtime. |
| `min_insync_replicas` | 2 | Broker fallback minimum for `acks=all`/`-1` when a topic has no `min_in_sync_replicas` override. |
| `default_replication_factor` | 3 | Default replica count for new distributed topics. |
| `internal_broker_port` | 0 | Dedicated broker-to-broker command listener; configure in production clusters. |
| `internal_auth_token` | empty | Shared internal command credential; always required when distribution is enabled. |
| `internal_use_tls` | false | Enables broker-internal TLS and client-certificate verification. |
| `allow_insecure_cluster_transport` | false | Explicit test-only opt-out from the distributed mTLS requirement. |
| `raft_peers` | [] | Initial Raft peer addresses. |
| `transactional_id_expiration_ms` | 604800000 | Retention for completed transaction payloads. Epoch tombstones remain for fencing; active transactions are not expired. |
| `producer_state_ttl_ms` | 1800000 | In-memory producer state cleanup window; durable records/checkpoints remain recovery sources. |
| `raft_port` | 9001 | Raft transport listener. |
| `discovery_port` | 8000 | Broker discovery and internal replication HTTP listener. |
| `raft_snapshot_interval_ms` | 120000 | Interval for evaluating snapshot creation. |
| `raft_snapshot_threshold` | 8192 | Outstanding Raft log entries required before snapshotting. |
| `raft_trailing_logs` | 10240 | Raft log entries retained after a snapshot. |
| `static_cluster_members` | [] | Stable `broker-id@host:raft-port` membership set. |
| `bootstrap_cluster` | false | Explicitly bootstrap a new Raft cluster. |
| `advertised_host` | localhost | Host advertised for broker discovery. |
| `advertised_broker_port` | 0 | Broker port advertised to peers when different from the listener. |
| `advertised_client_host` | empty | Client-facing host returned by routing metadata. |
| `max_client_connections` | 1000 | Concurrent client connection limit. |
| `client_idle_timeout_ms` | 60000 | Idle client connection deadline. |
| `max_stream_connections` | 1000 | Concurrent streaming connection limit. |
| `stream_timeout` | 30m | Maximum broker stream lifetime as a Go duration string. |
| `consumer_session_timeout_ms` | 10000 | Group member session timeout. |
| `consumer_heartbeat_check_ms` | 5000 | Broker interval for detecting expired members; normalized below the session timeout. |
| `enable_idempotence` | false | Broker default for producer idempotence; topic/request contracts can enable it explicitly. |

Distribution is disabled by default. Production clusters should use a dedicated internal listener, mTLS, least-privilege client users, and explicit advertised addresses.

Topic creation can set `min_in_sync_replicas=<N>` with `1 <= N <= replication_factor`. `ALTER_TOPIC_CONFIG topic=<name> min_in_sync_replicas=<N|default>` changes or removes that optional durable override. Old topic metadata without the field continues to use the broker fallback. Idempotent publishers require `acks=all` or `acks=-1`; the SDK and broker reject weaker combinations rather than silently changing them.

`bootstrap_servers` and `acks` are SDK publisher settings, while
`replication_factor` is a topic-definition field. They are not broker
configuration keys.

`static_consumer_groups` is a broker bootstrap facility. Each entry declares
`name`, `consumer_count`, `topics`, and an optional
`topic_partitions` map. Missing or non-positive partition counts normalize to
one for the corresponding topic. Network clients still use the durable
`REGISTER_GROUP`, `JOIN_GROUP`, and `SYNC_GROUP` lifecycle; static
in-process groups are not a substitute for that protocol.

# Using Configuration in Different Scenarios

## Scenario 1: Development with Defaults

For local development, you can run the broker with built-in defaults:

```
./bin/cursus
```

This uses:

- Port: 9000
- Health check port: 9080
- Exporter port: 9100
- Log directory: `./broker-logs`


## Scenario 2: Using a Configuration File

Create a configuration file and specify it:

```
./bin/cursus --config /path/to/config.yaml

// Or using the environment variable:
// export CONFIG_PATH=/path/to/config.yaml
// ./bin/cursus
```

The environment variable approach is checked in `pkg/config/properties.go`

## Scenario 3: Docker Deployment

In docker-compose deployments, configuration is typically mounted as a volume and referenced via environment variable:

```
services:
  broker:
    volumes:
      - ./config.yaml:/root/config.yaml
    environment:
      - CONFIG_PATH=/root/config.yaml
    ports:
      - "9000:9000"
      - "9100:9100"
      - "9080:9080"
```

## Scenario 4: Deployment-Time Overrides

Use supported environment variables when a deployment must override values
loaded from a configuration file:

```
CONFIG_PATH=config.yaml BROKER_PORT=9001 EXPORTER_PORT=9101 ./bin/cursus
```

Ordinary value flags are parsed before the file and are overwritten by matching
file values. This ordering is part of the current implementation contract.

## Scenario 5: High-Throughput Configuration

For maximum throughput at the cost of latency:

```
disk_flush_batch_size: 1000    # Batch more messages
linger_ms: 200                 # Wait longer before flush
channel_buffer_size: 20000     # Larger write buffer
partition_channel_buffer_size: 20000
consumer_channel_buffer_size: 5000
```

## Scenario 6: Low-Latency Configuration

For minimum latency at the cost of throughput:

```
disk_flush_batch_size: 50
linger_ms: 10
channel_buffer_size: 1024
partition_channel_buffer_size: 5000
consumer_channel_buffer_size: 500
```

# Configuration Parameter Mapping

The Config struct uses both YAML and JSON tags to support both formats. Here's how parameter names map between different formats:

| Go Field Name             | YAML Key                     | JSON Key                      | CLI Flag                  |
|---------------------------|------------------------------|-------------------------------|---------------------------|
| BrokerPort                | `broker_port`                | `broker.port`                 | --port                   |
| HealthCheckPort           | `health_check_port`          | `health.check.port`           | --health-port            |
| LogDir                    | `log_dir`                    | `log.dir`                     | --log-dir                |
| EnableExporter            | `enable_exporter`            | `enable.exporter`             | --exporter               |
| ExporterPort              | `exporter_port`              | `exporter.port`               | --exporter-port          |
| CleanupInterval           | `log_cleanup_interval`       | `log.cleanup.interval`        | --cleanup-interval       |
| UseTLS                    | `use_tls`                    | `tls.enable`                  | --tls                    |
| TLSCertPath               | `tls_cert_path`              | `tls.cert_path`               | --tls-cert               |
| TLSKeyPath                | `tls_key_path`               | `tls.key_path`                | --tls-key                |
| CompressionType           | `compression_type`           | `compression.type`            | --compression-type       |
| InternalBrokerPort        | `internal_broker_port`       | `distribution.internal_broker_port` | --internal-broker-port |
| InternalUseTLS            | `internal_use_tls`           | `internal_tls.enable`         | --internal-tls           |
| InternalTLSCertPath        | `internal_tls_cert_path`     | `internal_tls.cert_path`      | --internal-tls-cert      |
| InternalTLSKeyPath         | `internal_tls_key_path`      | `internal_tls.key_path`       | --internal-tls-key       |
| InternalTLSCAPath          | `internal_tls_ca_path`       | `internal_tls.ca_path`        | --internal-tls-ca        |
| InternalTLSServerName      | `internal_tls_server_name`   | `internal_tls.server_name`    | --internal-tls-server-name |
| EnableSASL                 | `enable_sasl`                | `sasl.enable`                 | --enable-sasl            |
| ProducerStateTTLMS        | `producer_state_ttl_ms`      | `producer.state.ttl.ms`       | --producer-state-ttl-ms  |
| TransactionalIDExpirationMS | `transactional_id_expiration_ms` | `transactional.id.expiration.ms` | --transactional-id-expiration-ms |
| DiskFlushBatchSize        | `disk_flush_batch_size`      | `disk.flush.batch.size`       | --disk-flush-batch       |
| LingerMS                  | `linger_ms`                  | `linger.ms`                   | --linger-ms              |
| ChannelBufferSize         | `channel_buffer_size`        | `channel.buffer.size`         | --channel-buffer         |
| DiskWriteTimeoutMS        | `disk_write_timeout_ms`      | `disk.write.timeout.ms`       | --disk-write-timeout     |
| PartitionChannelBufSize   | `partition_channel_buffer_size` | `partition.channel.buffer.size` | --partition-ch-buffer |
| ConsumerChannelBufSize    | `consumer_channel_buffer_size` | `consumer.channel.buffer.size` | --consumer-ch-buffer   |
| SegmentSize              | `log_segment_bytes`          | `log.segment.bytes`            | --segment-size         |
| SegmentRollTimeMS        | `log_segment_roll_ms`        | `log.segment.roll.ms`          | --segment-roll-time-ms |
| IndexSize                | `log_index_size_bytes`       | `log.index.size.bytes`         | --index-size           |
| CleanupPolicy            | `log_cleanup_policy`         | `log.cleanup.policy`           | --cleanup-policy       |
| RetentionHours           | `log_retention_hours`        | `log.retention.hours`          | --retention-hours      |
| RetentionBytes           | `log_retention_bytes`        | `log.retention.bytes`          | --retention-bytes      |

# Special Configuration Handling

## TLS Certificate Loading

When TLS is enabled, certificates are loaded during configuration initialization:

```
if cfg.UseTLS && cfg.TLSCertPath != "" && cfg.TLSKeyPath != "" {
    cert, err := tls.LoadX509KeyPair(cfg.TLSCertPath, cfg.TLSKeyPath)
    if err != nil {
        return nil, err
    }
    cfg.TLSCert = cert
}
```

The loaded certificate is stored in the TLSCert field of the Config struct and used by the server when establishing TLS connections.

## Bootstrap Servers Parsing

The bootstrap_servers field supports comma-separated values in a single string, which are automatically split:

```
if len(cfg.BootstrapServers) == 1 && strings.Contains(cfg.BootstrapServers[0], ",") {
    cfg.BootstrapServers = strings.Split(cfg.BootstrapServers[0], ",")
}
```

This allows configuration like:

```
bootstrap_servers: "broker1:9000,broker2:9000,broker3:9000"
```

## SDK Client Configuration

### Consumer TLS

The Go SDK consumer now supports TLS connections, matching the producer's TLS capabilities. Add the following fields to `ConsumerConfig`:

| Parameter      | Type   | Default | Description                          |
|---------------|--------|---------|--------------------------------------|
| `use_tls`      | bool   | false   | Enable TLS for consumer connections  |
| `tls_cert_path`| string | ""      | Path to TLS certificate file         |
| `tls_key_path` | string | ""      | Path to TLS private key file         |

```yaml
consumer:
  broker_addrs: ["broker1:9000"]
  topic: "orders"
  group_id: "my-group"
  use_tls: true
  tls_cert_path: "certs/client.crt"
  tls_key_path: "certs/client.key"
```

When `use_tls` is enabled, every SDK client uses the shared transport dialer and performs a context-bounded TLS handshake with TLS 1.2 minimum before Wire v2 negotiation.

### SDK Metrics (Prometheus)

Both `PublisherConfig` and `ConsumerConfig` support an `enable_metrics` field to opt in to Prometheus runtime metrics.

| Parameter       | Type | Default | Description                              |
|----------------|------|---------|------------------------------------------|
| `enable_metrics`| bool | false   | Enable Prometheus runtime metric collection |
| `auto_offset_reset` | string | `earliest` | Missing/out-of-range offset policy: `earliest`, `latest`, or `error` |
| `read_isolation` | string | `read_committed` | Consumer visibility: `read_committed` or `read_uncommitted` |

When enabled, the SDK registers the following metrics in a dedicated Prometheus registry:

**Producer Metrics:**

| Metric                                    | Type      | Labels  | Description                        |
|------------------------------------------|-----------|---------|-------------------------------------|
| `cursus_producer_messages_sent_total`     | Counter   | topic   | Messages successfully sent          |
| `cursus_producer_send_errors_total`       | Counter   | topic   | Send errors                         |
| `cursus_producer_batch_latency_seconds`   | Histogram | topic   | Batch send latency                  |

**Consumer Metrics:**

| Metric                                     | Type      | Labels       | Description                     |
|-------------------------------------------|-----------|--------------|----------------------------------|
| `cursus_consumer_messages_received_total`  | Counter   | topic, group | Messages received                |
| `cursus_consumer_commit_total`             | Counter   | topic, group | Offset commits                   |
| `cursus_consumer_commit_errors_total`      | Counter   | topic, group | Commit errors                    |
| `cursus_consumer_poll_latency_seconds`     | Histogram | topic, group | Poll operation latency           |
| `cursus_consumer_rebalance_total`          | Counter   | topic, group | Rebalance events                 |
| `cursus_consumer_offset_gap_total`         | Counter   | topic, group | Offsets skipped by the configured reset policy |
| `cursus_consumer_compacted_offsets_skipped_total` | Counter | topic, group | Logical offset holes skipped because the topic metadata declares compaction |
| `cursus_consumer_stale_workers_total`      | Counter   | topic, group, worker | Assignment workers fenced by a newer generation |

To expose metrics via HTTP:

```go
import "github.com/cursus-io/cursus/sdk"

http.Handle("/metrics", sdk.MetricsHandler())
log.Fatal(http.ListenAndServe(":2112", nil))
```

## Configuration Validation

`Config.Normalize()` applies safe fallbacks for invalid or non-positive values, including write batching, sync intervals, segment/index sizes, retention intervals, channel capacities, replica settings, and transaction/producer retention. TLS certificate loading still fails startup when configured files are invalid.

Cleanup policy values normalize to `delete`, `compact`, or canonical `delete,compact`; unknown values fall back to `delete` with a warning. Distributed application topics accept compact policies only after every active broker advertises lifecycle protocol version 2, and cleaner passes wait for full ISR plus authoritative, matching HWM/lifecycle/policy state. Event-sourcing topics always require `delete`. Operators should treat normalization and policy errors as configuration/provisioning failures and verify the effective topic policy with `METADATA`.

Missing values fall back to defaults in `pkg/config/properties.go`. The effective order is defaults, parsed flags, configuration file, supported environment overrides, then normalization and validation; `--raft-peers` is the documented post-file flag exception.
