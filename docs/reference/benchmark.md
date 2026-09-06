# Benchmarks

Cursus ships Docker end-to-end correctness benchmarks and Go storage microbenchmarks. Published throughput numbers are meaningful only with the workload, hardware, durability, topology, and correctness counters attached.

## Docker Workloads

| Topology | Compose file | Records | Partitions | Producer/consumer |
|---|---|---:|---:|---|
| Standalone | `test/docker-compose.yml` | 100000 | 12 | one idempotent batched publisher, one polling group consumer |
| Cluster | `test/cluster/docker-compose.yml` | 100000 | 12 | one multi-broker publisher, one coordinator/leader-routed polling group consumer |

The source settings are `test/publisher/config.yaml`, `test/consumer/config.yaml`, and the matching files under `test/cluster`. Benchmark storage uses tmpfs in the checked-in compose workloads, so results emphasize broker/client behavior and do not represent durable physical-disk latency.

## Running

```bash
RUN_E2E_BENCHMARK=1 go test -v -timeout 30m ./test/e2e-benchmark/...
```

PowerShell:

```powershell
$env:RUN_E2E_BENCHMARK = "1"
go test -v -timeout 30m ./test/e2e-benchmark/...
```

`make bench` runs the standalone compose workload. See [Benchmark Verification](../benchmark-verification.md) for direct compose commands and cleanup.

## Correctness Gate

The harness fails on non-zero container exit, timeout, incomplete/verification markers, anchored panic/fatal logs, failed publishes, missing records, duplicate message IDs, or duplicate logical offsets. Multi-digit counters are parsed numerically. A success banner without zero correctness counters is not sufficient.

Expected consumer summary fields include:

```text
Total Messages
Elapsed Time
Overall TPS
Duplicate (MessageID) : 0
Duplicate (Offset)    : 0
Message missing       : 0
```

The test output filters successful logs to benchmark summaries. Full compose service logs are collected when a process times out or verification fails.

## What It Measures

- idempotent producer throughput and retries,
- partition distribution,
- real group join/sync/heartbeat behavior,
- broker-owned offset resume and batch commit,
- polling consumer throughput,
- duplicate/missing correctness at 100000 records,
- standalone versus three-broker routing overhead.

## What It Does Not Prove

- physical disk fsync latency (the compose logs use tmpfs),
- long-duration retention or segment aging,
- streaming-mode throughput,
- event-sourcing 100000-record throughput,
- transaction commit throughput or every crash window,
- cross-zone network behavior,
- production TLS/mTLS certificate overhead.

Those require purpose-built workloads and failure tests. Do not infer them from this benchmark.

## CI Policy

Normal E2E and benchmark verdict unit tests run on pull requests. The Docker benchmark runs automatically on pushes to `main`, after normal E2E succeeds, in the final step of `.github/workflows/e2e-tests.yml`. It can also run on a selected branch by manually dispatching **E2E Tests** with `run_benchmarks=true`. It builds its own standalone/cluster compose images; Docker layer caching may help, but the active E2E containers are not reused.

## Storage Microbenchmarks

```bash
make bench-disk
make bench-serialize
```

Equivalent commands:

```bash
go test ./pkg/disk/ -bench=. -benchmem -count=3 -timeout=120s
go test ./pkg/disk/ -bench=BenchmarkSerializeDiskMessage -benchmem -count=5 -timeout=60s
```

Microbenchmarks isolate serialization and disk-handler operations from the network and coordinators. Record Go version, OS/filesystem, CPU, disk/tmpfs, benchmark count, and configuration when comparing results.

## Reporting Results

A useful report includes:

- commit SHA and dirty status,
- standalone or cluster topology,
- record size/count and partition count,
- acks/idempotence/isolation settings,
- storage medium and mount options,
- CPU/memory/container limits,
- producer throughput and p95/p99 latency,
- consumer TPS and elapsed time,
- all retry/failure/missing/duplicate counters,
- broker errors, leader changes, and commit redirects.

Do not commit generated benchmark result files unless they are intentionally curated evidence for a release or design document.
