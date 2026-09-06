# Benchmark Verification

The Docker benchmark verifies 100,000-record publish and polling-consume correctness in standalone and three-broker cluster topologies. It is a load/correctness gate for pub/sub; streaming and event-sourcing behavior are covered by separate E2E tests, not this 100,000-record workload.

## Prerequisites

- Docker Engine with `docker compose` (or legacy `docker-compose`) available.
- Enough CPU/memory to build and run the publisher, consumer, and up to three brokers.
- No unrelated containers using the fixed benchmark container names.

## Recommended Local Run

```bash
RUN_E2E_BENCHMARK=1 go test -v -timeout 30m ./test/e2e-benchmark/...
```

On PowerShell:

```powershell
$env:RUN_E2E_BENCHMARK = "1"
go test -v -timeout 30m ./test/e2e-benchmark/...
```

The test builds each compose stack, starts it with `--no-build --force-recreate`, waits for publisher and consumer exit, verifies summaries, and tears down volumes/containers. Fixed-name cleanup is limited to containers whose Compose labels point to this repository's test stacks.

Run one topology:

```bash
RUN_E2E_BENCHMARK=1 go test -v -timeout 15m -run TestStandaloneBenchmark ./test/e2e-benchmark/...
RUN_E2E_BENCHMARK=1 go test -v -timeout 15m -run TestClusterBenchmark ./test/e2e-benchmark/...
```

Without `RUN_E2E_BENCHMARK=1`, Docker benchmark tests skip. `go test -short` also skips them.

## Success Contract

A run passes only when:

- publisher and consumer containers exit with code 0,
- Docker reports no OOM kill or engine error, and complete container state and logs are available,
- publisher reports `Failed messages: 0` and a 100% rate,
- consumer reports `All messages consumed`,
- `Message missing`, `Duplicate (MessageID)`, and `Duplicate (Offset)` are all zero,
- logs contain no anchored panic/fatal, `benchmark incomplete`, or `verify failed` marker,
- compose startup and teardown complete within their timeouts.

The Go test prints the producer/consumer benchmark summary at INFO/test-log level and retains detailed compose output for failures. Benign words containing “fatal” do not match the anchored fatal patterns.

## Direct Compose Inspection

To leave the standalone containers visible while inspecting logs:

```bash
docker compose -f test/docker-compose.yml up --build --force-recreate
```

For the cluster:

```bash
docker compose -f test/cluster/docker-compose.yml up --build --force-recreate
```

Relevant containers are `bench-publisher`/`bench-consumer` and `broker-publisher`/`broker-consumer`. Tear down with the same compose file and `down -v --remove-orphans`.

## Make Target

`make bench` runs the standalone compose workload, checks container exit codes and correctness/fatal markers, prints compose logs, and always performs teardown. The Go `test/e2e-benchmark` suite is the authoritative way to run both topologies.

## CI

`.github/workflows/e2e-tests.yml` runs normal E2E tests on pull requests and pushes. The 100,000-record Docker benchmark is a final step only for a push to `main`, after `make e2e` succeeds, with `RUN_E2E_BENCHMARK=1`.

Benchmark verdict unit tests also run on pull requests with the race detector. To run both fixed workloads on a selected branch before merging, manually dispatch **E2E Tests** with `run_benchmarks=true`. This is still the 100,000-record tmpfs correctness workload, not a target-rate, physical-disk, long-duration production acceptance test.

The benchmark uses its own compose files and therefore performs its own image builds; it does not directly reuse the already running E2E compose stack. Docker layer cache may reduce repeated work. PR CI intentionally avoids this cold-build cost.

## Troubleshooting

- Commit errors or duplicates in cluster mode: inspect group coordinator redirects, generation/member state, and `BATCH_COMMIT` responses.
- Consumer timeout: compare published unique count with per-partition totals and committed offsets.
- Broker not ready: inspect `/ready`, Raft leader/ISR state, and fixed container-name collisions.
- Slow cold build: prebuild once locally; do not weaken correctness counters or fatal checks.
- Need deeper broker traces: temporarily use `log_level: debug` in test configs, then restore INFO for normal benchmark output.
