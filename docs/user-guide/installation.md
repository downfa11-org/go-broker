# Installation

## Requirements

- Go 1.25.0 or newer for source builds (`go.mod` is authoritative).
- Docker Engine with Compose for containerized E2E and benchmark workloads.
- GNU Make and Bash for Makefile convenience targets on Unix-like systems.

## Build From Source

```bash
git clone https://github.com/cursus-io/cursus.git
cd cursus
go mod download
make build
```

`make build` creates two binaries for the current OS:

```text
bin/cursus
bin/cursus-cli
```

Build individually:

```bash
make build-api
make build-cli
```

Cross-compile both for Linux:

```bash
make build-linux
```

Direct broker build:

```bash
CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/cursus ./cmd/broker
```

There is no standalone `cmd/bench` binary. End-to-end benchmarks use Docker Compose and `test/e2e-benchmark`; storage microbenchmarks use `go test -bench`. See [Benchmark Verification](../benchmark-verification.md).

## Run From Source

```bash
./bin/cursus
```

or:

```bash
make run
```

Without `-config`/`CONFIG_PATH`, built-in defaults are used. A missing explicitly named file logs a warning and falls back to effective flag defaults; malformed readable configuration fails startup.

## Container Image

Pull the published GHCR image:

```bash
docker pull ghcr.io/cursus-io/cursus:latest
docker run --rm \
  -p 9000:9000 -p 9080:9080 -p 9100:9100 \
  -v cursus-data:/root/broker-logs \
  ghcr.io/cursus-io/cursus:latest
```

Use a version tag for repeatable deployment:

```bash
docker pull ghcr.io/cursus-io/cursus:<version>
```

Build locally:

```bash
docker build -t cursus:local .
```

The multi-stage Dockerfile builds `/app/broker`, `/app/cli`, `/app/cursusctl` and `/app/cursus-storage`, then runs the broker as UID 1000. `entrypoint.sh` executes `/app/broker` with the supplied arguments. A missing explicitly configured file is an error. Production deployments should mount a configuration and durable log volume explicitly.

Example configuration mount:

```bash
docker run --rm \
  -p 9000:9000 -p 9080:9080 -p 9100:9100 \
  -e CONFIG_PATH=/app/config.yaml \
  -e LOG_DIR=/data/logs \
  -v "$PWD/config.yaml:/app/config.yaml:ro" \
  -v cursus-data:/data/logs \
  ghcr.io/cursus-io/cursus:latest
```

## Helm

A Helm chart is available under `manifests/helm`. Review `values.yaml`, persistent volume settings, TLS/internal mTLS secrets, advertised addresses, replica/quorum values, and resource limits before installing. Do not treat chart defaults as a production security profile.

`mode=standalone` runs exactly one Deployment with `Recreate` and one PVC. `mode=cluster` requires 3 or 5 StatefulSet replicas on separate Kubernetes nodes, a PVC per broker, and internal mTLS/token Secrets. The headless Service publishes not-ready addresses so quorum bootstrap does not wait for readiness. The chart advertises Pod DNS names and supports in-cluster clients only. StatefulSet updates use `OnDelete`: update the image/config, restart one broker, verify full ISR and zero under-replicated partitions, then proceed to the next broker. A PDB protects voluntary disruption, not forced deletion, node loss or data loss.

Use a new release for a new cluster. Do not toggle an existing standalone release into cluster mode, resize the voter set by changing `replicaCount`, rename a cluster release/namespace/domain, or attach an old standalone PVC to a cluster Pod. These require an explicit data/membership migration. Preserve the broker DNS identities when restoring all cluster volumes. Bootstrap only an empty new cluster or restart its original retained state; do not bootstrap replacements from a partial backup.

Before enabling `production=true`, prepare these Secrets in the release namespace:

- Client TLS: `tls.secretName`, type `kubernetes.io/tls`, keys `tls.crt` and `tls.key`. Certificates must cover the bootstrap Service and advertised Pod DNS names for cluster clients.
- Client authentication: `authentication.secretName`, key `users.json`, a JSON array of `{ "principal": "...", "token": "...", "permissions": [...] }` objects. Assign least-privilege broker permissions; do not commit credentials in values or ConfigMaps.
- Cluster token: `cluster.internalSecretName`, key `token`, a strong shared token.
- Internal mTLS: `cluster.internalTLSSecretName`, keys `tls.crt`, `tls.key`, `ca.crt`. Certificates need client/server usage and Pod DNS SANs (for example `*.cursus-headless.brokers.svc.cluster.local`). Never use the insecure cluster transport override in production.

Enable persistence, client TLS, authentication, monitoring and NetworkPolicy for the production guard. Set `mode=cluster`, `replicaCount=3` and the two internal Secret names for clustering. Pin `image.tag` to a tested image built from the same version as this chart; existing released images do not acquire these changes by changing chart values alone. Helm references Secrets without creating or embedding their contents. Rotate Secrets and restart brokers one at a time; Secret-backed environment variables do not hot reload.

NetworkPolicy allows client Pods in the same namespace selected by `networkPolicy.clientPodSelector` (default `cursus-client=true`), monitoring Pods selected by the configured namespace/Pod labels, and same-release internal broker traffic. Label/select the real clients and Prometheus Pods before enabling it, and verify that the CNI enforces policy. Egress is not restricted by this chart. Public load-balancer access and cross-namespace clients require a separately reviewed networking/address configuration.

The default resource requests/limits are a starting point, not a capacity guarantee. Size memory for active frames, queues, transaction state and the entire in-memory event index. Event history is retained indefinitely; alert on PVC free space and validate expansion/backup procedures. The startup probe allows ten minutes by default; increase it based on measured recovery time for the largest retained active segment. Keep graceful shutdown at least 60 seconds and measure actual flush time before deployment. The chart's `shutdownTimeoutMS` defaults to 30000 and must leave at least five seconds before `terminationGracePeriodSeconds` expires. If cleanup exceeds this signal-triggered process budget, the broker exits unsuccessfully instead of claiming a successful flush; test recovery from that forced exit before deployment.

The chart defaults to 16 client requests / 64 MiB of incoming frame reservations and a separate 32 internal requests / 128 MiB pool. Tune `limits.maxInFlightRequests`, `limits.maxRequestBytes`, `limits.maxInternalInFlightRequests` and `limits.maxInternalRequestBytes` with measured workloads; these are not total process-memory limits. See [request admission](configuration.md#request-admission) for accounting, rejection behavior and metrics.

`monitoring.enabled=true` exposes metrics and, by default, a ServiceMonitor. Without Prometheus Operator CRDs set `monitoring.serviceMonitor.enabled=false` and configure scraping yourself. `monitoring.rules.enabled=true` additionally creates readiness, scrape-failure and under-replication PrometheusRules; ensure your Prometheus selectors discover them and test alert delivery. PVC-capacity and absent-target discovery alerts remain environment-specific responsibilities.

For a recoverable backup, pause writers, confirm acknowledged operations and offsets, stop brokers gracefully, and snapshot the complete persistence unit. Standalone requires its entire log directory; cluster recovery requires all broker PVCs and their original identities, including Raft state, logs, manifests, HWM/producer checkpoints, transaction decisions and event snapshots. Keep backups immutable and restore into an isolated environment before trusting them. File-by-file live copies are not a consistent backup. No backup deletion or production restore is automated by this chart.

The `full-persistence-backup-restore` CI job stops standalone and three-node cluster test brokers, requires exit status 0, archives each complete log directory, and restores into freshly created containers with the original identities. It compares every archived file's SHA-256, mode and UID/GID before startup, then checks message order, idempotent retries, committed/open transaction recovery, consumer offsets, event snapshots and continued writes. Archive streaming and ownership preservation use [Docker's documented copy semantics](https://docs.docker.com/reference/cli/docker/container/cp/). This tests a cold full-directory backup, not live snapshots or Kubernetes CSI/provider behavior.

Run on a dedicated Docker test host with no existing `broker` or `broker-1` through `broker-3` containers, `test_network`/`cluster_network` networks, or `cursus-backup-restore` Compose project:

```bash
RUN_E2E_BACKUP_RESTORE=1 go test -v -count=1 -timeout 25m ./test/e2e-cluster -run '^TestFullPersistenceBackupRestore$'
```

The test refuses to replace pre-existing containers, recreates only its isolated Compose project, and removes its temporary archives on completion. Export and protect production backups separately; never point this test at production storage.

Validate the rendered resources with `go test ./test/helm` and run the deployment CI before promotion. Its `helm-runtime` job creates a disposable four-node [kind cluster](https://kind.sigs.k8s.io/docs/user/quick-start/), installs standalone and three-broker Helm releases with TLS/authentication and PVCs, and uses only the public Go SDK to publish/read records. It replaces every broker Pod, verifies retained PVC identities, then checks all original records plus new writes. Cluster Pods must run on three distinct workers. The fixture has its own kubeconfig and refuses to reuse a cluster; it runs only in GitHub Actions and deletes only its own kind cluster.

This fixture uses ephemeral test certificates and kind's local storage/default networking. It does not prove NetworkPolicy enforcement, Prometheus alert delivery, CSI backup behavior or production capacity. The manual Deployment Validation workflow can also repeat recovery drills 1–5 times. These drills recreate test clusters and are not a continuous production-load soak. Certificate/CNI behavior, full-volume restore, sustained workload and latency/memory budgets must still be verified on the deployment's real storage/network before declaring it production-ready.

## Verify

```bash
curl -f http://localhost:9080/live
curl -f http://localhost:9080/ready
curl -f http://localhost:9100/metrics
```

The client port accepts only Wire v2, so raw `nc` text without the required handshake and `CRS2` frame is not a valid protocol check. Use `bin/cursus-cli`, a supported SDK, or the E2E client helpers.

Run local validation:

```bash
go test ./...
make e2e
```

Docker benchmark tests are opt-in and main-push-only in CI:

```bash
RUN_E2E_BENCHMARK=1 go test -v -timeout 30m ./test/e2e-benchmark/...
```

## Clean

```bash
make clean
```

The current target removes files under `bin/` and local coverage outputs. It does not delete arbitrary broker log directories or Docker volumes; remove those intentionally with the matching Compose/volume command.

## Next Steps

- [Configuration](configuration.md)
- [Getting Started](README.md)
- [Architecture](../architecture.md)
- [Security And Observability](../reference/observability.md)
