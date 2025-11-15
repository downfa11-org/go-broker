# Installation

This page covers the installation of go-broker using different methods: building from source or using Docker. 

For configuration options after installation, see [Configuration](./configuration.md). For instructions on running the broker once installed, see [Running the Broker](../core/server.md).

## Prerequisites

### Go Version Requirement

go-broker requires `Go 1.25.0` or higher. This is specified in `go.mod`

### Dependencies

The project has minimal external dependencies:

| Dependency                                 | Purpose                                  |
|--------------------------------------------|-----------------------------------------|
| `github.com/prometheus/client_golang v1.23.2` | Metrics exporter                        |
| `gopkg.in/yaml.v3 v3.0.1`                  | Configuration file parsing               |
| `golang.org/x/exp`                         | Extended Go libraries                    |
| `golang.org/x/sys`                         | System-level calls (Linux optimizations)|

All dependencies are managed via Go modules and will be automatically downloaded during the build process.

# Installation Methods

## Method 1: Building from Source

The Makefile provides convenient build targets for all components. 

The primary command builds all binaries:

```
make build
```

This executes three sub-targets defined in Makefile

- `make build-api` - Builds the broker server
- `make build-cli` - Builds the CLI tool
- `make build-bench` - Builds the benchmarking tool

### Individual Build Commands:

| Target           | Output                  | Command              |
|-----------------|------------------------|--------------------|
| API Server       | `bin/go-broker`        | `make build-api`    |
| CLI Tool         | `bin/go-broker-cli`    | `make build-cli`    |
| Benchmark Tool   | `bin/go-broker-bench`  | `make build-bench`  |

### Build Flags


All builds use the following flags, as specified in Makefile

```
CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w"
```

### Flag Explanation:

| Flag                | Purpose                                                      |
|--------------------|--------------------------------------------------------------|
| `CGO_ENABLED=0`     | Produces static binaries without C dependencies             |
| `GOOS=linux`        | Targets Linux platform                                       |
| `-ldflags="-s -w"`  | Strips debug information and symbol tables to reduce binary size |

### Direct Go Build

Alternatively, build directly using go build:

- Build broker
    ```
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o bin/go-broker ./cmd/broker/main.go
    ```
- Build CLI
    ```
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o bin/go-broker-cli ./cmd/cli/main.go
    ```
- Build benchmark
    ```
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o bin/go-broker-bench ./cmd/bench/main.go
    ```
### Development Build

For local development with faster builds (without optimization flags):

```
make run    # Build and run broker
make cli    # Build and run CLI
```

These commands use go run which compiles and executes in one step.

## Method 2: Docker Installation (Multi-Stage Build)

The build process is defined in Dockerfile:

### Stage 1: Builder 

- Base: `golang:1.25.0`
- Downloads dependencies via `go mod download`
- Builds broker and cli binaries with `CGO_ENABLED=0`
- Uses `-ldflags="-s -w"` for size optimization

### Stage 2: Runtime

- Base: alpine:3.18 (minimal Linux distribution)
- Copies only compiled binaries from builder stage
- Installs bash and curl for operational needs
- Sets executable permissions on binaries
- Configures entrypoint.sh as the entry point

Building the Docker Image:

```
make docker
docker build -t go-broker:latest .
```

Alternatively, build directly with docker:

```
docker build -t go-broker:latest .
```

### Container Contents

The resulting Docker image contains:

| Path                 | Description                          |
|---------------------|--------------------------------------|
| `/root/broker`       | Main broker executable               |
| `/root/cli`          | CLI tool executable                  |
| `/root/entrypoint.sh`| Container startup script             |
| `/root/logs/`        | Default log directory (created at runtime) |


### Image Size Optimization

The multi-stage build significantly reduces image size:

- **Builder stage**: ~1GB (includes Go toolchain)
- **Final image**: ~50MB (only runtime dependencies + binaries)

This is achieved by copying only the compiled binaries from the builder stage to a minimal Alpine base.

## Binary Artifacts

### Output Directory Structure

After building from source, the `bin/` directory contains:

```
bin/
├── go-broker          # Main broker server
├── go-broker-cli      # Administrative CLI tool
└── go-broker-bench    # Performance benchmarking tool
```

## Binary Descriptions

| Binary            | Entry Point      | Purpose                                                                                     |
|-------------------|------------------|---------------------------------------------------------------------------------------------|
| **go-broker**     | `go-broker`      | Starts the broker server with TCP listener, health check endpoint, and metrics exporter     |
| **go-broker-cli** | `go-broker-cli`  | Interactive CLI for administrative commands (`CREATE`, `DELETE`, `LIST`, `SUBSCRIBE`, `PUBLISH`, `CONSUME`) |
| **go-broker-bench** | `go-broker-bench` | Runs performance benchmarks and load testing                                                |


# Broker Binary Initialization

The broker binary performs the following initialization sequence.

## Initialization Order:

1. Load configuration from file or environment variables
2. Display startup banner with port and feature flags
3. Create DiskManager for persistence layer
4. Create TopicManager with reference to DiskManager
5. Start server with all components
6. CLI Binary Initialization

The CLI binary initializes similarly but for interactive command processing, as shown in `cmd/cli/main.go`

1. Loads configuration via `config.LoadConfig()`
2. Creates DiskManager and TopicManager instances
3. Creates a ClientContext with group "cli-group"
4. Initializes CommandHandler for processing user input
5. Enters interactive loop reading from stdin

# Verification

## Check Binary Existence

After building, verify the binaries exist:

```
ls -lh bin/
```

Expected output:

```
-rwxr-xr-x  1 user  group   15M  go-broker
-rwxr-xr-x  1 user  group   12M  go-broker-cli
-rwxr-xr-x  1 user  group   13M  go-broker-bench
```

## Check Binary Execution


Test that binaries are executable:

```
./bin/go-broker --help
./bin/go-broker-cli --help
```

## Docker Image Verification

For Docker installations, verify the image was built:

```
docker images | grep go-broker
```

Expected output:

```
go-broker    latest    <image-id>    <timestamp>    ~50MB
```

Run a quick test container:

```
docker run --rm go-broker --help
```

## Cleaning Build Artifacts

To remove all built binaries and test data:

```
make clean
```

This removes:
- All binaries in `bin/`
- Test topic directories in `pkg/topic/`
- Test connection data in `pkg/server/`

As defined in Makefile,the clean target executes:

```
rm -rf bin/* pkg/topic/test-topic/* pkg/topic/topic1/* pkg/server/testconn/*
```

# Next Steps

After installation, proceed to:

- [Configuration](./configuration.md) - Configure broker settings, ports, and tuning parameters
- [Running the Broker](../core/server.md) - Start the broker and verify it's operational