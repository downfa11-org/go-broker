# Getting Started

This page provides a quick start guide to get go-broker running on your system. 

It covers the basic prerequisites, installation methods, and initial verification steps to ensure the broker is operational. For detailed configuration options, see [Configuration](../user-guide/configuration.md).

## Prerequisites

Before running go-broker, ensure you have the following installed:

| Requirement | Version | Purpose |
|------------|---------|---------|
| Go  | 1.21+ | Required for building from source |
| Docker  | 20.10+ | Required for containerized deployment |
| Make  | 3.81+ | For using Makefile commands (Optional) |

# Quick Start with Docker

The fastest way to get go-broker running is using Docker Compose:

## Clone the repository

```
git clone https://github.com/downfa11-org/go-broker
cd go-broker
```

## Start the broker

This starts a container with:

```
docker-compose -f manifests/docker-compose.yml up -d
```

- Main broker on port 9000 (TCP)
- Health check endpoint on port 9080 (HTTP)
- Prometheus metrics on port 9100 (HTTP)

The container uses the configuration at 
`manifests/config.yaml` and mounts a volume for persistent storage at `./logs`.


# Quick Start from Source

To build and run go-broker from source:

## Build all binaries

```
make build
```

## Run the broker

```
./bin/go-broker

// Or run in development mode:
// make run
```
The make build target invokes three sub-targets:

- `build-api` → builds `./bin/go-broker` 
- `build-cli` → builds `./bin/go-broker-cli`
- `build-bench` → builds `./bin/go-broker-bench` 

All builds use `CGO_ENABLED=0` and target Linux with static linking via `-ldflags="-s -w`".


# Verifying the Installation

After starting go-broker, verify it's running correctly:

1. **Health Check**

   ```
   curl http://localhost:9080/health
   ```
   Expected response: OK with HTTP 200 status.

2. **Metrics Endpoint**

    ```
    curl http://localhost:9100/metrics
    ```
    Expected response: Prometheus-formatted metrics including:
    - `broker_messages_published_total`
    - `broker_messages_consumed_total`
    - `broker_active_consumers`

3. **TCP Connection Test**

    Connect to the broker using `netcat`:

    ```
    nc localhost 9000
    ```

    Then send a command like `LIST` to verify command processing.


# Running Modes

Go-broker can run in two modes:

## Broker Mode (Server)

This is the standard mode that starts the TCP server:

```
./bin/go-broker

// Or using make:
// make run
```

The broker process:

1. Loads configuration 
2. Initializes DiskManager for persistence
3. Initializes TopicManager for routing
4. Starts TCP listener on port specified in `cfg.BrokerPort`
5. Starts health check HTTP server on port 9080
6. If enabled, starts Prometheus exporter on `cfg.ExporterPort`


## CLI Mode (Interactive)

This mode provides an interactive command-line interface:

```
./bin/go-broker-cli

// Or using make:
// make cli
```

The CLI process:

1. Loads the same configuration
2. Initializes DiskManager and TopicManager
3. Creates a CommandHandler for processing commands
4. Enters interactive mode accepting `stdin` commands

Supports commands: `CREATE`, `DELETE`, `LIST`, `SUBSCRIBE`, `PUBLISH`, `CONSUME`, `EXIT` 

# Common Makefile Commands

The repository includes a comprehensive Makefile for development tasks:

## Command	Purpose
| Command | Description |
|---------|-------------|
| `make build` | Build all binaries (broker, CLI, benchmark) |
| `make run` | Run broker in development mode |
| `make cli` | Run CLI in development mode |
| `make test` | Run unit tests with race detection |
| `make bench` | Run performance benchmarks |
| `make lint` | Run `golangci-lint` |
| `make docker` | Build Docker image |
| `make compose-up` | Start docker-compose stack |
| `make compose-down` | Stop docker-compose stack |
| `make clean` | Remove build artifacts and test data |
| `make coverage` | Generate test coverage report |


# Next Steps

Now that you have go-broker running, you can:

- Configure the broker - Learn about [configuration options in Configuration](./configuration.md)
- Create topics and publish messages - See [Running the Broker for basic operations](../core/server.md)
- Understand the architecture - Review [Architecture Overview for system design](../architecture.md)
- Monitor the broker - Learn about [metrics in Monitoring and Observability](../reference/observability.md)
- Tune performance - See [Performance Tuning for optimization parameters](../reference/performance.md)

For production deployments, refer to Deployment for [best practices](../reference/performance.md) and [operational considerations](../reference/observability.md)

