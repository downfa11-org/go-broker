# Architecture Overview

## Purpose and Scope

This document provides a high-level introduction to go-broker, a lightweight message broker system. 

It covers the system's purpose, core components, and architectural design. For detailed information about specific subsystems, see [Architecture Overview](./contributing/README.md) and [Core Systems](./core/README.md). 

For setup instructions, see [Getting Started](./user-guide/README.md).

## What is Go-Broker?

Go-broker is a lightweight message broker inspired by Kafka's design philosophy of **logically separated but physically distributed data management**. 

It provides publish-subscribe messaging with topic partitioning, consumer groups, and durable disk persistence, designed for single-node deployments with minimal operational complexity.

## Key characteristics:

- **Topic-based messaging**: Messages are organized into named topics with configurable partitions
- **Durable persistence**: All messages are persisted to disk using segment-based log files
- **Consumer groups**: Multiple consumer groups can independently consume the same topic
- **Deduplication**: Built-in 30-minute message deduplication window
- **Observable**: Prometheus metrics, health checks, and structured logging


## Network Interfaces

Go-broker exposes three network ports, each serving a distinct purpose:

| Port | Protocol | Handler | Purpose |
|------|----------|---------|---------|
| 9000 | TCP      | `server.RunServer()` | Main broker operations (`PUBLISH`, `CONSUME`, `CREATE`, etc.) |
| 9080 | HTTP     | `startHealthCheckServer()` | Health check endpoint for load balancers |
| 9100 | HTTP     | `metrics.StartMetricsServer()` | Prometheus metrics exporter |


## Core Data Flow

### Key flow characteristics:

- **Deduplication**: `TopicManager.Publish()` checks dedupMap using message ID (hash of payload) to prevent duplicate processing within 30 minutes 
- **Partition Selection**: `Topic.Publish()` uses key-based hashing for ordered delivery or round-robin counter for load balancing
- **Dual-path delivery**: `Partition.Enqueue()` sends to both disk (via DiskHandler) and consumer channels
- **Asynchronous writes**: DiskHandler batches up to 500 messages or flushes after 100ms linger timeout for efficiency
- **Consumer isolation**: Each ConsumerGroup receives messages independently through dedicated channels

## Message Persistence

Messages are persisted using a segment-based append-only log architecture

Each topic-partition pair gets its own DiskHandler instance:

- Writes asynchronously via `flushLoop()` goroutine
- Batches up to 500 messages or flushes after 100ms linger
- Rotates segments at 1MB boundaries
- Uses `mmap`(memory-mapped I/O) for reads
- Stores messages with 4-byte big-endian length prefixes

This architecture enables parallel I/O across partitions and efficient sequential reads. For detailed persistence mechanics, see [Disk Persistence System](./core/storage/disk-persistence.md).

