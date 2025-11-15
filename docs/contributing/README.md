# Design Philosophy

Go-broker is designed around several core principles:

1. **Simplicity over feature completeness**: Single-node architecture with minimal dependencies, avoiding distributed consensus complexity

2. **Separation of concerns**: Clear boundaries between connection handling, topic management, and disk persistence

3. **Asynchronous writes, synchronous reads**: Publishers don't block on disk I/O; consumers read directly from segments

4. **Batching and buffering**: Multiple levels of buffering balance throughput and latency

5. **Per-partition isolation**: Each partition operates independently, enabling parallelism

6. **Observable**: Built-in Prometheus metrics, health checks, and structured logging

These design choices prioritize operational **simplicity** and **performance** for single-node deployments while maintaining the core guarantees of a message broker (durability, ordering within partitions, at-least-once delivery).

Future versions plan to add distributed coordination using `etcd` for multi-node deployments.