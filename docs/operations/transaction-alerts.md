# Transaction coordinator alerts

The broker exports transaction coordinator state from the same in-memory state
used to serve requests. Recommended production alerts are:

- `cursus_transaction_recovery_ready != 1` for any ready broker. The broker
  should normally fail startup before this can occur.
- `cursus_transaction_oldest_active_seconds` above the application's maximum
  transaction duration. This detects stuck open or committing transactions.
- A sustained increase in `cursus_transactions{state="committing"}`. A brief
  non-zero value is normal while a commit is being applied.
- Unexpected growth in `cursus_transactions_expired`, paired with journal size
  and filesystem alerts. The standalone journal rewrites atomically after 256
  records or 16 MiB and retains the latest state per transactional ID.

Alert thresholds are workload-specific. Compare age and counts with broker
readiness, storage errors, Raft leadership, and consumer metadata recovery
metrics before taking recovery action. Metrics and diagnostic commands are
read-only and never create groups, transactions, topics, or offsets.

## Prepared transaction recovery

Ownership and non-regressing offsets are validated before a new prepare becomes
durable. A prepared transaction records the consumer group's registration epoch.
Recovery may finish after membership or generation changes, but cannot apply old
offsets to a deleted and recreated group. Replayed offsets preserve any newer
commits. A group incarnation mismatch is an explicit recovery failure; do not
reset the transaction or group to hide it.

Standalone prepares are journaled before becoming applicable. Distributed
prepares validate ownership again at Raft apply time; offset recovery references
the exact durable transaction identity and revision, not client-supplied offsets.
Registered brokers must all advertise broker protocol 3 before new offset-bearing
transactions can prepare. Upgrade all registered brokers before enabling this
path; downgrading after using it is not supported. Drain transactions prepared by
older versions before upgrading: snapshots without a registration epoch retain
the older ownership checks and are not granted unfenced recovery authority.
