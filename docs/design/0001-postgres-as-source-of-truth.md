# 0001. PostgreSQL is the source of truth

Status: accepted.

## Context

A job queue has to keep a job until a worker has finished it, whatever fails in between: a worker, a broker, the network. It has to give each job to one worker at a time, retry what fails, delay what is scheduled, and let an operator look at what is in it and why. It also has to be a system that a team can run without learning a new storage engine.

Two things shape the choice. The queue's correctness rests on a few conditional writes ("lease this job if it is ready", "complete it if I still hold it"), which want transactions on one row at a time, and on a clock that every component agrees on ([ADR 0004](0004-time-authority.md)). And the brokers should be stateless, so that a rolling restart, a crash or a scale-out never has to move state ([ADR 0006](0006-stateless-brokers-and-load-balancing.md)).

## Decision

All job state lives in one PostgreSQL table, `jobqueue.jobs`, in PostgreSQL 15 to 18. Brokers hold no job state.

- **One table, partial indexes.** A job is a row; its state is a column. Each hot path has a partial index (ready jobs by tenant and `run_at`, leased jobs by expiry, finished jobs by `finished_at`), so that lease, reap and prune read only what they need. The statements are written so that the planner uses them, and a test pins the rows each one reads.
- **Guarded single statements.** An acknowledgement is one `UPDATE` whose `WHERE` clause holds the guard (state, lease epoch, queue). A lease is one statement that picks rows with `FOR UPDATE SKIP LOCKED` and turns them into leases. Concurrent brokers never see the same row twice, and the common path costs one round trip. An acknowledgement first locks the rows it will change in ascending id order, through an ordered lock CTE, so that calls that name each job once, in opposite orders, cannot deadlock.
- **The database clock.** Every expiry and `run_at` comparison is made in SQL against `jobqueue.now()`.
- **`LISTEN`/`NOTIFY` is a wakeup, not storage.** Enqueue, release and requeue notify in their own transaction; each broker listens on one dedicated connection. A notification only tells a broker that work may exist. Losing one costs latency (the fallback poll is 1 s), never correctness.
- **Terminal rows are kept, then pruned.** Succeeded and cancelled jobs stay for 24 hours and dead jobs for 14 days, then every broker's pruner deletes them in batches of 1000. The retention is also the idempotency window: a key is honoured while its row exists.
- **Forward-only embedded migrations.** `jobqueuectl migrate` applies them under an advisory lock, and a broker refuses to start against a schema at another version.

## Consequences

Costs, stated plainly:

- **One PostgreSQL is the throughput ceiling.** Every enqueue, lease, heartbeat and acknowledgement is a write to the same table. The ways out are bigger hardware, batching (the SDK coalesces acknowledgements into one call of up to 128, and leases come in batches of up to 64) and, beyond that, sharding queues over several PostgreSQL clusters. Sharding is not implemented, and a queue cannot span clusters.
- **The table churns.** Each job leaves at least two dead row versions (one when it is leased, one when it finishes) plus one per heartbeat, and finished rows stay for hours or days. That is bloat by construction. The table has `fillfactor = 85` and autovacuum settings tighter than the defaults and, since migration 0002, no autovacuum cost delay, with the reasons written in the two migrations; whether the table size plateaus under a long run is what the soak experiment in [docs/benchmarks.md](../benchmarks.md) measures, and the symptoms and remedies are in [docs/operations.md](../operations.md).
- **`NOTIFY` needs a session.** The listener holds one connection that must live for the broker's life, so it cannot go through a pooler in transaction mode, and transaction-mode pooling is not supported at all: behind one, notifications never arrive, and the pool's cached prepared statements and the migration's session advisory lock may not survive either. Use a direct connection or a pooler in session mode. Each broker also holds a pool of up to 20 connections by default, so the number of brokers times that is a number to check against `max_connections`.
- **Polling costs database load.** A broker asks the store which tenants have work about once a second for each queue it has waiters on, plus a fallback poll, so database load grows with brokers times busy queues even when no jobs arrive.
- **High availability is the database's.** The queue is as durable and as available as the PostgreSQL under it. The suite does not test failover or replica promotion, and trusts the managed service's durability and switchover semantics.
- **The queue shares a database with whatever else is there.** A long transaction elsewhere in the same database holds back vacuum for this table too. Use a database of its own.

What it buys: a design in which the only distributed state is a row, a conditional update is the whole concurrency story, an operator can answer any question with SQL, and a backup of the database is a backup of the queue.

## Alternatives rejected

- **A storage engine and replication of our own (a log, Raft).** It would remove the single-database ceiling and cost the project's entire effort in a different problem: durability, compaction and recovery are a product of their own. Production job queues (River, Oban, Graphile Worker, Solid Queue) use the relational database they run beside for the same reason. Not done.
- **Redis.** Fast, and a poor fit for a queue that must not lose jobs: durability depends on persistence settings (`appendfsync`) that trade against speed, a lease needs a script to be atomic, and it adds a second system to operate and to keep consistent with the first. Rejected for durability, and for the extra moving part.
- **A log such as Kafka.** A log gives ordered, replayable streams, not per-message leases: there is no delay, no per-job retry with backoff, no cancel, no dead-letter queue and no heartbeat. Those would have to be rebuilt on top. Rejected as the wrong abstraction.
- **A managed queue (SQS, RabbitMQ).** Excellent products, and the point of this project is a self-hostable queue whose behaviour can be inspected, tested and reasoned about end to end. Rejected for that reason, not on merit.
- **A library with no broker** (workers talk to PostgreSQL directly, as River does). Simpler, and one hop fewer. It makes every worker hold database connections, so the connection count follows the worker count, it leaves fair scheduling across tenants without a place to live, and it ties clients to a database driver. A broker puts a narrow, versioned gRPC API in front. Rejected for those costs.
- **Storing leases in memory in the brokers.** Faster, and it makes a broker's crash a loss of leases, which would need an ownership protocol between brokers. Rejected: the database already is that protocol.
