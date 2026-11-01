# Architecture

This document explains how the queue is put together: the components, what is in the database, the life of a job, the path a job takes from a producer to a worker, and the rules that keep it correct. The reasons behind the larger choices are in the design records under [docs/design](design/0001-postgres-as-source-of-truth.md); this document says what the code does, and points to the record that says why.

## Components

```
   producers (SDK)                                     workers (SDK)
        |  Enqueue, Get, Cancel          Lease (long poll), Heartbeat, Ack, Release  |
        v                                                                            v
   +--------------------------- gRPC, round-robin over a headless Service -----------+
   |                                                                                  |
   |    broker 1            broker 2            broker 3        (stateless, any N)    |
   |    dispatcher          dispatcher          dispatcher                            |
   |    NOTIFY listener     NOTIFY listener     NOTIFY listener                       |
   |    reaper, pruner      reaper, pruner      reaper, pruner                        |
   +-----------------------------------+----------------------------------------------+
                                       |  SQL (pgx pool) and one LISTEN connection per broker
                                       v
                       PostgreSQL: table jobqueue.jobs, function jobqueue.now()

   KEDA --(external scaler protocol, own port)--> any broker --> job counts from PostgreSQL
```

- **Brokers** (`cmd/jobqueue-server`, package `internal/broker`) serve the gRPC API. A broker keeps no job state of its own: a call can go to any broker, and a broker can be killed at any time. What it does keep is soft: the long polls waiting on it, the fair-share state of the queues it has seen, and the metrics. Each broker also runs the reaper and the pruner, and listens for notifications.
- **PostgreSQL** is the source of truth and the only time authority ([ADR 0001](design/0001-postgres-as-source-of-truth.md), [ADR 0004](design/0004-time-authority.md)).
- **The SDK** (the root package `jobqueue`) is the Go client: `Dial`, `Producer` and `Worker`.
- **`jobqueuectl`** is the admin CLI: enqueue, get, cancel, stats, dead list, requeue, purge, migrate and token generation.
- **KEDA** scales worker Deployments on the backlog that a broker reports on a separate port.

### Layers

The code is layered, and the build enforces the direction of every arrow and the purity of the core:

```
cmd/jobqueue-server   cmd/jobqueuectl
        |                    |
  internal/broker (shell: gRPC, NOTIFY, timers, reaper, pruner)      jobqueue (SDK shell: goroutines, gRPC)
        |                                                                  |
  internal/store ---> memstore (reference model)                     internal/core/lease  (pure)
        |        \--> pgstore  (PostgreSQL, pgx)
        v
  internal/core/job       transitions, retry policy, validation (pure)
  internal/core/fair      the deficit round robin planner (pure)
  internal/core/dispatch  the broker-side waiter and wakeup logic (pure)
```

`internal/core/...` may import only the standard library, other core packages and `internal/flaw`, and it may not use `os`, `net`, `sync`, `time.Now` and its relatives, the global functions of `math/rand`, a `go` statement, `select` or a channel. Effects are values that the core returns and a shell carries out. Two layers enforce this: `depguard` and `forbidigo` in the linter, and `internal/archtest`, an ordinary test that reads the import graph and the syntax tree, so that `go test ./...` fails without the linter installed. [ADR 0003](design/0003-pure-cores-and-simulator.md) explains why the logic is built this way.

## The job

A job is one row of `jobqueue.jobs`:

| Column | Meaning |
| --- | --- |
| `id` | Identity, assigned by the database. Ids increase in enqueue order |
| `queue`, `tenant` | Where the job goes and whom it belongs to. Names are 1 to 128 bytes of `[A-Za-z0-9][A-Za-z0-9_.-]*` |
| `state` | `ready`, `leased`, `succeeded`, `dead` or `cancelled` |
| `payload`, `result` | Opaque bytes, at most 256 KiB and 64 KiB |
| `run_at` | The earliest time the job may be leased. A delayed job and a job that waits for a retry are both `ready` with a `run_at` in the future; there is no separate state |
| `attempts`, `max_attempts` | Leases granted and not refunded, and the budget (1 to 100, default 25) |
| `lease_epoch` | The fencing token. It grows by one on every lease and never goes back |
| `lease_expires_at`, `leased_by` | Set while the job is leased |
| `idempotency_key` | Optional, unique within its tenant and queue while the row exists |
| `last_error` | The message of the latest failure, at most 4096 bytes |
| `created_at`, `finished_at` | Times on the database clock |

The table has CHECK constraints behind the validation (a lease exists exactly while the job is leased, `finished_at` is set exactly in the terminal states, the size limits) and partial indexes for each hot path: ready jobs by `(queue, tenant, run_at, id)` and by `(queue, run_at, id)`, leased jobs by expiry, finished jobs by `finished_at`, dead jobs by queue, and a unique index for idempotency keys. `fillfactor` is 85 and autovacuum is tuned per table, with the reasons written next to the settings in `internal/store/pgstore/migrate/sql/0001_init.sql`.

### The transitions

`internal/core/job` is the single definition of what each operation does to a job. The memstore applies these functions directly, and the PostgreSQL store expresses the guards of the same table as conditions of one `UPDATE`.

| Operation | Needs | Result |
| --- | --- | --- |
| Enqueue | A new idempotency key, or none | `ready`, with `run_at` at now plus the delay. A key that exists returns the first job and `deduplicated` |
| Lease | `ready` and `run_at <= now` | `leased`, epoch + 1, attempts + 1, expiry now + TTL |
| Heartbeat | `leased`, epoch equal to the token, expiry after now | Expiry is now + TTL. Otherwise the answer is `Fenced` or, for a lease that ran out, `Expired`, and the lease is not revived |
| Complete | `leased` and epoch equal to the token. Expiry is not looked at | `succeeded`. The same token again is a `Duplicate` that keeps the first result |
| Fail | As Complete | `ready` after a delay while attempts remain and the failure is not permanent, otherwise `dead` |
| Release | As Complete | `ready` at once, and the attempt is refunded |
| Cancel | `ready` or `leased` | `cancelled`. The holder is fenced from then on and learns at its next heartbeat |
| Reap | `leased` and expiry at or before now | As Fail, retryable, with the error `lease expired` |
| Requeue | `dead` | `ready`, attempts back to 0, the last error kept |
| Prune | Terminal, and finished more than the retention ago | Deleted. Retention is 24 h for `succeeded` and `cancelled` and 336 h (14 days) for `dead` |

The boundaries are exact and tested: a job with `run_at == now` can be leased, a lease with `expires == now` can be reaped and a heartbeat at that instant is `Expired`, and a row is pruned only when `now - finished_at` is strictly greater than the retention. The idempotency window is the life of the row, so a key is honoured for at least the retention after the job finished.

## The dispatch path

A worker's request for jobs travels like this.

1. The worker's SDK sends `Lease(queue, max_jobs, wait, ttl, worker_id)` to some broker. `wait` is a long poll: 20 s by default, 30 s at most. `max_jobs` is at most 64, so that a response is bounded at 64 payloads.
2. The broker validates the request, checks the caller's role and queue, and hands it to its dispatcher. The dispatcher registers a **waiter**.
3. The dispatcher's core (`internal/core/dispatch`) decides when to look for work. A queue with waiters is fetched from when one of these happens: a `NOTIFY` arrives, a scheduled job falls due, the fallback poll interval passes (1 s), a previous fetch may have left jobs behind, or a waiter arrives at a queue that is not known to be empty. **Every such event starts at most one fetch**, for the oldest waiter. A fetch that returns a full batch chains the next waiter, and a fetch across all tenants that returns a short batch proves the queue empty and stops the chain, so the number of fetches follows the amount of work and not the number of waiters. (A short fetch for named tenants proves less; see *Fair sharing*.) At most 4 fetches per queue are in flight at once.
4. A waiter is told "nothing" only after the store has looked for it, at its deadline, so a long poll on a saturated queue can outlast its wait by the time its fetch takes. A failed fetch answers the waiters parked behind it on schedule, with `Unavailable` for the first, so that an outage does not stretch every poll.
5. To fetch, the shell asks the fair-share planner (below) how to split the batch between the tenants that have work, and calls `store.Lease`. In PostgreSQL that is one statement: for each tenant, a `LATERAL` subquery takes the oldest due jobs up to the tenant's limit with `FOR UPDATE SKIP LOCKED`, and an `UPDATE` turns them into leases. The response carries the time left on each lease as computed in the database (`remaining`), and how long the call waited at the broker (`waited`).
6. If the waiter's caller has gone away while the fetch was out (the client cancelled, or the broker is draining), the jobs fetched for it are released with the attempt refunded, rather than left to expire.

Notifications are produced by `Enqueue`, `Release` and `Requeue`, in the transaction of the write: `pg_notify('jobqueue_ready', '<queue>\t<tenant>')` for each queue and tenant that gained a job that is ready to run. A broker listens on one dedicated connection, because `LISTEN` belongs to a session. **A notification is a hint and never the source of truth.** It can be lost while the connection is down, so after every reconnect the listener asks the dispatcher to resync, and the fallback poll finds work in any case. Losing notifications costs latency, never correctness. A payload that is not a valid queue and tenant is counted and dropped, because anyone with database access can send one.

**The calls that notify hold a bounded share of the pool.** A transaction that runs `pg_notify` takes the lock that PostgreSQL uses to serialize the writers of the notification queue when it commits, and keeps it until the commit is over, the WAL flush included. Notifying transactions therefore commit one at a time. Under load they queue on that lock, each holding a pool connection, and left alone they would take all of them: Lease, heartbeats and acknowledgements, which do not notify, would wait for a connection while the database could serve them at once. A gate in `internal/store/pgstore` caps the connections that `Enqueue`, `Release` and `Requeue` hold together (`--max-notify-conns`; by default the pool size less a reserve of max(2, pool/4), which is 15 of 20). A call takes a permit before it asks the pool for a connection, so a call that waits for its turn holds nothing. The permit is taken before the statement runs and so before it is known whether it will notify: an `Enqueue` of delayed jobs only is gated too.

### Fair sharing

The dispatcher keeps, for each queue it serves, a deficit round robin planner over the tenants that currently have ready work. The ring is filled from the store's `Activity` call (the tenants with ready jobs and the next time a scheduled job falls due), refreshed every second and when a notification names a new tenant. A tenant's turn is worth 8 jobs times its weight (weights 1 to 1000, set with `--tenant-weights`, default 1). `Plan(n)` returns how many jobs to take from which tenant, `Settle` removes a tenant that delivered fewer than asked, and the result for `n` jobs in one call equals what `n` calls for one job each would give. `--scheduler fifo` replaces all of this with arrival order.

The planner also decides what a short fetch means. Under `drr` a fetch is planned as one take per tenant, and the store serves each take from its own tenant only, so a fetch that comes back short proves only that the tenants that came up short ran dry, and another tenant may still hold a backlog. Those tenants leave the ring and the next waiter's fetch is chained at once. Only a short fetch across all tenants, which is what `fifo` and an empty ring ask for, proves the queue empty. The chain ends because the planned takes add up to the batch size, so each short per-tenant fetch removes at least one tenant from the ring, and once the ring is empty the dispatcher reads the activity of the queue and fetches across tenants. This relies on `Activity` and `Lease` agreeing on what is ready now: both use `state = 'ready'` and `run_at <= jobqueue.now()`, and the store contract has a case, `ActivityListsOnlyTenantsThatLeaseCanServe`, that holds both stores to it. See [ADR 0005](design/0005-drr-fair-scheduling.md) for the bound and its limits: every broker does this alone, so fairness across brokers is approximate.

## Leases, heartbeats and acknowledgements

The worker side is a pure state machine too: `internal/core/lease`, driven by the shell in `worker_loop.go`.

- **Local deadline.** For each lease, the worker computes the instant at which it stops trusting it: the instant the Lease request was **sent**, plus the time the call waited at the broker before the fetch that produced the job began, plus the remaining time the database reported, minus the margin. The anchor is the send instant and never the arrival of the answer. The database evaluated `now()` no earlier than send + waited, so this deadline lies before the lease's expiry by at least the margin, however late the answer was. Only the worker's monotonic clock is used. When the deadline passes, the handler's context is cancelled. [ADR 0004](design/0004-time-authority.md) has the argument.
- **Heartbeats** go out every TTL/4 (7.5 s by default) for all held leases in one call, anchored the same way. A lease that the broker reports as lost is cancelled. Because heartbeats come every quarter of the TTL and the margin is less than a quarter of it, two heartbeats in a row can fail and the lease is still alive when the third goes out. A call that hangs is cut off after one period or 5 s, whichever is shorter (5 s at the defaults, where a period is 7.5 s), so a hung heartbeat counts as one failure and does not delay the next. The third has a quarter of the TTL minus the margin for its answer to come back (4.5 s by default). That is why the TTL must be strictly greater than four times the margin (defaults 30 s and 3 s); at exactly four, the third heartbeat and the deadline fall on the same instant.
- **Acknowledgements** are coalesced: the first finished job opens a 2 ms window, and the batch goes out when the window ends or when 128 are waiting (`WorkerOptions.AckWindow` and `AckBatch` change them, and an `AckBatch` of 1 sends every report by itself). One `Ack` call carries completions and failures. A failed batch is sent again with backoff, and a late report is harmless because Complete is idempotent for the same token and everything else is fenced.
- **The handler's context** ends on any of three causes, which `context.Cause` reports: the broker said the lease is lost, the local deadline passed, or a shutdown outlasted the drain timeout.
- **Outcomes.** A handler that returns `nil` completes the job. An error is a failure that is retried after a backoff; `RetryAfter(err, d)` chooses the delay and `Permanent(err)` sends the job to `dead`. A panic is recovered and reported as a retryable failure with its stack. The stored error is cut to 4096 bytes on a character boundary.
- **Shutdown.** The worker stops asking for work, gives running handlers the drain timeout (25 s), reports what finished, and releases the rest. A handler that ignores its context can still be running after `Run` returns; what it does then is fenced by its token.

A worker whose Lease call fails with `Unavailable` backs off with jitter and the gRPC round-robin picks another broker. A refusal that retrying cannot fix (`Unauthenticated`, `PermissionDenied`, `InvalidArgument`) makes the worker drain and `Run` return the error.

## Failure handling

- **A worker dies or stalls.** Its heartbeats stop, the lease expires on the database clock, and a reaper takes the job back as a failed attempt. A reaper runs on every broker every second and handles up to 1000 jobs per pass, repeating at once when the pass was full. It locks the expired rows with `FOR UPDATE SKIP LOCKED`, decides each row's fate with the same `job.Reap` function the reference store uses, and writes them back in one statement, so several brokers reaping together never take the same job. The delay between a lease's death and the job running again is at most the TTL, plus the reap interval, plus the retry backoff of the attempt that expired (up to 1 s after the first attempt, doubling with each later one up to 1 h; see *The retry delay* below), plus the dispatch latency.
- **Two calls acknowledge the same jobs.** An acknowledgement statement starts with an ordered lock: a materialized CTE selects the rows that the guard admits (job, queue, leased, epoch equal to the token), in ascending job id, with `FOR UPDATE`, before the statement looks at any of them, and the `UPDATE` then changes only those rows. Two calls that name the same jobs in opposite orders lock them in the same order and cannot deadlock, and an item with a stale token locks nothing, so it neither waits for nor blocks anyone. The order of the statement's own join is the planner's and changes with the statistics, so it cannot be relied on. A deadlock that PostgreSQL still detects rolls the statement back and is classified as retryable.
- **The retry delay** is chosen by the broker, never by the store: exponential from 1 s, doubling to a cap of 1 h, with full jitter, from a random source seeded per broker. Over the 24 retries of a job with 25 attempts, the ceilings add up to 47,295 s (about 13.1 hours), and with full jitter the expected span is about half of that.
- **A broker dies.** Calls in flight fail with `Unavailable` and the SDK repeats them on another broker. Enqueue is safe to repeat because every job carries an idempotency key, the caller's or one the SDK generated; Complete is idempotent for the same token; Heartbeat and Get are idempotent by nature.
- **The database is unavailable.** Statements fail with `Unavailable`, the broker's `/readyz` fails after its database check goes stale (5 s), and long polls answer empty or `Unavailable`. Nothing is lost: jobs are in the database, and leases that expire meanwhile are reaped when it returns. A deadlock or a serialization failure, which PostgreSQL rolls back, is classified as retryable.
- **Brokers on different hosts disagree about the time.** They do not compare it: expiry and `run_at` are compared in SQL, on the database clock.

## Time

The database clock is the only time authority. `jobqueue.now()` is defined as `clock_timestamp()`, and every statement reads it once, in a leading CTE, and uses that value throughout. The broker never compares its own wall clock with a database timestamp, and durations cross the boundary as whole microseconds, computed in the database (`remaining`), so that no clock but a worker's own monotonic one is ever read by a worker. Every `time.Time` inside a job is UTC at microsecond precision. In tests, the database function is replaced with one that reads a controllable table, so the Go code and the SQL are the ones that run in production. [ADR 0004](design/0004-time-authority.md) has the reasons.

## Load balancing and shutdown

A gRPC connection is long-lived and pins itself to one pod, so a new broker would receive no traffic. The SDK therefore dials `dns:///<headless service>:7070` with `round_robin`, which opens a connection to every address, and the server closes each connection after `--max-connection-age` (5 min, plus 30 s of grace for running calls), which makes the clients connect again and notice new brokers. [ADR 0006](design/0006-stateless-brokers-and-load-balancing.md) covers this and what it costs.

On SIGTERM a broker shuts down in this order:

1. `/readyz` starts to fail, so that the endpoint leaves the Service.
2. Lease calls are refused with `Unavailable` (the SDK moves to another broker) and every waiting long poll is answered empty.
3. The gRPC server stops accepting calls and waits for the running ones, for at most the shutdown grace (25 s). Heartbeats and acknowledgements are still served, so workers can report on what they hold.
4. The background loops stop, the HTTP endpoint closes and the database pool is released.

`/healthz` never fails while the process answers, so that a probe does not kill a broker during its grace period. The chart adds a preStop sleep of 5 s so that endpoint removal reaches the clients before draining starts, and refuses to render unless the sleep plus the shutdown grace fits inside `terminationGracePeriodSeconds` (40 s).

## Security model

- **Authentication** is by static bearer tokens in a file of SHA-256 hashes, so a leaked file reveals no credential. A token has one of three roles: a *producer* is bound to a tenant, a *worker* to a list of queues, and an *admin* may call every method. Producers can enqueue, get and cancel their own tenant's jobs; workers can lease, heartbeat, acknowledge and release on their queues; the rest are admin calls.
- **Transport.** TLS is optional on a loopback address. The server refuses to start on any other address without both a certificate and a tokens file, unless `--insecure` is given. The SDK refuses to send a token over a connection that is not TLS unless the caller chose `WithInsecure`.
- **Validation before authorization before the store.** Every handler validates the whole request, then authorizes it, and only then touches the store, so a malformed or forbidden request never reaches the database.
- **The metrics and health endpoint** (`--http-addr`) carries no authentication, as probes and scrapers send no tokens. **The KEDA scaler port** does not either, and it is plaintext gRPC, because KEDA's external scaler protocol sends no token and KEDA dials it without TLS; it reads job counts only and changes nothing, and it must stay inside the cluster: the chart's NetworkPolicy admits it only from the KEDA namespace.

## Observability

Prometheus metrics are served at `/metrics` on the HTTP port: jobs enqueued, deduplicated, leased, reaped and pruned; acknowledgements by operation and result (`accepted`, `duplicate`, `fenced`); dispatch fetches by result; waiting long polls; how long a Lease waited; store call durations; the number of jobs of each queue by state; NOTIFY reconnects; and build info. Queue labels are capped at 100 distinct values, with later queues counted under `other`. Logs are structured (`log/slog`, JSON by default). [docs/operations.md](operations.md) lists every metric and says what to do with them.

## Migrations

Migrations are embedded SQL files, forward-only, numbered without gaps. `jobqueuectl migrate` applies the missing ones, each in its own transaction with its bookkeeping row, while holding a session advisory lock, so concurrent callers apply each migration exactly once. A broker checks the schema version when it starts and refuses to run against a database at another version, with an error that names both versions and the remedy. There is no down migration: a change that must be undone is a new migration.

## Where to read more

[docs/testing.md](testing.md) explains how each of these properties is tested, [docs/operations.md](operations.md) how to run the system, and [docs/design](design/0001-postgres-as-source-of-truth.md) why it is built this way.
