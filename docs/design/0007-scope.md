# 0007. Scope: what the queue does not do

Status: accepted.

## Context

A queue that tries to be everything is correct at nothing. Each feature on this list is real and wanted by someone, and each would add a state surface, a semantic that has to be specified and a column of tests, in a project whose claim is that what it does is proved. The list says what is out, so that a reader does not wait for it and a contributor does not have to guess.

## Decision

The following are not part of this queue. Each has its reason, and where there is a way to get the effect without the feature, it is said.

1. **A storage engine or replication of our own.** A log with Raft would remove the ceiling of one database, and the whole project would then be about durability, compaction and recovery. Production queues (River, Oban, Graphile Worker, Solid Queue) use their relational database for this reason ([ADR 0001](0001-postgres-as-source-of-truth.md)). *Instead:* sharding queues over several PostgreSQL clusters is the way to scale past one. It is described and not implemented.
2. **Priorities.** Priority and tenant fairness are two dimensions, and offering both multiplies the scheduling semantics and the test matrix. Within a tenant, jobs are dispatched in `(run_at, id)` order as a **best effort**: with several brokers and `SKIP LOCKED` the dispatch order is not strict, and with concurrent workers the execution order is less so. The README says so, and the order is not a tested guarantee. *Instead:* use a separate queue for urgent work.
3. **Cron, workflows, DAGs and job dependencies.** Each is a product of its own, the domain of workflow engines such as Temporal. *Instead:* put a scheduler in front of the queue, or have a handler enqueue the next step.
4. **Per-queue rate limits.** Fair scheduling already covers the noisy-neighbour case, which is the core of the need. A limiter is another piece of state with its own clock semantics. *Instead:* limit the worker's concurrency, or the producer.
5. **Distributed tracing (OpenTelemetry).** Valuable, and it changes none of the correctness arguments. Metrics and structured logs are enough to operate the queue. *Instead:* carry a trace id in the payload.
6. **Large results.** A result is limited to 64 KiB, and a payload to 256 KiB, so that rows stay small enough to be TOASTed and a batch response is bounded in memory (64 payloads, 16 MiB). *Instead:* write the data elsewhere and put a reference in the result.
7. **Global fairness across brokers.** Each broker shares its own fetches by weight, so the overall share is the sum of the brokers' shares, and only approximately fair. We say so and do not pretend otherwise ([ADR 0005](0005-drr-fair-scheduling.md)).
8. **SDKs in other languages.** The gRPC protocol is public and versioned (`proto/jobqueue/v1`, checked by `buf breaking`), and only the Go client ships. *Instead:* generate a client from the protocol.
9. **Correctness across a PostgreSQL failover.** The queue trusts a managed PostgreSQL's durability and its promotion semantics, and the test suite does not promote a replica ([docs/testing.md](../testing.md)).
10. **Exactly-once delivery.** Not offered, because it cannot be. The queue delivers at-least-once and fences stale holders; effectively-once needs a fenced or idempotent sink ([ADR 0002](0002-leases-and-fencing.md)).

**Dependencies are scope too.** The code of the main module imports the standard library, `google.golang.org/grpc`, `google.golang.org/protobuf`, `github.com/jackc/pgx/v5`, `github.com/prometheus/client_golang` and the vendored KEDA `externalscaler.proto`. Tests may also use `pgregory.net/rapid`, `golang.org/x/tools/go/packages` and the Toxiproxy client. The benchmark module is separate and may add an HDR histogram and River. There is no ORM and no CLI framework (`flag` only), and logs go through `log/slog`. A test (`internal/archtest`) fails on any other import, and a new dependency needs a design record that says why it is worth having.

## Consequences

- **Users need workarounds for the missing features,** listed above, and the README must not suggest otherwise.
- **The ceiling is one PostgreSQL.** A workload beyond it needs sharding that this project does not provide.
- **Some questions have an answer of "no", on purpose:** a dependency graph between jobs, a schedule, a rate limit, a trace. A request for one is a request for a new design record, not for a patch.
- **The scope is checkable.** The dependency list and the limits are enforced by tests, not left to good intentions.

## Alternatives rejected

- **A broader first release** (priorities, cron, rate limits, tracing). Each would have been present and under-tested. The goal of the project is that what is built has its failure paths built too, and a smaller scope is how that stays possible. Rejected.
- **Priorities as an extension of tenant weights.** Weights share capacity; priorities order it, and the two interact (what does weight 5 at low priority mean?). Rejected until the interaction can be specified and tested.
- **Leaving the list unwritten.** Unwritten non-goals become broken promises. Rejected: this record is the list.
