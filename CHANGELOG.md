# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **A worker no longer gives up a finished job's acknowledgement while it holds the lease.** It gave up after five failed resends, about 3 s, although the lease held for up to 27 s more, so an outage of a few seconds made the job wait for its lease to expire and run again. In the Kubernetes end-to-end test, a rolling restart that replaced every broker a worker knew left it without a broker for about 10 s, and the jobs it had finished took 31 s from enqueue to finish. A failed acknowledgement is now sent again, at most 2 s apart, for as long as the worker holds the lease, and at least five times; past a shutdown's drain timeout only the five tries count ([#78](https://github.com/jason-te-sde/distributed-job-queue/issues/78)).
- **The documentation of a broker's shutdown.** It said that a draining broker keeps serving heartbeats and acknowledgements. A draining broker finishes the calls that are running and refuses new calls of every kind, which workers send to other brokers.

## [0.1.0] - 2026-11-01

The first release.

### Added

- **The queue.** Jobs with a queue, a tenant, a payload of up to 256 KiB, a delay or a start time, a budget of up to 100 attempts and an idempotency key; the states `ready`, `leased`, `succeeded`, `dead` and `cancelled`; retries with exponential backoff and full jitter; a dead-letter state with requeue and purge.
- **Leases with fencing tokens.** Delivery is at-least-once. Every lease carries a token that grows by one, and every write by a holder is conditional on it, so a stalled worker cannot change a job that another worker has taken. `Job.Token()` exposes the token for fenced writes downstream.
- **Weighted fair scheduling** across tenants within a queue, by deficit round robin (`--scheduler drr`, `--tenant-weights`, `--quantum`), and a first-come-first-served mode (`--scheduler fifo`).
- **The broker**, `jobqueue-server`: a stateless gRPC server over PostgreSQL 15 to 18, with long-polling leases, `LISTEN`/`NOTIFY` wakeups with a fallback poll, a bound on the pool connections that notifying calls may hold (`--max-notify-conns`), so that leases and acknowledgements keep connections of their own in any pool of two or more, a reaper and a pruner on every broker, Prometheus metrics, `/healthz` and `/readyz`, and a graceful shutdown that drains.
- **Authentication and transport.** Static bearer tokens for producers (bound to a tenant), workers (bound to queues) and admins, stored as hashes; TLS; a refusal to serve a non-loopback address without TLS and tokens unless `--insecure` is given.
- **The Go SDK**: `Dial`, `Producer` (retries, automatic idempotency keys, batches) and `Worker` (concurrency, batched heartbeats, coalesced acknowledgements, cancellation when a lease is lost, panic recovery, graceful drain).
- **`jobqueuectl`**: enqueue, get, cancel, stats, dead list, requeue, purge, migrate and token generation.
- **Migrations**: embedded, forward-only SQL under an advisory lock, and a broker that refuses a schema at another version. `0001` creates the schema; `0002` removes autovacuum's cost delay from the jobs table: in the soak a vacuum of the table took as long as autovacuum's naptime (60 s), and without the delay it took 0.69 s at the median.
- **A multi-architecture distroless container image** and a **Helm chart** (two brokers, headless Service, PodDisruptionBudget, probes, a migration hook, a NetworkPolicy), with a **KEDA external scaler** that reports a queue's backlog, so workers can scale from zero.
- **An example worker** that renders thumbnails and writes through a fenced PostgreSQL table.
- **Verification**: pure cores with an architecture test; a deterministic simulator that checks eight safety invariants and two liveness invariants after every step; nine planted flaws, at fourteen sites, that a CI job requires the suite to catch; a differential test between the reference store and the PostgreSQL store; PostgreSQL concurrency tests; a multi-process chaos test with fault injection; a Kubernetes end-to-end test on kind; fuzz targets.
- **A benchmark harness**, `jobqueue-bench`, in a module of its own: open-loop load with HDR histograms for throughput and latency, the ablations, the noisy neighbour, recovery after a killed worker or broker, a soak, and River as a reference, with the method, the results of one laptop and the raw output in `docs/benchmarks.md`.
- **Documentation**: the README, the architecture, seven design records, an operations guide, a testing guide, and the contributing, security, conduct and releasing policies.

### Notes

- Delivery is at-least-once, never exactly-once. Effectively-once needs a fenced or idempotent sink.
- The schema is at version 2.
- Known limitations, tracked as issues: every broker computes the KEDA scaler's backlog with an aggregate over all the ready, leased and dead rows of the queue about once a second, so its cost grows with the backlog and with the dead letters, which are kept for 336 hours, and it was not measured at a large backlog ([#58](https://github.com/jason-te-sde/distributed-job-queue/issues/58)); the broker bounds neither its heap nor the bytes of the requests in flight, so with messages of up to 20 MiB and the chart's 512Mi memory limit, enough concurrent maximum-size Enqueue batches can get a broker killed for running out of memory, which loses no job but leaves its leases to expire ([#59](https://github.com/jason-te-sde/distributed-job-queue/issues/59)).

[Unreleased]: https://github.com/jason-te-sde/distributed-job-queue/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/jason-te-sde/distributed-job-queue/releases/tag/v0.1.0
