# 0004. The database clock is the only time authority

Status: accepted.

## Context

A lease is a promise about time: "this job is yours until T". Three parties handle that promise, each with a clock of its own. The database stores the expiry, a broker decides when a lease has run out, and the worker decides when to stop working. Their clocks differ, by milliseconds on a good day and by seconds after a bad NTP step, and a broker with a fast clock that reaps a lease early produces two valid holders of one job. Worse, a worker that measures its lease from the moment it *received* the answer, not from the moment it asked, believes it holds the job longer than the broker does, by as much as the answer was delayed.

The rule has to hold with unbounded network delay, because delay cannot be bounded, and it has to be testable without changing the code that runs in production.

## Decision

**One clock decides. It is PostgreSQL's.**

- `jobqueue.now()` is defined as `clock_timestamp()`. Every statement reads it **once**, in a leading CTE named `clock`, and uses that value throughout, so that a job's `created_at` and `run_at` agree and so that the time left on a lease is exactly the TTL that was asked for.
- Every expiry and every `run_at` comparison happens in SQL, against that value. A broker never compares its own wall clock with a database timestamp. Whether a lease can be reaped, a heartbeat refused, a job leased or a row pruned is decided where the timestamps live. The boundaries are fixed: `run_at == now` can be leased, `expires == now` can be reaped and a heartbeat at that instant is `Expired`, and a row is pruned only when `now - finished_at` is strictly greater than the retention.
- The store hands back **durations**, not timestamps: the time `remaining` on a lease, computed in the database as `expires - now`. Durations cross the boundary as whole microseconds, in an integer, and the stores reject anything finer. Every `time.Time` inside a job is UTC at microsecond precision.
- The store never generates randomness and never decides policy. The retry delay, with its jitter, is drawn by the broker from a seeded source and passed in as a parameter.

**A worker measures only durations, with its monotonic clock, and anchors them at the request.** For a lease granted in answer to a request, or extended in answer to a heartbeat, the worker's local deadline is

> send instant + waited + remaining - margin

where *send instant* is the monotonic reading taken when the request left, *waited* is how long the request waited at the broker before the database was asked, up to the start of the fetch that found the job, so that the time the database takes to answer is not in it (non-zero for a long poll, measured by the broker's monotonic clock), and *remaining* is the database's `expires - now`. The database evaluated `now()` at some instant that is at least the send instant plus the time waited, because the request had to reach the broker, wait and reach the database. So the true expiry, which is that instant plus *remaining*, is **never earlier** than send + waited + remaining. The local deadline therefore lies before the true expiry by at least the margin, whatever the delay of the request, the processing at the broker, the commit or the answer. The argument does not use a bound on any latency.

The margin is then free to do two other things. It absorbs the difference in the *rate* of monotonic clocks (at most about 100 ppm, 3 ms over a 30 s lease), and it gives the handler time to wind down and the worker time to deliver a final acknowledgement before the reaper can act. The defaults are a 30 s TTL and a 3 s margin; the TTL must be strictly greater than four times the margin.

**Tests run the production path.** The PostgreSQL tests replace `jobqueue.now()` with a function that reads a controllable table, and move the clock by writing to the table. The Go code and the SQL are exactly what runs in production; only the one database function differs. The reference store and the simulator take their time from an injected clock, and the simulator gives every node a clock with its own offset and drift.

## Consequences

- **The database clock is a single point of dependence.** If it steps forward, leases expire early and are reaped (the fencing token keeps that correct, and the work done is thrown away); if it steps backward, expiries are late and jobs wait longer. `clock_timestamp()` is wall time, not monotonic. A failover to a replica with a different clock does the same. This is a smaller exposure than N clocks, not none, and it is why leases are never trusted by clock alone ([ADR 0002](0002-leases-and-fencing.md)).
- **Time is decided where it is stored.** Every statement carries the clock CTE, and the planner needs help to use it as an index bound (a comparison against `(SELECT now FROM clock)` is an index range; a join with the clock is a scan). A test pins which rows the hot statements read.
- **The margin shortens every lease a little.** A worker that cannot reach the broker stops its handlers 27 s after the last extension with the defaults, not 30 s. Long jobs are covered by heartbeats; a very short TTL leaves little room, which is why the TTL must exceed four margins.
- **Clocks of the worker and the broker are not needed to agree, and are not.** Monotonic frequency drift beyond the margin model is not covered, and the testing guide lists it as not covered.
- **Flaw F5 shows what would go wrong.** With the deadline anchored at the arrival of the answer, correctness would depend on the end-to-end latency being less than the margin. The planted flaw does exactly that, and the simulator's check S5 (the local view is conservative) goes red.

## Alternatives rejected

- **The broker's wall clock for expiry.** Simple, and wrong with more than one broker: a broker whose clock runs ahead reaps leases early, and two workers then hold one job.
- **Assuming synchronized clocks (NTP).** The bound on the error is not known, and the rule would be as strong as the worst machine on a bad day. Rejected for a rule that holds without a bound.
- **Hybrid logical clocks or TrueTime-style uncertainty intervals.** They solve ordering across machines with real machinery (hardware time sources or extra protocol) for a problem that a single database already settles. Rejected as more than the problem needs.
- **Anchoring the worker's deadline at the arrival of the answer.** Natural, and it makes correctness depend on latency being below the margin, which cannot be guaranteed. Rejected; flaw F5 is the proof by construction.
- **Passing timestamps instead of durations.** It would require the worker's clock and the database's to agree. Durations measured on one monotonic clock need nothing of the kind.
- **A "test clock" branch in the Go code.** The code under test would then differ from the code that ships. Replacing the database function keeps one code path.
