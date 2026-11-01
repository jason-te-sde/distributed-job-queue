# 0002. Leases and fencing tokens

Status: accepted.

## Context

A worker takes a job, and then it can stop being the worker without knowing it. A garbage collection pause, a stopped container, a swapped-out process or a network partition can freeze it for longer than anyone expected, and when it wakes up it believes it still holds the job. By then the broker has given the job to someone else, because it had to: a job whose worker is gone must run again.

A lease with an expiry cannot close this gap. The worker checks the clock, sees that it is still inside its lease, and then pauses before it writes. Nothing the worker does between the check and the write can make them one step. This is the situation that Martin Kleppmann describes in *Designing Data-Intensive Applications*, chapter 8 ("The Trouble with Distributed Systems", first edition), under fencing tokens: a client holding a lease pauses, the lease expires, a second client takes it and writes, and the first client wakes up and writes as well. The remedy there is the one used here. The lock service hands out a number that grows with every grant, and the resource being written refuses a write that carries a lower number than one it has already seen.

The design therefore has to separate two jobs that expiry alone cannot do together: letting another worker take over a job whose holder is gone (liveness), and making sure the old holder can do no harm after that (correctness).

## Decision

**Correctness from the epoch, liveness from expiry.** The epoch decides whether a write is valid; expiry only decides when another worker may take the job over. Neither does the other's work.

- **The epoch.** Every job has a `lease_epoch` that grows by one each time the job is leased and never decreases. It is the fencing token. Release gives the attempt back but not the epoch, and requeueing a dead job resets its attempts but not its epoch, so the token is monotonic over the whole life of the job. (The attempt count is not used as a token for this reason: it goes down.)
- **Every holder's write is conditional on the token.** Heartbeat, Complete, Fail and Release apply only to a job that is `leased` with `lease_epoch` equal to the token the caller holds. In PostgreSQL the condition is in the `WHERE` clause of the `UPDATE`, so the check and the write are one atomic step. Once a newer lease exists, every write by an older token is rejected, however late it arrives. This alone makes the queue's own state safe against a zombie.
- **Expiry only grants the right to take over.** The lease expires on the database clock; the reaper takes an expired job back as a failed attempt; another worker leases it and the epoch moves on. Expiry has no part in deciding whether a write is valid.
- **Complete ignores expiry.** A worker that finishes after its lease ran out, before the reaper has taken the job back, has done valid work, and nobody holds a newer token. Accepting its Complete is safe and saves the work. The window closes at the reap, not at the next lease: the reaper makes the job `ready` again under the same epoch, and a Complete under that token is fenced from then on, even if nobody has leased the job since. The reap comes within about a reap interval (1 s) of the expiry. Heartbeat is stricter: it refuses a lease that has run out and does not revive it, because the reaper may act at any instant, and a worker's view of its leases must be conservative.
- **The token goes to the user's code.** `Job.Token()` exposes the epoch of the running attempt. A downstream system that keeps the highest token it has seen for each job and refuses a write under a lower one is protected from a zombie that the queue cannot stop. The thumbnail example does this with one conditional `INSERT ... ON CONFLICT DO UPDATE ... WHERE sink.token < EXCLUDED.token`. A write under the same token is also refused, so a handler writes once per attempt.
- **The worker stops itself first, so that few zombies exist.** The worker's local deadline lies a margin before the lease's expiry ([ADR 0004](0004-time-authority.md)), and when it passes, the SDK cancels the handler's context. Heartbeats go out every TTL/4, and the TTL must be strictly greater than four times the margin, so that two lost heartbeats do not cost a lease. A handler that ignores its context is not stopped, only fenced.
- **On the wire**, an acknowledgement is `ACCEPTED`, `DUPLICATE` or `FENCED`, and a heartbeat answer is `HELD` or `LOST`. A job that is gone or whose lease ran out is as fenced to the worker as one that someone else leased.

## Consequences

- **Delivery is at-least-once.** A job whose worker stalled runs again on another, so it can run twice, and nothing in a queue can prevent that. We never claim exactly-once. What the queue offers is that only one attempt's writes are accepted by the queue itself, and, with a fenced or idempotent sink, that the *effect* happens once ("effectively once"). The README says so, and so does every place that describes delivery.
- **Effectively-once needs the user's sink.** If the downstream system does not keep a token or is not idempotent, a zombie's side effects (an email, a charge) happen. The queue cannot fix that. `Job.Token()` is the hook, and the zombie-worker example in the README shows the pattern.
- **A zombie wastes work.** A handler that ignores cancellation runs to the end for nothing. The SDK promises only that it cannot write through the queue, not that it can be stopped.
- **A lease can be lost to a slow network.** If the heartbeats cannot get through for longer than the lease allows, the job is taken back, and the work in progress is thrown away. A longer TTL trades slower recovery for fewer such losses.
- **A lost lease costs an attempt.** A reaped lease counts as a failed attempt, so a job whose workers keep losing their leases (a handler that stalls every time, say) ends `dead` after `max_attempts`, with the last error `lease expired`.
- **Two internal outcomes, one external.** The store distinguishes `Fenced` from `Expired` and `NotFound`, and the broker reports all three as fenced to the worker, which is all the worker needs.

## Alternatives rejected

- **Expiry as the only protection.** Correct only if no process ever pauses between its check and its write, which no scheduler, runtime or network can promise. This is the failure that fencing tokens exist for. Rejected.
- **Exactly-once delivery.** It cannot be had across a network and an arbitrary downstream system without the downstream's cooperation: a worker cannot know whether its own write happened if the response was lost. Claiming it would be wrong, and the design avoids the claim.
- **PostgreSQL advisory locks as the lease.** They belong to a session and vanish when the connection drops, which is when the worker is least aware of it; they carry no token the user can pass on, and a worker would need a connection to the database. Rejected.
- **A random lease id (a UUID) as the token.** It identifies a lease but cannot be compared, so a downstream system could say "different" but not "older". Fencing needs an order. Rejected.
- **The attempt count as the token.** Release refunds an attempt and requeue resets them, so it is not monotonic. Rejected in favour of a separate epoch.
- **A very long TTL with no heartbeats.** Simple, and a worker's death is then noticed only after the whole TTL. Heartbeats keep the TTL short for recovery and let a long job live. Rejected.
- **Checking the token only at the sink.** The queue would then accept a zombie's Complete and mark a job done that the new holder is still running. The queue's own state needs the guard too. Rejected as the sole protection, kept as the user's.
