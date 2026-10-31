# 0005. Weighted fair scheduling with deficit round robin

Status: accepted.

## Context

A queue shared by several tenants has the noisy-neighbour problem. When one tenant enqueues 100,000 jobs and another enqueues ten a second, a first-come-first-served queue makes the second tenant wait behind all of the first's backlog, and its latency is set by someone else's behaviour.

The scheduler has to give each tenant with work its weighted share of the leases, cost almost nothing per lease (a lease is on the hot path, once per batch), and be a pure function that the simulator and the property tests can drive ([ADR 0003](0003-pure-cores-and-simulator.md)). It is a different dimension from priority, which is not offered ([ADR 0007](0007-scope.md)).

## Decision

Within a queue, each broker shares its fetches between tenants with **deficit round robin** (DRR), as in `internal/core/fair`.

- **Ring.** The tenants that currently have ready work sit in a ring, in a fixed order. The ring comes from the store's `Activity` call, refreshed every `--activity-interval` (1 s), and a notification that names a tenant adds it at once. A tenant that delivers fewer jobs than it was asked for has run out of work, leaves the ring and loses its deficit.
- **Turns.** A tenant's turn begins by crediting it a quantum of 8 jobs times its weight (`--quantum`, `--tenant-weights`; the weight is 1 to 1000 and defaults to 1) and lasts until the tenant has used it. A turn that the end of a batch cuts short stays open for the next call.
- **Plan.** `Plan(n)` returns, for a batch of `n` jobs, how many to take from which tenants. The dispatcher turns that into one store call: a `LATERAL` subquery per tenant with its own `LIMIT`, with `FOR UPDATE SKIP LOCKED`. `Settle` reports how many jobs each take really delivered.
- **Batch independence.** Planning `n` jobs in one call grants every tenant exactly what planning one job `n` times would. The share a tenant gets does not depend on how the worker's requests are sized or combined. A test asserts it over random weights, quanta and batch sizes.
- **FIFO mode.** `--scheduler fifo` bypasses the planner and leases in `(run_at, id)` order across tenants. It exists as the baseline of the noisy-neighbour experiment and for operators who do not want fairness.

**The bound.** The scheme is Shreedhar and Varghese's, *Efficient Fair Queuing Using Deficit Round-Robin*, SIGCOMM 1995, in the extended version in *IEEE/ACM Transactions on Networking* 4(3), June 1996, pp. 375-385. Their analysis is stated for packets of different sizes, with `Max` the largest packet and `Q_i` the quantum of flow `i`. The numbering below is that of the 1996 journal version, checked against the paper (the 1995 conference version numbers the same results in section 4 differently):

- Lemma 1: the deficit counter of every flow satisfies `0 <= DC_i < Max` at all times.
- Lemma 2: over any interval in which flow `i` is backlogged, and in which it receives `m` service opportunities, the bytes it sends satisfy `m * Q_i - Max <= sent_i <= m * Q_i + Max`. The proof telescopes the deficit counter over the rounds and uses Lemma 1.
- Theorem 3: for two backlogged flows, the fairness measure over any interval is at most `2 * Max + Q`, where `Q` is the smallest quantum. The proof adds that between two service opportunities of one flow every other backlogged flow gets one.
- Theorem 4: the work per packet is O(1) when every quantum is at least `Max`.

Jobs are all of the same size here (one lease grant each), so `Max` is one job and a turn is spent exactly. The property test `TestPlanShareBound` states the corresponding bound for this planner: with every tenant always active, after every call the number of jobs granted to tenant `i`, out of `N` granted in all, differs from its weighted share `N * w_i / W` by at most the tenant's own quantum, `Q * w_i`. This is the form that Lemma 2 and the argument of Theorem 3 take for unit-size items; the test checks the bound for this code and does not rely on the paper's proof being applied correctly. The batch-independence property above has no counterpart in the paper; it comes from keeping a cut turn open between calls.

## Consequences

- **Fairness is per broker.** Each broker runs its own planner over the queue, so the share a tenant gets overall is the sum of its shares at the brokers: close to its weight, not exactly it. The bound holds at one broker with every active tenant always having work, which is the case it is proved for.
- **A job costs one, whatever it takes to run.** The share is of lease grants, not of worker time. A tenant whose jobs take ten times longer occupies ten times the worker capacity for the same number of grants. There is no cost model.
- **Weights are static.** They come from a flag, and changing them takes a restart of the brokers (a rolling one).
- **The ring is eventually accurate.** A tenant that gains its first job is added by its notification within the notification's latency, or by the next `Activity` refresh (up to 1 s). A planner may spend one fetch on a tenant that has just run out of work, and it is then removed.
- **Order within a tenant is best effort.** Jobs of one tenant are taken by `(run_at, id)`, and with several brokers and `SKIP LOCKED` the order of dispatch is not strict, and the order of finishing is whatever concurrent workers produce. Strict FIFO is not offered.
- **A quantum of 8 trades latency for efficiency.** A larger quantum fetches fewer, bigger groups from one tenant at a time; a smaller one interleaves tenants more finely and can put more tenants into one fetch. 8 is a default, and it is a flag.
- **The planted flaw F8** drops the unused part of a turn that a batch cut short, so a tenant's share would depend on the batch size. `TestPlanBatchIndependence` and `TestPlanShareBound` fail on it.

## Alternatives rejected

- **First come, first served.** The noisy neighbour is exactly its failure. Kept as `--scheduler fifo` for comparison.
- **Strict priorities.** Starve the low priority under load, and priorities are a separate feature with their own semantics and test matrix. Rejected here and in [ADR 0007](0007-scope.md).
- **A queue per tenant.** The caller would have to split the traffic, and workers would have to know every tenant's queue to share workers between them. Rejected: tenants are a field, not a topology.
- **Weighted fair queuing with virtual time (WFQ, and its relatives).** Tighter fairness, at O(log n) per decision and with a job cost that must be known up front. For unit-cost grants DRR is O(1) and has the same bound in shape. Rejected as more than needed.
- **Choosing a tenant at random, in proportion to its weight.** Right on average and with a large variance over short windows, and with no deterministic bound to test. Rejected.
- **Per-tenant rate limits.** They protect a system from a tenant rather than share capacity among tenants, they need state and a clock, and they are a separate feature ([ADR 0007](0007-scope.md)). Rejected.
