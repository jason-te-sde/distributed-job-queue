# 0003. Pure cores and a deterministic simulator

Status: accepted.

## Context

The hard bugs of a queue live in interleavings: a heartbeat that arrives after the lease was reaped, a notification lost while a long poll expires, a worker that wakes up after its job has been finished by another. They need a particular order of events across several machines, they appear once in a million runs, and when they do appear nobody can reproduce them. Tests that start real processes and sleep are slow, flaky and unable to say which of the interleavings they covered.

The remedy is to make a whole distributed run a function of one integer, so that a failing run can be replayed, shrunk and fixed, and to run enough of them to find the rare orders.

## Decision

**The decision logic is pure.** Four packages hold every rule about what happens next, and none of them performs an effect:

- `internal/core/job`: the transition table, retry policy and validation.
- `internal/core/fair`: the deficit round robin planner.
- `internal/core/dispatch`: the broker-side logic of long-polling waiters, wakeups and fetches.
- `internal/core/lease`: the worker-side tracker of leases, deadlines, heartbeats and acknowledgements.

Each takes the current instant as an argument and returns **effects** as values (fetch these jobs, answer this waiter, send this heartbeat, cancel that handler, arm the timer for this instant). A thin shell, in `internal/broker` and in the SDK, owns the goroutines, the sockets and the clock, carries the effects out and reports what happened by calling the core again. The shell has one event-loop goroutine that owns the core, and a build with the race detector checks that no other goroutine enters it.

**Purity is enforced by the build, not by habit.** `internal/core/...` may import only the standard library minus the packages that perform effects (`os`, `net`, `sync`, `sync/atomic`, `syscall`, `unsafe`, `math/rand`, `crypto/rand`, `log`, `log/slog`, `runtime`, `reflect`, `io/ioutil`), `internal/flaw` and other core packages. It has no `go` statement, no `select` and no channel type, and it never calls `time.Now` and its relatives or the global functions of `math/rand/v2`. Two layers check this. The linter has `depguard` and `forbidigo` rules, and `internal/archtest` is an ordinary `go test` that loads the packages with `go/packages` and checks the import graph and the syntax tree, so a plain `go test ./...` goes red without the linter. The same test checks the layering between packages and that only the listed dependencies are imported. Sum types are interfaces with an unexported marker method, and the linter requires every type switch over them and every switch over an enum to be exhaustive, so adding a new effect or state does not compile until every user handles it.

**The simulator runs the real cores.** `internal/sim` is a single-goroutine kernel with a virtual clock and a seeded random source. It builds a cluster of one to three brokers and two to five workers over the in-memory reference store, with a network that drops, duplicates, delays (with a long tail well beyond the safety margin) and partitions messages; a producer that retries by idempotency key; and a fenced downstream sink. Faults are scheduled from the seed: worker crashes and restarts, worker stalls longer than the TTL, broker crashes, store outages, partitions and lost notifications. Every node has its own clock, with an offset and a rate that drifts. Nothing reads the wall clock or starts a goroutine, which the architecture test checks for the whole package, so a run is a function of its seed, and a failing seed replays exactly.

**An oracle checks after every step.** Safety, at every event: no job confirmed to a producer is lost (S1); finished jobs do not come back (S2); one holder at a time, with epochs that grow and are not reused (S3); an accepted write carries the current epoch, and the last write to the sink for a succeeded job carries its completing token (S4); a worker's local view of every lease is conservative: its deadline is no later than the store's expiry minus the margin, plus a bound for clock drift (S5); attempts stay within the budget (S6); an idempotency key names one job while its row exists (S7); no lease is granted before `run_at` (S8). Liveness, once the faults have stopped and the system has had time to heal: every confirmed job that was not cancelled reaches `succeeded` or `dead` (L1), and no lease outlives its expiry by more than the reap interval (L2). Each batch of seeds must also be rough enough to mean something: the run asserts that fenced rejections, reaps, duplicate deliveries and every kind of fault happened. The checker has tests of its own, with hand-built bad histories that must raise an alarm and good ones that must not.

**The suite must be able to turn red.** A build tag, `jobqueue_flaws`, compiles in planted defects (a Complete that ignores the token, a lease that does not advance the epoch, a deadline anchored at the arrival of the answer, a planner that drops its deficit, and the rest of the table in [docs/testing.md](../testing.md)). In an ordinary build they are compiled out. A CI job builds with each flaw in turn and requires the tests that name it to fail. The store flaws exist in both the memory store and the PostgreSQL store, and are qualified by site (F1-mem, F1-pg): the simulator catches the first and the differential test the second.

**The PostgreSQL store is bound to the reference model by a differential test.** The in-memory store implements the transitions by calling the pure core, so it is the model. A state-machine property test applies the same random operations, including advancing the clock, reaping and pruning, to the memory store and to the PostgreSQL store and compares every answer and the whole table after each step, shrinking a failure to a minimal sequence. The conformance suite that both stores pass pins what the contract says; the differential test finds what nobody wrote down.

CI replays 500 seeds on every run; the nightly job runs 20,000. A seed that exposed a bug is kept in `internal/sim/testdata/regression_seeds.txt` and runs every time.

## Consequences

- **The simulator's broker and worker glue re-implements the shell.** The simulated broker and worker nodes carry out the effects themselves; they are not the goroutines, gRPC streams and timers of `internal/broker` and the SDK. A bug in the real shell (a race, a leaked goroutine, a missed timer) is outside what the simulator can see. The multi-process chaos test runs the real binaries with real signals and a fault-injecting proxy, and is where the shell is tested.
- **PostgreSQL's behaviour is not simulated.** Row locking, `SKIP LOCKED`, deadlocks and the planner's choices are real only in the tests that use a real database: the conformance suite, the differential test and the concurrency tests.
- **A passing run is evidence, not proof.** The simulator explores the interleavings its fault model produces, in numbers; what it does not model it cannot find.
- **More code per feature.** Every behaviour is an effect type, a method on a core and a shell clause, where a straight-line goroutine would have been shorter. That is the price of replayable runs.
- **The reference model has to be right.** If the memory store and the PostgreSQL store shared a mistake, the differential test would not see it. The conformance suite and the property tests of the core are what guard the model.
- **A restriction on contributors.** Code in the core cannot take a shortcut through the clock or a goroutine, and the build says so at once.

## Alternatives rejected

- **Only real-process tests.** They are necessary (the chaos test is one) and not enough: a failure there cannot be replayed, coverage of interleavings is by luck, and every run costs seconds to minutes.
- **Mocks and injected clocks over ordinary concurrent code.** A fake clock removes the sleeps and keeps the nondeterminism of the scheduler, so a failing run still cannot be replayed, and the code under test is full of goroutines and locks that the test cannot order.
- **`testing/synctest`.** It virtualizes time for goroutines in one process, which is useful, and it would still test the goroutines instead of the logic. It has no network with loss, duplication and partitions, no drifting clocks, no seeded fault schedule and no multi-node model.
- **Model checking the design (TLA+ or similar).** It proves a model, not the code, and the interesting bugs sit in the gap between them. It would be a good addition; it is not a replacement for running the code.
- **An external consistency checker over real clusters (as Jepsen does).** The chaos test applies this idea to a smaller system. It is slow and cannot be seeded, so it is the last layer, not the first.
- **Sharing the transition code between the two stores.** The PostgreSQL store could call the core for every row, at the price of a round trip per step on the hot path. It states the guards as SQL instead, and the differential test is what binds the SQL to the table.
