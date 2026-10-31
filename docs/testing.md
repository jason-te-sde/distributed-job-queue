# Testing

The suite puts each check on the cheapest layer that can catch the bug. A rule about one transition is a table-driven unit test; a rule about every interleaving of crashes and stalls is a simulator invariant; a rule about SQL needs a real PostgreSQL; a rule about the real processes needs real processes. This document lists the layers, what each one catches, how to run it, how the suite proves that it can fail, how to reproduce and keep a failing seed, and what it does not cover.

Two rules hold everywhere. A test that needs a database or a cluster **fails loudly** when the environment is missing; it never skips. And a test is only trusted if it can be shown to fail: the planted flaws below exist for that.

## The layers

| Layer | Where | What it catches | Runs in |
| --- | --- | --- | --- |
| Unit and property tests of the pure cores | `internal/core/{job,fair,dispatch,lease}` | Every cell of the transition table, the backoff bound, the fair-share bound and batch independence, the dispatch core's wakeup and liveness rules, the lease tracker's safety rule (it never reports holding a lease past its local deadline). Property tests use `rapid` | `go test -race ./...` |
| Conformance suite | `internal/store/storetest`, run by `memstore` and by `pgstore` | What the store contract says: validation, ordering, boundaries (`run_at == now`, `expires == now`, retention), idempotency scope, size limits, hostile names, batch alignment | `go test` (memory store); `-tags pg` (PostgreSQL) |
| Architecture test | `internal/archtest` | A core that imports `os` or `net`, calls `time.Now`, starts a goroutine or uses a channel; a layering violation; an import outside the dependency list. It needs no linter | `go test ./...` |
| Broker and SDK tests | `internal/broker`, root package | Handlers over an in-process gRPC connection and the memory store: validation before authorization, role and tenant scope, error mapping, the dispatcher shell, the reaper and pruner, the metrics, the SDK producer and worker, the zombie handler, graceful drain. Race builds also check that the cores are entered from one goroutine | `go test -race ./...` |
| Deterministic simulator | `internal/sim` | Whole-system safety and liveness under crashes, stalls, partitions, loss, duplication, reordering, clock drift and lost notifications; see below | `go test -race ./...` (500 seeds) |
| Planted flaws | build tag `jobqueue_flaws`, `scripts/check-flaws.sh` | That the suite goes red when the code is wrong; see below | the `flaws` CI job |
| Differential test | `internal/store/pgstore` (`TestPGDifferential`) | Any divergence between PostgreSQL and the reference store over random operation sequences, with the clock moving, reaping and pruning; shrinks to a minimal sequence | `-tags pg` |
| PostgreSQL concurrency tests | `internal/store/pgstore` (`TestPGConcurrent...`) | Double leases, a duplicate idempotency key under concurrent enqueues, an acknowledgement racing the reaper, lost or invented jobs under load, deadlocks between opposite-order acknowledgements | `-tags pg`, always with `-race` |
| PostgreSQL integration tests | `internal/broker`, `internal/store/pgstore/migrate`, `cmd/jobqueue-server` | The real broker over a real database: schema mismatch refused, tokens enforced, graceful shutdown, the NOTIFY listener reconnecting and resyncing, migrations applied once under concurrency, the schema's constraints and indexes | `-tags pg` |
| End-to-end on PostgreSQL | `e2e_pg_test.go` (lands with the end-to-end task) | The whole stack on real time: three brokers, the SDK, no test doubles; a zombie worker fenced; a rolling restart of every broker; delayed jobs on time; a noisy neighbour under `drr` and `fifo` | `-tags pg` |
| Chaos | `test/chaos` (lands with the chaos task) | The real processes: 3 `jobqueue-server`, workers and a fault-injecting proxy, with `kill -9`, `SIGSTOP`, delays, partitions and PostgreSQL restarts. Checks S1, S4, S7 and L1 on the recorded history | `-tags chaos`, Linux CI |
| Kubernetes end-to-end | `test/e2e` (lands with the end-to-end task) | Helm install in a kind cluster, a broker pod deleted and all brokers restarted under load with zero lost jobs, KEDA scaling a worker from 0 to N and back | `-tags e2e`, Linux CI |
| Fuzzing | `Fuzz...` functions | Request and spec validation, the transition functions (any input leaves the table intact), the NOTIFY payload parser, the tokens file parser and the flag parser never panic | seed corpus in `go test`; the nightly job fuzzes each target |
| Script and documentation tests | `scripts/tests`, `docs_test.go` | The hygiene check (no CJK text, no attribution), the pg test count floor, the Helm render checks, and that the README cites tests that exist, every link resolves and every design record has its sections | `make test-scripts`, `go test` |

### The simulator

`internal/sim` is described in [ADR 0003](design/0003-pure-cores-and-simulator.md). In short: one goroutine, a virtual clock and a seed. It builds one to three brokers, two to five workers, a producer that retries by idempotency key, a fenced sink and a lossy network, runs the **real** dispatch, fair and lease cores over the reference store, and checks the invariants after every event.

| Invariant | Holds at every step |
| --- | --- |
| S1 | No job that was confirmed to a producer is lost; only the pruner deletes, and only a finished job past its retention |
| S2 | Finished jobs do not come back; a dead job leaves that state only through requeue |
| S3 | One holder at a time; epochs grow by one and are never reused |
| S4 | An accepted write carries the current epoch; the sink's last write for a succeeded job carries its completing token |
| S5 | A worker's local view of a lease is conservative: its deadline is no later than the store's expiry minus the margin, plus a bound for clock drift. It holds whatever the latency, the processing delay or the stall |
| S6 | `attempts <= max_attempts` |
| S7 | One id per `(tenant, queue, key)` while the row exists |
| S8 | No lease before `run_at` |

and, after the faults stop and the system has had time to heal:

| Invariant | Holds after healing |
| --- | --- |
| L1 | Every confirmed job that was not cancelled is `succeeded` or `dead` |
| L2 | No lease outlives its expiry by more than the reap interval |

Each batch of seeds must also be *rough enough to mean something*: it asserts that duplicate deliveries, fenced acknowledgements, sink rejections, reaps, prunes, worker crashes and stalls, broker crashes, store outages, partitions and dropped notifications all happened. A system that is left alone passes every safety check. The checker itself has tests: hand-built bad histories must raise the right alarm and good ones must not.

CI runs 500 seeds on every run, which took 62 s under `-race` on an Apple M2. The nightly job runs 20,000; a run of 20,000 seeds found no violation.

## How to run each layer

```sh
go test -race ./...                       # unit, property, conformance (memory), architecture, broker, SDK, simulator (500 seeds), docs
make test                                 # the same

export JOBQUEUE_TEST_DSN='postgres://localhost:5432/postgres?sslmode=disable'
make test-pg                              # everything behind the pg tag, with -race, and a count of the tests that ran
go test -race -count=1 -tags pg -run '^TestPGDifferential$' ./internal/store/pgstore/
go test -race -count=1 -tags pg -run '^TestPGConcurrent' ./internal/store/pgstore/

make sim                                  # the simulator alone, 500 seeds under -race (lands with the simulator)
JOBQUEUE_SIM_SEEDS=20000 go test -count=1 -timeout 60m -run TestSimSeeds ./internal/sim/    # what the nightly job runs

scripts/check-flaws.sh                    # every planted flaw must turn its tests red (lands with the flaws job)

go test -run '^$' -fuzz '^FuzzTransitions$' -fuzztime 30s ./internal/core/job/    # one fuzz target

go test -tags chaos -count=1 -timeout 25m ./test/chaos/                           # needs Docker and Toxiproxy (lands with the chaos task)
go test -tags e2e -count=1 -timeout 40m ./test/e2e/                               # needs kind, Helm and an image (lands with the end-to-end task)

make test-scripts                         # the tests of the scripts
make hygiene                              # no CJK text, no attribution, in files and in commit messages
make lint                                 # golangci-lint, buf, govulncheck
make preflight                            # everything CI runs that can run on a laptop
```

**The `pg` tag.** The PostgreSQL tests sit behind the build tag `pg`, and every test function in such a file is named `TestPG...`. Without the tag they are not compiled, so the macOS jobs run only the rest. With the tag and no `JOBQUEUE_TEST_DSN`, or a database that cannot be reached, they **fail**: there is no switch to skip them. The pg job also counts the tests that actually ran and fails if the count is below the floor in `scripts/pg-min-tests.txt`, so that a whole group accidentally excluded cannot leave the build green. The tests use a local PostgreSQL 17 on a laptop, and 15 and 18 in CI. Each test creates a private, migrated database on the server that the DSN names (`jobqueue_test_<pid>_<n>`) and drops it afterwards, so the role needs `CREATEDB`.

The tags in all: `pg` (needs `JOBQUEUE_TEST_DSN`), `jobqueue_flaws` (compiles the flaw sites in), `chaos` and `e2e`.

## The planted flaws

A suite that has never failed has not shown that it can. Nine defects can be planted, behind the build tag `jobqueue_flaws`. In an ordinary build the sites compile away (`flaw.On` is a constant false). With the tag, `JOBQUEUE_FLAW=<id>` activates one. The store defects exist **twice**, once in the reference store and once in the PostgreSQL store, and the ids say which: the memory copy is caught by the conformance suite and the simulator, the PostgreSQL copy by the differential test.

| Flaw | What is broken | Caught by |
| --- | --- | --- |
| F1-mem, F1-pg | Complete does not compare the token | `TestConformance/CompleteStaleTokenFenced`, simulator S4 (mem); `TestPGDifferential` (pg) |
| F2-mem, F2-pg | Lease does not advance the epoch | `TestConformance/LeaseIncrementsEpochAndAttempts`, simulator S3 (mem); `TestPGDifferential` (pg) |
| F3-mem, F3-pg | The reaper ignores `max_attempts` | `TestConformance/ReapExhaustedGoesDead`, simulator S6 (mem); `TestPGDifferential` (pg) |
| F4-mem, F4-pg | Heartbeat does not compare the epoch | `TestConformance/HeartbeatStaleTokenIsFenced`, simulator S4 (mem); `TestPGDifferential` (pg) |
| F5 | The lease tracker anchors the deadline at the arrival of the response | the tracker tests (`TestGrantDeadlineAnchorsAtSendTime`, `TestDeadlineIgnoresResponseLatency`), the property test `TestTrackerProperties`, `TestWorkerDiscardsAGrantThatArrivedPastItsLocalDeadline`, simulator S5 |
| F6 | The PostgreSQL idempotency check is a check followed by an insert | `TestPGConcurrentEnqueueSameKey` |
| F7 | The PostgreSQL lease select has no `FOR UPDATE SKIP LOCKED` | `TestPGConcurrentLeaseNoDoubleGrant` |
| F8 | The planner drops the unused part of a turn that a batch cut short | `TestPlanBatchIndependence`, `TestPlanShareBound` |
| F9-mem, F9-pg | Lease ignores `run_at` | `TestConformance/LeaseRespectsRunAt`, simulator S8 (mem); `TestPGDifferential` (pg) |

`scripts/check-flaws.sh` (the `flaws` CI job; it lands with the flaws task, and the simulator rows join it when the simulator lands) reads `scripts/flaws.tsv` and, for each row, runs `JOBQUEUE_FLAW=<id> go test -count=1 -tags "jobqueue_flaws[,pg]" -run <regex> <package>` and requires it to fail, then runs the whole suite with no flaw active and requires it to pass. A flaw that no test notices fails the job. You can do one by hand:

```sh
JOBQUEUE_FLAW=F8 go test -count=1 -tags jobqueue_flaws -run 'TestPlanBatchIndependence|TestPlanShareBound' ./internal/core/fair/   # fails
go test -count=1 -tags jobqueue_flaws ./internal/core/...                                                                             # passes
```

When you add an invariant or a defence, plant its opposite and add a row.

## Reproducing and keeping a failing seed

A failing simulator run prints its seed, the invariant and the job, and the command that replays it. It also writes `internal/sim/testdata/failures/seed-<n>.txt` with the configuration, the error and the command; CI uploads that directory as an artifact.

1. **Replay it.** The run is a function of the seed, so it fails the same way every time:

   ```sh
   JOBQUEUE_SIM_SEEDS=1 JOBQUEUE_SIM_SEED_START=<n> go test -race ./internal/sim -run TestSimSeeds
   ```

   `sim.DefaultConfig(<n>)` is the configuration, and `sim.Run` returns the violation, which names the invariant (S1 to S8, L1, L2), the job and what was seen.
2. **Find the cause.** Start from the job in the violation. Look at the cause, not at the symptom, and do not loosen the check.
3. **Fix it, and pin it lower if you can.** If the bug belongs to a core or the store, write a unit or conformance test that fails without the fix. A test on a lower layer fails faster and says more.
4. **Keep the seed.** Add a line `<seed> # <what it exposed>` to `internal/sim/testdata/regression_seeds.txt`. `TestSimRegressionSeeds` runs every listed seed on every run, so the bug cannot come back unnoticed. The file's first entry, 8123, is a smoke seed and stays first. Never delete a seed.

The differential test and the other property tests are reproduced from rapid's output: a failure prints the minimal sequence and a `-rapid.seed` to replay it, and `-rapid.checks` and `-rapid.steps` widen the search (the nightly job runs the differential test with `-rapid.checks=2000 -rapid.steps=200`). Rapid writes failure files to `testdata/rapid/`, which git ignores: turn a useful minimal sequence into a named test instead.

## What the suite does not cover

- **PostgreSQL failover and replica promotion.** The queue trusts the managed database's durability and switchover semantics. No test promotes a replica, or fails a primary over, mid-run.
- **Multi-region deployments.**
- **A worker clock whose frequency drifts beyond the margin model.** The safety rule needs the worker's monotonic clock to run within about 100 ppm of true time, so that a margin of seconds absorbs the difference. The simulator drifts clocks within that bound, and nothing tests a clock that does worse.
- **Malicious (byzantine) workers.** A worker with a valid token can lie about what it did; the queue fences stale holders and does not verify results.
- **PostgreSQL's own durability.** `fsync` and the storage under it are trusted.
- **Performance on other hardware.** The benchmarks are one Apple M2 laptop, with PostgreSQL on the same machine; CI numbers are smoke only.
- **Exactly-once effects in your systems.** The queue fences and exposes the token; whether your sink uses it is yours to test.

And three limits of the layers themselves:

- **The simulator's broker and worker glue re-implements the shell.** The simulated broker and worker nodes carry out the cores' effects themselves; they are not the goroutines, gRPC streams and timers of `internal/broker` and the SDK. A race, a leaked goroutine or a missed timer in the real shell is outside what the simulator can find. The chaos test covers the real shell, with real processes, signals and a fault-injecting proxy, and the end-to-end and broker tests cover its wiring.
- **PostgreSQL's behaviour is not simulated.** Locking, `SKIP LOCKED`, deadlocks and the planner's choices are exercised only by the tests that use a real database.
- **A pass is evidence, not proof.** The simulator and the property tests explore many orders and cannot visit all of them; the chaos test samples a few thousand jobs. What they have not been told to try, they cannot find. Fuzzing is bounded in time.
