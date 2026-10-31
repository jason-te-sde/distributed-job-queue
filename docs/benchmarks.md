# Benchmarks

What was measured, how, and what it means. Every performance figure that the README or any other document quotes comes from this file, with the command that produced it and the raw output under `docs/benchmarks/`.

This file holds the method and the commands. The measurements are taken by the benchmark run and written into the Results section below. The harness is `jobqueue-bench`, in the `bench` directory, which is a Go module of its own; `jobqueue-bench <experiment> -h` lists the flags of each experiment.

## Method

- **Machine.** One Apple M2 laptop with 16 GB of memory, other heavy applications closed, PostgreSQL 17 on the same machine. The run records `sysctl -n machdep.cpu.brand_string hw.memsize`, `uptime` and `pg_config --version`, and dumps the non-default settings of the server (`SELECT name, setting FROM pg_settings WHERE source <> 'default'`) into `docs/benchmarks/postgresql-settings.txt`. `synchronous_commit` is `on`: a job is durable when the enqueue returns.
- **Open-loop load.** The intended send time of job `k` is `start + k / rate`, and the latency of a job is measured **from its intended time**, never from the actual send. A system that stalls then shows the stall in every job that was due during it. A closed loop, which waits for each answer before sending the next, would hide it (this is coordinated omission).
- **HDR histograms.** Latencies are recorded in an HDR histogram, in microseconds, from 1 µs to 1 h with three significant digits, and reported as p50, p90, p99, p99.9 and max.
- **Repetition.** Each command runs three times. The table reports the median run, with the minimum and the maximum of the three, and all three raw JSON outputs are committed.
- **Brokers and workers** run in the benchmark process (`internal/broker` in-process) over a real network connection to a real PostgreSQL, with empty handlers unless an experiment says otherwise.
- **CI numbers are smoke only.** CI runs a short benchmark to catch a harness that no longer works. Its figures are never quoted.

## Experiments

| | Question | Command |
| --- | --- | --- |
| E1 | What is the highest sustained throughput with 1 and with 3 brokers, and what is the enqueue-to-start latency at 50% and 80% of it? | `jobqueue-bench throughput --brokers 1 --find-max --duration <D> --payload-bytes <B> --concurrency <C>`, which searches for the highest rate that holds and then measures at 50% and 80% of it; the same with `--brokers 3`. `--rate <R>` offers one fixed load instead |
| E2 | What does each optimisation buy? Batched leases (1 vs 64 jobs per call), coalesced acknowledgements (off vs the 2 ms or 128 window), `NOTIFY` against polling alone (the latency of an enqueue on an idle queue) | `jobqueue-bench ablate --what batch`, `--what coalesce`, `--what notify` (the last starts the broker with `--disable-listen`) |
| E3 | How long does a small tenant wait while another floods the queue, under first-come-first-served and under deficit round robin? Tenant A enqueues 100,000 jobs, tenant B 10 per second; each handler takes `--service-time` (2 ms by default), which sets the capacity that the flood exceeds | `jobqueue-bench noisy --scheduler fifo`, `jobqueue-bench noisy --scheduler drr`, then `jobqueue-bench plot --in noisy-fifo.json --in noisy-drr.json --out docs/benchmarks/noisy-neighbor.svg` |
| E4 | How long until the jobs of a killed worker run again, against the bound TTL + reap interval + first backoff (up to 1 s) + dispatch latency? How long is the error window that workers see when a broker dies? | `jobqueue-bench recovery --kill worker`, `jobqueue-bench recovery --kill broker` |
| E5 | Does throughput drift, and does the size of `jobqueue.jobs` plateau, over 30 minutes at half the maximum rate, with pruning on? | `jobqueue-bench soak --duration 30m --rate <50% of the maximum>` |
| E6 | How does it compare with River on the same machine and PostgreSQL? River runs as a library with no network hop, so this is a reference point and not a comparison of like with like | `jobqueue-bench river --find-max --duration <D>` |

E3 is the experiment behind the noisy-neighbour graph: the empirical distribution of tenant B's wait under the two schedulers.

E5 also tests a design choice. The table's `fillfactor` and autovacuum settings in `internal/store/pgstore/migrate/sql/0001_init.sql` are meant to keep its size level under churn. If the table keeps growing, the settings change in a new migration, with the reason, and the soak is run again.

## Results

**Results will be filled in by the benchmark run.**

Nothing below is a measurement until that run has written it. The run produces, for each experiment, a table of the median of three runs with the minimum and maximum, a sentence on what the result means, and the raw output files under `docs/benchmarks/`. It also adds the machine description and the PostgreSQL settings, embeds the noisy-neighbour graph, and records a profile and its finding for any result that contradicts the design.

| Experiment | Will report |
| --- | --- |
| E1 | maximum sustained throughput; p50, p99 and p99.9 enqueue-to-start latency at 50% and 80% of it; 1 broker and 3 brokers |
| E2 | the effect of batched leases, coalesced acknowledgements and `NOTIFY` |
| E3 | tenant B's p50, p99 and maximum wait under FIFO and under DRR, and the graph |
| E4 | time until a killed worker's jobs run again, against the bound; the error window of a killed broker |
| E5 | throughput and table size per minute over 30 minutes |
| E6 | River's throughput on the same machine |

## How to reproduce

1. Use a machine you can leave alone, with a local PostgreSQL 15 to 18 and `synchronous_commit = on`. Quiet other work.
2. Run the migrations against an empty database (`jobqueuectl migrate`) and point the harness at it with `--database-url` or `$JOBQUEUE_DATABASE_URL`.
3. Record the machine and the server settings as described under Method.
4. From the `bench` directory (it is a separate Go module): `go run ./cmd/jobqueue-bench <experiment> ...`, three times each, with the commands in the table above, and the parameters (`<D>`, `<B>`, `<C>`) written down next to each result. One JSON line per result goes to standard output and a summary to standard error; `--out <file>` appends the JSON to a file.
5. Commit the raw outputs next to this file and update the tables from the median runs.

## Caveats

- One laptop, with PostgreSQL on the same machine: no network between the broker and the database, and the benchmark process shares the CPU with both. Figures on a server with a remote database will differ.
- The M2 is the only hardware measured. Nothing here is a claim about other hardware.
- CI figures are smoke only.
- River runs in library mode, with no network hop and no broker.
- Results are for the defaults named in the commands (lease 30 s, margin 3 s, quantum 8). Other settings were not measured.
