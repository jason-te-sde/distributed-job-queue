# Benchmarks

What was measured, how, and what it means. Every performance figure that the README or any other document quotes comes from this file, with the command that produced it and the raw output under `docs/benchmarks/`.

The measurements were taken on 2026-10-31, on the code of commit `8f72a3c` (the documentation commits on top of it change no code). The harness is `jobqueue-bench`, in the `bench` directory, which is a Go module of its own; `jobqueue-bench <experiment> -h` lists the flags of each experiment.

## Method

- **Machine.** One Apple M2 laptop (8 cores, 16 GB of memory) running macOS 26.6.2, Go 1.27.3, and PostgreSQL 17.10 (Homebrew) on the same machine. The run recorded `sysctl -n machdep.cpu.brand_string hw.memsize` and `pg_config --version`, and dumped the non-default settings of the server (`SELECT name, setting FROM pg_settings WHERE source <> 'default'`) into [`docs/benchmarks/postgresql-settings.txt`](benchmarks/postgresql-settings.txt). `synchronous_commit` is `on`: a job is durable when the enqueue returns. `wal_sync_method` is `open_datasync`, the default on macOS, which leaves the drive's own write cache alone (`fsync_writethrough` is the setting that flushes it), so a commit here is cheaper than on a server disk that flushes on every commit.
- **A quiet machine, as far as one laptop can be.** Experiments ran one at a time. Before each run the harness script waited until the 1-minute load average was 4 or lower (`docs/benchmarks/run-log.txt` has the load before and after every run), which took one to three minutes after the previous run, because a long run itself drives the 1-minute load average to between 8 and 29. A browser and the usual macOS background services were still running. The latency figures are only as good as that: under CPU contention the round trips to the database grow from about 1 ms to hundreds of milliseconds.
- **Every run starts on a database that was just created and migrated.** The harness migrates the schema itself. (The first series of E1, described under E1, reused one database across runs and is not used.)
- **Open-loop load.** The intended send time of job `k` is `start + k / rate`, and the latency of a job is measured **from its intended time**, never from the actual send. A system that stalls then shows the stall in every job that was due during it. A closed loop, which waits for each answer before sending the next, would hide it (this is coordinated omission).
- **HDR histograms.** Latencies are recorded in an HDR histogram, in microseconds, from 1 µs to 1 h with three significant digits, and reported as p50, p90, p99, p99.9 and max.
- **Sustained, and the maximum.** A rate is sustained if the p99 enqueue-to-start latency stays under 1 s and the backlog is finished within 2 s of the end of the load. `--find-max` probes the low end of a range, then bisects between the highest rate that was sustained and the lowest that was not, and reports the highest rate that passed a probe. It then measures at 50% and 80% of that rate. A probe is one run of the load for `--duration`, so the maximum is the highest rate that passed once: near the limit, the same rate passes in one run and fails in the next, and the maximum is an optimistic figure, not a rate that holds every time. The tables below show each run for that reason.
- **Repetition.** Each command runs three times, one after the other. The tables show the three runs ordered by their maximum and labelled min, median and max, so that every figure in a row comes from one run; the raw JSON of all three is committed. **E5 ran once, at 30 minutes:** a soak is a time series, and three 30-minute runs would add an hour without adding a new kind of evidence, so the drift and the table size are judged from its series.
- **Brokers and workers** run in the benchmark process (`internal/broker` in-process) over a real network connection to a real PostgreSQL, with empty handlers unless an experiment says otherwise. One benchmark process holds the brokers, the workers and the load generator, and PostgreSQL runs beside it on the same eight cores.
- **CI numbers are smoke only.** CI runs a short benchmark to catch a harness that no longer works. Its figures are never quoted.

## Experiments

In the commands, `jobqueue-bench` stands for `go run ./cmd/jobqueue-bench`, run in the `bench` directory with `JOBQUEUE_DATABASE_URL` set to the database of the run (`--database-url` does the same); every command was also given `--out <file>`, which appends the JSON to a file. The parameters that the commands leave open in the design are fixed as follows: `<D>` = `30s`, `<B>` = `64` bytes, `<C>` = `64` handlers per worker (with the default of 4 workers, 256 handler slots in all). `--payload-bytes` has a floor of 16: the first 16 bytes of every payload carry the stamp that the harness reads the latency from.

| | Question | Command |
| --- | --- | --- |
| E1 | What is the highest sustained throughput with 1 and with 3 brokers, and what is the enqueue-to-start latency at 50% and 80% of it? | `jobqueue-bench throughput --brokers 1 --find-max --lo 500 --hi 8000 --steps 5 --duration 30s --payload-bytes 64 --concurrency 64`, which searches for the highest rate that holds and then measures at 50% and 80% of it; the same with `--brokers 3`. `--rate <R>` offers one fixed load instead |
| E2 | What does each optimisation buy? Batched leases (1 vs 64 jobs per call), coalesced acknowledgements (off vs the 2 ms or 128 window), `NOTIFY` against polling alone (the latency of an enqueue on an idle queue) | `jobqueue-bench ablate --what batch --find-max --lo 500 --hi 8000 --steps 5 --duration 30s --payload-bytes 64 --concurrency 64`, the same with `--what coalesce`; `jobqueue-bench ablate --what notify --rate 20 --duration 60s --payload-bytes 64 --concurrency 64` (the brokers of the second variant start with `--disable-listen`) |
| E3 | How long does a small tenant wait while another floods the queue, under first-come-first-served and under deficit round robin? Tenant A enqueues 100,000 jobs, tenant B 10 per second; each handler takes `--service-time` (2 ms by default), which sets the capacity that the flood exceeds | `jobqueue-bench noisy --scheduler fifo`, `jobqueue-bench noisy --scheduler drr` (defaults: 100,000 flood jobs, 10 jobs per second for B for 30 s, 2 ms per job, 2 workers with 4 handlers each, 64-byte payloads), then `jobqueue-bench plot --in noisy-fifo.json --in noisy-drr.json --out docs/benchmarks/noisy-neighbor.svg` on the median runs |
| E4 | How long until the jobs of a killed worker run again, against the bound TTL + reap interval + first backoff (up to 1 s) + dispatch latency? How long is the error window that workers see when a broker dies? | `jobqueue-bench recovery --kill worker` (the worker holds 100 leases, TTL 30 s, margin 3 s), `jobqueue-bench recovery --kill broker` (3 brokers, 200 jobs per second for 20 s, one broker cut off at 10 s) |
| E5 | Does throughput drift, and does the size of `jobqueue.jobs` plateau, over 30 minutes at half the maximum rate, with pruning on? | `jobqueue-bench soak --duration 30m --rate 1890 --payload-bytes 64 --concurrency 64`: 1890 is half of E1's median maximum for one broker. Finished jobs are kept for 5 minutes instead of the production 24 hours and pruned every minute, so that the pruner reaches its steady state inside the run |
| E6 | How does it compare with River on the same machine and PostgreSQL? River runs as a library with no network hop, so this is a reference point and not a comparison of like with like | `jobqueue-bench river --find-max --lo 500 --hi 20000 --steps 6 --duration 30s --payload-bytes 64` (River v0.50.0, `MaxWorkers` 256, `FetchCooldown` 1 ms; River's own default is 100 ms) |

E3 is the experiment behind the noisy-neighbour graph: the empirical distribution of tenant B's wait under the two schedulers.

E5 also tests a design choice. The table's `fillfactor` and autovacuum settings in `internal/store/pgstore/migrate/sql/0001_init.sql` are meant to keep its size level under churn. If the table keeps growing, the settings change in a new migration, with the reason, and the soak is run again.

## Results

In the tables, a rate marked with `*` is a phase that did not meet the sustained criterion (p99 under 1 s and the backlog finished within 2 s). The raw JSON of every run is in `docs/benchmarks/`; each result carries the machine, the PostgreSQL settings and the storage options of the jobs table.

| Experiment | Result |
| --- | --- |
| E1 | One broker: the highest sustained rate was 3,781 jobs per second (median of three runs; 2,844 to 4,250), and at half of it the enqueue-to-start latency was 0.97 ms at p50 and 19 ms at p99. Three brokers: 4,250 jobs per second (4,250 to 6,125) |
| E2 | Batched leases: 1.6 times the rate (4,250 against 2,609 jobs per second with one job per call). Coalesced acknowledgements: 1.4 times (3,312 against 2,375). `NOTIFY`: a job on an idle queue starts after 5.4 ms at p50, against 498 ms with polling alone |
| E3 | Tenant B's wait behind a flood of 100,000 jobs: p99 28.5 s under FIFO, 18 ms under deficit round robin (p50 15.4 s against 2.6 ms) |
| E4 | A killed worker's 100 jobs all started again about 32 s after the kill (31.96 to 31.97 s; bound 33 s, TTL 30 s). A broker cut off under load: 1 to 3 failed `Lease` calls within under a millisecond, and 4,000 of 4,000 jobs started |
| E5 | 30 minutes at 1,890 jobs per second, run once: throughput did not drift (+0.17%), but the table did not plateau: it was level at 433 to 442 MiB from minute 15 to 27, then grew to 569 MiB at minute 30 and was still growing |
| E6 | River in library mode: 10,250 jobs per second (10,250 to 11,469), 2.7 times one broker |

### E1: throughput and latency

One broker. The first column after the file name is the highest sustained rate that run found; the next two groups are the latencies measured at 50% and at 80% of it.

| Run | File | Maximum (jobs/s) | 50%: jobs/s | p50 | p99 | p99.9 | 80%: jobs/s | p50 | p99 | p99.9 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| min | `e1-1broker-run2.json` | 2,844 | 1,422 | 0.71 ms | 2.6 ms | 22 ms | 2,275 | 1.4 ms | 46 ms | 89 ms |
| median | `e1-1broker-run1.json` | 3,781 | 1,891 | 0.97 ms | 19 ms | 42 ms | 3,025 | 1.4 ms | 66 ms | 81 ms |
| max | `e1-1broker-run3.json` | 4,250 | 2,125 | 1.3 ms | 12 ms | 19 ms | 3,400 \* | 3.28 s | 4.51 s | 4.55 s |

Three brokers.

| Run | File | Maximum (jobs/s) | 50%: jobs/s | p50 | p99 | p99.9 | 80%: jobs/s | p50 | p99 | p99.9 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| min | `e1-3brokers-run2.json` | 4,250 | 2,125 | 25 ms | 381 ms | 403 ms | 3,400 \* | 109 ms | 1.92 s | 2.09 s |
| median | `e1-3brokers-run3.json` | 4,250 | 2,125 | 2.8 ms | 128 ms | 162 ms | 3,400 \* | 3.68 s | 6.64 s | 6.67 s |
| max | `e1-3brokers-run1.json` | 6,125 | 3,062 \* | 1.23 s | 4.35 s | 4.50 s | 4,900 \* | 12.33 s | 26.94 s | 27.05 s |

One broker sustained a median maximum of 3,781 jobs per second (2,844 to 4,250 over the three runs). At half of that rate the median run's enqueue-to-start latency was 0.97 ms at p50, 19 ms at p99 and 42 ms at p99.9. The 50% phase met the sustained criterion in all three runs, and the 80% phase in two of three: it did not in the run with the highest maximum, whose 80% phase offered 3,400 jobs per second and saw a p99 of 4.51 s.

Three brokers did not sustain three times the rate: the median maximum was 4,250 jobs per second (4,250 to 6,125), and the 80% phase met the criterion in none of the three runs, nor did the 50% phase of the run that found 6,125. The brokers, the workers, the load generator and PostgreSQL share the same eight cores, so a second and third broker add ways in for the load but no capacity behind them. The profile below says what does and does not limit the rate.

A first series, run before the search range was narrowed, used the default range of 100 to 20,000 jobs per second in eight steps on one database that was reused from run to run. Its maxima were 5,075, 3,598 and 2,510 jobs per second for one broker and 2,588, 2,743 and 2,743 for three. Its first real probe offers 10,050 jobs per second, two to three times what the brokers sustain, which leaves 300,000 jobs of backlog and the deletion of them in front of the probes that follow; its spread was wider, and the only run that began on a fresh database found the highest maximum. I could not tell which of the two caused that: one fixed rate, 5,075 jobs per second, passed on both of two fresh databases and on one of two reused ones (`profile/reused-vs-fresh-*.json`), which is too few runs to say. So the range was narrowed and every run was made to start on a new database. The first series is kept in `docs/benchmarks/exploratory/` and is used in no figure.

### E2: what each optimisation buys

Batched leases and coalesced acknowledgements: the highest sustained rate with the feature on and off, in the same run, on the same database. The search has five steps over 500 to 8,000 jobs per second, so a maximum is one of a grid of values 234 jobs per second apart. That all three runs of `max_jobs_1` report 2,609 means that each passed 2,609 and failed the next value it probed above, 2,844.

| Variant | Maximum (jobs/s): median (min to max) | Run 1 | Run 2 | Run 3 |
| --- | ---: | ---: | ---: | ---: |
| `max_jobs_64` | 4,250 (4,250 to 5,422) | 5,422 | 4,250 | 4,250 |
| `max_jobs_1` | 2,609 (2,609 to 2,609) | 2,609 | 2,609 | 2,609 |

| Variant | Maximum (jobs/s): median (min to max) | Run 1 | Run 2 | Run 3 |
| --- | ---: | ---: | ---: | ---: |
| `ack_batch_128` | 3,312 (3,312 to 4,250) | 3,312 | 4,250 | 3,312 |
| `ack_batch_1` | 2,375 (2,375 to 2,844) | 2,844 | 2,375 | 2,375 |

`NOTIFY` against polling alone, on an idle queue (20 jobs per second, 60 s, so a job arrives after the last one has long started). Each cell is the median of the three runs, with the minimum and the maximum in brackets.

| Variant | p50 | p90 | p99 | p99.9 | max |
| --- | ---: | ---: | ---: | ---: | ---: |
| `listen_on` | 5.4 ms (5.4 ms to 5.6 ms) | 6.3 ms (6.3 ms to 7.0 ms) | 8.2 ms (8.0 ms to 8.7 ms) | 16 ms (10 ms to 52 ms) | 55 ms (14 ms to 66 ms) |
| `listen_off` | 498 ms (497 ms to 498 ms) | 898 ms (898 ms to 898 ms) | 995 ms (994 ms to 996 ms) | 997 ms (997 ms to 998 ms) | 1.00 s (998 ms to 1.04 s) |

Batched leases bought a factor of 1.6 in the median: with one job per `Lease` call the highest sustained rate was 2,609 jobs per second in all three runs, against 4,250 (4,250 to 5,422) with up to 64 jobs per call. Coalesced acknowledgements bought a factor of 1.4: 2,375 (2,375 to 2,844) with one acknowledgement per call, against 3,312 (3,312 to 4,250) with the 2 ms or 128-item window. The two "on" rows are the same configuration, and they differ by the spread that E1 shows, so compare the on and the off of one ablation, which ran in the same process one after the other, and not the rows of the two.

`NOTIFY` is what makes an idle queue fast: a job arriving on an idle queue started after 5.4 ms at p50 and 8.2 ms at p99 with the listener, and after 498 ms and 995 ms with polling alone, where a job waits for the next of the 1-second fallback polls. The scratch copy of the profile below, which sends no notification on enqueue, shows the same effect at 3,000 jobs per second: p50 343 ms against 1.2 ms for the tree as it is.

### E3: a small tenant behind a flood

Tenant B's wait, in each run, over its 300 jobs (so p99.9 and max are one and the same job). Each cell is the median of three runs, with the minimum and the maximum in brackets.

| Scheduler | B p50 | B p90 | B p99 | B p99.9 and max | A's last job started (s after the start) |
| --- | ---: | ---: | ---: | ---: | ---: |
| `fifo` | 15.36 s (15.12 s to 15.65 s) | 26.13 s (25.79 s to 26.53 s) | 28.52 s (28.10 s to 28.92 s) | 28.82 s (28.38 s to 29.21 s) | 32.0 (32.0 to 32.4) |
| `drr` | 2.6 ms (2.5 ms to 2.6 ms) | 3.7 ms (3.6 ms to 4.4 ms) | 18 ms (17 ms to 25 ms) | 25 ms (21 ms to 26 ms) | 32.8 (32.6 to 33.0) |

Under FIFO tenant B waited behind tenant A's backlog: p50 15.4 s and p99 28.5 s, while the 100,000 flood jobs took about 32 s to be started (the capacity is 4,000 jobs per second: 8 handler slots of 2 ms). Under deficit round robin B's p50 was 2.6 ms and its p99 18 ms (25 ms in the worst run), and A's flood finished about 0.8 s later than under FIFO (32.8 s against 32.0 s): the scheduler put B's jobs ahead and cost A under a second.

![Empirical distribution of tenant B's wait under FIFO and under deficit round robin](benchmarks/noisy-neighbor.svg)

The graph is drawn by `jobqueue-bench plot` from the median run of each scheduler by p99 (`e3-fifo-run1.json` and `e3-drr-run3.json`). The x axis is logarithmic.

### E4: killing a worker and a broker

Killing a worker with `SIGKILL` while it holds 100 leases (TTL 30 s, margin 3 s, one broker):

| Measure | Run 1 | Run 2 | Run 3 |
| --- | ---: | ---: | ---: |
| Jobs held by the killed worker | 100 | 100 | 100 |
| First job started again, after the kill | 31.02 s | 31.02 s | 31.02 s |
| All jobs started again, after the kill | 31.96 s | 31.96 s | 31.97 s |
| Bound: TTL + reap interval + first backoff + dispatch | 33.00 s | 33.00 s | 33.00 s |
| Within the bound | yes | yes | yes |

All 100 jobs started again 31.96 to 31.97 s after the kill, inside the bound of 33 s: the 30 s TTL, the 1 s reap interval, up to 1 s of the first backoff, and 1 s for the dispatch fallback poll. The first job restarted 31.02 s after the kill, and the 0.94 s between the first and the last is the jitter of the backoff. The worker was killed within a second of taking its leases, before its first heartbeat, so the leases ran their whole TTL after the kill. That is the slowest case: a worker that dies after a heartbeat leaves less than a TTL, so about 32 s is the upper end of the recovery time for a TTL of 30 s.

Cutting a broker off, one of three, while 200 jobs per second flow. The harness resets every client connection of that broker and makes its port refuse new ones, while the broker process itself keeps running, so this is a failed network path, not a crashed process.

| Measure | Run 1 | Run 2 | Run 3 |
| --- | ---: | ---: | ---: |
| Client connections reset | 5 | 5 | 5 |
| Worker errors before the cut | 0 | 0 | 0 |
| Worker errors after the cut (all `Lease` calls) | 3 | 1 | 2 |
| First error after the cut | 0.64 ms | 0.53 ms | 0.79 ms |
| Error window, first to last error | 0.77 ms | 0.00 ms | 0.26 ms |
| Jobs started of jobs sent | 4000 of 4000 | 4000 of 4000 | 4000 of 4000 |
| Duplicate starts | 0 | 0 | 0 |
| Enqueue-to-start p99 over the run | 4.7 ms | 9.7 ms | 4.4 ms |

Between 1 and 3 `Lease` calls failed, all within a window of under a millisecond that began 0.5 to 0.8 ms after the cut, and none failed before it. No acknowledgement or heartbeat failed, the load went on through the other two brokers, and every job of the load (4,000 of 4,000) started, none of them twice. Because the harness cuts connections instead of crashing the process, this measures how fast clients move to another broker; what a broker that dies in the middle of a transaction leaves behind is not measured here.

### E5: a half-hour soak

The soak offered 1,890 jobs per second for 30 minutes, keeping finished jobs for 5 minutes and pruning every minute. It ran once (see Method). All 3,402,000 jobs that were sent started, none of them twice.

**Throughput did not drift.** The mean of the first five one-minute samples was 1,886.8 jobs per second and of the last five 1,890.0, which is +0.17%.

**The table did not plateau.** The number of rows settled at 610,000 to 680,000 from minute 6 and the heap at 254.6 to 255.7 MiB from minute 7 to minute 28, while the indexes grew from 126 MiB at minute 7 to 186 MiB at minute 27, in uneven steps and by about 0.7 MiB a minute from minute 15 on. Table plus indexes was 381 MiB at minute 7, 433 MiB at minute 15 and 442 MiB at minute 27. Then it broke away: in minutes 28 to 30 it grew by 127 MiB (the heap from 255.7 to 343.8 MiB, the indexes from 186 to 225 MiB), 75 MiB and 43 MiB in the last two minutes, and the run ended at its largest size, 569 MiB. Thirty minutes is too short to say whether it would have settled at a higher level. Autovacuum ran throughout (29 runs on the table by the end, from `pg_stat_user_tables`), but the dead tuples swing between about 17,000 and 950,000, and they stood at 570,000 to 950,000 in the last four minutes: vacuum keeps up in cycles, not steadily. The interval p99 followed: 10 to 41 ms in the first six minutes, mostly under 200 ms in the middle (with spikes of 0.45 s, 1.5 s and 2.5 s), and 0.6 to 0.9 s in the last four, when the p50 rose from about 2 ms to between 18 and 97 ms. The profile runs show the autovacuum worker waiting in its cost-based throttle (`Timeout/VacuumDelay`, 0.3 backends on average at 6,000 jobs per second in `profile/wait-events.txt`).

**The server checkpointed on WAL volume throughout.** With the default `max_wal_size` of 1 GB it started a checkpoint every 70 to 80 seconds (`checkpoint starting: wal` in the server log). That is a candidate for the intervals whose p99 is a second or more (minutes 16 and 21); it was not shown to be the cause.

**Verdict: the size of `jobqueue.jobs` did not plateau in this run**, so this soak does not validate the `fillfactor` and autovacuum settings of `0001_init.sql`. The question is open. No migration was written and the soak was not run again.

| Minute | Jobs/s | p50 | p99 | Table + indexes (MiB) | Rows | Dead tuples |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1,874 | 0.93 ms | 10.0 ms | 78.4 | 112,462 | 194,390 |
| 2 | 1,890 | 1.0 ms | 15 ms | 122.2 | 225,857 | 191,183 |
| 3 | 1,890 | 1.0 ms | 20 ms | 171.7 | 339,258 | 191,752 |
| 4 | 1,890 | 1.0 ms | 20 ms | 224.7 | 452,655 | 189,668 |
| 5 | 1,890 | 1.1 ms | 30 ms | 274.6 | 566,055 | 187,497 |
| 6 | 1,890 | 1.1 ms | 41 ms | 321.9 | 679,451 | 186,747 |
| 7 | 1,890 | 1.2 ms | 86 ms | 380.8 | 673,947 | 182,504 |
| 8 | 1,890 | 1.4 ms | 76 ms | 384.3 | 674,575 | 172,193 |
| 9 | 1,890 | 1.1 ms | 50 ms | 386.0 | 674,477 | 154,245 |
| 10 | 1,890 | 2.9 ms | 42 ms | 402.2 | 614,540 | 301,733 |
| 11 | 1,890 | 1.7 ms | 92 ms | 415.1 | 652,523 | 203,216 |
| 12 | 1,890 | 2.3 ms | 74 ms | 416.0 | 630,082 | 268,875 |
| 13 | 1,889 | 1.9 ms | 67 ms | 421.3 | 644,576 | 224,670 |
| 14 | 1,890 | 28 ms | 454 ms | 425.1 | 621,638 | 589,200 |
| 15 | 1,891 | 1.9 ms | 188 ms | 433.4 | 645,397 | 19,985 |
| 16 | 1,890 | 2.5 ms | 1.46 s | 433.8 | 629,295 | 274,831 |
| 17 | 1,890 | 1.8 ms | 73 ms | 435.9 | 646,872 | 17,127 |
| 18 | 1,890 | 2.1 ms | 99 ms | 436.4 | 634,906 | 35,403 |
| 19 | 1,890 | 2.0 ms | 165 ms | 437.3 | 641,319 | 240,099 |
| 20 | 1,890 | 2.1 ms | 76 ms | 438.0 | 631,048 | 48,678 |
| 21 | 1,890 | 2.2 ms | 2.46 s | 438.7 | 633,975 | 22,147 |
| 22 | 1,890 | 1.9 ms | 146 ms | 439.4 | 639,496 | 586,280 |
| 23 | 1,884 | 2.1 ms | 202 ms | 439.9 | 635,903 | 36,880 |
| 24 | 1,896 | 2.2 ms | 312 ms | 440.6 | 633,857 | 591,959 |
| 25 | 1,890 | 2.0 ms | 98 ms | 441.1 | 639,732 | 587,187 |
| 26 | 1,890 | 1.9 ms | 67 ms | 441.4 | 637,257 | 321,115 |
| 27 | 1,889 | 18 ms | 881 ms | 442.0 | 630,161 | 589,056 |
| 28 | 1,883 | 97 ms | 635 ms | 451.0 | 632,369 | 586,177 |
| 29 | 1,897 | 75 ms | 691 ms | 525.8 | 614,249 | 945,895 |
| 30 | 1,891 | 28 ms | 581 ms | 569.3 | 628,950 | 571,943 |

### E6: River as a reference

| Run | File | Maximum (jobs/s) | 50%: jobs/s | p50 | p99 | p99.9 | 80%: jobs/s | p50 | p99 | p99.9 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| min | `e6-river-run2.json` | 10,250 | 5,125 | 3.3 ms | 88 ms | 146 ms | 8,200 | 35 ms | 527 ms | 604 ms |
| median | `e6-river-run3.json` | 10,250 | 5,125 | 1.4 ms | 928 ms | 1.13 s | 8,200 \* | 17 ms | 1.53 s | 1.60 s |
| max | `e6-river-run1.json` | 11,469 | 5,734 | 6.8 ms | 693 ms | 850 ms | 9,175 \* | 5.71 s | 11.32 s | 11.44 s |

River's median maximum was 10,250 jobs per second (10,250 to 11,469), 2.7 times the 3,781 of one broker. The difference includes the network hop from the producer to the broker and from the worker to the broker, which River in library mode does not have, the lease with a fencing token that every job here gets, and fair scheduling across tenants; this experiment did not separate them. It is a reference point, not a comparison of like with like. River's 80% phase met the criterion in one of three runs, and the brokers' in two of three.

### What did not match the design: the notify lock

**What was expected.** PostgreSQL commits the transactions that run `pg_notify` one at a time: each takes a database-wide lock at its commit (locktype `object`, classid 1262) and keeps it across the flush of the WAL, which is why `internal/store/pgstore/gate.go` bounds the connections that such transactions hold. Enqueue is such a transaction, so E1's maximum was expected to sit near the ceiling that this lock sets. It does not. The profile is below; the files are in `docs/benchmarks/profile/`.

**Step 1, the ceiling.** The Enqueue statement of the store, as it is and with its `pg_notify` replaced by `SELECT 1`, run by `pgbench` against the migrated schema with no broker running (`pgbench -n -c <clients> -j 4 -T 10 -f <script>`, with `profile/enq-notify.sql` and `profile/enq-nonotify.sql`):

| Clients | Enqueues per second, with `pg_notify` | Without |
| ---: | ---: | ---: |
| 1 | 4,647 | 4,911 |
| 4 | 14,925 | 16,430 |
| 8 | 14,787 | 22,780 |
| 16 | 16,393 | 22,580 |
| 45 | 16,194 | 30,034 |

With the notification the rate stops growing at four clients, at about 15,000 commits per second; without it, it keeps growing to between 22,000 and 30,000. The lock does set a ceiling, and on this machine it is about 15,000 enqueues per second.

**Step 2, E1 is far below it.** E1's maxima for one broker, 2,844 to 4,250 jobs per second, are about a quarter of that. During 30-second fixed-rate runs at 1,500, 3,000 and 6,000 jobs per second (`throughput --brokers 1 --rate <R> --duration 30s`), `pg_stat_activity` was sampled every 0.25 s (`profile/wait-events.txt`). Averaged over every sample of a run, drain included, the number of backends waiting for the notify lock (wait event `Lock`/`object`) was 0 at 1,500 jobs per second, 0.45 at 3,000 and 2.6 at 6,000, while the number of active backends running on a CPU, with no wait event, went from 1.4 to 6.8 to 14.7, on 8 cores. In a separate 20-second run at 6,000 jobs per second, `pg_locks` sampled every 0.25 s showed exactly one granted `AccessExclusiveLock` on locktype `object`, classid 1262, objid 0, and on average 11 waiting for it (up to 13) in the 70 samples in which the lock was in use (`profile/pg-locks-6000.txt`): there the queue behind the lock is close to the 15 connections that the gate lets notifying calls hold. At 3,000 jobs per second, below E1's median maximum, the queue was short (0.45 on average) and the lock passed 3,000 commits per second, a fifth of its ceiling.

**Step 3, removing the lock from Enqueue does not move the knee.** A scratch copy of the tree in which the `pg_notify` of Enqueue is replaced by `SELECT 1` (`profile/no-enqueue-notify.patch`; it was never part of the code), with one broker and 30-second fixed rates. At 6,000 jobs per second offered it delivered 4,129 jobs per second, against 3,514 for the tree as it is; at 10,050 it delivered 3,147, against 3,139 to 4,546 in five runs of the tree as it is and 3,657 to 5,362 in the six probes of the first series of E1 at that rate. Its wait events show no wait on the lock and, in its place, waits for the WAL write lock; the mean number of backends running on a CPU was 7.1, 13.9 and 15.9 at 3,000, 6,000 and 10,050 jobs per second.

**What follows, and what was not shown.** On this machine the enqueue-side notify lock is not what limits the system at the rates it reaches: taking it out did not raise the delivered rate beyond the spread of the tree as it is. What the profile does show is a machine that is out of CPU at and past the knee: at 6,000 jobs per second and above, 14 to 16 backends run on 8 cores, `top` puts the PostgreSQL processes at about 0.9, 2.1 and 3.9 cores and the benchmark process at about 0.7, 0.9 and 1.0 cores at 1,500, 3,000 and 6,000 jobs per second, and beyond the knee the delivered rate falls below what a probe just under it sustained (2,633 to 3,907 jobs per second delivered at offers of 5,153 to 7,562 in the first series, against 4,999 sustained at 5,075). I did not isolate the CPU as the cause, which would take a CPU profile of PostgreSQL that this run did not make, and I do not explain the factor of 1.5 between runs of the same command.

### Duplicate starts

Every result carries `duplicate_starts`, the number of times a job that had already started was started again. Across the 299 results in `docs/benchmarks/` it is 0 in 293 and non-zero in six. All six are probes that did not meet the sustained criterion, at rates above the maximum found for that configuration, and each was far behind: it delivered 904 to 3,147 jobs per second against 4,250 to 10,050 offered, and its longest wait was 38 to 111 s. None is a sustained probe, a 50% or 80% phase, an E3, E4 or E2 notify run, or the soak (3,402,000 jobs sent, 3,402,000 started, none twice). Below saturation it was 0 everywhere.

The six are `e1-3brokers-run1.json` (6,359 jobs per second offered, 1,483 duplicates), `e1-3brokers-run3.json` (4,484, 22,975), `e2-coalesce-run2.json` (`ack_batch_1` at 4,250, 58,978), `exploratory/e1-wide-range-reused-db-1broker-run1.json` (7,562, 28,784), and two fixed-rate profile runs at 10,050 (`profile/fixed-rate-no-enqueue-notify-10050.json`, 67,103; `profile/reused-vs-fresh-overload-between.json`, 46,314).

The cause is not established. A producer call that times out and is retried can create a second job if the first one was pruned in between, but these probes keep finished jobs for 24 hours, so nothing was pruned. The other candidate is that a lease ran out and the job was started again. Every one of the six had a longest wait above the 30 s lease TTL, which fits, but other overloaded probes with waits as long had no duplicate, and four further probes at 10,050 jobs per second on fresh databases, with the table sampled every 3 to 4 s (`profile/duplicate-diagnostic-run*.json` and `-rows.txt`), had none. There is therefore no row-level evidence of whether those jobs were started again after their lease ran out or were created twice.

## How to reproduce

1. Use a machine you can leave alone, with a local PostgreSQL 15 to 18 and `synchronous_commit = on`. Quiet other work.
2. For every run, create an empty database (`createdb jobqueue_bench`; drop and create it again for the next run). The harness migrates the schema itself, with the same migrations as `jobqueuectl migrate`. Point it at the database with `--database-url` or `$JOBQUEUE_DATABASE_URL`.
3. Record the machine and the server settings as described under Method.
4. From the `bench` directory (it is a separate Go module): `go run ./cmd/jobqueue-bench <experiment> ...`, three times each, with the commands in the table above. One JSON line per result goes to standard output and a summary to standard error; `--out <file>` appends the JSON to a file.
5. Commit the raw outputs next to this file and update the tables from the median runs.

## Caveats

- One laptop, with PostgreSQL on the same machine: no network between the broker and the database, and the benchmark process shares the CPU with both. Figures on a server with a remote database will differ.
- The M2 is the only hardware measured. Nothing here is a claim about other hardware.
- The maxima are the highest rate that passed one probe, and they move by up to a factor of 1.5 between runs of the same command (E1). The latency figures are meaningful on a quiet machine and not otherwise.
- The server flushes the WAL the way the macOS default does (see Method), which makes a commit cheaper than on a server disk that flushes on every commit, and therefore makes the notify lock cheaper than it would be there: the ceiling of about 15,000 commits per second below is not a figure for a server.
- PostgreSQL serializes every transaction that commits a `NOTIFY` (see the section above), so a workload of many small enqueues has a ceiling of its own on any hardware; on this machine it measured about 15,000 commits per second for the enqueue statement alone, which the whole system did not reach.
- CI figures are smoke only.
- River runs in library mode, with no network hop and no broker.
- Results are for the defaults named in the commands (lease 30 s, margin 3 s, quantum 8). Other settings were not measured.
- `--payload-bytes` has a floor of 16, because every payload starts with a 16-byte stamp. The payloads of the runs are 64 bytes; larger payloads were not measured.

## Files

Everything under `docs/benchmarks/`:

- `e1-1broker-run{1,2,3}.json`, `e1-3brokers-run{1,2,3}.json`: E1.
- `e2-batch-run*.json`, `e2-coalesce-run*.json`, `e2-notify-run*.json`: E2.
- `e3-fifo-run*.json`, `e3-drr-run*.json`, `noisy-neighbor.svg`: E3 and its graph.
- `e4-kill-worker-run*.json`, `e4-kill-broker-run*.json`: E4.
- `e5-soak.json`: E5, one line per minute and a summary.
- `e6-river-run*.json`: E6.
- `postgresql-settings.txt`: the machine and the server settings.
- `run-log.txt`: every run with its command as run, the time it started and took, and the 1-minute load average before and after it.
- `profile/`: the notify-lock profile (the statement, the scripts, the wait events, the patch of the scratch copy, and the fixed-rate runs).
- `exploratory/`: the first series of E1, which is not used (see E1).
