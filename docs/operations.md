# Operations

How to run the queue, and what to do when it misbehaves. The second half is written to be read at 3 a.m.: each symptom says what it means, which metric or command confirms it, and what to do.

Contents: [Running it](#running-it), [The symptoms](#the-symptoms), [Reference](#reference).

## Running it

### What you run

- **Brokers**: `jobqueue-server`, two or more of them, all pointing at the same PostgreSQL database. They are interchangeable and keep no job state.
- **PostgreSQL** 15 to 18, in a database of its own. Long transactions from other applications in the same database hold back vacuum for the queue's table.
- **Workers**: your processes, built on the SDK.
- Optionally **KEDA**, to scale workers on the backlog.

### Migrate before you start

```sh
jobqueuectl migrate --database-url "$JOBQUEUE_DATABASE_URL"     # prints: applied=1 version=1
```

Migrations are forward-only and take an advisory lock, so running them from several places at once is safe. A broker checks the schema version when it starts and exits if it differs, with `database schema version X, this binary needs Y: run jobqueuectl migrate`. The Helm chart runs the migration as a pre-install and pre-upgrade hook.

### Tokens

Callers authenticate with static bearer tokens. The broker holds only their SHA-256 hashes, in a file that you pass with `--tokens-file`. Mint a token and its line with the CLI; the token is shown once and only the line goes into the file.

```sh
jobqueuectl token new --role producer --tenant acme          # for the code that enqueues for tenant acme
jobqueuectl token new --role worker --queues thumbnails,video # for workers of those queues
jobqueuectl token new --role admin                           # for operators
```

Each prints `token: ...` and `line: ...`. The file is one line per token, with `#` comments and blank lines allowed:

```
producer tenant=acme    <sha256 hex of the token>
worker   queues=thumbnails,video  <sha256 hex of the token>
admin    -              <sha256 hex of the token>
```

| Role | May call |
| --- | --- |
| `producer` (bound to a tenant) | Enqueue, GetJob, CancelJob, for its own tenant only. A job of another tenant is reported as not found |
| `worker` (bound to queues) | Lease, Heartbeat, Ack, Release, on its queues only |
| `admin` | everything, including GetStats, ListDeadJobs, RequeueJob and PurgeJobs |

A malformed line, or a hash that appears twice, makes the broker refuse to start; it never falls back to an open API. Clients send the token with `jobqueuectl --token` or `$JOBQUEUE_TOKEN` (prefer the variable: a flag shows in the process list) and in the SDK with `jobqueue.WithToken`, which also requires `WithTLS` unless the caller chose `WithInsecure`.

### TLS and the insecure flag

Give the broker `--tls-cert` and `--tls-key` (both or neither). Clients use `jobqueue.WithTLS` and, for a private CA, `jobqueuectl --tls-ca file`. On a loopback address (`127.0.0.1`, `::1`, `localhost`) a broker needs neither TLS nor tokens, and without a tokens file every caller is an admin. On any other address, including `:7070`, which listens everywhere, the broker refuses to start unless it has both TLS and a tokens file, or `--insecure` is given. Use `--insecure` only on a network that you trust completely or in a test.

The HTTP port (`--http-addr`, default `:7071`) serves `/healthz`, `/readyz` and `/metrics` without authentication, because probes and scrapers send no tokens. Do not expose it beyond the cluster.

### On Kubernetes

```sh
kubectl create secret generic jobqueue-database --from-literal=url='postgres://USER:PASSWORD@HOST:5432/jobqueue?sslmode=require'
kubectl create secret generic jobqueue-tokens   --from-file=tokens=./tokens
kubectl create secret tls     jobqueue-tls      --cert=tls.crt --key=tls.key
helm install jobqueue deploy/helm/jobqueue \
  --set tokens.secretName=jobqueue-tokens --set tls.secretName=jobqueue-tls
```

The chart creates no Secrets, because the migration hook runs before anything else. Clients dial the headless Service, which the install notes print. For a release named `jobqueue` in the namespace `apps` it is

```
dns:///jobqueue-grpc.apps.svc.cluster.local:7070
```

and for a release named `prod` it is `prod-jobqueue-grpc` (the release name is prefixed unless it already contains `jobqueue`). The same release also has `<name>-http` (health and metrics) and `<name>-scaler` (KEDA).

The values that matter:

| Value | Default | Meaning |
| --- | --- | --- |
| `replicas` | 2 | Broker replicas. Keep at least 2, so that a rolling restart or a node drain always leaves one |
| `image.repository`, `image.tag` | `ghcr.io/jason-te-sde/distributed-job-queue`, the chart's `appVersion` | The image |
| `database.secretName`, `database.secretKey` | `jobqueue-database`, `url` | The Secret with the connection URL |
| `tokens.secretName` | empty | A Secret with the key `tokens` |
| `tls.secretName` | empty | A Secret of type `kubernetes.io/tls` |
| `insecure` | `false` | Adds `--insecure`. Without it, and without both Secrets, the pods exit at start |
| `scheduler`, `tenantWeights` | `drr`, empty | Fair scheduling, and weights such as `a=4,b=1` |
| `scaler.enabled`, `scaler.port`, `scaler.allowNamespace` | `true`, 7072, `keda` | The KEDA scaler endpoint, and the namespace allowed to reach it |
| `pdb.minAvailable` | 1 | PodDisruptionBudget |
| `terminationGracePeriodSeconds`, `preStopSleepSeconds` | 40, 5 | The chart refuses to render if the sleep plus the 25 s shutdown grace is not less than the grace period |
| `example.enabled`, `example.keda.enabled` | `false` | The thumbnail example worker, and its `ScaledObject` |
| `example.tokenSecret`, `example.tokenKey` | empty | The Secret key with the example worker's bearer token (a worker token for the queue `thumbnails`), required when the brokers require tokens. With `tls.secretName` the TLS Secret also needs a `ca.crt` key, which the worker trusts |
| `migrations.enabled` | `true` | The migration hook |

### Rolling updates and restarts

A broker restart is safe at any time. On SIGTERM a broker fails `/readyz`, refuses new Lease calls (workers move to another broker), answers waiting long polls empty, serves heartbeats and acknowledgements for up to `--shutdown-grace` (25 s) and exits. With two or more brokers and the PodDisruptionBudget, a rolling update drops no call. Workers are safe to stop with SIGTERM too: they stop leasing, wait up to the drain timeout (25 s) for running handlers, and give back the rest with their attempts refunded. Keep `terminationGracePeriodSeconds` for workers above their drain timeout.

### Sizing

- **Database connections.** Each broker holds a pool of `--max-db-conns` (default 20) plus one connection for `LISTEN`. Brokers times 21 must fit in `max_connections` with room for everyone else.
- **The listener needs a real session.** Point `--database-url` at PostgreSQL itself or at a pooler in session mode. Behind a pooler in transaction mode `LISTEN` does not work: notifications never arrive and the fallback poll (1 s) carries all the work, which still functions and adds up to a second of latency.
- **Retention.** Succeeded and cancelled jobs are kept 24 h (`--retention-done`) and dead jobs 14 days (`--retention-dead`). The table's size follows the retention times the finish rate, and an idempotency key is honoured for as long as its row exists.
- **Throughput** is bounded by the one PostgreSQL. [docs/benchmarks.md](benchmarks.md) has the measured figures and the machine.

## The symptoms

Before any of these: is the broker up? `curl -s http://BROKER:7071/readyz` answers `ready` or says why not. Is the database up? Every symptom below starts there if the answer is no.

Useful everywhere:

```sh
jobqueuectl stats --queue Q              # per tenant: ready, scheduled, leased, dead, oldest ready age
jobqueuectl dead list --queue Q          # dead jobs with their last error
```

and, in `psql`:

```sql
SELECT state, count(*), min(run_at) AS oldest_run_at, min(lease_expires_at) AS first_expiry
FROM jobqueue.jobs WHERE queue = 'Q' GROUP BY state;
```

### The queue is stuck

**What it looks like.** Jobs are ready and nothing runs, or the oldest ready job gets older.

**What it can mean, in the order to check.**

1. *The jobs are not due.* `jobqueuectl stats` counts `scheduled` apart from `ready`: a delayed job, or one waiting out a retry backoff (up to an hour for late attempts), is `scheduled` and not stuck.
2. *Nobody is asking.* Metric `jobqueue_waiters{queue="Q"}` is 0 on every broker: no worker is long-polling this queue. Check that workers run, that their queue name is spelled as the jobs' queue, and that their token lists the queue (a worker refused with `PermissionDenied` stops with an error; look at its logs: `the broker refuses this worker's lease requests, stopping`).
3. *The workers are busy.* All their slots hold handlers: `leased` is about workers times concurrency, and `ready` grows. Add workers, or check the next item.
4. *A handler is stuck and its lease is kept alive.* The SDK heartbeats for as long as a handler runs, and nothing ends a handler that never returns. `leased` stays high and flat for long. Find the worker (`SELECT leased_by, count(*) FROM jobqueue.jobs WHERE state = 'leased' AND queue = 'Q' GROUP BY 1`), restart it (a graceful stop gives its jobs back, a kill lets them expire after the TTL), or cancel the job with `jobqueuectl cancel ID`: its worker learns at the next heartbeat and cancels the handler.
5. *The brokers cannot fetch.* `jobqueue_dispatch_fetches_total{result="error"}` rises, and the logs say `lease fetch failed`. That is the database: connection limit, slow statements (`jobqueue_store_call_seconds{op="lease"}`), a long lock, a failover.
6. *Leases are not being reaped.* `leased` jobs whose `first_expiry` in the query above is in the past: the reaper is not running or cannot reach the database. `jobqueue_jobs_reaped_total` is flat and the logs show `reap failed`. If brokers are up and the database is fine, look for a lock held by another session on `jobqueue.jobs`.

**What to do.** Fix the cause above. The queue loses nothing while stuck: jobs are rows, and when workers or the database come back, the backlog drains in fair order.

### The dead-letter queue keeps growing

**What it means.** Jobs run out of attempts (25 by default) or are failed with `Permanent`. A few are normal; a steady rise means a bug, a bad deploy or a broken dependency.

**Confirm.** `jobqueue_queue_jobs{queue="Q",state="dead"}` (refreshed every 15 s, and only for queues this broker has served since it started), `jobqueuectl dead list --queue Q`, and the errors:

```sql
SELECT left(last_error, 100) AS error, count(*) FROM jobqueue.jobs
WHERE queue = 'Q' AND state = 'dead' GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
```

How to read `last_error`:

| Error | Meaning |
| --- | --- |
| `lease expired` | Workers lost their leases: a handler that stalls, a worker starved of CPU, a network cut. See *fenced rejections are spiking* |
| `panic: ...` with a stack | A handler panicked on every attempt |
| Your own error text | The handler failed. It is retried until the budget is gone, unless it was wrapped in `Permanent` |
| `result exceeds 65536 bytes` | A handler returned a result over 64 KiB. No retry can shorten it, so the job is dead at once |

**What to do.** Fix the cause first, then give the jobs another run: `jobqueuectl requeue ID` (attempts restart at 0). To discard dead jobs: `jobqueuectl purge --queue Q --state dead --before 2026-10-01T00:00:00Z`. Dead jobs are deleted by themselves 14 days after they died, so decide before then.

### The table is bloating

**What it means.** `jobqueue.jobs` churns: every job leaves dead row versions when it is leased, heartbeated and finished, and finished rows stay for their retention. The table has `fillfactor = 85` and tighter autovacuum settings, so its size should level off at about the retention times the finish rate. A size that keeps climbing past that means vacuum is not keeping up, or something holds it back, or pruning is not running.

**Confirm.**

```sql
SELECT pg_size_pretty(pg_total_relation_size('jobqueue.jobs')) AS total,
       pg_size_pretty(pg_relation_size('jobqueue.jobs')) AS heap;
SELECT n_live_tup, n_dead_tup, last_autovacuum, last_autoanalyze
FROM pg_stat_user_tables WHERE schemaname = 'jobqueue' AND relname = 'jobs';
```

Then, in this order:

1. *Is pruning running?* `jobqueue_jobs_pruned_total` should rise as jobs finish, and the logs show `deleted finished jobs`. If it does not, see whether brokers are up and `prune failed` appears in their logs.
2. *Is the retention the cause?* The table holds `finish rate x retention` rows by design. A dead-letter queue that fills (see above) keeps its rows for 14 days.
3. *Is something holding vacuum back?* `n_dead_tup` high with a recent `last_autovacuum` means vacuum runs and cannot remove the rows: an old snapshot is holding them.

   ```sql
   SELECT pid, now() - xact_start AS age, state, left(query, 80) FROM pg_stat_activity
   WHERE xact_start IS NOT NULL ORDER BY xact_start LIMIT 5;          -- long transactions
   SELECT slot_name, active, xmin FROM pg_replication_slots;           -- stuck replication slots
   ```
4. *Is autovacuum starved?* `last_autovacuum` is old or null while `n_dead_tup` grows: check `autovacuum_max_workers`, `autovacuum_vacuum_cost_limit` and whether other large tables occupy the workers.

**What to do.** End or fix the long transaction or the slot. Give autovacuum more room (more workers, a higher cost limit). Shorten `--retention-done` if you do not need a day of history. Do not `VACUUM FULL` a live queue: it takes an exclusive lock on the table. If you must reclaim space, `pg_repack` or a maintenance window are the options. The settings of the table and the reasons for them are in `internal/store/pgstore/migrate/sql/0001_init.sql`, and the soak experiment in [docs/benchmarks.md](benchmarks.md) measures whether the size plateaus.

### A broker is not ready

**What it means.** `/readyz` answers 503 with the reason as the body:

| Body | Meaning and action |
| --- | --- |
| `draining` | The broker got SIGTERM and is shutting down. Normal during a rollout |
| `database not checked` | It just started and has not pinged the database yet. Wait a few seconds; if it persists the pinger is not running |
| `database check is stale` | No successful or failed check in the last 5 s: the database call hangs. Look at network and the database's load |
| `database unavailable` | The last check failed. Check the database, the credentials in the URL and the pool limit. The logs say `the database does not answer` with the error |

If the pod exits at start instead, read its last log line:

| Message | Cause |
| --- | --- |
| `database schema version X, this binary needs Y: run jobqueuectl migrate` | The migration did not run, or an older broker meets a newer schema. Run `jobqueuectl migrate` (the chart's hook does it on install and upgrade) |
| `refusing to serve ... without TLS and tokens; pass --insecure to override` | Not both `tls.secretName` and `tokens.secretName` are set. Set both, or `insecure=true` on a trusted network |
| `tokens file line N: ...` | A malformed token line, or a duplicate hash. The message never contains a hash |
| `no database URL` | The Secret or `--database-url` is missing |

A broker that is not ready is out of the Service, so clients use the others. If **all** brokers are not ready, the cause is almost certainly the database.

### Fenced rejections are spiking

**What it means.** `jobqueue_acks_total{result="fenced"}` counts reports from workers that the broker refused because the worker no longer holds the lease. Some are normal: a producer cancelled a job (its worker's next heartbeat is `fenced`), or a worker was stopped hard and a lease ran out. A spike means workers are losing leases and doing work for nothing.

**Confirm.**

```
sum by (op) (rate(jobqueue_acks_total{result="fenced"}[5m]))
```

- `op="heartbeat"` is a lease lost while its handler ran. Workers log `cancelling a handler` with `reason` `lost` (the broker said so), `deadline` (heartbeats could not get through in time) or `drain`.
- `op="complete"` or `op="fail"` is a report that arrived after the job was taken over: a zombie handler finishing late. The worker logs `an ack was fenced: the lease was no longer this worker's`.

**Likely causes.** Workers starved of CPU (a container limit throttles the process, so heartbeats go late), long garbage collections or paused containers, a network that drops or delays traffic between workers and brokers, a TTL too short for the handlers' stalls, a clock step on the database host (a forward step expires every lease at once), or a rise in cancellations by producers.

**What to do.** Look for the cause on the worker side first (CPU throttling, memory pressure, node pressure, network). Raise `LeaseTTL` if the stalls are real and tolerable (the margin and the heartbeat rhythm follow it; the TTL must exceed four margins). The queue's own state is safe: fenced reports change nothing. What is at risk is your side effects: if the handlers write to a system that does not keep the fencing token (`Job.Token()`) or is not idempotent, a zombie's write has landed. See the zombie-worker example in the README, and fix the sink.

### The NOTIFY listener keeps reconnecting

**What it means.** `jobqueue_notify_reconnects_total` rises. Each broker holds one connection for `LISTEN`; when it drops, the broker reconnects with jittered backoff (up to 5 s) and resyncs. While it is down only the 1 s fallback poll finds work, so dispatch latency rises by up to a second. No job is lost.

**Confirm.** Logs: `notify listener lost its connection` with the error and the delay before the retry. In the database:

```sql
SELECT pid, state, backend_start, state_change FROM pg_stat_activity
WHERE application_name = 'jobqueue-listener';
```

A `backend_start` that keeps getting newer is the reconnect loop.

**Likely causes.** A pooler in transaction mode between the broker and PostgreSQL (the connection is recycled or `LISTEN` is not honoured), a load balancer or firewall that closes idle connections (the listener pings every 30 s of silence, so a timeout below that will cut it), a database restart or failover, `idle_session_timeout`, or `max_connections` exhausted so that the connection cannot be made.

**What to do.** Give the listener a direct or session-mode connection (`--database-url`), raise idle timeouts above 30 s, and check `max_connections`. Also watch `jobqueue_notify_malformed_total`: it counts notifications on the channel `jobqueue_ready` that are not a valid queue and tenant, which only something other than the queue can send.

### KEDA is not scaling

**What it means.** KEDA asks a broker for a queue's backlog through the broker's external scaler, a gRPC service on its own port (`--scaler-addr`, which the chart sets to `:7072`), and scales the worker Deployment to match. The backlog is the number of **ready plus leased** jobs of the queue; scheduled jobs are not counted until they fall due. Leased jobs are included, so KEDA never scales workers to zero while they hold leases: the scaler is active whenever a job is leased, whatever `activationBacklog` says, and otherwise when the backlog is **greater than** `activationBacklog`. The metric is named `jobqueue-<queue>-backlog`. A backlog that a broker has just read is reused for half a second, so many KEDA streams do not mean many database queries.

**Check, in this order.**

1. *The queue and the numbers.* `jobqueuectl stats --queue Q`: is the backlog there, and is it the queue the `ScaledObject` names (`metadata.queue`)? A typo in `queue`, `targetBacklog` (an integer of at least 1, default 10) or `activationBacklog` (an integer of at least 0, default 0) is refused by every call.
2. *Activation and cooldown.* With no job leased, the scaler is inactive until the backlog exceeds `activationBacklog`. Scale-to-zero waits for `cooldownPeriod` after the last activity, and for the Deployment's pods to terminate.
3. *The ScaledObject.* `kubectl describe scaledobject <name>` shows conditions and errors; `kubectl get hpa` shows what KEDA created. With the chart, `example.keda.enabled` renders one for the example worker, with `minReplicas`, `maxReplicas`, `targetBacklog`, `pollingInterval` and `cooldownPeriod` from the values.
4. *Reachability.* The scaler Service is `<fullname>-scaler` on port 7072, and the trigger's `scalerAddress` is `<fullname>-scaler.<namespace>.svc.cluster.local:7072`. The chart's NetworkPolicy admits that port only from the namespace `scaler.allowNamespace` (default `keda`); if KEDA runs in another namespace, set it. From a pod in the KEDA namespace, check that the address connects. KEDA dials it without TLS.
5. *KEDA's own logs.* `kubectl logs -n keda deploy/keda-operator` shows the external scaler errors.
6. *The brokers.* The scaler is served by the brokers, so brokers that are not ready cannot answer; see above. A broker with no `--scaler-addr` does not serve it: with the chart, `scaler.enabled` must be `true`.
7. *The example worker's own access.* The example worker still needs a way into the brokers: `--set insecure=true` for a plaintext test cluster, or the Secret named by `example.tokenSecret` and `example.tokenKey` (a worker token for the queue `thumbnails`) and, with TLS, a `ca.crt` key in the TLS Secret. A worker that is refused stops, and the backlog does not drain.

**Mind the exposure.** The scaler port is **unauthenticated, plaintext gRPC on a port of its own, and it must stay inside the cluster behind the chart's NetworkPolicy.** KEDA's external scaler protocol sends no token, so the broker puts no authentication in front of it, and it is served without TLS, which is how KEDA dials it unless the trigger asks for TLS. What the chart does: when `scaler.enabled` is true it renders a NetworkPolicy that selects the broker pods and admits TCP 7070 and 7071 from anywhere and TCP 7072 (`scaler.port`) only from namespaces labelled `kubernetes.io/metadata.name: <scaler.allowNamespace>`. The `<fullname>-scaler` Service is of type ClusterIP. The policy is enforced only by a network plugin that supports NetworkPolicy; on a cluster that has none, the port is open to every pod. Within the KEDA namespace any pod can reach it. What a caller learns is the job count of a queue it names (the cache of queue names is capped at 1024 so that this cannot be used to grow memory), and it changes nothing. Do not publish the port outside the cluster, whether by a `LoadBalancer`, a `NodePort` or an ingress, and do not point `scaler.allowNamespace` at a namespace whose pods you do not trust.

## Reference

### Flags of `jobqueue-server`

Every flag has a default; `--database-url` (or `$JOBQUEUE_DATABASE_URL`) is the only one required.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--database-url` | `$JOBQUEUE_DATABASE_URL` | PostgreSQL connection URL |
| `--max-db-conns` | 20 | Size of the database connection pool |
| `--grpc-addr` | `:7070` | Listen address of the gRPC API |
| `--http-addr` | `:7071` | Listen address of `/healthz`, `/readyz` and `/metrics` |
| `--scaler-addr` | empty (off) | Listen address of the autoscaler service: unauthenticated plaintext gRPC, to be reachable only by KEDA through a NetworkPolicy |
| `--tokens-file` | none | File of token hashes |
| `--tls-cert`, `--tls-key` | none | TLS certificate and key of the gRPC listener, both or neither |
| `--insecure` | false | Serve a non-loopback address without TLS and tokens |
| `--scheduler` | `drr` | `drr` (weighted fair) or `fifo` (arrival order) |
| `--tenant-weights` | none | `tenant=weight,tenant=weight`, weights 1 to 1000, others weigh 1 |
| `--quantum` | 8 | Jobs a tenant of weight 1 takes per turn under `drr` |
| `--max-in-flight` | 4 | Most store fetches in flight per queue |
| `--disable-listen` | false | Do not listen for notifications; find work by polling only. For measurements: production leaves it off |
| `--poll-interval` | 1s | Fallback poll for work when no notification arrives |
| `--activity-interval` | 1s | How often the dispatcher asks which tenants have work |
| `--reap-interval`, `--reap-batch` | 1s, 1000 | How often expired leases are taken back, and how many at once |
| `--prune-interval`, `--prune-batch` | 1m, 1000 | How often finished jobs are deleted, and how many at once |
| `--retention-done` | 24h | How long succeeded and cancelled jobs are kept |
| `--retention-dead` | 336h | How long dead jobs are kept |
| `--max-connection-age` | 5m | Age at which a client connection is asked to reconnect |
| `--max-connection-age-grace` | 30s | How long running calls may outlive that |
| `--shutdown-grace` | 25s | How long a shutdown waits for running calls |
| `--log-level`, `--log-format` | `info`, `json` | `debug`, `info`, `warn`, `error`; `json` or `text` |
| `--version` | | Print the version and exit |

### `jobqueuectl`

```
jobqueuectl [global flags] <command> [flags]

enqueue --queue Q [--tenant T] [--delay D | --run-at RFC3339] [--max-attempts N] [--key K] (--payload @file | --payload-string S)
get <id>                      show a job
cancel <id>                   stop a job that has not finished
stats --queue Q               count a queue's jobs per tenant
dead list --queue Q [--after ID] [--limit N]
requeue <id>                  give a dead job another run
purge --queue Q --state succeeded|dead|cancelled --before RFC3339
migrate --database-url URL    apply the database migrations (env JOBQUEUE_DATABASE_URL)
token new --role producer --tenant T | --role worker --queues a,b | --role admin

global flags (before or after the command):
  --addr host:port   the broker (env JOBQUEUE_ADDR, default dns:///localhost:7070)
  --insecure         connect without TLS
  --tls-ca file      the CA that signed the broker's certificate
  --token token      bearer token (env JOBQUEUE_TOKEN; prefer the variable)
  --json             one JSON object per result
  --timeout d        limit for the whole command (default 10s)
```

There is no default transport: use `--insecure` or `--tls-ca`.

### Metrics

Served at `/metrics` on the HTTP port. A `queue` label holds the queue name for the first 100 distinct queues a broker sees and `other` for the rest.

| Metric | Labels | What it counts |
| --- | --- | --- |
| `jobqueue_jobs_enqueued_total` | `queue` | Jobs created |
| `jobqueue_jobs_deduplicated_total` | `queue` | Enqueue items whose idempotency key already named a job |
| `jobqueue_jobs_leased_total` | `queue` | Jobs the store leased to this broker's dispatcher |
| `jobqueue_acks_total` | `op` (`complete`, `fail`, `release`, `heartbeat`), `result` (`accepted`, `duplicate`, `fenced`) | Reports from workers. A heartbeat that held its lease counts as `accepted`, one that lost it as `fenced` |
| `jobqueue_dispatch_fetches_total` | `queue`, `result` (`jobs`, `empty`, `error`) | Store fetches of the dispatcher. A waiting queue is fetched about once a second, so a steady stream of `empty` is idle polling and not a fault |
| `jobqueue_waiters` | `queue` | Long polls waiting at this broker |
| `jobqueue_lease_wait_seconds` | | How long a Lease call waited before it was given jobs (histogram) |
| `jobqueue_orphan_releases_total` | | Jobs given back because the Lease call they were fetched for had gone |
| `jobqueue_jobs_reaped_total` | | Jobs taken back from expired leases |
| `jobqueue_jobs_pruned_total` | | Finished jobs deleted after their retention |
| `jobqueue_store_call_seconds` | `op` | Store call durations (histogram) |
| `jobqueue_queue_jobs` | `queue`, `state` (`ready`, `scheduled`, `leased`, `dead`) | Jobs by state, refreshed every 15 s for the queues this broker has seen |
| `jobqueue_notify_reconnects_total` | | Times the NOTIFY listener lost its connection |
| `jobqueue_notify_malformed_total` | | Notifications that were not a queue and a tenant |
| `jobqueue_build_info` | `version`, `go_version` | A constant 1 |

Go runtime and process collectors are served as well.

Every broker counts its own calls: sum over brokers for a total. `jobqueue_queue_jobs` reads the whole queue, so take one broker's value (`max by (queue, state)`) and not the sum.

### Logs

Structured JSON on stderr by default (`--log-format text` for people). The messages an operator looks for: `broker started`, `shutting down`, `took back expired leases` and `deleted finished jobs` (with `jobs`), `lease fetch failed`, `activity read failed`, `notify listener lost its connection`, `the database does not answer`, `reap failed`, `prune failed`, and `internal error` with a job id and state, which is a bug to report.

### Upgrading

Run `jobqueuectl migrate` first, then roll the brokers. A migration is forward-only. A broker checks the schema version when it starts, and exits unless it is exactly the one the binary was built for, so a new broker will not start on an old schema and an old broker will not start on a new one. A broker that is already running does not check again: during the roll, old brokers that are still up keep working only if the migration is compatible with them, and the [CHANGELOG](../CHANGELOG.md) says when one is not. CI checks every change to the gRPC protocol against the main branch for breaking changes, so one that would break an earlier client is caught before it merges.

**`helm rollback` does not undo a migration.** After a release that added one, the older brokers refuse to start against the new schema. That includes an old pod that is only rescheduled, by a node drain or an eviction, while the new schema is in place. Going back therefore means restoring the database from a backup taken before the migration, which also drops every job written since, or rolling forward with a fix. Back up the database before you upgrade to a release whose changelog entry names a migration. Rolling back to a release that has the same schema version as the current one is safe.
