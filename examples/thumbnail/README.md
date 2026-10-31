# thumbnail

An example worker for the job queue. It does CPU work that comes out the same
every time, and records each result in PostgreSQL through a sink that the
fencing token of the job's lease protects.

A job's payload is `{"seed": 7, "size": 1024}`. The worker draws a picture of
`size` by `size` pixels from the seed (a gradient with a few rectangles on it),
shrinks it to 128 by 128 with a box filter, encodes it as a PNG, and writes the
SHA-256 and the length of the PNG to the `thumbnails` table. The code uses the
standard library only, and integer arithmetic only, so a seed draws the same
pixels on every machine. The PNG bytes come from the standard library's
encoder, so the SHA-256 of a PNG can change with the Go version.

## The fenced sink

Every lease of a job has a fencing token that is higher than the one before.
The worker writes with the token of its own lease:

```sql
INSERT INTO thumbnails (job_id, token, sha256, bytes, written_at)
VALUES ($1, $2, $3, $4, clock_timestamp())
ON CONFLICT (job_id) DO UPDATE SET ...
WHERE thumbnails.token < EXCLUDED.token
```

A handler that lost its lease but kept running, a stalled process that woke up
late, cannot overwrite what the next holder wrote: its token is lower, so the
statement changes no row. The worker then fails the job for good
(`superseded by a newer lease`), because a newer holder has already done the
work and a retry would only repeat it.

Only a higher token replaces a row. A write under the same token is refused as
well, so a caller must not write twice under one lease, even to repeat a write
that failed: the first may have been applied. The handler writes once per
attempt, and a job that is tried again runs under a new lease with a higher
token, so a refused write there always means that a later lease has written.

The worker applies `schema.sql` when it starts. The statement is idempotent and
runs under an advisory lock, so replicas that start together do not collide.

## Commands

```
thumbnail produce --addr ADDR (--insecure | --tls-ca FILE) [--token TOKEN]
                  [--count N] [--tenant T]
thumbnail work    --addr ADDR (--insecure | --tls-ca FILE) [--token TOKEN]
                  [--database-url URL] [--concurrency C]
```

`produce` enqueues `N` jobs (10 by default) on the queue `thumbnails`, with the
seeds 0 to N-1 and a source size of 1024. It does not set idempotency keys of
its own, so running it twice enqueues twice. With no `--tenant` the broker
chooses: the tenant of the token, or `default`.

`work` runs a worker on the queue `thumbnails` until it gets SIGTERM or an
interrupt, then drains. It renders `C` jobs at once (4 by default).

| Flag or variable | Meaning |
| --- | --- |
| `--addr` | Broker address, for example `dns:///jobqueue-grpc:7070`. Required. |
| `--insecure` | Connect without TLS. A token is then sent in the clear. |
| `--tls-ca FILE` | Connect over TLS, trusting the CA certificates in this PEM file. |
| `--token`, `$JOBQUEUE_TOKEN` | Bearer token. The flag wins. An empty `--token` is an error. |
| `--database-url`, `$JOBQUEUE_DATABASE_URL` | PostgreSQL URL of the sink. The flag wins. |

There is no default transport. Without `--insecure` or `--tls-ca`, and with
both, the command exits with a usage error (status 2) instead of guessing.
Prefer the variables for secrets: a flag shows up in the process list.

The worker writes nothing to disk and builds every picture in memory, so it
runs under `readOnlyRootFilesystem`.

## Try it

With a broker on a loopback address and a PostgreSQL database for the sink:

```sh
go build -o thumbnail ./examples/thumbnail

export JOBQUEUE_DATABASE_URL='postgres://localhost:5432/postgres?sslmode=disable'
./thumbnail work --addr 127.0.0.1:7070 --insecure &
./thumbnail produce --addr 127.0.0.1:7070 --insecure --count 20

psql "$JOBQUEUE_DATABASE_URL" -c 'SELECT job_id, token, left(sha256, 12), bytes FROM thumbnails ORDER BY job_id'
```

## Tests

```sh
go test -race ./examples/thumbnail/
JOBQUEUE_TEST_DSN='postgres://localhost:5432/postgres?sslmode=disable' go test -race -tags pg ./examples/thumbnail/
```

The `pg` tests include two end-to-end runs: both commands against an in-process
broker, once over plaintext and once over TLS with tokens. Each checks that the
produced jobs end `succeeded`, and that a job whose sink row a newer lease has
already written, and a job whose payload cannot be rendered, end `dead` after
one attempt.

The `pg` tests belong to this repository: they start the broker from
`internal/broker` and use `internal/pgtest` and the migrations, which only code
inside the module can import. With `-tags pg` the directory's tests therefore do
not compile if it is copied out of the module. The program, and the tests
without the tag, need only the public SDK, `pgx` and the standard library.
