# Contributing

Thank you for looking at the code. This guide says how to build and test it, what a change has to look like, and the few rules that the build enforces.

## Set up

You need:

- Go 1.26 or newer (CI runs 1.26 and 1.27).
- PostgreSQL 15 to 18 for the tests behind the `pg` tag.
- The pinned tools, so that generated code and lint results do not depend on your machine: `golangci-lint` v2.15.x, `buf` 1.75.0 and, for the Helm checks, `helm`. `make lint` tells you the install command when one is missing or has the wrong version.

```sh
git clone https://github.com/jason-te-sde/distributed-job-queue
cd distributed-job-queue
make test                 # go test -race ./...  (unit, property, simulator, architecture, docs)

export JOBQUEUE_TEST_DSN='postgres://localhost:5432/postgres?sslmode=disable'
make test-pg              # the PostgreSQL tests; the role needs CREATEDB
make sim                  # the simulator alone, 500 seeds under -race
make flaws                # every planted flaw must turn its tests red
make preflight            # everything CI runs that can run on a laptop
```

A `pg`-tagged test that cannot reach a database fails; it never skips. [docs/testing.md](docs/testing.md) lists every layer, how to run it and what it catches.

## The rules the build enforces

- **The core is pure.** `internal/core/...` imports only the standard library (minus the packages that perform effects), other core packages and `internal/flaw`. It never reads a clock, starts a goroutine, uses a channel or `select`, or calls the global functions of `math/rand/v2`; `math/rand` may not be imported at all. It returns effects as values, and the shells in `internal/broker` and the SDK carry them out. `internal/archtest` and the linter reject a violation ([ADR 0003](docs/design/0003-pure-cores-and-simulator.md)).
- **The database clock is the only time authority.** Compare times in SQL, against `jobqueue.now()`. Workers measure durations with the monotonic clock ([ADR 0004](docs/design/0004-time-authority.md)).
- **Dependencies are a short list.** The standard library, grpc, protobuf, pgx, the Prometheus client and the vendored KEDA proto in the main module; `rapid`, `golang.org/x/tools/go/packages` and the Toxiproxy client in tests only. No ORM, no CLI framework (`flag` only), logs through `log/slog`. Any other import in non-test code fails the architecture test, and a new dependency needs a design record that says why it earns its place.
- **Sum types and enums are exhaustive.** A sum type is an interface with an unexported marker method and a `//sumtype:decl` comment, and every switch over it or over an enum names every case. Adding a case must not compile until every user handles it.
- **Errors are wrapped with `%w`** and inspected with `errors.Is` and `errors.As`. A caller's mistake is `job.ErrInvalid`, which is a different thing from a broken internal invariant (`*store.InternalError`). Library code never panics on input.
- **Every exported identifier has a doc comment,** and every package has a package comment.
- **Randomness** comes from `math/rand/v2` generators with explicit seeds. `crypto/rand` is used only where unpredictability matters.
- **Everything in the repository is English:** code, comments, documentation, commit messages, test names, logs. `make hygiene` checks that no tracked file or commit message contains CJK text or an attribution line of an AI tool, and CI runs it over the whole history. The licence names its holder, and **no file and no commit message contains an email address**; the script does not check that, so review does.

## Making a change

1. **Test first.** Write the test that fails for the right reason, see it fail, then write the code. Put the test on the cheapest layer that can catch the bug: a transition in `internal/core/job`, a contract rule in `internal/store/storetest`, an interleaving in the simulator, SQL behaviour in a `pg` test. A bug found by a higher layer (a simulator seed, the chaos test) gets a test on a lower one too, where that is possible.
2. **Keep a change to one thing.** A failing seed is kept in `internal/sim/testdata/regression_seeds.txt`, as docs/testing.md describes.
3. **Show that a new guard can fail.** If you add a safety property or a defence, plant its opposite behind the `jobqueue_flaws` tag, name it in `internal/flaw/flaw.go` and add a row to `scripts/flaws.tsv` for the test that must catch it (`TestEveryFlawHasARow` fails without one), then run `make flaws`.
4. **Run `make preflight` before you push.** CI runs the same checks, and more.

### Generated code and the protocol

The protocol is `proto/jobqueue/v1/jobqueue.proto`, and its field numbers never change once merged: `buf breaking` compares a change with the main branch. The Go code under `internal/gen` is generated and committed; run `make gen` after a change and commit the result, and never edit it by hand. The vendored `proto/externalscaler` file is KEDA's and is not edited.

### Migrations

Migrations are files `internal/store/pgstore/migrate/sql/NNNN_description.sql`, numbered without gaps, forward-only and embedded in the binary. To change the schema, add the next file, raise `migrate.Version`, and say in the file why. There is no down migration: undoing a change is another migration. A migration that an older broker cannot run against must be called out in the changelog.

### Commits and pull requests

Commit messages follow [Conventional Commits](https://www.conventionalcommits.org) with one of these scopes: `core`, `store`, `pg`, `sim`, `broker`, `sdk`, `ctl`, `chaos`, `deploy`, `bench`, `docs`, `build`, `ci`. For example, `fix(pg): lock acknowledged rows in id order`. The body is for a reader who was not there. It says:

- **why** the change is made;
- what was **considered and rejected**;
- what is **not done**;
- how far it was **verified**, with the commands.

A pull request has the same shape. Squash merges keep the history linear; the squashed message should read the same way.

## Reporting problems

A bug report that helps has the version, what you ran, what you expected and what happened. For a failing simulator seed, give the seed. A security problem does not belong in a public issue: see [SECURITY.md](SECURITY.md).

By taking part you agree to the [Code of Conduct](CODE_OF_CONDUCT.md). Contributions are licensed under the Apache License 2.0, like the rest of the project.
