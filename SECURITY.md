# Security policy

## Reporting a vulnerability

Please report a vulnerability **privately**, through GitHub's private vulnerability reporting: open the repository's **Security** tab and choose **Report a vulnerability**. The report reaches the maintainers only, and GitHub keeps the discussion, any fix and the advisory together.

Please do not open a public issue or a pull request for a vulnerability, and do not post details elsewhere before there is a fix.

A useful report says what the problem is, which version or commit it affects, how to reproduce it (a request, a configuration, a short program), and what an attacker gains. You do not need a fix. Reports are read and answered on a best-effort basis, and a confirmed vulnerability is fixed and disclosed through a GitHub security advisory, with credit to the reporter if they want it.

## Supported versions

Security fixes are made for the latest released version, `0.1.x`. There are no long-term support branches.

## What the security model is

So that a report can be judged against it:

- **Authentication.** A broker authenticates callers by static bearer tokens. It stores only the SHA-256 hashes of the tokens, in a file that it reads at start. A token has one of three roles: producer (bound to one tenant), worker (bound to a list of queues) or admin.
- **Authorization.** Every call is validated first, then checked against the caller's role and its tenant or queues, and only then reaches the database. A producer sees and cancels only its own tenant's jobs, and another tenant's job is reported as not found.
- **Transport.** TLS is supported on the gRPC port. A broker on a non-loopback address refuses to start without TLS and a tokens file unless it is given `--insecure`, so that an unauthenticated queue is never exposed by omission. On a loopback address without a tokens file, every caller is an admin.
- **Queue and tenant names, sizes and keys** are validated at the broker and again in the store, and the schema's constraints back them up.
- **The database is trusted.** Anyone who can write to `jobqueue.jobs`, or send `NOTIFY` on its channel, can do what the queue can do. A notification with a malformed payload is counted and dropped.

## What is in scope

A flaw in these is a vulnerability: bypassing authentication or authorization (reading or changing another tenant's jobs, using a producer or worker token for an admin call, a request that passes without a token when tokens are configured); a way to make the broker accept a stale fencing token; SQL injection or any input that reaches the database unvalidated; a request that crashes a broker or exhausts its memory or connections out of proportion to its size; a leak of a token or of its hash through logs, metrics or error messages; and a weakness in how the SDK handles tokens and TLS.

## What is not

These are known properties, described in the documentation, and not vulnerabilities:

- **The HTTP port** (`--http-addr`: `/healthz`, `/readyz`, `/metrics`) has no authentication, because probes and scrapers send no tokens. Keep it inside the cluster.
- **The KEDA scaler port** is unauthenticated plaintext gRPC on a port of its own, because KEDA's external scaler protocol sends no token and KEDA dials it without TLS. It must stay inside the cluster. It reads job counts only. The Helm chart's NetworkPolicy restricts it to the KEDA namespace, which depends on a network plugin that enforces NetworkPolicy.
- **`--insecure`** and **`WithInsecure`** turn protection off on purpose. A token sent over a plaintext connection is visible to the network.
- **A token on a command line** (`jobqueuectl --token`) shows in the process list. Use `JOBQUEUE_TOKEN`.
- **Delivery is at-least-once.** A job can run more than once; side effects in your own systems need the fencing token or idempotency ([README](README.md)).
- **A malicious worker** with a valid token can report a false result for a job it holds.
- **Anyone with database access** can read and change jobs.

## Dependencies

The dependency list is short and checked by a test, and CI runs `govulncheck` over the code on every change. Dependency updates arrive through Dependabot.
