# 0006. Stateless brokers and client-side load balancing

Status: accepted.

## Context

Brokers must be easy to add, remove and restart: a rolling update of the Deployment, a node drain and a crash should cost nothing but the calls in flight. Workers hold long polls open for up to 30 s, and a worker's heartbeats and acknowledgements must keep working while a broker is being replaced.

Two properties of the platform fight this. A gRPC connection is one long-lived HTTP/2 connection, and a Kubernetes `ClusterIP` Service balances connections, not calls, so every client stays with the broker it first reached, and a broker that is added later gets none of the existing traffic. And a long poll that is cut in the middle of a fetch can strand jobs.

## Decision

- **Brokers share nothing.** All job state is in PostgreSQL ([ADR 0001](0001-postgres-as-source-of-truth.md)), so any broker can serve any call, and brokers do not talk to each other. What a broker holds is soft: the long polls waiting on it, the fair-share state of the queues it has seen and the metrics. A new broker can serve any call as soon as its `/readyz` passes.
- **The client balances.** The SDK's `Dial` defaults to `round_robin` over every address the target resolves to. In Kubernetes the target is `dns:///<fullname>-grpc.<namespace>.svc.cluster.local:7070` (`<fullname>` is the release name with `-jobqueue` appended, unless it already contains `jobqueue`), a headless Service (`clusterIP: None`) whose DNS name has one record per ready broker. A call goes to the next connection, and a failed or draining broker is skipped.
- **Connections are recycled.** The server sets `MaxConnectionAge` to 5 minutes with 30 seconds of grace for running calls, so clients reconnect, resolve the name again and find brokers that were added in the meantime. The client sends a keepalive after 30 s without traffic while a call is in flight, so that a connection that was cut without a reset is noticed.
- **Shutdown has an order and a budget.** On SIGTERM `/readyz` fails first; then Lease calls are refused with `Unavailable` and waiting long polls are answered empty, so the SDK moves to another broker; then the server stops accepting connections and calls and waits up to the shutdown grace (25 s) for the running ones, while workers send their heartbeats and acknowledgements to the other brokers; then the background loops stop and the pool closes. `/healthz` keeps answering during all this, so that the liveness probe does not kill a draining pod. The chart adds a `preStop` sleep of 5 s, as `jobqueuectl sleep` because the image has no shell, so that the endpoint's removal reaches clients before draining starts, and refuses to render unless the sleep plus the grace is less than `terminationGracePeriodSeconds` (40 s).
- **Every call can be repeated.** Enqueue carries an idempotency key; Complete is idempotent for the same token; heartbeat and get are idempotent; a lease whose answer is lost is simply a lease that expires. So the producer repeats a call that failed with `Unavailable`, `ResourceExhausted`, `Aborted` or a per-call timeout, with jittered backoff, and the round robin sends the repeat to another broker. A worker's heartbeat that fails is repeated once at once, its lease request is repeated with its own jittered backoff, and its acknowledgement is repeated for as long as it holds the lease.
- **Background work runs on every broker.** The reaper and the pruner run on all of them. They are idempotent, and the reaper locks the rows it takes with `SKIP LOCKED`, so brokers reaping at once never take the same job. There is no leader.
- **The autoscaler endpoint is its own server.** KEDA's external scaler is served on a separate port, as plaintext gRPC without the token interceptor because KEDA sends no tokens and dials it without TLS. It must stay inside the cluster, and a NetworkPolicy admits it only from the KEDA namespace.

## Consequences

- **Database load grows with brokers.** Each broker polls PostgreSQL for the queues it has waiters on (an activity read about every second, and a fallback poll), and runs the reaper and pruner every second or minute, so the idle load is brokers times queues, not a constant. The statements are index-only when there is nothing to do, and a test pins that; the load is small and not zero.
- **Fairness is per broker** ([ADR 0005](0005-drr-fair-scheduling.md)) and the overall share is approximate.
- **Rebalancing takes minutes.** After a scale-out, the new broker receives traffic as connections are recycled: within a connection age (5 minutes), not at once. A load that must be spread faster needs a shorter `--max-connection-age`, at the price of more reconnects.
- **Reconnects are constant, low-level churn.** Every connection is replaced every 5 minutes, and the SDK has to handle that without dropping a lease. It does, because a heartbeat that fails is retried on another broker at once.
- **A broker's death costs its in-flight calls,** which the SDK repeats, and the jobs fetched for a long poll that died with it, which expire and are reaped. A broker that drains gracefully releases them instead.
- **DNS has to be right.** Load balancing depends on the client seeing every broker's address. A resolver that caches for long, or a Service that is not headless, brings back the pinning.
- **A client can lag a rolling restart by up to 30 s.** gRPC's DNS resolver resolves a name at most once every 30 s, so a client that resolved it less than 30 s before every broker it knows is replaced reaches no broker until it may resolve again; in one end-to-end run that took about 10 s. Its calls are repeated through the window, and a finished job's acknowledgement is kept for as long as the lease holds, so the window delays work and loses none ([#78](https://github.com/jason-te-sde/distributed-job-queue/issues/78)).
- **No sticky sessions, no ownership of queues.** Nothing about a broker is special, which is what makes it replaceable, and what means two brokers always do some redundant work.

## Alternatives rejected

- **A `ClusterIP` Service, or any layer-4 balancer.** It balances connections, so a long-lived gRPC client stays on one pod and new pods are idle. Rejected.
- **A layer-7 proxy in front (Envoy, an ingress).** It balances calls, and adds a component, a hop and a place where long polls and idle timeouts have to be tuned. Client-side round robin needs only DNS. Rejected as more machinery than the problem needs.
- **A service-discovery protocol (xDS) or a lookup service.** More infrastructure for a list of addresses that a headless Service already provides. Rejected.
- **Brokers that own queues** (consistent hashing, a leader per queue). It would give strict per-queue fairness and ordering, and in exchange it needs membership, failure detection and handover, and a failed owner stalls its queues until the cluster notices. Rejected: the database already serializes what needs serializing.
- **A leader for the reaper and the pruner.** It would remove redundant polls and add an election, and a stalled leader would stop reaping. Redundant, idempotent work is the cheaper failure mode. Rejected.
- **Killing long polls without a drain.** It would strand jobs fetched for a poller that has gone, which then wait out their lease. Draining answers every waiter and releases orphans. Rejected.
