// Package pgstore is the PostgreSQL implementation of store.Store, the one
// that production runs on.
//
// The database is the only time authority. Every statement reads the clock
// once, in a leading CTE named clock that calls jobqueue.now(), and uses that
// value throughout, so that a job's created_at and run_at agree and so that
// the time left on a lease is exactly the TTL that was asked for. Durations
// travel as whole microseconds in an int8 and become intervals in SQL; no
// float seconds are involved.
//
// Rules the statements follow:
//   - A write is one guarded UPDATE (state, lease epoch and queue are in its
//     WHERE clause), so the common case costs one round trip. The items that
//     the UPDATE did not touch are read back and given to the matching
//     transition of package job, which says why (fenced, expired, a duplicate,
//     the wrong state or not found). The policy lives in package job; SQL only
//     guards the write.
//   - An acknowledgement (Complete, Fail, Heartbeat, Release) starts with a CTE
//     that locks the rows it is about to change in ascending job id, and its
//     UPDATE then changes only those rows. Two calls that name the same jobs
//     in opposite orders therefore take their locks in the same order and
//     cannot deadlock, whatever plan the planner picks. Only rows that pass the
//     guard are locked, so an item with a stale token blocks nobody. A call
//     that names one job id more than once runs one pass per repetition in one
//     transaction; the order is not kept across passes, and such a call can
//     still deadlock against another, which is reported as ErrUnavailable.
//   - A lease picks rows with FOR UPDATE SKIP LOCKED in a materialized CTE,
//     so concurrent leases never see the same row and the planner cannot
//     evaluate the pick twice.
//   - A comparison with the clock that should bound an index range is written
//     against (SELECT now FROM clock), which the planner evaluates once and
//     uses as the bound; against a join with the clock it reads every row of the
//     index and filters. Reap and Prune find their candidates through the index
//     of the column they test and fetch the rows by id, because asked for the
//     lowest ids directly, the planner walks the primary key. A test pins the
//     rows each of these statements reads.
//   - NULL stands for the zero time in lease_expires_at and finished_at, for
//     the empty string in leased_by, idempotency_key and last_error, and for
//     an empty result.
//   - Enqueue, Release and Requeue send a notification on the channel
//     NotifyChannel, in the transaction of the write, for each queue and
//     tenant that gained a job that is ready to run. A listener learns that
//     work exists; it reads the table to learn what.
//   - Those same three calls hold at most a bounded share of the pool's
//     connections at once (WithMaxNotifying): every committing NOTIFY takes a
//     database-wide lock, so notifying transactions commit one at a time and
//     queue on it, each holding a connection. The bound is taken before the
//     connection, so that Lease, Activity and the acknowledgements, which do
//     not notify, find one: with the default bound a pool keeps at least two
//     connections for them from 3 connections up, one at 2 and none at 1. A
//     call that waits under the bound holds no connection, and its context
//     ends the wait.
//
// Errors are wrapped by classify. These wrap store.ErrUnavailable, which the
// caller may retry: a connection that cannot be opened or was lost (a network
// error, an error the driver reports as safe to retry, or the end of the
// stream on a pooled connection that the other end closed while it sat idle),
// a pool that has been closed, a server that is shutting down (57P01, 57P02),
// not yet accepting connections (57P03) or out of them (53300), any connection
// exception (class 08), and a call that PostgreSQL rolled back because it
// deadlocked with another (40P01) or could not be serialized (40001). A
// rollback applied nothing, but a lost connection may have committed the call
// first, so a retry is safe only for a call that tolerates repetition: an
// acknowledgement is fenced by its token and an Enqueue with an idempotency key
// is deduplicated, but an Enqueue without a key may create the job twice. A
// cancelled or expired context stays a context error, and any other error is
// wrapped unchanged.
package pgstore
