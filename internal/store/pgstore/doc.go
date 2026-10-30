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
//   - A lease picks rows with FOR UPDATE SKIP LOCKED in a materialized CTE,
//     so concurrent leases never see the same row and the planner cannot
//     evaluate the pick twice.
//   - NULL stands for the zero time in lease_expires_at and finished_at, for
//     the empty string in leased_by, idempotency_key and last_error, and for
//     an empty result.
//   - Enqueue, Release and Requeue send a notification on the channel
//     NotifyChannel, in the transaction of the write, for each queue and
//     tenant that gained a job that is ready to run. A listener learns that
//     work exists; it reads the table to learn what.
//
// Errors are wrapped by classify: a lost connection, a closed pool and a
// server that is shutting down or full wrap store.ErrUnavailable, which the
// caller may retry; a cancelled context stays a context error.
package pgstore
