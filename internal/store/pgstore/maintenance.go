package pgstore

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/flaw"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

// reapSelectSQL locks the leases that have run out, lowest id first, skipping
// rows that another transaction holds, and returns them with the store's now.
//
// The expired ids are found first, through jobs_leased_expiry, and the rows are
// then fetched by id. Asked directly for the lowest expired ids, the planner
// walks the primary key and filters, which reads the whole table whenever no
// lease has expired. The outer WHERE repeats the conditions, because a row that
// changed while the statement waited for its lock is checked again against them.
//
//	$1 limit
const reapSelectSQL = `
WITH clock AS MATERIALIZED (SELECT jobqueue.now() AS now)
SELECT ` + jobCols + `, (SELECT now FROM clock)
FROM jobqueue.jobs j
WHERE j.id = ANY(ARRAY(SELECT x.id FROM jobqueue.jobs x
                       WHERE x.state = 'leased' AND x.lease_expires_at <= (SELECT now FROM clock)))
  AND j.state = 'leased' AND j.lease_expires_at <= (SELECT now FROM clock)
ORDER BY j.id
LIMIT $1::int8
FOR UPDATE OF j SKIP LOCKED`

// reapUpdateSQL writes what the transition decided for the locked rows. The
// lease columns are cleared in every case.
//
//	$1 ids, $2 states, $3 run_at, $4 finished_at, $5 last_error
const reapUpdateSQL = `
UPDATE jobqueue.jobs AS j
SET state = u.state, run_at = u.run_at, finished_at = u.finished_at, last_error = nullif(u.last_error, ''),
    lease_expires_at = NULL, leased_by = NULL
FROM unnest($1::int8[], $2::text[], $3::timestamptz[], $4::timestamptz[], $5::text[])
     AS u(id, state, run_at, finished_at, last_error)
WHERE j.id = u.id`

// Reap implements store.Store. It runs in one transaction: it locks up to limit
// expired leases, asks backoff for the delay of each in ascending id, decides
// each row's fate with job.Reap, and writes them all. A delay that is not
// valid rejects the call before any row is written.
func (s *Store) Reap(ctx context.Context, limit int, backoff store.BackoffFunc) (int, error) {
	if err := validateLimit(limit); err != nil {
		return 0, err
	}
	if backoff == nil {
		return 0, invalid("backoff", "must not be nil")
	}
	reaped := 0
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, reapSelectSQL, int64(limit))
		if err != nil {
			return err
		}
		var now time.Time
		expired, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (job.Job, error) {
			var at time.Time
			j, err := scanJob(row, &at)
			now = job.NormalizeTime(at)
			return j, err
		})
		if err != nil {
			return err
		}
		if len(expired) == 0 {
			return nil
		}

		n := len(expired)
		ids, states := make([]int64, n), make([]string, n)
		runAts, finishedAts, lastErrors := make([]time.Time, n), make([]*time.Time, n), make([]string, n)
		for k, before := range expired {
			delay := backoff(before.Attempts)
			if err := job.ValidateDelay("backoff", delay); err != nil {
				return err
			}
			in := before
			// Flaw F3PG decides as if the job had an attempt left. The update
			// never writes max_attempts, so the stored budget stays as it was.
			if flaw.On(flaw.F3PG) {
				in.MaxAttempts = in.Attempts + 1
			}
			next, outcome := job.Reap(in, now, delay)
			if outcome != job.Applied {
				return &store.InternalError{ID: before.ID, State: before.State, Msg: "an expired lease was not reaped: " + outcome.String()}
			}
			ids[k], states[k], runAts[k], lastErrors[k] = next.ID, next.State.String(), next.RunAt, next.LastError
			if !next.FinishedAt.IsZero() {
				finishedAts[k] = &next.FinishedAt
			}
		}
		tag, err := tx.Exec(ctx, reapUpdateSQL, ids, states, runAts, finishedAts, lastErrors)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != int64(n) {
			return &store.InternalError{ID: expired[0].ID, State: expired[0].State,
				Msg: "the rows locked for reaping were not all updated"}
		}
		reaped = n
		return nil
	})
	if err != nil {
		return 0, classify("reap", err)
	}
	return reaped, nil
}

// pruneSQL deletes up to a limit of finished jobs that have been finished for
// longer than their retention, lowest id first. Exactly at the retention a job
// is kept. The clock CTE holds the two cutoffs, both computed from one reading
// of the database clock.
//
// The candidates are the jobs finished before the older of the two cutoffs,
// found through jobs_finished; the rows are then fetched by id and checked
// against their own cutoff. See reapSelectSQL for why not in one step.
//
//	$1 retention of succeeded and cancelled jobs, $2 retention of dead jobs, both
//	in microseconds, $3 limit
const pruneSQL = `
WITH clock AS MATERIALIZED (
    SELECT n.now,
           n.now - $1::int8 * interval '1 microsecond' AS done_before,
           n.now - $2::int8 * interval '1 microsecond' AS dead_before
    FROM (SELECT jobqueue.now() AS now) n
),
doomed AS MATERIALIZED (
    SELECT j.id FROM jobqueue.jobs j
    WHERE j.id = ANY(ARRAY(SELECT x.id FROM jobqueue.jobs x
                           WHERE x.finished_at < (SELECT greatest(done_before, dead_before) FROM clock)))
      AND ((j.state IN ('succeeded', 'cancelled') AND j.finished_at < (SELECT done_before FROM clock))
        OR (j.state = 'dead' AND j.finished_at < (SELECT dead_before FROM clock)))
    ORDER BY j.id
    LIMIT $3::int8
    FOR UPDATE OF j SKIP LOCKED
)
DELETE FROM jobqueue.jobs AS j USING doomed WHERE j.id = doomed.id`

// Prune implements store.Store.
func (s *Store) Prune(ctx context.Context, r store.Retention, limit int) (int, error) {
	if err := validateLimit(limit); err != nil {
		return 0, err
	}
	if err := validateRetention("retention.done", r.Done); err != nil {
		return 0, err
	}
	if err := validateRetention("retention.dead", r.Dead); err != nil {
		return 0, err
	}
	tag, err := s.pool.Exec(ctx, pruneSQL, micros(r.Done), micros(r.Dead), int64(limit))
	if err != nil {
		return 0, classify("prune", err)
	}
	return int(tag.RowsAffected()), nil
}
