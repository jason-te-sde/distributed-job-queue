package pgstore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

// collectJobs reads every row of a query that selects jobCols.
func collectJobs(rows pgx.Rows) ([]job.Job, error) {
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (job.Job, error) { return scanJob(row) })
}

// Get implements store.Store.
func (s *Store) Get(ctx context.Context, id int64, tenant string) (job.Job, error) {
	if err := validateTenantScope(tenant); err != nil {
		return job.Job{}, err
	}
	j, err := scanJob(s.pool.QueryRow(ctx,
		`SELECT `+jobCols+` FROM jobqueue.jobs j WHERE j.id = $1 AND ($2::text = '' OR j.tenant = $2::text)`, id, tenant))
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return job.Job{}, fmt.Errorf("pgstore get: job %d: %w", id, store.ErrNotFound)
	case err != nil:
		return job.Job{}, classify("get", err)
	}
	return j, nil
}

// Snapshot implements store.Snapshotter.
func (s *Store) Snapshot(ctx context.Context) ([]job.Job, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+jobCols+` FROM jobqueue.jobs j ORDER BY j.id`)
	if err != nil {
		return nil, classify("snapshot", err)
	}
	jobs, err := collectJobs(rows)
	return jobs, classify("snapshot", err)
}

// statsSQL counts the jobs of a queue per tenant. The three states are read
// separately so that each uses its own partial index.
//
//	$1 queue
const statsSQL = `
WITH clock AS MATERIALIZED (SELECT jobqueue.now() AS now),
live AS (
    SELECT j.tenant, j.state, j.run_at FROM jobqueue.jobs j WHERE j.queue = $1 AND j.state = 'ready'
    UNION ALL
    SELECT j.tenant, j.state, j.run_at FROM jobqueue.jobs j WHERE j.queue = $1 AND j.state = 'leased'
    UNION ALL
    SELECT j.tenant, j.state, j.run_at FROM jobqueue.jobs j WHERE j.queue = $1 AND j.state = 'dead'
)
SELECT live.tenant,
       count(*) FILTER (WHERE live.state = 'ready' AND live.run_at <= clock.now),
       count(*) FILTER (WHERE live.state = 'ready' AND live.run_at > clock.now),
       count(*) FILTER (WHERE live.state = 'leased'),
       count(*) FILTER (WHERE live.state = 'dead'),
       min(live.run_at) FILTER (WHERE live.state = 'ready' AND live.run_at <= clock.now),
       clock.now
FROM live, clock
GROUP BY live.tenant, clock.now`

// Stats implements store.Store.
func (s *Store) Stats(ctx context.Context, queue string) ([]store.TenantStats, error) {
	if err := job.ValidateName("queue", queue); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, statsSQL, queue)
	if err != nil {
		return nil, classify("stats", err)
	}
	stats, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (store.TenantStats, error) {
		var (
			st     store.TenantStats
			oldest *time.Time
			now    time.Time
		)
		if err := row.Scan(&st.Tenant, &st.Ready, &st.Scheduled, &st.Leased, &st.Dead, &oldest, &now); err != nil {
			return st, err
		}
		if oldest != nil {
			st.OldestReadyAge = job.NormalizeTime(now).Sub(job.NormalizeTime(*oldest))
		}
		return st, nil
	})
	if err != nil {
		return nil, classify("stats", err)
	}
	// The database orders by its collation; the contract is byte order.
	slices.SortFunc(stats, func(a, b store.TenantStats) int { return compareBytes(a.Tenant, b.Tenant) })
	return stats, nil
}

// activitySQL finds the tenants with due work and the next scheduled job.
//
// The tenants with a ready job come from a loose index scan of
// jobs_ready_tenant: the recursive CTE hops from one tenant to the next
// greater one, so it reads one index entry per tenant however many jobs each
// has. A tenant has due work if its first job by run_at is due, which the
// EXISTS finds with one probe of the same index.
//
// The next scheduled job is the first ready job of the queue after now, by
// run_at, whatever its tenant: a range scan of jobs_ready_fifo that stops at
// the first row. A tenant that has due jobs and also a later scheduled one
// counts for NextDueIn like any other.
//
//	$1 queue
const activitySQL = `
WITH RECURSIVE clock AS MATERIALIZED (SELECT jobqueue.now() AS now),
ready_tenants(tenant) AS (
    (SELECT j.tenant FROM jobqueue.jobs j WHERE j.queue = $1 AND j.state = 'ready' ORDER BY j.tenant LIMIT 1)
    UNION ALL
    SELECT (SELECT j.tenant FROM jobqueue.jobs j
            WHERE j.queue = $1 AND j.state = 'ready' AND j.tenant > r.tenant
            ORDER BY j.tenant LIMIT 1)
    FROM ready_tenants r WHERE r.tenant IS NOT NULL
),
due AS (
    SELECT r.tenant FROM ready_tenants r
    WHERE r.tenant IS NOT NULL
      AND EXISTS (SELECT 1 FROM jobqueue.jobs j
                  WHERE j.queue = $1 AND j.tenant = r.tenant AND j.state = 'ready'
                    AND j.run_at <= (SELECT now FROM clock))
)
SELECT (SELECT array_agg(due.tenant) FROM due),
       (SELECT j.run_at FROM jobqueue.jobs j
        WHERE j.queue = $1 AND j.state = 'ready' AND j.run_at > (SELECT now FROM clock)
        ORDER BY j.run_at LIMIT 1),
       (SELECT now FROM clock)`

// Activity implements store.Store.
func (s *Store) Activity(ctx context.Context, queue string) (store.Activity, error) {
	if err := job.ValidateName("queue", queue); err != nil {
		return store.Activity{}, err
	}
	var (
		tenants []string
		next    *time.Time
		now     time.Time
	)
	if err := s.pool.QueryRow(ctx, activitySQL, queue).Scan(&tenants, &next, &now); err != nil {
		return store.Activity{}, classify("activity", err)
	}
	slices.Sort(tenants) // byte order, whatever the database's collation
	act := store.Activity{Tenants: tenants}
	if next != nil {
		act.HasNextDue = true
		act.NextDueIn = job.NormalizeTime(*next).Sub(job.NormalizeTime(now))
	}
	return act, nil
}

// compareBytes orders strings by their bytes.
func compareBytes(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// listDeadSQL is a keyset page of a queue's dead jobs, read through jobs_dead.
//
//	$1 queue, $2 id to start after, $3 limit
const listDeadSQL = `SELECT ` + jobCols + ` FROM jobqueue.jobs j
WHERE j.queue = $1 AND j.state = 'dead' AND j.id > $2
ORDER BY j.id
LIMIT $3::int8`

// ListDead implements store.Store.
func (s *Store) ListDead(ctx context.Context, queue string, afterID int64, limit int) ([]job.Job, error) {
	if err := job.ValidateName("queue", queue); err != nil {
		return nil, err
	}
	if err := validateLimit(limit); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, listDeadSQL, queue, afterID, int64(limit))
	if err != nil {
		return nil, classify("list dead", err)
	}
	jobs, err := collectJobs(rows)
	if err != nil {
		return nil, classify("list dead", err)
	}
	return jobs, nil
}

// Representable instants of the database: PostgreSQL's timestamptz covers the
// years 4713 BC to 294276, but pgx refuses a time.Time outside what it can
// encode. No job finishes outside the years 1 to 9999, so a bound beyond them
// selects the same jobs as the nearest instant inside.
var (
	earliestBound = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	latestBound   = time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC)
)

// Purge implements store.Store.
func (s *Store) Purge(ctx context.Context, req store.PurgeRequest) (int, error) {
	if err := job.ValidateName("queue", req.Queue); err != nil {
		return 0, err
	}
	if !req.State.Terminal() {
		return 0, invalid("state", "must be a terminal state, got "+req.State.String())
	}
	before := job.NormalizeTime(req.FinishedBefore)
	if before.Before(earliestBound) {
		before = earliestBound
	}
	if before.After(latestBound) {
		before = latestBound
	}
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM jobqueue.jobs WHERE queue = $1 AND state = $2 AND finished_at < $3`,
		req.Queue, req.State.String(), before)
	if err != nil {
		return 0, classify("purge", err)
	}
	return int(tag.RowsAffected()), nil
}
