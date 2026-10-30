package pgstore

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

// leaseSQL builds a lease statement around a pick, the SELECT that chooses the
// ids to lease, which may use the clock CTE. The pick is a materialized CTE
// that locks its rows with SKIP LOCKED, so that the planner cannot evaluate it
// twice and get two different sets. ttl and worker are the numbers of the
// parameters that hold the TTL in microseconds and the worker id.
//
// A pick compares run_at with (SELECT now FROM clock), not with a join to the
// clock: the subquery is evaluated once and the planner can use it as the upper
// bound of the index range, whereas a join would read every scheduled job of
// the queue, in run_at order, and filter them one by one.
func leaseSQL(pick string, ttl, worker int) string {
	return fmt.Sprintf(`
WITH clock AS MATERIALIZED (SELECT jobqueue.now() AS now),
picked AS MATERIALIZED (%s)
UPDATE jobqueue.jobs AS j
SET state = 'leased', lease_epoch = j.lease_epoch + 1, attempts = j.attempts + 1,
    lease_expires_at = clock.now + $%d::int8 * interval '1 microsecond', leased_by = $%d::text
FROM picked, clock
WHERE j.id = picked.id
RETURNING %s, j.lease_expires_at - clock.now`, pick, ttl, worker, jobCols)
}

// tenantLeaseSQL leases for named tenants: for each, the oldest due jobs up to
// its limit, read through jobs_ready_tenant.
//
//	$1 queue, $2 tenants, $3 limits, $4 TTL in microseconds, $5 worker
var tenantLeaseSQL = leaseSQL(`
    SELECT p.id
    FROM unnest($2::text[], $3::int4[]) AS t(tenant, lim)
    CROSS JOIN LATERAL (
        SELECT j.id FROM jobqueue.jobs j
        WHERE j.queue = $1 AND j.tenant = t.tenant AND j.state = 'ready' AND j.run_at <= (SELECT now FROM clock)
        ORDER BY j.run_at, j.id
        LIMIT t.lim
        FOR UPDATE OF j SKIP LOCKED
    ) p`, 4, 5)

// fifoLeaseSQL leases the oldest due jobs of the queue whatever their tenant,
// read through jobs_ready_fifo.
//
//	$1 queue, $2 limit, $3 TTL in microseconds, $4 worker
var fifoLeaseSQL = leaseSQL(`
    SELECT j.id FROM jobqueue.jobs j
    WHERE j.queue = $1 AND j.state = 'ready' AND j.run_at <= (SELECT now FROM clock)
    ORDER BY j.run_at, j.id
    LIMIT $2::int4
    FOR UPDATE OF j SKIP LOCKED`, 3, 4)

// Lease implements store.Store. All takes are served by one statement, which
// also reads the clock once: a grant's Remaining is its expiry minus that one
// reading, so it is exactly the TTL.
func (s *Store) Lease(ctx context.Context, req store.LeaseRequest) ([]store.Grant, error) {
	if err := validateLease(req); err != nil {
		return nil, err
	}
	var (
		sql  string
		args []any
	)
	if isFIFO(req.Takes) {
		sql, args = fifoLeaseSQL, []any{req.Queue, int32(req.Takes[0].Limit), micros(req.TTL), req.Worker}
	} else {
		// Takes of one tenant, which the reference store serves one after the
		// other, are one take of their sum.
		limits := map[string]int32{}
		var tenants []string
		for _, tk := range req.Takes {
			if _, seen := limits[tk.Tenant]; !seen {
				tenants = append(tenants, tk.Tenant)
			}
			limits[tk.Tenant] += int32(tk.Limit)
		}
		perTenant := make([]int32, len(tenants))
		for i, tenant := range tenants {
			perTenant[i] = limits[tenant]
		}
		sql, args = tenantLeaseSQL, []any{req.Queue, tenants, perTenant, micros(req.TTL), req.Worker}
	}

	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, classify("lease", err)
	}
	defer rows.Close()
	var grants []store.Grant
	for rows.Next() {
		var remaining pgtype.Interval
		j, err := scanJob(rows, &remaining)
		if err != nil {
			return nil, classify("lease", err)
		}
		grants = append(grants, store.Grant{Job: j, Remaining: duration(remaining)})
	}
	if err := rows.Err(); err != nil {
		return nil, classify("lease", err)
	}
	// The statement returns the rows in no particular order; the contract is
	// (run_at, id) across all takes.
	slices.SortFunc(grants, func(a, b store.Grant) int { return byRunAtThenID(a.Job, b.Job) })
	return grants, nil
}

// byRunAtThenID orders jobs the way a lease serves them.
func byRunAtThenID(a, b job.Job) int {
	return cmp.Or(a.RunAt.Compare(b.RunAt), cmp.Compare(a.ID, b.ID))
}
