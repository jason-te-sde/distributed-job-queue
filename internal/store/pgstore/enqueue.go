package pgstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

// insertSQL inserts one job and returns its id, or nothing when its
// idempotency key already names a job. The id is taken even then: every item
// of an Enqueue consumes one. A job that is ready to run at once also wakes
// the listeners of its queue and tenant. PostgreSQL delivers equal payloads
// of one transaction once, so a batch for one queue and tenant wakes them once.
//
//	$1 queue, $2 tenant, $3 payload, $4 run_at or NULL, $5 delay in microseconds,
//	$6 max_attempts, $7 idempotency key or NULL
const insertSQL = `
WITH clock AS MATERIALIZED (SELECT jobqueue.now() AS now),
ins AS (
    INSERT INTO jobqueue.jobs (queue, tenant, state, payload, run_at, attempts, max_attempts, idempotency_key, created_at)
    SELECT $1::text, $2::text, 'ready', $3::bytea,
           greatest(coalesce($4::timestamptz, clock.now), clock.now) + $5::int8 * interval '1 microsecond',
           0, $6::int4, $7::text, clock.now
    FROM clock
    ON CONFLICT (tenant, queue, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
    RETURNING id, queue, tenant, run_at
),
wake AS (
    SELECT pg_notify('` + NotifyChannel + `', ins.queue || chr(9) || ins.tenant)
    FROM ins, clock
    WHERE ins.run_at <= clock.now
)
SELECT ins.id FROM ins, (SELECT count(*) FROM wake) AS woken`

// resolveSQL finds the jobs that idempotency keys name.
//
//	$1 tenants, $2 queues, $3 keys
const resolveSQL = `
SELECT j.tenant, j.queue, j.idempotency_key, j.id
FROM jobqueue.jobs j
WHERE (j.tenant, j.queue, j.idempotency_key) IN (SELECT * FROM unnest($1::text[], $2::text[], $3::text[]))`

// scope is where an idempotency key is unique.
type scope struct{ tenant, queue, key string }

// Enqueue implements store.Store. The whole batch is one transaction sent in
// one round trip: an insert per item, in request order, then one read that
// resolves the items whose key already named a job. An item that repeats an
// earlier item's key within the batch resolves to that item's job.
func (s *Store) Enqueue(ctx context.Context, specs []job.Spec) ([]store.EnqueueResult, error) {
	if err := job.ValidateBatch("specs", len(specs), job.MaxBatch); err != nil {
		return nil, err
	}
	for _, sp := range specs {
		if err := job.ValidateSpec(sp); err != nil {
			return nil, err
		}
	}

	batch := &pgx.Batch{}
	var tenants, queues, keys []string
	for _, sp := range specs {
		payload := sp.Payload
		if payload == nil {
			payload = []byte{} // a nil slice would be stored as NULL
		}
		var runAt any
		if !sp.RunAt.IsZero() {
			runAt = job.NormalizeTime(sp.RunAt)
		}
		maxAttempts := sp.MaxAttempts
		if maxAttempts == 0 {
			maxAttempts = job.DefaultMaxAttempts
		}
		batch.Queue(insertSQL, sp.Queue, sp.Tenant, payload, runAt, micros(sp.Delay), maxAttempts, nullable(sp.IdempotencyKey))
		if sp.IdempotencyKey != "" {
			tenants, queues, keys = append(tenants, sp.Tenant), append(queues, sp.Queue), append(keys, sp.IdempotencyKey)
		}
	}
	if len(keys) > 0 {
		batch.Queue(resolveSQL, tenants, queues, keys)
	}

	results, missing, err := s.runEnqueue(ctx, batch, specs, len(keys) > 0)
	if err != nil {
		return nil, classify("enqueue", err)
	}
	if len(missing) > 0 {
		if err := s.resolveLate(ctx, specs, results, missing); err != nil {
			return nil, err
		}
	}
	return results, nil
}

// runEnqueue sends the batch and reads its results. The batch has been applied
// when it returns without an error, because a batch commits as a whole when its
// last result has been read. It also returns the items that were deduplicated
// and whose job the batch's own read-back did not find.
func (s *Store) runEnqueue(ctx context.Context, batch *pgx.Batch, specs []job.Spec, resolve bool) (_ []store.EnqueueResult, _ []int, err error) {
	br := s.pool.SendBatch(ctx, batch)
	defer func() {
		// The batch commits when its last result has been read and the
		// connection reports ready, which is what Close waits for.
		if closeErr := br.Close(); err == nil {
			err = closeErr
		}
	}()

	out := make([]store.EnqueueResult, len(specs))
	for i := range specs {
		var id int64
		switch err := br.QueryRow().Scan(&id); {
		case err == nil:
			out[i].ID = id
		case errors.Is(err, pgx.ErrNoRows):
			out[i].Deduplicated = true
		default:
			return nil, nil, err
		}
	}
	if !resolve {
		return out, nil, nil
	}

	rows, err := br.Query()
	if err != nil {
		return nil, nil, err
	}
	named, err := scanNamed(rows)
	if err != nil {
		return nil, nil, err
	}
	var missing []int
	for i, sp := range specs {
		if !out[i].Deduplicated {
			continue
		}
		id, ok := named[scope{sp.Tenant, sp.Queue, sp.IdempotencyKey}]
		if !ok {
			missing = append(missing, i)
			continue
		}
		out[i].ID = id
	}
	return out, missing, nil
}

// scanNamed reads the rows of resolveSQL.
func scanNamed(rows pgx.Rows) (map[scope]int64, error) {
	defer rows.Close()
	named := map[scope]int64{}
	for rows.Next() {
		var sc scope
		var id int64
		if err := rows.Scan(&sc.tenant, &sc.queue, &sc.key, &id); err != nil {
			return nil, err
		}
		named[sc] = id
	}
	return named, rows.Err()
}

// resolveLate finds the jobs of the missing items, which the batch found to be
// duplicates of a job that its own read-back then did not see: the job was
// deleted between the two statements, or committed only after the read-back
// began. It runs after the batch has committed, so whatever it finds is the
// job that holds the key now. If some key is still held by no job, the batch
// cannot be undone, and the error says so: the caller must not send the whole
// batch again, which would create its unkeyed jobs a second time.
func (s *Store) resolveLate(ctx context.Context, specs []job.Spec, out []store.EnqueueResult, missing []int) error {
	var tenants, queues, keys []string
	for _, i := range missing {
		tenants, queues, keys = append(tenants, specs[i].Tenant), append(queues, specs[i].Queue), append(keys, specs[i].IdempotencyKey)
	}
	rows, err := s.pool.Query(ctx, resolveSQL, tenants, queues, keys)
	var named map[scope]int64
	if err == nil {
		named, err = scanNamed(rows)
	}
	if err != nil {
		return fmt.Errorf("pgstore enqueue: the batch was applied, but reading back the jobs of %d duplicate items failed, "+
			"so do not send the whole batch again: %w", len(missing), err)
	}
	var gone []int
	for _, i := range missing {
		id, ok := named[scope{specs[i].Tenant, specs[i].Queue, specs[i].IdempotencyKey}]
		if !ok {
			gone = append(gone, i)
			continue
		}
		out[i].ID = id
	}
	if len(gone) > 0 {
		return fmt.Errorf("pgstore enqueue: the batch was applied, but no job holds the idempotency key of item(s) %v any more "+
			"(it was deleted after the item was found to duplicate it), so do not send the whole batch again; "+
			"send only those items", gone)
	}
	return nil
}
