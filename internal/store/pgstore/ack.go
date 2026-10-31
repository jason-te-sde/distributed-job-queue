package pgstore

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/flaw"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

// querier is what a pool and a transaction have in common.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// rounds splits the n items of an acknowledgement call into passes in which no
// job id occurs twice, and returns the indices of the items of each pass in
// request order. The k-th occurrence of an id goes to pass k. The reference
// store handles the items one after the other, so a job that appears twice sees
// the first item's effect when the second is handled; one UPDATE cannot do that
// for the same row, and it would pick either of the two items. A call without
// repeated ids, which is the usual one, is a single pass.
func rounds(n int, id func(i int) int64) [][]int {
	seen := make(map[int64]int, n)
	var out [][]int
	for i := range n {
		k := seen[id(i)]
		seen[id(i)] = k + 1
		if k == len(out) {
			out = append(out, nil)
		}
		out[k] = append(out[k], i)
	}
	return out
}

// acks runs one pass per round of an acknowledgement call. A call in one pass,
// the usual case, is a single statement and needs no transaction. Passes after
// the first see the effect of the earlier ones, so they run in one transaction
// that commits only if every pass succeeds.
func (s *Store) acks(ctx context.Context, op string, n int, id func(i int) int64, pass func(ctx context.Context, q querier, idx []int) error) error {
	rs := rounds(n, id)
	if len(rs) == 1 {
		return classify(op, pass(ctx, s.pool, rs[0]))
	}
	return classify(op, pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		for _, idx := range rs {
			if err := pass(ctx, tx, idx); err != nil {
				return err
			}
		}
		return nil
	}))
}

// loadSQL reads jobs by id together with the store's now.
//
//	$1 ids
const loadSQL = `
WITH clock AS MATERIALIZED (SELECT jobqueue.now() AS now)
SELECT ` + jobCols + `, clock.now
FROM jobqueue.jobs j, clock
WHERE j.id = ANY($1::int8[])`

// loadRows returns the jobs among ids that exist, by id, and the store's now.
// The time is zero when none exists, and nothing then needs it.
func loadRows(ctx context.Context, q querier, ids []int64) (map[int64]job.Job, time.Time, error) {
	rows, err := q.Query(ctx, loadSQL, ids)
	if err != nil {
		return nil, time.Time{}, err
	}
	defer rows.Close()
	found := make(map[int64]job.Job, len(ids))
	var now time.Time
	for rows.Next() {
		var at time.Time
		j, err := scanJob(rows, &at)
		if err != nil {
			return nil, time.Time{}, err
		}
		found[j.ID], now = j, job.NormalizeTime(at)
	}
	return found, now, rows.Err()
}

// settle gives an outcome to each item of a pass that its guarded UPDATE did
// not touch, by reading the jobs again and trying the item's transition on the
// row as it is now. applied[k] says that the UPDATE touched the item idx[k];
// the caller has recorded those. A ref that names no job, or a job of another
// queue, is NotFound. The transition's outcome is the item's, except that
// Applied cannot be: the UPDATE found nothing to write, so if the row now
// accepts the transition it changed in between, and then fallback, the
// outcome that the miss itself stood for, is reported.
func settle(ctx context.Context, q querier, idx []int, applied []bool, ref func(i int) store.Ref, fallback job.Outcome,
	try func(i int, j job.Job, now time.Time) job.Outcome, set func(i int, o job.Outcome)) error {
	var ids []int64
	for k, i := range idx {
		if !applied[k] {
			ids = append(ids, ref(i).ID)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	found, now, err := loadRows(ctx, q, ids)
	if err != nil {
		return err
	}
	for k, i := range idx {
		if applied[k] {
			continue
		}
		r := ref(i)
		j, ok := found[r.ID]
		if !ok || j.Queue != r.Queue {
			set(i, job.NotFound)
			continue
		}
		o := try(i, j, now)
		if o == job.Applied {
			o = fallback
		}
		set(i, o)
	}
	return nil
}

// ackGuard says which row an acknowledgement item may change: the item's job,
// in the item's queue, leased under the item's token. It is the WHERE of every
// acknowledgement statement and of the lock that precedes it.
const ackGuard = `j.id = r.id AND j.queue = r.queue AND j.state = 'leased' AND j.lease_epoch = r.token`

// lockAcked is the CTE that an acknowledgement statement starts its work with.
// It locks the rows that the statement is about to change, in ascending job id,
// before the statement looks at any of them. Two calls that name the same jobs
// in opposite orders then lock them in the same order and cannot deadlock. The
// order of a statement's own join is the planner's, and it changes with the
// statistics (an index lookup per item, or a hash join over a scan of the
// table), so it cannot be relied on. Only rows that the guard admits are
// locked: an item with a stale token neither waits for nor blocks anyone. The
// statement then changes only the rows named here, which it has already locked
// and which it reaches through the primary key. extra is more conditions on the
// rows, if the statement has any.
//
// The order holds within one statement, not across the passes of a call that
// names a job id more than once (see rounds). Those run one statement per pass
// in one transaction, and a pass locks whatever its guard admits, which can be
// a row that an earlier pass did not lock: the first item for an id carried a
// stale token and a later one the current token. Such a call takes its locks in
// no global order and can still deadlock against a call of a single pass that
// names the same jobs. PostgreSQL then aborts one of them with a deadlock
// error, which classify reports as store.ErrUnavailable, so the caller sees a
// retryable failure and never a hang.
func lockAcked(extra string) string {
	return `locked AS MATERIALIZED (
    SELECT j.id FROM jobqueue.jobs j, r, clock WHERE ` + ackGuard + extra + `
    ORDER BY j.id
    FOR UPDATE OF j
)`
}

// lockedOnly restricts an acknowledgement statement to the rows of lockAcked.
const lockedOnly = ` AND j.id = ANY(ARRAY(SELECT id FROM locked))`

// failSQL fails the leased jobs whose lease epoch is the caller's token: a job
// with attempts left that is not failed for good becomes ready again after its
// delay, any other becomes dead. The messages arrive already truncated, and an
// empty one is stored as NULL.
//
//	$1 ids, $2 queues, $3 tokens, $4 permanent, $5 delays in microseconds, $6 messages
var failSQL = `
WITH clock AS MATERIALIZED (SELECT jobqueue.now() AS now),
r AS (
    SELECT * FROM unnest($1::int8[], $2::text[], $3::int8[], $4::bool[], $5::int8[], $6::text[])
    WITH ORDINALITY AS r(id, queue, token, permanent, delay_us, err, ord)
),
` + lockAcked("") + `
UPDATE jobqueue.jobs AS j
SET state = CASE WHEN NOT r.permanent AND j.attempts < j.max_attempts THEN 'ready' ELSE 'dead' END,
    run_at = CASE WHEN NOT r.permanent AND j.attempts < j.max_attempts
                  THEN clock.now + r.delay_us * interval '1 microsecond' ELSE j.run_at END,
    finished_at = CASE WHEN NOT r.permanent AND j.attempts < j.max_attempts THEN NULL ELSE clock.now END,
    last_error = nullif(r.err, ''),
    lease_expires_at = NULL, leased_by = NULL
FROM r, clock
WHERE ` + ackGuard + lockedOnly + `
RETURNING r.ord`

// Fail implements store.Store.
func (s *Store) Fail(ctx context.Context, reqs []store.FailRequest) ([]job.Outcome, error) {
	if err := job.ValidateBatch("failures", len(reqs), job.MaxAckBatch); err != nil {
		return nil, err
	}
	for _, r := range reqs {
		if err := job.ValidateName("queue", r.Ref.Queue); err != nil {
			return nil, err
		}
		if err := job.ValidateDelay("delay", r.Delay); err != nil {
			return nil, err
		}
	}
	out := make([]job.Outcome, len(reqs))
	err := s.acks(ctx, "fail", len(reqs), func(i int) int64 { return reqs[i].Ref.ID },
		func(ctx context.Context, q querier, idx []int) error {
			n := len(idx)
			ids, queues, tokens := make([]int64, n), make([]string, n), make([]int64, n)
			permanent, delays, messages := make([]bool, n), make([]int64, n), make([]string, n)
			for k, i := range idx {
				r := reqs[i]
				ids[k], queues[k], tokens[k] = r.Ref.ID, r.Ref.Queue, r.Ref.Token
				permanent[k], delays[k], messages[k] = r.Permanent, micros(r.Delay), job.TruncateError(r.Error)
			}
			rows, err := q.Query(ctx, failSQL, ids, queues, tokens, permanent, delays, messages)
			if err != nil {
				return err
			}
			ords, err := pgx.CollectRows(rows, pgx.RowTo[int64])
			if err != nil {
				return err
			}
			applied := make([]bool, n)
			for _, ord := range ords {
				applied[ord-1] = true
				out[idx[ord-1]] = job.Applied
			}
			return settle(ctx, q, idx, applied,
				func(i int) store.Ref { return reqs[i].Ref }, job.Fenced,
				func(i int, j job.Job, now time.Time) job.Outcome {
					r := reqs[i]
					_, o := job.Fail(j, r.Ref.Token, now, r.Permanent, r.Delay, r.Error)
					return o
				},
				func(i int, o job.Outcome) { out[i] = o })
		})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// heartbeatSQL extends the leases that are held under the caller's token and
// have not run out, and returns the time left on each. Flaw F4PG drops the
// token comparison.
//
//	$1 ids, $2 queues, $3 tokens, $4 TTL in microseconds
var heartbeatSQL = without(flaw.F4PG, `
WITH clock AS MATERIALIZED (SELECT jobqueue.now() AS now),
r AS (
    SELECT * FROM unnest($1::int8[], $2::text[], $3::int8[]) WITH ORDINALITY AS r(id, queue, token, ord)
),
`+lockAcked(" AND j.lease_expires_at > clock.now")+`
UPDATE jobqueue.jobs AS j
SET lease_expires_at = clock.now + $4::int8 * interval '1 microsecond'
FROM r, clock
WHERE `+ackGuard+`
  AND j.lease_expires_at > clock.now`+lockedOnly+`
RETURNING r.ord, j.lease_expires_at - clock.now`, " AND j.lease_epoch = r.token")

// validateRefs checks the refs of a Heartbeat or Release call.
func validateRefs(refs []store.Ref) error {
	if err := job.ValidateBatch("refs", len(refs), job.MaxAckBatch); err != nil {
		return err
	}
	for _, r := range refs {
		if err := job.ValidateName("queue", r.Queue); err != nil {
			return err
		}
	}
	return nil
}

// Heartbeat implements store.Store.
func (s *Store) Heartbeat(ctx context.Context, refs []store.Ref, ttl time.Duration) ([]store.HeartbeatResult, error) {
	if err := validateRefs(refs); err != nil {
		return nil, err
	}
	if err := job.ValidateTTL(ttl); err != nil {
		return nil, err
	}
	out := make([]store.HeartbeatResult, len(refs))
	err := s.acks(ctx, "heartbeat", len(refs), func(i int) int64 { return refs[i].ID },
		func(ctx context.Context, q querier, idx []int) error {
			n := len(idx)
			ids, queues, tokens := make([]int64, n), make([]string, n), make([]int64, n)
			for k, i := range idx {
				ids[k], queues[k], tokens[k] = refs[i].ID, refs[i].Queue, refs[i].Token
			}
			rows, err := q.Query(ctx, heartbeatSQL, ids, queues, tokens, micros(ttl))
			if err != nil {
				return err
			}
			type extended struct {
				ord       int64
				remaining pgtype.Interval
			}
			hits, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (extended, error) {
				var e extended
				return e, row.Scan(&e.ord, &e.remaining)
			})
			if err != nil {
				return err
			}
			applied := make([]bool, n)
			for _, e := range hits {
				applied[e.ord-1] = true
				out[idx[e.ord-1]] = store.HeartbeatResult{Outcome: job.Applied, Remaining: duration(e.remaining)}
			}
			return settle(ctx, q, idx, applied,
				func(i int) store.Ref { return refs[i] }, job.Fenced,
				func(i int, j job.Job, now time.Time) job.Outcome {
					_, o := job.Heartbeat(j, refs[i].Token, now, ttl)
					return o
				},
				func(i int, o job.Outcome) { out[i] = store.HeartbeatResult{Outcome: o} })
		})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// completeSQL records the leased jobs whose lease epoch is the caller's token
// as succeeded. It does not look at the lease's expiry: finished work is kept
// as long as nobody else has leased the job since. A result of no bytes
// arrives as NULL. Flaw F1PG drops the token comparison.
//
//	$1 ids, $2 queues, $3 tokens, $4 results
var completeSQL = without(flaw.F1PG, `
WITH clock AS MATERIALIZED (SELECT jobqueue.now() AS now),
r AS (
    SELECT * FROM unnest($1::int8[], $2::text[], $3::int8[], $4::bytea[]) WITH ORDINALITY AS r(id, queue, token, result, ord)
),
`+lockAcked("")+`
UPDATE jobqueue.jobs AS j
SET state = 'succeeded', result = r.result, finished_at = clock.now, lease_expires_at = NULL, leased_by = NULL
FROM r, clock
WHERE `+ackGuard+lockedOnly+`
RETURNING r.ord`, " AND j.lease_epoch = r.token")

// Complete implements store.Store.
func (s *Store) Complete(ctx context.Context, reqs []store.CompleteRequest) ([]job.Outcome, error) {
	if err := job.ValidateBatch("completions", len(reqs), job.MaxAckBatch); err != nil {
		return nil, err
	}
	for _, r := range reqs {
		if err := job.ValidateName("queue", r.Ref.Queue); err != nil {
			return nil, err
		}
		if err := job.ValidateResult(r.Result); err != nil {
			return nil, err
		}
	}
	out := make([]job.Outcome, len(reqs))
	err := s.acks(ctx, "complete", len(reqs), func(i int) int64 { return reqs[i].Ref.ID },
		func(ctx context.Context, q querier, idx []int) error {
			n := len(idx)
			ids, queues, tokens, results := make([]int64, n), make([]string, n), make([]int64, n), make([][]byte, n)
			for k, i := range idx {
				r := reqs[i]
				ids[k], queues[k], tokens[k] = r.Ref.ID, r.Ref.Queue, r.Ref.Token
				if len(r.Result) > 0 {
					results[k] = r.Result // an empty result stays nil, which is NULL
				}
			}
			rows, err := q.Query(ctx, completeSQL, ids, queues, tokens, results)
			if err != nil {
				return err
			}
			ords, err := pgx.CollectRows(rows, pgx.RowTo[int64])
			if err != nil {
				return err
			}
			applied := make([]bool, n)
			for _, ord := range ords {
				applied[ord-1] = true
				out[idx[ord-1]] = job.Applied
			}
			return settle(ctx, q, idx, applied,
				func(i int) store.Ref { return reqs[i].Ref }, job.Fenced,
				func(i int, j job.Job, now time.Time) job.Outcome {
					_, o := job.Complete(j, reqs[i].Ref.Token, now, nil)
					return o
				},
				func(i int, o job.Outcome) { out[i] = o })
		})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// releaseSQL gives the leased jobs whose lease epoch is the caller's token
// back unrun: the attempt is refunded and the job is ready at once. Each queue
// and tenant that gained a ready job is announced once, in the same
// transaction.
//
//	$1 ids, $2 queues, $3 tokens
var releaseSQL = `
WITH clock AS MATERIALIZED (SELECT jobqueue.now() AS now),
r AS (
    SELECT * FROM unnest($1::int8[], $2::text[], $3::int8[]) WITH ORDINALITY AS r(id, queue, token, ord)
),
` + lockAcked("") + `,
released AS (
    UPDATE jobqueue.jobs AS j
    SET state = 'ready', run_at = clock.now, attempts = j.attempts - 1, lease_expires_at = NULL, leased_by = NULL
    FROM r, clock
    WHERE ` + ackGuard + lockedOnly + `
    RETURNING r.ord, j.queue, j.tenant
),
wake AS (
    SELECT pg_notify('` + NotifyChannel + `', w.queue || chr(9) || w.tenant)
    FROM (SELECT DISTINCT queue, tenant FROM released) w
)
SELECT released.ord FROM released, (SELECT count(*) FROM wake) AS woken`

// Release implements store.Store.
func (s *Store) Release(ctx context.Context, refs []store.Ref) ([]job.Outcome, error) {
	if err := validateRefs(refs); err != nil {
		return nil, err
	}
	out := make([]job.Outcome, len(refs))
	err := s.acks(ctx, "release", len(refs), func(i int) int64 { return refs[i].ID },
		func(ctx context.Context, q querier, idx []int) error {
			n := len(idx)
			ids, queues, tokens := make([]int64, n), make([]string, n), make([]int64, n)
			for k, i := range idx {
				ids[k], queues[k], tokens[k] = refs[i].ID, refs[i].Queue, refs[i].Token
			}
			rows, err := q.Query(ctx, releaseSQL, ids, queues, tokens)
			if err != nil {
				return err
			}
			ords, err := pgx.CollectRows(rows, pgx.RowTo[int64])
			if err != nil {
				return err
			}
			applied := make([]bool, n)
			for _, ord := range ords {
				applied[ord-1] = true
				out[idx[ord-1]] = job.Applied
			}
			return settle(ctx, q, idx, applied,
				func(i int) store.Ref { return refs[i] }, job.Fenced,
				func(i int, j job.Job, now time.Time) job.Outcome {
					_, o := job.Release(j, refs[i].Token, now)
					return o
				},
				func(i int, o job.Outcome) { out[i] = o })
		})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// settleOne is settle for the single job of Cancel and Requeue. They name no
// queue, and Cancel may name a tenant, which a job of another tenant does not
// match: that job is NotFound, and is left alone.
func (s *Store) settleOne(ctx context.Context, id int64, tenant string, fallback job.Outcome,
	try func(j job.Job, now time.Time) job.Outcome) (job.Outcome, error) {
	found, now, err := loadRows(ctx, s.pool, []int64{id})
	if err != nil {
		return 0, err
	}
	j, ok := found[id]
	if !ok || tenant != "" && j.Tenant != tenant {
		return job.NotFound, nil
	}
	if o := try(j, now); o != job.Applied {
		return o, nil
	}
	return fallback, nil
}

// cancelSQL cancels a job that has not finished, if it belongs to the tenant
// when one is named.
//
//	$1 id, $2 tenant or ''
const cancelSQL = `
WITH clock AS MATERIALIZED (SELECT jobqueue.now() AS now)
UPDATE jobqueue.jobs AS j
SET state = 'cancelled', finished_at = clock.now, lease_expires_at = NULL, leased_by = NULL
FROM clock
WHERE j.id = $1 AND ($2::text = '' OR j.tenant = $2::text) AND j.state IN ('ready', 'leased')
RETURNING j.id`

// Cancel implements store.Store.
func (s *Store) Cancel(ctx context.Context, id int64, tenant string) (job.Outcome, error) {
	if err := validateTenantScope(tenant); err != nil {
		return 0, err
	}
	var hit int64
	switch err := s.pool.QueryRow(ctx, cancelSQL, id, tenant).Scan(&hit); {
	case err == nil:
		return job.Applied, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return 0, classify("cancel", err)
	}
	out, err := s.settleOne(ctx, id, tenant, job.WrongState,
		func(j job.Job, now time.Time) job.Outcome { _, o := job.Cancel(j, now); return o })
	return out, classify("cancel", err)
}

// requeueSQL revives a dead job for another run at once, with a fresh attempt
// budget, and announces its queue and tenant in the same transaction.
//
//	$1 id
const requeueSQL = `
WITH clock AS MATERIALIZED (SELECT jobqueue.now() AS now),
revived AS (
    UPDATE jobqueue.jobs AS j
    SET state = 'ready', attempts = 0, run_at = clock.now, finished_at = NULL, result = NULL
    FROM clock
    WHERE j.id = $1 AND j.state = 'dead'
    RETURNING j.id, j.queue, j.tenant
),
wake AS (
    SELECT pg_notify('` + NotifyChannel + `', revived.queue || chr(9) || revived.tenant) FROM revived
)
SELECT revived.id FROM revived, (SELECT count(*) FROM wake) AS woken`

// Requeue implements store.Store.
func (s *Store) Requeue(ctx context.Context, id int64) (job.Outcome, error) {
	var hit int64
	switch err := s.pool.QueryRow(ctx, requeueSQL, id).Scan(&hit); {
	case err == nil:
		return job.Applied, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return 0, classify("requeue", err)
	}
	out, err := s.settleOne(ctx, id, "", job.WrongState,
		func(j job.Job, now time.Time) job.Outcome { _, o := job.Requeue(j, now); return o })
	return out, classify("requeue", err)
}
