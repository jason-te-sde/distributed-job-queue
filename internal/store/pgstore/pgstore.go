package pgstore

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/flaw"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

// NotifyChannel is the channel on which the store announces that jobs have
// become ready to run.
const NotifyChannel = "jobqueue_ready"

// NotifyPayload is the payload of a notification for the given queue and
// tenant: the two names joined by a tab. The names cannot contain a tab,
// because job.ValidateName admits only letters, digits and "_.-".
func NotifyPayload(queue, tenant string) string { return queue + "\t" + tenant }

// ParseNotifyPayload is the inverse of NotifyPayload. It reports false unless
// s holds exactly one tab and both parts are valid names. Anyone with access
// to the database can send a notification, so a listener must not trust one.
func ParseNotifyPayload(s string) (queue, tenant string, ok bool) {
	queue, tenant, found := strings.Cut(s, "\t")
	if !found {
		return "", "", false
	}
	// A second tab would be part of the tenant, which no valid name contains.
	if job.ValidateName("queue", queue) != nil || job.ValidateName("tenant", tenant) != nil {
		return "", "", false
	}
	return queue, tenant, true
}

// Store is a store.Store on PostgreSQL. It is safe for concurrent use.
type Store struct {
	pool *pgxpool.Pool
	// notifying bounds the connections that the calls that notify hold at once.
	// See the comment of gate.
	notifying *gate
}

var (
	_ store.Store       = (*Store)(nil)
	_ store.Snapshotter = (*Store)(nil)
)

// Option configures a Store.
type Option func(*options)

type options struct{ maxNotifying int }

// WithMaxNotifying sets the most connections of the pool that calls which
// notify may hold at once: Enqueue, Release and Requeue, whose transactions
// queue on the lock that PostgreSQL takes for every committing NOTIFY. A call
// over the bound waits for another to finish before it takes a connection, so
// the connections above the bound stay free for the calls that do not notify,
// Lease and Activity among them.
//
// A value below 1 selects the default, all but a reserve of max(2, poolMax/4)
// connections of the pool; see defaultMaxNotifying for the reasoning and for
// what a small pool leaves over. A value that would leave no connection to the
// calls that do not notify, the size of the pool or more, is lowered to the
// pool size minus one, because a bound as large as the pool turns itself off. A
// pool of one connection cannot spare one, and its bound is that connection. The
// broker rejects such values instead of lowering them; MaxNotifying reports the
// bound in effect.
func WithMaxNotifying(n int) Option { return func(o *options) { o.maxNotifying = n } }

// New returns a Store that runs its statements on pool. The pool is the
// caller's: closing it is the caller's business too, and afterwards every
// call fails with an error that wraps store.ErrUnavailable. The database must
// already be migrated; see package migrate.
//
// The calls that notify hold at most a bounded share of the pool's
// connections, so that the others always find one; see WithMaxNotifying. A call
// that waits for its turn under the bound fails with its context's error if the
// context ends first, as a call that waits for a connection does.
func New(pool *pgxpool.Pool, opts ...Option) *Store {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	poolMax := int(pool.Config().MaxConns)
	n := o.maxNotifying
	if n < 1 {
		n = defaultMaxNotifying(poolMax)
	}
	return &Store{pool: pool, notifying: newGate(min(n, notifyingCeiling(poolMax)))}
}

// MaxNotifying returns the most connections of the pool that calls which
// notify (Enqueue, Release and Requeue) may hold at once: the bound that
// WithMaxNotifying set, or the default, after it was brought within the pool.
func (s *Store) MaxNotifying() int { return s.notifying.capacity() }

// Open connects to the database at url and returns a pool for New. It holds
// at most maxConns connections, or pgxpool's default when maxConns is below 1,
// marks them with the application name "jobqueue", and pings the server before
// it returns.
func Open(ctx context.Context, url string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("pgstore open: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	cfg.ConnConfig.RuntimeParams["application_name"] = "jobqueue"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, classify("open", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, classify("open", err)
	}
	return pool, nil
}

// invalid builds the error for an argument that is out of range.
func invalid(field, reason string) error { return &job.InvalidError{Field: field, Reason: reason} }

// validateLimit checks the limit of Reap, Prune and ListDead.
func validateLimit(limit int) error {
	if limit < 1 {
		return invalid("limit", "must be at least 1, got "+strconv.Itoa(limit))
	}
	return nil
}

// validateRetention checks one retention period: not negative, and whole
// microseconds like every duration that crosses the store boundary.
func validateRetention(field string, d time.Duration) error {
	if d < 0 || d%time.Microsecond != 0 {
		return invalid(field, "must be a non-negative whole number of microseconds, got "+d.String())
	}
	return nil
}

// validateLease checks a lease request as a whole.
func validateLease(req store.LeaseRequest) error {
	if err := job.ValidateName("queue", req.Queue); err != nil {
		return err
	}
	if err := job.ValidateTTL(req.TTL); err != nil {
		return err
	}
	if err := job.ValidateWorkerID(req.Worker); err != nil {
		return err
	}
	if err := job.ValidateBatch("takes", len(req.Takes), job.MaxBatch); err != nil {
		return err
	}
	fifo := isFIFO(req.Takes)
	total := 0
	for _, tk := range req.Takes {
		if !fifo {
			if err := job.ValidateName("tenant", tk.Tenant); err != nil {
				return err
			}
		}
		// Each limit is checked on its own before the sum, so that a
		// negative limit cannot hide a large one.
		if err := job.ValidateBatch("limit", tk.Limit, job.MaxBatch); err != nil {
			return err
		}
		total += tk.Limit
	}
	return job.ValidateBatch("limit", total, job.MaxBatch)
}

// isFIFO reports whether takes is the one take of an empty tenant, which
// serves jobs in order across all tenants.
func isFIFO(takes []store.TenantTake) bool { return len(takes) == 1 && takes[0].Tenant == "" }

// validateTenantScope checks the tenant argument of Get and Cancel, where an
// empty tenant means any tenant.
func validateTenantScope(tenant string) error {
	if tenant == "" {
		return nil
	}
	return job.ValidateName("tenant", tenant)
}

// without returns sql, or sql with every occurrence of clause removed when the
// planted flaw id is on. That is how the flaw sites in the statements work:
// each names the one clause whose absence is the defect, which a statement may
// state more than once (a lock and the update that follows it). Without the
// jobqueue_flaws build tag no flaw is ever on and sql comes back unchanged. A
// clause that sql does not contain panics, so that a site that has drifted from
// its statement cannot quietly stop testing anything.
func without(id flaw.ID, sql, clause string) string {
	if !flaw.On(id) {
		return sql
	}
	if !strings.Contains(sql, clause) {
		panic("pgstore: flaw " + string(id) + ": the statement has no clause " + strconv.Quote(clause))
	}
	return strings.ReplaceAll(sql, clause, "")
}
