package pgstore

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
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
}

var (
	_ store.Store       = (*Store)(nil)
	_ store.Snapshotter = (*Store)(nil)
)

// New returns a Store that runs its statements on pool. The pool is the
// caller's: closing it is the caller's business too, and afterwards every
// call fails with an error that wraps store.ErrUnavailable. The database must
// already be migrated; see package migrate.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

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

// errUnresolved is returned when a job that an idempotency key named has gone.
func errUnresolved(item int) error {
	return fmt.Errorf("%w: item %d was neither inserted nor found under its idempotency key, "+
		"because the job that held the key was deleted in between; retry",
		store.ErrUnavailable, item)
}
