// Package store is the contract between the queue's brokers and the place
// its jobs are kept.
//
// A Store applies the transitions of package job to rows, and it is the only
// authority on time: every comparison against a lease expiry or a run_at uses
// the store's own clock, never the caller's. Two implementations exist, the
// PostgreSQL store for production and the in-memory store that serves as the
// reference model, and the conformance suite in package storetest holds both
// to the rules below.
//
// Rules that every method follows:
//   - It validates its whole input before writing anything. An invalid item
//     fails the whole call with an error that wraps job.ErrInvalid. The checks
//     are job.ValidateSpec for specs; job.ValidateBatch for batch sizes, with
//     job.MaxBatch for Enqueue and for the sum of the take limits of Lease and
//     job.MaxAckBatch for Heartbeat, Complete, Fail and Release;
//     job.ValidateTTL, job.ValidateDelay, job.ValidateResult and
//     job.ValidateWorkerID for the values they name; and job.ValidateName for
//     queue and tenant names.
//   - Per-item results line up with the items of the request.
//   - A Ref whose ID does not exist, or whose Queue differs from the job's,
//     yields job.NotFound for that item.
//   - Every item of an Enqueue consumes one id, a deduplicated one included,
//     and ids increase in request order.
//   - A Lease selects, for each take, the jobs of that tenant that are ready
//     with run_at <= now, ordered by (run_at, id), up to the take's limit. A
//     take whose tenant is empty selects across all tenants in the same
//     order. The grants come back in (run_at, id) order across the takes.
//   - Reap visits expired leases in ascending id, at most limit of them, and
//     calls the backoff function once per visited row, in that order. Prune
//     deletes prunable rows in ascending id, at most limit of them. Their
//     limits, and that of ListDead, are at least 1.
//   - A backoff delay and a retention period are not negative and are whole
//     microseconds, like every duration that crosses the boundary.
//   - A queue or tenant name that is not a valid name is rejected, in the
//     read and admin calls as well. The one exception is the empty tenant of
//     Get and Cancel, which means any tenant.
package store

import (
	"context"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
)

// Ref names a job on behalf of the worker that holds its lease.
type Ref struct {
	// ID is the job's id.
	ID int64
	// Queue is the queue the job was leased from; a Ref naming another queue
	// finds nothing.
	Queue string
	// Token is the fencing token of the lease, the job's Epoch when it was
	// leased.
	Token int64
}

// EnqueueResult is what became of one spec of an Enqueue call.
type EnqueueResult struct {
	// ID is the id of the new job, or of the job the spec's idempotency key
	// already named.
	ID int64
	// Deduplicated is true when the spec's idempotency key already named a
	// job, so that no job was created.
	Deduplicated bool
}

// TenantTake asks a Lease for up to Limit jobs of one tenant.
type TenantTake struct {
	// Tenant is the tenant to take from. A request holding a single take
	// with an empty Tenant takes in FIFO order across all tenants.
	Tenant string
	// Limit is the most jobs to take, at least 1.
	Limit int
}

// LeaseRequest asks for jobs of one queue on behalf of one worker.
type LeaseRequest struct {
	// Queue is the queue to lease from.
	Queue string
	// Takes say how many jobs to take from which tenants. Their limits sum
	// to at most job.MaxBatch.
	Takes []TenantTake
	// TTL is how long the leases last; see job.ValidateTTL.
	TTL time.Duration
	// Worker identifies the lease holder; see job.ValidateWorkerID.
	Worker string
}

// Grant is one job handed out by a Lease.
type Grant struct {
	// Job is the job as leased: its Epoch is the fencing token.
	Job job.Job
	// Remaining is the time from the store's now to Job.LeaseExpiresAt,
	// computed by the store. The holder measures its deadline from this, not
	// from a timestamp, so that no clock but its own monotonic one counts.
	Remaining time.Duration
}

// HeartbeatResult is what became of one Ref of a Heartbeat call.
type HeartbeatResult struct {
	// Outcome says whether the lease was extended.
	Outcome job.Outcome
	// Remaining is the time left on the extended lease. It is positive only
	// when Outcome is job.Applied.
	Remaining time.Duration
}

// CompleteRequest reports one job as done.
type CompleteRequest struct {
	// Ref names the job and the lease that did the work.
	Ref Ref
	// Result is what the worker returned; see job.ValidateResult.
	Result []byte
}

// FailRequest reports one job as failed.
type FailRequest struct {
	// Ref names the job and the lease that did the work.
	Ref Ref
	// Permanent sends the job to the dead state even if it has attempts left.
	Permanent bool
	// Delay is how long a retry waits; see job.ValidateDelay.
	Delay time.Duration
	// Error is the failure message; the store truncates it with
	// job.TruncateError.
	Error string
}

// BackoffFunc returns how long a job whose lease expired after the given
// number of attempts waits before its next one.
type BackoffFunc func(attempts int32) time.Duration

// Retention is how long finished jobs are kept.
type Retention struct {
	// Done is the retention of succeeded and cancelled jobs.
	Done time.Duration
	// Dead is the retention of dead jobs.
	Dead time.Duration
}

// PurgeRequest deletes finished jobs of one queue in one state.
type PurgeRequest struct {
	// Queue is the queue to purge.
	Queue string
	// State is the state of the jobs to delete; it must be terminal.
	State job.State
	// FinishedBefore selects the jobs that finished strictly before it.
	FinishedBefore time.Time
}

// TenantStats counts one tenant's jobs in one queue at the store's now.
type TenantStats struct {
	// Tenant is the tenant name.
	Tenant string
	// Ready counts jobs that are ready with run_at <= now.
	Ready int64
	// Scheduled counts jobs that are ready with run_at > now.
	Scheduled int64
	// Leased counts jobs that are leased.
	Leased int64
	// Dead counts jobs that are dead.
	Dead int64
	// OldestReadyAge is now minus the smallest run_at among the Ready jobs,
	// or 0 if there are none.
	OldestReadyAge time.Duration
}

// Activity says where a queue has work.
type Activity struct {
	// Tenants lists, in ascending order, the tenants that have at least one
	// Ready job whose run_at has come. A Lease for such a tenant at the same
	// instant gets a job, and it gets none for a tenant that is not listed,
	// unless a writer intervenes.
	Tenants []string
	// NextDueIn is the time from now to the earliest run_at among the
	// queue's Scheduled jobs. It is meaningful only when HasNextDue is true.
	NextDueIn time.Duration
	// HasNextDue is true when the queue has a Scheduled job.
	HasNextDue bool
}

// Store keeps the jobs of the queue and applies the transitions of package
// job to them. An implementation is safe for concurrent use.
//
// Methods that take a tenant treat an empty tenant as any tenant. Methods
// that take several items return one result per item, in request order.
type Store interface {
	// Enqueue creates one job for each spec, or finds the job that the
	// spec's idempotency key already names.
	Enqueue(ctx context.Context, specs []job.Spec) ([]EnqueueResult, error)
	// Lease hands ready jobs to a worker.
	Lease(ctx context.Context, req LeaseRequest) ([]Grant, error)
	// Heartbeat extends leases to the store's now plus ttl.
	Heartbeat(ctx context.Context, refs []Ref, ttl time.Duration) ([]HeartbeatResult, error)
	// Complete records finished jobs.
	Complete(ctx context.Context, reqs []CompleteRequest) ([]job.Outcome, error)
	// Fail records failed jobs, retrying those that have attempts left.
	Fail(ctx context.Context, reqs []FailRequest) ([]job.Outcome, error)
	// Release gives leased jobs back unrun, refunding their attempts.
	Release(ctx context.Context, refs []Ref) ([]job.Outcome, error)
	// Cancel stops a job that has not finished. A job of another tenant is
	// reported as job.NotFound and left alone.
	Cancel(ctx context.Context, id int64, tenant string) (job.Outcome, error)
	// Requeue revives a dead job.
	Requeue(ctx context.Context, id int64) (job.Outcome, error)
	// Reap takes back up to limit expired leases, delaying each retry by
	// what backoff returns for the job's attempts, and returns how many it
	// took back.
	Reap(ctx context.Context, limit int, backoff BackoffFunc) (int, error)
	// Prune deletes up to limit finished jobs that outlived the retention
	// and returns how many it deleted.
	Prune(ctx context.Context, r Retention, limit int) (int, error)
	// Purge deletes the finished jobs that req selects and returns how many.
	Purge(ctx context.Context, req PurgeRequest) (int, error)
	// Get returns a job. It returns ErrNotFound when the id is unknown or the
	// job belongs to another tenant.
	Get(ctx context.Context, id int64, tenant string) (job.Job, error)
	// ListDead returns up to limit dead jobs of a queue with an id above
	// afterID, in ascending id.
	ListDead(ctx context.Context, queue string, afterID int64, limit int) ([]job.Job, error)
	// Stats counts a queue's jobs per tenant, in ascending tenant order. A
	// tenant appears only while it has a ready, scheduled, leased or dead job.
	Stats(ctx context.Context, queue string) ([]TenantStats, error)
	// Activity says which tenants of a queue have due work and when the next
	// scheduled job falls due.
	Activity(ctx context.Context, queue string) (Activity, error)
}

// Snapshotter lists every row of a store. Tests and the simulator use it to
// check the store as a whole; the broker never does.
type Snapshotter interface {
	// Snapshot returns every row, in ascending id.
	Snapshot(ctx context.Context) ([]job.Job, error)
}
