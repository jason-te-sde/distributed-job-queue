package broker

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	jobqueuev1 "github.com/jason-te-sde/distributed-job-queue/internal/gen/jobqueue/v1"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

const (
	// defaultWait is how long a Lease waits for work when the request names
	// no wait, and maxWait is the longest it ever waits.
	defaultWait = 20 * time.Second
	maxWait     = 30 * time.Second
	// defaultTenant is the tenant of a job that an admin enqueues without
	// one.
	defaultTenant = "default"
	// defaultDeadPage is the size of a ListDeadJobs page when the request
	// names none. A page never exceeds job.MaxBatch jobs: a dead job carries
	// a payload of up to 256 KiB, and 64 of them still fit in the 20 MiB
	// message limit.
	defaultDeadPage = 50
)

// LeaseCall asks the dispatcher for jobs on behalf of one worker.
type LeaseCall struct {
	// Queue is the queue to lease from.
	Queue string
	// Max is the most jobs to return, from 1 to job.MaxBatch.
	Max int
	// Wait is how long to wait for work when there is none, from 0 to 30s.
	Wait time.Duration
	// TTL is how long the leases last; see job.ValidateTTL.
	TTL time.Duration
	// Worker identifies the lease holder; see job.ValidateWorkerID.
	Worker string
}

// LeaseAnswer is what a dispatcher found for a LeaseCall.
type LeaseAnswer struct {
	// Grants are the jobs leased, possibly none.
	Grants []store.Grant
	// Waited is how long the call waited.
	Waited time.Duration
}

// LeaseDispatcher serves Lease calls, waiting for work and sharing it
// between tenants. Its errors are already gRPC statuses.
type LeaseDispatcher interface {
	// Lease returns up to c.Max jobs of c.Queue, waiting up to c.Wait for
	// the first one, and returns early when ctx is done.
	Lease(ctx context.Context, c LeaseCall) (LeaseAnswer, error)
}

// Server implements the gRPC API of the queue over a store. Every handler
// validates its whole request first, then authorizes the caller from the
// Principal that UnaryAuth put in the context, and only then calls the store,
// so that a request that is malformed or not allowed never reaches it.
type Server struct {
	jobqueuev1.UnimplementedJobQueueServiceServer

	st         store.Store
	dispatcher LeaseDispatcher
	metrics    *Metrics
	log        *slog.Logger
	backoff    job.BackoffPolicy

	// jitter draws the random part of a retry's backoff. The handlers run
	// concurrently and a generator is not safe for that, hence the mutex.
	jitterMu sync.Mutex
	jitter   *rand.Rand

	draining atomic.Bool
}

var _ jobqueuev1.JobQueueServiceServer = (*Server)(nil)

// NewServer returns a Server over st that leases through d. Backoff jitter is
// drawn from a generator seeded with seed, so that a run is reproducible. It
// panics if any other argument is nil.
func NewServer(st store.Store, d LeaseDispatcher, m *Metrics, log *slog.Logger, seed uint64) *Server {
	switch {
	case st == nil:
		panic("broker.NewServer: store must not be nil")
	case d == nil:
		panic("broker.NewServer: dispatcher must not be nil")
	case m == nil:
		panic("broker.NewServer: metrics must not be nil")
	case log == nil:
		panic("broker.NewServer: logger must not be nil")
	}
	return &Server{
		st: st, dispatcher: d, metrics: m, log: log,
		backoff: job.DefaultBackoff,
		jitter:  rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
	}
}

// SetDraining makes every Lease from now on fail with codes.Unavailable and
// "broker is draining", so that clients move to another broker. Everything
// else keeps working: workers must still be able to report on what they hold.
func (s *Server) SetDraining() { s.draining.Store(true) }

// fail maps err to the status a handler of operation op returns, logging it
// first if it is an internal error.
func (s *Server) fail(op string, err error) error { return toStatusLogged(s.log, op, err) }

// backoffDelay draws the delay of a retry after the given attempt.
func (s *Server) backoffDelay(attempt int32) time.Duration {
	s.jitterMu.Lock()
	u := s.jitter.Float64()
	s.jitterMu.Unlock()
	return s.backoff.Delay(attempt, u)
}

// Enqueue creates the jobs of a batch. A producer may enqueue only for its own
// tenant, which is also the tenant of an item that names none; an admin's
// items without a tenant go to "default".
func (s *Server) Enqueue(ctx context.Context, req *jobqueuev1.EnqueueRequest) (*jobqueuev1.EnqueueResponse, error) {
	const op = "enqueue"
	items := req.GetItems()
	if err := job.ValidateBatch("items", len(items), job.MaxBatch); err != nil {
		return nil, s.fail(op, err)
	}
	specs := make([]job.Spec, len(items))
	for i, it := range items {
		spec, err := specOf(it)
		if err == nil {
			// An empty tenant is valid here: it is filled in below, once the
			// caller is known. Check the rest of the spec with a stand-in.
			check := spec
			if check.Tenant == "" {
				check.Tenant = defaultTenant
			}
			err = job.ValidateSpec(check)
		}
		if err != nil {
			return nil, s.fail(op, fmt.Errorf("items[%d]: %w", i, err))
		}
		specs[i] = spec
	}

	p := PrincipalFrom(ctx)
	if err := Authorize(p, methodEnqueue); err != nil {
		return nil, err
	}
	for i := range specs {
		switch {
		case p.Role == RoleProducer:
			if specs[i].Tenant != "" && specs[i].Tenant != p.Tenant {
				return nil, status.Errorf(codes.PermissionDenied,
					"items[%d]: a producer for tenant %q may not enqueue for tenant %q", i, p.Tenant, specs[i].Tenant)
			}
			specs[i].Tenant = p.Tenant
		case specs[i].Tenant == "":
			specs[i].Tenant = defaultTenant
		}
	}

	results, err := s.st.Enqueue(ctx, specs)
	if err != nil {
		return nil, s.fail(op, err)
	}
	if len(results) != len(specs) {
		return nil, s.fail(op, fmt.Errorf("store returned %d results for %d specs", len(results), len(specs)))
	}
	resp := &jobqueuev1.EnqueueResponse{Results: make([]*jobqueuev1.EnqueueResult, len(results))}
	for i, r := range results {
		resp.Results[i] = &jobqueuev1.EnqueueResult{Id: r.ID, Deduplicated: r.Deduplicated}
		if r.Deduplicated {
			s.metrics.deduplicated.WithLabelValues(specs[i].Queue).Inc()
		} else {
			s.metrics.enqueued.WithLabelValues(specs[i].Queue).Inc()
		}
	}
	return resp, nil
}

// specOf converts an enqueue item. The checks of job.ValidateSpec are left to
// the caller.
func specOf(it *jobqueuev1.EnqueueItem) (job.Spec, error) {
	delay, err := durationOf("delay", it.GetDelay())
	if err != nil {
		return job.Spec{}, err
	}
	runAt, err := timeOf("run_at", it.GetRunAt())
	if err != nil {
		return job.Spec{}, err
	}
	return job.Spec{
		Queue: it.GetQueue(), Tenant: it.GetTenant(), Payload: it.GetPayload(),
		Delay: delay, RunAt: runAt, MaxAttempts: it.GetMaxAttempts(), IdempotencyKey: it.GetIdempotencyKey(),
	}, nil
}

// Lease hands jobs of one queue to a worker through the dispatcher. A worker
// may lease only from its own queues. The wait defaults to 20s and is clamped
// into [0, 30s].
func (s *Server) Lease(ctx context.Context, req *jobqueuev1.LeaseRequest) (*jobqueuev1.LeaseResponse, error) {
	const op = "lease"
	if err := job.ValidateName("queue", req.GetQueue()); err != nil {
		return nil, s.fail(op, err)
	}
	if n := req.GetMaxJobs(); n < 1 || n > job.MaxBatch {
		return nil, s.fail(op, invalidf("max_jobs", "must be between 1 and %d, got %d", job.MaxBatch, n))
	}
	ttl, err := ttlOf(req.GetTtl())
	if err != nil {
		return nil, s.fail(op, err)
	}
	wait := defaultWait
	if req.GetWait() != nil {
		if wait, err = durationOf("wait", req.GetWait()); err != nil {
			return nil, s.fail(op, err)
		}
		wait = min(max(wait, 0), maxWait)
	}
	if err := job.ValidateWorkerID(req.GetWorkerId()); err != nil {
		return nil, s.fail(op, err)
	}

	p := PrincipalFrom(ctx)
	if err := Authorize(p, methodLease); err != nil {
		return nil, err
	}
	if err := authorizeQueue(p, req.GetQueue()); err != nil {
		return nil, err
	}
	if s.draining.Load() {
		return nil, status.Error(codes.Unavailable, "broker is draining")
	}

	answer, err := s.dispatcher.Lease(ctx, LeaseCall{
		Queue: req.GetQueue(), Max: int(req.GetMaxJobs()), Wait: wait, TTL: ttl, Worker: req.GetWorkerId(),
	})
	if err != nil {
		return nil, s.fail(op, err)
	}
	resp := &jobqueuev1.LeaseResponse{Jobs: make([]*jobqueuev1.LeasedJob, len(answer.Grants)), Waited: durationpb.New(answer.Waited)}
	for i, g := range answer.Grants {
		resp.Jobs[i] = &jobqueuev1.LeasedJob{
			Id: g.Job.ID, Queue: g.Job.Queue, Tenant: g.Job.Tenant, Payload: g.Job.Payload,
			Token: g.Job.Epoch, Attempt: g.Job.Attempts, MaxAttempts: g.Job.MaxAttempts,
			Remaining: durationpb.New(g.Remaining), EnqueuedAt: timestamppb.New(g.Job.CreatedAt),
		}
	}
	return resp, nil
}

// Heartbeat extends leases. A lease that was extended is HELD with the time it
// has left; any other, whether fenced, expired or unknown, is LOST.
func (s *Server) Heartbeat(ctx context.Context, req *jobqueuev1.HeartbeatRequest) (*jobqueuev1.HeartbeatResponse, error) {
	const op = "heartbeat"
	refs, err := refsOf("leases", req.GetLeases())
	if err != nil {
		return nil, s.fail(op, err)
	}
	ttl, err := ttlOf(req.GetTtl())
	if err != nil {
		return nil, s.fail(op, err)
	}
	if err := authorizeRefs(PrincipalFrom(ctx), methodHeartbeat, refs); err != nil {
		return nil, err
	}

	results, err := s.st.Heartbeat(ctx, refs, ttl)
	if err != nil {
		return nil, s.fail(op, err)
	}
	if len(results) != len(refs) {
		return nil, s.fail(op, fmt.Errorf("store returned %d results for %d leases", len(results), len(refs)))
	}
	resp := &jobqueuev1.HeartbeatResponse{Results: make([]*jobqueuev1.HeartbeatResult, len(results))}
	for i, r := range results {
		if r.Outcome == job.Applied {
			resp.Results[i] = &jobqueuev1.HeartbeatResult{Status: jobqueuev1.LeaseStatus_LEASE_STATUS_HELD, Remaining: durationpb.New(r.Remaining)}
			s.metrics.acks.WithLabelValues(opHeartbeat, resultAccepted).Inc()
		} else {
			resp.Results[i] = &jobqueuev1.HeartbeatResult{Status: jobqueuev1.LeaseStatus_LEASE_STATUS_LOST}
			s.metrics.acks.WithLabelValues(opHeartbeat, resultFenced).Inc()
		}
	}
	return resp, nil
}

// pendingFail is a failure report that has been validated but has no delay
// yet.
type pendingFail struct {
	req     store.FailRequest
	attempt int32
}

// Ack reports finished and failed jobs. The items are split into one Complete
// call and one Fail call, and the per-item statuses come back in request
// order. A failure waits retry_after when that is set, and otherwise an
// exponential backoff with full jitter, taken for the attempt number the
// client reports: the broker keeps no count of its own that it could trust
// after a restart.
//
// The two store calls are not one transaction. If the Fail call fails after
// the Complete call succeeded, the caller gets the error and repeats the whole
// batch; the completions then come back as duplicates, which the store treats
// as no change.
func (s *Server) Ack(ctx context.Context, req *jobqueuev1.AckRequest) (*jobqueuev1.AckResponse, error) {
	const op = "ack"
	items := req.GetItems()
	if err := job.ValidateBatch("items", len(items), job.MaxAckBatch); err != nil {
		return nil, s.fail(op, err)
	}
	var (
		completes  []store.CompleteRequest
		completeAt []int
		fails      []pendingFail
		failAt     []int
		refs       = make([]store.Ref, len(items))
	)
	for i, it := range items {
		if err := validateRef(it.GetLease()); err != nil {
			return nil, s.fail(op, fmt.Errorf("items[%d]: %w", i, err))
		}
		l := it.GetLease()
		ref := store.Ref{ID: l.GetId(), Queue: l.GetQueue(), Token: l.GetToken()}
		refs[i] = ref
		switch outcome := it.GetOutcome().(type) {
		case *jobqueuev1.AckItem_Success:
			result := outcome.Success.GetResult()
			if err := job.ValidateResult(result); err != nil {
				return nil, s.fail(op, fmt.Errorf("items[%d]: %w", i, err))
			}
			completes = append(completes, store.CompleteRequest{Ref: ref, Result: result})
			completeAt = append(completeAt, i)
		case *jobqueuev1.AckItem_Failure:
			f := outcome.Failure
			retryAfter, err := durationOf("retry_after", f.GetRetryAfter())
			if err == nil {
				err = job.ValidateDelay("retry_after", retryAfter)
			}
			if err != nil {
				return nil, s.fail(op, fmt.Errorf("items[%d]: %w", i, err))
			}
			fails = append(fails, pendingFail{
				req:     store.FailRequest{Ref: ref, Permanent: f.GetPermanent(), Delay: retryAfter, Error: f.GetError()},
				attempt: f.GetAttempt(),
			})
			failAt = append(failAt, i)
		case nil:
			return nil, s.fail(op, fmt.Errorf("items[%d]: %w", i, invalidf("outcome", "must be a success or a failure")))
		}
	}

	if err := authorizeRefs(PrincipalFrom(ctx), methodAck, refs); err != nil {
		return nil, err
	}

	statuses := make([]jobqueuev1.AckStatus, len(items))
	if len(completes) > 0 {
		outcomes, err := s.st.Complete(ctx, completes)
		if err != nil {
			return nil, s.fail(op, err)
		}
		if err := s.record(opComplete, completeAt, outcomes, statuses); err != nil {
			return nil, s.fail(op, err)
		}
	}
	if len(fails) > 0 {
		reqs := make([]store.FailRequest, len(fails))
		for i, f := range fails {
			reqs[i] = f.req
			if f.req.Delay == 0 {
				reqs[i].Delay = s.backoffDelay(f.attempt)
			}
		}
		outcomes, err := s.st.Fail(ctx, reqs)
		if err != nil {
			return nil, s.fail(op, err)
		}
		if err := s.record(opFail, failAt, outcomes, statuses); err != nil {
			return nil, s.fail(op, err)
		}
	}
	return &jobqueuev1.AckResponse{Results: statuses}, nil
}

// Release gives leased jobs back unrun. The statuses come back in request
// order, as they do for Ack.
func (s *Server) Release(ctx context.Context, req *jobqueuev1.ReleaseRequest) (*jobqueuev1.ReleaseResponse, error) {
	const op = "release"
	refs, err := refsOf("leases", req.GetLeases())
	if err != nil {
		return nil, s.fail(op, err)
	}
	if err := authorizeRefs(PrincipalFrom(ctx), methodRelease, refs); err != nil {
		return nil, err
	}
	outcomes, err := s.st.Release(ctx, refs)
	if err != nil {
		return nil, s.fail(op, err)
	}
	at := make([]int, len(refs))
	for i := range at {
		at[i] = i
	}
	statuses := make([]jobqueuev1.AckStatus, len(refs))
	if err := s.record(opRelease, at, outcomes, statuses); err != nil {
		return nil, s.fail(op, err)
	}
	return &jobqueuev1.ReleaseResponse{Results: statuses}, nil
}

// record turns the outcomes of one store call into ack statuses, writes
// outcome k to statuses[at[k]], and counts them under op. It fails, writing
// and counting nothing, if the store did not answer once per item or answered
// with an outcome that the operation cannot have.
func (s *Server) record(op string, at []int, outcomes []job.Outcome, statuses []jobqueuev1.AckStatus) error {
	if len(outcomes) != len(at) {
		return fmt.Errorf("store returned %d outcomes for %d items of %s", len(outcomes), len(at), op)
	}
	mapped := make([]jobqueuev1.AckStatus, len(outcomes))
	labels := make([]string, len(outcomes))
	for k, o := range outcomes {
		st, label, ok := ackStatus(o)
		if !ok {
			return fmt.Errorf("store answered %s for an item of %s", o, op)
		}
		mapped[k], labels[k] = st, label
	}
	for k := range outcomes {
		statuses[at[k]] = mapped[k]
		s.metrics.acks.WithLabelValues(op, labels[k]).Inc()
	}
	return nil
}

// ackStatus maps the outcome of a Complete, Fail or Release to the status that
// the worker sees and the label that counts it. A job that is gone, or whose
// lease ran out, is as fenced to the worker as one that someone else leased.
// ok is false for WrongState, which none of these operations returns.
func ackStatus(o job.Outcome) (st jobqueuev1.AckStatus, label string, ok bool) {
	switch o {
	case job.Applied:
		return jobqueuev1.AckStatus_ACK_STATUS_ACCEPTED, resultAccepted, true
	case job.Duplicate:
		return jobqueuev1.AckStatus_ACK_STATUS_DUPLICATE, resultDuplicate, true
	case job.Fenced, job.NotFound, job.Expired:
		return jobqueuev1.AckStatus_ACK_STATUS_FENCED, resultFenced, true
	case job.WrongState:
	}
	return jobqueuev1.AckStatus_ACK_STATUS_UNSPECIFIED, "", false
}

// GetJob returns one job. A producer sees only its tenant's jobs; another
// tenant's job is reported as not found.
func (s *Server) GetJob(ctx context.Context, req *jobqueuev1.GetJobRequest) (*jobqueuev1.GetJobResponse, error) {
	p := PrincipalFrom(ctx)
	if err := Authorize(p, methodGetJob); err != nil {
		return nil, err
	}
	tenant, err := tenantScope(p)
	if err != nil {
		return nil, err
	}
	j, err := s.st.Get(ctx, req.GetId(), tenant)
	if err != nil {
		return nil, s.fail("get_job", err)
	}
	return &jobqueuev1.GetJobResponse{Job: jobToProto(j)}, nil
}

// CancelJob cancels a job that has not finished. Cancelling a cancelled job
// succeeds again; a job that succeeded or died is FailedPrecondition. A
// producer can cancel only its tenant's jobs.
func (s *Server) CancelJob(ctx context.Context, req *jobqueuev1.CancelJobRequest) (*jobqueuev1.CancelJobResponse, error) {
	const op = "cancel_job"
	p := PrincipalFrom(ctx)
	if err := Authorize(p, methodCancelJob); err != nil {
		return nil, err
	}
	tenant, err := tenantScope(p)
	if err != nil {
		return nil, err
	}
	outcome, err := s.st.Cancel(ctx, req.GetId(), tenant)
	if err != nil {
		return nil, s.fail(op, err)
	}
	if err := s.adminOutcome(ctx, op, outcome, req.GetId(), tenant); err != nil {
		return nil, err
	}
	return &jobqueuev1.CancelJobResponse{State: jobqueuev1.JobState_JOB_STATE_CANCELLED}, nil
}

// RequeueJob revives a dead job. A job in any other state is
// FailedPrecondition.
func (s *Server) RequeueJob(ctx context.Context, req *jobqueuev1.RequeueJobRequest) (*jobqueuev1.RequeueJobResponse, error) {
	const op = "requeue_job"
	if err := Authorize(PrincipalFrom(ctx), methodRequeueJob); err != nil {
		return nil, err
	}
	outcome, err := s.st.Requeue(ctx, req.GetId())
	if err != nil {
		return nil, s.fail(op, err)
	}
	if err := s.adminOutcome(ctx, op, outcome, req.GetId(), ""); err != nil {
		return nil, err
	}
	return &jobqueuev1.RequeueJobResponse{}, nil
}

// adminOutcome turns the outcome of Cancel or Requeue into nil for success or
// the error to return. A job in the wrong state is reported with the state it
// is in now, read in a second call; if the job has changed in between, the
// message names the newer state.
func (s *Server) adminOutcome(ctx context.Context, op string, outcome job.Outcome, id int64, tenant string) error {
	switch outcome {
	case job.Applied, job.Duplicate:
		return nil
	case job.NotFound:
		return s.fail(op, store.ErrNotFound)
	case job.WrongState:
		j, err := s.st.Get(ctx, id, tenant)
		if err != nil {
			return s.fail(op, err)
		}
		return status.Errorf(codes.FailedPrecondition, "job is %s", j.State)
	case job.Fenced, job.Expired:
		return s.fail(op, fmt.Errorf("store answered %s for job %d", outcome, id))
	}
	return s.fail(op, fmt.Errorf("store answered %s for job %d", outcome, id))
}

// GetStats counts a queue's jobs per tenant.
func (s *Server) GetStats(ctx context.Context, req *jobqueuev1.GetStatsRequest) (*jobqueuev1.GetStatsResponse, error) {
	const op = "get_stats"
	if err := job.ValidateName("queue", req.GetQueue()); err != nil {
		return nil, s.fail(op, err)
	}
	if err := Authorize(PrincipalFrom(ctx), methodGetStats); err != nil {
		return nil, err
	}
	stats, err := s.st.Stats(ctx, req.GetQueue())
	if err != nil {
		return nil, s.fail(op, err)
	}
	resp := &jobqueuev1.GetStatsResponse{Tenants: make([]*jobqueuev1.TenantStats, len(stats))}
	for i, t := range stats {
		resp.Tenants[i] = &jobqueuev1.TenantStats{
			Tenant: t.Tenant, Ready: t.Ready, Scheduled: t.Scheduled, Leased: t.Leased, Dead: t.Dead,
			OldestReadyAge: durationpb.New(t.OldestReadyAge),
		}
	}
	return resp, nil
}

// ListDeadJobs lists a queue's dead jobs after an id, in ascending id. The
// limit defaults to 50 and is at most job.MaxBatch. A page that is full names
// the id to continue after in next_after_id; a shorter page ends the list, and
// next_after_id is 0.
func (s *Server) ListDeadJobs(ctx context.Context, req *jobqueuev1.ListDeadJobsRequest) (*jobqueuev1.ListDeadJobsResponse, error) {
	const op = "list_dead_jobs"
	if err := job.ValidateName("queue", req.GetQueue()); err != nil {
		return nil, s.fail(op, err)
	}
	limit := int(req.GetLimit())
	switch {
	case limit == 0:
		limit = defaultDeadPage
	case limit < 0 || limit > job.MaxBatch:
		return nil, s.fail(op, invalidf("limit", "must be between 1 and %d, or 0 for %d, got %d", job.MaxBatch, defaultDeadPage, limit))
	}
	if err := Authorize(PrincipalFrom(ctx), methodListDeadJobs); err != nil {
		return nil, err
	}
	jobs, err := s.st.ListDead(ctx, req.GetQueue(), req.GetAfterId(), limit)
	if err != nil {
		return nil, s.fail(op, err)
	}
	resp := &jobqueuev1.ListDeadJobsResponse{Jobs: make([]*jobqueuev1.Job, len(jobs))}
	for i, j := range jobs {
		resp.Jobs[i] = jobToProto(j)
	}
	if len(jobs) == limit {
		resp.NextAfterId = jobs[len(jobs)-1].ID
	}
	return resp, nil
}

// PurgeJobs deletes the jobs of a queue that finished before a time, in one
// terminal state. The state and the time are both required, so that a request
// that forgets one deletes nothing instead of everything.
func (s *Server) PurgeJobs(ctx context.Context, req *jobqueuev1.PurgeJobsRequest) (*jobqueuev1.PurgeJobsResponse, error) {
	const op = "purge_jobs"
	if err := job.ValidateName("queue", req.GetQueue()); err != nil {
		return nil, s.fail(op, err)
	}
	state, err := stateFromProto(req.GetState())
	if err == nil && !state.Terminal() {
		err = invalidf("state", "must be succeeded, dead or cancelled, got %s", state)
	}
	if err != nil {
		return nil, s.fail(op, err)
	}
	if req.GetFinishedBefore() == nil {
		return nil, s.fail(op, invalidf("finished_before", "is required"))
	}
	before, err := timeOf("finished_before", req.GetFinishedBefore())
	if err != nil {
		return nil, s.fail(op, err)
	}
	if err := Authorize(PrincipalFrom(ctx), methodPurgeJobs); err != nil {
		return nil, err
	}
	n, err := s.st.Purge(ctx, store.PurgeRequest{Queue: req.GetQueue(), State: state, FinishedBefore: before})
	if err != nil {
		return nil, s.fail(op, err)
	}
	return &jobqueuev1.PurgeJobsResponse{Deleted: int64(n)}, nil
}

// authorizeQueue says whether p may use queue. Only a worker is limited to
// its queues; the role check of the method has already shut out a producer.
func authorizeQueue(p Principal, queue string) error {
	if p.Role == RoleWorker && !slices.Contains(p.Queues, queue) {
		return status.Errorf(codes.PermissionDenied, "worker may not use queue %q", queue)
	}
	return nil
}

// authorizeRefs checks the role for method and the queue of every ref.
func authorizeRefs(p Principal, method string, refs []store.Ref) error {
	if err := Authorize(p, method); err != nil {
		return err
	}
	for _, r := range refs {
		if err := authorizeQueue(p, r.Queue); err != nil {
			return err
		}
	}
	return nil
}

// tenantScope returns the tenant argument of the store calls that a producer
// may make: its own tenant. An admin may touch every tenant, which the store
// spells "". The role check of the method has already shut out a worker, and a
// producer without a tenant, which UnaryAuth never produces, is refused
// rather than treated as an admin.
func tenantScope(p Principal) (string, error) {
	if p.Role != RoleProducer {
		return "", nil
	}
	if p.Tenant == "" {
		return "", status.Error(codes.PermissionDenied, "producer is not bound to a tenant")
	}
	return p.Tenant, nil
}

// invalidf returns an error that toStatus makes an InvalidArgument.
func invalidf(field, format string, args ...any) error {
	return &job.InvalidError{Field: field, Reason: fmt.Sprintf(format, args...)}
}

// durationOf converts an optional duration field; absent is 0. A duration
// whose seconds and nanos are out of range or disagree in sign is rejected,
// where AsDuration alone would saturate.
func durationOf(field string, d *durationpb.Duration) (time.Duration, error) {
	if d == nil {
		return 0, nil
	}
	if err := d.CheckValid(); err != nil {
		return 0, invalidf(field, "%v", err)
	}
	return d.AsDuration(), nil
}

// ttlOf converts the required ttl field and checks it with job.ValidateTTL,
// which also demands a whole number of microseconds.
func ttlOf(d *durationpb.Duration) (time.Duration, error) {
	if d == nil {
		return 0, invalidf("ttl", "is required")
	}
	ttl, err := durationOf("ttl", d)
	if err != nil {
		return 0, err
	}
	return ttl, job.ValidateTTL(ttl)
}

// timeOf converts an optional timestamp field; absent is the zero time.
func timeOf(field string, t *timestamppb.Timestamp) (time.Time, error) {
	if t == nil {
		return time.Time{}, nil
	}
	if err := t.CheckValid(); err != nil {
		return time.Time{}, invalidf(field, "%v", err)
	}
	return t.AsTime(), nil
}

// validateRef checks the queue of a lease reference; its id and token are the
// store's to judge.
func validateRef(l *jobqueuev1.LeaseRef) error {
	if l == nil {
		return invalidf("lease", "is required")
	}
	return job.ValidateName("queue", l.GetQueue())
}

// refsOf converts and validates the lease references of a Heartbeat or Release
// call: between 1 and job.MaxAckBatch of them, each in a valid queue.
func refsOf(field string, leases []*jobqueuev1.LeaseRef) ([]store.Ref, error) {
	if err := job.ValidateBatch(field, len(leases), job.MaxAckBatch); err != nil {
		return nil, err
	}
	refs := make([]store.Ref, len(leases))
	for i, l := range leases {
		if err := validateRef(l); err != nil {
			return nil, fmt.Errorf("%s[%d]: %w", field, i, err)
		}
		refs[i] = store.Ref{ID: l.GetId(), Queue: l.GetQueue(), Token: l.GetToken()}
	}
	return refs, nil
}

// stateFromProto converts a job state of the API.
func stateFromProto(s jobqueuev1.JobState) (job.State, error) {
	switch s {
	case jobqueuev1.JobState_JOB_STATE_READY:
		return job.StateReady, nil
	case jobqueuev1.JobState_JOB_STATE_LEASED:
		return job.StateLeased, nil
	case jobqueuev1.JobState_JOB_STATE_SUCCEEDED:
		return job.StateSucceeded, nil
	case jobqueuev1.JobState_JOB_STATE_DEAD:
		return job.StateDead, nil
	case jobqueuev1.JobState_JOB_STATE_CANCELLED:
		return job.StateCancelled, nil
	case jobqueuev1.JobState_JOB_STATE_UNSPECIFIED:
		return 0, invalidf("state", "is required")
	}
	return 0, invalidf("state", "unknown state %d", int32(s))
}

// stateToProto converts a job state to the API's.
func stateToProto(s job.State) jobqueuev1.JobState {
	switch s {
	case job.StateReady:
		return jobqueuev1.JobState_JOB_STATE_READY
	case job.StateLeased:
		return jobqueuev1.JobState_JOB_STATE_LEASED
	case job.StateSucceeded:
		return jobqueuev1.JobState_JOB_STATE_SUCCEEDED
	case job.StateDead:
		return jobqueuev1.JobState_JOB_STATE_DEAD
	case job.StateCancelled:
		return jobqueuev1.JobState_JOB_STATE_CANCELLED
	}
	return jobqueuev1.JobState_JOB_STATE_UNSPECIFIED
}

// timeToProto converts a time to a timestamp; the zero time, which a Job holds
// for "never", becomes absent.
func timeToProto(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// jobToProto converts a job to the API's message.
func jobToProto(j job.Job) *jobqueuev1.Job {
	return &jobqueuev1.Job{
		Id: j.ID, Queue: j.Queue, Tenant: j.Tenant, State: stateToProto(j.State), Payload: j.Payload,
		RunAt: timeToProto(j.RunAt), Attempts: j.Attempts, MaxAttempts: j.MaxAttempts, Epoch: j.Epoch,
		LeaseExpiresAt: timeToProto(j.LeaseExpiresAt), LastError: j.LastError, Result: j.Result,
		CreatedAt: timeToProto(j.CreatedAt), FinishedAt: timeToProto(j.FinishedAt), IdempotencyKey: j.IdempotencyKey,
	}
}
