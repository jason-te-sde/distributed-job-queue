package jobqueue

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	jobqueuev1 "github.com/jason-te-sde/distributed-job-queue/internal/gen/jobqueue/v1"
)

// keyBytes is the size of a generated idempotency key: 128 bits, written as
// 32 hex characters.
const keyBytes = 16

// EnqueueParams describes one job to enqueue.
type EnqueueParams struct {
	// Queue is the queue to put the job in.
	Queue string
	// Tenant is the tenant the job belongs to. Empty means the producer's
	// default (ProducerOptions.Tenant), and failing that the broker's choice.
	Tenant string
	// Payload is the job's input, at most 256 KiB. The queue does not look
	// inside it.
	Payload []byte
	// Delay is how long to wait before the job may run, up to 365 days.
	// Delay and RunAt are alternatives; a job that names both is refused.
	Delay time.Duration
	// RunAt is the earliest time the job may run. The zero time means now.
	RunAt time.Time
	// MaxAttempts is how many times the job may be tried before it is dead:
	// 1 to 100. Zero means the default, 25.
	MaxAttempts int
	// IdempotencyKey names the job within its tenant and queue: enqueuing
	// the same key again returns the first job instead of making another,
	// for as long as that job is kept. Empty means a random key, generated
	// once per call, which makes the call safe to repeat after a lost answer
	// but does not deduplicate calls from one another.
	IdempotencyKey string
}

// EnqueueResult is the outcome of enqueuing one job.
type EnqueueResult struct {
	// ID is the job's id.
	ID int64
	// Deduplicated is true when the idempotency key already named a job and
	// ID is that job's. It is always false for a call that generated its own
	// key: a generated key can match only an earlier attempt of the same
	// call, and that attempt is part of the call that made the job.
	Deduplicated bool
}

// JobInfo is what a producer may learn about a job. The payload is not part
// of it.
type JobInfo struct {
	// ID, Queue and Tenant identify the job.
	ID            int64
	Queue, Tenant string
	// State is "ready", "leased", "succeeded", "dead" or "cancelled". A job
	// that waits for its run time or for a retry is "ready" too: RunAt says
	// when it may run.
	State string
	// Attempts is the number of times the job was leased and not given back
	// unrun, and MaxAttempts is the budget.
	Attempts, MaxAttempts int
	// RunAt is the earliest time the job may be leased, and CreatedAt is
	// when it was enqueued. FinishedAt is when it reached a final state, and
	// the zero time before that.
	RunAt, CreatedAt, FinishedAt time.Time
	// LastError is the message of the latest failure of a handler, if any.
	LastError string
	// Result is what the handler of a succeeded job returned through
	// Job.SetResult.
	Result []byte
}

// Producer enqueues jobs and looks after the ones it enqueued. It is safe for
// concurrent use. Every call that can be repeated safely is repeated when it
// fails with a retryable error, up to the RetryPolicy: Enqueue is safe
// because every job carries an idempotency key, and Get and Cancel are
// idempotent by nature.
type Producer struct {
	client  jobqueuev1.JobQueueServiceClient
	tenant  string
	retry   RetryPolicy
	timeout time.Duration

	// rng draws the jitter of the retry waits. A generator is not safe for
	// concurrent use, hence the mutex.
	mu  sync.Mutex
	rng *rand.Rand
}

// NewProducer returns a producer that calls the broker through conn, which
// Dial opens. The caller keeps conn and closes it.
func NewProducer(conn grpc.ClientConnInterface, opts ProducerOptions) *Producer {
	p := &Producer{
		client:  jobqueuev1.NewJobQueueServiceClient(conn),
		tenant:  opts.Tenant,
		retry:   opts.Retry.withDefaults(),
		timeout: opts.PerCallTimeout,
		rng:     rand.New(rand.NewPCG(randomSeed())),
	}
	if p.timeout <= 0 {
		p.timeout = defaultPerCallTimeout
	}
	return p
}

// randomSeed returns two words of the operating system's randomness. If that
// is unavailable the clock stands in: the jitter of retries and the backoff of
// a worker need to differ between processes, not to be unpredictable.
func randomSeed() (s1, s2 uint64) {
	var seed [16]byte
	if _, err := crand.Read(seed[:]); err != nil {
		now := uint64(time.Now().UnixNano())
		return now, now ^ 0x9e3779b97f4a7c15
	}
	return binary.LittleEndian.Uint64(seed[:8]), binary.LittleEndian.Uint64(seed[8:])
}

// Enqueue creates a job. It retries retryable failures, which is safe because
// the job carries an idempotency key, the caller's or a generated one.
func (p *Producer) Enqueue(ctx context.Context, e EnqueueParams) (EnqueueResult, error) {
	res, err := p.enqueue(ctx, []EnqueueParams{e})
	if err != nil {
		return EnqueueResult{}, err
	}
	return res[0], nil
}

// EnqueueBatch creates 1 to 64 jobs in one call, in the order given, and
// returns one result for each. It is all or nothing: if one job is refused,
// none is created. Within a batch, a key that repeats names the job of its
// first item.
func (p *Producer) EnqueueBatch(ctx context.Context, es []EnqueueParams) ([]EnqueueResult, error) {
	if len(es) < 1 || len(es) > job.MaxBatch {
		return nil, fmt.Errorf("%w: a batch has 1 to %d jobs, got %d", ErrInvalid, job.MaxBatch, len(es))
	}
	return p.enqueue(ctx, es)
}

func (p *Producer) enqueue(ctx context.Context, es []EnqueueParams) ([]EnqueueResult, error) {
	req := &jobqueuev1.EnqueueRequest{Items: make([]*jobqueuev1.EnqueueItem, len(es))}
	generated := make([]bool, len(es))
	for i, e := range es {
		item, gen, err := p.item(e)
		if err != nil {
			if len(es) > 1 {
				err = fmt.Errorf("item %d: %w", i, err)
			}
			return nil, err
		}
		req.Items[i], generated[i] = item, gen
	}

	var resp *jobqueuev1.EnqueueResponse
	err := p.call(ctx, "enqueue", func(ctx context.Context) (err error) {
		resp, err = p.client.Enqueue(ctx, req)
		return err
	})
	if err != nil {
		return nil, err
	}
	if len(resp.GetResults()) != len(es) {
		return nil, fmt.Errorf("jobqueue: enqueue: the broker answered %d results for %d jobs", len(resp.GetResults()), len(es))
	}
	out := make([]EnqueueResult, len(es))
	for i, r := range resp.GetResults() {
		out[i] = EnqueueResult{ID: r.GetId(), Deduplicated: r.GetDeduplicated() && !generated[i]}
	}
	return out, nil
}

// item builds the wire form of one job, generating its idempotency key if the
// caller gave none. generated says whether it did.
func (p *Producer) item(e EnqueueParams) (item *jobqueuev1.EnqueueItem, generated bool, err error) {
	if e.MaxAttempts < math.MinInt32 || e.MaxAttempts > math.MaxInt32 {
		return nil, false, fmt.Errorf("%w: max attempts %d does not fit in 32 bits", ErrInvalid, e.MaxAttempts)
	}
	key := e.IdempotencyKey
	if key == "" {
		var raw [keyBytes]byte
		if _, err := crand.Read(raw[:]); err != nil {
			return nil, false, fmt.Errorf("jobqueue: generate an idempotency key: %w", err)
		}
		key, generated = hex.EncodeToString(raw[:]), true
	}
	tenant := e.Tenant
	if tenant == "" {
		tenant = p.tenant
	}
	item = &jobqueuev1.EnqueueItem{
		Queue: e.Queue, Tenant: tenant, Payload: e.Payload,
		MaxAttempts: int32(e.MaxAttempts), IdempotencyKey: key,
	}
	if e.Delay != 0 {
		item.Delay = durationpb.New(e.Delay)
	}
	if !e.RunAt.IsZero() {
		item.RunAt = timestamppb.New(e.RunAt)
	}
	return item, generated, nil
}

// Get returns what the broker knows of a job. A job that does not exist, was
// pruned, or belongs to another tenant than the producer's token names is
// ErrNotFound.
func (p *Producer) Get(ctx context.Context, id int64) (*JobInfo, error) {
	var resp *jobqueuev1.GetJobResponse
	err := p.call(ctx, "get", func(ctx context.Context) (err error) {
		resp, err = p.client.GetJob(ctx, &jobqueuev1.GetJobRequest{Id: id})
		return err
	})
	if err != nil {
		return nil, err
	}
	j := resp.GetJob()
	if j == nil {
		return nil, errors.New("jobqueue: get: the broker answered no job")
	}
	info := &JobInfo{
		ID: j.GetId(), Queue: j.GetQueue(), Tenant: j.GetTenant(), State: stateName(j.GetState()),
		Attempts: int(j.GetAttempts()), MaxAttempts: int(j.GetMaxAttempts()),
		RunAt: timeOf(j.GetRunAt()), CreatedAt: timeOf(j.GetCreatedAt()), FinishedAt: timeOf(j.GetFinishedAt()),
		LastError: j.GetLastError(),
	}
	if r := j.GetResult(); len(r) > 0 {
		info.Result = r
	}
	return info, nil
}

// Cancel stops a job that has not finished: a ready job will not run, and a
// leased job's worker finds out at its next heartbeat and is told to stop.
// Cancelling a cancelled job succeeds again. A job that already succeeded or
// died cannot be cancelled; the error is a gRPC status with FailedPrecondition.
func (p *Producer) Cancel(ctx context.Context, id int64) error {
	return p.call(ctx, "cancel", func(ctx context.Context) error {
		_, err := p.client.CancelJob(ctx, &jobqueuev1.CancelJobRequest{Id: id})
		return err
	})
}

// timeOf converts a timestamp. An absent one is the zero time, where AsTime
// would give the Unix epoch.
func timeOf(t *timestamppb.Timestamp) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.AsTime()
}

// stateName is the name of a job state as JobInfo spells it.
func stateName(s jobqueuev1.JobState) string {
	switch s {
	case jobqueuev1.JobState_JOB_STATE_READY:
		return "ready"
	case jobqueuev1.JobState_JOB_STATE_LEASED:
		return "leased"
	case jobqueuev1.JobState_JOB_STATE_SUCCEEDED:
		return "succeeded"
	case jobqueuev1.JobState_JOB_STATE_DEAD:
		return "dead"
	case jobqueuev1.JobState_JOB_STATE_CANCELLED:
		return "cancelled"
	case jobqueuev1.JobState_JOB_STATE_UNSPECIFIED:
		return "unknown"
	}
	return fmt.Sprintf("state(%d)", int32(s))
}

// call makes the call attempt, repeating it under the retry policy while it
// fails with a retryable error and ctx lives. Each attempt has its own
// deadline. The error is wrapped for the operation op.
func (p *Producer) call(ctx context.Context, op string, attempt func(context.Context) error) error {
	for n := 1; ; n++ {
		actx, cancel := context.WithTimeout(ctx, p.timeout)
		err := attempt(actx)
		cancel()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return callFailed(op, err, ctx.Err())
		}
		if n >= p.retry.Attempts || !IsRetryable(err) {
			return callFailed(op, err, nil)
		}
		if waitErr := p.sleep(ctx, n); waitErr != nil {
			return callFailed(op, err, waitErr)
		}
	}
}

// sleep waits the jittered delay after failed attempt n, or until ctx ends, in
// which case it returns the context's error.
func (p *Producer) sleep(ctx context.Context, n int) error {
	ceiling := p.retry.Base
	for i := 1; i < n && ceiling < p.retry.Cap; i++ {
		ceiling *= 2
	}
	ceiling = min(ceiling, p.retry.Cap)
	p.mu.Lock()
	d := time.Duration(p.rng.Int64N(int64(ceiling) + 1))
	p.mu.Unlock()

	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
