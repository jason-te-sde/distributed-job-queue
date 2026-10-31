package jobqueue_test

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	jobqueue "github.com/jason-te-sde/distributed-job-queue"
	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

func TestProducerEnqueueAndGet(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	p := jobqueue.NewProducer(c.dial(), jobqueue.ProducerOptions{Tenant: "acme"})

	before := time.Now()
	res, err := p.Enqueue(t.Context(), jobqueue.EnqueueParams{Queue: "emails", Payload: []byte("hello"), MaxAttempts: 5})
	if err != nil {
		t.Fatal(err)
	}
	if res.ID == 0 || res.Deduplicated {
		t.Fatalf("Enqueue = %+v, want a new job", res)
	}

	info, err := p.Get(t.Context(), res.ID)
	if err != nil {
		t.Fatal(err)
	}
	if info.ID != res.ID || info.Queue != "emails" || info.Tenant != "acme" || info.State != "ready" {
		t.Errorf("Get = %+v, want job %d in queue emails of tenant acme, ready", info, res.ID)
	}
	if info.Attempts != 0 || info.MaxAttempts != 5 {
		t.Errorf("Attempts, MaxAttempts = %d, %d, want 0, 5", info.Attempts, info.MaxAttempts)
	}
	if info.CreatedAt.Before(before.Add(-time.Second)) || !info.FinishedAt.IsZero() || info.LastError != "" || info.Result != nil {
		t.Errorf("Get = %+v: want a fresh job with no finish time, error or result", info)
	}
}

func TestProducerEnqueueHonorsDelayRunAtAndTenant(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	p := jobqueue.NewProducer(c.dial(), jobqueue.ProducerOptions{Tenant: "acme"})

	delayed, err := p.Enqueue(t.Context(), jobqueue.EnqueueParams{Queue: "q", Delay: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	runAt := time.Now().Add(2 * time.Hour).Truncate(time.Second)
	scheduled, err := p.Enqueue(t.Context(), jobqueue.EnqueueParams{Queue: "q", RunAt: runAt, Tenant: "other"})
	if err != nil {
		t.Fatal(err)
	}

	d, err := p.Get(t.Context(), delayed.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := time.Until(d.RunAt); got < 59*time.Minute || got > time.Hour {
		t.Errorf("a job delayed by 1h runs in %v", got)
	}
	if d.Tenant != "acme" {
		t.Errorf("tenant = %q, want the producer's default acme", d.Tenant)
	}

	s := c.get(scheduled.ID)
	if !s.RunAt.Equal(runAt) {
		t.Errorf("RunAt = %v, want %v", s.RunAt, runAt)
	}
	if s.Tenant != "other" {
		t.Errorf("tenant = %q, want the call's own other to win over the producer's default", s.Tenant)
	}
}

func TestProducerGeneratesIdempotencyKey(t *testing.T) {
	var attempts atomic.Int32
	loseThirdResponse := jobqueue.WithGRPCOptions(grpc.WithChainUnaryInterceptor(
		func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			err := invoker(ctx, method, req, reply, cc, opts...)
			if err == nil && attempts.Add(1) == 3 {
				// The server applied the third Enqueue; the answer is lost.
				return status.Error(codes.Unavailable, "response lost")
			}
			return err
		}))

	c := newCluster(t, clusterOpts{})
	p := c.producer(loseThirdResponse)

	a, err := p.Enqueue(t.Context(), jobqueue.EnqueueParams{Queue: "q", Payload: []byte("same")})
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Enqueue(t.Context(), jobqueue.EnqueueParams{Queue: "q", Payload: []byte("same")})
	if err != nil {
		t.Fatal(err)
	}
	if a.ID == b.ID {
		t.Fatalf("two calls without a key got the same job %d", a.ID)
	}
	if k := c.get(a.ID).IdempotencyKey; len(k) != 32 {
		t.Errorf("generated key %q is not 32 hex characters (128 bits)", k)
	}
	if c.get(a.ID).IdempotencyKey == c.get(b.ID).IdempotencyKey {
		t.Error("two calls generated the same key")
	}

	// The third call: the server applies it, the answer is lost, and the retry
	// must find the job it made instead of making another.
	res, err := p.Enqueue(t.Context(), jobqueue.EnqueueParams{Queue: "q", Payload: []byte("lost")})
	if err != nil {
		t.Fatal(err)
	}
	if got := attempts.Load(); got != 4 {
		t.Fatalf("attempts = %d, want 4: two calls, and a third that was tried twice", got)
	}
	if n := c.count("q", job.StateReady); n != 3 {
		t.Fatalf("%d jobs in the queue, want 3: the lost answer must not duplicate the job", n)
	}
	if res.ID == a.ID || res.ID == b.ID {
		t.Errorf("the retried call returned the id %d of another call's job", res.ID)
	}
	if res.Deduplicated {
		t.Error("Deduplicated is true for a call that made its job; a generated key can only match this call's own earlier attempt")
	}
	if got := string(c.get(res.ID).Payload); got != "lost" {
		t.Errorf("job %d has payload %q, want the retried call's", res.ID, got)
	}
}

func TestProducerKeepsAnExplicitIdempotencyKey(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	p := c.producer()
	a, err := p.Enqueue(t.Context(), jobqueue.EnqueueParams{Queue: "q", IdempotencyKey: "order-1"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Enqueue(t.Context(), jobqueue.EnqueueParams{Queue: "q", IdempotencyKey: "order-1"})
	if err != nil {
		t.Fatal(err)
	}
	if a.Deduplicated || !b.Deduplicated || a.ID != b.ID {
		t.Fatalf("results %+v, %+v: want the second call to name the first job and say so", a, b)
	}
}

func TestProducerRetriesUnavailable(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	var failures atomic.Int32
	c.brokers[0].setFault(func(_ context.Context, method string) error {
		if method == "Enqueue" && failures.Add(1) <= 2 {
			return status.Error(codes.Unavailable, "try again")
		}
		return nil
	})
	p := c.producer()

	if _, err := p.Enqueue(t.Context(), jobqueue.EnqueueParams{Queue: "q"}); err != nil {
		t.Fatalf("Enqueue = %v, want success on the third attempt", err)
	}
	if got := c.brokers[0].callCount("Enqueue"); got != 3 {
		t.Fatalf("the broker saw %d attempts, want 3", got)
	}
}

func TestProducerGivesUpAfterTheConfiguredAttempts(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	c.brokers[0].setFault(func(context.Context, string) error { return status.Error(codes.ResourceExhausted, "busy") })
	p := jobqueue.NewProducer(c.dial(), jobqueue.ProducerOptions{
		Retry: jobqueue.RetryPolicy{Attempts: 4, Base: time.Millisecond, Cap: 2 * time.Millisecond},
	})

	_, err := p.Enqueue(t.Context(), jobqueue.EnqueueParams{Queue: "q"})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("Enqueue = %v, want the last status, ResourceExhausted", err)
	}
	if !jobqueue.IsRetryable(err) {
		t.Errorf("IsRetryable(%v) = false, want true", err)
	}
	if got := c.brokers[0].callCount("Enqueue"); got != 4 {
		t.Fatalf("the broker saw %d attempts, want 4", got)
	}
}

func TestProducerDoesNotRetryInvalid(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	p := c.producer()

	_, err := p.Enqueue(t.Context(), jobqueue.EnqueueParams{Queue: "not a valid name"})
	if !errors.Is(err, jobqueue.ErrInvalid) {
		t.Fatalf("Enqueue = %v, want ErrInvalid", err)
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("status.Code(%v) = %v, want the status kept for the caller", err, status.Code(err))
	}
	if jobqueue.IsRetryable(err) {
		t.Errorf("IsRetryable(%v) = true, want false", err)
	}
	if got := c.brokers[0].callCount("Enqueue"); got != 1 {
		t.Fatalf("the broker saw %d attempts, want 1", got)
	}
}

func TestProducerPerCallTimeoutIsRetried(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	var hung atomic.Bool
	c.brokers[0].setFault(func(ctx context.Context, method string) error {
		if method == "Enqueue" && !hung.Swap(true) {
			<-ctx.Done() // the first attempt never answers
			return ctx.Err()
		}
		return nil
	})
	p := jobqueue.NewProducer(c.dial(), jobqueue.ProducerOptions{
		PerCallTimeout: 50 * time.Millisecond,
		Retry:          jobqueue.RetryPolicy{Attempts: 3, Base: time.Millisecond, Cap: 2 * time.Millisecond},
	})

	if _, err := p.Enqueue(t.Context(), jobqueue.EnqueueParams{Queue: "q"}); err != nil {
		t.Fatalf("Enqueue = %v, want the retry after the first attempt timed out to succeed", err)
	}
	if got := c.brokers[0].callCount("Enqueue"); got != 2 {
		t.Fatalf("the broker saw %d attempts, want 2", got)
	}
}

func TestProducerStopsRetryingWhenTheCallersContextEnds(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	c.brokers[0].setFault(func(context.Context, string) error { return status.Error(codes.Unavailable, "down") })
	p := jobqueue.NewProducer(c.dial(), jobqueue.ProducerOptions{
		Retry: jobqueue.RetryPolicy{Attempts: 1000, Base: 20 * time.Millisecond, Cap: 20 * time.Millisecond},
	})

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := p.Enqueue(ctx, jobqueue.EnqueueParams{Queue: "q"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Enqueue = %v, want an error that wraps the context's", err)
	}
	if jobqueue.IsRetryable(err) {
		t.Errorf("IsRetryable(%v) = true: a caller whose deadline passed must not be told to try again", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Enqueue took %v after its context ended", elapsed)
	}
}

func TestProducerCancelAndNotFound(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	p := c.producer()

	res, err := p.Enqueue(t.Context(), jobqueue.EnqueueParams{Queue: "q"})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Cancel(t.Context(), res.ID); err != nil {
		t.Fatal(err)
	}
	info, err := p.Get(t.Context(), res.ID)
	if err != nil || info.State != "cancelled" || info.FinishedAt.IsZero() {
		t.Fatalf("Get = %+v, %v, want a cancelled job with a finish time", info, err)
	}
	if err := p.Cancel(t.Context(), res.ID); err != nil {
		t.Errorf("cancelling a cancelled job = %v, want success", err)
	}

	const missing = 987654
	if _, err := p.Get(t.Context(), missing); !errors.Is(err, jobqueue.ErrNotFound) {
		t.Errorf("Get(missing) = %v, want ErrNotFound", err)
	}
	if err := p.Cancel(t.Context(), missing); !errors.Is(err, jobqueue.ErrNotFound) {
		t.Errorf("Cancel(missing) = %v, want ErrNotFound", err)
	}
	if got := c.brokers[0].callCount("GetJob"); got != 2 {
		t.Errorf("GetJob calls = %d, want 2: ErrNotFound is final and must not be retried", got)
	}
	if got := c.brokers[0].callCount("CancelJob"); got != 3 {
		t.Errorf("CancelJob calls = %d, want 3", got)
	}

	// A job that succeeded cannot be cancelled; the status says why.
	done := c.add("q", 1)[0]
	grants, err := c.mem.Lease(t.Context(), store.LeaseRequest{
		Queue: "q", Takes: []store.TenantTake{{Limit: 1}}, TTL: time.Minute, Worker: "w",
	})
	if err != nil || len(grants) != 1 || grants[0].Job.ID != done {
		t.Fatalf("Lease = %v, %v", grants, err)
	}
	ref := store.Ref{ID: done, Queue: "q", Token: grants[0].Job.Epoch}
	if out, err := c.mem.Complete(t.Context(), []store.CompleteRequest{{Ref: ref}}); err != nil || out[0] != job.Applied {
		t.Fatalf("Complete = %v, %v", out, err)
	}
	err = p.Cancel(t.Context(), done)
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("Cancel(succeeded job) = %v, want FailedPrecondition", err)
	}
}

func TestProducerGetReportsResultAndError(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	p := c.producer()
	ids := c.add("q", 2)
	grants, err := c.mem.Lease(t.Context(), store.LeaseRequest{
		Queue: "q", Takes: []store.TenantTake{{Limit: 2}}, TTL: time.Minute, Worker: "w",
	})
	if err != nil || len(grants) != 2 {
		t.Fatalf("Lease = %v, %v", grants, err)
	}
	ok := store.Ref{ID: grants[0].Job.ID, Queue: "q", Token: grants[0].Job.Epoch}
	bad := store.Ref{ID: grants[1].Job.ID, Queue: "q", Token: grants[1].Job.Epoch}
	if _, err := c.mem.Complete(t.Context(), []store.CompleteRequest{{Ref: ok, Result: []byte("42")}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.mem.Fail(t.Context(), []store.FailRequest{{Ref: bad, Permanent: true, Error: "boom"}}); err != nil {
		t.Fatal(err)
	}

	succeeded, err := p.Get(t.Context(), ids[0])
	if err != nil || succeeded.State != "succeeded" || !bytes.Equal(succeeded.Result, []byte("42")) || succeeded.Attempts != 1 {
		t.Errorf("Get = %+v, %v, want a succeeded job with result 42 after 1 attempt", succeeded, err)
	}
	dead, err := p.Get(t.Context(), ids[1])
	if err != nil || dead.State != "dead" || dead.LastError != "boom" {
		t.Errorf("Get = %+v, %v, want a dead job with the error boom", dead, err)
	}
}

func TestProducerEnqueueBatch(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	p := c.producer()

	res, err := p.EnqueueBatch(t.Context(), []jobqueue.EnqueueParams{
		{Queue: "a", Payload: []byte("1")},
		{Queue: "b", Payload: []byte("2"), IdempotencyKey: "k"},
		{Queue: "b", Payload: []byte("3"), IdempotencyKey: "k"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 3 {
		t.Fatalf("%d results, want 3", len(res))
	}
	if res[0].ID >= res[1].ID || res[1].Deduplicated || !res[2].Deduplicated || res[2].ID != res[1].ID {
		t.Errorf("results %+v: want ids in request order and the third item to name the second's job", res)
	}
	if got := string(c.get(res[0].ID).Payload); got != "1" {
		t.Errorf("job %d has payload %q, want 1", res[0].ID, got)
	}
}

func TestProducerEnqueueBatchSizeLimits(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	p := c.producer()

	if _, err := p.EnqueueBatch(t.Context(), nil); !errors.Is(err, jobqueue.ErrInvalid) {
		t.Errorf("EnqueueBatch(nil) = %v, want ErrInvalid", err)
	}
	over := make([]jobqueue.EnqueueParams, job.MaxBatch+1)
	for i := range over {
		over[i] = jobqueue.EnqueueParams{Queue: "q"}
	}
	if _, err := p.EnqueueBatch(t.Context(), over); !errors.Is(err, jobqueue.ErrInvalid) {
		t.Errorf("EnqueueBatch(65 items) = %v, want ErrInvalid", err)
	}
	if got := c.brokers[0].callCount("Enqueue"); got != 0 {
		t.Errorf("the broker saw %d calls, want 0: a batch of the wrong size is refused before it is sent", got)
	}
	if res, err := p.EnqueueBatch(t.Context(), over[:job.MaxBatch]); err != nil || len(res) != job.MaxBatch {
		t.Errorf("EnqueueBatch(64 items) = %d results, %v, want all 64", len(res), err)
	}
}

func TestProducerRejectsAMaxAttemptsThatDoesNotFit(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	p := c.producer()
	_, err := p.Enqueue(t.Context(), jobqueue.EnqueueParams{Queue: "q", MaxAttempts: 1 << 40})
	if !errors.Is(err, jobqueue.ErrInvalid) {
		t.Fatalf("Enqueue = %v, want ErrInvalid", err)
	}
	if got := c.brokers[0].callCount("Enqueue"); got != 0 {
		t.Errorf("the broker saw %d calls, want 0", got)
	}
}

func TestIsRetryable(t *testing.T) {
	for code, want := range map[codes.Code]bool{
		codes.Unavailable:        true,
		codes.ResourceExhausted:  true,
		codes.Aborted:            true,
		codes.DeadlineExceeded:   true,
		codes.InvalidArgument:    false,
		codes.NotFound:           false,
		codes.PermissionDenied:   false,
		codes.Unauthenticated:    false,
		codes.FailedPrecondition: false,
		codes.Canceled:           false,
		codes.Internal:           false,
		codes.Unknown:            false,
	} {
		if got := jobqueue.IsRetryable(status.Error(code, "x")); got != want {
			t.Errorf("IsRetryable(%v) = %v, want %v", code, got, want)
		}
	}
	if jobqueue.IsRetryable(nil) {
		t.Error("IsRetryable(nil) = true")
	}
	if jobqueue.IsRetryable(errors.New("not a status")) {
		t.Error("IsRetryable(plain error) = true")
	}
}
