package jobqueue_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	jobqueue "github.com/jason-te-sde/distributed-job-queue"
	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	jobqueuev1 "github.com/jason-te-sde/distributed-job-queue/internal/gen/jobqueue/v1"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/memstore"
)

// within is how long a test waits for something that should take a few
// milliseconds. It is far above that and far below the lease TTLs and
// timeouts that a missing step would fall back on.
const within = 10 * time.Second

func noop(context.Context, *jobqueue.Job) error { return nil }

// addSpec stores one job with the given spec and returns its id.
func (c *cluster) addSpec(s job.Spec) int64 {
	c.t.Helper()
	if s.Queue == "" {
		s.Queue = "q"
	}
	if s.Tenant == "" {
		s.Tenant = "default"
	}
	res, err := c.mem.Enqueue(c.t.Context(), []job.Spec{s})
	if err != nil {
		c.t.Fatal(err)
	}
	return res[0].ID
}

func TestWorkerProcessesJobs(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	p := c.producer()

	var handled atomic.Int64
	c.startWorker(jobqueue.WorkerOptions{Concurrency: 8}, func(_ context.Context, j *jobqueue.Job) error {
		handled.Add(1)
		return nil
	})

	batch := make([]jobqueue.EnqueueParams, 0, 64)
	for i := range 100 {
		batch = append(batch, jobqueue.EnqueueParams{Queue: "q", Payload: fmt.Appendf(nil, "job %d", i)})
		if len(batch) == cap(batch) || i == 99 {
			if _, err := p.EnqueueBatch(t.Context(), batch); err != nil {
				t.Fatal(err)
			}
			batch = batch[:0]
		}
	}

	c.await("100 jobs succeeded", 5*time.Second, func() bool { return c.count("q", job.StateSucceeded) == 100 })
	if got := handled.Load(); got != 100 {
		t.Errorf("handlers ran %d times for 100 jobs", got)
	}
}

func TestWorkerHandlerSeesTheJob(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	id := c.addSpec(job.Spec{Queue: "q", Tenant: "acme", Payload: []byte("hello"), MaxAttempts: 7})

	seen := make(chan jobqueue.Job, 1)
	c.startWorker(jobqueue.WorkerOptions{}, func(_ context.Context, j *jobqueue.Job) error {
		seen <- *j
		return nil
	})
	got := <-seen

	if got.ID != id || got.Queue != "q" || got.Tenant != "acme" || string(got.Payload) != "hello" {
		t.Errorf("job = %+v, want id %d, queue q, tenant acme, payload hello", got, id)
	}
	if got.Attempt != 1 || got.MaxAttempts != 7 {
		t.Errorf("Attempt, MaxAttempts = %d, %d, want 1, 7", got.Attempt, got.MaxAttempts)
	}
	stored := c.get(id)
	if got.Token() != stored.Epoch || got.Token() == 0 {
		t.Errorf("Token() = %d, want the lease's fencing token %d", got.Token(), stored.Epoch)
	}
	if !got.EnqueuedAt.Equal(stored.CreatedAt) {
		t.Errorf("EnqueuedAt = %v, want %v", got.EnqueuedAt, stored.CreatedAt)
	}
}

func TestWorkerRetryAfterAndPermanent(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	later := c.addSpec(job.Spec{})
	never := c.addSpec(job.Spec{})
	plain := c.addSpec(job.Spec{})

	var plainAttempts []int
	var mu sync.Mutex
	c.startWorker(jobqueue.WorkerOptions{Concurrency: 3}, func(_ context.Context, j *jobqueue.Job) error {
		switch j.ID {
		case later:
			return jobqueue.RetryAfter(errors.New("try me in an hour"), time.Hour)
		case never:
			return jobqueue.Permanent(fmt.Errorf("wrapped: %w", errors.New("cannot be done")))
		}
		mu.Lock()
		plainAttempts = append(plainAttempts, j.Attempt)
		mu.Unlock()
		if j.Attempt == 1 {
			return errors.New("flaky")
		}
		return nil
	})

	c.await("the three outcomes", within, func() bool {
		return c.get(later).LastError != "" && c.get(never).State == job.StateDead && c.get(plain).State == job.StateSucceeded
	})

	l := c.get(later)
	if l.State != job.StateReady || l.Attempts != 1 || l.LastError != "try me in an hour" {
		t.Errorf("RetryAfter job = %+v, want ready after 1 attempt with the handler's error", l)
	}
	if d := time.Until(l.RunAt); d < 59*time.Minute || d > time.Hour {
		t.Errorf("RetryAfter job runs in %v, want about an hour", d)
	}
	if n := c.get(never); n.Attempts != 1 || n.LastError != "wrapped: cannot be done" {
		t.Errorf("Permanent job = %+v, want dead after 1 attempt with the wrapped error's text", n)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(plainAttempts) != 2 || plainAttempts[0] != 1 || plainAttempts[1] != 2 {
		t.Errorf("a plain error ran attempts %v, want [1 2]: it must retry", plainAttempts)
	}
}

func TestWorkerPanicIsRetryableFailure(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	// The job has the default attempts, so a panic that is treated as the
	// retryable failure it should be leaves it ready, where a permanent one
	// would leave it dead. It panics on its first attempt only, so that
	// nothing churns after the failure has been looked at.
	boom := c.addSpec(job.Spec{})
	after := c.addSpec(job.Spec{})

	c.startWorker(jobqueue.WorkerOptions{}, func(_ context.Context, j *jobqueue.Job) error {
		if j.ID == boom && j.Attempt == 1 {
			panic("kaboom")
		}
		return nil
	})

	// The failure the panic reported is read from the change the store made
	// for it: by the time a test looks at the row, the retry may have run.
	failure := func() (memstore.Change, bool) {
		for _, ch := range c.changesOf(boom) {
			if ch.Op == memstore.OpFail {
				return ch, true
			}
		}
		return memstore.Change{}, false
	}
	c.await("the panic to be reported and the other job to finish", within, func() bool {
		_, reported := failure()
		return reported && c.get(after).State == job.StateSucceeded
	})
	ch, _ := failure()
	if ch.After.State != job.StateReady || ch.After.Attempts != 1 {
		t.Errorf("after the panic the job is %v with %d attempts, want ready with 1: a panic is a retryable failure", ch.After.State, ch.After.Attempts)
	}
	if got := ch.After.LastError; !strings.HasPrefix(got, "panic: kaboom\n") || !strings.Contains(got, "goroutine") {
		t.Errorf("LastError = %q, want %q followed by the stack", got, "panic: kaboom")
	}
}

func TestWorkerCancelsHandlerOnLostLease(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	p := c.producer()
	id := c.add("q", 1)[0]

	started := make(chan struct{})
	cancelled := make(chan error, 1)
	c.startWorker(jobqueue.WorkerOptions{}, func(ctx context.Context, _ *jobqueue.Job) error {
		close(started)
		<-ctx.Done()
		cancelled <- context.Cause(ctx)
		return ctx.Err()
	})

	<-started
	cancelledAt := time.Now()
	if err := p.Cancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	select {
	case cause := <-cancelled:
		if d := time.Since(cancelledAt); d > 600*time.Millisecond {
			t.Errorf("the handler's context ended %v after the job was cancelled, want within 600ms", d)
		}
		if !strings.Contains(cause.Error(), "lease lost") {
			t.Errorf("context.Cause = %q, want it to say the lease was lost", cause)
		}
	case <-time.After(within):
		t.Fatal("the handler's context never ended")
	}
}

func TestWorkerUncooperativeHandlerIsFenced(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	id := c.add("q", 1)[0]

	// Worker A is cut off from the broker's heartbeats, so its lease runs out,
	// and its handler ignores the cancellation it is sent.
	var cut atomic.Bool
	cut.Store(true)
	partition := jobqueue.WithGRPCOptions(grpc.WithChainUnaryInterceptor(
		func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			if strings.HasSuffix(method, "/Heartbeat") && cut.Load() {
				return status.Error(codes.Unavailable, "partitioned")
			}
			return invoker(ctx, method, req, reply, cc, opts...)
		}))
	aToken := make(chan int64, 1)
	unblockA := make(chan struct{})
	a := c.startWorker(jobqueue.WorkerOptions{}, func(_ context.Context, j *jobqueue.Job) error {
		aToken <- j.Token()
		<-unblockA // deaf to the context
		return nil
	}, partition)
	tokenA := <-aToken

	// Worker B takes the job over once the broker has taken it back.
	bToken := make(chan int64, 1)
	c.startWorker(jobqueue.WorkerOptions{}, func(_ context.Context, j *jobqueue.Job) error {
		bToken <- j.Token()
		return nil
	})
	c.await("worker B to finish the job", within, func() bool { return c.get(id).State == job.StateSucceeded })
	tokenB := <-bToken
	if tokenB == tokenA {
		t.Fatalf("both workers ran the job under token %d", tokenA)
	}

	// A's handler finally returns, and its report is turned away. Stopping A
	// waits for the report to be answered.
	close(unblockA)
	if err := a.stop(); err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if got := c.ackCount("complete", "fenced"); got != 1 {
		t.Errorf("fenced completes = %v, want 1: A's late report", got)
	}
	if got := c.ackCount("complete", "accepted"); got != 1 {
		t.Errorf("accepted completes = %v, want 1: B's report", got)
	}
	final := c.get(id)
	if final.State != job.StateSucceeded || final.Epoch != tokenB {
		t.Errorf("job = state %v under token %d, want succeeded under B's token %d", final.State, final.Epoch, tokenB)
	}
}

func TestWorkerConcurrencyBound(t *testing.T) {
	const concurrency, jobs = 3, 20
	c := newCluster(t, clusterOpts{})
	c.add("q", jobs)

	var running, peak, arrived atomic.Int32
	barrier := make(chan struct{})
	c.startWorker(jobqueue.WorkerOptions{Concurrency: concurrency}, func(context.Context, *jobqueue.Job) error {
		n := running.Add(1)
		defer running.Add(-1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		// The first handlers wait for each other, which shows that the worker
		// really runs `concurrency` of them at once.
		if k := arrived.Add(1); k <= concurrency {
			if k == concurrency {
				close(barrier)
			}
			<-barrier
		}
		return nil
	})

	c.await("all jobs succeeded", 5*time.Second, func() bool { return c.count("q", job.StateSucceeded) == jobs })
	if got := peak.Load(); got != concurrency {
		t.Errorf("most handlers at once = %d, want exactly %d", got, concurrency)
	}
}

func TestWorkerSlotFreedOnlyWhenHandlerReturns(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	p := c.producer()
	first := c.add("q", 1)[0]

	started := make(chan struct{})
	lost := make(chan struct{})
	release := make(chan struct{})
	c.startWorker(jobqueue.WorkerOptions{Concurrency: 1}, func(ctx context.Context, j *jobqueue.Job) error {
		if j.ID != first {
			return nil
		}
		close(started)
		<-ctx.Done()
		close(lost)
		<-release // the handler goes on running after its lease is lost
		return nil
	})

	<-started
	second := c.add("q", 1)[0]
	if err := p.Cancel(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	<-lost

	// The lease is lost and its handler told to stop, but the handler still
	// runs: the slot is still taken, so the worker asks for nothing and the
	// second job stays where it is.
	time.Sleep(600 * time.Millisecond)
	if got := c.brokers[0].callCount("Lease"); got != 1 {
		t.Errorf("the worker made %d Lease calls, want 1: no slot was free for another", got)
	}
	if s := c.get(second); s.State != job.StateReady || s.Attempts != 0 {
		t.Errorf("second job = %+v, want it untouched while the only slot is held", s)
	}

	close(release)
	c.await("the second job once the slot is free", within, func() bool { return c.get(second).State == job.StateSucceeded })
}

func TestWorkerDrainReleasesAfterTimeout(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	id := c.add("q", 1)[0]

	started := make(chan struct{})
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	w := jobqueue.NewWorker(c.dial(), fast(jobqueue.WorkerOptions{DrainTimeout: 300 * time.Millisecond}))
	if err := w.Handle(func(context.Context, *jobqueue.Job) error {
		close(started)
		<-block // never returns, whatever it is told
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	<-started
	cancel()
	cancelledAt := time.Now()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
		if d := time.Since(cancelledAt); d > time.Second {
			t.Errorf("Run returned %v after its context ended, want within 1s", d)
		}
	case <-time.After(within):
		t.Fatal("Run did not return although the drain timeout passed")
	}

	if j := c.get(id); j.State != job.StateReady || j.Attempts != 0 {
		t.Errorf("job = state %v after %d attempts, want ready with the attempt given back", j.State, j.Attempts)
	}
}

func TestWorkerDrainWaitsForRunningHandlers(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	id := c.add("q", 1)[0]

	started := make(chan struct{})
	finish := make(chan struct{})
	rw := c.startWorker(jobqueue.WorkerOptions{DrainTimeout: 5 * time.Second}, func(ctx context.Context, _ *jobqueue.Job) error {
		close(started)
		<-finish
		if ctx.Err() != nil {
			return fmt.Errorf("cancelled during a drain that was not over: %w", context.Cause(ctx))
		}
		return nil
	})

	<-started
	rw.cancel()
	select {
	case <-rw.done:
		t.Fatalf("Run returned %v while a handler was still running", rw.err)
	default:
	}
	close(finish)
	if err := rw.stop(); err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if j := c.get(id); j.State != job.StateSucceeded || j.Attempts != 1 {
		t.Errorf("job = state %v after %d attempts, want succeeded: the drain waits for the handler and reports it", j.State, j.Attempts)
	}
}

// A Lease call that is abandoned by a drain can still be answered with jobs:
// the broker leased them before the call was cancelled. The tracker has
// stopped by then, but the leases are real, so the worker must give them back
// instead of leaving them to run out.
func TestWorkerReleasesTheJobsOfALateLeaseAnswer(t *testing.T) {
	c := newCluster(t, clusterOpts{})

	// The connection holds the answer of a Lease call that carries jobs, and
	// ignores the cancellation of the call, until the test lets it go.
	held := make(chan struct{})
	gate := make(chan struct{})
	var once sync.Once
	hold := jobqueue.WithGRPCOptions(grpc.WithChainUnaryInterceptor(
		func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			if !strings.HasSuffix(method, "/Lease") {
				return invoker(ctx, method, req, reply, cc, opts...)
			}
			err := invoker(context.WithoutCancel(ctx), method, req, reply, cc, opts...)
			if resp, ok := reply.(*jobqueuev1.LeaseResponse); err == nil && ok && len(resp.GetJobs()) > 0 {
				once.Do(func() { close(held) })
				<-gate
			}
			return err
		}))
	draining := newSignalWriter("worker draining")
	logger := slog.New(slog.NewTextHandler(draining, nil))
	rw := c.startWorker(jobqueue.WorkerOptions{Logger: logger}, func(context.Context, *jobqueue.Job) error {
		t.Error("a handler ran for a job whose lease answer came after the drain")
		return nil
	}, hold)

	id := c.add("q", 1)[0]
	<-held
	rw.cancel()
	<-draining.seen // the loop has called the tracker's Drain, and the next thing it hears is the answer
	close(gate)
	if err := rw.stop(); err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}

	if j := c.get(id); j.State != job.StateReady || j.Attempts != 0 {
		t.Errorf("job = state %v after %d attempts, want ready with the attempt given back: Run must wait for the late answer and release its jobs", j.State, j.Attempts)
	}
}

func TestWorkerRunTwiceAndHandleAfterRun(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	c.add("q", 1)
	w := jobqueue.NewWorker(c.dial(), fast(jobqueue.WorkerOptions{}))
	var once sync.Once
	started := make(chan struct{})
	if err := w.Handle(func(context.Context, *jobqueue.Job) error {
		once.Do(func() { close(started) })
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	_, _, line, _ := runtime.Caller(0)
	go func() { done <- w.Run(ctx) }() // the line after the one above
	<-started
	site := fmt.Sprintf("worker_test.go:%d", line+1)

	if err := w.Run(ctx); !errors.Is(err, jobqueue.ErrAlreadyRunning) || !strings.Contains(err.Error(), site) {
		t.Errorf("second Run = %v, want ErrAlreadyRunning naming %s", err, site)
	}
	if err := w.Handle(noop); !errors.Is(err, jobqueue.ErrAlreadyRunning) || !strings.Contains(err.Error(), site) {
		t.Errorf("Handle after Run = %v, want ErrAlreadyRunning naming %s", err, site)
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run = %v, want nil", err)
	}
}

func TestWorkerRunThatFailedValidationDoesNotCount(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	id := c.add("q", 1)[0]
	w := jobqueue.NewWorker(c.dial(), fast(jobqueue.WorkerOptions{}))

	if err := w.Run(t.Context()); !errors.Is(err, jobqueue.ErrInvalid) {
		t.Fatalf("Run without a handler = %v, want ErrInvalid", err)
	}
	if err := w.Handle(nil); !errors.Is(err, jobqueue.ErrInvalid) {
		t.Errorf("Handle(nil) = %v, want ErrInvalid", err)
	}
	if err := w.Handle(noop); err != nil {
		t.Fatalf("Handle after a Run that did not start = %v, want nil", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	c.await("the job to succeed", within, func() bool { return c.get(id).State == job.StateSucceeded })
	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run = %v, want nil", err)
	}
}

func TestWorkerOptionsValidation(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	conn := c.dial()
	cancelled, cancel := context.WithCancel(t.Context())
	cancel() // a valid worker returns at once instead of starting

	for name, mutate := range map[string]func(*jobqueue.WorkerOptions){
		"ttl of 11s with a margin of 3s": func(o *jobqueue.WorkerOptions) { o.LeaseTTL, o.Margin = 11*time.Second, 3*time.Second },
		"ttl exactly four margins":       func(o *jobqueue.WorkerOptions) { o.LeaseTTL, o.Margin = 12*time.Second, 3*time.Second },
		"ttl below a second":             func(o *jobqueue.WorkerOptions) { o.LeaseTTL, o.Margin = 500*time.Millisecond, 10*time.Millisecond },
		"negative margin":                func(o *jobqueue.WorkerOptions) { o.Margin = -time.Millisecond },
		"no queue":                       func(o *jobqueue.WorkerOptions) { o.Queue = "" },
		"bad queue name":                 func(o *jobqueue.WorkerOptions) { o.Queue = "not a name" },
		"negative concurrency":           func(o *jobqueue.WorkerOptions) { o.Concurrency = -1 },
		"too much concurrency":           func(o *jobqueue.WorkerOptions) { o.Concurrency = 10001 },
		"long poll over 30s":             func(o *jobqueue.WorkerOptions) { o.LongPoll = 31 * time.Second },
		"negative long poll":             func(o *jobqueue.WorkerOptions) { o.LongPoll = -time.Second },
		"negative drain timeout":         func(o *jobqueue.WorkerOptions) { o.DrainTimeout = -time.Second },
		"negative ack window":            func(o *jobqueue.WorkerOptions) { o.AckWindow = -time.Millisecond },
		"negative ack batch":             func(o *jobqueue.WorkerOptions) { o.AckBatch = -1 },
		"ack batch over 128":             func(o *jobqueue.WorkerOptions) { o.AckBatch = 129 },
		"worker id with a newline":       func(o *jobqueue.WorkerOptions) { o.WorkerID = "bad\nid" },
		"worker id of 129 bytes":         func(o *jobqueue.WorkerOptions) { o.WorkerID = strings.Repeat("w", 129) },
	} {
		t.Run(name, func(t *testing.T) {
			o := fast(jobqueue.WorkerOptions{})
			mutate(&o)
			w := jobqueue.NewWorker(conn, o)
			if err := w.Handle(noop); err != nil {
				t.Fatal(err)
			}
			if err := w.Run(cancelled); !errors.Is(err, jobqueue.ErrInvalid) {
				t.Errorf("Run = %v, want ErrInvalid", err)
			}
		})
	}

	t.Run("ttl just above four margins", func(t *testing.T) {
		o := fast(jobqueue.WorkerOptions{})
		o.LeaseTTL, o.Margin = 13*time.Second, 3*time.Second
		w := jobqueue.NewWorker(conn, o)
		if err := w.Handle(noop); err != nil {
			t.Fatal(err)
		}
		if err := w.Run(cancelled); err != nil {
			t.Errorf("Run = %v, want TTL 13s with margin 3s accepted", err)
		}
	})
	t.Run("no handler", func(t *testing.T) {
		w := jobqueue.NewWorker(conn, fast(jobqueue.WorkerOptions{}))
		if err := w.Run(cancelled); !errors.Is(err, jobqueue.ErrInvalid) {
			t.Errorf("Run = %v, want ErrInvalid", err)
		}
	})
	t.Run("no connection", func(t *testing.T) {
		w := jobqueue.NewWorker(nil, fast(jobqueue.WorkerOptions{}))
		if err := w.Handle(noop); err != nil {
			t.Fatal(err)
		}
		if err := w.Run(cancelled); !errors.Is(err, jobqueue.ErrInvalid) {
			t.Errorf("Run = %v, want ErrInvalid", err)
		}
	})
	t.Run("defaults are valid", func(t *testing.T) {
		w := jobqueue.NewWorker(conn, jobqueue.WorkerOptions{Queue: "q"})
		if err := w.Handle(noop); err != nil {
			t.Fatal(err)
		}
		if err := w.Run(cancelled); err != nil {
			t.Errorf("Run = %v, want the defaults (30s TTL, 3s margin, generated worker id) to be accepted", err)
		}
	})
}

func TestWorkerSwitchesBrokerOnUnavailable(t *testing.T) {
	c := newCluster(t, clusterOpts{brokers: 2})
	c.brokers[0].srv.SetDraining()
	c.add("q", 20)

	// The worker starts out knowing only the draining broker, so its first
	// Lease is certain to be refused. Once that has happened the other broker
	// becomes known.
	refused := make(chan struct{})
	var once sync.Once
	c.brokers[0].setFault(func(_ context.Context, method string) error {
		if method == "Lease" {
			once.Do(func() { close(refused) })
		}
		return nil
	})
	r := manual.NewBuilderWithScheme("phase")
	r.InitialState(resolver.State{Addresses: []resolver.Address{{Addr: c.brokers[0].addr}}})
	conn, err := jobqueue.Dial("phase:///brokers", jobqueue.WithInsecure(), jobqueue.WithGRPCOptions(grpc.WithResolvers(r)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	w := jobqueue.NewWorker(conn, fast(jobqueue.WorkerOptions{Concurrency: 4}))
	if err := w.Handle(noop); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	<-refused
	r.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: c.brokers[0].addr}, {Addr: c.brokers[1].addr}}})
	c.await("all 20 jobs to finish", within, func() bool { return c.count("q", job.StateSucceeded) == 20 })
	if got := c.brokers[1].callCount("Lease"); got == 0 {
		t.Error("the other broker never served a Lease")
	}
	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run = %v, want nil", err)
	}
}

func TestWorkerResendsAFailedAckBatch(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	id := c.add("q", 1)[0]
	var failed atomic.Bool
	c.brokers[0].setFault(func(_ context.Context, method string) error {
		if method == "Ack" && !failed.Swap(true) {
			return status.Error(codes.Unavailable, "ack lost")
		}
		return nil
	})
	c.startWorker(jobqueue.WorkerOptions{}, noop)

	c.await("the job to succeed after the first ack failed", within, func() bool { return c.get(id).State == job.StateSucceeded })
	if got := c.brokers[0].callCount("Ack"); got != 2 {
		t.Errorf("Ack calls = %d, want 2: the failed batch is sent again", got)
	}
	if j := c.get(id); j.Attempts != 1 {
		t.Errorf("job ran %d times, want 1", j.Attempts)
	}
}

func TestWorkerHeartbeatKeepsALongHandlerAlive(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	id := c.add("q", 1)[0]

	// The handler runs longer than the 1s lease, so the job is reaped and run
	// again unless heartbeats extend it.
	started := make(chan struct{})
	finish := make(chan struct{})
	c.startWorker(jobqueue.WorkerOptions{}, func(ctx context.Context, _ *jobqueue.Job) error {
		close(started)
		select {
		case <-finish:
			return nil
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	})
	<-started
	time.Sleep(1200 * time.Millisecond)
	if j := c.get(id); j.State != job.StateLeased || j.Attempts != 1 {
		t.Fatalf("job = state %v after %d attempts, want it still leased after the first lease's TTL", j.State, j.Attempts)
	}
	if got := c.brokers[0].callCount("Heartbeat"); got < 3 {
		t.Errorf("Heartbeat calls = %d, want at least 3 in 1.2s with a 250ms period", got)
	}
	close(finish)
	c.await("the job to succeed", within, func() bool { return c.get(id).State == job.StateSucceeded })
}

func TestWorkerSurvivesTwoHungHeartbeatsAtTheSmallestLegalTTL(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	id := c.add("q", 1)[0]

	// The first two heartbeats reach a broker that never answers them. A
	// worker that waits out a long timeout for each loses the lease at once,
	// because no later heartbeat goes out while one is in flight; one that
	// gives up after a period sends the third in time. The TTL is the
	// shortest the broker accepts, 1s, and the margin leaves room for it.
	var calls atomic.Int32
	hang := jobqueue.WithGRPCOptions(grpc.WithChainUnaryInterceptor(
		func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			if strings.HasSuffix(method, "/Heartbeat") && calls.Add(1) <= 2 {
				<-ctx.Done()
				return ctx.Err()
			}
			return invoker(ctx, method, req, reply, cc, opts...)
		}))
	started := make(chan struct{})
	finish := make(chan struct{})
	cancelled := make(chan error, 1)
	c.startWorker(jobqueue.WorkerOptions{Margin: 100 * time.Millisecond}, func(ctx context.Context, _ *jobqueue.Job) error {
		close(started)
		select {
		case <-finish:
			return nil
		case <-ctx.Done():
			cancelled <- context.Cause(ctx)
			return ctx.Err()
		}
	}, hang)
	<-started

	// Well past the first lease's TTL, and past the third heartbeat.
	select {
	case cause := <-cancelled:
		t.Fatalf("the handler was cancelled (%v) although only two heartbeats were lost", cause)
	case <-time.After(1500 * time.Millisecond):
	}
	if j := c.get(id); j.State != job.StateLeased || j.Attempts != 1 {
		t.Fatalf("job = state %v after %d attempts, want it still leased after the first lease's TTL", j.State, j.Attempts)
	}
	if got := calls.Load(); got < 3 {
		t.Errorf("Heartbeat calls = %d, want at least 3: two hung ones and the one that kept the lease", got)
	}
	close(finish)
	c.await("the job to succeed", within, func() bool { return c.get(id).State == job.StateSucceeded })
}

func TestWorkerSetResultAndTheResultLimit(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	ok := c.addSpec(job.Spec{})
	exact := c.addSpec(job.Spec{})
	over := c.addSpec(job.Spec{})
	big := bytes.Repeat([]byte{'x'}, job.MaxResultBytes)

	c.startWorker(jobqueue.WorkerOptions{Concurrency: 3}, func(_ context.Context, j *jobqueue.Job) error {
		switch j.ID {
		case ok:
			j.SetResult([]byte("42"))
		case exact:
			j.SetResult(big)
		case over:
			j.SetResult(append(bytes.Clone(big), 'x'))
		}
		return nil
	})

	c.await("the three jobs to finish", within, func() bool {
		return c.get(ok).State == job.StateSucceeded && c.get(exact).State == job.StateSucceeded && c.get(over).State == job.StateDead
	})
	if got := string(c.get(ok).Result); got != "42" {
		t.Errorf("result = %q, want 42", got)
	}
	if got := c.get(exact).Result; !bytes.Equal(got, big) {
		t.Errorf("a result of exactly %d bytes came back as %d bytes", job.MaxResultBytes, len(got))
	}
	if j := c.get(over); j.LastError != "result exceeds 65536 bytes" || j.Attempts != 1 || len(j.Result) != 0 {
		t.Errorf("oversized job = state %v, %d attempts, error %q, %d result bytes, want dead at once with the limit as the error",
			j.State, j.Attempts, j.LastError, len(j.Result))
	}
}

func TestWorkerFailsWithErrInternalWhenTheBrokerBreaksTheProtocol(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	// The worker asks for one job; the broker answers with two.
	c.brokers[0].setOverride(func(method string, _ any) (any, bool) {
		if method != "Lease" {
			return nil, false
		}
		return &jobqueuev1.LeaseResponse{
			Jobs: []*jobqueuev1.LeasedJob{
				{Id: 7, Queue: "q", Token: 11, Remaining: durationpb.New(time.Second)},
				{Id: 8, Queue: "q", Token: 12, Remaining: durationpb.New(time.Second)},
			},
			Waited: durationpb.New(0),
		}, true
	})
	rw := c.startWorker(jobqueue.WorkerOptions{Concurrency: 1}, func(context.Context, *jobqueue.Job) error {
		t.Error("a handler ran although the response was not valid")
		return nil
	})

	select {
	case <-rw.done:
		if !errors.Is(rw.err, jobqueue.ErrInternal) {
			t.Fatalf("Run = %v, want ErrInternal", rw.err)
		}
		if !strings.Contains(rw.err.Error(), "job 8 token 12") {
			t.Errorf("Run = %q, want the job and token of the tracker's error kept", rw.err)
		}
	case <-time.After(within):
		t.Fatal("Run did not fail")
	}
}

// The tracker measures a lease from the instant its request was sent, never
// from the instant the answer came, because the answer may have waited in the
// network for any time. These two tests hold an answer back after the broker
// has given it, which is what a slow network does, and check that the worker
// counts from the send.

func TestWorkerDiscardsAGrantThatArrivedPastItsLocalDeadline(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	c.add("q", 1)

	// The first Lease answer that carries a job reaches the worker 900ms
	// after the broker gave it. The lease is 1s with a margin of 200ms, so
	// the worker must give up on it 800ms after the request left: the grant
	// arrives past its own deadline.
	var delayed atomic.Bool
	slowAnswer := jobqueue.WithGRPCOptions(grpc.WithChainUnaryInterceptor(
		func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			err := invoker(ctx, method, req, reply, cc, opts...)
			if resp, ok := reply.(*jobqueuev1.LeaseResponse); ok && err == nil && len(resp.GetJobs()) > 0 && !delayed.Swap(true) {
				time.Sleep(900 * time.Millisecond)
			}
			return err
		}))
	// The broker holds the Release it gets, so that nothing can run the job
	// again while the test looks.
	released := make(chan struct{})
	proceed := make(chan struct{})
	var once sync.Once
	c.brokers[0].setFault(func(ctx context.Context, method string) error {
		if method == "Release" {
			once.Do(func() { close(released) })
			select {
			case <-proceed:
			case <-ctx.Done():
			}
		}
		return nil
	})
	ran := make(chan int64, 4)
	c.startWorker(jobqueue.WorkerOptions{}, func(_ context.Context, j *jobqueue.Job) error {
		ran <- j.ID
		return nil
	}, slowAnswer)

	select {
	case <-released:
	case id := <-ran:
		t.Fatalf("the handler ran for job %d, whose grant arrived after its local deadline", id)
	case <-time.After(within):
		t.Fatal("the worker never released the grant that arrived past its deadline")
	}
	select {
	case id := <-ran:
		t.Fatalf("the handler ran for job %d, whose grant arrived after its local deadline", id)
	default:
	}
	if got := c.brokers[0].callCount("Release"); got < 1 {
		t.Errorf("Release calls = %d, want at least 1", got)
	}

	// Released, the job is an ordinary ready job again.
	close(proceed)
	select {
	case <-ran:
	case <-time.After(within):
		t.Fatal("the released job never ran")
	}
}

func TestWorkerHeartbeatDeadlineIsAnchoredAtTheSendTime(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	c.add("q", 1)

	// The first heartbeat is answered at once by the broker, which extends the
	// lease to a full second from then, but the answer reaches the worker
	// 500ms later. Every later heartbeat is lost. The lease then lasts until
	// the first heartbeat's send time plus 1s, less the 200ms margin: counting
	// from the arrival instead would add the 500ms of delay.
	var calls atomic.Int32
	sent := make(chan time.Time, 1)
	slowAnswer := jobqueue.WithGRPCOptions(grpc.WithChainUnaryInterceptor(
		func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			if !strings.HasSuffix(method, "/Heartbeat") {
				return invoker(ctx, method, req, reply, cc, opts...)
			}
			if calls.Add(1) > 1 {
				return status.Error(codes.Unavailable, "lost")
			}
			sent <- time.Now()
			err := invoker(ctx, method, req, reply, cc, opts...)
			time.Sleep(500 * time.Millisecond)
			return err
		}))
	type ended struct {
		at    time.Time
		cause error
	}
	cancelled := make(chan ended, 1)
	c.startWorker(jobqueue.WorkerOptions{}, func(ctx context.Context, _ *jobqueue.Job) error {
		<-ctx.Done()
		cancelled <- ended{time.Now(), context.Cause(ctx)}
		return ctx.Err()
	}, slowAnswer)

	var heartbeatSent time.Time
	select {
	case heartbeatSent = <-sent:
	case <-time.After(within):
		t.Fatal("no heartbeat was sent")
	}
	select {
	case e := <-cancelled:
		got := e.at.Sub(heartbeatSent)
		if got < 750*time.Millisecond || got > 1050*time.Millisecond {
			t.Errorf("the handler was cancelled %v after the heartbeat was sent, want about 800ms (send time + 1s - 200ms); 1.3s would mean the 500ms of delay was added", got)
		}
		if !strings.Contains(e.cause.Error(), "lease deadline reached") {
			t.Errorf("context.Cause = %q, want it to say the deadline was reached", e.cause)
		}
	case <-time.After(within):
		t.Fatal("the handler was never cancelled")
	}
}

func TestWorkerRunStartedDirectlyWithGoNamesNoAssemblyFile(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	c.add("q", 1)
	stopped := newSignalWriter("worker stopped")
	w := jobqueue.NewWorker(c.dial(), fast(jobqueue.WorkerOptions{Logger: slog.New(slog.NewTextHandler(stopped, nil))}))
	started := make(chan struct{})
	var once sync.Once
	if err := w.Handle(func(context.Context, *jobqueue.Job) error {
		once.Do(func() { close(started) })
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// With the goroutine's first function Run itself, the caller that the
	// runtime reports is its own assembly stub, runtime.goexit.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go w.Run(ctx) //nolint:errcheck // what the test is about is the call site of Run, not its result
	<-started

	const want = "a goroutine started with go"
	err := w.Run(ctx)
	if !errors.Is(err, jobqueue.ErrAlreadyRunning) || !strings.Contains(err.Error(), want) {
		t.Errorf("second Run = %v, want ErrAlreadyRunning naming %q", err, want)
	}
	if err != nil && (strings.Contains(err.Error(), ".s:") || strings.Contains(err.Error(), "goexit")) {
		t.Errorf("second Run = %q, want no assembly file or runtime function in it", err)
	}
	if err := w.Handle(noop); !errors.Is(err, jobqueue.ErrAlreadyRunning) || !strings.Contains(err.Error(), want) {
		t.Errorf("Handle after Run = %v, want ErrAlreadyRunning naming %q", err, want)
	}
	cancel()
	select {
	case <-stopped.seen:
	case <-time.After(within):
		t.Fatal("Run did not stop")
	}
}

// A refusal that no retry can undo ends the worker: it drains, which lets the
// handlers that are running finish and be reported, and then Run returns the
// refusal. The retryable errors keep the worker backing off and asking again.

func TestWorkerStopsOnLeaseErrorsThatRetryingCannotFix(t *testing.T) {
	for _, code := range []codes.Code{codes.Unauthenticated, codes.PermissionDenied, codes.InvalidArgument} {
		t.Run(code.String(), func(t *testing.T) {
			c := newCluster(t, clusterOpts{})
			c.brokers[0].setFault(func(_ context.Context, method string) error {
				if method == "Lease" {
					return status.Error(code, "refused")
				}
				return nil
			})
			rw := c.startWorker(jobqueue.WorkerOptions{}, noop)

			select {
			case <-rw.done:
			case <-time.After(5 * time.Second):
				t.Fatal("Run did not return: it keeps retrying a request that was refused for good")
			}
			if got := status.Code(rw.err); got != code {
				t.Errorf("Run = %v, want an error carrying the status %v", rw.err, code)
			}
			if code == codes.InvalidArgument && !errors.Is(rw.err, jobqueue.ErrInvalid) {
				t.Errorf("Run = %v, want an InvalidArgument to wrap ErrInvalid", rw.err)
			}
			if got := c.brokers[0].callCount("Lease"); got != 1 {
				t.Errorf("the worker made %d Lease calls, want 1", got)
			}
		})
	}
}

func TestWorkerWithABadTokenStops(t *testing.T) {
	c := newCluster(t, clusterOpts{auth: tokensFor(t, "acme", "s3cret")})
	for name, tc := range map[string]struct {
		token string
		code  codes.Code
	}{
		"unknown token": {"wrong", codes.Unauthenticated},
		"token of a producer, a role that may not lease": {"s3cret", codes.PermissionDenied},
	} {
		t.Run(name, func(t *testing.T) {
			rw := c.startWorker(jobqueue.WorkerOptions{}, noop, jobqueue.WithToken(tc.token))
			select {
			case <-rw.done:
			case <-time.After(5 * time.Second):
				t.Fatal("Run did not return")
			}
			if got := status.Code(rw.err); got != tc.code {
				t.Errorf("Run = %v, want an error carrying the status %v", rw.err, tc.code)
			}
		})
	}
}

func TestWorkerDrainsRunningHandlersBeforeReturningARefusal(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	id := c.add("q", 1)[0]
	var leases atomic.Int32
	c.brokers[0].setFault(func(_ context.Context, method string) error {
		// The first Lease gets the job. The worker has a free slot left, so it
		// asks again at once, and that call is refused.
		if method == "Lease" && leases.Add(1) > 1 {
			return status.Error(codes.PermissionDenied, "token revoked")
		}
		return nil
	})
	draining := newSignalWriter("worker draining")
	started := make(chan struct{})
	finish := make(chan struct{})
	rw := c.startWorker(jobqueue.WorkerOptions{Concurrency: 2, DrainTimeout: 5 * time.Second, Logger: slog.New(slog.NewTextHandler(draining, nil))},
		func(ctx context.Context, _ *jobqueue.Job) error {
			close(started)
			<-finish
			return ctx.Err()
		})

	<-started
	select {
	case <-draining.seen:
	case <-time.After(within):
		t.Fatal("the worker did not start to drain after its Lease call was refused")
	}
	select {
	case <-rw.done:
		t.Fatalf("Run returned %v while a handler was still running", rw.err)
	default:
	}
	close(finish)
	select {
	case <-rw.done:
	case <-time.After(within):
		t.Fatal("Run did not return after the handler did")
	}
	if status.Code(rw.err) != codes.PermissionDenied {
		t.Errorf("Run = %v, want the refusal", rw.err)
	}
	if j := c.get(id); j.State != job.StateSucceeded {
		t.Errorf("job = state %v, want succeeded: the drain reports the handler's result", j.State)
	}
}

func TestWorkerKeepsBackingOffOnRetryableLeaseErrors(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.ResourceExhausted, codes.Aborted, codes.DeadlineExceeded, codes.Internal, codes.Unknown} {
		t.Run(code.String(), func(t *testing.T) {
			c := newCluster(t, clusterOpts{})
			id := c.add("q", 1)[0]
			var calls atomic.Int32
			c.brokers[0].setFault(func(_ context.Context, method string) error {
				if method == "Lease" && calls.Add(1) <= 2 {
					return status.Error(code, "try later")
				}
				return nil
			})
			rw := c.startWorker(jobqueue.WorkerOptions{}, noop)

			c.await("the job to succeed after two refused Lease calls", within, func() bool { return c.get(id).State == job.StateSucceeded })
			select {
			case <-rw.done:
				t.Fatalf("Run returned %v: %v is no reason to stop", rw.err, code)
			default:
			}
		})
	}
}

// ackSizes records the number of items in every Ack call that reaches the
// broker, and passes the call on.
type ackSizes struct {
	mu    sync.Mutex
	sizes []int
}

func (a *ackSizes) record(method string, req any) (any, bool) {
	if r, ok := req.(*jobqueuev1.AckRequest); ok && method == "Ack" {
		a.mu.Lock()
		a.sizes = append(a.sizes, len(r.GetItems()))
		a.mu.Unlock()
	}
	return nil, false
}

func (a *ackSizes) snapshot() []int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]int(nil), a.sizes...)
}

// finishTogether runs n jobs on a worker with the options, whose handlers all
// wait until every one of them has started and then return together, so that
// n acks become due at the same instant. It returns the size of every Ack call
// the broker received.
func finishTogether(t *testing.T, n int, opts jobqueue.WorkerOptions) []int {
	t.Helper()
	c := newCluster(t, clusterOpts{})
	sizes := &ackSizes{}
	c.brokers[0].setOverride(sizes.record)
	c.add("q", n)

	var started atomic.Int64
	allStarted := make(chan struct{})
	opts.Concurrency = n
	c.startWorker(opts, func(ctx context.Context, _ *jobqueue.Job) error {
		if started.Add(1) == int64(n) {
			close(allStarted)
		}
		select {
		case <-allStarted:
		case <-ctx.Done():
		}
		return nil
	})
	c.await("every job to succeed", within, func() bool { return c.count("q", job.StateSucceeded) == n })
	return sizes.snapshot()
}

func TestWorkerAckBatchOneSendsEachAck(t *testing.T) {
	const n = 20
	sizes := finishTogether(t, n, jobqueue.WorkerOptions{AckBatch: 1})
	if len(sizes) != n {
		t.Errorf("Ack calls = %d (sizes %v), want %d: AckBatch 1 sends every ack by itself", len(sizes), sizes, n)
	}
	for _, s := range sizes {
		if s != 1 {
			t.Fatalf("an Ack call carried %d items (sizes %v), want 1 with AckBatch 1", s, sizes)
		}
	}
}

func TestWorkerDefaultAckBatchCoalescesAcks(t *testing.T) {
	// The control of the test above: with the defaults the same 20 acks share
	// calls, so a worker that ignored AckBatch could not pass both.
	const n = 20
	sizes := finishTogether(t, n, jobqueue.WorkerOptions{})
	total, most := 0, 0
	for _, s := range sizes {
		total += s
		most = max(most, s)
	}
	if total != n {
		t.Errorf("Ack calls carried %d items in all (sizes %v), want %d", total, sizes, n)
	}
	if most < 2 {
		t.Errorf("no Ack call carried more than one item (sizes %v): the default window and batch did not coalesce", sizes)
	}
}

func TestWorkerAckWindowDelaysTheFirstAck(t *testing.T) {
	const window = 400 * time.Millisecond
	c := newCluster(t, clusterOpts{})
	c.add("q", 1)

	var returned, acked atomic.Int64 // nanoseconds on the monotonic clock of base
	base := time.Now()
	c.brokers[0].setOverride(func(method string, _ any) (any, bool) {
		if method == "Ack" {
			acked.CompareAndSwap(0, int64(time.Since(base)))
		}
		return nil, false
	})
	c.startWorker(jobqueue.WorkerOptions{AckWindow: window}, func(context.Context, *jobqueue.Job) error {
		returned.Store(int64(time.Since(base)))
		return nil
	})
	c.await("the job to succeed", within, func() bool { return c.count("q", job.StateSucceeded) == 1 })

	// The window starts when the loop learns that the handler returned, which
	// is after the handler read its clock, so the wait can only be longer.
	if got := time.Duration(acked.Load() - returned.Load()); got < window {
		t.Errorf("the first ack reached the broker %v after the handler returned, want at least the window of %v", got, window)
	}
}
