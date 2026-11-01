package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/dispatch"
	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/memstore"
)

// The upper bound of a wait that should be quick. It is far above what the
// code takes and far below the intervals that a missing wakeup would fall
// back on (the poll interval is an hour unless a test says otherwise), so a
// pass shows that the wakeup came from where the test says it did.
const quickly = 5 * time.Second

// waitFor polls cond until it holds or within has passed.
func waitFor(t testing.TB, what string, within time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", within, what)
		}
		time.Sleep(time.Millisecond)
	}
}

// gateStore holds every Lease until its gate is opened, so that a test can
// act while a fetch is in flight.
type gateStore struct {
	store.Store
	gate    chan struct{}
	entered chan struct{}
}

func newGateStore(inner store.Store) *gateStore {
	return &gateStore{Store: inner, gate: make(chan struct{}), entered: make(chan struct{}, 64)}
}

func (g *gateStore) Lease(ctx context.Context, req store.LeaseRequest) ([]store.Grant, error) {
	select {
	case g.entered <- struct{}{}:
	default:
	}
	select {
	case <-g.gate:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return g.Store.Lease(ctx, req)
}

// hangStore never answers a Lease before its context ends.
type hangStore struct{ store.Store }

func (h hangStore) Lease(ctx context.Context, _ store.LeaseRequest) ([]store.Grant, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type leaseResult struct {
	ans LeaseAnswer
	err error
}

// dispEnv is a Dispatcher over a memstore with the real clock, running.
type dispEnv struct {
	t    *testing.T
	mem  *memstore.Store
	reg  *prometheus.Registry
	d    *Dispatcher
	logs *syncBuffer

	stop context.CancelFunc
	done chan error
}

// newDispEnv starts a dispatcher. Unless cfg says otherwise the scheduler is
// drr and the poll and activity intervals are an hour, so that nothing but the
// mechanism under test can wake a waiter. wrap, if set, wraps the store the
// dispatcher uses; configure runs on the dispatcher before it starts.
func newDispEnv(t *testing.T, cfg DispatcherConfig, wrap func(store.Store) store.Store, configure ...func(*Dispatcher)) *dispEnv {
	t.Helper()
	if cfg.Scheduler == "" {
		cfg.Scheduler = "drr"
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = time.Hour
	}
	if cfg.ActivityInterval == 0 {
		cfg.ActivityInterval = time.Hour
	}
	e := &dispEnv{t: t, reg: prometheus.NewRegistry(), logs: &syncBuffer{}, done: make(chan error, 1)}
	e.mem = memstore.New(memstore.Options{Clock: time.Now})
	var st store.Store = e.mem
	if wrap != nil {
		st = wrap(e.mem)
	}
	log := slog.New(slog.NewTextHandler(e.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	e.d = NewDispatcher(st, cfg, NewMetrics(e.reg), log)
	for _, c := range configure {
		c(e.d)
	}
	ctx, cancel := context.WithCancel(context.Background())
	e.stop = cancel
	go func() { e.done <- e.d.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-e.done:
			if err != nil {
				t.Errorf("Run returned %v after shutdown", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("Run did not return within 10s of its context ending")
		}
	})
	return e
}

// shutdown ends Run and returns what it returned, failing if it takes too
// long. Cleanup then finds Run already done.
func (e *dispEnv) shutdown() error {
	e.t.Helper()
	e.stop()
	select {
	case err := <-e.done:
		e.done <- err // for the cleanup
		return err
	case <-time.After(10 * time.Second):
		e.t.Fatal("Run did not return within 10s of its context ending")
		return nil
	}
}

// add enqueues n jobs for a tenant and returns their ids.
func (e *dispEnv) add(queue, tenant string, n int) []int64 {
	e.t.Helper()
	return e.addWith(job.Spec{Queue: queue, Tenant: tenant, Payload: []byte("payload")}, n)
}

func (e *dispEnv) addWith(spec job.Spec, n int) []int64 {
	e.t.Helper()
	var ids []int64
	for n > 0 {
		batch := min(n, job.MaxBatch)
		specs := make([]job.Spec, batch)
		for i := range specs {
			specs[i] = spec
		}
		res, err := e.mem.Enqueue(e.t.Context(), specs)
		if err != nil {
			e.t.Fatalf("Enqueue: %v", err)
		}
		for _, r := range res {
			ids = append(ids, r.ID)
		}
		n -= batch
	}
	return ids
}

func (e *dispEnv) leaseAsyncCtx(ctx context.Context, queue string, max int, wait time.Duration) <-chan leaseResult {
	ch := make(chan leaseResult, 1)
	go func() {
		ans, err := e.d.Lease(ctx, LeaseCall{Queue: queue, Max: max, Wait: wait, TTL: 30 * time.Second, Worker: "w1"})
		ch <- leaseResult{ans, err}
	}()
	return ch
}

func (e *dispEnv) leaseAsync(queue string, max int, wait time.Duration) <-chan leaseResult {
	return e.leaseAsyncCtx(e.t.Context(), queue, max, wait)
}

// lease is a Lease call that the test waits for.
func (e *dispEnv) lease(queue string, max int, wait time.Duration) (LeaseAnswer, error) {
	e.t.Helper()
	r := recv(e.t, e.leaseAsync(queue, max, wait), 30*time.Second)
	return r.ans, r.err
}

// recv takes the result of an async Lease, failing if it takes too long.
func recv(t testing.TB, ch <-chan leaseResult, within time.Duration) leaseResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(within):
		t.Fatalf("the Lease call did not return within %v", within)
		return leaseResult{}
	}
}

func (e *dispEnv) metric(name string, labels map[string]string) float64 {
	e.t.Helper()
	return metricValue(e.t, e.reg, name, labels)
}

// waitFetches waits until the dispatcher has finished n fetches of the queue
// with the given result.
func (e *dispEnv) waitFetches(queue, result string, n float64) {
	e.t.Helper()
	labels := map[string]string{"queue": queue, "result": result}
	waitFor(e.t, "dispatch fetches", quickly, func() bool {
		return e.metric("jobqueue_dispatch_fetches_total", labels) >= n
	})
}

func (e *dispEnv) waitWaiters(queue string, n float64) {
	e.t.Helper()
	waitFor(e.t, "the waiters gauge", quickly, func() bool {
		return e.metric("jobqueue_waiters", map[string]string{"queue": queue}) == n
	})
}

func (e *dispEnv) job(id int64) job.Job {
	e.t.Helper()
	j, err := e.mem.Get(e.t.Context(), id, "")
	if err != nil {
		e.t.Fatalf("Get(%d): %v", id, err)
	}
	return j
}

func wantGrants(t testing.TB, r leaseResult, n int) []store.Grant {
	t.Helper()
	if r.err != nil {
		t.Fatalf("Lease failed: %v", r.err)
	}
	if len(r.ans.Grants) != n {
		t.Fatalf("Lease returned %d jobs, want %d", len(r.ans.Grants), n)
	}
	return r.ans.Grants
}

func TestDispatcherDeliversOnNotify(t *testing.T) {
	e := newDispEnv(t, DispatcherConfig{}, nil)
	res := e.leaseAsync("q", 1, 30*time.Second)
	e.waitFetches("q", fetchEmpty, 1) // the first fetch found nothing, so only a wakeup can help now
	ids := e.add("q", "t", 1)
	start := time.Now()
	e.d.Notify("q", "t")

	grants := wantGrants(t, recv(t, res, 100*time.Millisecond), 1)
	if grants[0].Job.ID != ids[0] {
		t.Errorf("leased job %d, want %d", grants[0].Job.ID, ids[0])
	}
	t.Logf("delivered %v after the notification", time.Since(start))
	if got := e.metric("jobqueue_jobs_leased_total", map[string]string{"queue": "q"}); got != 1 {
		t.Errorf("jobs_leased_total = %v, want 1", got)
	}
	e.waitWaiters("q", 0)
}

func TestDispatcherLongPollTimesOutEmpty(t *testing.T) {
	e := newDispEnv(t, DispatcherConfig{}, nil)
	const wait = 200 * time.Millisecond
	start := time.Now()
	ans, err := e.lease("q", 1, wait)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Lease = %v, want an empty answer", err)
	}
	if len(ans.Grants) != 0 {
		t.Fatalf("Lease returned %d jobs from an empty queue", len(ans.Grants))
	}
	t.Logf("Lease returned after %v", elapsed)
	if elapsed < wait || elapsed > 2*wait {
		t.Errorf("Lease returned after %v, want between %v and %v", elapsed, wait, 2*wait)
	}
	if ans.Waited < wait-10*time.Millisecond {
		t.Errorf("answer says it waited %v, want about %v", ans.Waited, wait)
	}
	e.waitWaiters("q", 0)
}

func TestDispatcherPollFindsWorkWithoutNotify(t *testing.T) {
	e := newDispEnv(t, DispatcherConfig{PollInterval: 100 * time.Millisecond}, nil)
	res := e.leaseAsync("q", 1, 30*time.Second)
	e.waitFetches("q", fetchEmpty, 1)
	start := time.Now()
	e.add("q", "t", 1) // no Notify
	wantGrants(t, recv(t, res, 300*time.Millisecond), 1)
	t.Logf("delivered %v after the enqueue", time.Since(start))
}

// slowReturnStore answers each Lease delay after the inner store has leased
// the jobs, as a store does when the way back from the database is slow, and
// notes when the call began: the inner store reads its clock after that.
type slowReturnStore struct {
	store.Store
	delay time.Duration

	mu    sync.Mutex
	began time.Time
}

func (s *slowReturnStore) Lease(ctx context.Context, req store.LeaseRequest) ([]store.Grant, error) {
	s.mu.Lock()
	s.began = time.Now()
	s.mu.Unlock()
	grants, err := s.Store.Lease(ctx, req)
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return grants, err
}

func (s *slowReturnStore) leaseBegan() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.began
}

// slowReturn is the delay that a slowReturnStore adds in the tests below.
const slowReturn = 300 * time.Millisecond

// leaseThroughASlowReturn leases one job from a store that takes slowReturn to
// answer after it leased the job. It returns the answer, the instant the call
// was made, and the instant the store was asked, which is no later than the
// instant it read its clock.
func leaseThroughASlowReturn(t *testing.T) (e *dispEnv, ans LeaseAnswer, sentAt, asked time.Time) {
	t.Helper()
	var ss *slowReturnStore
	e = newDispEnv(t, DispatcherConfig{}, func(s store.Store) store.Store {
		ss = &slowReturnStore{Store: s, delay: slowReturn}
		return ss
	})
	e.add("q", "t", 1)
	sentAt = time.Now()
	ans, err := e.lease("q", 1, 0)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	wantGrants(t, leaseResult{ans: ans}, 1)
	return e, ans, sentAt, ss.leaseBegan()
}

func TestDispatcherAnswersWithTheWaitUpToTheFetch(t *testing.T) {
	// A worker takes sentAt + Waited + Remaining as a lower bound of the
	// lease's expiry. That holds only if Waited ends before the store read its
	// clock, so it must not count the time the answer took to come back.
	_, ans, sentAt, asked := leaseThroughASlowReturn(t)
	if over := sentAt.Add(ans.Waited).Sub(asked); over > 0 {
		t.Errorf("the answer says the call waited %v, which ends %v after the store was asked: a worker would trust the lease for that much longer than it lasts", ans.Waited, over)
	}
}

func TestDispatcherLeaseWaitMetricCountsUpToTheDelivery(t *testing.T) {
	// The wait that the answer reports ends at the fetch, but what the metric
	// says is how long a caller waited for its jobs, which ends at the delivery.
	e, _, _, _ := leaseThroughASlowReturn(t)
	families, err := e.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range families {
		if mf.GetName() != "jobqueue_lease_wait_seconds" {
			continue
		}
		h := mf.GetMetric()[0].GetHistogram()
		if h.GetSampleCount() != 1 || h.GetSampleSum() < slowReturn.Seconds() {
			t.Errorf("lease_wait_seconds has %d samples adding up to %vs, want one of at least %vs: the store's return trip counts", h.GetSampleCount(), h.GetSampleSum(), slowReturn.Seconds())
		}
		return
	}
	t.Error("the lease_wait_seconds metric was not registered")
}

func TestDispatcherReleasesOrphanedGrants(t *testing.T) {
	var gs *gateStore
	e := newDispEnv(t, DispatcherConfig{}, func(s store.Store) store.Store { gs = newGateStore(s); return gs })
	id := e.add("q", "t", 1)[0]

	ctx, cancel := context.WithCancel(t.Context())
	res := e.leaseAsyncCtx(ctx, "q", 1, 30*time.Second)
	<-gs.entered // the fetch is in flight
	cancel()
	r := recv(t, res, quickly)
	if got := status.Code(r.err); got != codes.Canceled {
		t.Fatalf("Lease after the caller's context ended = %v, want Canceled", r.err)
	}

	close(gs.gate) // the fetch now finds the job, for a caller that is gone
	waitFor(t, "the orphaned job to be released", quickly, func() bool {
		return e.metric("jobqueue_orphan_releases_total", nil) == 1
	})
	j := e.job(id)
	if j.State != job.StateReady || j.Attempts != 0 {
		t.Errorf("job is %v with %d attempts, want ready with 0: the release must refund the attempt", j.State, j.Attempts)
	}
	if j.Epoch != 1 {
		t.Errorf("job epoch = %d, want 1: the job should have been leased once and then released", j.Epoch)
	}
	e.waitWaiters("q", 0)
}

func TestDispatcherDrainAnswersWaiters(t *testing.T) {
	e := newDispEnv(t, DispatcherConfig{}, nil)
	var results []<-chan leaseResult
	for range 5 {
		results = append(results, e.leaseAsync("q", 1, 30*time.Second))
	}
	e.waitWaiters("q", 5)

	start := time.Now()
	e.d.Drain()
	deadline := start.Add(100 * time.Millisecond) // all five, not each
	for i, res := range results {
		r := recv(t, res, max(time.Until(deadline), time.Millisecond))
		if r.err != nil || len(r.ans.Grants) != 0 {
			t.Errorf("waiter %d got %v, %v; want an empty answer", i, r.ans, r.err)
		}
	}
	t.Logf("all five answered %v after Drain", time.Since(start))

	_, err := e.lease("q", 1, time.Second)
	if status.Code(err) != codes.Unavailable || status.Convert(err).Message() != "broker is draining" {
		t.Errorf("Lease after Drain = %v, want Unavailable \"broker is draining\"", err)
	}
}

func TestDispatcherDRRServesSmallTenant(t *testing.T) {
	e := newDispEnv(t, DispatcherConfig{}, nil)
	e.add("q", "a", 400)
	e.add("q", "b", 8)

	var fromB int
	for i := range 3 {
		ans, err := e.lease("q", 8, 0)
		if err != nil {
			t.Fatalf("Lease %d: %v", i, err)
		}
		for _, g := range ans.Grants {
			if g.Job.Tenant == "b" {
				fromB++
			}
		}
	}
	if fromB != 8 {
		t.Errorf("tenant b got %d of its 8 jobs in the first 3 answers, want all 8: tenant a's backlog must not starve it", fromB)
	}
}

func TestDispatcherFIFOMode(t *testing.T) {
	e := newDispEnv(t, DispatcherConfig{Scheduler: "fifo"}, nil)
	e.add("q", "a", 400)
	e.add("q", "b", 8)

	var got []store.Grant
	for {
		ans, err := e.lease("q", 64, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(ans.Grants) == 0 {
			break
		}
		got = append(got, ans.Grants...)
	}
	if len(got) != 408 {
		t.Fatalf("leased %d jobs, want 408", len(got))
	}
	for i, g := range got {
		want := "a"
		if i >= 400 {
			want = "b"
		}
		if g.Job.Tenant != want {
			t.Fatalf("job %d of the sequence belongs to %q, want %q: FIFO serves tenant a's whole backlog first", i, g.Job.Tenant, want)
		}
		if i > 0 && g.Job.ID <= got[i-1].Job.ID {
			t.Fatalf("job %d has id %d after id %d, want ascending ids", i, g.Job.ID, got[i-1].Job.ID)
		}
	}
}

func TestDispatcherDelayedJobWakesAtDue(t *testing.T) {
	e := newDispEnv(t, DispatcherConfig{ActivityInterval: 50 * time.Millisecond}, nil)
	const delay = 300 * time.Millisecond
	start := time.Now()
	e.addWith(job.Spec{Queue: "q", Tenant: "t", Payload: []byte("p"), Delay: delay}, 1)

	ans, err := e.lease("q", 1, 30*time.Second)
	elapsed := time.Since(start)
	if err != nil || len(ans.Grants) != 1 {
		t.Fatalf("Lease = %v, %v; want the delayed job", ans, err)
	}
	t.Logf("job delivered after %v", elapsed)
	if elapsed < delay || elapsed > 450*time.Millisecond {
		t.Errorf("job delivered after %v, want between %v and 450ms", elapsed, delay)
	}
}

func TestDispatcherResyncWakesParkedWaiters(t *testing.T) {
	e := newDispEnv(t, DispatcherConfig{}, nil)
	res := e.leaseAsync("q", 1, 30*time.Second)
	e.waitFetches("q", fetchEmpty, 1)
	e.add("q", "t", 1) // the notification of this job is "lost"
	e.d.Resync()
	wantGrants(t, recv(t, res, quickly), 1)
}

func TestDispatcherCancelledWaiterIsForgotten(t *testing.T) {
	e := newDispEnv(t, DispatcherConfig{}, nil)
	ctx, cancel := context.WithCancel(t.Context())
	res := e.leaseAsyncCtx(ctx, "q", 1, 30*time.Second)
	e.waitFetches("q", fetchEmpty, 1)
	e.waitWaiters("q", 1)

	cancel()
	if r := recv(t, res, quickly); status.Code(r.err) != codes.Canceled {
		t.Fatalf("Lease = %v, want Canceled", r.err)
	}
	e.waitWaiters("q", 0)
}

func TestDispatcherStoreTimeoutAnswersUnavailable(t *testing.T) {
	e := newDispEnv(t, DispatcherConfig{}, func(s store.Store) store.Store { return hangStore{s} },
		func(d *Dispatcher) { d.storeTimeout = 100 * time.Millisecond })
	start := time.Now()
	_, err := e.lease("q", 1, 30*time.Second)
	elapsed := time.Since(start)
	if status.Code(err) != codes.Unavailable || status.Convert(err).Message() != "store unavailable" {
		t.Fatalf("Lease on a store that never answers = %v, want Unavailable \"store unavailable\"", err)
	}
	if elapsed < 100*time.Millisecond || elapsed > quickly {
		t.Errorf("Lease failed after %v, want about the store timeout of 100ms", elapsed)
	}
	if got := e.metric("jobqueue_dispatch_fetches_total", map[string]string{"queue": "q", "result": fetchError}); got != 1 {
		t.Errorf("fetches{error} = %v, want 1", got)
	}
	e.waitWaiters("q", 0)
}

func TestDispatcherShutdownDeliversInFlightFetch(t *testing.T) {
	var gs *gateStore
	e := newDispEnv(t, DispatcherConfig{}, func(s store.Store) store.Store { gs = newGateStore(s); return gs })
	e.add("q", "t", 1)
	res := e.leaseAsync("q", 1, 30*time.Second)
	<-gs.entered

	e.stop()
	select {
	case err := <-e.done:
		t.Fatalf("Run returned %v while a fetch was still in flight", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(gs.gate)

	wantGrants(t, recv(t, res, quickly), 1) // the leases are valid, so the caller gets them
	if err := e.shutdown(); err != nil {
		t.Errorf("Run = %v, want nil", err)
	}
}

func TestDispatcherShutdownReleasesOrphansBeforeReturning(t *testing.T) {
	var gs *gateStore
	e := newDispEnv(t, DispatcherConfig{}, func(s store.Store) store.Store { gs = newGateStore(s); return gs })
	id := e.add("q", "t", 1)[0]
	ctx, cancel := context.WithCancel(t.Context())
	res := e.leaseAsyncCtx(ctx, "q", 1, 30*time.Second)
	<-gs.entered
	cancel()
	recv(t, res, quickly)

	e.stop()
	close(gs.gate)
	if err := e.shutdown(); err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	// The release happened before Run returned, not after.
	if j := e.job(id); j.State != job.StateReady || j.Attempts != 0 || j.Epoch != 1 {
		t.Errorf("after Run returned the job is %v, attempts %d, epoch %d; want ready, 0, 1", j.State, j.Attempts, j.Epoch)
	}
}

func TestDispatcherLeaseAfterRunReturnedIsUnavailable(t *testing.T) {
	e := newDispEnv(t, DispatcherConfig{}, nil)
	if err := e.shutdown(); err != nil {
		t.Fatal(err)
	}
	_, err := e.lease("q", 1, time.Second)
	if status.Code(err) != codes.Unavailable {
		t.Errorf("Lease after Run returned = %v, want Unavailable", err)
	}
}

func TestDispatcherRunTwiceFails(t *testing.T) {
	e := newDispEnv(t, DispatcherConfig{}, nil)
	if _, err := e.lease("q", 1, 0); err != nil { // the loop is running once it answers
		t.Fatal(err)
	}
	if err := e.d.Run(t.Context()); err == nil {
		t.Error("a second Run returned nil, want an error: the dispatcher has one event loop")
	}
}

func TestRespondOutcomeMapsEveryReason(t *testing.T) {
	const waited = 3 * time.Second
	for _, tc := range []struct {
		reason  dispatch.Reason
		code    codes.Code
		message string
	}{
		{dispatch.ReasonTimeout, codes.OK, ""},
		{dispatch.ReasonDraining, codes.OK, ""},
		{dispatch.ReasonRejected, codes.Unavailable, "broker is draining"},
		{dispatch.ReasonUnavailable, codes.Unavailable, "store unavailable"},
	} {
		t.Run(tc.reason.String(), func(t *testing.T) {
			out := respondOutcome(tc.reason, waited)
			if got := status.Code(out.err); got != tc.code {
				t.Fatalf("code = %v, want %v (error: %v)", got, tc.code, out.err)
			}
			if tc.code != codes.OK {
				if got := status.Convert(out.err).Message(); got != tc.message {
					t.Errorf("message = %q, want %q", got, tc.message)
				}
				return
			}
			if len(out.ans.Grants) != 0 || out.ans.Waited != waited {
				t.Errorf("answer = %+v, want no jobs and Waited %v", out.ans, waited)
			}
		})
	}
}

func TestDispatcherSeedsDRRFromActivity(t *testing.T) {
	// Tenants that already have work when the first waiter arrives are known
	// to the ring before the first fetch, so none of them depends on a
	// notification. The jobs of b come before those of a in id order here,
	// and the quantum is 1, so a fair plan alternates.
	e := newDispEnv(t, DispatcherConfig{Quantum: 1}, nil)
	e.add("q", "b", 4)
	e.add("q", "a", 4)
	ans, err := e.lease("q", 4, 0)
	if err != nil {
		t.Fatal(err)
	}
	tenants := make([]string, len(ans.Grants))
	for i, g := range ans.Grants {
		tenants[i] = g.Job.Tenant
	}
	slices.Sort(tenants)
	if want := []string{"a", "a", "b", "b"}; !slices.Equal(tenants, want) {
		t.Errorf("tenants of the first 4 jobs = %v, want %v", tenants, want)
	}
}

// opsStore records the Activity and Lease calls the dispatcher makes, in
// order, and can fail Lease calls.
type opsStore struct {
	store.Store

	mu         sync.Mutex
	ops        []string
	failLeases int // the next n Lease calls fail
}

func takesString(takes []store.TenantTake) string {
	parts := make([]string, len(takes))
	for i, tk := range takes {
		tenant := tk.Tenant
		if tenant == "" {
			tenant = "*"
		}
		parts[i] = fmt.Sprintf("%s=%d", tenant, tk.Limit)
	}
	return strings.Join(parts, ",")
}

func (o *opsStore) Activity(ctx context.Context, queue string) (store.Activity, error) {
	o.mu.Lock()
	o.ops = append(o.ops, "activity")
	o.mu.Unlock()
	return o.Store.Activity(ctx, queue)
}

func (o *opsStore) Lease(ctx context.Context, req store.LeaseRequest) ([]store.Grant, error) {
	o.mu.Lock()
	o.ops = append(o.ops, "lease:"+takesString(req.Takes))
	fail := o.failLeases > 0
	if fail {
		o.failLeases--
	}
	o.mu.Unlock()
	if fail {
		return nil, fmt.Errorf("lease: %w", store.ErrUnavailable)
	}
	return o.Store.Lease(ctx, req)
}

func (o *opsStore) seen() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return slices.Clone(o.ops)
}

func TestDispatcherEmptyRingReadsActivityBeforePlanning(t *testing.T) {
	// The poll of a queue whose ring is empty must read the activity first. A
	// job that appeared without a notification is then planned for its tenant,
	// instead of being found by a fetch across all tenants, which would leave
	// the ring empty again.
	var ops *opsStore
	e := newDispEnv(t, DispatcherConfig{PollInterval: 300 * time.Millisecond},
		func(s store.Store) store.Store { ops = &opsStore{Store: s}; return ops })
	res := e.leaseAsync("q", 1, 30*time.Second)
	e.waitFetches("q", fetchEmpty, 1)
	e.add("q", "t", 1) // no Notify; the next poll is 300ms away
	wantGrants(t, recv(t, res, quickly), 1)

	seen := ops.seen()
	if n := len(seen); n < 2 || seen[n-2] != "activity" || seen[n-1] != "lease:t=1" {
		t.Errorf("the store calls end with %v, want an activity read and then a lease planned for tenant t", seen)
	}
	if seen[0] != "activity" || seen[1] != "lease:*=1" {
		t.Errorf("the store calls begin with %v, want an activity read and then a lease across tenants: the queue had no tenants", seen)
	}
}

func TestDispatcherFailedFetchLeavesTheRingAlone(t *testing.T) {
	// A fetch that failed delivered nothing because the store failed, not
	// because the tenants ran dry, so the planner must not be told they did. If
	// it were, the ring would be empty and the next fetch would read the
	// activity again.
	var ops *opsStore
	e := newDispEnv(t, DispatcherConfig{}, func(s store.Store) store.Store { ops = &opsStore{Store: s}; return ops })
	e.add("q", "a", 3)
	if ans, err := e.lease("q", 1, 0); err != nil || len(ans.Grants) != 1 {
		t.Fatalf("first Lease = %v, %v", ans, err)
	}
	ops.mu.Lock()
	ops.failLeases = 1
	ops.mu.Unlock()
	if _, err := e.lease("q", 1, 0); status.Code(err) != codes.Unavailable {
		t.Fatalf("Lease on a failing store = %v, want Unavailable", err)
	}
	if ans, err := e.lease("q", 1, 0); err != nil || len(ans.Grants) != 1 {
		t.Fatalf("third Lease = %v, %v", ans, err)
	}
	want := []string{"activity", "lease:a=1", "lease:a=1", "lease:a=1"}
	if got := ops.seen(); !slices.Equal(got, want) {
		t.Errorf("store calls = %v, want %v", got, want)
	}
}

func TestDispatcherDueTimeWakesWithoutLaterActivity(t *testing.T) {
	// The activity interval is an hour, so the only read that sees the
	// scheduled job is the one at arrival: only its due time can wake the
	// waiter.
	e := newDispEnv(t, DispatcherConfig{}, nil)
	const delay = 300 * time.Millisecond
	start := time.Now()
	e.addWith(job.Spec{Queue: "q", Tenant: "t", Payload: []byte("p"), Delay: delay}, 1)
	ans, err := e.lease("q", 1, 30*time.Second)
	if err != nil || len(ans.Grants) != 1 {
		t.Fatalf("Lease = %v, %v; want the delayed job", ans, err)
	}
	if elapsed := time.Since(start); elapsed < delay {
		t.Errorf("job delivered after %v, before its delay of %v", elapsed, delay)
	}
}

func TestDispatcherActivityFindsReadyJobForParkedWaiter(t *testing.T) {
	// No notification and no poll: the activity read that finds a tenant with
	// a ready job wakes the parked waiter.
	e := newDispEnv(t, DispatcherConfig{ActivityInterval: 50 * time.Millisecond}, nil)
	res := e.leaseAsync("q", 1, 30*time.Second)
	e.waitFetches("q", fetchEmpty, 1)
	e.add("q", "t", 1)
	wantGrants(t, recv(t, res, quickly), 1)
}

// holdStore passes Lease calls through until hold is called, and from then
// on keeps each one until release is called.
type holdStore struct {
	store.Store
	entered chan struct{}

	mu   sync.Mutex
	gate chan struct{} // nil while Lease calls pass
}

func newHoldStore(inner store.Store) *holdStore {
	return &holdStore{Store: inner, entered: make(chan struct{}, 64)}
}

func (h *holdStore) hold() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.gate = make(chan struct{})
}

func (h *holdStore) release() {
	h.mu.Lock()
	defer h.mu.Unlock()
	close(h.gate)
	h.gate = nil
}

func (h *holdStore) Lease(ctx context.Context, req store.LeaseRequest) ([]store.Grant, error) {
	h.mu.Lock()
	gate := h.gate
	h.mu.Unlock()
	if gate != nil {
		select {
		case h.entered <- struct{}{}:
		default:
		}
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return h.Store.Lease(ctx, req)
}

// seedRing leases one job, which fills the ring of the queue from the
// activity, and returns once the answer is in. The read that fills the ring
// raises a notification, which keeps the queue dirty until the next fetch
// starts, and a dirty queue chains after any fetch: the tests below would
// pass for that reason, and not for the one they are about.
func (e *dispEnv) seedRing(queue string) {
	e.t.Helper()
	ans, err := e.lease(queue, 1, 0)
	if err != nil || len(ans.Grants) != 1 {
		e.t.Fatalf("the seeding Lease = %v, %v; want one job", ans, err)
	}
}

// addFloodAndOne puts 200 ready jobs of tenant a on the queue, then one of
// tenant b, and fills the ring with both. A batch of 16 from then on is
// planned as 8 jobs for each tenant and comes back with 9: tenant b ran dry,
// and tenant a still has 190 jobs.
func (e *dispEnv) addFloodAndOne(queue string) {
	e.t.Helper()
	e.add(queue, "a", 200)
	e.add(queue, "b", 1)
	e.seedRing(queue)
}

func TestDispatcherShortPlannedFetchChainsTheNextWaiter(t *testing.T) {
	// The second waiter is parked behind the only fetch slot, and nothing can
	// wake it but the fetch that chains: the poll and the activity interval
	// are an hour, and no notification arrives. The first fetch is short, but
	// only because tenant b ran dry.
	var hs *holdStore
	e := newDispEnv(t, DispatcherConfig{MaxInFlight: 1}, func(s store.Store) store.Store { hs = newHoldStore(s); return hs })
	e.addFloodAndOne("q")

	hs.hold()
	first := e.leaseAsync("q", 16, 30*time.Second)
	select {
	case <-hs.entered:
	case <-time.After(quickly):
		t.Fatalf("the fetch of the first waiter did not reach the store within %v", quickly)
	}
	second := e.leaseAsync("q", 16, 30*time.Second)
	e.waitWaiters("q", 2)
	hs.release()

	var fromB int
	for _, g := range wantGrants(t, recv(t, first, quickly), 9) {
		if g.Job.Tenant == "b" {
			fromB++
		}
	}
	if fromB != 1 {
		t.Fatalf("the first batch holds %d jobs of tenant b, want its only job", fromB)
	}
	for _, g := range wantGrants(t, recv(t, second, quickly), 16) {
		if g.Job.Tenant != "a" {
			t.Errorf("the second batch holds a job of tenant %q, want only tenant a: b has no more", g.Job.Tenant)
		}
	}
}

func TestDispatcherShortPlannedFetchDoesNotMarkTheQueueEmpty(t *testing.T) {
	// A waiter that arrives after the short fetch must fetch for itself. If
	// the queue were taken for empty it would wait for a wakeup that never
	// comes: the poll and the activity interval are an hour.
	e := newDispEnv(t, DispatcherConfig{}, nil)
	e.addFloodAndOne("q")

	ans, err := e.lease("q", 16, 0)
	wantGrants(t, leaseResult{ans, err}, 9)
	res := e.leaseAsync("q", 16, 30*time.Second)
	wantGrants(t, recv(t, res, quickly), 16)
}

func TestDispatcherShortFetchAcrossTenantsStillMarksTheQueueEmpty(t *testing.T) {
	// A fetch that was not shared between tenants did look at all of them, so
	// a short answer is the end of the queue: no fetch chains, and a waiter
	// that arrives next parks without one. The poll and the activity interval
	// are an hour, so a fetch that did start would be one the dispatcher made
	// up.
	for _, tc := range []struct {
		name      string
		scheduler string
		jobs      int      // ready jobs of tenant t
		want      []string // the store calls, and no more
	}{
		{"fifo, fewer jobs than asked", "fifo", 3, []string{"lease:*=8"}},
		{"drr, an empty ring and no jobs", "drr", 0, []string{"activity", "lease:*=8"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var ops *opsStore
			e := newDispEnv(t, DispatcherConfig{Scheduler: tc.scheduler},
				func(s store.Store) store.Store { ops = &opsStore{Store: s}; return ops })
			if tc.jobs > 0 {
				e.add("q", "t", tc.jobs)
			}
			ans, err := e.lease("q", 8, 0)
			if err != nil || len(ans.Grants) != tc.jobs {
				t.Fatalf("first Lease = %d jobs, %v; want %d", len(ans.Grants), err, tc.jobs)
			}
			res := e.leaseAsync("q", 8, 30*time.Second)
			e.waitWaiters("q", 1)
			select {
			case r := <-res:
				t.Fatalf("a waiter on a queue known to be empty was answered: %+v, %v", r.ans, r.err)
			case <-time.After(100 * time.Millisecond):
			}
			if got := ops.seen(); !slices.Equal(got, tc.want) {
				// A dispatcher that chains here fetches in a loop, so the list is cut.
				t.Errorf("store calls = %v of %d, want %v", got[:min(len(got), len(tc.want)+4)], len(got), tc.want)
			}
		})
	}
}

func TestDispatcherQueueLabelsStayWithinTheCap(t *testing.T) {
	// More queues than the cap, each leased once through the dispatcher: the
	// series of its fetches, leases and waiters must stop at the cap, and the
	// late queues must share "other".
	e := newDispEnv(t, DispatcherConfig{}, nil)
	const late = 50
	for i := range maxQueueLabels + late {
		queue := fmt.Sprintf("q%03d", i)
		e.add(queue, "t", 1)
		if ans, err := e.lease(queue, 1, 0); err != nil || len(ans.Grants) != 1 {
			t.Fatalf("Lease on %s = %v, %v", queue, ans, err)
		}
	}

	wantQueueLabelsWithinTheCap(t, e.reg)
	values := queueLabelValues(t, e.reg)
	for _, name := range []string{"jobqueue_jobs_leased_total", "jobqueue_dispatch_fetches_total", "jobqueue_waiters"} {
		if n := len(values[name]); n != maxQueueLabels+1 {
			t.Errorf("%s has %d queue labels, want %d: the first %d queues and %q", name, n, maxQueueLabels+1, maxQueueLabels, otherQueueLabel)
		}
	}
	other := map[string]string{"queue": otherQueueLabel}
	if got := e.metric("jobqueue_jobs_leased_total", other); got != late {
		t.Errorf("leased{queue=other} = %v, want %d", got, late)
	}
	if got := e.metric("jobqueue_dispatch_fetches_total", map[string]string{"queue": otherQueueLabel, "result": fetchJobs}); got != late {
		t.Errorf("fetches{queue=other,result=jobs} = %v, want %d", got, late)
	}
	if got := e.metric("jobqueue_waiters", other); got != 0 {
		t.Errorf("waiters{queue=other} = %v after every call returned, want 0: the shared gauge must count up and down", got)
	}
}

// bareDispatcher is a dispatcher whose loop is not running, so that a test can
// call the pieces the loop owns from its own goroutine.
func bareDispatcher(t *testing.T) (*Dispatcher, *memstore.Store, *prometheus.Registry) {
	t.Helper()
	mem := memstore.New(memstore.Options{Clock: time.Now})
	reg := prometheus.NewRegistry()
	d := NewDispatcher(mem, DispatcherConfig{Scheduler: "drr"}, NewMetrics(reg), slog.New(slog.NewTextHandler(&syncBuffer{}, nil)))
	return d, mem, reg
}

// leasedJob enqueues a job and leases it straight from the store, as a fetch
// of the dispatcher would, and returns its id and the grant.
func leasedJob(t *testing.T, mem *memstore.Store) (int64, []store.Grant) {
	t.Helper()
	res, err := mem.Enqueue(t.Context(), []job.Spec{{Queue: "q", Tenant: "t", Payload: []byte("p")}})
	if err != nil {
		t.Fatal(err)
	}
	grants, err := mem.Lease(t.Context(), store.LeaseRequest{
		Queue: "q", Takes: []store.TenantTake{{Limit: 1}}, TTL: time.Minute, Worker: "w",
	})
	if err != nil || len(grants) != 1 {
		t.Fatalf("Lease = %v, %v", grants, err)
	}
	if j := grants[0].Job; j.State != job.StateLeased || j.Attempts != 1 {
		t.Fatalf("leased job is %v with %d attempts, want leased with 1", j.State, j.Attempts)
	}
	return res[0].ID, grants
}

func wantReleased(t *testing.T, mem *memstore.Store, id int64) {
	t.Helper()
	j, err := mem.Get(t.Context(), id, "")
	if err != nil {
		t.Fatal(err)
	}
	if j.State != job.StateReady || j.Attempts != 0 {
		t.Errorf("job is %v with %d attempts, want ready with 0: the release must refund the attempt", j.State, j.Attempts)
	}
}

func wantStillLeased(t *testing.T, mem *memstore.Store, id int64) {
	t.Helper()
	j, err := mem.Get(t.Context(), id, "")
	if err != nil {
		t.Fatal(err)
	}
	if j.State != job.StateLeased {
		t.Errorf("job is %v, want it still leased: nothing should have released it", j.State)
	}
}

func TestFinishReleasesJobsForACallerWhoseContextEnded(t *testing.T) {
	// The answer reached the caller's goroutine at the moment its context
	// ended, so the client is gone and nobody will run the jobs.
	d, mem, reg := bareDispatcher(t)
	id, grants := leasedJob(t, mem)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	ans, err := d.finish(ctx, outcome{ans: LeaseAnswer{Grants: grants}})
	if status.Code(err) != codes.Canceled || len(ans.Grants) != 0 {
		t.Fatalf("finish = %v, %v; want no jobs and Canceled", ans, err)
	}
	wantReleased(t, mem, id)
	if got := metricValue(t, reg, "jobqueue_orphan_releases_total", nil); got != 1 {
		t.Errorf("orphan_releases_total = %v, want 1", got)
	}
}

func TestFinishKeepsJobsForACallerWhoIsStillThere(t *testing.T) {
	d, mem, reg := bareDispatcher(t)
	id, grants := leasedJob(t, mem)

	ans, err := d.finish(t.Context(), outcome{ans: LeaseAnswer{Grants: grants}})
	if err != nil || len(ans.Grants) != 1 {
		t.Fatalf("finish = %v, %v; want the job", ans, err)
	}
	wantStillLeased(t, mem, id)
	if got := metricValue(t, reg, "jobqueue_orphan_releases_total", nil); got != 0 {
		t.Errorf("orphan_releases_total = %v, want 0", got)
	}

	// An error of the loop passes through untouched, and releases nothing.
	boom := status.Error(codes.Unavailable, "store unavailable")
	if _, err := d.finish(t.Context(), outcome{err: boom}); !errors.Is(err, boom) {
		t.Errorf("finish of an error = %v, want it unchanged", err)
	}
}

func TestAnswerReleasesJobsForAnAbandonedWaiter(t *testing.T) {
	// The caller gave up first, so the loop must not answer: the jobs fetched
	// for the waiter go back to the store. The loop is not running, so the
	// test may call answer and read what it owns.
	d, mem, reg := bareDispatcher(t)
	id, grants := leasedJob(t, mem)
	w := &waiter{id: 1, call: LeaseCall{Queue: "q"}, answer: make(chan outcome, 1)}
	w.state.Store(waiterAbandoned)

	d.answer(w, outcome{ans: LeaseAnswer{Grants: grants}})

	select {
	case out := <-w.answer:
		t.Fatalf("an abandoned waiter was answered with %+v", out)
	default:
	}
	waitFor(t, "the orphaned job to be released", quickly, func() bool {
		return metricValue(t, reg, "jobqueue_orphan_releases_total", nil) == 1
	})
	wantReleased(t, mem, id)
	if d.pending != 1 {
		t.Errorf("pending = %d, want 1: the loop waits for the release it started", d.pending)
	}
	select {
	case ev := <-d.events:
		if _, ok := ev.(evSettled); !ok {
			t.Errorf("the release reported %T, want evSettled", ev)
		}
	case <-time.After(quickly):
		t.Error("the release did not report back to the loop")
	}
}

func TestAnswerHandsAWaitingWaiterItsAnswer(t *testing.T) {
	d, mem, reg := bareDispatcher(t)
	id, grants := leasedJob(t, mem)
	w := &waiter{id: 1, call: LeaseCall{Queue: "q"}, answer: make(chan outcome, 1)}

	d.answer(w, outcome{ans: LeaseAnswer{Grants: grants}})

	select {
	case out := <-w.answer:
		if out.err != nil || len(out.ans.Grants) != 1 {
			t.Errorf("answer = %+v, want the job", out)
		}
	default:
		t.Fatal("a waiting waiter was not answered")
	}
	if got := w.state.Load(); got != waiterAnswered {
		t.Errorf("waiter state = %d, want waiterAnswered", got)
	}
	wantStillLeased(t, mem, id)
	if d.pending != 0 {
		t.Errorf("pending = %d, want 0: nothing was released", d.pending)
	}
	if got := metricValue(t, reg, "jobqueue_orphan_releases_total", nil); got != 0 {
		t.Errorf("orphan_releases_total = %v, want 0", got)
	}
}
