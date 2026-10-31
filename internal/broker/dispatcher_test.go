package broker

import (
	"context"
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

	grants := wantGrants(t, recv(t, res, quickly), 1)
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
	if elapsed < wait || elapsed > quickly {
		t.Errorf("Lease returned after %v, want between %v and %v", elapsed, wait, quickly)
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
	e.add("q", "t", 1) // no Notify
	wantGrants(t, recv(t, res, quickly), 1)
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

	e.d.Drain()
	for i, res := range results {
		r := recv(t, res, quickly)
		if r.err != nil || len(r.ans.Grants) != 0 {
			t.Errorf("waiter %d got %v, %v; want an empty answer", i, r.ans, r.err)
		}
	}

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
	if elapsed < delay || elapsed > quickly {
		t.Errorf("job delivered after %v, want between %v and %v", elapsed, delay, quickly)
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
