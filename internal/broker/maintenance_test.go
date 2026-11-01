package broker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/memstore"
)

// maintEnv is a memstore on a clock the test moves, and a registry.
type maintEnv struct {
	t     *testing.T
	clock *fakeClock
	mem   *memstore.Store
	reg   *prometheus.Registry
	m     *Metrics
	log   *slog.Logger
}

func newMaintEnv(t *testing.T) *maintEnv {
	t.Helper()
	e := &maintEnv{t: t, clock: &fakeClock{now: t0}, reg: prometheus.NewRegistry(), log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	e.mem = memstore.New(memstore.Options{Clock: e.clock.Now})
	e.m = NewMetrics(e.reg)
	return e
}

// leaseJobs enqueues n jobs in the queue and leases them all with a TTL of
// 1s.
func (e *maintEnv) leaseJobs(queue string, n int) []store.Grant {
	e.t.Helper()
	specs := make([]job.Spec, n)
	for i := range specs {
		specs[i] = job.Spec{Queue: queue, Tenant: "t", Payload: []byte("p")}
	}
	if _, err := e.mem.Enqueue(e.t.Context(), specs); err != nil {
		e.t.Fatal(err)
	}
	grants, err := e.mem.Lease(e.t.Context(), store.LeaseRequest{
		Queue: queue, Takes: []store.TenantTake{{Limit: n}}, TTL: time.Second, Worker: "w",
	})
	if err != nil || len(grants) != n {
		e.t.Fatalf("Lease = %d grants, %v; want %d", len(grants), err, n)
	}
	return grants
}

// start runs fn until the test ends and waits for it to return.
func (e *maintEnv) start(fn func(ctx context.Context)) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); fn(ctx) }()
	e.t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			e.t.Error("the loop did not return after its context ended")
		}
	})
}

func (e *maintEnv) counter(name string) float64 { return metricValue(e.t, e.reg, name, nil) }

func TestReaperReapsExpired(t *testing.T) {
	e := newMaintEnv(t)
	grants := e.leaseJobs("q", 1)
	e.clock.Advance(2 * time.Second) // the lease of 1s has run out
	e.start(func(ctx context.Context) { RunReaper(ctx, e.mem, 10*time.Millisecond, 100, 1, e.m, e.log) })

	waitFor(t, "the reaper to count the job", quickly, func() bool { return e.counter("jobqueue_jobs_reaped_total") == 1 })
	j, err := e.mem.Get(t.Context(), grants[0].Job.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if j.State != job.StateReady {
		t.Errorf("job is %v after the reaper ran, want ready", j.State)
	}
}

// reapSpy records when each Reap call ran and what it returned.
type reapSpy struct {
	store.Store
	mu    sync.Mutex
	times []time.Time
	got   []int
}

func (s *reapSpy) Reap(ctx context.Context, limit int, backoff store.BackoffFunc) (int, error) {
	n, err := s.Store.Reap(ctx, limit, backoff)
	s.mu.Lock()
	s.times = append(s.times, time.Now())
	s.got = append(s.got, n)
	s.mu.Unlock()
	return n, err
}

func (s *reapSpy) calls() (times []time.Time, got []int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.times...), append([]int(nil), s.got...)
}

func TestReaperRepeatsWhileFull(t *testing.T) {
	e := newMaintEnv(t)
	e.leaseJobs("q", 25)
	e.clock.Advance(2 * time.Second)
	spy := &reapSpy{Store: e.mem}
	const every = time.Second
	e.start(func(ctx context.Context) { RunReaper(ctx, spy, every, 10, 1, e.m, e.log) })

	waitFor(t, "all 25 jobs to be reaped", 10*time.Second, func() bool { return e.counter("jobqueue_jobs_reaped_total") == 25 })
	times, got := spy.calls()
	if len(got) < 3 || got[0] != 10 || got[1] != 10 || got[2] != 5 {
		t.Fatalf("Reap calls returned %v, want 10, 10, 5 first", got)
	}
	// All three belong to one tick: without the repeat they would be a tick
	// apart.
	if span := times[2].Sub(times[0]); span > every/2 {
		t.Errorf("the three calls spanned %v, want well within one tick of %v", span, every)
	}
}

func TestReaperPassesBackoffForTheAttemptCount(t *testing.T) {
	// The delay of a reaped job comes from the default backoff for its
	// attempts, which is at most a second after the first one.
	e := newMaintEnv(t)
	grants := e.leaseJobs("q", 1)
	e.clock.Advance(2 * time.Second)
	e.start(func(ctx context.Context) { RunReaper(ctx, e.mem, 10*time.Millisecond, 100, 7, e.m, e.log) })
	waitFor(t, "the reaper to count the job", quickly, func() bool { return e.counter("jobqueue_jobs_reaped_total") == 1 })
	j, err := e.mem.Get(t.Context(), grants[0].Job.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if delay := j.RunAt.Sub(e.clock.Now()); delay < 0 || delay > time.Second {
		t.Errorf("reaped job runs %v from now, want within [0, 1s]", delay)
	}
}

// callCounter counts the Reap and Prune calls that reach it, answers each with
// the given result, and records the deadline of the context of the last one.
type callCounter struct {
	store.Store
	n   int
	err error

	reaps, prunes atomic.Int64
	deadlines     chan time.Duration // time left on the context of each call, as it arrives
}

func newCallCounter(n int, err error) *callCounter {
	return &callCounter{n: n, err: err, deadlines: make(chan time.Duration, 64)}
}

func (c *callCounter) note(ctx context.Context) {
	left := time.Duration(-1) // no deadline
	if dl, ok := ctx.Deadline(); ok {
		left = time.Until(dl)
	}
	select {
	case c.deadlines <- left:
	default:
	}
}

func (c *callCounter) Reap(ctx context.Context, _ int, _ store.BackoffFunc) (int, error) {
	c.reaps.Add(1)
	c.note(ctx)
	return c.n, c.err
}

func (c *callCounter) Prune(ctx context.Context, _ store.Retention, _ int) (int, error) {
	c.prunes.Add(1)
	c.note(ctx)
	return c.n, c.err
}

// wantAtMostOnePerTick fails if the loop called the store more often than it
// ticked: once per tick, plus one for a tick that has just fired.
func wantAtMostOnePerTick(t *testing.T, name string, calls int64, since time.Time, every time.Duration) {
	t.Helper()
	if maxCalls := int64(time.Since(since)/every) + 1; calls > maxCalls {
		t.Errorf("%s made %d calls in %v with a tick every %v, want at most %d: it is spinning", name, calls, time.Since(since), every, maxCalls)
	}
}

func TestMaintenanceLoopsSurviveStoreErrors(t *testing.T) {
	e := newMaintEnv(t)
	logs := &syncBuffer{}
	log := slog.New(slog.NewTextHandler(logs, nil))
	st := newCallCounter(0, errors.New("boom"))
	const every = 50 * time.Millisecond
	started := time.Now()
	e.start(func(ctx context.Context) { RunReaper(ctx, st, every, 10, 1, e.m, log) })
	e.start(func(ctx context.Context) {
		RunPruner(ctx, st, every, 10, store.Retention{Done: time.Hour, Dead: time.Hour}, e.m, log)
	})
	// Both keep ticking after an error, and each call waits for its tick: an
	// error is not a reason to call again at once.
	waitFor(t, "repeated errors", quickly, func() bool {
		return st.reaps.Load() >= 3 && st.prunes.Load() >= 3
	})
	wantAtMostOnePerTick(t, "the reaper", st.reaps.Load(), started, every)
	wantAtMostOnePerTick(t, "the pruner", st.prunes.Load(), started, every)
	if s := logs.String(); !strings.Contains(s, "reap failed") || !strings.Contains(s, "prune failed") {
		t.Errorf("the errors were not logged: %s", s)
	}
}

// blockingStore holds every Reap and Prune call until its context ends, and
// says when one has begun.
type blockingStore struct {
	store.Store
	began chan struct{}
}

func (b *blockingStore) Reap(ctx context.Context, _ int, _ store.BackoffFunc) (int, error) {
	b.began <- struct{}{}
	<-ctx.Done()
	return 0, ctx.Err()
}

func (b *blockingStore) Prune(ctx context.Context, _ store.Retention, _ int) (int, error) {
	b.began <- struct{}{}
	<-ctx.Done()
	return 0, ctx.Err()
}

func TestMaintenanceLoopsDoNotLogTheEndOfTheirOwnContext(t *testing.T) {
	// A shutdown that interrupts a call makes it fail with the context's
	// error. That is not a failure of the store, and a clean shutdown must not
	// log one.
	e := newMaintEnv(t)
	logs := &syncBuffer{}
	log := slog.New(slog.NewTextHandler(logs, nil))
	st := &blockingStore{began: make(chan struct{}, 2)}
	ctx, cancel := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	wg.Go(func() { RunReaper(ctx, st, time.Millisecond, 10, 1, e.m, log) })
	wg.Go(func() {
		RunPruner(ctx, st, time.Millisecond, 10, store.Retention{Done: time.Hour, Dead: time.Hour}, e.m, log)
	})
	for range 2 {
		select {
		case <-st.began:
		case <-time.After(quickly):
			t.Fatal("a loop did not call the store")
		}
	}
	cancel()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the loops did not return after their context ended")
	}
	if s := logs.String(); s != "" {
		t.Errorf("the loops logged while shutting down: %s", s)
	}
}

func TestMaintenanceLoopsDoNotSpinOnABatchBelowOne(t *testing.T) {
	// A pass that found nothing never repeats, whatever the batch is, so a
	// batch of 0 (which Config.Validate rules out) cannot make the loop spin.
	e := newMaintEnv(t)
	st := newCallCounter(0, nil)
	const every = 50 * time.Millisecond
	started := time.Now()
	e.start(func(ctx context.Context) { RunReaper(ctx, st, every, 0, 1, e.m, e.log) })
	e.start(func(ctx context.Context) {
		RunPruner(ctx, st, every, 0, store.Retention{Done: time.Hour, Dead: time.Hour}, e.m, e.log)
	})
	waitFor(t, "a few ticks", quickly, func() bool { return st.reaps.Load() >= 3 && st.prunes.Load() >= 3 })
	wantAtMostOnePerTick(t, "the reaper", st.reaps.Load(), started, every)
	wantAtMostOnePerTick(t, "the pruner", st.prunes.Load(), started, every)
}

func TestMaintenanceCallsHaveATimeoutOfTheirOwn(t *testing.T) {
	// A call on a connection that hangs must end by itself, or it would stall
	// its loop for good.
	e := newMaintEnv(t)
	st := newCallCounter(0, nil)
	e.start(func(ctx context.Context) { RunReaper(ctx, st, 5*time.Millisecond, 10, 1, e.m, e.log) })
	e.start(func(ctx context.Context) {
		RunPruner(ctx, st, 5*time.Millisecond, 10, store.Retention{Done: time.Hour, Dead: time.Hour}, e.m, e.log)
	})
	// The reaper's and the pruner's first calls arrive in either order.
	for range 2 {
		select {
		case left := <-st.deadlines:
			if left < 8*time.Second || left > maintenanceCallTimeout {
				t.Errorf("a call has %v left on its context, want a deadline of about %v", left, maintenanceCallTimeout)
			}
		case <-time.After(quickly):
			t.Fatal("a loop did not call the store")
		}
	}
}

func TestMaintenanceLoopsRejectANonPositiveInterval(t *testing.T) {
	// A ticker cannot have one, and the panic says which call was wrong.
	retention := store.Retention{Done: time.Hour, Dead: time.Hour}
	for name, call := range map[string]func(){
		"RunReaper": func() {
			RunReaper(t.Context(), newCallCounter(0, nil), 0, 10, 1, NewMetrics(prometheus.NewRegistry()), slog.New(slog.NewTextHandler(io.Discard, nil)))
		},
		"RunPruner": func() {
			RunPruner(t.Context(), newCallCounter(0, nil), -time.Second, 10, retention, NewMetrics(prometheus.NewRegistry()), slog.New(slog.NewTextHandler(io.Discard, nil)))
		},
	} {
		func() {
			defer func() {
				msg, _ := recover().(string)
				if !strings.Contains(msg, name) || !strings.Contains(msg, "must be positive") {
					t.Errorf("%s with a non-positive interval panicked with %q, want a message naming the call", name, msg)
				}
			}()
			call()
		}()
	}
}

func TestPrunerHonorsRetention(t *testing.T) {
	e := newMaintEnv(t)
	ctx := t.Context()
	finish := func(permanent bool) int64 {
		e.t.Helper()
		g := e.leaseJobs("q", 1)[0].Job
		ref := store.Ref{ID: g.ID, Queue: "q", Token: g.Epoch}
		var err error
		if permanent {
			_, err = e.mem.Fail(ctx, []store.FailRequest{{Ref: ref, Permanent: true, Error: "x"}})
		} else {
			_, err = e.mem.Complete(ctx, []store.CompleteRequest{{Ref: ref}})
		}
		if err != nil {
			e.t.Fatal(err)
		}
		return g.ID
	}
	oldDone := finish(false)
	dead := finish(true)
	e.clock.Advance(30 * time.Minute)
	newDone := finish(false)
	e.clock.Advance(31 * time.Minute) // oldDone finished 61m ago, newDone 31m ago

	retention := store.Retention{Done: time.Hour, Dead: 3 * time.Hour}
	e.start(func(ctx context.Context) { RunPruner(ctx, e.mem, 10*time.Millisecond, 100, retention, e.m, e.log) })
	waitFor(t, "the pruner to count the job", quickly, func() bool { return e.counter("jobqueue_jobs_pruned_total") == 1 })

	if _, err := e.mem.Get(ctx, oldDone, ""); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the job finished 61m ago, over the 1h retention, was kept: %v", err)
	}
	for name, id := range map[string]int64{"the job finished 31m ago": newDone, "the dead job, whose retention is 3h": dead} {
		if _, err := e.mem.Get(ctx, id, ""); err != nil {
			t.Errorf("%s was pruned: %v", name, err)
		}
	}
}

// fakePinger answers Ping from a switch.
type fakePinger struct {
	mu  sync.Mutex
	err error
}

func (p *fakePinger) Ping(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func (p *fakePinger) set(err error) {
	p.mu.Lock()
	p.err = err
	p.mu.Unlock()
}

func TestPingerReportsToHealth(t *testing.T) {
	e := newMaintEnv(t)
	h := NewHealth()
	handler := h.Handler()
	p := &fakePinger{}
	e.start(func(ctx context.Context) { runPinger(ctx, p, h, 5*time.Millisecond, e.log) })

	readyz := func() (int, string) { return get(handler, "/readyz") }
	waitFor(t, "the first successful ping", quickly, func() bool { code, _ := readyz(); return code == 200 })

	p.set(errors.New("database is down"))
	waitFor(t, "the failure to show", quickly, func() bool { _, body := readyz(); return body == "database unavailable" })
	p.set(nil)
	waitFor(t, "the recovery to show", quickly, func() bool { code, _ := readyz(); return code == 200 })
}
