package memstore_test

import (
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/memstore"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/storetest"
)

// start is the instant at which every harness clock starts.
var start = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// harness is a memstore whose clock is a variable that Advance moves.
type harness struct {
	t   *testing.T
	s   *memstore.Store
	now time.Time
}

func newHarness(t *testing.T, observer func(memstore.Change)) *harness {
	h := &harness{t: t, now: start}
	h.s = memstore.New(memstore.Options{Clock: func() time.Time { return h.now }, Observer: observer})
	return h
}

func (h *harness) Store() store.Store { return h.s }

func (h *harness) Now() time.Time { return h.now }

func (h *harness) Advance(d time.Duration) {
	if d%time.Microsecond != 0 {
		h.t.Fatalf("Advance(%v): not a whole number of microseconds", d)
	}
	h.now = h.now.Add(d)
}

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) storetest.Harness { return newHarness(t, nil) })
}

func spec(queue, tenant string) job.Spec {
	return job.Spec{Queue: queue, Tenant: tenant, Payload: []byte("payload")}
}

func TestObserverSeesEveryMutationInOrder(t *testing.T) {
	var changes []memstore.Change
	h := newHarness(t, func(c memstore.Change) { changes = append(changes, c) })
	ctx := t.Context()

	res, err := h.s.Enqueue(ctx, []job.Spec{spec("q1", "t1")})
	if err != nil {
		t.Fatal(err)
	}
	h.Advance(time.Second)
	grants, err := h.s.Lease(ctx, store.LeaseRequest{
		Queue: "q1", Takes: []store.TenantTake{{Tenant: "t1", Limit: 1}}, TTL: 30 * time.Second, Worker: "w1",
	})
	if err != nil || len(grants) != 1 {
		t.Fatalf("Lease = %v, %v", grants, err)
	}
	epoch := grants[0].Job.Epoch
	h.Advance(time.Second)
	ref := store.Ref{ID: res[0].ID, Queue: "q1", Token: epoch}
	if _, err := h.s.Complete(ctx, []store.CompleteRequest{{Ref: ref, Result: []byte("done")}}); err != nil {
		t.Fatal(err)
	}

	var ops []memstore.Op
	for _, c := range changes {
		ops = append(ops, c.Op)
	}
	if want := []memstore.Op{memstore.OpEnqueue, memstore.OpLease, memstore.OpComplete}; !slices.Equal(ops, want) {
		t.Fatalf("ops = %v, want %v", ops, want)
	}

	enq, lease, done := changes[0], changes[1], changes[2]
	if !enq.Inserted || enq.Deleted || enq.Before.ID != 0 || enq.After.ID != res[0].ID || enq.After.State != job.StateReady || !enq.Now.Equal(start) {
		t.Errorf("enqueue change = %+v, want an insertion of a ready job at %v", enq, start)
	}
	if lease.Token != 0 || lease.Before.State != job.StateReady || lease.After.State != job.StateLeased || lease.After.Epoch != epoch ||
		!lease.Now.Equal(start.Add(time.Second)) {
		t.Errorf("lease change = %+v, want ready to leased at %v with no token", lease, start.Add(time.Second))
	}
	if done.Token != epoch || done.Before.State != job.StateLeased || done.After.State != job.StateSucceeded ||
		done.Inserted || done.Deleted || !done.Now.Equal(start.Add(2*time.Second)) {
		t.Errorf("complete change = %+v, want token %d, leased to succeeded at %v", done, epoch, start.Add(2*time.Second))
	}
}

func TestObserverSkipsCallsThatChangeNothing(t *testing.T) {
	var changes []memstore.Change
	h := newHarness(t, func(c memstore.Change) { changes = append(changes, c) })
	ctx := t.Context()
	s := spec("q1", "t1")
	s.IdempotencyKey = "k"
	if _, err := h.s.Enqueue(ctx, []job.Spec{s}); err != nil {
		t.Fatal(err)
	}
	grants, err := h.s.Lease(ctx, store.LeaseRequest{
		Queue: "q1", Takes: []store.TenantTake{{Tenant: "t1", Limit: 1}}, TTL: 30 * time.Second, Worker: "w1",
	})
	if err != nil || len(grants) != 1 {
		t.Fatalf("Lease = %v, %v", grants, err)
	}
	before := len(changes)

	stale := store.Ref{ID: grants[0].Job.ID, Queue: "q1", Token: grants[0].Job.Epoch + 1}
	noErr := func(call string, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", call, err)
		}
	}
	_, err = h.s.Enqueue(ctx, []job.Spec{s}) // deduplicated
	noErr("Enqueue", err)
	_, err = h.s.Heartbeat(ctx, []store.Ref{stale}, time.Second)
	noErr("Heartbeat", err)
	_, err = h.s.Complete(ctx, []store.CompleteRequest{{Ref: stale}})
	noErr("Complete", err)
	_, err = h.s.Fail(ctx, []store.FailRequest{{Ref: stale}})
	noErr("Fail", err)
	_, err = h.s.Release(ctx, []store.Ref{stale})
	noErr("Release", err)
	_, err = h.s.Lease(ctx, store.LeaseRequest{Queue: "q1", Takes: []store.TenantTake{{Tenant: "t1", Limit: 1}}, TTL: time.Second, Worker: "w2"})
	noErr("Lease", err) // nothing is ready
	if len(changes) != before {
		t.Errorf("the observer saw %d changes from calls that changed no row: %+v", len(changes)-before, changes[before:])
	}
}

func TestObserverChangesAreCopies(t *testing.T) {
	var changes []memstore.Change
	h := newHarness(t, func(c memstore.Change) { changes = append(changes, c) })
	res, err := h.s.Enqueue(t.Context(), []job.Spec{spec("q1", "t1")})
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range [][]byte{changes[0].After.Payload, changes[0].Before.Payload} {
		for i := range b {
			b[i] = '!'
		}
	}
	got, err := h.s.Get(t.Context(), res[0].ID, "")
	if err != nil || string(got.Payload) != "payload" {
		t.Errorf("after the observer changed the bytes of a Change, Get = %q, %v; want the original payload", got.Payload, err)
	}
}

func TestNewWithoutClockPanics(t *testing.T) {
	defer func() {
		const want = "memstore.New: Options.Clock is required"
		if got := recover(); got != want {
			t.Errorf("panic = %v, want %q", got, want)
		}
	}()
	memstore.New(memstore.Options{})
}

// tickingClock returns a clock that moves one second on every reading, so
// that a call reading it twice would give two different instants, and a
// pointer to the number of readings.
func tickingClock() (func() time.Time, *int) {
	reads := 0
	return func() time.Time {
		reads++
		return start.Add(time.Duration(reads) * time.Second)
	}, &reads
}

func TestWriteCallsReadTheClockOnce(t *testing.T) {
	clock, reads := tickingClock()
	s := memstore.New(memstore.Options{Clock: clock})
	ctx := t.Context()

	// once runs fn and fails unless it read the clock exactly once.
	once := func(name string, fn func()) {
		t.Helper()
		before := *reads
		fn()
		if got := *reads - before; got != 1 {
			t.Errorf("%s read the clock %d times, want once per call", name, got)
		}
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	allEqual := func(name string, times []time.Time) {
		t.Helper()
		for _, x := range times[1:] {
			if !x.Equal(times[0]) {
				t.Errorf("%s: rows were stamped with different times %v", name, times)
				return
			}
		}
	}
	rows := func() []job.Job {
		t.Helper()
		before := *reads
		out, err := s.Snapshot(ctx)
		must(err)
		if *reads != before {
			t.Error("Snapshot read the clock")
		}
		return out
	}
	times := func(f func(job.Job) time.Time, ids ...int) []time.Time {
		var out []time.Time
		all := rows()
		for _, id := range ids {
			out = append(out, f(all[id]))
		}
		return out
	}

	var specs []job.Spec
	for range 6 {
		specs = append(specs, spec("q1", "t1"))
	}
	once("Enqueue", func() { _, err := s.Enqueue(ctx, specs); must(err) })
	allEqual("Enqueue CreatedAt", times(func(j job.Job) time.Time { return j.CreatedAt }, 0, 1, 2, 3, 4, 5))

	var grants []store.Grant
	once("Lease", func() {
		var err error
		grants, err = s.Lease(ctx, store.LeaseRequest{Queue: "q1", Takes: []store.TenantTake{{Tenant: "t1", Limit: 6}}, TTL: time.Minute, Worker: "w1"})
		must(err)
	})
	if len(grants) != 6 {
		t.Fatalf("leased %d jobs, want 6", len(grants))
	}
	allEqual("Lease LeaseExpiresAt", times(func(j job.Job) time.Time { return j.LeaseExpiresAt }, 0, 1, 2, 3, 4, 5))
	ref := func(i int) store.Ref { return store.Ref{ID: grants[i].Job.ID, Queue: "q1", Token: grants[i].Job.Epoch} }

	once("Heartbeat", func() { _, err := s.Heartbeat(ctx, []store.Ref{ref(0), ref(1)}, time.Minute); must(err) })
	allEqual("Heartbeat LeaseExpiresAt", times(func(j job.Job) time.Time { return j.LeaseExpiresAt }, 0, 1))
	once("Complete", func() {
		_, err := s.Complete(ctx, []store.CompleteRequest{{Ref: ref(2)}, {Ref: ref(3)}})
		must(err)
	})
	allEqual("Complete FinishedAt", times(func(j job.Job) time.Time { return j.FinishedAt }, 2, 3))
	once("Fail", func() { _, err := s.Fail(ctx, []store.FailRequest{{Ref: ref(4)}, {Ref: ref(5)}}); must(err) })
	allEqual("Fail RunAt", times(func(j job.Job) time.Time { return j.RunAt }, 4, 5))

	once("Lease again", func() {
		var err error
		grants, err = s.Lease(ctx, store.LeaseRequest{Queue: "q1", Takes: []store.TenantTake{{Tenant: "t1", Limit: 2}}, TTL: time.Minute, Worker: "w1"})
		must(err)
	})
	if len(grants) != 2 {
		t.Fatalf("leased %d jobs the second time, want 2", len(grants))
	}
	once("Release", func() {
		_, err := s.Release(ctx, []store.Ref{
			{ID: grants[0].Job.ID, Queue: "q1", Token: grants[0].Job.Epoch},
			{ID: grants[1].Job.ID, Queue: "q1", Token: grants[1].Job.Epoch},
		})
		must(err)
	})
	allEqual("Release RunAt", times(func(j job.Job) time.Time { return j.RunAt }, 4, 5))

	before := *reads
	if _, err := s.Get(ctx, 1, ""); err != nil {
		t.Fatal(err)
	}
	if *reads != before {
		t.Error("Get read the clock")
	}
}

func TestObserverSeesMaintenanceMutations(t *testing.T) {
	var changes []memstore.Change
	h := newHarness(t, func(c memstore.Change) { changes = append(changes, c) })
	s, ctx := h.s, t.Context()
	noErr := func(call string, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", call, err)
		}
	}
	lease := func(limit int) []store.Grant {
		t.Helper()
		g, err := s.Lease(ctx, store.LeaseRequest{Queue: "q1", Takes: []store.TenantTake{{Tenant: "t1", Limit: limit}}, TTL: 30 * time.Second, Worker: "w1"})
		noErr("Lease", err)
		return g
	}
	_, err := s.Enqueue(ctx, []job.Spec{spec("q1", "t1"), spec("q1", "t1"), spec("q1", "t1"), spec("q1", "t1")})
	noErr("Enqueue", err)
	grants := lease(3)
	_, err = s.Cancel(ctx, 4, "")
	noErr("Cancel", err)
	h.Advance(31 * time.Second)
	n, err := s.Reap(ctx, 2, func(int32) time.Duration { return 0 })
	noErr("Reap", err)
	if n != 2 {
		t.Fatalf("Reap = %d, want 2", n)
	}
	_, err = s.Fail(ctx, []store.FailRequest{{Ref: store.Ref{ID: 3, Queue: "q1", Token: grants[2].Job.Epoch}, Permanent: true}})
	noErr("Fail", err)
	_, err = s.Requeue(ctx, 3)
	noErr("Requeue", err)
	h.Advance(25 * time.Hour)
	n, err = s.Prune(ctx, store.Retention{Done: 24 * time.Hour, Dead: 336 * time.Hour}, 10)
	noErr("Prune", err)
	if n != 1 {
		t.Fatalf("Prune = %d, want 1", n)
	}
	_, err = s.Cancel(ctx, 1, "t1")
	noErr("Cancel", err)
	n, err = s.Purge(ctx, store.PurgeRequest{Queue: "q1", State: job.StateCancelled, FinishedBefore: h.Now().Add(time.Hour)})
	noErr("Purge", err)
	if n != 1 {
		t.Fatalf("Purge = %d, want 1 (job 4 was pruned)", n)
	}

	type step struct {
		op      memstore.Op
		id      int64
		deleted bool
	}
	want := []step{
		{memstore.OpEnqueue, 1, false}, {memstore.OpEnqueue, 2, false}, {memstore.OpEnqueue, 3, false}, {memstore.OpEnqueue, 4, false},
		{memstore.OpLease, 1, false}, {memstore.OpLease, 2, false}, {memstore.OpLease, 3, false},
		{memstore.OpCancel, 4, false},
		{memstore.OpReap, 1, false}, {memstore.OpReap, 2, false},
		{memstore.OpFail, 3, false}, {memstore.OpRequeue, 3, false},
		{memstore.OpPrune, 4, true},
		{memstore.OpCancel, 1, false},
		{memstore.OpPurge, 1, true},
	}
	var got []step
	for _, c := range changes {
		id := c.After.ID
		if c.Deleted {
			id = c.Before.ID
		}
		got = append(got, step{c.Op, id, c.Deleted})
	}
	if !slices.Equal(got, want) {
		t.Fatalf("changes = %v, want %v", got, want)
	}
	for _, c := range changes {
		switch {
		case c.Op == memstore.OpFail && c.Token != grants[2].Job.Epoch:
			t.Errorf("fail change carries token %d, want %d", c.Token, grants[2].Job.Epoch)
		case c.Op != memstore.OpFail && c.Token != 0:
			t.Errorf("%v change carries token %d, want 0", c.Op, c.Token)
		case c.Deleted && (c.Inserted || c.After.ID != 0):
			t.Errorf("%v change of a deleted row = %+v, want an empty After", c.Op, c)
		}
	}
	if c := changes[11]; c.Before.State != job.StateDead || c.After.State != job.StateReady {
		t.Errorf("requeue change goes %v to %v, want dead to ready", c.Before.State, c.After.State)
	}
}

func TestMaintenanceCallsReadTheClockOnce(t *testing.T) {
	clock, reads := tickingClock()
	var changes []memstore.Change
	s := memstore.New(memstore.Options{Clock: clock, Observer: func(c memstore.Change) { changes = append(changes, c) }})
	ctx := t.Context()

	// call runs fn and fails unless it read the clock the given number of
	// times and stamped every row it changed with one instant. It returns the
	// changes the call made.
	call := func(name string, wantReads int, fn func() error) []memstore.Change {
		t.Helper()
		r0, c0 := *reads, len(changes)
		if err := fn(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := *reads - r0; got != wantReads {
			t.Errorf("%s read the clock %d times, want %d", name, got, wantReads)
		}
		mine := changes[c0:]
		for _, c := range mine {
			if !c.Now.Equal(mine[0].Now) {
				t.Errorf("%s stamped its rows with different times: %v and %v", name, mine[0].Now, c.Now)
				break
			}
		}
		return mine
	}
	lease := func(tenant string, limit int) {
		t.Helper()
		call("Lease", 1, func() error {
			_, err := s.Lease(ctx, store.LeaseRequest{Queue: "q1", Takes: []store.TenantTake{{Tenant: tenant, Limit: limit}}, TTL: time.Second, Worker: "w1"})
			return err
		})
	}
	cancel := func(id int64) {
		t.Helper()
		call("Cancel", 1, func() error { _, err := s.Cancel(ctx, id, ""); return err })
	}
	rows := func(mine []memstore.Change, want int, name string) {
		t.Helper()
		if len(mine) != want {
			t.Fatalf("%s changed %d rows, want %d", name, len(mine), want)
		}
	}

	call("Enqueue", 1, func() error {
		_, err := s.Enqueue(ctx, []job.Spec{spec("q1", "t1"), spec("q1", "t1"), spec("q1", "t1"), spec("q1", "t1"), spec("q1", "t2")})
		return err
	})
	lease("t1", 4)
	// The clock moves a second per reading, so a 1s lease has just run out at
	// the next call.
	reaped := call("Reap", 1, func() error {
		n, err := s.Reap(ctx, 10, func(int32) time.Duration { return 0 })
		if err == nil && n != 4 {
			t.Fatalf("Reap = %d, want 4", n)
		}
		return err
	})
	rows(reaped, 4, "Reap")
	cancel(1)
	cancel(2)
	pruned := call("Prune", 1, func() error {
		n, err := s.Prune(ctx, store.Retention{}, 10)
		if err == nil && n != 2 {
			t.Fatalf("Prune = %d, want 2", n)
		}
		return err
	})
	rows(pruned, 2, "Prune")
	cancel(3)
	cancel(4)
	purged := call("Purge", 1, func() error {
		n, err := s.Purge(ctx, store.PurgeRequest{Queue: "q1", State: job.StateCancelled, FinishedBefore: start.Add(time.Hour)})
		if err == nil && n != 2 {
			t.Fatalf("Purge = %d, want 2", n)
		}
		return err
	})
	rows(purged, 2, "Purge")

	lease("t2", 1)
	call("Fail", 1, func() error {
		_, err := s.Fail(ctx, []store.FailRequest{{Ref: store.Ref{ID: 5, Queue: "q1", Token: 1}, Permanent: true}})
		return err
	})
	call("Requeue", 1, func() error { _, err := s.Requeue(ctx, 5); return err })
	call("Stats", 1, func() error { _, err := s.Stats(ctx, "q1"); return err })
	call("Activity", 1, func() error { _, err := s.Activity(ctx, "q1"); return err })
	call("ListDead", 0, func() error { _, err := s.ListDead(ctx, "q1", 0, 10); return err })
	call("Get", 0, func() error { _, err := s.Get(ctx, 5, ""); return err })
}

func TestConcurrentCallsAreSafe(t *testing.T) {
	const workers, rounds = 8, 40
	s := memstore.New(memstore.Options{Clock: func() time.Time { return start }})
	ctx := t.Context()
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tenant := "t" + strconv.Itoa(w)
			for range rounds {
				if _, err := s.Enqueue(ctx, []job.Spec{spec("q1", tenant)}); err != nil {
					t.Errorf("Enqueue: %v", err)
					return
				}
				grants, err := s.Lease(ctx, store.LeaseRequest{Queue: "q1", Takes: []store.TenantTake{{Tenant: tenant, Limit: 1}}, TTL: time.Minute, Worker: "w"})
				if err != nil || len(grants) != 1 {
					t.Errorf("Lease = %v, %v; want the job just enqueued", grants, err)
					return
				}
				ref := store.Ref{ID: grants[0].Job.ID, Queue: "q1", Token: grants[0].Job.Epoch}
				if out, err := s.Complete(ctx, []store.CompleteRequest{{Ref: ref}}); err != nil || out[0] != job.Applied {
					t.Errorf("Complete = %v, %v; want applied", out, err)
					return
				}
				if _, err := s.Snapshot(ctx); err != nil {
					t.Errorf("Snapshot: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	rows, err := s.Snapshot(ctx)
	if err != nil || len(rows) != workers*rounds {
		t.Fatalf("Snapshot = %d rows, %v; want %d", len(rows), err, workers*rounds)
	}
	for _, j := range rows {
		if j.State != job.StateSucceeded {
			t.Errorf("job %d is %v, want succeeded", j.ID, j.State)
		}
	}
}
