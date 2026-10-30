package memstore_test

import (
	"slices"
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
