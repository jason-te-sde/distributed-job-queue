//go:build pg

package pgstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/pgtest"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/memstore"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/pgstore"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/storetest"
)

// harness is a pgstore.Store on a test database whose clock the test moves.
type harness struct {
	t    testing.TB
	pool *pgxpool.Pool
	s    *pgstore.Store
}

func (h *harness) Store() store.Store { return h.s }

func (h *harness) Now() time.Time {
	h.t.Helper()
	var now time.Time
	if err := h.pool.QueryRow(h.t.Context(), "SELECT jobqueue.now()").Scan(&now); err != nil {
		h.t.Fatalf("read the test clock: %v", err)
	}
	return now.UTC()
}

func (h *harness) Advance(d time.Duration) {
	h.t.Helper()
	if err := pgtest.AdvanceClock(h.t.Context(), h.pool, d); err != nil {
		h.t.Fatalf("advance the test clock: %v", err)
	}
}

// newHarness returns the factory the conformance suite wants: one database
// for the whole top-level test, emptied and set back to the epoch for each
// subtest.
func newHarness(t *testing.T) storetest.NewHarness {
	pool := pgtest.NewDB(t)
	s := pgstore.New(pool)
	return func(t *testing.T) storetest.Harness {
		if err := pgtest.Reset(t.Context(), pool); err != nil {
			t.Fatalf("reset the test database: %v", err)
		}
		return &harness{t: t, pool: pool, s: s}
	}
}

func spec(queue, tenant string) job.Spec {
	return job.Spec{Queue: queue, Tenant: tenant, Payload: []byte("payload")}
}

func TestPGConformanceEnqueue(t *testing.T) {
	storetest.RunEnqueue(t, newHarness(t))
}

func TestPGConformanceLease(t *testing.T) {
	storetest.RunLease(t, newHarness(t))
}

func TestPGConformanceAck(t *testing.T) {
	storetest.RunAck(t, newHarness(t))
}

func TestPGConformanceMaintenance(t *testing.T) {
	storetest.RunMaintenance(t, newHarness(t))
}

// listener is a connection that LISTENs on the ready channel.
type listener struct {
	t    *testing.T
	conn *pgx.Conn
}

func listen(t *testing.T, pool *pgxpool.Pool) *listener {
	t.Helper()
	conn, err := pgx.ConnectConfig(t.Context(), pool.Config().ConnConfig.Copy())
	if err != nil {
		t.Fatalf("connect the listener: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	if _, err := conn.Exec(t.Context(), "LISTEN "+pgstore.NotifyChannel); err != nil {
		t.Fatalf("LISTEN: %v", err)
	}
	return &listener{t: t, conn: conn}
}

// next waits up to d for a notification.
func (l *listener) next(d time.Duration) (*pgconn.Notification, bool) {
	l.t.Helper()
	ctx, cancel := context.WithTimeout(l.t.Context(), d)
	defer cancel()
	n, err := l.conn.WaitForNotification(ctx)
	if err != nil {
		return nil, false
	}
	return n, true
}

// wantPayloads fails unless the notifications that arrive are exactly the
// given payloads, in any order, and no others follow.
func (l *listener) wantPayloads(want ...string) {
	l.t.Helper()
	var got []string
	for range want {
		n, ok := l.next(2 * time.Second)
		if !ok {
			break
		}
		if n.Channel != pgstore.NotifyChannel {
			l.t.Errorf("notification on channel %q, want %q", n.Channel, pgstore.NotifyChannel)
		}
		got = append(got, n.Payload)
	}
	slices.Sort(got)
	want = slices.Sorted(slices.Values(want))
	if !slices.Equal(got, want) {
		l.t.Fatalf("notifications = %q, want %q", got, want)
	}
	if n, ok := l.next(300 * time.Millisecond); ok {
		l.t.Errorf("an extra notification arrived: %q", n.Payload)
	}
}

func TestPGEnqueueNotifies(t *testing.T) {
	pool := pgtest.NewDB(t)
	s := pgstore.New(pool)
	l := listen(t, pool)

	delayed := spec("q", "t")
	delayed.Delay = 5 * time.Second
	if _, err := s.Enqueue(t.Context(), []job.Spec{spec("q", "t"), delayed}); err != nil {
		t.Fatal(err)
	}
	l.wantPayloads("q\tt")
}

func TestPGEnqueueNotifiesOncePerQueueAndTenant(t *testing.T) {
	pool := pgtest.NewDB(t)
	s := pgstore.New(pool)
	l := listen(t, pool)

	// Three due jobs of one queue and tenant wake the listeners once; a
	// second tenant and a second queue each add one wake-up of their own.
	specs := []job.Spec{spec("q", "a"), spec("q", "a"), spec("q", "a"), spec("q", "b"), spec("r", "a")}
	if _, err := s.Enqueue(t.Context(), specs); err != nil {
		t.Fatal(err)
	}
	l.wantPayloads("q\ta", "q\tb", "r\ta")
}

func TestPGEnqueueOfOnlyScheduledOrDeduplicatedJobsDoesNotNotify(t *testing.T) {
	pool := pgtest.NewDB(t)
	s := pgstore.New(pool)
	l := listen(t, pool)

	keyed := spec("q", "t")
	keyed.IdempotencyKey = "k"
	if _, err := s.Enqueue(t.Context(), []job.Spec{keyed}); err != nil {
		t.Fatal(err)
	}
	l.wantPayloads("q\tt")

	// The same key creates no job, so nothing becomes ready; a job one
	// microsecond in the future is not ready either.
	later := spec("q", "t")
	later.Delay = time.Microsecond
	res, err := s.Enqueue(t.Context(), []job.Spec{keyed, later})
	if err != nil || !res[0].Deduplicated {
		t.Fatalf("Enqueue = %v, %v; want the first item deduplicated", res, err)
	}
	l.wantPayloads()
}

func TestPGEnqueueIsAtomic(t *testing.T) {
	pool := pgtest.NewDB(t)
	s := pgstore.New(pool)
	// A trigger that refuses one tenant stands in for any failure in the
	// middle of a batch.
	for _, stmt := range []string{
		`CREATE FUNCTION jobqueue_test.refuse() RETURNS trigger LANGUAGE plpgsql AS $$
		 BEGIN IF NEW.tenant = 'boom' THEN RAISE EXCEPTION 'refused'; END IF; RETURN NEW; END $$`,
		`CREATE TRIGGER refuse BEFORE INSERT ON jobqueue.jobs FOR EACH ROW EXECUTE FUNCTION jobqueue_test.refuse()`,
	} {
		if _, err := pool.Exec(t.Context(), stmt); err != nil {
			t.Fatal(err)
		}
	}
	_, err := s.Enqueue(t.Context(), []job.Spec{spec("q", "a"), spec("q", "b"), spec("q", "boom")})
	if err == nil {
		t.Fatal("Enqueue of a batch holding a refused item succeeded")
	}
	rows, err := s.Snapshot(t.Context())
	if err != nil || len(rows) != 0 {
		t.Errorf("after the failed batch Snapshot = %d rows, %v; want none: the batch is one transaction", len(rows), err)
	}
}

// installTickingClock replaces jobqueue.now() by a clock that moves one
// millisecond on every call, so that a statement which reads it twice sees two
// different instants.
func installTickingClock(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	for _, stmt := range []string{
		`CREATE SEQUENCE jobqueue_test.tick`,
		`CREATE OR REPLACE FUNCTION jobqueue.now() RETURNS timestamptz LANGUAGE sql VOLATILE AS
		 $$ SELECT timestamptz '2026-01-01 00:00:00+00' + nextval('jobqueue_test.tick') * interval '1 millisecond' $$`,
	} {
		if _, err := pool.Exec(t.Context(), stmt); err != nil {
			t.Fatal(err)
		}
	}
	var a, b time.Time
	if err := pool.QueryRow(t.Context(), "SELECT jobqueue.now(), jobqueue.now()").Scan(&a, &b); err != nil || !b.After(a) {
		t.Fatalf("test setup: the ticking clock read %v then %v, %v; want the second later", a, b, err)
	}
}

// Every statement reads jobqueue.now() once. With a clock that moves on every
// read, a statement that read it twice would disagree with itself: a job
// created at one instant and due at another, or a lease whose remaining time
// is not the TTL.
func TestPGEachStatementReadsTheClockOnce(t *testing.T) {
	pool := pgtest.NewDB(t)
	installTickingClock(t, pool)
	s := pgstore.New(pool)

	res, err := s.Enqueue(t.Context(), []job.Spec{spec("q", "t")})
	if err != nil {
		t.Fatal(err)
	}
	j, err := s.Get(t.Context(), res[0].ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if !j.CreatedAt.Equal(j.RunAt) {
		t.Errorf("enqueued job has CreatedAt %v and RunAt %v, want one instant: Enqueue read the clock twice", j.CreatedAt, j.RunAt)
	}

	const ttl = 30 * time.Second
	grants, err := s.Lease(t.Context(), store.LeaseRequest{
		Queue: "q", Takes: []store.TenantTake{{Tenant: "t", Limit: 1}}, TTL: ttl, Worker: "w",
	})
	if err != nil || len(grants) != 1 {
		t.Fatalf("Lease = %v, %v", grants, err)
	}
	if grants[0].Remaining != ttl {
		t.Errorf("Remaining = %v, want exactly %v: Lease read the clock twice", grants[0].Remaining, ttl)
	}

	ref := store.Ref{ID: res[0].ID, Queue: "q", Token: grants[0].Job.Epoch}
	beats, err := s.Heartbeat(t.Context(), []store.Ref{ref}, 45*time.Second)
	if err != nil || len(beats) != 1 || beats[0].Outcome != job.Applied {
		t.Fatalf("Heartbeat = %v, %v", beats, err)
	}
	if beats[0].Remaining != 45*time.Second {
		t.Errorf("Remaining of a heartbeat = %v, want exactly 45s: Heartbeat read the clock twice", beats[0].Remaining)
	}
}

// Tenants come back in byte order, whatever collation the database uses.
func TestPGTenantsAreOrderedByByte(t *testing.T) {
	pool := pgtest.NewDB(t)
	s := pgstore.New(pool)
	tenants := []string{"b", "B", "a-b", "ab", "a", "A", "x_y", "9"}
	var specs []job.Spec
	for _, tenant := range tenants {
		specs = append(specs, spec("q", tenant))
	}
	if _, err := s.Enqueue(t.Context(), specs); err != nil {
		t.Fatal(err)
	}
	want := slices.Sorted(slices.Values(tenants))

	act, err := s.Activity(t.Context(), "q")
	if err != nil || !slices.Equal(act.Tenants, want) {
		t.Errorf("Activity tenants = %q, %v; want %q", act.Tenants, err, want)
	}
	stats, err := s.Stats(t.Context(), "q")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, st := range stats {
		got = append(got, st.Tenant)
	}
	if !slices.Equal(got, want) {
		t.Errorf("Stats tenants = %q, want %q", got, want)
	}
}

// Two takes of one tenant are served as one take of their sum, the way the
// reference store serves them one after the other.
func TestPGLeaseMergesRepeatedTenantTakes(t *testing.T) {
	pool := pgtest.NewDB(t)
	s := pgstore.New(pool)
	var specs []job.Spec
	for range 10 {
		specs = append(specs, spec("q", "a"))
	}
	if _, err := s.Enqueue(t.Context(), specs); err != nil {
		t.Fatal(err)
	}
	grants, err := s.Lease(t.Context(), store.LeaseRequest{
		Queue: "q", Takes: []store.TenantTake{{Tenant: "a", Limit: 2}, {Tenant: "a", Limit: 3}}, TTL: time.Minute, Worker: "w",
	})
	if err != nil {
		t.Fatal(err)
	}
	var got []int64
	for _, g := range grants {
		got = append(got, g.Job.ID)
	}
	if want := []int64{1, 2, 3, 4, 5}; !slices.Equal(got, want) {
		t.Errorf("leased %v, want %v", got, want)
	}
}

// leaseAll enqueues n jobs of queue q and tenant t on every store and leases
// them all, and returns the refs of the leases, which are the same on every
// store because they start from empty ones with the same clock.
func leaseAll(t *testing.T, n int, stores ...store.Store) []store.Ref {
	t.Helper()
	var refs []store.Ref
	for k, s := range stores {
		specs := make([]job.Spec, n)
		for i := range specs {
			specs[i] = spec("q", "t")
		}
		if _, err := s.Enqueue(t.Context(), specs); err != nil {
			t.Fatal(err)
		}
		grants, err := s.Lease(t.Context(), store.LeaseRequest{
			Queue: "q", Takes: []store.TenantTake{{Tenant: "t", Limit: n}}, TTL: time.Minute, Worker: "w",
		})
		if err != nil || len(grants) != n {
			t.Fatalf("Lease on store %d = %d grants, %v; want %d", k, len(grants), err, n)
		}
		var got []store.Ref
		for _, g := range grants {
			got = append(got, store.Ref{ID: g.Job.ID, Queue: "q", Token: g.Job.Epoch})
		}
		if k > 0 && !slices.Equal(got, refs) {
			t.Fatalf("store %d leased %v, store 0 leased %v", k, got, refs)
		}
		refs = got
	}
	return refs
}

// A job that appears twice in one call is handled as the reference store
// handles it, one item after the other: the first item's effect is what the
// second finds.
func TestPGRepeatedItemsInOneCallMatchTheReferenceStore(t *testing.T) {
	pool := pgtest.NewDB(t)
	pg := pgstore.New(pool)
	mem := memstore.New(memstore.Options{Clock: func() time.Time { return pgtest.Epoch }})
	refs := leaseAll(t, 6, mem, pg)
	with := func(r store.Ref, token int64) store.Ref { r.Token = token; return r }

	calls := map[string]func(s store.Store) (any, error){
		// The first item of job 1 is stale and the next two are not.
		"heartbeat": func(s store.Store) (any, error) {
			return s.Heartbeat(t.Context(), []store.Ref{with(refs[0], 0), refs[0], refs[0]}, 20*time.Second)
		},
		// Job 2 completes once, and the first result is the one kept.
		"complete": func(s store.Store) (any, error) {
			return s.Complete(t.Context(), []store.CompleteRequest{
				{Ref: refs[1], Result: []byte("a")}, {Ref: refs[1], Result: []byte("b")}, {Ref: refs[2], Result: []byte("c")},
			})
		},
		// Job 4 fails once and is then no longer leased.
		"fail": func(s store.Store) (any, error) {
			return s.Fail(t.Context(), []store.FailRequest{
				{Ref: refs[3], Error: "one"}, {Ref: refs[3], Error: "two"}, {Ref: refs[3], Permanent: true, Error: "three"},
			})
		},
		"release": func(s store.Store) (any, error) {
			return s.Release(t.Context(), []store.Ref{refs[4], refs[4], with(refs[4], 9)})
		},
		// The stale item comes first, so the item after it is the one that applies.
		"fail, stale first": func(s store.Store) (any, error) {
			return s.Fail(t.Context(), []store.FailRequest{
				{Ref: with(refs[5], 9), Error: "stale"}, {Ref: refs[5], Permanent: true, Error: "real"},
			})
		},
	}
	for _, name := range slices.Sorted(func(yield func(string) bool) {
		for name := range calls {
			if !yield(name) {
				return
			}
		}
	}) {
		want, wantErr := calls[name](mem)
		got, gotErr := calls[name](pg)
		if wantErr != nil || gotErr != nil {
			t.Fatalf("%s: errors %v (reference), %v (pgstore)", name, wantErr, gotErr)
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("%s: pgstore returned %v, the reference store %v", name, got, want)
		}
	}

	memRows, err := mem.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	pgRows, err := pg.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(pgRows) != len(memRows) {
		t.Fatalf("pgstore holds %d rows, the reference store %d", len(pgRows), len(memRows))
	}
	for i := range memRows {
		if !job.Equal(pgRows[i], memRows[i]) {
			t.Errorf("job %d differs:\n  pgstore   %+v\n  reference %+v", memRows[i].ID, pgRows[i], memRows[i])
		}
	}
}

// What the model calls empty is NULL in the table: no lease, no key, no error
// and no result.
func TestPGEmptyValuesAreStoredAsNull(t *testing.T) {
	pool := pgtest.NewDB(t)
	s := pgstore.New(pool)
	refs := leaseAll(t, 2, s)

	// Job 1 is completed with an empty result, job 2 fails with an empty
	// message and is then leased again and released.
	if out, err := s.Complete(t.Context(), []store.CompleteRequest{{Ref: refs[0], Result: []byte{}}}); err != nil || out[0] != job.Applied {
		t.Fatalf("Complete = %v, %v", out, err)
	}
	if out, err := s.Fail(t.Context(), []store.FailRequest{{Ref: refs[1]}}); err != nil || out[0] != job.Applied {
		t.Fatalf("Fail = %v, %v", out, err)
	}

	var resultNull, keyNull bool
	if err := pool.QueryRow(t.Context(), "SELECT result IS NULL, idempotency_key IS NULL FROM jobqueue.jobs WHERE id = 1").Scan(&resultNull, &keyNull); err != nil {
		t.Fatal(err)
	}
	if !resultNull || !keyNull {
		t.Errorf("job 1: result NULL = %t, idempotency_key NULL = %t, want both", resultNull, keyNull)
	}
	var leasedByNull, expiryNull, errNull bool
	if err := pool.QueryRow(t.Context(), "SELECT leased_by IS NULL, lease_expires_at IS NULL, last_error IS NULL FROM jobqueue.jobs WHERE id = 2").
		Scan(&leasedByNull, &expiryNull, &errNull); err != nil {
		t.Fatal(err)
	}
	if !leasedByNull || !expiryNull || !errNull {
		t.Errorf("job 2: leased_by NULL = %t, lease_expires_at NULL = %t, last_error NULL = %t, want all", leasedByNull, expiryNull, errNull)
	}
}

func TestPGReleaseAndRequeueNotify(t *testing.T) {
	pool := pgtest.NewDB(t)
	s := pgstore.New(pool)
	l := listen(t, pool)

	// Jobs 1 and 2 belong to tenant a, job 3 to tenant b. Enqueueing wakes
	// the listeners; that is not what is under test, so it is read away.
	if _, err := s.Enqueue(t.Context(), []job.Spec{spec("q", "a"), spec("q", "a"), spec("q", "b")}); err != nil {
		t.Fatal(err)
	}
	l.wantPayloads("q\ta", "q\tb")
	grants, err := s.Lease(t.Context(), store.LeaseRequest{
		Queue: "q", Takes: []store.TenantTake{{Tenant: "", Limit: 3}}, TTL: time.Minute, Worker: "w",
	})
	if err != nil || len(grants) != 3 {
		t.Fatalf("Lease = %d grants, %v", len(grants), err)
	}
	refs := make([]store.Ref, len(grants))
	for i, g := range grants {
		refs[i] = store.Ref{ID: g.Job.ID, Queue: "q", Token: g.Job.Epoch}
	}

	// A release that changes nothing wakes nobody.
	stale := refs[0]
	stale.Token = 99
	if out, err := s.Release(t.Context(), []store.Ref{stale}); err != nil || out[0] != job.Fenced {
		t.Fatalf("Release with a stale token = %v, %v", out, err)
	}
	l.wantPayloads()

	// Releasing three jobs wakes each of their tenants once.
	if out, err := s.Release(t.Context(), refs); err != nil || slices.Contains(out, job.Fenced) {
		t.Fatalf("Release = %v, %v", out, err)
	}
	l.wantPayloads("q\ta", "q\tb")

	// Requeueing a job that is not dead wakes nobody; requeueing a dead one
	// wakes its tenant.
	if out, err := s.Requeue(t.Context(), refs[2].ID); err != nil || out != job.WrongState {
		t.Fatalf("Requeue of a ready job = %v, %v", out, err)
	}
	l.wantPayloads()
	grants, err = s.Lease(t.Context(), store.LeaseRequest{
		Queue: "q", Takes: []store.TenantTake{{Tenant: "b", Limit: 1}}, TTL: time.Minute, Worker: "w",
	})
	if err != nil || len(grants) != 1 {
		t.Fatalf("Lease = %d grants, %v", len(grants), err)
	}
	dead := []store.FailRequest{{Ref: store.Ref{ID: grants[0].Job.ID, Queue: "q", Token: grants[0].Job.Epoch}, Permanent: true, Error: "no"}}
	if out, err := s.Fail(t.Context(), dead); err != nil || out[0] != job.Applied {
		t.Fatalf("Fail = %v, %v", out, err)
	}
	l.wantPayloads()
	if out, err := s.Requeue(t.Context(), grants[0].Job.ID); err != nil || out != job.Applied {
		t.Fatalf("Requeue of a dead job = %v, %v", out, err)
	}
	l.wantPayloads("q\tb")
}

// deadDSN is a DSN for a port that nothing listens on.
func deadDSN(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("postgres://127.0.0.1:%d/postgres?sslmode=disable&connect_timeout=2", port)
}

// everyCall runs each method of the store once with valid arguments and
// returns the error of each, by method name.
func everyCall(ctx context.Context, s *pgstore.Store) map[string]error {
	ref := store.Ref{ID: 1, Queue: "q", Token: 1}
	calls := map[string]func() error{
		"Enqueue": func() error { _, err := s.Enqueue(ctx, []job.Spec{spec("q", "t")}); return err },
		"Lease": func() error {
			_, err := s.Lease(ctx, store.LeaseRequest{Queue: "q", Takes: []store.TenantTake{{Tenant: "t", Limit: 1}}, TTL: time.Minute, Worker: "w"})
			return err
		},
		"Heartbeat": func() error { _, err := s.Heartbeat(ctx, []store.Ref{ref}, time.Minute); return err },
		"Complete":  func() error { _, err := s.Complete(ctx, []store.CompleteRequest{{Ref: ref}}); return err },
		"Fail":      func() error { _, err := s.Fail(ctx, []store.FailRequest{{Ref: ref}}); return err },
		"Release":   func() error { _, err := s.Release(ctx, []store.Ref{ref}); return err },
		"Cancel":    func() error { _, err := s.Cancel(ctx, 1, ""); return err },
		"Requeue":   func() error { _, err := s.Requeue(ctx, 1); return err },
		"Reap":      func() error { _, err := s.Reap(ctx, 1, func(int32) time.Duration { return 0 }); return err },
		"Prune":     func() error { _, err := s.Prune(ctx, store.Retention{}, 1); return err },
		"Purge": func() error {
			_, err := s.Purge(ctx, store.PurgeRequest{Queue: "q", State: job.StateSucceeded, FinishedBefore: pgtest.Epoch})
			return err
		},
		"Get":      func() error { _, err := s.Get(ctx, 1, ""); return err },
		"ListDead": func() error { _, err := s.ListDead(ctx, "q", 0, 1); return err },
		"Stats":    func() error { _, err := s.Stats(ctx, "q"); return err },
		"Activity": func() error { _, err := s.Activity(ctx, "q"); return err },
		"Snapshot": func() error { _, err := s.Snapshot(ctx); return err },
	}
	out := make(map[string]error, len(calls))
	for name, call := range calls {
		out[name] = call()
	}
	return out
}

func TestPGClassifiesConnectionErrors(t *testing.T) {
	t.Run("a server that is not there", func(t *testing.T) {
		pool, err := pgxpool.New(t.Context(), deadDSN(t))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		for name, err := range everyCall(t.Context(), pgstore.New(pool)) {
			if !errors.Is(err, store.ErrUnavailable) {
				t.Errorf("%s: error = %v, want one wrapping store.ErrUnavailable", name, err)
			}
			if errors.Is(err, job.ErrInvalid) {
				t.Errorf("%s: error = %v, which must not look like a caller's mistake", name, err)
			}
		}
	})

	t.Run("a pool that was closed", func(t *testing.T) {
		pool := pgtest.NewDB(t)
		s := pgstore.New(pool)
		pool.Close()
		for name, err := range everyCall(t.Context(), s) {
			if !errors.Is(err, store.ErrUnavailable) {
				t.Errorf("%s: error = %v, want one wrapping store.ErrUnavailable", name, err)
			}
		}
	})

	t.Run("a cancelled context", func(t *testing.T) {
		s := pgstore.New(pgtest.NewDB(t))
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		for name, err := range everyCall(ctx, s) {
			if !errors.Is(err, context.Canceled) {
				t.Errorf("%s: error = %v, want one wrapping context.Canceled", name, err)
			}
			if errors.Is(err, store.ErrUnavailable) {
				t.Errorf("%s: error = %v, which must not wrap store.ErrUnavailable: the caller gave up, the store did not fail", name, err)
			}
		}
	})
}

func TestPGOpen(t *testing.T) {
	dsn := pgtest.DSN(t)

	pool, err := pgstore.Open(t.Context(), dsn, 3)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if got := pool.Config().MaxConns; got != 3 {
		t.Errorf("MaxConns = %d, want 3", got)
	}
	var app string
	if err := pool.QueryRow(t.Context(), "SHOW application_name").Scan(&app); err != nil || app != "jobqueue" {
		t.Errorf("application_name = %q, %v; want jobqueue", app, err)
	}

	def, err := pgstore.Open(t.Context(), dsn, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(def.Close)
	if got := def.Config().MaxConns; got < 1 {
		t.Errorf("MaxConns for a limit of 0 = %d, want pgxpool's default", got)
	}

	if _, err := pgstore.Open(t.Context(), deadDSN(t), 2); !errors.Is(err, store.ErrUnavailable) {
		t.Errorf("Open on a port nothing listens on: error = %v, want one wrapping store.ErrUnavailable", err)
	}
	if _, err := pgstore.Open(t.Context(), "not a url", 2); err == nil || errors.Is(err, store.ErrUnavailable) {
		t.Errorf("Open on a malformed URL: error = %v, want a plain error", err)
	}
}

// The reference store takes any instant as the bound of a purge, however far
// from the present; so does pgstore, although the database cannot hold some of
// them.
func TestPGPurgeAcceptsAnyInstant(t *testing.T) {
	pool := pgtest.NewDB(t)
	s := pgstore.New(pool)
	refs := leaseAll(t, 2, s)
	if out, err := s.Complete(t.Context(), []store.CompleteRequest{{Ref: refs[0]}, {Ref: refs[1]}}); err != nil || slices.Contains(out, job.Fenced) {
		t.Fatalf("Complete = %v, %v", out, err)
	}
	purge := func(before time.Time) int {
		t.Helper()
		n, err := s.Purge(t.Context(), store.PurgeRequest{Queue: "q", State: job.StateSucceeded, FinishedBefore: before})
		if err != nil {
			t.Fatalf("Purge(before %v): %v", before, err)
		}
		return n
	}
	for _, before := range []time.Time{{}, time.Unix(-1<<60, 0), pgtest.Epoch} {
		if n := purge(before); n != 0 {
			t.Errorf("Purge of jobs that finished before %v deleted %d, want 0", before, n)
		}
	}
	if n := purge(time.Unix(1<<60, 0)); n != 2 {
		t.Errorf("Purge of jobs that finished before the end of time deleted %d, want 2", n)
	}
}

// planNode is the part of a node of EXPLAIN's JSON output that the test reads.
type planNode struct {
	NodeType       string     `json:"Node Type"`
	Relation       string     `json:"Relation Name"`
	ActualRows     float64    `json:"Actual Rows"`
	ActualLoops    float64    `json:"Actual Loops"`
	RemovedFilter  float64    `json:"Rows Removed by Filter"`
	RemovedJoin    float64    `json:"Rows Removed by Join Filter"`
	RemovedRecheck float64    `json:"Rows Removed by Index Recheck"`
	Plans          []planNode `json:"Plans"`
}

// rowsRead is the number of rows of the jobs table that the plan below n read,
// whether it kept them or threw them away.
func (n planNode) rowsRead() float64 {
	total := 0.0
	if n.Relation == "jobs" {
		total = n.ActualRows*n.ActualLoops + n.RemovedFilter + n.RemovedJoin + n.RemovedRecheck
	}
	for _, c := range n.Plans {
		total += c.rowsRead()
	}
	return total
}

// The statements that run every second or on every poll must read the rows
// they use and no others, however many rows the table holds. Each of them has a
// formulation that reads the whole table when no job qualifies, which no
// functional test sees, because the tables they use are small: a lease that
// compares run_at with a joined clock, a reap that asks the primary key for the
// lowest expired ids, a prune that does the same. This test fills the table
// with rows that none of them may touch and bounds the rows each one reads;
// Activity gets a queue with a backlog of due jobs as well, since it looks past
// them for the next scheduled one.
func TestPGHotStatementsDoNotScanTheTable(t *testing.T) {
	pool := pgtest.NewDB(t)
	// The test clock stands still at the epoch, so now is the epoch throughout.
	for _, stmt := range []string{
		// Ready, but scheduled for tomorrow or later: nothing is due.
		`INSERT INTO jobqueue.jobs (queue, tenant, state, payload, run_at, attempts, max_attempts, created_at)
		 SELECT 'q', 't' || (g % 10), 'ready', ''::bytea, jobqueue.now() + interval '1 day' + g * interval '1 second', 0, 25, jobqueue.now()
		 FROM generate_series(1, 10000) g`,
		// Finished an hour ago, younger than any retention used below.
		`INSERT INTO jobqueue.jobs (queue, tenant, state, payload, run_at, attempts, max_attempts, created_at, finished_at)
		 SELECT 'q', 't' || (g % 10), CASE WHEN g % 3 = 0 THEN 'dead' ELSE 'succeeded' END, ''::bytea,
		        jobqueue.now(), 1, 25, jobqueue.now(), jobqueue.now() - interval '1 hour'
		 FROM generate_series(1, 15000) g`,
		// Leased until tomorrow: no lease has run out.
		`INSERT INTO jobqueue.jobs (queue, tenant, state, payload, run_at, attempts, max_attempts, lease_epoch, lease_expires_at, leased_by, created_at)
		 SELECT 'q', 't' || (g % 10), 'leased', ''::bytea, jobqueue.now(), 1, 25, 1, jobqueue.now() + interval '1 day', 'w', jobqueue.now()
		 FROM generate_series(1, 5000) g`,
		`ANALYZE jobqueue.jobs`,
	} {
		if _, err := pool.Exec(t.Context(), stmt); err != nil {
			t.Fatal(err)
		}
	}

	// A backlog of due jobs in a second queue, and one job scheduled after it:
	// Activity must find the one scheduled job without reading the backlog.
	for _, stmt := range []string{
		`INSERT INTO jobqueue.jobs (queue, tenant, state, payload, run_at, attempts, max_attempts, created_at)
		 SELECT 'r', 't' || (g % 10), 'ready', ''::bytea, jobqueue.now() - interval '1 hour' + g * interval '1 millisecond', 0, 25, jobqueue.now()
		 FROM generate_series(1, 10000) g`,
		`INSERT INTO jobqueue.jobs (queue, tenant, state, payload, run_at, attempts, max_attempts, created_at)
		 VALUES ('r', 'late', 'ready', ''::bytea, jobqueue.now() + interval '1 minute', 0, 25, jobqueue.now())`,
		`ANALYZE jobqueue.jobs`,
	} {
		if _, err := pool.Exec(t.Context(), stmt); err != nil {
			t.Fatal(err)
		}
	}

	ttl := time.Minute.Microseconds()
	for name, c := range map[string]struct {
		sql  string
		args []any
	}{
		"Lease of one tenant after another": {pgstore.TenantLeaseSQL, []any{"q", []string{"t1", "t2", "t3"}, []int32{5, 5, 5}, ttl, "w"}},
		"Lease in FIFO order":               {pgstore.FIFOLeaseSQL, []any{"q", int32(10), ttl, "w"}},
		"Reap":                              {pgstore.ReapSelectSQL, []any{int64(100)}},
		"Prune":                             {pgstore.PruneSQL, []any{(24 * time.Hour).Microseconds(), (336 * time.Hour).Microseconds(), int64(100)}},
		"Activity of a queue with scheduled jobs": {pgstore.ActivitySQL, []any{"q"}},
		"Activity of a queue with a backlog":      {pgstore.ActivitySQL, []any{"r"}},
	} {
		// EXPLAIN ANALYZE runs the statement, so it runs in a transaction that
		// is rolled back.
		tx, err := pool.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		var raw []byte
		err = tx.QueryRow(t.Context(), "EXPLAIN (ANALYZE, FORMAT JSON) "+c.sql, c.args...).Scan(&raw)
		_ = tx.Rollback(t.Context())
		if err != nil {
			t.Fatalf("%s: EXPLAIN: %v", name, err)
		}
		var plans []struct{ Plan planNode }
		if err := json.Unmarshal(raw, &plans); err != nil || len(plans) != 1 {
			t.Fatalf("%s: plan %s: %v", name, raw, err)
		}
		// A few rows may be read: the tenants of Activity, the probes of a lateral
		// join. Anything near the size of the table is a scan.
		if n := plans[0].Plan.rowsRead(); n > 100 {
			t.Errorf("%s read %.0f rows of a table of 40000, only a few of which it needs; plan: %s", name, n, raw)
		}
	}
}
