//go:build pg

package pgstore_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/pgtest"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
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
