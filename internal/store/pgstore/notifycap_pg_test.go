//go:build pg

package pgstore_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/pgtest"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/pgstore"
)

const (
	// stallPoolSize is the pool of the store under test. The default bound on
	// notifying transactions for a pool of 8 is 6: a reserve of max(2, 8/4) = 2.
	stallPoolSize   = 8
	stallDefaultCap = 6
	// stallKey is the advisory lock that stalls the commits.
	stallKey = 5300
)

// connect opens a connection of the test's own, outside every pool under test.
func connect(t *testing.T, pool *pgxpool.Pool) *pgx.Conn {
	t.Helper()
	conn, err := pgx.ConnectConfig(t.Context(), pool.Config().ConnConfig.Copy())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.WithoutCancel(t.Context())) })
	return conn
}

// stalledEnqueues is a store whose notifying transactions are stuck at their
// commit, together with the callers that are stuck in them.
type stalledEnqueues struct {
	st      *pgstore.Store
	pool    *pgxpool.Pool
	results chan error
	callers int
	monitor *pgx.Conn
	holder  *pgx.Conn
}

// stallEnqueues starts callers Enqueue calls on a store with a pool of
// stallPoolSize connections and holds every one of them at its commit, until
// the returned value is released. A job of queue "q1" is ready for leasing
// before that. The transactions that insert jobs are the ones that notify.
//
// A transaction that has run NOTIFY waits at its commit for the lock that
// serializes notifying commits (locktype object, classid 1262), and that lock
// can be taken from SQL by nothing but such a commit, so the test cannot hold
// it. It holds the commits one step earlier instead, in a deferred constraint
// trigger that waits for an advisory lock that the test owns: the commit holds
// its connection and waits for something outside its control, which is all
// that the store can tell apart from the notify lock. Deferred triggers fire
// before the notify lock is taken, so the stalled transactions do not queue on
// it, but what the test is about is the connections they hold.
func stallEnqueues(t *testing.T, callers int, opts ...pgstore.Option) *stalledEnqueues {
	t.Helper()
	admin := pgtest.NewDB(t)
	cfg := admin.Config()
	cfg.MaxConns = stallPoolSize
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatalf("open the pool: %v", err)
	}
	t.Cleanup(pool.Close)
	st := pgstore.New(pool, opts...)

	if _, err := st.Enqueue(t.Context(), []job.Spec{
		{Queue: "q1", Tenant: "t1", Payload: []byte("leasable")}, {Queue: "q1", Tenant: "t1", Payload: []byte("leasable")}, {Queue: "q1", Tenant: "t1", Payload: []byte("leasable")},
	}); err != nil {
		t.Fatalf("Enqueue of the jobs to lease: %v", err)
	}
	for _, stmt := range []string{
		`CREATE FUNCTION stall_commit() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN PERFORM pg_advisory_xact_lock(` + strconv.Itoa(stallKey) + `); RETURN NULL; END $$`,
		`CREATE CONSTRAINT TRIGGER stall_commit AFTER INSERT ON jobqueue.jobs
			DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stall_commit()`,
	} {
		if _, err := admin.Exec(t.Context(), stmt); err != nil {
			t.Fatalf("install the stall: %v", err)
		}
	}
	s := &stalledEnqueues{st: st, pool: pool, results: make(chan error, callers), callers: callers,
		monitor: connect(t, admin), holder: connect(t, admin)}
	if _, err := s.holder.Exec(t.Context(), "SELECT pg_advisory_lock($1)", stallKey); err != nil {
		t.Fatalf("take the stall lock: %v", err)
	}
	t.Cleanup(func() { _, _ = s.holder.Exec(context.WithoutCancel(t.Context()), "SELECT pg_advisory_unlock_all()") })

	for range callers {
		go func() {
			_, err := st.Enqueue(t.Context(), []job.Spec{{Queue: "bulk", Tenant: "t1", Payload: []byte("x")}})
			s.results <- err
		}()
	}
	return s
}

// blocked counts the transactions that wait at their commit.
func (s *stalledEnqueues) blocked(t *testing.T) int {
	t.Helper()
	var n int
	if err := s.monitor.QueryRow(t.Context(),
		`SELECT count(*) FROM pg_stat_activity
		 WHERE datname = current_database() AND wait_event_type = 'Lock' AND wait_event = 'advisory'`).Scan(&n); err != nil {
		t.Fatalf("count the stalled transactions: %v", err)
	}
	return n
}

// saturate waits until at least n of the store's notifying transactions are
// stuck at their commit, and then a little longer. A store without a bound
// lets every caller that fits take a connection, so the test waits for the
// whole pool in that case; a store with a bound never gets there, and the short
// wait for it is the price.
func (s *stalledEnqueues) saturate(t *testing.T, n int) {
	t.Helper()
	waitUntil(t, "transactions blocked at their commit", 15*time.Second, func() bool { return s.blocked(t) >= n })
	deadline := time.Now().Add(500 * time.Millisecond)
	for s.pool.Stat().AcquiredConns() < stallPoolSize && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
}

// release lets the stalled commits through and checks that every caller then
// succeeds.
func (s *stalledEnqueues) release(t *testing.T) {
	t.Helper()
	if _, err := s.holder.Exec(t.Context(), "SELECT pg_advisory_unlock($1)", stallKey); err != nil {
		t.Fatalf("release the stall lock: %v", err)
	}
	for range s.callers {
		select {
		case err := <-s.results:
			if err != nil {
				t.Errorf("an Enqueue that waited out the stall failed: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("an Enqueue was still stuck 30s after the stall was released")
		}
	}
}

func waitUntil(t *testing.T, what string, within time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", within, what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestPGNotifyingTransactionsLeaveConnectionsForLeasing(t *testing.T) {
	// More Enqueue calls than the pool has connections are stuck at their
	// commit, as they are when the notify lock is held by a slow commit. The
	// calls that do not notify must still find a connection.
	s := stallEnqueues(t, 3*stallPoolSize)
	s.saturate(t, stallDefaultCap)

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	grants, err := s.st.Lease(ctx, store.LeaseRequest{
		Queue: "q1", Takes: []store.TenantTake{{Tenant: "t1", Limit: 1}}, TTL: 30 * time.Second, Worker: "w1"})
	if err != nil || len(grants) != 1 {
		t.Fatalf("Lease while the notifying transactions are stuck = %d grants, %v; want the job: it starves behind the connections that they hold", len(grants), err)
	}
	if _, err := s.st.Activity(ctx, "q1"); err != nil {
		t.Fatalf("Activity while the notifying transactions are stuck: %v", err)
	}
	if held := s.pool.Stat().AcquiredConns(); held > stallDefaultCap {
		t.Errorf("%d connections are held, want at most %d: the bound for notifying transactions in a pool of %d", held, stallDefaultCap, stallPoolSize)
	}
	s.release(t)

	var rows int
	if err := s.monitor.QueryRow(t.Context(), `SELECT count(*) FROM jobqueue.jobs WHERE queue = 'bulk'`).Scan(&rows); err != nil || rows != 3*stallPoolSize {
		t.Errorf("%d jobs of the queue the stalled callers wrote to, %v; want %d: every caller must get through once the stall ends", rows, err, 3*stallPoolSize)
	}
}

func TestPGEnqueueWaitingForItsTurnFailsWithTheContextError(t *testing.T) {
	// A call that waits for its turn behind the stuck transactions, and whose
	// context ends first, fails as a call whose deadline passed does: with the
	// context's error, which the broker answers as DeadlineExceeded or Canceled
	// and the SDK retries, and not as unavailable or as an internal error.
	s := stallEnqueues(t, 2*stallPoolSize)
	s.saturate(t, stallDefaultCap)

	for name, mkCtx := range map[string]func() (context.Context, context.CancelFunc){
		"deadline": func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(t.Context(), 200*time.Millisecond)
		},
		"cancel": func() (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(t.Context())
			time.AfterFunc(200*time.Millisecond, cancel)
			return ctx, cancel
		},
	} {
		ctx, cancel := mkCtx()
		start := time.Now()
		_, err := s.st.Enqueue(ctx, []job.Spec{{Queue: "bulk", Tenant: "t1", Payload: []byte("late")}})
		cancel()
		want := context.DeadlineExceeded
		if name == "cancel" {
			want = context.Canceled
		}
		switch {
		case !errors.Is(err, want):
			t.Errorf("%s: Enqueue = %v, want an error that wraps %v", name, err, want)
		case errors.Is(err, store.ErrUnavailable):
			t.Errorf("%s: Enqueue = %v, must not report the store as unavailable", name, err)
		case time.Since(start) > 3*time.Second:
			t.Errorf("%s: Enqueue took %v to give up on a 200ms context", name, time.Since(start))
		}
	}
	s.release(t)
}

func TestPGOnlyNotifyingCallsWaitForTheBound(t *testing.T) {
	// With a bound of one, a single notifying transaction is stuck and holds the
	// only permit. The other notifying calls wait for it, without a connection,
	// and the calls that do not notify go through: the bound covers Enqueue,
	// Release and Requeue and nothing else.
	s := stallEnqueues(t, 4, pgstore.WithMaxNotifying(1))
	s.saturate(t, 1)
	if n := s.pool.Stat().AcquiredConns(); n != 1 {
		t.Fatalf("%d connections are held, want the one of the stuck transaction: the waiters must hold none", n)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	grants, err := s.st.Lease(ctx, store.LeaseRequest{
		Queue: "q1", Takes: []store.TenantTake{{Tenant: "t1", Limit: 2}}, TTL: 30 * time.Second, Worker: "w1"})
	if err != nil || len(grants) != 2 {
		t.Fatalf("Lease = %d grants, %v; want 2", len(grants), err)
	}
	refs := []store.Ref{{ID: grants[0].Job.ID, Queue: "q1", Token: grants[0].Job.Epoch}, {ID: grants[1].Job.ID, Queue: "q1", Token: grants[1].Job.Epoch}}
	if res, err := s.st.Heartbeat(ctx, refs[:1], time.Minute); err != nil || len(res) != 1 {
		t.Fatalf("Heartbeat = %v, %v", res, err)
	}
	if out, err := s.st.Complete(ctx, []store.CompleteRequest{{Ref: refs[0]}}); err != nil || len(out) != 1 || out[0] != job.Applied {
		t.Fatalf("Complete = %v, %v; want applied", out, err)
	}
	if _, err := s.st.Activity(ctx, "q1"); err != nil {
		t.Fatalf("Activity: %v", err)
	}

	short := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(t.Context(), 300*time.Millisecond)
	}
	for name, call := range map[string]func(context.Context) error{
		"Enqueue": func(ctx context.Context) error {
			_, err := s.st.Enqueue(ctx, []job.Spec{{Queue: "bulk", Tenant: "t1", Payload: []byte("late")}})
			return err
		},
		"Release": func(ctx context.Context) error {
			_, err := s.st.Release(ctx, refs[1:])
			return err
		},
		"Requeue": func(ctx context.Context) error {
			_, err := s.st.Requeue(ctx, refs[0].ID)
			return err
		},
	} {
		ctx, cancel := short()
		err := call(ctx)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("%s while the bound is used up = %v, want it to wait for its turn and fail with the deadline", name, err)
		}
	}
	if n := s.pool.Stat().AcquiredConns(); n != 1 {
		t.Errorf("%d connections are held after the abandoned waits, want 1", n)
	}
	s.release(t)

	// The permit is back once the stuck transaction has finished, and a call
	// that was refused for the wait can run.
	if out, err := s.st.Release(t.Context(), refs[1:]); err != nil || len(out) != 1 || out[0] != job.Applied {
		t.Errorf("Release after the stall = %v, %v; want applied", out, err)
	}
}
