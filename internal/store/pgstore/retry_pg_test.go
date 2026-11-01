//go:build pg

package pgstore_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/pgtest"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/pgstore"
)

// newConcurrentStore returns a store on a fresh test database whose pool holds
// conns connections, all already open. The default pool opens connections as
// they are needed, and a goroutine that has to wait for a new connection
// starts late, which would turn "all at once" into "one after the other". Each
// option may change the pool's configuration before it is opened.
func newConcurrentStore(t *testing.T, conns int32, options ...func(*pgxpool.Config)) (*pgxpool.Pool, *pgstore.Store) {
	t.Helper()
	cfg := pgtest.NewDB(t).Config()
	cfg.MaxConns = conns
	for _, option := range options {
		option(cfg)
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatalf("open a pool of %d connections: %v", conns, err)
	}
	t.Cleanup(pool.Close)

	held := make([]*pgxpool.Conn, 0, conns)
	for range conns {
		c, err := pool.Acquire(t.Context())
		if err != nil {
			t.Fatalf("open the pool's connections: %v", err)
		}
		held = append(held, c)
	}
	for _, c := range held {
		c.Release()
	}
	return pool, pgstore.New(pool)
}

// quickDeadlockDetection makes PostgreSQL look for a deadlock after 20 ms
// instead of a second, so that a test in which deadlocks happen, or would
// without the fix it checks, does not wait a second for each.
func quickDeadlockDetection(cfg *pgxpool.Config) {
	cfg.ConnConfig.RuntimeParams["deadlock_timeout"] = "20ms"
}

// proxy forwards connections to a PostgreSQL server and can cut all of them at
// once, which is what a firewall or a load balancer does to a connection that
// has been idle for too long: the end of the stream, with no error.
type proxy struct {
	listener net.Listener

	mu    sync.Mutex
	conns []net.Conn
}

func newProxy(t *testing.T, target string) *proxy {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{listener: l}
	t.Cleanup(func() {
		_ = l.Close()
		p.cut()
	})
	go func() {
		for {
			client, err := l.Accept()
			if err != nil {
				return
			}
			server, err := net.Dial("tcp", target)
			if err != nil {
				_ = client.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, client, server)
			p.mu.Unlock()
			go func() { _, _ = io.Copy(server, client); _ = server.Close() }()
			go func() { _, _ = io.Copy(client, server); _ = client.Close() }()
		}
	}()
	return p
}

// cut closes every connection that the proxy carries.
func (p *proxy) cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
}

// A pooled connection that the other end closed while it sat idle is found out
// only when it is used. The failure that comes back, the end of the stream, is
// not a network error and not one that the driver reports as safe to retry, and
// it must still be a store.ErrUnavailable: the connection is gone, and the next
// call opens a new one. Here the call never ran, but the error cannot say so for
// a connection that dropped in the middle of a call, which may have committed;
// that is why a retry is safe only for a call that tolerates repetition.
func TestPGClassifiesALostConnection(t *testing.T) {
	cfg := pgtest.NewDB(t).Config()
	p := newProxy(t, net.JoinHostPort(cfg.ConnConfig.Host, strconv.Itoa(int(cfg.ConnConfig.Port))))
	cfg.ConnConfig.Host, cfg.ConnConfig.Port, cfg.ConnConfig.Fallbacks = "127.0.0.1", uint16(p.listener.Addr().(*net.TCPAddr).Port), nil
	cfg.MaxConns = 1 // the one connection is the stale one
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s := pgstore.New(pool)

	for name, call := range storeCalls(t.Context(), s) {
		if _, err := pool.Exec(t.Context(), "SELECT 1"); err != nil { // opens a connection and returns it to the pool
			t.Fatalf("%s: warm up: %v", name, err)
		}
		p.cut()
		if err := call(); !errors.Is(err, store.ErrUnavailable) {
			t.Errorf("%s on a connection that was closed under the pool: error = %v, want one wrapping store.ErrUnavailable", name, err)
		}
	}
}

// planShapes are the two ways in which the tests for opposite-order calls have
// the statements of an acknowledgement planned: as the planner chooses them for
// a small table, and with index lookups and nested loops ruled out, which leaves
// a hash join over a scan of the table, whose order is that of the pages. That
// is the plan that autovacuum's statistics can bring about on a table that has
// grown.
//
// The second shape also turns JIT compilation off, and the tests are unusable
// without that. Every statement of the store joins the one-row clock CTE, which
// can only be a nested loop, so with nested loops ruled out PostgreSQL 17 and
// earlier add its disable_cost of 1e10 to the plan (version 18 counts disabled
// nodes instead). That puts every statement far above jit_above_cost, and a
// server built with LLVM then compiles each one, with inlining and optimization,
// in 50 to 190 ms. On PostgreSQL 15 that made an Enqueue of 24 jobs, which is 24
// statements, take 1.1 s, and the three-minute tests that ran into the package's
// ten-minute timeout. JIT does not change which plan is chosen, only what a
// statement costs to start, so turning it off keeps the shape.
var planShapes = map[string]func(*pgxpool.Config){
	"with the plan the planner chooses": func(*pgxpool.Config) {},
	"with a hash join over a scan": func(cfg *pgxpool.Config) {
		for _, param := range []string{"enable_nestloop", "enable_indexscan", "enable_bitmapscan", "enable_indexonlyscan", "enable_mergejoin"} {
			cfg.ConnConfig.RuntimeParams[param] = "off"
		}
		cfg.ConnConfig.RuntimeParams["jit"] = "off"
	},
}

// leaseFresh enqueues n jobs in queue and leases them all, and returns their
// refs in ascending id.
func leaseFresh(t *testing.T, pg *pgstore.Store, queue string, n int) []store.Ref {
	t.Helper()
	specs := make([]job.Spec, n)
	for i := range specs {
		specs[i] = spec(queue, "t")
	}
	if _, err := pg.Enqueue(t.Context(), specs); err != nil {
		t.Fatal(err)
	}
	grants, err := pg.Lease(t.Context(), store.LeaseRequest{
		Queue: queue, Takes: []store.TenantTake{{Tenant: "t", Limit: n}}, TTL: time.Minute, Worker: "w",
	})
	if err != nil || len(grants) != n {
		t.Fatalf("Lease in %s = %d grants, %v; want %d", queue, len(grants), err, n)
	}
	refs := make([]store.Ref, len(grants))
	for i, g := range grants {
		refs[i] = store.Ref{ID: g.Job.ID, Queue: queue, Token: g.Job.Epoch}
	}
	slices.SortFunc(refs, func(a, b store.Ref) int { return cmp.Compare(a.ID, b.ID) })
	return refs
}

// raceBehindAGate runs call(0) to call(callers-1) in goroutines of their own,
// all in contention for the rows of refs, and returns when they have finished.
// The pool needs a connection for each caller and two more.
//
// A transaction holds a lock on every one of the rows while the goroutines
// start, and commits only when PostgreSQL reports a backend waiting for a row
// lock for each caller. The calls are then known to be in flight together, each
// stopped at its first row, and they all start on the rows in the same instant.
// Starting goroutines together cannot promise that: a call that runs to its end
// before another begins also leaves the other nothing to wait for, and the test
// would pass without having raced anything. If the callers are not all waiting
// within ten seconds the test fails and says so.
func raceBehindAGate(t *testing.T, pool *pgxpool.Pool, refs []store.Ref, callers int, call func(c int)) {
	t.Helper()
	ids := make([]int64, len(refs))
	for i, r := range refs {
		ids[i] = r.ID
	}
	raceBehind(t, pool, callers, func(tx pgx.Tx) error {
		_, err := tx.Exec(t.Context(), "SELECT id FROM jobqueue.jobs WHERE id = ANY($1::int8[]) ORDER BY id FOR UPDATE", ids)
		return err
	}, call)
}

// raceBehindATableLock is raceBehindAGate for calls that insert rows, which have
// no rows to wait for yet: the transaction that stops them holds a lock on the
// table of jobs that every insert needs, so each caller waits at its first
// insert and they all go on in the same instant.
func raceBehindATableLock(t *testing.T, pool *pgxpool.Pool, callers int, call func(c int)) {
	t.Helper()
	raceBehind(t, pool, callers, func(tx pgx.Tx) error {
		_, err := tx.Exec(t.Context(), "LOCK TABLE jobqueue.jobs IN SHARE ROW EXCLUSIVE MODE")
		return err
	}, call)
}

// raceBehind runs call(0) to call(callers-1) in goroutines of their own, once
// hold has taken in a transaction the locks that stop them, and releases the
// locks when PostgreSQL reports a backend waiting for a lock for each caller.
// See raceBehindAGate.
func raceBehind(t *testing.T, pool *pgxpool.Pool, callers int, hold func(tx pgx.Tx) error, call func(c int)) {
	t.Helper()
	gate, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = gate.Rollback(context.Background()) }() // does nothing after the commit
	if err := hold(gate); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for c := range callers {
		wg.Go(func() { call(c) })
	}

	waiting := 0
	for deadline := time.Now().Add(10 * time.Second); waiting < callers && time.Now().Before(deadline); {
		// Each test has a database of its own, so every waiting backend in it is a caller.
		err := pool.QueryRow(t.Context(),
			"SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'").Scan(&waiting)
		if err != nil {
			t.Errorf("count the calls that wait for a lock: %v", err)
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if waiting < callers {
		t.Errorf("%d of %d calls were waiting for a lock, so they did not overlap", waiting, callers)
	}
	if err := gate.Commit(t.Context()); err != nil {
		t.Errorf("release the locks: %v", err)
	}
	wg.Wait()
}

// testOppositeOrderAck has callers goroutines acknowledge the same jobs at the
// same moment (see raceBehindAGate), half of them naming the jobs in ascending
// id and half in descending id, and requires that no call fails. Every outcome
// must be one of allowed. If exclusive, an acknowledgement ends the lease and
// each job is acknowledged by exactly one of the callers, and the others see
// that it is gone; if not, as for a heartbeat, every caller finds every job
// applied.
//
// A call that arrives after the lease has ended locks nothing, so only calls
// that arrive together can deadlock. Every round therefore leases new jobs, in
// a queue of its own.
func testOppositeOrderAck(t *testing.T, allowed []job.Outcome, exclusive bool, ack func(ctx context.Context, pg *pgstore.Store, refs []store.Ref) ([]job.Outcome, error)) {
	t.Helper()
	const (
		jobs    = 24
		callers = 8
		rounds  = 40
	)
	for name, option := range planShapes {
		t.Run(name, func(t *testing.T) {
			// A connection for each caller, one for the gate and one to watch.
			pool, pg := newConcurrentStore(t, callers+2, quickDeadlockDetection, option)
			for round := range rounds {
				ascending := leaseFresh(t, pg, fmt.Sprintf("q%d", round), jobs)
				orders := [2][]store.Ref{ascending, slices.Clone(ascending)}
				slices.Reverse(orders[1])

				results := make([][]job.Outcome, callers)
				raceBehindAGate(t, pool, ascending, callers, func(c int) {
					res, err := ack(t.Context(), pg, orders[c%2])
					if err != nil {
						t.Errorf("round %d, caller %d: %v, want no error: calls in opposite orders must not deadlock", round, c, err)
						return
					}
					results[c] = res
				})
				if t.Failed() {
					return
				}

				won := make(map[int64]int, jobs)
				for c, res := range results {
					for i, o := range res {
						if !slices.Contains(allowed, o) {
							t.Errorf("round %d, caller %d: job %d = %v, want one of %v", round, c, orders[c%2][i].ID, o, allowed)
						}
						if o == job.Applied {
							won[orders[c%2][i].ID]++
						}
					}
				}
				want := callers
				if exclusive {
					want = 1
				}
				for _, r := range ascending {
					if won[r.ID] != want {
						t.Errorf("round %d: job %d was applied for %d callers, want %d", round, r.ID, won[r.ID], want)
					}
				}
				if t.Failed() {
					return
				}
			}
		})
	}
}

// Calls that name the same jobs in opposite orders must not deadlock. The
// statements of an acknowledgement call lock the rows they change in ascending
// id, whatever the order of the request and of the plan, so that every call
// takes the row locks in the same order. Without that, calls fail with a
// deadlock. The statements are run under each of planShapes, and the callers
// are held at the same moment, as testOppositeOrderAck describes. Heartbeats
// leave the lease in place, so every caller finds every job applied.
func TestPGOppositeOrderHeartbeatsDoNotDeadlock(t *testing.T) {
	testOppositeOrderAck(t, []job.Outcome{job.Applied}, false,
		func(ctx context.Context, pg *pgstore.Store, refs []store.Ref) ([]job.Outcome, error) {
			res, err := pg.Heartbeat(ctx, refs, time.Minute)
			if err != nil {
				return nil, err
			}
			out := make([]job.Outcome, len(res))
			for i, r := range res {
				out[i] = r.Outcome
			}
			return out, nil
		})
}

// Completions that name the same jobs in opposite orders must not deadlock; see
// TestPGOppositeOrderHeartbeatsDoNotDeadlock.
func TestPGOppositeOrderCompletesDoNotDeadlock(t *testing.T) {
	testOppositeOrderAck(t, []job.Outcome{job.Applied, job.Duplicate, job.Fenced}, true,
		func(ctx context.Context, pg *pgstore.Store, refs []store.Ref) ([]job.Outcome, error) {
			reqs := make([]store.CompleteRequest, len(refs))
			for i, r := range refs {
				reqs[i] = store.CompleteRequest{Ref: r, Result: []byte("done")}
			}
			return pg.Complete(ctx, reqs)
		})
}

// Failures that name the same jobs in opposite orders must not deadlock. They
// are permanent, so that the job ends and does not come back for another try;
// see TestPGOppositeOrderHeartbeatsDoNotDeadlock.
func TestPGOppositeOrderFailsDoNotDeadlock(t *testing.T) {
	testOppositeOrderAck(t, []job.Outcome{job.Applied, job.Fenced}, true,
		func(ctx context.Context, pg *pgstore.Store, refs []store.Ref) ([]job.Outcome, error) {
			reqs := make([]store.FailRequest, len(refs))
			for i, r := range refs {
				reqs[i] = store.FailRequest{Ref: r, Permanent: true, Error: "boom"}
			}
			return pg.Fail(ctx, reqs)
		})
}

// Releases that name the same jobs in opposite orders must not deadlock; see
// TestPGOppositeOrderHeartbeatsDoNotDeadlock.
func TestPGOppositeOrderReleasesDoNotDeadlock(t *testing.T) {
	testOppositeOrderAck(t, []job.Outcome{job.Applied, job.Fenced}, true,
		func(ctx context.Context, pg *pgstore.Store, refs []store.Ref) ([]job.Outcome, error) {
			return pg.Release(ctx, refs)
		})
}

// Enqueue keeps the items in request order, because ids follow it, so two
// batches that insert the same new keys in opposite orders can deadlock in the
// unique index. PostgreSQL aborts one of them, and the caller must be told that
// it may try again: the abort is a store.ErrUnavailable, and after the retries
// the table holds one job for each key.
//
// The callers are held at the same moment, as raceBehindATableLock describes,
// and the test fails unless an abort was seen: a run in which the calls happened
// not to overlap, or in which the abort was no longer reported, would otherwise
// pass.
func TestPGOppositeOrderEnqueuesDeadlockAsRetryable(t *testing.T) {
	const (
		callers = 2
		rounds  = 8
		keys    = 10
	)
	// A connection for each caller, one for the gate and one to watch.
	pool, pg := newConcurrentStore(t, callers+2, quickDeadlockDetection)

	var (
		mu        sync.Mutex
		deadlocks int
	)
	for round := range rounds {
		raceBehindATableLock(t, pool, callers, func(c int) {
			specs := make([]job.Spec, keys)
			for k := range keys {
				at := k
				if c%2 == 1 {
					at = keys - 1 - k
				}
				specs[at] = job.Spec{Queue: "q", Tenant: "t", IdempotencyKey: fmt.Sprintf("r%d-k%d", round, k)}
			}
			for attempt := 0; ; attempt++ {
				_, err := pg.Enqueue(t.Context(), specs)
				if err == nil {
					return
				}
				if !errors.Is(err, store.ErrUnavailable) {
					t.Errorf("caller %d round %d: Enqueue: %v, want nil or an error wrapping store.ErrUnavailable", c, round, err)
					return
				}
				mu.Lock()
				deadlocks++
				mu.Unlock()
				if attempt == 50 {
					t.Errorf("caller %d round %d: Enqueue still fails after %d tries: %v", c, round, attempt, err)
					return
				}
			}
		})
		if t.Failed() {
			return
		}
	}

	var n int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM jobqueue.jobs").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if want := rounds * keys; n != want {
		t.Errorf("the table holds %d jobs, want %d: one for each key, however many tries it took", n, want)
	}
	if deadlocks == 0 {
		t.Errorf("no Enqueue was aborted in %d rounds of %d callers: the batches did not collide, so nothing was checked", rounds, callers)
	}
	t.Logf("%d enqueues were aborted and retried", deadlocks)
}

// When an Enqueue finds that an item duplicates a job and the job then goes
// before the batch reads it back, the batch has still been applied. Triggers
// stand in for a prune that runs between the two statements: a row inserted for
// the tenant "vanish" deletes the job that holds the key, and one inserted for
// the tenant "swap" does that too and, at commit, inserts a new job under the
// same key.
func TestPGEnqueueWhenADuplicateJobVanishes(t *testing.T) {
	setup := func(t *testing.T) (*pgxpool.Pool, *pgstore.Store) {
		pool := pgtest.NewDB(t)
		for _, stmt := range []string{
			`CREATE FUNCTION jobqueue_test.vanish() RETURNS trigger LANGUAGE plpgsql AS $$
			 BEGIN DELETE FROM jobqueue.jobs WHERE idempotency_key = 'held'; RETURN NEW; END $$`,
			`CREATE TRIGGER vanish BEFORE INSERT ON jobqueue.jobs FOR EACH ROW
			 WHEN (NEW.tenant IN ('vanish', 'swap')) EXECUTE FUNCTION jobqueue_test.vanish()`,
			`CREATE FUNCTION jobqueue_test.swap() RETURNS trigger LANGUAGE plpgsql AS $$
			 BEGIN
			   INSERT INTO jobqueue.jobs (queue, tenant, state, payload, run_at, attempts, max_attempts, idempotency_key, created_at)
			   VALUES ('q', 'a', 'ready', ''::bytea, jobqueue.now(), 0, 25, 'held', jobqueue.now());
			   RETURN NULL;
			 END $$`,
			`CREATE CONSTRAINT TRIGGER swap AFTER INSERT ON jobqueue.jobs DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
			 WHEN (NEW.tenant = 'swap') EXECUTE FUNCTION jobqueue_test.swap()`,
		} {
			if _, err := pool.Exec(t.Context(), stmt); err != nil {
				t.Fatal(err)
			}
		}
		s := pgstore.New(pool)
		if _, err := s.Enqueue(t.Context(), []job.Spec{{Queue: "q", Tenant: "a", IdempotencyKey: "held"}}); err != nil {
			t.Fatal(err)
		}
		return pool, s
	}
	batch := func(tenant string) []job.Spec {
		return []job.Spec{
			{Queue: "q", Tenant: "a", IdempotencyKey: "held"}, // a duplicate of job 1
			{Queue: "q", Tenant: tenant, Payload: []byte("unkeyed")},
		}
	}
	countUnkeyed := func(t *testing.T, pool *pgxpool.Pool, tenant string) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM jobqueue.jobs WHERE tenant = $1", tenant).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	t.Run("a job that is back by the time it is looked for again", func(t *testing.T) {
		pool, s := setup(t)
		res, err := s.Enqueue(t.Context(), batch("swap"))
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		var held int64
		if err := pool.QueryRow(t.Context(), "SELECT id FROM jobqueue.jobs WHERE idempotency_key = 'held'").Scan(&held); err != nil {
			t.Fatal(err)
		}
		if !res[0].Deduplicated || res[0].ID != held {
			t.Errorf("item 0 = %+v, want a duplicate of job %d, the job that holds the key now", res[0], held)
		}
		if res[1].Deduplicated || res[1].ID == 0 {
			t.Errorf("item 1 = %+v, want a new job", res[1])
		}
	})

	t.Run("a job that is gone for good", func(t *testing.T) {
		pool, s := setup(t)
		res, err := s.Enqueue(t.Context(), batch("vanish"))
		if err == nil {
			t.Fatalf("Enqueue = %+v, want an error: the job that item 0 duplicates is gone", res)
		}
		if !strings.Contains(err.Error(), "the batch was applied") {
			t.Errorf("error = %q, want it to say that the batch was applied", err)
		}
		if errors.Is(err, store.ErrUnavailable) {
			t.Errorf("error = %q wraps store.ErrUnavailable, which invites sending the whole batch again", err)
		}
		if n := countUnkeyed(t, pool, "vanish"); n != 1 {
			t.Errorf("the batch's unkeyed job was created %d times, want once", n)
		}
	})
}
