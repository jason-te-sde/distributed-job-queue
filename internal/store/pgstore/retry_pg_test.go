//go:build pg

package pgstore_test

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

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
// it must still be a store.ErrUnavailable: the call did not run, and the next
// one opens a new connection.
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

// Calls that name the same jobs in opposite orders must not deadlock. The
// statements of an acknowledgement call lock the rows they change in ascending
// id, whatever the order of the request and of the plan, so that every call
// takes the row locks in the same order. Without that, calls fail with a
// deadlock. The statements are run once as the planner chooses them for a small
// table and once with index lookups and nested loops ruled out, which leaves a
// hash join over a scan of the table, whose order is that of the pages: the
// plan that autovacuum's statistics can bring about on a table that has grown.
func TestPGOppositeOrderHeartbeatsDoNotDeadlock(t *testing.T) {
	const (
		jobs    = 24
		callers = 8
		rounds  = 100
	)
	for name, option := range map[string]func(*pgxpool.Config){
		"with the plan the planner chooses": func(*pgxpool.Config) {},
		"with a hash join over a scan": func(cfg *pgxpool.Config) {
			for _, param := range []string{"enable_nestloop", "enable_indexscan", "enable_bitmapscan", "enable_indexonlyscan", "enable_mergejoin"} {
				cfg.ConnConfig.RuntimeParams[param] = "off"
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, pg := newConcurrentStore(t, callers, quickDeadlockDetection, option)
			refs := leaseAll(t, jobs, pg)
			descending := make([]store.Ref, jobs)
			for i, r := range refs {
				descending[jobs-1-i] = r
			}

			var wg sync.WaitGroup
			for c := range callers {
				order := refs
				if c%2 == 1 {
					order = descending
				}
				wg.Go(func() {
					for range rounds {
						res, err := pg.Heartbeat(t.Context(), order, time.Minute)
						if err != nil {
							t.Errorf("caller %d: Heartbeat: %v", c, err)
							return
						}
						for i, r := range res {
							if r.Outcome != job.Applied {
								t.Errorf("caller %d: item %d (job %d) = %v, want applied", c, i, order[i].ID, r.Outcome)
								return
							}
						}
					}
				})
			}
			wg.Wait()
		})
	}
}

// Enqueue keeps the items in request order, because ids follow it, so two
// batches that insert the same new keys in opposite orders can deadlock in the
// unique index. PostgreSQL aborts one of them, and the caller must be told that
// it may try again: the abort is a store.ErrUnavailable and nothing of the
// batch was written.
func TestPGOppositeOrderEnqueuesDeadlockAsRetryable(t *testing.T) {
	const (
		callers = 6
		rounds  = 8
		keys    = 10
	)
	pool, pg := newConcurrentStore(t, callers, quickDeadlockDetection)

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		deadlocks int
	)
	for c := range callers {
		wg.Go(func() {
			for round := range rounds {
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
						break
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
			}
		})
	}
	wg.Wait()

	var n int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM jobqueue.jobs").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if want := rounds * keys; n != want {
		t.Errorf("the table holds %d jobs, want %d: one for each key, however many tries it took", n, want)
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
