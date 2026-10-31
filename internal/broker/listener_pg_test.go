//go:build pg

package broker

import (
	"context"
	"io"
	"log/slog"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/pgtest"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/pgstore"
)

// dbURL returns a connection URL for the private database of a pgtest pool: the
// server of JOBQUEUE_TEST_DSN with the pool's database name.
func dbURL(t testing.TB, pool *pgxpool.Pool) string {
	t.Helper()
	u, err := url.Parse(pgtest.DSN(t))
	if err != nil {
		t.Fatalf("parse JOBQUEUE_TEST_DSN: %v", err)
	}
	u.Path = "/" + pool.Config().ConnConfig.Database
	return u.String()
}

// listenEnv is a pgstore dispatcher and a listener on it, both running.
type listenEnv struct {
	t    *testing.T
	pool *pgxpool.Pool
	st   *pgstore.Store
	reg  *prometheus.Registry
	d    *Dispatcher
	l    *Listener
}

func newListenEnv(t *testing.T, configure ...func(*Listener)) *listenEnv {
	t.Helper()
	e := &listenEnv{t: t, pool: pgtest.NewDB(t), reg: prometheus.NewRegistry()}
	e.st = pgstore.New(e.pool)
	m := NewMetrics(e.reg)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	e.d = NewDispatcher(e.st, DispatcherConfig{Scheduler: "drr", PollInterval: time.Hour, ActivityInterval: time.Hour}, m, log)
	e.l = NewListener(dbURL(t, e.pool), e.d, m, log)
	for _, c := range configure {
		c(e.l)
	}

	ctx, cancel := context.WithCancel(context.Background())
	dispDone, listenDone := make(chan error, 1), make(chan error, 1)
	go func() { dispDone <- e.d.Run(ctx) }()
	go func() { listenDone <- e.l.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		for name, done := range map[string]chan error{"dispatcher": dispDone, "listener": listenDone} {
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("%s Run returned %v", name, err)
				}
			case <-time.After(10 * time.Second):
				t.Errorf("%s Run did not return", name)
			}
		}
	})
	return e
}

func (e *listenEnv) metric(name string, labels map[string]string) float64 {
	e.t.Helper()
	return metricValue(e.t, e.reg, name, labels)
}

func (e *listenEnv) enqueue(queue, tenant string) {
	e.t.Helper()
	if _, err := e.st.Enqueue(e.t.Context(), []job.Spec{{Queue: queue, Tenant: tenant, Payload: []byte("p")}}); err != nil {
		e.t.Fatalf("Enqueue: %v", err)
	}
}

// parkWaiter starts a long poll on the queue and waits until its first fetch
// has found nothing, so that only a wakeup can serve it.
func (e *listenEnv) parkWaiter(queue string) <-chan leaseResult {
	e.t.Helper()
	ch := make(chan leaseResult, 1)
	go func() {
		ans, err := e.d.Lease(e.t.Context(), LeaseCall{Queue: queue, Max: 1, Wait: 30 * time.Second, TTL: 30 * time.Second, Worker: "w"})
		ch <- leaseResult{ans, err}
	}()
	waitFor(e.t, "the first fetch", quickly, func() bool {
		return e.metric("jobqueue_dispatch_fetches_total", map[string]string{"queue": queue, "result": fetchEmpty}) >= 1
	})
	return ch
}

func (e *listenEnv) waitListening(n int64) {
	e.t.Helper()
	waitFor(e.t, "the listener to LISTEN", quickly, func() bool { return e.l.listens.Load() >= n })
}

func TestPGListenerDeliversNotify(t *testing.T) {
	e := newListenEnv(t)
	e.waitListening(1)
	res := e.parkWaiter("q")

	start := time.Now()
	e.enqueue("q", "t") // on another connection: the store notifies in the transaction
	wantGrants(t, recv(t, res, quickly), 1)
	t.Logf("delivered %v after the enqueue", time.Since(start))
}

func TestPGListenerReconnectsAndResyncs(t *testing.T) {
	// A long backoff keeps the listener away for a while, so that the job
	// enqueued in between is announced to nobody and only the resync after
	// the reconnect can find it.
	e := newListenEnv(t, func(l *Listener) { l.backoff = func(int) time.Duration { return 500 * time.Millisecond } })
	e.waitListening(1)
	res := e.parkWaiter("q")

	if _, err := e.pool.Exec(t.Context(),
		`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		  WHERE (query LIKE 'LISTEN%' OR application_name = 'jobqueue-listener') AND pid <> pg_backend_pid()`); err != nil {
		t.Fatalf("terminate the listener's backend: %v", err)
	}
	waitFor(t, "the reconnect counter", 6*time.Second, func() bool {
		return e.metric("jobqueue_notify_reconnects_total", nil) == 1
	})
	e.enqueue("q", "t") // the listener is not connected: this notification is lost
	wantGrants(t, recv(t, res, 10*time.Second), 1)

	// The new connection also delivers notifications.
	e.waitListening(2)
	res = e.parkWaiter("q2")
	e.enqueue("q2", "t")
	wantGrants(t, recv(t, res, quickly), 1)
}

func TestPGListenerCountsMalformedPayloads(t *testing.T) {
	e := newListenEnv(t)
	e.waitListening(1)
	for _, payload := range []string{"no-tab", "a b\tt", "q\t"} {
		if _, err := e.pool.Exec(t.Context(), "SELECT pg_notify($1, $2)", pgstore.NotifyChannel, payload); err != nil {
			t.Fatalf("notify %q: %v", payload, err)
		}
	}
	waitFor(t, "the malformed counter", quickly, func() bool {
		return e.metric("jobqueue_notify_malformed_total", nil) == 3
	})
}

func TestPGListenerSurvivesIdlePings(t *testing.T) {
	// A liveness check runs while the connection is idle. It must not break
	// the connection or lose a notification that arrives right after it.
	e := newListenEnv(t, func(l *Listener) { l.pingEvery = 20 * time.Millisecond })
	e.waitListening(1)
	res := e.parkWaiter("q")
	time.Sleep(200 * time.Millisecond) // about ten pings; there is nothing to wait on instead
	e.enqueue("q", "t")
	wantGrants(t, recv(t, res, quickly), 1)
	if got := e.metric("jobqueue_notify_reconnects_total", nil); got != 0 {
		t.Errorf("notify_reconnects_total = %v, want 0: a ping must not cost the connection", got)
	}
}
