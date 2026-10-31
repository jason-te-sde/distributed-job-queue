//go:build pg

package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	jobqueuev1 "github.com/jason-te-sde/distributed-job-queue/internal/gen/jobqueue/v1"
	"github.com/jason-te-sde/distributed-job-queue/internal/pgtest"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/pgstore/migrate"
)

// runningBroker is a Broker on loopback ports, running.
type runningBroker struct {
	t      *testing.T
	b      *Broker
	stop   context.CancelFunc
	done   chan error
	client jobqueuev1.JobQueueServiceClient
}

// startBroker runs a broker over the private database of pool. tune changes the
// configuration, which starts from the defaults with the ports left to the
// system.
func startBroker(t *testing.T, pool *pgxpool.Pool, tune func(*Config)) *runningBroker {
	t.Helper()
	cfg, err := ParseFlags([]string{
		"--grpc-addr=127.0.0.1:0", "--http-addr=127.0.0.1:0", "--database-url=" + dbURL(t, pool),
		"--log-level=debug",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if tune != nil {
		tune(&cfg)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	b, err := New(ctx, cfg, log)
	if err != nil {
		cancel()
		t.Fatalf("New: %v", err)
	}
	rb := &runningBroker{t: t, b: b, stop: cancel, done: make(chan error, 1)}
	go func() { rb.done <- b.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-rb.done:
			if err != nil {
				t.Errorf("Run returned %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Error("Run did not return")
		}
	})

	conn, err := grpc.NewClient("passthrough:///"+b.GRPCAddr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	rb.client = jobqueuev1.NewJobQueueServiceClient(conn)
	return rb
}

func (rb *runningBroker) httpURL(path string) string {
	return "http://" + rb.b.HTTPAddr().String() + path
}

// httpGet returns the status and body of a GET on the HTTP endpoint.
func (rb *runningBroker) httpGet(path string) (int, string, error) {
	ctx, cancel := context.WithTimeout(rb.t.Context(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rb.httpURL(path), nil)
	if err != nil {
		return 0, "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(body)), err
}

// series returns the value of a series of /metrics, such as
// `jobqueue_waiters{queue="q"}`, and whether it is there.
func (rb *runningBroker) series(name string) (float64, bool) {
	rb.t.Helper()
	code, body, err := rb.httpGet("/metrics")
	if err != nil || code != http.StatusOK {
		rb.t.Fatalf("GET /metrics = %d, %v", code, err)
	}
	for _, line := range strings.Split(body, "\n") {
		if value, ok := strings.CutPrefix(line, name+" "); ok {
			v, err := strconv.ParseFloat(value, 64)
			if err != nil {
				rb.t.Fatalf("series %s has the value %q", name, value)
			}
			return v, true
		}
	}
	return 0, false
}

func (rb *runningBroker) waitSeries(name string, want float64) {
	rb.t.Helper()
	waitFor(rb.t, name+" to be "+fmt.Sprint(want), 10*time.Second, func() bool {
		v, ok := rb.series(name)
		return ok && v == want
	})
}

// waitSeriesAtLeast waits for a counter that other events may also advance:
// the resync after the listener's first LISTEN, for one, causes a fetch of its
// own.
func (rb *runningBroker) waitSeriesAtLeast(name string, want float64) {
	rb.t.Helper()
	waitFor(rb.t, name+" to reach "+fmt.Sprint(want), 10*time.Second, func() bool {
		v, ok := rb.series(name)
		return ok && v >= want
	})
}

// settleListener waits until the listener has started to listen and the loop
// of the dispatcher has acted on the resync that follows, so that no fetch of
// that resync is still to come. A test that blocks the table, or counts
// fetches, starts from here.
func (rb *runningBroker) settleListener() {
	rb.t.Helper()
	d := rb.b.dispatcher
	waitFor(rb.t, "the listener to listen", 10*time.Second, func() bool { return rb.b.listener.listens.Load() >= 1 })
	waitFor(rb.t, "the loop to take the resync", 10*time.Second, func() bool {
		d.nmu.Lock()
		defer d.nmu.Unlock()
		return len(d.wake) == 0 && !d.resync
	})
	// A call that waits for nothing goes through the loop, so when it returns
	// the loop has finished the pass that took the resync.
	_, err := rb.client.Lease(rb.t.Context(), &jobqueuev1.LeaseRequest{
		Queue: "settle", MaxJobs: 1, Ttl: durationpb.New(30 * time.Second), Wait: durationpb.New(0), WorkerId: "w",
	})
	if err != nil {
		rb.t.Fatalf("settling Lease: %v", err)
	}
}

func (rb *runningBroker) waitReady() {
	rb.t.Helper()
	waitFor(rb.t, "/readyz to be 200", 10*time.Second, func() bool {
		code, _, err := rb.httpGet("/readyz")
		return err == nil && code == http.StatusOK
	})
}

func withToken(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

func TestPGBrokerRefusesSchemaMismatch(t *testing.T) {
	pool := pgtest.NewEmptyDB(t) // no migrations
	cfg, err := ParseFlags([]string{
		"--grpc-addr=127.0.0.1:0", "--http-addr=127.0.0.1:0", "--database-url=" + dbURL(t, pool),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(t.Context(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	var mismatch *migrate.SchemaMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("New on an unmigrated database = %v, %v; want a *migrate.SchemaMismatchError", b, err)
	}
	if mismatch.Have != 0 || mismatch.Want != migrate.Version {
		t.Errorf("mismatch = %+v, want have 0 and want %d", mismatch, migrate.Version)
	}
}

func TestPGBrokerServesAndReportsItself(t *testing.T) {
	rb := startBroker(t, pgtest.NewDB(t), nil)
	rb.b.SetVersion("v9.9.9")
	rb.waitReady()

	// Without a tokens file the caller is an admin, which the loopback address
	// allows.
	ctx := t.Context()
	enq, err := rb.client.Enqueue(ctx, &jobqueuev1.EnqueueRequest{Items: []*jobqueuev1.EnqueueItem{{Queue: "q", Tenant: "t", Payload: []byte("p")}}})
	if err != nil || len(enq.GetResults()) != 1 {
		t.Fatalf("Enqueue = %v, %v", enq, err)
	}
	lease, err := rb.client.Lease(ctx, &jobqueuev1.LeaseRequest{
		Queue: "q", MaxJobs: 1, Ttl: durationpb.New(30 * time.Second), Wait: durationpb.New(5 * time.Second), WorkerId: "w",
	})
	if err != nil || len(lease.GetJobs()) != 1 || lease.GetJobs()[0].GetId() != enq.GetResults()[0].GetId() {
		t.Fatalf("Lease = %v, %v; want the enqueued job", lease, err)
	}

	if code, body, err := rb.httpGet("/healthz"); err != nil || code != 200 || body != "ok" {
		t.Errorf("/healthz = %d %q, %v", code, body, err)
	}
	if code, body, err := rb.httpGet("/readyz"); err != nil || code != 200 || body != "ready" {
		t.Errorf("/readyz = %d %q, %v", code, body, err)
	}
	for series, want := range map[string]float64{
		`jobqueue_jobs_enqueued_total{queue="q"}`:                                      1,
		`jobqueue_jobs_leased_total{queue="q"}`:                                        1,
		`jobqueue_store_call_seconds_count{op="enqueue"}`:                              1,
		`jobqueue_build_info{go_version="` + runtime.Version() + `",version="v9.9.9"}`: 1,
	} {
		rb.waitSeries(series, want)
	}
	if _, ok := rb.series("go_goroutines"); !ok {
		t.Error("/metrics has no go_goroutines")
	}
}

func TestPGBrokerEnforcesTheTokensFile(t *testing.T) {
	file := writeTokens(t, "worker queues=q "+hashOf("worker-token")+"\n")
	rb := startBroker(t, pgtest.NewDB(t), func(c *Config) { c.TokensFile = file })
	rb.waitReady()

	lease := func(ctx context.Context) error {
		_, err := rb.client.Lease(ctx, &jobqueuev1.LeaseRequest{
			Queue: "q", MaxJobs: 1, Ttl: durationpb.New(30 * time.Second), Wait: durationpb.New(0), WorkerId: "w",
		})
		return err
	}
	if err := lease(t.Context()); status.Code(err) != codes.Unauthenticated {
		t.Errorf("Lease without a token = %v, want Unauthenticated: a tokens file must not leave the API open", err)
	}
	if err := lease(withToken(t.Context(), "wrong")); status.Code(err) != codes.Unauthenticated {
		t.Errorf("Lease with an unknown token = %v, want Unauthenticated", err)
	}
	if err := lease(withToken(t.Context(), "worker-token")); err != nil {
		t.Errorf("Lease with the worker's token = %v", err)
	}
}

func TestPGBrokerGracefulShutdown(t *testing.T) {
	pool := pgtest.NewDB(t)
	const grace = 1500 * time.Millisecond
	rb := startBroker(t, pool, func(c *Config) {
		c.ShutdownGrace = grace
		// Nothing but the shutdown may wake the long poll or touch the table
		// while the test holds it.
		c.PollInterval, c.ActivityInterval = time.Hour, time.Hour
		c.ReapInterval, c.PruneInterval = time.Hour, time.Hour
	})
	rb.waitReady()
	rb.settleListener()

	// 1. A long poll with a wait of 20s.
	type leaseResult struct {
		resp *jobqueuev1.LeaseResponse
		err  error
	}
	polled := make(chan leaseResult, 1)
	go func() {
		resp, err := rb.client.Lease(context.Background(), &jobqueuev1.LeaseRequest{
			Queue: "q", MaxJobs: 1, Ttl: durationpb.New(30 * time.Second), Wait: durationpb.New(20 * time.Second), WorkerId: "w",
		})
		polled <- leaseResult{resp, err}
	}()
	rb.waitSeries(`jobqueue_waiters{queue="q"}`, 1)
	rb.waitSeriesAtLeast(`jobqueue_dispatch_fetches_total{queue="q",result="empty"}`, 1)

	// 2. A call the shutdown has to wait for: a lock on the table makes a
	// store call block, so the grace period has something to run out on.
	lock, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lock.Rollback(context.Background()) })
	if _, err := lock.Exec(t.Context(), "LOCK TABLE jobqueue.jobs IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	statsDone := make(chan error, 1)
	go func() {
		_, err := rb.client.GetStats(context.Background(), &jobqueuev1.GetStatsRequest{Queue: "q"})
		statsDone <- err
	}()
	waitFor(t, "GetStats to block on the lock", 10*time.Second, func() bool {
		var n int
		err := pool.QueryRow(t.Context(),
			`SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&n)
		return err == nil && n >= 1
	})

	// 3. Shut down.
	start := time.Now()
	rb.stop()

	select {
	case r := <-polled:
		if r.err != nil || len(r.resp.GetJobs()) != 0 {
			t.Errorf("the long poll ended with %v, %v; want an empty response", r.resp, r.err)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Errorf("the long poll ended %v after the shutdown began, want within 1s", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the long poll was not answered after the shutdown began")
	}

	// The blocked call holds the grace period open, and readiness has failed
	// for all of it while liveness has not.
	if code, body, err := rb.httpGet("/readyz"); err != nil || code != http.StatusServiceUnavailable || body != "draining" {
		t.Errorf("/readyz during the grace period = %d %q, %v; want 503 \"draining\"", code, body, err)
	}
	if code, _, err := rb.httpGet("/healthz"); err != nil || code != http.StatusOK {
		t.Errorf("/healthz during the grace period = %d, %v; want 200", code, err)
	}

	select {
	case err := <-rb.done:
		if err != nil {
			t.Errorf("Run returned %v, want nil", err)
		}
		rb.done <- nil // for the cleanup
		if elapsed := time.Since(start); elapsed > grace+time.Second {
			t.Errorf("Run returned %v after the shutdown began, want within %v", elapsed, grace+time.Second)
		} else if elapsed < grace/2 {
			t.Errorf("Run returned after %v: it did not wait for the blocked call", elapsed)
		}
	case <-time.After(grace + 5*time.Second):
		t.Fatal("Run did not return")
	}
	if err := <-statsDone; err == nil {
		t.Error("the blocked GetStats succeeded; the grace period should have cut it off")
	}
}
