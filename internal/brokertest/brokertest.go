// Package brokertest starts real brokers, in this process, over a database
// that pgtest made: the server side of an end-to-end test.
//
// The brokers listen on loopback ports chosen by the system and demand no
// tokens, which Config.Validate allows for a loopback address and which makes
// every caller an administrator. Their loops run on short intervals so that a
// test spends milliseconds, not seconds, waiting for a reap or a poll.
//
// It lives beside pgtest and not in it because the pg tests of internal/broker
// import pgtest, so pgtest importing broker would be an import cycle. Like
// pgtest it imports testing, so only test files may import it; the
// architecture test enforces that.
package brokertest

import (
	"context"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jason-te-sde/distributed-job-queue/internal/broker"
	"github.com/jason-te-sde/distributed-job-queue/internal/pgtest"
)

const (
	// loopInterval is how often the brokers reap, poll and look for active
	// tenants. The server's defaults are a second each.
	loopInterval = 100 * time.Millisecond
	// shutdownGrace bounds how long stopping a broker waits for its running
	// calls, so a test that stops one is not held up by the server's default
	// of 25s.
	shutdownGrace = 5 * time.Second
	// stopTimeout bounds how long Stop waits for Run to return after its
	// context ends: the grace period, with room for the rest of the shutdown.
	stopTimeout = 15 * time.Second
)

// running is a broker whose Run is going in the background.
type running struct {
	cancel context.CancelFunc
	done   chan error
}

// started maps each broker that Start returned and Stop has not yet stopped to
// its running state. It is shared by every test of the binary, which is safe
// because a broker is a distinct pointer.
var started sync.Map // *broker.Broker -> *running

// errorLogs maps each broker that Start returned to the lines it logged at
// Error level. Unlike started it keeps a broker after Stop, so a test can ask
// about a broker that it replaced.
var errorLogs sync.Map // *broker.Broker -> *errorLog

// StartBrokers starts n brokers over the database of pool, which must already
// be migrated, and returns them once they listen. Each runs until the test
// ends or until Stop stops it. mutate, if not nil, changes the configuration
// of every broker before it is built; the configuration starts from the
// server's defaults with loopback addresses on port 0, loops that run every
// 100ms and a shutdown grace period of 5s. Any failure ends the test.
func StartBrokers(t testing.TB, pool *pgxpool.Pool, n int, mutate func(*broker.Config)) []*broker.Broker {
	t.Helper()
	brokers := make([]*broker.Broker, n)
	for i := range brokers {
		brokers[i] = Start(t, pool, mutate)
	}
	return brokers
}

// Start starts one broker as StartBrokers does. A test that restarts brokers
// calls it for the replacement.
func Start(t testing.TB, pool *pgxpool.Pool, mutate func(*broker.Config)) *broker.Broker {
	t.Helper()
	cfg, err := broker.ParseFlags([]string{
		"--grpc-addr=127.0.0.1:0",
		"--http-addr=127.0.0.1:0",
		"--database-url=" + databaseURL(t, pool),
		"--reap-interval=" + loopInterval.String(),
		"--poll-interval=" + loopInterval.String(),
		"--activity-interval=" + loopInterval.String(),
		"--shutdown-grace=" + shutdownGrace.String(),
	}, nil)
	if err != nil {
		t.Fatalf("brokertest: parse the flags: %v", err)
	}
	if mutate != nil {
		mutate(&cfg)
	}

	w := &testWriter{t: t}
	// Registered before Stop's cleanup, so it runs after it: the lines that a
	// shutdown logs still reach the test log.
	t.Cleanup(w.close)
	errs := &errorLog{}
	log := slog.New(recordingHandler{
		Handler: slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelWarn}),
		errs:    errs,
	})
	ctx, cancel := context.WithCancel(context.Background())
	b, err := broker.New(ctx, cfg, log)
	if err != nil {
		cancel()
		t.Fatalf("brokertest: New: %v", err)
	}
	r := &running{cancel: cancel, done: make(chan error, 1)}
	started.Store(b, r)
	errorLogs.Store(b, errs)
	go func() { r.done <- b.Run(ctx) }()
	t.Cleanup(func() { Stop(t, b) })
	return b
}

// Stop ends the Run of b, which is how a deployment shuts a broker down, and
// waits until it has returned. A broker that was already stopped is left
// alone, so a test may stop one itself and let the cleanup find it gone. It
// fails the test if Run returns an error or does not return in time.
func Stop(t testing.TB, b *broker.Broker) {
	t.Helper()
	v, ok := started.LoadAndDelete(b)
	if !ok {
		return
	}
	r := v.(*running)
	r.cancel()
	select {
	case err := <-r.done:
		if err != nil {
			t.Errorf("brokertest: Run returned %v", err)
		}
	case <-time.After(stopTimeout):
		t.Errorf("brokertest: Run did not return within %v of its context ending", stopTimeout)
	}
}

// ErrorLines returns what b has logged at Error level since it started, one
// string per record. A broker logs at that level only for something that went
// wrong inside it, such as a store call that failed for a reason other than the
// caller's, so a healthy run has none. A call that a client repeats and then
// succeeds still leaves its line here, which is the point: the client's retry
// hides the failure from every other check. It returns nil for a broker that
// Start did not start.
func ErrorLines(b *broker.Broker) []string {
	v, ok := errorLogs.Load(b)
	if !ok {
		return nil
	}
	return v.(*errorLog).lines()
}

// errorLog collects the Error-level records of one broker.
type errorLog struct {
	mu   sync.Mutex
	list []string
}

func (e *errorLog) add(line string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.list = append(e.list, line)
}

func (e *errorLog) lines() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.list...)
}

// recordingHandler passes every record to the handler it wraps and keeps those
// at Error level and above. It wraps the handlers that With and WithGroup make
// too, because the broker logs through a logger it has added attributes to.
type recordingHandler struct {
	slog.Handler
	errs *errorLog
}

func (h recordingHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Level >= slog.LevelError {
		var line strings.Builder
		line.WriteString(r.Message)
		r.Attrs(func(a slog.Attr) bool {
			line.WriteString(" " + a.String())
			return true
		})
		h.errs.add(line.String())
	}
	return h.Handler.Handle(ctx, r)
}

func (h recordingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return recordingHandler{Handler: h.Handler.WithAttrs(attrs), errs: h.errs}
}

func (h recordingHandler) WithGroup(name string) slog.Handler {
	return recordingHandler{Handler: h.Handler.WithGroup(name), errs: h.errs}
}

// databaseURL is the URL of the database of pool on the server that
// JOBQUEUE_TEST_DSN names.
func databaseURL(t testing.TB, pool *pgxpool.Pool) string {
	t.Helper()
	u, err := url.Parse(pgtest.DSN(t))
	if err != nil {
		t.Fatalf("brokertest: parse JOBQUEUE_TEST_DSN: %v", err)
	}
	u.Path = "/" + pool.Config().ConnConfig.Database
	return u.String()
}

// testWriter sends the log lines of a broker to the test log, which go test
// shows only when the test fails. A broker that outlives its test, because
// Stop gave up on it, may log when the test log is closed and writing to it
// would panic, so lines are dropped once close has been called.
type testWriter struct {
	t testing.TB

	mu     sync.Mutex
	closed bool
}

func (w *testWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.closed {
		w.t.Logf("broker: %s", p)
	}
	return len(p), nil
}

func (w *testWriter) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
}
