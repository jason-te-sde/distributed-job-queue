//go:build pg

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	jobqueuev1 "github.com/jason-te-sde/distributed-job-queue/internal/gen/jobqueue/v1"
	"github.com/jason-te-sde/distributed-job-queue/internal/pgtest"
)

// syncBuffer is a writer that the command's goroutine and the test share.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// startedAddrs finds the gRPC and HTTP addresses in the "broker started" line
// of the log.
func startedAddrs(logs string) (grpcAddr, httpAddr string, ok bool) {
	for _, line := range strings.Split(logs, "\n") {
		var rec struct {
			Msg  string `json:"msg"`
			GRPC string `json:"grpc"`
			HTTP string `json:"http"`
		}
		if json.Unmarshal([]byte(line), &rec) == nil && rec.Msg == "broker started" {
			return rec.GRPC, rec.HTTP, true
		}
	}
	return "", "", false
}

// httpAddrFrom finds the HTTP address in the "broker started" line of the log.
func httpAddrFrom(logs string) (string, bool) {
	_, addr, ok := startedAddrs(logs)
	return addr, ok
}

// dbURL returns the URL of the private database of a pgtest pool.
func dbURL(t testing.TB, pool *pgxpool.Pool) string {
	t.Helper()
	u, err := url.Parse(pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + pool.Config().ConnConfig.Database
	return u.String()
}

func TestPGCommandServesAndStopsOnSIGTERM(t *testing.T) {
	pool := pgtest.NewDB(t)

	var stdout, stderr syncBuffer
	status := make(chan int, 1)
	go func() {
		status <- run([]string{
			"--database-url=" + dbURL(t, pool), "--grpc-addr=127.0.0.1:0", "--http-addr=127.0.0.1:0",
			"--shutdown-grace=2s", "--log-format=json",
		}, func(string) string { return "" }, &stdout, &stderr)
	}()

	var addr string
	deadline := time.Now().Add(10 * time.Second)
	for {
		var ok bool
		if addr, ok = httpAddrFrom(stderr.String()); ok {
			break
		}
		select {
		case s := <-status:
			t.Fatalf("the command exited with %d before it started: %s", s, stderr.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("the command did not start within 10s: %s", stderr.String())
		}
		time.Sleep(5 * time.Millisecond)
	}

	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if want := `jobqueue_build_info{go_version="`; !strings.Contains(string(body), want) || !strings.Contains(string(body), `version="`+version+`"`) {
		t.Errorf("/metrics does not report the build version %q", version)
	}

	// The signal handler is installed before the broker starts, so the process
	// is not killed by this.
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case s := <-status:
		if s != 0 {
			t.Errorf("the command exited with %d after SIGTERM, want 0: %s", s, stderr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the command did not exit within 10s of SIGTERM")
	}
	if stdout.String() != "" {
		t.Errorf("the command wrote to stdout: %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "broker stopped") {
		t.Errorf("the log does not say the broker stopped: %s", stderr.String())
	}
}

func TestPGCommandSecondSignalEndsTheProcessDuringTheGracePeriod(t *testing.T) {
	// The first SIGTERM starts the graceful shutdown, here held open for 30s by
	// a call that is blocked on a table lock. A second one must end the process
	// at once: the command hands the signals back after the first. This runs
	// the real binary, because a signal cannot be sent twice to a test.
	pool := pgtest.NewDB(t)
	bin := filepath.Join(t.TempDir(), "jobqueue-server")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build the command: %v\n%s", err, out)
	}
	cmd := exec.Command(bin,
		"--database-url="+dbURL(t, pool), "--grpc-addr=127.0.0.1:0", "--http-addr=127.0.0.1:0",
		"--shutdown-grace=30s", "--log-format=json",
		// Nothing but the test may touch the table while it is locked.
		"--poll-interval=1h", "--activity-interval=1h", "--reap-interval=1h", "--prune-interval=1h")
	var stderr syncBuffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill() // a no-op once it has exited
		select {
		case err := <-exited:
			_ = err // the error of a killed process is the point of the test
		case <-time.After(10 * time.Second):
		}
	})

	var grpcAddr string
	deadline := time.Now().Add(15 * time.Second)
	for {
		var ok bool
		if grpcAddr, _, ok = startedAddrs(stderr.String()); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the command did not start within 15s: %s", stderr.String())
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Hold the grace period open: a call that blocks on a lock on the table.
	lock, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lock.Rollback(context.Background()) })
	if _, err := lock.Exec(t.Context(), "LOCK TABLE jobqueue.jobs IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient("passthrough:///"+grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	go func() {
		_, _ = jobqueuev1.NewJobQueueServiceClient(conn).GetStats(context.Background(), &jobqueuev1.GetStatsRequest{Queue: "q"})
	}()
	waitUntil(t, "GetStats to block on the lock", 10*time.Second, func() bool {
		var n int
		err := pool.QueryRow(t.Context(),
			`SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&n)
		return err == nil && n >= 1
	})

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "the shutdown to begin", 10*time.Second, func() bool { return strings.Contains(stderr.String(), "shutting down") })
	select {
	case err := <-exited:
		t.Fatalf("the process ended after the first SIGTERM, which the held call should have kept waiting for: %v\n%s", err, stderr.String())
	case <-time.After(300 * time.Millisecond):
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-exited:
		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			t.Fatalf("the process ended with %v, want it killed by the second signal", err)
		}
		if ws, ok := exit.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != syscall.SIGTERM {
			t.Errorf("the process ended with %v, want it killed by SIGTERM", exit)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("a second SIGTERM did not end the process within 5s, in a grace period of 30s:\n%s", stderr.String())
	}
}

func waitUntil(t testing.TB, what string, within time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", within, what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
