//go:build pg

package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

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

// httpAddrFrom finds the HTTP address in the "broker started" line of the log.
func httpAddrFrom(logs string) (string, bool) {
	for _, line := range strings.Split(logs, "\n") {
		var rec struct {
			Msg  string `json:"msg"`
			HTTP string `json:"http"`
		}
		if json.Unmarshal([]byte(line), &rec) == nil && rec.Msg == "broker started" {
			return rec.HTTP, true
		}
	}
	return "", false
}

func TestPGCommandServesAndStopsOnSIGTERM(t *testing.T) {
	pool := pgtest.NewDB(t)
	u, err := url.Parse(pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + pool.Config().ConnConfig.Database

	var stdout, stderr syncBuffer
	status := make(chan int, 1)
	go func() {
		status <- run([]string{
			"--database-url=" + u.String(), "--grpc-addr=127.0.0.1:0", "--http-addr=127.0.0.1:0",
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
