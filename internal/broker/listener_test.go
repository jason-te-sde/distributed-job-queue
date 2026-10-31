package broker

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/jason-te-sde/distributed-job-queue/internal/store/memstore"
)

func TestReconnectDelayIsFullJitterUpToTheCap(t *testing.T) {
	// The ceiling doubles from 100ms to 5s, and the delay is a uniform draw
	// below it.
	for _, tc := range []struct {
		attempt int
		u       float64
		want    time.Duration
	}{
		{0, 0, 0},
		{0, 0.5, 50 * time.Millisecond},
		{1, 0.5, 100 * time.Millisecond},
		{3, 0.5, 400 * time.Millisecond},
		{6, 0.5, 2500 * time.Millisecond},  // 6.4s would be the ceiling, capped to 5s
		{60, 0.5, 2500 * time.Millisecond}, // no overflow for a long outage
		{1 << 30, 0.5, 2500 * time.Millisecond},
	} {
		if got := reconnectDelay(tc.attempt, tc.u); got != tc.want {
			t.Errorf("reconnectDelay(%d, %v) = %v, want %v", tc.attempt, tc.u, got, tc.want)
		}
	}
	if got := reconnectDelay(10, 0.999999); got >= 5*time.Second {
		t.Errorf("reconnectDelay(10, 0.999999) = %v, want below the 5s cap", got)
	}
}

// silentPostgres is a server that completes the PostgreSQL startup and then
// reads and ignores everything: a connection that accepts and says nothing. It
// returns the URL to reach it.
func silentPostgres(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
			go func() {
				var size [4]byte // the startup message: its length, then the rest
				if _, err := io.ReadFull(c, size[:]); err != nil {
					return
				}
				if _, err := io.ReadFull(c, make([]byte, binary.BigEndian.Uint32(size[:])-4)); err != nil {
					return
				}
				// AuthenticationOk, then ReadyForQuery.
				_, _ = c.Write([]byte{'R', 0, 0, 0, 8, 0, 0, 0, 0, 'Z', 0, 0, 0, 5, 'I'})
				_, _ = io.Copy(io.Discard, c)
			}()
		}
	}()
	t.Cleanup(func() {
		_ = lis.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	})
	return fmt.Sprintf("postgres://u@%s/db?sslmode=disable", lis.Addr())
}

func TestListenerGivesUpOnALISTENThatIsNeverAnswered(t *testing.T) {
	// The connection is accepted and then silent. Without a bound on the LISTEN
	// the listener would wait on it for ever, deaf, with no reconnect.
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	logs := &syncBuffer{}
	log := slog.New(slog.NewTextHandler(logs, nil))
	d := NewDispatcher(memstore.New(memstore.Options{Clock: time.Now}), DispatcherConfig{Scheduler: "drr"}, m, log)
	l := NewListener(silentPostgres(t), d, m, log)
	l.listenTimeout = 100 * time.Millisecond
	l.backoff = func(int) time.Duration { return time.Hour } // one session is all this test needs

	done := make(chan error, 1)
	ctx := t.Context()
	go func() { done <- l.Run(ctx) }()
	waitFor(t, "the reconnect counter", quickly, func() bool {
		return metricValue(t, reg, "jobqueue_notify_reconnects_total", nil) == 1
	})
	if n := l.listens.Load(); n != 0 {
		t.Errorf("the listener reports %d LISTENs on a server that never answered one", n)
	}
	if s := logs.String(); !strings.Contains(s, "listen:") {
		t.Errorf("the log does not say that LISTEN failed: %s", s)
	}
}
