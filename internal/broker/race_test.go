//go:build race

package broker

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/jason-te-sde/distributed-job-queue/internal/store/memstore"
)

func newIdleDispatcher() *Dispatcher {
	st := memstore.New(memstore.Options{Clock: time.Now})
	return NewDispatcher(st, DispatcherConfig{Scheduler: "drr"}, NewMetrics(prometheus.NewRegistry()),
		slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// TestLoopRejectsSecondGoroutine pins the rule that the dispatch core has one
// owner goroutine: in a race build the second goroutine to enter the loop
// panics, naming the core.
func TestLoopRejectsSecondGoroutine(t *testing.T) {
	d := newIdleDispatcher()
	d.enter() // this goroutine owns the loop

	recovered := make(chan any, 1)
	go func() {
		defer func() { recovered <- recover() }()
		d.enter()
	}()
	got := <-recovered
	const want = "dispatch core entered from two goroutines"
	if s, _ := got.(string); s != want {
		t.Fatalf("second goroutine's enter() panicked with %v, want %q", got, want)
	}
}

func TestLoopAcceptsTheSameGoroutineAgain(t *testing.T) {
	d := newIdleDispatcher()
	d.enter()
	d.enter() // every iteration of the loop enters again
}
