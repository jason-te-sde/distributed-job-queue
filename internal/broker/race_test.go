//go:build race

package broker

import (
	"context"
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

// TestEveryLoopEntryRejectsASecondGoroutine runs the loop and then calls each
// of its entry points from another goroutine. The loop's goroutine is the
// owner after its first event, so each call must panic with the documented
// message before it touches anything the loop owns. A handler that does not
// enter would instead change the core from two goroutines.
func TestEveryLoopEntryRejectsASecondGoroutine(t *testing.T) {
	d := newIdleDispatcher()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run returned %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("Run did not return")
		}
	})
	// An answer from the loop means its goroutine has entered and owns it.
	if _, err := d.Lease(t.Context(), LeaseCall{Queue: "q", Max: 1, Wait: 0, TTL: 30 * time.Second, Worker: "w"}); err != nil {
		t.Fatal(err)
	}

	const want = "dispatch core entered from two goroutines"
	for name, call := range map[string]func(){
		"onEvent":        func() { d.onEvent(evSettled{}) },
		"onWake":         d.onWake,
		"onTimer":        d.onTimer,
		"onStop":         d.onStop,
		"onActivityTick": d.onActivityTick,
	} {
		t.Run(name, func(t *testing.T) {
			recovered := make(chan any, 1)
			go func() {
				defer func() { recovered <- recover() }()
				call()
			}()
			if s, _ := (<-recovered).(string); s != want {
				t.Errorf("%s from a second goroutine did not panic with %q", name, want)
			}
		})
	}
}
