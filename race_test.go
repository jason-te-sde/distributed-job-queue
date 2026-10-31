//go:build race

package jobqueue

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/lease"
)

// TestLoopRejectsSecondGoroutine pins the rule that the lease tracker has one
// owner goroutine: in a race build the second goroutine to enter the worker
// loop panics, naming the tracker.
func TestLoopRejectsSecondGoroutine(t *testing.T) {
	l := &workerLoop{}
	l.enter() // this goroutine owns the loop

	recovered := make(chan any, 1)
	go func() {
		defer func() { recovered <- recover() }()
		l.enter()
	}()
	got := <-recovered
	const want = "lease tracker entered from two goroutines"
	if s, _ := got.(string); s != want {
		t.Fatalf("second goroutine's enter() panicked with %v, want %q", got, want)
	}
}

func TestLoopAcceptsTheSameGoroutineAgain(t *testing.T) {
	l := &workerLoop{}
	l.enter()
	l.enter() // every iteration of the loop enters again
}

// idleConn is a broker that never answers: a Lease call waits until its
// context ends. It says when the first call has arrived, which is after the
// loop has entered.
type idleConn struct {
	called chan struct{}
	once   sync.Once
}

func (c *idleConn) Invoke(ctx context.Context, _ string, _, _ any, _ ...grpc.CallOption) error {
	c.once.Do(func() { close(c.called) })
	<-ctx.Done()
	return status.FromContextError(ctx.Err()).Err()
}

func (c *idleConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, errors.New("idleConn has no streams")
}

// TestLoopEntriesRejectASecondGoroutine runs a loop and then enters it from a
// second goroutine by each way in: the loop itself, the handling of an event,
// the timer, the drain, and the carrying out of effects. Each must panic,
// naming the tracker, before it touches anything. Taking out the check of any
// of them fails the test for it.
func TestLoopEntriesRejectASecondGoroutine(t *testing.T) {
	tracker, err := lease.New(lease.Config{}.WithDefaults(), 1)
	if err != nil {
		t.Fatal(err)
	}
	conn := &idleConn{called: make(chan struct{})}
	l := newWorkerLoop(conn, WorkerOptions{Queue: "q", WorkerID: "w", Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, noopHandler, tracker)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.run(ctx) }()
	<-conn.called // the loop goroutine has entered and sent its first request

	// A second run, if it were let in, must end by itself and not hang the test.
	ended, stop := context.WithCancel(context.Background())
	stop()
	for name, enter := range map[string]func(){
		"run":     func() { _ = l.run(ended) },
		"handle":  func() { _, _ = l.handle(releaseAnswer{}) },
		"tick":    func() { l.tick() },
		"drain":   func() { l.drain() },
		"perform": func() { _ = l.perform(nil) },
	} {
		t.Run(name, func(t *testing.T) {
			recovered := make(chan any, 1)
			go func() {
				defer func() { recovered <- recover() }()
				enter()
			}()
			got := <-recovered
			const want = "lease tracker entered from two goroutines"
			if s, _ := got.(string); s != want {
				t.Fatalf("entering from a second goroutine panicked with %v, want %q", got, want)
			}
		})
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not end after its context did")
	}
}

func noopHandler(context.Context, *Job) error { return nil }
