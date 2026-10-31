//go:build race

package jobqueue

import "testing"

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
