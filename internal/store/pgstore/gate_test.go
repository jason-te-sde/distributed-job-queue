package pgstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestDefaultMaxNotifying(t *testing.T) {
	// The reserve is max(2, poolMax/4), and at least one connection is left to
	// notifying calls, or Enqueue could never run. So from a pool of 3 up the
	// default leaves at least two connections to the calls that do not notify, a
	// pool of 2 leaves one, and a pool of 1 leaves none.
	for _, tc := range []struct{ pool, want, left int }{
		{-3, 1, 0}, {0, 1, 0}, {1, 1, 0}, {2, 1, 1}, {3, 1, 2}, {4, 2, 2}, {5, 3, 2}, {8, 6, 2}, {10, 8, 2}, {12, 9, 3},
		{16, 12, 4}, {20, 15, 5}, {100, 75, 25},
	} {
		got := defaultMaxNotifying(tc.pool)
		if got != tc.want {
			t.Errorf("defaultMaxNotifying(%d) = %d, want %d", tc.pool, got, tc.want)
		}
		if tc.pool >= 1 && tc.pool-got != tc.left {
			t.Errorf("a pool of %d leaves %d connections to the calls that do not notify, want %d", tc.pool, tc.pool-got, tc.left)
		}
	}
	for pool := 3; pool <= 200; pool++ {
		if left := pool - defaultMaxNotifying(pool); left < 2 {
			t.Errorf("a pool of %d leaves %d connections to the calls that do not notify, want at least 2 from a pool of 3 up", pool, left)
		}
	}
}

func TestNotifyingCeilingLeavesOneConnectionWhereThePoolCanSpareIt(t *testing.T) {
	for _, tc := range []struct{ pool, want int }{{-1, 1}, {0, 1}, {1, 1}, {2, 1}, {3, 2}, {20, 19}, {100, 99}} {
		if got := notifyingCeiling(tc.pool); got != tc.want {
			t.Errorf("notifyingCeiling(%d) = %d, want %d", tc.pool, got, tc.want)
		}
	}
	// The default is never above the ceiling, so the two rules agree.
	for pool := 1; pool <= 200; pool++ {
		if d, c := defaultMaxNotifying(pool), notifyingCeiling(pool); d > c {
			t.Errorf("pool %d: the default %d is above the ceiling %d", pool, d, c)
		}
	}
}

// poolOf returns a pool of max connections that has not connected to anything:
// a pool opens its connections when they are first needed.
func poolOf(t *testing.T, max string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(t.Context(), "postgres://nobody@127.0.0.1:1/none?pool_max_conns="+max)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestNewSizesTheGateFromThePoolOrTheOption(t *testing.T) {
	for _, tc := range []struct {
		name string
		pool string
		opts []Option
		want int
	}{
		{"default from a pool of 20", "20", nil, 15},
		{"default from a pool of 8", "8", nil, 6},
		{"an explicit size", "20", []Option{WithMaxNotifying(4)}, 4},
		{"the most that leaves a connection to the others", "20", []Option{WithMaxNotifying(19)}, 19},
		{"the whole pool is lowered by one, or the bound would be off", "20", []Option{WithMaxNotifying(20)}, 19},
		{"above the pool is lowered the same", "20", []Option{WithMaxNotifying(25)}, 19},
		{"zero means the default", "20", []Option{WithMaxNotifying(0)}, 15},
		{"a negative size means the default", "20", []Option{WithMaxNotifying(-1)}, 15},
		{"a pool of 2 keeps one connection back", "2", []Option{WithMaxNotifying(2)}, 1},
		{"a pool of 1 has only the one", "1", []Option{WithMaxNotifying(1)}, 1},
		{"a pool of 1 with a larger value", "1", []Option{WithMaxNotifying(9)}, 1},
		{"a pool of 1 by default", "1", nil, 1},
	} {
		if got := New(poolOf(t, tc.pool), tc.opts...).MaxNotifying(); got != tc.want {
			t.Errorf("%s: the bound is %d, want %d", tc.name, got, tc.want)
		}
	}
}

func TestGateBoundsTheHolders(t *testing.T) {
	g := newGate(2)
	var held []*permit
	for range 2 {
		p, err := g.acquire(t.Context())
		if err != nil {
			t.Fatalf("acquire with room: %v", err)
		}
		held = append(held, p)
	}
	// Full: a third caller waits.
	got := make(chan *permit, 1)
	go func() {
		p, err := g.acquire(t.Context())
		if err != nil {
			t.Errorf("the waiting acquire failed: %v", err)
		}
		got <- p
	}()
	select {
	case <-got:
		t.Fatal("a third acquire returned while two holders were in")
	case <-time.After(50 * time.Millisecond):
	}
	held[0].release()
	select {
	case p := <-got:
		held[0] = p
	case <-time.After(5 * time.Second):
		t.Fatal("the waiting acquire did not return after a release")
	}
	if n := g.held(); n != 2 {
		t.Errorf("%d holders after the hand-over, want 2", n)
	}
	held[0].release()
	held[1].release()
	if n := g.held(); n != 0 {
		t.Errorf("%d holders after every release, want 0", n)
	}
}

func TestGateAcquireHonorsTheContext(t *testing.T) {
	g := newGate(1)

	// An ended context is refused even when there is room, and takes nothing.
	ended, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := g.acquire(ended); !errors.Is(err, context.Canceled) {
		t.Errorf("acquire with a cancelled context = %v, want context.Canceled", err)
	}
	if n := g.held(); n != 0 {
		t.Fatalf("a refused acquire left %d holders", n)
	}

	// Full: a context whose deadline passes while it waits ends the wait with
	// its own error, and the slot count is not disturbed.
	first, err := g.acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := g.acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("acquire past its deadline = %v, want context.DeadlineExceeded", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("the wait took %v for a 20ms deadline", took)
	}
	// And a cancel while waiting.
	waiting, cancelWaiting := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := g.acquire(waiting)
		done <- err
	}()
	cancelWaiting()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("acquire cancelled while it waited = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a cancelled waiter did not return")
	}
	if n := g.held(); n != 1 {
		t.Errorf("%d holders after two abandoned waits, want the one that was in", n)
	}
	first.release()
	again, err := g.acquire(t.Context())
	if err != nil {
		t.Fatalf("acquire after the abandoned waits and a release: %v", err)
	}
	again.release()
}

// mustPanicWith runs f and fails unless it panics with a message that holds
// want.
func mustPanicWith(t *testing.T, want string, f func()) {
	t.Helper()
	defer func() {
		t.Helper()
		r := recover()
		if r == nil {
			t.Fatalf("did not panic, want a panic naming %q", want)
		}
		if msg := fmt.Sprint(r); !strings.Contains(msg, want) {
			t.Fatalf("panic = %q, want it to name %q", msg, want)
		}
	}()
	f()
}

func TestPermitReleasedTwiceIsAMisuseThatPanics(t *testing.T) {
	// A second release of one permit must not hand back a permit that another
	// holder owns, whether or not the gate has anyone else in it: the bound would
	// silently stop holding.
	for name, others := range map[string]int{"alone in the gate": 0, "with another holder in": 1} {
		g := newGate(2)
		var bystanders []*permit
		for range others {
			p, err := g.acquire(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			bystanders = append(bystanders, p)
		}
		p, err := g.acquire(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		p.release()
		mustPanicWith(t, "released twice", p.release)
		if n := g.held(); n != others {
			t.Errorf("%s: %d permits held after the refused second release, want the %d of the others", name, n, others)
		}
		for _, b := range bystanders {
			b.release()
		}
	}
}

func TestGateGivingBackWithNothingHeldPanicsInsteadOfBlocking(t *testing.T) {
	g := newGate(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		mustPanicWith(t, "no permit", g.give)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("giving back a permit that nobody holds blocked")
	}
}
