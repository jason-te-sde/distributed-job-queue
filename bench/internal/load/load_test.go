package load

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestScheduleIntendedTimes(t *testing.T) {
	start := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		rate float64
		k    int
		want time.Duration
	}{
		{1000, 0, 0},
		{1000, 1, time.Millisecond},
		{1000, 1500, 1500 * time.Millisecond},
		{200, 7, 35 * time.Millisecond},
		{3, 1, 333333333 * time.Nanosecond},
		{3, 3, time.Second},
		{0.5, 1, 2 * time.Second},
		{100000, 1, 10 * time.Microsecond},
	} {
		s := Schedule{Start: start, Rate: tc.rate}
		if got := s.Intended(tc.k).Sub(start); got != tc.want {
			t.Errorf("rate %v: Intended(%d) = start+%v, want start+%v", tc.rate, tc.k, got, tc.want)
		}
	}
}

func TestScheduleIntendedTimesDoNotDrift(t *testing.T) {
	// Each time is computed from k, not by adding a rounded interval k times,
	// so a million jobs at a rate with no exact interval end where they should.
	s := Schedule{Start: time.Unix(0, 0), Rate: 3}
	if got := s.Intended(3_000_000).Sub(s.Start); got != 1_000_000*time.Second {
		t.Errorf("Intended(3000000) = start+%v, want start+1000000s", got)
	}
}

func TestIntendedPanicsOnABadRate(t *testing.T) {
	for _, rate := range []float64{0, -1} {
		func() {
			defer func() {
				r := recover()
				msg, _ := r.(string)
				if msg == "" || !contains(msg, "Intended") {
					t.Errorf("rate %v: recovered %v, want a message that names Intended", rate, r)
				}
			}()
			Schedule{Start: time.Now(), Rate: rate}.Intended(1)
		}()
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestRunRejectsABadSchedule(t *testing.T) {
	send := func(context.Context, int, time.Time) error { t.Error("send was called"); return nil }
	for _, rate := range []float64{0, -5} {
		if _, _, err := Run(t.Context(), Schedule{Start: time.Now(), Rate: rate}, 3, send); err == nil {
			t.Errorf("Run with rate %v returned no error", rate)
		}
	}
	if _, _, err := Run(t.Context(), Schedule{Start: time.Now(), Rate: 10}, -1, send); err == nil {
		t.Error("Run with n = -1 returned no error")
	}
}

func TestRunSendsEveryJobOnceWithItsIntendedTime(t *testing.T) {
	s := Schedule{Start: time.Now().Add(5 * time.Millisecond), Rate: 2000}
	const n = 200
	var mu sync.Mutex
	seen := map[int]time.Time{}
	sent, _, err := Run(t.Context(), s, n, func(_ context.Context, k int, intended time.Time) error {
		mu.Lock()
		defer mu.Unlock()
		if _, dup := seen[k]; dup {
			t.Errorf("job %d was sent twice", k)
		}
		seen[k] = intended
		return nil
	})
	if err != nil || sent != n {
		t.Fatalf("Run = %d sent, %v; want %d, nil", sent, err, n)
	}
	for k := range n {
		if got, ok := seen[k]; !ok || !got.Equal(s.Intended(k)) {
			t.Fatalf("job %d got intended %v (sent: %v), want %v", k, got, ok, s.Intended(k))
		}
	}
}

func TestRunDoesNotSendEarly(t *testing.T) {
	s := Schedule{Start: time.Now().Add(20 * time.Millisecond), Rate: 100}
	var mu sync.Mutex
	var early []int
	_, _, err := Run(t.Context(), s, 20, func(_ context.Context, k int, intended time.Time) error {
		if time.Now().Before(intended) {
			mu.Lock()
			early = append(early, k)
			mu.Unlock()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(early) > 0 {
		t.Errorf("jobs %v were sent before their intended time", early)
	}
}

func TestRunReportsLateSendsAndKeepsTheirIntendedTime(t *testing.T) {
	// The schedule began a second ago, so every job is already overdue when
	// Run starts. The sends must be counted late, and must still carry the
	// time they were meant for, which is how a latency measured from it shows
	// the second that a generator that stalled would hide.
	s := Schedule{Start: time.Now().Add(-time.Second), Rate: 50}
	const n = 20
	var mu sync.Mutex
	var overdue []time.Duration
	sent, late, err := Run(t.Context(), s, n, func(_ context.Context, k int, intended time.Time) error {
		mu.Lock()
		overdue = append(overdue, time.Since(intended))
		mu.Unlock()
		return nil
	})
	if err != nil || sent != n {
		t.Fatalf("Run = %d sent, %v; want %d, nil", sent, err, n)
	}
	if late != n {
		t.Errorf("late = %d, want all %d jobs counted late", late, n)
	}
	for _, d := range overdue {
		if d < 600*time.Millisecond {
			t.Fatalf("a job saw itself %v after its intended time, want at least 600ms: its intended time was rewritten", d)
		}
	}
}

// stalledConsumer is the system under test of the open-loop test: it takes one
// request at a time, and its first request waits for stall before anything
// is taken at all.
type stalledConsumer struct {
	in chan consumerReq

	mu        sync.Mutex
	latencies []time.Duration // time from the intended instant to being taken
}

type consumerReq struct {
	intended time.Time
	taken    chan struct{}
}

func newStalledConsumer(ctx context.Context, stall time.Duration) *stalledConsumer {
	c := &stalledConsumer{in: make(chan consumerReq)}
	go func() {
		timer := time.NewTimer(stall)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return
		}
		for {
			select {
			case r := <-c.in:
				c.mu.Lock()
				c.latencies = append(c.latencies, time.Since(r.intended))
				c.mu.Unlock()
				close(r.taken)
			case <-ctx.Done():
				return
			}
		}
	}()
	return c
}

// send is a call that returns only once the consumer has taken the request,
// as an RPC does.
func (c *stalledConsumer) send(ctx context.Context, _ int, intended time.Time) error {
	r := consumerReq{intended: intended, taken: make(chan struct{})}
	select {
	case c.in <- r:
	case <-ctx.Done():
		return ctx.Err()
	}
	<-r.taken
	return nil
}

func TestOpenLoopMeasuresFromIntendedTime(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	const stall = 500 * time.Millisecond
	consumer := newStalledConsumer(ctx, stall)

	// 200 jobs a second for a second, with the consumer away for the first half.
	s := Schedule{Start: time.Now(), Rate: 200}
	const n = 200
	sent, _, err := Run(ctx, s, n, consumer.send)
	if err != nil || sent != n {
		t.Fatalf("Run = %d sent, %v; want %d, nil", sent, err, n)
	}

	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if len(consumer.latencies) != n {
		t.Fatalf("the consumer took %d requests, want %d", len(consumer.latencies), n)
	}
	sort.Slice(consumer.latencies, func(i, j int) bool { return consumer.latencies[i] < consumer.latencies[j] })
	if max := consumer.latencies[n-1]; max < stall-50*time.Millisecond {
		t.Errorf("the slowest job waited %v, want about the stall of %v: the job meant for the start of the stall waited all of it", max, stall)
	}
	// The jobs meant for the first quarter second of the stall wait at least
	// another quarter second. A loop that waited for each reply before it
	// timed the next send would report about a millisecond for all but one of
	// them.
	slow := 0
	for _, d := range consumer.latencies {
		if d >= 240*time.Millisecond {
			slow++
		}
	}
	if slow < 45 {
		t.Errorf("%d jobs waited at least 240ms, want at least 45 of the 50 meant for the first quarter second of the stall", slow)
	}
}

func TestRunReturnsTheFirstSendError(t *testing.T) {
	boom := errors.New("boom")
	s := Schedule{Start: time.Now(), Rate: 1000}
	sent, _, err := Run(t.Context(), s, 100, func(_ context.Context, k int, _ time.Time) error {
		if k == 10 {
			return boom
		}
		return nil
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want it to wrap boom", err)
	}
	if sent >= 100 {
		t.Errorf("sent = %d: a failed send must stop the schedule", sent)
	}
}

func TestRunStopsWhenTheContextEnds(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	s := Schedule{Start: time.Now(), Rate: 10} // 100 jobs would take 10s
	start := time.Now()
	sent, _, err := Run(ctx, s, 100, func(context.Context, int, time.Time) error { return nil })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Run error = %v, want the context's", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("Run took %v after its context ended", took)
	}
	if sent >= 100 {
		t.Errorf("sent = %d, want fewer than 100", sent)
	}
}

func TestRunWaitsForSendsInFlight(t *testing.T) {
	var running, maxRunning int
	var mu sync.Mutex
	s := Schedule{Start: time.Now(), Rate: 100000}
	sent, _, err := Run(t.Context(), s, 50, func(ctx context.Context, _ int, _ time.Time) error {
		mu.Lock()
		running++
		maxRunning = max(maxRunning, running)
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		running--
		mu.Unlock()
		return nil
	})
	if err != nil || sent != 50 {
		t.Fatalf("Run = %d sent, %v; want 50, nil", sent, err)
	}
	if running != 0 {
		t.Errorf("%d sends were still running when Run returned", running)
	}
	if maxRunning < 10 {
		t.Errorf("at most %d sends overlapped, want an open loop to overlap them", maxRunning)
	}
}

func TestRunMeasuresLatenessAfterTheSlotIsAcquiredAndNeverExceedsTheCap(t *testing.T) {
	// 64 jobs on a schedule that wants them all within a millisecond, a cap of 8
	// sends at once, and sends that block behind a gate for about 100ms. Eight
	// sends start at once and block. The other 56 wait for a slot, so each of them
	// begins about 100ms after its intended time, and that wait is lateness: it is
	// the generator falling behind a system that stopped answering, which the
	// latency measured from the intended time must show.
	const (
		n, limit = 64, 8
		block    = 100 * time.Millisecond
	)
	s := Schedule{Start: time.Now(), Rate: 100000}
	gate := make(chan struct{})
	opened := time.AfterFunc(block, func() { close(gate) })
	defer opened.Stop()

	var (
		mu         sync.Mutex
		running    int
		maxRunning int
		behind     = make([]time.Duration, n) // time.Since(intended) when the send began
	)
	sent, late, err := run(t.Context(), s, n, func(ctx context.Context, k int, intended time.Time) error {
		mu.Lock()
		behind[k] = time.Since(intended)
		running++
		maxRunning = max(maxRunning, running)
		mu.Unlock()
		defer func() {
			mu.Lock()
			running--
			mu.Unlock()
		}()
		select {
		case <-gate:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}, limit)
	if err != nil || sent != n {
		t.Fatalf("run = %d sent, %v; want %d, nil", sent, err, n)
	}

	// Lower bounds only: a slow machine makes the sends later, never earlier.
	if late < n-limit {
		t.Errorf("late = %d, want at least %d: the %d sends that waited for a slot began about %v behind schedule", late, n-limit, n-limit, block)
	}
	for k := limit; k < n; k++ {
		if behind[k] < 90*time.Millisecond {
			t.Errorf("job %d began %v after its intended time, want at least about %v: it waited behind the cap for a slot", k, behind[k], block)
		}
	}
	if maxRunning > limit {
		t.Errorf("%d sends ran at once, want at most the cap of %d", maxRunning, limit)
	}
	if maxRunning < limit {
		t.Errorf("only %d sends overlapped, want the cap of %d to be reached by a schedule that wants 64 at once", maxRunning, limit)
	}
}
