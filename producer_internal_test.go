package jobqueue

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

func TestRetryCeilingNeverOverflows(t *testing.T) {
	for name, policy := range map[string]RetryPolicy{
		"defaults":              {},
		"tiny base, huge cap":   {Base: 1, Cap: math.MaxInt64},
		"huge base and cap":     {Base: math.MaxInt64, Cap: math.MaxInt64},
		"base that doubles out": {Base: 3e18, Cap: math.MaxInt64},
		"base above the cap":    {Base: math.MaxInt64, Cap: time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			p := policy.withDefaults()
			var previous time.Duration
			for n := 1; n <= 100; n++ {
				got := p.ceiling(n)
				if got < p.Base || got > p.Cap || got < previous {
					t.Fatalf("ceiling(%d) = %v after %v, want it between Base %v and Cap %v and never falling", n, got, previous, p.Base, p.Cap)
				}
				previous = got
			}
		})
	}
}

func TestRetryCeilingDoublesUpToTheCap(t *testing.T) {
	p := RetryPolicy{Base: 50 * time.Millisecond, Cap: 2 * time.Second}.withDefaults()
	want := []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond,
		800 * time.Millisecond, 1600 * time.Millisecond, 2 * time.Second, 2 * time.Second}
	for i, w := range want {
		if got := p.ceiling(i + 1); got != w {
			t.Errorf("ceiling(%d) = %v, want %v", i+1, got, w)
		}
	}
}

// Whatever the policy, a wait is drawn without a panic and ends at once when
// the context has ended.
func TestSleepWithAHugeCeilingEndsWithTheContext(t *testing.T) {
	p := NewProducer(nil, ProducerOptions{Retry: RetryPolicy{Base: math.MaxInt64, Cap: math.MaxInt64}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for n := 1; n <= 100; n++ {
		if err := p.sleep(ctx, n); !errors.Is(err, context.Canceled) {
			t.Fatalf("sleep(%d) = %v, want context.Canceled", n, err)
		}
	}
}
