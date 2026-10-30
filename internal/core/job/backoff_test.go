package job

import (
	"math"
	"math/big"
	"testing"
	"time"

	"pgregory.net/rapid"
)

func TestCeilingDoublesThenCaps(t *testing.T) {
	p := DefaultBackoff
	for a, want := range map[int32]time.Duration{1: time.Second, 2: 2 * time.Second, 12: 2048 * time.Second, 13: time.Hour, 25: time.Hour, 1000: time.Hour, 0: time.Second, -5: time.Second} {
		if got := p.Ceiling(a); got != want {
			t.Errorf("Ceiling(%d) = %v, want %v", a, got, want)
		}
	}
}

func TestCeilingSumOverDefaultRetries(t *testing.T) {
	var sum time.Duration
	for a := int32(1); a <= DefaultMaxAttempts-1; a++ {
		sum += DefaultBackoff.Ceiling(a)
	}
	if sum != 47295*time.Second {
		t.Fatalf("sum = %v", sum) // quoted in docs as "at most about 13.1 hours"
	}
}

func TestCeilingNeverOverflows(t *testing.T) {
	huge := BackoffPolicy{Base: 1, Cap: math.MaxInt64}
	for _, a := range []int32{62, 63, 64, 65, 1000, math.MaxInt32} {
		got := huge.Ceiling(a)
		if got <= 0 {
			t.Errorf("Ceiling(%d) = %v: overflowed", a, got)
		}
	}
	if got := huge.Ceiling(math.MaxInt32); got != math.MaxInt64 {
		t.Errorf("Ceiling(MaxInt32) = %d, want the cap", got)
	}
	// 2^62 is the largest power of two that fits: attempt 63 reaches it,
	// and the next doubling must stop at the cap.
	if got := huge.Ceiling(63); got != 1<<62 {
		t.Errorf("Ceiling(63) = %d, want 2^62", got)
	}
	if got := huge.Ceiling(64); got != math.MaxInt64 {
		t.Errorf("Ceiling(64) = %d, want the cap", got)
	}
}

func TestCeilingOfDegeneratePolicies(t *testing.T) {
	for name, tc := range map[string]struct {
		p    BackoffPolicy
		want time.Duration
	}{
		"zero value":     {BackoffPolicy{}, 0},
		"zero base":      {BackoffPolicy{Base: 0, Cap: time.Hour}, 0},
		"negative base":  {BackoffPolicy{Base: -time.Second, Cap: time.Hour}, 0},
		"zero cap":       {BackoffPolicy{Base: time.Second, Cap: 0}, 0},
		"negative cap":   {BackoffPolicy{Base: time.Second, Cap: -time.Hour}, 0},
		"base above cap": {BackoffPolicy{Base: time.Hour, Cap: time.Second}, time.Second},
	} {
		for _, a := range []int32{-1, 1, 5, math.MaxInt32} {
			if got := tc.p.Ceiling(a); got != tc.want {
				t.Errorf("%s: Ceiling(%d) = %v, want %v", name, a, got, tc.want)
			}
		}
		// A policy with no ceiling must also give no delay, and must not
		// loop for long: MaxInt32 attempts is the worst case.
		if got := tc.p.Delay(math.MaxInt32, 0.5); got < 0 || got > tc.want {
			t.Errorf("%s: Delay = %v, want within [0, %v]", name, got, tc.want)
		}
	}
}

func TestCeilingMatchesExactArithmetic(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		p := BackoffPolicy{
			Base: time.Duration(rapid.Int64Range(1, math.MaxInt64).Draw(rt, "base")),
			Cap:  time.Duration(rapid.Int64Range(1, math.MaxInt64).Draw(rt, "cap")),
		}
		attempt := rapid.Int32Range(-3, 200).Draw(rt, "attempt")
		// min(Cap, Base * 2^(attempt-1)) in arbitrary precision; an attempt
		// below 1 counts as the first.
		want := new(big.Int).Lsh(big.NewInt(int64(p.Base)), uint(max(attempt, 1)-1))
		if limit := big.NewInt(int64(p.Cap)); want.Cmp(limit) > 0 {
			want = limit
		}
		if got := p.Ceiling(attempt); got != time.Duration(want.Int64()) {
			rt.Fatalf("%+v Ceiling(%d) = %d, want %s", p, attempt, int64(got), want)
		}
	})
}

func TestDelayIsFullJitter(t *testing.T) {
	p := DefaultBackoff
	if got := p.Delay(3, 0); got != 0 {
		t.Errorf("Delay(3, 0) = %v, want 0", got)
	}
	if got := p.Delay(3, 0.5); got != 2*time.Second {
		t.Errorf("Delay(3, 0.5) = %v, want 2s", got)
	}
	if got := p.Delay(3, 0.9999999); got >= 4*time.Second {
		t.Errorf("Delay(3, 0.9999999) = %v, want below 4s", got)
	}
	if got := p.Delay(3, 1.5); got >= 4*time.Second {
		t.Errorf("Delay(3, 1.5) = %v, want below 4s: u is clamped", got)
	}
	if got := p.Delay(3, 1); got != 4*time.Second-time.Microsecond {
		t.Errorf("Delay(3, 1) = %v, want the largest whole microsecond below 4s", got)
	}
	if got := p.Delay(3, -1); got != 0 {
		t.Errorf("Delay(3, -1) = %v, want 0", got)
	}
	if got := p.Delay(3, math.NaN()); got != 0 {
		t.Errorf("Delay(3, NaN) = %v, want 0", got)
	}
	if got := p.Delay(3, math.Inf(1)); got >= 4*time.Second {
		t.Errorf("Delay(3, +Inf) = %v, want below 4s", got)
	}
	if got := p.Delay(3, math.Inf(-1)); got != 0 {
		t.Errorf("Delay(3, -Inf) = %v, want 0", got)
	}
}

func TestDelayIsWholeMicroseconds(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		a := rapid.Int32Range(1, 40).Draw(rt, "attempt")
		u := float64(rapid.Uint64Range(0, 1<<53-1).Draw(rt, "u")) / (1 << 53) // uniform in [0, 1)
		d := DefaultBackoff.Delay(a, u)
		if d%time.Microsecond != 0 || d < 0 || d >= DefaultBackoff.Ceiling(a) {
			rt.Fatalf("Delay(%d, %v) = %v: want whole microseconds in [0, %v)", a, u, d, DefaultBackoff.Ceiling(a))
		}
	})
}

func TestDelayStaysBelowTheCeilingForAnyPolicy(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		p := BackoffPolicy{
			Base: time.Duration(rapid.Int64Range(1, math.MaxInt64).Draw(rt, "base")),
			Cap:  time.Duration(rapid.Int64Range(1, math.MaxInt64).Draw(rt, "cap")),
		}
		a := rapid.Int32Range(1, 100).Draw(rt, "attempt")
		u := float64(rapid.Uint64Range(0, 1<<53-1).Draw(rt, "u")) / (1 << 53)
		ceiling, d := p.Ceiling(a), p.Delay(a, u)
		if d%time.Microsecond != 0 || d < 0 || d >= ceiling && ceiling >= time.Microsecond {
			rt.Fatalf("%+v Delay(%d, %v) = %d: want whole microseconds in [0, %d)", p, a, u, int64(d), int64(ceiling))
		}
		if d > ceiling {
			rt.Fatalf("%+v Delay(%d, %v) = %d exceeds the ceiling %d", p, a, u, int64(d), int64(ceiling))
		}
	})
}

func TestDelayMonotoneInU(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		a := rapid.Int32Range(1, 40).Draw(rt, "attempt")
		n1 := rapid.Uint64Range(0, 1<<53-1).Draw(rt, "n1")
		n2 := rapid.Uint64Range(n1, 1<<53-1).Draw(rt, "n2")
		u1, u2 := float64(n1)/(1<<53), float64(n2)/(1<<53)
		if d1, d2 := DefaultBackoff.Delay(a, u1), DefaultBackoff.Delay(a, u2); d1 > d2 {
			rt.Fatalf("Delay(%d, %v) = %v exceeds Delay(%d, %v) = %v", a, u1, d1, a, u2, d2)
		}
	})
}
