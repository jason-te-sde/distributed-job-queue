package exp

import (
	"context"
	"errors"
	"math"
	"testing"
)

// below is a synthetic system that keeps up with any rate under limit.
func below(limit float64, probes *[]float64) func(float64) (bool, error) {
	return func(rate float64) (bool, error) {
		*probes = append(*probes, rate)
		return rate < limit, nil
	}
}

func TestFindMaxConverges(t *testing.T) {
	const limit = 1234.0
	var probes []float64
	got, err := FindMax(t.Context(), below(limit, &probes), 100, 10000, 10)
	if err != nil {
		t.Fatalf("FindMax: %v", err)
	}
	if got > limit {
		t.Errorf("FindMax = %v, above the limit %v: it must return a rate that was ok", got, limit)
	}
	if math.Abs(got-limit) > 0.02*limit {
		t.Errorf("FindMax = %v, want within 2%% of %v", got, limit)
	}
	// One probe of the low end and one per step.
	if len(probes) != 11 {
		t.Errorf("FindMax tried %d rates (%v), want 11: the low end and 10 bisection steps", len(probes), probes)
	}
}

func TestFindMaxOnlyReturnsAProbedOkRate(t *testing.T) {
	ok := map[float64]bool{}
	try := func(rate float64) (bool, error) {
		good := rate < 777
		ok[rate] = good
		return good, nil
	}
	got, err := FindMax(t.Context(), try, 10, 5000, 12)
	if err != nil {
		t.Fatal(err)
	}
	if !ok[got] {
		t.Errorf("FindMax = %v, which was never probed or was not ok (probes: %v)", got, ok)
	}
}

func TestFindMaxReportsWhenNothingFailed(t *testing.T) {
	var probes []float64
	got, err := FindMax(t.Context(), below(1e9, &probes), 100, 10000, 6)
	if !errors.Is(err, ErrNeverFailed) {
		t.Fatalf("FindMax error = %v, want ErrNeverFailed: the true maximum is above the range", err)
	}
	if got < 9000 {
		t.Errorf("FindMax = %v, want the highest rate tried, close to the top of the range", got)
	}
}

func TestFindMaxReportsWhenTheLowEndFails(t *testing.T) {
	var probes []float64
	got, err := FindMax(t.Context(), below(50, &probes), 100, 10000, 6)
	if !errors.Is(err, ErrLowEndFailed) {
		t.Fatalf("FindMax error = %v, want ErrLowEndFailed", err)
	}
	if got != 0 {
		t.Errorf("FindMax = %v, want 0 when not even the low end is sustained", got)
	}
	if len(probes) != 1 {
		t.Errorf("FindMax tried %d rates after the low end failed, want to stop at once", len(probes))
	}
}

func TestFindMaxPropagatesAnErrorFromTry(t *testing.T) {
	boom := errors.New("boom")
	calls := 0
	_, err := FindMax(t.Context(), func(float64) (bool, error) {
		calls++
		if calls == 3 {
			return false, boom
		}
		return true, nil
	}, 100, 10000, 10)
	if !errors.Is(err, boom) {
		t.Errorf("FindMax error = %v, want boom", err)
	}
	if calls != 3 {
		t.Errorf("FindMax kept going after an error: %d calls", calls)
	}
}

func TestFindMaxStopsWhenTheContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	calls := 0
	_, err := FindMax(ctx, func(float64) (bool, error) {
		calls++
		cancel()
		return true, nil
	}, 100, 10000, 10)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("FindMax error = %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Errorf("FindMax made %d calls after its context ended, want 1", calls)
	}
}

func TestFindMaxRejectsABadRange(t *testing.T) {
	try := func(float64) (bool, error) { t.Error("try was called"); return true, nil }
	for _, tc := range []struct {
		name   string
		lo, hi float64
		steps  int
	}{
		{"low end not positive", 0, 100, 5},
		{"range empty", 100, 100, 5},
		{"range reversed", 200, 100, 5},
		{"no steps", 10, 100, 0},
		{"not a number", math.NaN(), 100, 5},
	} {
		if _, err := FindMax(t.Context(), try, tc.lo, tc.hi, tc.steps); err == nil {
			t.Errorf("%s: FindMax returned no error", tc.name)
		}
	}
}
