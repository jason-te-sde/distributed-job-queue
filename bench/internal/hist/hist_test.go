package hist

import (
	"sync"
	"testing"
	"time"
)

// within reports whether got is within the relative error of want. Three
// significant digits mean a recorded value comes back within 0.1%.
func within(got, want time.Duration, rel float64) bool {
	diff := float64(got - want)
	if diff < 0 {
		diff = -diff
	}
	return diff <= rel*float64(want)
}

func TestHistQuantiles(t *testing.T) {
	h := New()
	// 1ms, 2ms, ... 1000ms, once each: the q-th percentile is q*10 ms.
	for i := 1; i <= 1000; i++ {
		h.Record(time.Duration(i) * time.Millisecond)
	}
	q := h.Quantiles()
	for key, want := range map[string]time.Duration{
		"p50":   500 * time.Millisecond,
		"p90":   900 * time.Millisecond,
		"p99":   990 * time.Millisecond,
		"p99.9": 999 * time.Millisecond,
		"max":   1000 * time.Millisecond,
	} {
		got, ok := q[key]
		if !ok {
			t.Errorf("Quantiles() has no %q: %v", key, q)
			continue
		}
		if !within(got, want, 0.002) {
			t.Errorf("%s = %v, want %v within 0.2%%", key, got, want)
		}
	}
	if len(q) != 5 {
		t.Errorf("Quantiles() has %d keys, want exactly p50, p90, p99, p99.9 and max: %v", len(q), q)
	}
	if got := h.Count(); got != 1000 {
		t.Errorf("Count() = %d, want 1000", got)
	}
}

func TestHistSeparatesAHeavyTail(t *testing.T) {
	h := New()
	for range 9980 {
		h.Record(time.Millisecond)
	}
	for range 20 {
		h.Record(2 * time.Second)
	}
	q := h.Quantiles()
	if !within(q["p99"], time.Millisecond, 0.01) {
		t.Errorf("p99 = %v, want about 1ms: 0.2%% of the samples are slow", q["p99"])
	}
	if !within(q["p99.9"], 2*time.Second, 0.01) {
		t.Errorf("p99.9 = %v, want about 2s: the slowest 0.2%% of the samples are 2s", q["p99.9"])
	}
	if !within(q["max"], 2*time.Second, 0.001) {
		t.Errorf("max = %v, want 2s", q["max"])
	}
}

func TestHistEmptyReportsZeros(t *testing.T) {
	q := New().Quantiles()
	if len(q) != 5 {
		t.Fatalf("Quantiles() of an empty histogram has %d keys, want 5: %v", len(q), q)
	}
	for key, v := range q {
		if v != 0 {
			t.Errorf("%s = %v in an empty histogram, want 0", key, v)
		}
	}
}

func TestHistClampsToItsRange(t *testing.T) {
	h := New()
	h.Record(-time.Second)         // a clock step backwards must not be lost or panic
	h.Record(0)                    // below the 1us floor
	h.Record(2 * time.Hour)        // above the 1h ceiling
	h.Record(30 * time.Nanosecond) // sub-microsecond
	if got := h.Count(); got != 4 {
		t.Errorf("Count() = %d after four records, want 4: out-of-range values are clamped, not dropped", got)
	}
	q := h.Quantiles()
	if !within(q["max"], time.Hour, 0.001) {
		t.Errorf("max = %v, want the 1h ceiling for a 2h sample", q["max"])
	}
}

func TestHistConcurrentRecord(t *testing.T) {
	h := New()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 1000 {
				h.Record(time.Millisecond)
				if i%100 == 0 {
					h.Quantiles()
				}
			}
		}()
	}
	wg.Wait()
	if got := h.Count(); got != 8000 {
		t.Errorf("Count() = %d, want 8000", got)
	}
}

func TestHistTakeEmptiesIt(t *testing.T) {
	h := New()
	h.Record(time.Millisecond)
	h.Record(3 * time.Millisecond)
	taken := h.Take()
	if got := taken.Count(); got != 2 {
		t.Errorf("Take().Count() = %d, want 2", got)
	}
	if got := h.Count(); got != 0 {
		t.Errorf("Count() after Take = %d, want 0", got)
	}
	h.Record(5 * time.Millisecond)
	if got := taken.Count(); got != 2 {
		t.Errorf("a record after Take changed the taken histogram: Count() = %d, want 2", got)
	}
}

func TestHistMerge(t *testing.T) {
	a, b := New(), New()
	for range 100 {
		a.Record(time.Millisecond)
		b.Record(time.Second)
	}
	a.Merge(b)
	if got := a.Count(); got != 200 {
		t.Fatalf("Count() after Merge = %d, want 200", got)
	}
	if got := b.Count(); got != 100 {
		t.Errorf("Merge changed its argument: Count() = %d, want 100", got)
	}
	if q := a.Quantiles(); !within(q["max"], time.Second, 0.001) || !within(q["p50"], time.Millisecond, 0.01) {
		t.Errorf("merged quantiles = %v, want p50 about 1ms and max 1s", q)
	}
}

func TestHistCDF(t *testing.T) {
	h := New()
	for i := 1; i <= 1000; i++ {
		h.Record(time.Duration(i) * time.Millisecond)
	}
	cdf := h.CDF()
	if len(cdf) < 50 {
		t.Fatalf("CDF() has %d points, want enough to draw a curve", len(cdf))
	}
	for i := 1; i < len(cdf); i++ {
		if cdf[i].Wait < cdf[i-1].Wait || cdf[i].Fraction < cdf[i-1].Fraction {
			t.Fatalf("CDF() is not monotone at %d: %+v then %+v", i, cdf[i-1], cdf[i])
		}
	}
	last := cdf[len(cdf)-1]
	if last.Fraction != 1 || !within(last.Wait, time.Second, 0.001) {
		t.Errorf("the last CDF point is %+v, want fraction 1 at the maximum of 1s", last)
	}
	if len(New().CDF()) != 0 {
		t.Error("CDF() of an empty histogram is not empty")
	}
}
