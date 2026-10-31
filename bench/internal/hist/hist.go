// Package hist records latencies in an HDR histogram.
//
// It wraps github.com/HdrHistogram/hdrhistogram-go with the one configuration
// the benchmarks use, which is microseconds from 1us to 1h at three significant
// digits, and with a lock, because the handlers of many workers record into the
// same histogram at once.
package hist

import (
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

const (
	minMicros = 1
	maxMicros = int64(time.Hour / time.Microsecond)
	digits    = 3
)

// quantiles are the keys of Hist.Quantiles and the percentiles they stand for,
// in the order they are reported.
var quantiles = []struct {
	key     string
	percent float64
}{
	{"p50", 50}, {"p90", 90}, {"p99", 99}, {"p99.9", 99.9},
}

// Point is one point of an empirical cumulative distribution: Fraction of the
// samples were at most Wait.
type Point struct {
	Wait     time.Duration `json:"wait_ns"`
	Fraction float64       `json:"fraction"`
}

// Hist is a latency histogram, safe for concurrent use.
type Hist struct {
	mu sync.Mutex
	h  *hdrhistogram.Histogram
}

// New returns an empty histogram of microseconds from 1us to 1h with three
// significant digits.
func New() *Hist {
	return &Hist{h: hdrhistogram.New(minMicros, maxMicros, digits)}
}

// Record adds one sample. A sample below 1us counts as 1us and one above 1h
// as 1h, so that an out-of-range value, a negative one after a clock step
// included, is counted in the tail instead of being lost or failing the run.
func (h *Hist) Record(d time.Duration) {
	us := min(max(d.Microseconds(), minMicros), maxMicros)
	h.mu.Lock()
	// RecordValue fails only for a value outside the range, which the clamp
	// above rules out.
	_ = h.h.RecordValue(us)
	h.mu.Unlock()
}

// Count is the number of samples recorded.
func (h *Hist) Count() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.h.TotalCount()
}

// Quantiles returns the latency at "p50", "p90", "p99" and "p99.9", and the
// largest sample as "max". An empty histogram reports zero for each.
func (h *Hist) Quantiles() map[string]time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[string]time.Duration, len(quantiles)+1)
	for _, q := range quantiles {
		out[q.key] = micros(h.h.ValueAtQuantile(q.percent))
	}
	out["max"] = micros(h.h.Max())
	return out
}

// Take returns what has been recorded so far and empties h, so that a caller
// can read the samples of one interval at a time.
func (h *Hist) Take() *Hist {
	h.mu.Lock()
	defer h.mu.Unlock()
	taken := &Hist{h: h.h}
	h.h = hdrhistogram.New(minMicros, maxMicros, digits)
	return taken
}

// Merge adds the samples of other to h. other is not changed.
func (h *Hist) Merge(other *Hist) {
	other.mu.Lock()
	snapshot := hdrhistogram.Import(other.h.Export())
	other.mu.Unlock()
	h.mu.Lock()
	defer h.mu.Unlock()
	h.h.Merge(snapshot)
}

// CDF returns the empirical distribution as points a plot can join: every whole
// percentile, and a finer step in the tail, up to the maximum at fraction 1. An
// empty histogram has none.
func (h *Hist) CDF() []Point {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.h.TotalCount() == 0 {
		return nil
	}
	var percents []float64
	for p := 1; p <= 99; p++ {
		percents = append(percents, float64(p))
	}
	for p := 991; p <= 999; p++ {
		percents = append(percents, float64(p)/10)
	}
	for p := 9991; p <= 9999; p++ {
		percents = append(percents, float64(p)/100)
	}
	pts := make([]Point, 0, len(percents)+1)
	for _, p := range percents {
		pts = append(pts, Point{Wait: micros(h.h.ValueAtQuantile(p)), Fraction: p / 100})
	}
	return append(pts, Point{Wait: micros(h.h.Max()), Fraction: 1})
}

func micros(v int64) time.Duration { return time.Duration(v) * time.Microsecond }
