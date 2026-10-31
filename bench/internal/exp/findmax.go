package exp

import (
	"context"
	"errors"
	"fmt"
	"math"
)

var (
	// ErrLowEndFailed is returned by FindMax when the lowest rate of the range
	// is not sustained: there is nothing to bisect between.
	ErrLowEndFailed = errors.New("exp: the lowest rate of the range is not sustained")
	// ErrNeverFailed is returned by FindMax, with the highest rate it tried,
	// when every rate it probed was sustained: the true maximum is at or
	// above the top of the range.
	ErrNeverFailed = errors.New("exp: every rate probed was sustained; the maximum is at or above the top of the range")
)

// FindMax finds the highest rate that try reports as ok by bisection, assuming
// that try is monotone: every rate below an ok rate is ok. It probes lo first,
// and returns ErrLowEndFailed with 0 if that is not ok. It then makes steps
// probes, each at the midpoint of the interval between the highest rate known
// to be ok and the lowest known not to be, which starts at hi and is taken as
// not ok without a probe. The result is the highest rate that was probed and
// ok, so it is a rate the system was seen to sustain, within (hi-lo)/2^steps of
// the true maximum. If no probe failed it returns ErrNeverFailed with the
// highest rate probed.
//
// In the experiments try runs the system at the rate and reports whether the
// p99 enqueue-to-start latency stayed under 1s and the backlog was finished
// within 2s of the end of the load.
func FindMax(ctx context.Context, try func(rate float64) (ok bool, err error), lo, hi float64, steps int) (float64, error) {
	if !(lo > 0) || !(hi > lo) || math.IsInf(hi, 0) || steps < 1 {
		return 0, fmt.Errorf("exp: FindMax needs 0 < lo < hi and at least one step, got lo %v, hi %v, steps %d", lo, hi, steps)
	}
	probe := func(rate float64) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		return try(rate)
	}
	ok, err := probe(lo)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("%w (%v/s)", ErrLowEndFailed, lo)
	}
	best, ceiling := lo, hi
	failed := false
	for range steps {
		mid := (best + ceiling) / 2
		ok, err := probe(mid)
		if err != nil {
			return 0, err
		}
		if ok {
			best = mid
		} else {
			ceiling = mid
			failed = true
		}
	}
	if !failed {
		return best, ErrNeverFailed
	}
	return best, nil
}
