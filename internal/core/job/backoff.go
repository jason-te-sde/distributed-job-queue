package job

import (
	"math"
	"time"
)

// BackoffPolicy is how long a failed job waits before its next attempt:
// exponential growth from Base, doubling with each attempt up to Cap, with
// full jitter, so that the jobs of a failing dependency do not all come back
// at the same instant.
type BackoffPolicy struct {
	// Base is the ceiling of the first retry's delay.
	Base time.Duration
	// Cap is the largest ceiling of any retry's delay.
	Cap time.Duration
}

// DefaultBackoff starts at one second and doubles to a cap of one hour.
// Across the default 25 attempts the ceilings of the 24 retries sum to
// 47295 seconds, about 13.1 hours.
var DefaultBackoff = BackoffPolicy{Base: time.Second, Cap: time.Hour}

// Ceiling is the upper bound of the delay after the given attempt, which
// counts from 1: min(Cap, Base * 2^(attempt-1)). An attempt below 1 counts
// as the first. It stops doubling at Cap, so it cannot overflow however
// large the attempt. A policy whose Base or Cap is not positive has no
// room for a delay, and its ceiling is 0.
func (p BackoffPolicy) Ceiling(attempt int32) time.Duration {
	if p.Base <= 0 || p.Cap <= 0 {
		return 0
	}
	c := min(p.Base, p.Cap)
	for i := int32(1); i < attempt && c < p.Cap; i++ {
		if c > p.Cap/2 {
			return p.Cap // doubling would reach or pass the cap
		}
		c *= 2
	}
	return c
}

// Delay is the wait after the given attempt for a uniform random draw u in
// [0, 1): full jitter, so the wait is spread over the whole span from 0 to
// Ceiling(attempt). The draw comes from the caller, which keeps this
// function pure and the choice reproducible. A u below 0 or not a number
// counts as 0, and a u of 1 or more as the largest value below 1. The
// result is a whole number of microseconds, the precision the database
// stores, and is below the ceiling whenever the ceiling is at least one
// microsecond.
func (p BackoffPolicy) Delay(attempt int32, u float64) time.Duration {
	ceiling := p.Ceiling(attempt)
	switch {
	case !(u > 0): // also catches NaN
		u = 0
	case u >= 1:
		u = math.Nextafter(1, 0)
	}
	d := time.Duration(u * float64(ceiling))
	return d - d%time.Microsecond
}
