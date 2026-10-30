package sim

import (
	"math/rand/v2"
	"time"
)

// Rand is a seeded random source. It is not safe for concurrent use, which
// the simulator never needs: it runs on one goroutine.
type Rand struct {
	r *rand.Rand
}

// NewRand returns a generator whose stream is a function of seed alone.
func NewRand(seed uint64) *Rand {
	return &Rand{r: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))}
}

// Chance reports true with probability p. A p of 0 or less never reports
// true and a p of 1 or more always does. It draws once from r whatever p is.
func (r *Rand) Chance(p float64) bool {
	return r.r.Float64() < p
}

// IntN returns a uniform integer in [0, n). It panics if n <= 0.
func (r *Rand) IntN(n int) int {
	return r.r.IntN(n)
}

// Between returns a duration drawn uniformly from the whole microseconds in
// [lo, hi], both ends included. lo and hi are truncated to whole
// microseconds first. It panics if hi < lo.
func (r *Rand) Between(lo, hi time.Duration) time.Duration {
	if hi < lo {
		panic("sim: Rand.Between called with hi < lo")
	}
	loUs, hiUs := int64(lo/time.Microsecond), int64(hi/time.Microsecond)
	return time.Duration(loUs+r.r.Int64N(hiUs-loUs+1)) * time.Microsecond
}

// Float64 returns a uniform number in [0, 1).
func (r *Rand) Float64() float64 {
	return r.r.Float64()
}

// Fork returns a child generator seeded from one draw of r. Giving each node
// its own child keeps one node's draws from shifting another's, while the
// whole family of streams still follows from the world's seed.
func (r *Rand) Fork() *Rand {
	return NewRand(r.r.Uint64())
}
