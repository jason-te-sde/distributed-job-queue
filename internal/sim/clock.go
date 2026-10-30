package sim

import (
	"math"
	"time"
)

// NodeID names a simulated node.
type NodeID string

// Clock is the monotonic clock of one simulated node. Each node's clock
// starts at its own Offset and runs at its own Rate, so nodes disagree about
// elapsed time the way real hosts do. A node uses its clock to measure
// durations; it never reads World.Now.
type Clock struct {
	// Offset is what the clock reads at virtual time zero.
	Offset time.Duration
	// Rate is how many nanoseconds the clock advances per virtual
	// nanosecond. Realistic drift keeps it within 1e-4 of 1.
	Rate float64
}

// Mono returns the clock's reading at the given virtual time: Offset plus
// virtual*Rate, rounded to the nearest nanosecond. It panics if Rate is not
// positive.
func (c Clock) Mono(virtual time.Duration) time.Duration {
	c.mustRun("Mono")
	return c.Offset + time.Duration(math.Round(float64(virtual)*c.Rate))
}

// Virtual returns the virtual time at which the clock reads mono, rounded to
// the nearest microsecond. It inverts Mono, so a node can schedule a world
// event for a deadline it computed on its own clock. It panics if Rate is
// not positive.
func (c Clock) Virtual(mono time.Duration) time.Duration {
	c.mustRun("Virtual")
	ns := float64(mono-c.Offset) / c.Rate
	return time.Duration(math.Round(ns/float64(time.Microsecond))) * time.Microsecond
}

// mustRun panics unless the clock advances. The zero Clock does not, and a
// silent constant clock would hide a node that was never given one.
func (c Clock) mustRun(method string) {
	if !(c.Rate > 0) {
		panic("sim: Clock." + method + " called on a clock whose Rate is not positive")
	}
}
