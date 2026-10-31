package sim

import "time"

// wakeAt returns the virtual time at which a node's clock first reads mono
// or later, clamped to the present. Clock.Virtual rounds to the nearest
// microsecond, so its result can read a hair before mono, and a node that
// woke up then would find nothing due and re-arm the same instant forever.
// Rounding up instead keeps the wakeup at or after the instant, on a whole
// microsecond.
func wakeAt(w *World, c Clock, mono time.Duration) time.Duration {
	v := c.Virtual(mono)
	// A clock within 1e-4 of real time needs one step at most; the bound
	// only keeps a freak clock from looping.
	for i := 0; i < 3 && c.Mono(v) < mono; i++ {
		v += time.Microsecond
	}
	return max(v, w.Now)
}

// afterMono returns the virtual time at which d has passed on a node's clock.
func afterMono(w *World, c Clock, d time.Duration) time.Duration {
	return wakeAt(w, c, c.Mono(w.Now)+d)
}

// nodeTimer is the single timer that a core's Arm effect drives: arming it
// again replaces the pending wakeup.
type nodeTimer struct {
	w     *World
	clock Clock
	seq   uint64        // identifies the live wakeup
	at    time.Duration // virtual time of the live wakeup
	live  bool
}

// arm schedules fire for the instant mono on the node's clock and drops any
// earlier wakeup. Arming the instant that is already pending does nothing.
func (t *nodeTimer) arm(mono time.Duration, label string, fire func()) {
	at := wakeAt(t.w, t.clock, mono)
	if t.live && t.at == at {
		return
	}
	t.seq++
	seq := t.seq
	t.at, t.live = at, true
	t.w.At(at, label, func() {
		if t.seq != seq {
			return
		}
		t.live = false
		fire()
	})
}

// cancel drops the pending wakeup.
func (t *nodeTimer) cancel() {
	t.seq++
	t.live = false
}
