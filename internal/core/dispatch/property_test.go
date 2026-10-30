package dispatch

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
)

// phase is where the model believes a waiter is.
type phase uint8

const (
	phWaiting   phase = iota + 1 // parked, no fetch out
	phFetching                   // a fetch is out
	phGone                       // cancelled while fetching; the fetch is still out
	phCancelled                  // cancelled while parked
	phDone                       // answered, or dropped after a cancelled fetch came back empty
)

// modelWaiter is the model's record of one waiter.
type modelWaiter struct {
	w                 Waiter
	phase             phase
	answers           int  // Deliver, Respond and Release effects addressed to it
	cancelledFetching bool // cancelled while a fetch was out
}

// fetchDone describes the FetchDone call whose effects are being checked.
type fetchDone struct {
	id     WaiterID
	prev   phase // the phase before the call: phFetching or phGone
	got    int   // what the core is told it holds: 0 when the fetch failed
	failed bool
}

// model drives a Core with random calls and checks every effect against its
// own record of the waiters. It reads the core only through the public
// counters.
type model struct {
	t        *rapid.T
	c        *Core
	maxFly   int
	now      Mono
	next     WaiterID
	queues   []string
	waiters  map[WaiterID]*modelWaiter
	draining bool
}

func TestDispatchProperties(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		cfg := Config{
			MaxInFlight:  rapid.IntRange(1, 6).Draw(rt, "maxInFlight"),
			PollInterval: time.Duration(rapid.IntRange(100, 3000).Draw(rt, "pollMs")) * time.Millisecond,
		}
		m := &model{
			t:       rt,
			c:       New(cfg),
			maxFly:  cfg.MaxInFlight,
			next:    1,
			waiters: map[WaiterID]*modelWaiter{},
		}
		for i := range rapid.IntRange(1, 3).Draw(rt, "queues") {
			m.queues = append(m.queues, fmt.Sprintf("q%d", i))
		}
		for range rapid.IntRange(1, 200).Draw(rt, "calls") {
			m.step()
		}
		m.finish()
	})
}

// step makes one random call after a random pause.
func (m *model) step() {
	m.now += time.Duration(rapid.IntRange(0, 1500).Draw(m.t, "pauseMs")) * time.Millisecond
	switch k := rapid.IntRange(0, 999).Draw(m.t, "call"); {
	case k < 300:
		m.arrive()
	case k < 400:
		m.notify()
	case k < 480:
		m.dueAt()
	case k < 750:
		m.fetchDone()
	case k < 820:
		m.cancel()
	case k < 990:
		m.tick()
	default:
		m.drain()
	}
}

func (m *model) queue() string {
	return m.queues[rapid.IntRange(0, len(m.queues)-1).Draw(m.t, "queue")]
}

func (m *model) arrive() {
	id := m.next
	m.next++
	var wait time.Duration
	if rapid.IntRange(0, 3).Draw(m.t, "waitKind") > 0 { // one call in four does not wait at all
		wait = time.Duration(rapid.IntRange(1, 30_000).Draw(m.t, "waitMs")) * time.Millisecond
	}
	w := Waiter{
		ID:       id,
		Queue:    m.queue(),
		Max:      rapid.IntRange(1, job.MaxBatch).Draw(m.t, "max"),
		Arrived:  m.now,
		Deadline: m.now + wait,
	}
	m.waiters[id] = &modelWaiter{w: w, phase: phWaiting}
	m.check("Arrive", m.c.Arrive(m.now, w), nil)
}

func (m *model) notify() {
	m.check("Notify", m.c.Notify(m.now, m.queue()), nil)
}

func (m *model) dueAt() {
	q := m.queue()
	var at Mono // zero clears the due time
	if rapid.IntRange(0, 5).Draw(m.t, "dueKind") > 0 {
		at = max(m.now+time.Duration(rapid.IntRange(-2000, 10_000).Draw(m.t, "dueDeltaMs"))*time.Millisecond, 1)
	}
	m.check("DueAt", m.c.DueAt(m.now, q, at), nil)
}

// outstanding returns the waiters whose fetch is out, in id order.
func (m *model) outstanding() []WaiterID {
	var ids []WaiterID
	for id, mw := range m.waiters {
		if mw.phase == phFetching || mw.phase == phGone {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

func (m *model) fetchDone() {
	ids := m.outstanding()
	if len(ids) == 0 {
		return
	}
	id := ids[rapid.IntRange(0, len(ids)-1).Draw(m.t, "outstanding")]
	m.completeFetch(id)
}

// completeFetch ends the fetch of waiter id with a random result.
func (m *model) completeFetch(id WaiterID) {
	mw := m.waiters[id]
	got := rapid.IntRange(0, mw.w.Max).Draw(m.t, "got")
	failed := rapid.IntRange(0, 9).Draw(m.t, "failed") == 0
	fd := &fetchDone{id: id, prev: mw.phase, got: got, failed: failed}
	if failed {
		fd.got = 0 // a failed fetch holds nothing, whatever the shell passes
	}
	m.check("FetchDone", m.c.FetchDone(m.now, id, got, failed), fd)
}

func (m *model) cancel() {
	if m.next == 1 {
		return
	}
	id := WaiterID(rapid.IntRange(1, int(m.next-1)).Draw(m.t, "cancelID"))
	mw := m.waiters[id]
	switch mw.phase {
	case phWaiting:
		mw.phase = phCancelled
	case phFetching:
		mw.phase = phGone
		mw.cancelledFetching = true
	case phGone, phCancelled, phDone:
		// Cancelling twice, or after the answer, does nothing.
	}
	m.check("Cancel", m.c.Cancel(m.now, id), nil)
}

func (m *model) tick() {
	m.now += time.Duration(rapid.IntRange(0, 5000).Draw(m.t, "tickPauseMs")) * time.Millisecond
	m.check("Tick", m.c.Tick(m.now), nil)
}

func (m *model) drain() {
	m.draining = true
	m.check("Drain", m.c.Drain(m.now), nil)
}

// answer records that waiter id received a Deliver, Respond or Release.
func (m *model) answer(op string, id WaiterID) {
	m.t.Helper()
	mw := m.waiters[id]
	if mw == nil {
		m.t.Fatalf("%s: answer for unknown waiter %d", op, id)
	}
	mw.answers++
	if mw.answers > 1 {
		m.t.Fatalf("%s: waiter %d answered %d times", op, id, mw.answers)
	}
	mw.phase = phDone
}

// check applies the effects of one call to the model, failing on any
// violation, and then compares the core's counters and its Arm with the
// model.
func (m *model) check(op string, effs []Effect, fd *fetchDone) {
	m.t.Helper()
	fail := func(format string, args ...any) {
		m.t.Helper()
		m.t.Fatalf("%s at %s: %s\neffects: %s", op, m.now, fmt.Sprintf(format, args...), describeAll(effs))
	}

	// What a FetchDone does to its waiter when nothing answers it: a waiter
	// whose fetch came back empty waits again, and a cancelled one is dropped.
	ownAnswer := func(e Effect) bool {
		switch e := e.(type) {
		case Deliver:
			return fd != nil && e.Waiter == fd.id
		case Respond:
			return fd != nil && e.Waiter == fd.id
		case Release:
			return fd != nil && e.Waiter == fd.id
		case Fetch, Arm:
			return false
		}
		return false
	}
	if fd != nil && !slices.ContainsFunc(effs, ownAnswer) {
		if fd.got > 0 {
			fail("fetch of waiter %d came back with %d jobs and nothing was delivered or released", fd.id, fd.got)
		}
		if fd.prev == phFetching {
			m.waiters[fd.id].phase = phWaiting
		} else {
			m.waiters[fd.id].phase = phDone
		}
	}
	fromFetch := func(id WaiterID, prev phase) bool {
		return fd != nil && id == fd.id && fd.prev == prev
	}

	var arm Mono
	var armSeen bool
	for i, e := range effs {
		switch e := e.(type) {
		case Fetch:
			mw := m.waiters[e.Waiter]
			switch {
			case op == "Cancel" || op == "DueAt" || op == "Drain":
				fail("%s must not start a fetch", op)
			case mw == nil || mw.phase != phWaiting:
				fail("Fetch for waiter %d, which is not waiting", e.Waiter)
			case m.draining:
				fail("Fetch after Drain")
			case e.Queue != mw.w.Queue || e.Max != mw.w.Max:
				fail("Fetch %s does not match the waiter %+v", describe(e), mw.w)
			case e.Waited != m.now-mw.w.Arrived:
				fail("Fetch.Waited = %s, want %s", e.Waited, m.now-mw.w.Arrived)
			}
			mw.phase = phFetching
		case Deliver:
			if !fromFetch(e.Waiter, phFetching) || fd.got == 0 {
				fail("Deliver to waiter %d without a fetch that found jobs", e.Waiter)
			}
			m.answer(op, e.Waiter)
		case Release:
			if !fromFetch(e.Waiter, phGone) || fd.got == 0 {
				fail("Release for waiter %d without a cancelled fetch that found jobs", e.Waiter)
			}
			m.answer(op, e.Waiter)
		case Respond:
			mw := m.waiters[e.Waiter]
			if mw == nil {
				fail("Respond to unknown waiter %d", e.Waiter)
			}
			if mw.phase != phWaiting && (!fromFetch(e.Waiter, phFetching) || mw.phase != phFetching) {
				fail("Respond to waiter %d, which is in phase %d", e.Waiter, mw.phase)
			}
			switch e.Reason {
			case ReasonTimeout:
				if m.now < mw.w.Deadline {
					fail("Respond{Timeout} to waiter %d before its deadline %s", e.Waiter, mw.w.Deadline)
				}
				if op == "FetchDone" && (fd.got != 0 || fd.failed) {
					fail("Respond{Timeout} to a fetch that did not come back empty")
				}
			case ReasonDraining:
				if !m.draining {
					fail("Respond{Draining} before Drain")
				}
				if op == "FetchDone" && (fd.got != 0 || fd.failed) {
					fail("Respond{Draining} to a fetch that did not come back empty")
				}
			case ReasonRejected:
				if op != "Arrive" || !m.draining {
					fail("Respond{Rejected} outside an Arrive after Drain")
				}
			case ReasonUnavailable:
				if op != "FetchDone" || !fd.failed {
					fail("Respond{Unavailable} without a failed fetch")
				}
			}
			m.answer(op, e.Waiter)
		case Arm:
			if i != len(effs)-1 || armSeen {
				fail("Arm is not the single last effect")
			}
			arm, armSeen = e.At, true
		}
	}

	// What each call may emit.
	for _, e := range effs {
		switch e.(type) {
		case Respond:
			if op == "Notify" || op == "DueAt" || op == "Cancel" {
				fail("%s must not answer a waiter", op)
			}
		case Fetch, Deliver, Release, Arm:
		}
		switch e.(type) {
		case Deliver, Release:
			if op != "FetchDone" {
				fail("%s must not deliver or release", op)
			}
		case Fetch, Respond, Arm:
		}
	}

	// A waiter that is parked must be woken by its deadline at the latest,
	// and nothing else is worth arming.
	var parked int
	var earliest Mono
	for _, mw := range m.waiters {
		if mw.phase == phWaiting {
			if parked == 0 || mw.w.Deadline < earliest {
				earliest = mw.w.Deadline
			}
			parked++
		}
	}
	switch {
	case parked > 0 && !armSeen:
		fail("%d waiters are parked and nothing is armed", parked)
	case parked > 0 && arm > earliest:
		fail("Arm at %s is after the earliest deadline %s", arm, earliest)
	case parked == 0 && armSeen:
		fail("Arm at %s with no waiter parked", arm)
	}
	if op == "Tick" && armSeen && arm <= m.now {
		fail("Arm at %s is not after the Tick instant: the shell would spin", arm)
	}
	if m.draining && parked > 0 {
		fail("%d waiters are parked while draining", parked)
	}

	// The core's counters match the model's.
	for _, q := range m.queues {
		var waiting, flying int
		for _, mw := range m.waiters {
			if mw.w.Queue != q {
				continue
			}
			switch mw.phase {
			case phWaiting:
				waiting++
			case phFetching, phGone:
				flying++
			case phCancelled, phDone:
			}
		}
		if got := m.c.Waiting(q); got != waiting {
			fail("Waiting(%s) = %d, want %d", q, got, waiting)
		}
		if got := m.c.InFlight(q); got != flying {
			fail("InFlight(%s) = %d, want %d", q, got, flying)
		}
		if flying > m.maxFly {
			fail("%d fetches in flight on %s, the bound is %d", flying, q, m.maxFly)
		}
	}
}

// finish ends the sequence the way a shutting down broker does: Drain, every
// outstanding fetch completes, one Tick, and then nothing may be left.
func (m *model) finish() {
	if !m.draining {
		m.drain()
	}
	for {
		ids := m.outstanding()
		if len(ids) == 0 {
			break
		}
		m.completeFetch(ids[rapid.IntRange(0, len(ids)-1).Draw(m.t, "finishing")])
	}
	m.now += time.Second
	m.check("Tick", m.c.Tick(m.now), nil)

	for _, q := range m.queues {
		if w, f := m.c.Waiting(q), m.c.InFlight(q); w != 0 || f != 0 {
			m.t.Fatalf("after the drain Waiting(%s), InFlight(%s) = %d, %d, want 0, 0", q, q, w, f)
		}
	}
	for id, mw := range m.waiters {
		switch {
		case mw.phase == phWaiting || mw.phase == phFetching || mw.phase == phGone:
			m.t.Fatalf("waiter %d is still in phase %d after the drain", id, mw.phase)
		case mw.phase == phCancelled && mw.answers != 0:
			m.t.Fatalf("waiter %d was cancelled while parked and still answered", id)
		case mw.phase == phDone && !mw.cancelledFetching && mw.answers != 1:
			m.t.Fatalf("waiter %d was not cancelled and was answered %d times", id, mw.answers)
		}
	}
}
