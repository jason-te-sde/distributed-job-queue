package dispatch

import (
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
)

// maxRing is the most tenants the model's shell holds in a ring.
const maxRing = 3

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
	fetched           bool // a Fetch was issued for it, or one failed while it was parked
}

// fetchDone describes the FetchDone call whose effects are being checked.
type fetchDone struct {
	id     WaiterID
	prev   phase // the phase before the call: phFetching or phGone
	got    int   // as reported to the core, even when the fetch failed
	failed bool
	full   bool // the fetch succeeded and found as many jobs as the waiter asked for
	// perTenant says the fetch took from named tenants, as the shell does while
	// its ring has tenants, and not across all of them.
	perTenant bool
}

// modelQueue is the model's own account of what the core knows about a queue.
// It follows the same rules as the core, written down a second time, so that
// the core is checked against them and not against itself. A queue the core
// has never heard of, or has dropped as idle, is the zero modelQueue.
type modelQueue struct {
	knownEmpty bool
	dirty      bool
	lastFetch  Mono
	dueAt      Mono
	// ring is how many tenants the shell's fair ring holds for the queue. The
	// shell fetches per tenant while it holds any, and across tenants once it
	// holds none. A short fetch per tenant takes at least one tenant out, and
	// only a notification puts one in: that is what keeps a chain of such
	// fetches finite.
	ring int
}

// expectation is what the model requires of the effects of the current call,
// worked out before the call from the model's own state.
type expectation struct {
	fetch    map[string]WaiterID // for each queue that must start a fetch, the waiter it is for
	timeouts map[WaiterID]bool   // the waiters a Tick or an Arrive must answer with Timeout
}

// model drives a Core with random calls and checks the effects of every call
// against its own record of the waiters and the queues. It reads the core only
// through the public counters.
type model struct {
	t        *rapid.T
	c        *Core
	maxFly   int
	poll     time.Duration
	now      Mono
	next     WaiterID
	queues   []string
	qs       map[string]*modelQueue
	waiters  map[WaiterID]*modelWaiter
	draining bool
	noDrain  bool // the sequence never drains

	want expectation
	// armed and armAt are the timer the shell holds: the Arm of the latest call.
	armed bool
	armAt Mono
}

func newModel(rt *rapid.T, noDrain bool) *model {
	cfg := Config{
		MaxInFlight:  rapid.IntRange(1, 6).Draw(rt, "maxInFlight"),
		PollInterval: time.Duration(rapid.IntRange(100, 3000).Draw(rt, "pollMs")) * time.Millisecond,
	}
	m := &model{
		t:       rt,
		c:       New(cfg),
		maxFly:  cfg.MaxInFlight,
		poll:    cfg.PollInterval,
		next:    1,
		qs:      map[string]*modelQueue{},
		waiters: map[WaiterID]*modelWaiter{},
		noDrain: noDrain,
	}
	for i := range rapid.IntRange(1, 3).Draw(rt, "queues") {
		name := fmt.Sprintf("q%d", i)
		m.queues = append(m.queues, name)
		m.qs[name] = &modelQueue{ring: rapid.IntRange(0, maxRing).Draw(rt, "ring")}
	}
	return m
}

// TestDispatchProperties checks the safety of random call sequences that end
// in a drain.
func TestDispatchProperties(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		m := newModel(rt, false)
		for range rapid.IntRange(1, 200).Draw(rt, "calls") {
			m.step()
		}
		m.finish()
	})
}

// TestDispatchLiveness checks that, with no drain, a shell that completes
// every fetch and calls Tick at the latest Arm leaves no waiter parked beyond
// its deadline plus one poll interval.
func TestDispatchLiveness(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		m := newModel(rt, true)
		for range rapid.IntRange(1, 200).Draw(rt, "calls") {
			m.step()
		}
		m.settle()
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
	case k < 990 || m.noDrain:
		m.tick()
	default:
		m.drain()
	}
}

func (m *model) queue() string {
	return m.queues[rapid.IntRange(0, len(m.queues)-1).Draw(m.t, "queue")]
}

func (m *model) newWant() {
	m.want = expectation{fetch: map[string]WaiterID{}, timeouts: map[WaiterID]bool{}}
}

// parkedIn returns the waiters parked on the queue, oldest first. Waiter ids
// grow with arrival, and a waiter that waits again keeps its place.
func (m *model) parkedIn(q string) []WaiterID {
	var ids []WaiterID
	for id, mw := range m.waiters {
		if mw.phase == phWaiting && mw.w.Queue == q {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

// flyingIn returns the fetches out for the queue, cancelled ones included.
func (m *model) flyingIn(q string) int {
	var n int
	for _, mw := range m.waiters {
		if (mw.phase == phFetching || mw.phase == phGone) && mw.w.Queue == q {
			n++
		}
	}
	return n
}

// pumpChoice returns the waiter a fetch must start for on the queue, given
// the waiters parked and the fetches out when it is tried, or zero.
func (m *model) pumpChoice(q string, parked []WaiterID, flying int) WaiterID {
	mq := m.qs[q]
	if m.draining || flying >= m.maxFly || (mq.knownEmpty && !mq.dirty) || len(parked) == 0 {
		return 0
	}
	return parked[0]
}

// wakeAndPump applies the wake rules of the queue, which are shared by Tick
// and Arrive, and returns the waiter a fetch must start for, or zero. A wake
// is a poll interval without a fetch while a waiter is parked, or a due time
// that has come. It reopens the queue, and with no free slot it marks the
// queue dirty instead. A fetch is tried after a wake, and after every Arrive
// whether or not it woke the queue; Tick tries one only after a wake.
func (m *model) wakeAndPump(q string, parked []WaiterID, always bool) WaiterID {
	mq := m.qs[q]
	wake := len(parked) > 0 && m.now-mq.lastFetch >= m.poll
	if mq.dueAt != 0 && mq.dueAt <= m.now {
		mq.dueAt = 0
		wake = true
	}
	flying := m.flyingIn(q)
	if wake {
		mq.knownEmpty = false
		if flying >= m.maxFly {
			mq.dirty = true
		}
	}
	if !wake && !always {
		return 0
	}
	return m.pumpChoice(q, parked, flying)
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

	m.newWant()
	if !m.draining {
		f := m.wakeAndPump(w.Queue, m.parkedIn(w.Queue), true) // the new waiter is parked
		if f != 0 {
			m.want.fetch[w.Queue] = f
		}
		// Not fetched, on a queue known empty, and asked for no wait: answered at once.
		if f != id && m.qs[w.Queue].knownEmpty && w.Deadline == w.Arrived {
			m.want.timeouts[id] = true
		}
	}
	m.call("Arrive", nil, func() []Effect { return m.c.Arrive(m.now, w) })
}

func (m *model) notify() {
	q := m.queue()
	mq := m.qs[q]
	mq.knownEmpty = false
	mq.dirty = true
	mq.ring = min(mq.ring+1, maxRing) // the notification names a tenant, which joins the ring
	m.newWant()
	if f := m.pumpChoice(q, m.parkedIn(q), m.flyingIn(q)); f != 0 {
		m.want.fetch[q] = f
	}
	m.call("Notify", nil, func() []Effect { return m.c.Notify(m.now, q) })
}

func (m *model) dueAt() {
	q := m.queue()
	var at Mono // zero clears the due time
	if rapid.IntRange(0, 5).Draw(m.t, "dueKind") > 0 {
		at = max(m.now+time.Duration(rapid.IntRange(-2000, 10_000).Draw(m.t, "dueDeltaMs"))*time.Millisecond, 1)
	}
	m.qs[q].dueAt = at
	m.newWant()
	m.call("DueAt", nil, func() []Effect { return m.c.DueAt(m.now, q, at) })
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
	m.completeFetch(ids[rapid.IntRange(0, len(ids)-1).Draw(m.t, "outstanding")])
}

// completeFetch ends the fetch of waiter id with a random result. A failed
// fetch may still list jobs: the shell's report is not trusted to be tidy. The
// fetch is per tenant, at random, while the shell's ring holds a tenant.
func (m *model) completeFetch(id WaiterID) {
	mw := m.waiters[id]
	q := mw.w.Queue
	mq := m.qs[q]
	got := rapid.IntRange(0, mw.w.Max).Draw(m.t, "got")
	failed := rapid.IntRange(0, 9).Draw(m.t, "failed") == 0
	perTenant := mq.ring > 0 && rapid.Bool().Draw(m.t, "perTenant")
	fd := &fetchDone{id: id, prev: mw.phase, got: got, failed: failed, full: !failed && got == mw.w.Max, perTenant: perTenant}
	scope := AcrossTenants
	if perTenant {
		scope = PerTenant
	}
	if perTenant && !failed && !fd.full {
		mq.ring-- // a tenant came up short, and left the ring
	}
	// The fetch may have left jobs behind if it came back full, or if it took
	// from named tenants and came back short. Only a short fetch across all
	// tenants finds the queue empty.
	more := fd.full || (!failed && perTenant)
	// The waiter waits again only if its fetch found nothing, it did not fail,
	// the broker is not draining, and its deadline is ahead.
	returns := fd.prev == phFetching && !failed && got == 0 && !m.draining && m.now < mw.w.Deadline
	if !failed {
		switch {
		case more:
			mq.knownEmpty = false
		case !mq.dirty:
			mq.knownEmpty = true
		}
	}
	if failed {
		// A failed fetch gives the waiters parked behind it the store's chance.
		for _, id := range m.parkedIn(q) {
			m.waiters[id].fetched = true
		}
	}
	m.newWant()
	if more || mq.dirty {
		parked := m.parkedIn(q)
		if returns {
			parked = append(parked, id)
			slices.Sort(parked)
		}
		if f := m.pumpChoice(q, parked, m.flyingIn(q)-1); f != 0 {
			m.want.fetch[q] = f
		}
	}
	m.call("FetchDone", fd, func() []Effect { return m.c.FetchDone(m.now, id, got, failed, scope) })
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
	m.newWant()
	m.call("Cancel", nil, func() []Effect { return m.c.Cancel(m.now, id) })
}

func (m *model) tick() {
	m.now += time.Duration(rapid.IntRange(0, 5000).Draw(m.t, "tickPauseMs")) * time.Millisecond
	m.tickNow()
}

// tickNow calls Tick at the current instant. A Tick answers with Timeout the
// parked waiters that are past their deadline and covered (a fetch served
// them, or the queue was known empty before the Tick), then wakes each queue.
func (m *model) tickNow() {
	m.newWant()
	for _, q := range m.queues {
		mq := m.qs[q]
		var left []WaiterID
		for _, id := range m.parkedIn(q) {
			mw := m.waiters[id]
			if mw.w.Deadline <= m.now && (mw.fetched || mq.knownEmpty) {
				m.want.timeouts[id] = true
			} else {
				left = append(left, id)
			}
		}
		if f := m.wakeAndPump(q, left, false); f != 0 {
			m.want.fetch[q] = f
		}
	}
	m.call("Tick", nil, func() []Effect { return m.c.Tick(m.now) })
}

func (m *model) drain() {
	m.draining = true
	m.newWant()
	m.call("Drain", nil, func() []Effect { return m.c.Drain(m.now) })
}

// call runs one core call and checks its effects.
func (m *model) call(op string, fd *fetchDone, f func() []Effect) {
	m.t.Helper()
	m.check(op, f(), fd)
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
// violation, and then compares the effects with the expectation, the Arm with
// what the shell needs, and the core's counters with the model's.
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

	fetched := map[string]WaiterID{}
	fetchesByQueue := map[string]int{}
	timeouts := map[WaiterID]bool{}
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
			fetchesByQueue[e.Queue]++
			if fetchesByQueue[e.Queue] > 1 {
				fail("more than one Fetch for queue %s from one %s", e.Queue, op)
			}
			fetched[e.Queue] = e.Waiter
			mq := m.qs[e.Queue]
			mq.dirty = false
			mq.lastFetch = m.now
			mw.fetched = true
			mw.phase = phFetching
		case Deliver:
			if !fromFetch(e.Waiter, phFetching) || fd.got == 0 || fd.failed {
				fail("Deliver to waiter %d without a successful fetch that found jobs", e.Waiter)
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
				timeouts[e.Waiter] = true
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
	m.armed, m.armAt = armSeen, arm

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

	// The effects are exactly what the model's rules call for: a fetch starts
	// when and only when a slot is free, a waiter is parked and the queue is
	// not known empty (or is dirty), for the oldest parked waiter; and a Tick
	// answers every covered waiter past its deadline, and no other.
	if !maps.Equal(fetched, m.want.fetch) {
		fail("fetches %v (queue to waiter), want %v", fetched, m.want.fetch)
	}
	if (op == "Tick" || op == "Arrive") && !maps.Equal(timeouts, m.want.timeouts) {
		fail("waiters answered with Timeout %v, want %v", slices.Sorted(maps.Keys(timeouts)), slices.Sorted(maps.Keys(m.want.timeouts)))
	}

	// What the shell needs armed. A parked waiter that may be told "nothing"
	// (a fetch looked for it, or its queue is known empty) must be woken by
	// its deadline at the latest. A parked waiter on a queue with a free
	// fetch slot needs the poll, no later than a poll interval after the last
	// fetch. A due time on a queue with a parked waiter needs a wake. A waiter
	// that waits for a busy slot needs nothing: the fetch that frees the slot
	// reports back. And nothing is armed when no waiter is parked.
	var parked int
	var needsPoll, haveCovered bool
	var earliest, pollBy, dueBy Mono
	var haveDue bool
	for _, mw := range m.waiters {
		if mw.phase != phWaiting {
			continue
		}
		parked++
		mq := m.qs[mw.w.Queue]
		if m.flyingIn(mw.w.Queue) < m.maxFly {
			if !needsPoll || mq.lastFetch+m.poll < pollBy {
				pollBy = mq.lastFetch + m.poll
			}
			needsPoll = true
		}
		if mq.dueAt != 0 && (!haveDue || mq.dueAt < dueBy) {
			dueBy, haveDue = mq.dueAt, true
		}
		if mw.fetched || mq.knownEmpty {
			if !haveCovered || mw.w.Deadline < earliest {
				earliest = mw.w.Deadline
			}
			haveCovered = true
		}
	}
	switch {
	case haveCovered && !armSeen:
		fail("a parked waiter can time out and nothing is armed")
	case haveCovered && arm > earliest:
		fail("Arm at %s is after the earliest deadline %s of a parked waiter that can time out", arm, earliest)
	case needsPoll && !armSeen:
		fail("a parked waiter has a free fetch slot and nothing is armed for the poll")
	case needsPoll && arm > pollBy:
		fail("Arm at %s is after the poll at %s, a poll interval past the last fetch", arm, pollBy)
	case haveDue && !armSeen:
		fail("a due time and a parked waiter and nothing is armed")
	case haveDue && arm > dueBy:
		fail("Arm at %s is after the due time %s", arm, dueBy)
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
	m.tickNow()

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

// settle runs the shell's part to the end without a drain: every fetch
// completes, and Tick is called at the latest Arm. No waiter may then be left
// parked beyond its deadline plus one poll interval: the latest instant by
// which every waiter has either been served by a fetch or told that the
// queue is empty.
func (m *model) settle() {
	var last Mono
	for _, mw := range m.waiters {
		last = max(last, mw.w.Deadline)
	}
	limit := last + m.poll // every waiter's deadline plus a poll interval is at or before this
	for i := 0; ; i++ {
		if i > 100_000 {
			m.t.Fatalf("settling did not end: the shell is kept busy at %s", m.now)
		}
		if ids := m.outstanding(); len(ids) > 0 {
			m.completeFetch(ids[rapid.IntRange(0, len(ids)-1).Draw(m.t, "settling")])
			continue
		}
		if !m.armed || m.armAt > limit {
			break
		}
		m.now = max(m.now, m.armAt)
		m.tickNow()
	}
	for id, mw := range m.waiters {
		if mw.phase == phWaiting {
			m.t.Fatalf("waiter %d is still parked at %s, with its deadline at %s and a poll interval of %s; armed %v at %s",
				id, m.now, mw.w.Deadline, m.poll, m.armed, m.armAt)
		}
	}
}
