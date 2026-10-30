package lease

import (
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
)

// The model below is the smallest shell and server that can exercise the
// tracker's safety rules. It is a discrete-event loop in a single goroutine:
//
//   - The shell side performs the tracker's effects. Every send effect
//     becomes one reply event after a random delay of 0 to 8s, and one send in
//     five fails. One Lease reply in eight also stalls for another 25s or
//     more, long enough for its grants to arrive past their deadline. A
//     handler is an event too: it returns after a random time, or never.
//   - The server side keeps the true expiry of every lease it granted. The
//     instant at which it served a request is drawn between the instant the
//     request was sent (plus the wait a grant reports) and the instant its
//     reply arrives, so a reply that took long was served early. That is
//     exactly the gap in which an anchor at the arrival would overshoot.
//   - The single armed timer fires at the tracker's latest Arm, before any
//     reply that falls at the same instant. A Tick may also come at random
//     extra instants, because a shell is allowed to call it spuriously.
//   - A Drain call comes after a drawn number of events, so the run ends in a
//     graceful stop, which must come.
//
// Every violated rule is reported by the model as soon as it is seen.

const (
	// maxReplyDelay is the longest a reply takes.
	maxReplyDelay = 8 * time.Second
	// maxHandlerRun is the longest a handler runs before it returns.
	maxHandlerRun = 40 * time.Second
	// maxSteps bounds one run, so that a livelock fails instead of hanging.
	maxSteps = 100000
)

// event is one scheduled delivery, ordered by time and then by creation.
type event struct {
	at   Mono
	ord  int
	dead bool // dropped before delivery, as a cancelled RPC is
	run  func()
}

// model holds the shell's and the server's state for one run.
type model struct {
	t   *rapid.T
	cfg Config
	tr  *Tracker

	now    Mono
	events []*event
	ord    int
	armed  bool // whether the one timer is set
	armAt  Mono

	// What the tracker has asked for so far.
	lastSeq         uint64
	outstanding     uint64            // seq of the Lease request awaiting its reply, or 0
	requestEvent    map[uint64]*event // the pending reply of each Lease request
	started         map[Ref]bool      // StartHandler seen
	returned        map[Ref]bool      // the handler has returned
	cancels         map[Ref]int       // CancelHandler count per started handler
	released        map[Ref]bool      // named by a SendRelease
	acked           map[Ref]bool      // named by a SendAcks
	handlerEvent    map[Ref]*event    // when the handler will return, if it ever does
	running         int               // started handlers that have not returned
	acksInFlight    int
	releasesPending int
	draining        bool
	drainedAt       Mono
	stopped         bool

	// The server: the true expiry of each lease it granted, and the leases it
	// revoked for a reason other than expiry.
	expiry  map[Ref]Mono
	revoked map[Ref]bool
	nextID  int64
}

// delay draws a duration in [0, limit], in whole milliseconds.
func (m *model) delay(label string, limit time.Duration) time.Duration {
	return time.Duration(rapid.Int64Range(0, int64(limit/time.Millisecond)).Draw(m.t, label)) * time.Millisecond
}

// within draws an instant in [lo, hi], in whole microseconds. It returns lo
// when the interval is empty.
func (m *model) within(label string, lo, hi Mono) Mono {
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(rapid.Int64Range(0, int64((hi-lo)/time.Microsecond)).Draw(m.t, label))*time.Microsecond
}

// oneIn draws true one time in n.
func (m *model) oneIn(label string, n int) bool {
	return rapid.IntRange(0, n-1).Draw(m.t, label) == 0
}

// after schedules f to run d from now.
func (m *model) after(d time.Duration, f func()) *event {
	m.ord++
	ev := &event{at: m.now + d, ord: m.ord, run: f}
	m.events = append(m.events, ev)
	return ev
}

// earliest returns the next live event, or nil.
func (m *model) earliest() *event {
	var best *event
	for _, ev := range m.events {
		if ev.dead {
			continue
		}
		if best == nil || ev.at < best.at || (ev.at == best.at && ev.ord < best.ord) {
			best = ev
		}
	}
	return best
}

// remove drops a delivered event.
func (m *model) remove(ev *event) {
	m.events = slices.DeleteFunc(m.events, func(e *event) bool { return e == ev || e.dead })
}

// do checks the effects a call returned and carries them out. A call that
// returned an error is a failure: the model never breaks the protocol.
func (m *model) do(effs []Effect, err error) {
	m.t.Helper()
	if err != nil {
		m.t.Fatalf("at %v: unexpected error: %v", m.now, err)
	}
	var discarded []Ref
	var releasedNow []Ref
	for i, e := range effs {
		last := i == len(effs)-1
		switch e := e.(type) {
		case RequestLeases:
			m.requestLeases(e)
		case AbandonLeaseRequest:
			m.abandonLeaseRequest(e)
		case StartHandler:
			m.startHandler(e)
		case CancelHandler:
			m.cancelHandler(e)
		case SendHeartbeat:
			m.sendHeartbeat(e)
		case SendAcks:
			m.sendAcks(e)
		case SendRelease:
			m.sendRelease(e)
			releasedNow = append(releasedNow, e.Refs...)
		case Discard:
			if m.started[e.Ref] {
				m.t.Fatalf("at %v: Discard of started handler %+v", m.now, e.Ref)
			}
			discarded = append(discarded, e.Ref)
		case Arm:
			if !last {
				m.t.Fatalf("at %v: Arm is not the last effect of %v", m.now, effs)
			}
			m.armed, m.armAt = true, e.At
		case Stopped:
			if !last {
				m.t.Fatalf("at %v: Stopped is not the last effect of %v", m.now, effs)
			}
			m.stop()
		}
	}
	for _, ref := range discarded {
		if !slices.Contains(releasedNow, ref) {
			m.t.Fatalf("at %v: discarded grant %+v is not released in the same call", m.now, ref)
		}
	}
	if m.running > m.cfg.Concurrency {
		m.t.Fatalf("at %v: %d handlers run with Concurrency %d", m.now, m.running, m.cfg.Concurrency)
	}
}

// newSeq checks that a send effect carries a seq no earlier effect used.
func (m *model) newSeq(seq uint64) {
	m.t.Helper()
	if seq <= m.lastSeq {
		m.t.Fatalf("at %v: seq %d does not follow %d", m.now, seq, m.lastSeq)
	}
	m.lastSeq = seq
}

func (m *model) requestLeases(e RequestLeases) {
	m.t.Helper()
	m.newSeq(e.Seq)
	switch {
	case m.draining:
		m.t.Fatalf("at %v: a Lease request after Drain", m.now)
	case m.outstanding != 0:
		m.t.Fatalf("at %v: Lease request %d while %d is outstanding", m.now, e.Seq, m.outstanding)
	case e.Max != min(m.cfg.Concurrency-m.running, job.MaxBatch) || e.Max < 1:
		m.t.Fatalf("at %v: Lease request Max %d with %d of %d slots free", m.now, e.Max, m.cfg.Concurrency-m.running, m.cfg.Concurrency)
	case e.Wait != m.cfg.LongPoll || e.TTL != m.cfg.TTL:
		m.t.Fatalf("at %v: Lease request %+v does not carry the configured wait and TTL", m.now, e)
	}
	m.outstanding = e.Seq
	sentAt := m.now
	failed := m.oneIn("lease request fails", 5)
	d := m.delay("lease reply delay", maxReplyDelay)
	if m.oneIn("lease reply stalls", 8) {
		d += 25*time.Second + m.delay("lease stall", 10*time.Second)
	}
	m.requestEvent[e.Seq] = m.after(d, func() {
		if m.outstanding == e.Seq {
			m.outstanding = 0
		}
		if failed {
			m.do(m.tr.LeasesFailed(m.now, e.Seq), nil)
			return
		}
		// The server waited, then served the request somewhere between the
		// end of that wait and now.
		waited := m.within("lease wait", 0, min(e.Wait, m.now-sentAt))
		served := m.within("lease served at", sentAt+waited, m.now)
		n := rapid.IntRange(0, e.Max).Draw(m.t, "grants")
		out := make([]Grant, n)
		for i := range out {
			m.nextID++
			ref := Ref{ID: m.nextID, Queue: "q", Token: m.nextID*10 + 1}
			m.expiry[ref] = served + e.TTL
			out[i] = Grant{Ref: ref, Attempt: 1, Remaining: e.TTL}
		}
		m.do(m.tr.LeasesGranted(m.now, e.Seq, sentAt, waited, out))
	})
}

func (m *model) abandonLeaseRequest(e AbandonLeaseRequest) {
	m.t.Helper()
	if e.Seq != m.outstanding {
		m.t.Fatalf("at %v: AbandonLeaseRequest %d, but %d is outstanding", m.now, e.Seq, m.outstanding)
	}
	m.outstanding = 0
	// The cancelled RPC may still have been answered: the reply then arrives
	// for a request the tracker no longer waits for.
	if m.oneIn("abandoned reply is dropped", 2) {
		m.requestEvent[e.Seq].dead = true
	}
}

func (m *model) startHandler(e StartHandler) {
	m.t.Helper()
	ref := e.Grant.Ref
	if m.started[ref] || m.released[ref] {
		m.t.Fatalf("at %v: StartHandler for %+v, which was started or released before", m.now, ref)
	}
	// A handler never starts on a lease with less than the margin left.
	if exp := m.expiry[ref]; m.now >= exp-m.cfg.Margin {
		m.t.Fatalf("at %v: StartHandler for %+v, whose lease expires at %v (margin %v)", m.now, ref, exp, m.cfg.Margin)
	}
	m.started[ref] = true
	m.running++
	if !m.oneIn("handler hangs", 12) {
		m.handlerEvent[ref] = m.after(m.delay("handler run time", maxHandlerRun), func() { m.handlerReturns(ref) })
	}
}

func (m *model) handlerReturns(ref Ref) {
	m.returned[ref] = true
	m.running--
	delete(m.handlerEvent, ref)
	kinds := []OutcomeKind{OutcomeSuccess, OutcomeRetry, OutcomePermanent}
	out := Outcome{Kind: rapid.SampledFrom(kinds).Draw(m.t, "outcome")}
	m.do(m.tr.HandlerDone(m.now, ref, out))
}

func (m *model) cancelHandler(e CancelHandler) {
	m.t.Helper()
	if !m.started[e.Ref] {
		m.t.Fatalf("at %v: CancelHandler for %+v, which was never started", m.now, e.Ref)
	}
	m.cancels[e.Ref]++
	if m.cancels[e.Ref] > 1 {
		m.t.Fatalf("at %v: handler %+v was cancelled %d times", m.now, e.Ref, m.cancels[e.Ref])
	}
	// A handler that notices the cancellation may return early.
	if ev, ok := m.handlerEvent[e.Ref]; ok && m.oneIn("handler heeds the cancel", 2) {
		ev.at = min(ev.at, m.now+m.delay("handler stop time", 2*time.Second))
	}
}

func (m *model) sendHeartbeat(e SendHeartbeat) {
	m.t.Helper()
	m.newSeq(e.Seq)
	if len(e.Refs) == 0 || len(e.Refs) > m.cfg.AckBatch || e.TTL != m.cfg.TTL {
		m.t.Fatalf("at %v: malformed heartbeat %+v", m.now, e)
	}
	for _, ref := range e.Refs {
		if !m.started[ref] || m.released[ref] {
			m.t.Fatalf("at %v: heartbeat names %+v, which is not started or was released", m.now, ref)
		}
	}
	sentAt := m.now
	failed := m.oneIn("heartbeat fails", 5)
	m.after(m.delay("heartbeat reply delay", maxReplyDelay), func() {
		if failed {
			m.do(m.tr.HeartbeatFailed(m.now, e.Seq), nil)
			return
		}
		served := m.within("heartbeat served at", sentAt, m.now)
		statuses := make([]HeartbeatStatus, len(e.Refs))
		for i, ref := range e.Refs {
			if m.oneIn("server revokes", 25) {
				m.revoked[ref] = true
			}
			if m.revoked[ref] || served >= m.expiry[ref] {
				continue // not held: the zero status
			}
			m.expiry[ref] = max(m.expiry[ref], served+e.TTL)
			statuses[i] = HeartbeatStatus{Held: true, Remaining: m.expiry[ref] - served}
		}
		m.do(m.tr.HeartbeatDone(m.now, e.Seq, sentAt, statuses))
	})
}

func (m *model) sendAcks(e SendAcks) {
	m.t.Helper()
	m.newSeq(e.Seq)
	m.acksInFlight++
	if m.acksInFlight > 1 {
		m.t.Fatalf("at %v: two ack batches in flight", m.now)
	}
	if len(e.Acks) == 0 || len(e.Acks) > m.cfg.AckBatch {
		m.t.Fatalf("at %v: SendAcks with %d acks, AckBatch %d", m.now, len(e.Acks), m.cfg.AckBatch)
	}
	for _, a := range e.Acks {
		if m.released[a.Ref] { // invariant (e)
			m.t.Fatalf("at %v: ack names released lease %+v", m.now, a.Ref)
		}
		if !m.returned[a.Ref] {
			m.t.Fatalf("at %v: ack names %+v, whose handler has not returned", m.now, a.Ref)
		}
		m.acked[a.Ref] = true
	}
	failed := m.oneIn("acks fail", 5)
	m.after(m.delay("ack reply delay", maxReplyDelay), func() {
		m.acksInFlight--
		if failed {
			m.do(m.tr.AcksFailed(m.now, e.Seq), nil)
			return
		}
		statuses := make([]AckStatus, len(e.Acks))
		for i := range statuses {
			statuses[i] = rapid.SampledFrom([]AckStatus{AckAccepted, AckDuplicate, AckFenced}).Draw(m.t, "ack status")
		}
		m.do(m.tr.AcksDone(m.now, e.Seq, statuses))
	})
}

func (m *model) sendRelease(e SendRelease) {
	m.t.Helper()
	m.newSeq(e.Seq)
	if len(e.Refs) == 0 || len(e.Refs) > m.cfg.AckBatch {
		m.t.Fatalf("at %v: SendRelease with %d refs, AckBatch %d", m.now, len(e.Refs), m.cfg.AckBatch)
	}
	for _, ref := range e.Refs {
		m.released[ref] = true
	}
	m.releasesPending++
	m.after(m.delay("release reply delay", maxReplyDelay), func() {
		m.releasesPending--
		m.do(m.tr.ReleaseDone(m.now, e.Seq), nil) // best effort: success or failure
	})
}

// stop checks what a graceful stop promises.
func (m *model) stop() {
	m.t.Helper()
	m.stopped = true
	if !m.draining {
		m.t.Fatalf("at %v: Stopped without a Drain", m.now)
	}
	if m.running > 0 && m.now < m.drainedAt+m.cfg.DrainTimeout {
		m.t.Fatalf("at %v: Stopped with %d handlers running before the drain deadline %v", m.now, m.running, m.drainedAt+m.cfg.DrainTimeout)
	}
	if m.acksInFlight != 0 || m.releasesPending != 0 {
		m.t.Fatalf("at %v: Stopped with %d ack and %d release batches in flight", m.now, m.acksInFlight, m.releasesPending)
	}
	for ref := range m.returned {
		if !m.released[ref] && !m.acked[ref] {
			m.t.Fatalf("at %v: Stopped, but the finished handler of %+v was never acked", m.now, ref)
		}
	}
}

// check evaluates the rules that must hold between any two events.
func (m *model) check() {
	m.t.Helper()
	held := m.tr.Held(m.now)
	heldSet := make(map[Ref]bool, len(held))
	for i, h := range held {
		heldSet[h.Ref] = true
		exp, ok := m.expiry[h.Ref]
		if !ok {
			m.t.Fatalf("at %v: Held names %+v, which the server never granted", m.now, h.Ref)
		}
		// Invariant (a): the local deadline is always at least a margin
		// ahead of the lease's true expiry.
		if h.Deadline > exp-m.cfg.Margin {
			m.t.Fatalf("at %v: lease %+v has deadline %v but expires at %v (margin %v)", m.now, h.Ref, h.Deadline, exp, m.cfg.Margin)
		}
		if h.Deadline <= m.now || (i > 0 && held[i-1].Ref.ID >= h.Ref.ID) {
			m.t.Fatalf("at %v: Held is not ascending and unexpired: %+v", m.now, held)
		}
	}
	// A handler that was not cancelled runs on a lease the tracker still
	// considers held.
	for ref := range m.started {
		if !m.returned[ref] && m.cancels[ref] == 0 && !heldSet[ref] {
			m.t.Fatalf("at %v: handler %+v runs uncancelled on a lease that is not held", m.now, ref)
		}
	}
	// Invariant (b).
	if m.running > m.cfg.Concurrency {
		m.t.Fatalf("at %v: %d handlers run with Concurrency %d", m.now, m.running, m.cfg.Concurrency)
	}
}

// run drives the tracker until it stops.
func (m *model) run(drainAfter int) {
	m.t.Helper()
	m.do(m.tr.Start(m.now), nil)
	for step := 0; !m.stopped; step++ {
		if step > maxSteps {
			m.t.Fatalf("no Stopped after %d steps (draining %v, now %v)", maxSteps, m.draining, m.now)
		}
		if step == drainAfter {
			m.draining, m.drainedAt = true, m.now
			m.do(m.tr.Drain(m.now), nil)
			m.check()
			continue
		}
		if !m.draining && m.oneIn("spurious tick", 10) {
			m.after(m.delay("spurious tick delay", 3*time.Second), func() { m.do(m.tr.Tick(m.now), nil) })
		}
		ev := m.earliest()
		switch {
		case m.armed && (ev == nil || m.armAt <= ev.at):
			m.now = max(m.now, m.armAt)
			m.armed = false
			effs := m.tr.Tick(m.now)
			m.do(effs, nil)
			if !m.stopped && (!m.armed || m.armAt <= m.now) {
				m.t.Fatalf("at %v: Tick left the timer at %v (armed %v), want a later instant", m.now, m.armAt, m.armed)
			}
		case ev != nil:
			m.now = max(m.now, ev.at)
			m.remove(ev)
			ev.run()
		default:
			m.t.Fatalf("at %v: nothing is scheduled and the tracker has not stopped", m.now)
		}
		if !m.stopped {
			m.check()
		}
	}
}

func TestTrackerProperties(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		cfg := Config{
			Concurrency: rapid.SampledFrom([]int{1, 2, 3, 6, 70}).Draw(t, "concurrency"),
			TTL:         30 * time.Second,
			Margin:      3 * time.Second,
			AckWindow:   2 * time.Millisecond,
			AckBatch:    rapid.SampledFrom([]int{1, 2, 3, 128}).Draw(t, "ack batch"),
		}.WithDefaults()
		tr, err := New(cfg, rapid.Uint64().Draw(t, "seed"))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		m := &model{
			t: t, cfg: cfg, tr: tr,
			requestEvent: map[uint64]*event{},
			started:      map[Ref]bool{},
			returned:     map[Ref]bool{},
			cancels:      map[Ref]int{},
			released:     map[Ref]bool{},
			acked:        map[Ref]bool{},
			handlerEvent: map[Ref]*event{},
			expiry:       map[Ref]Mono{},
			revoked:      map[Ref]bool{},
		}
		m.run(rapid.IntRange(0, 400).Draw(t, "events before drain"))
	})
}
