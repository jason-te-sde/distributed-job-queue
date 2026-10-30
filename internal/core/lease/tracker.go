package lease

import (
	"cmp"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/flaw"
)

const (
	// seedStream is the second word of the generator's seed. The first is
	// the caller's seed.
	seedStream = 0x9e3779b97f4a7c15
	// maxLeaseFailures stops the failure count where the backoff ceiling has
	// long reached its cap, so that the count cannot overflow.
	maxLeaseFailures = 64
	// maxAckRetryDelay caps one wait between ack resends, so that a
	// configuration with very many retries cannot overflow a duration.
	maxAckRetryDelay = 24 * time.Hour
)

// leaseState is where a lease is in its life.
type leaseState uint8

const (
	// stateRunning is a lease whose handler is active.
	stateRunning leaseState = iota + 1
	// stateAcking is a lease whose handler returned and whose ack is pending
	// or in flight.
	stateAcking
	// stateLost is a lease that is no longer heartbeated: the broker denied
	// it or its local deadline passed. Its handler may still run.
	stateLost
	// stateReleased is a lease given back at the drain deadline. Its handler
	// may still run, and its end is dropped.
	stateReleased
)

// lease is the tracker's record of one grant.
type lease struct {
	ref      Ref
	attempt  int32
	state    leaseState
	handler  bool // the handler has not returned: the lease holds a slot
	deadline Mono
}

// live reports whether the lease is heartbeated and held.
func (l *lease) live() bool { return l.state == stateRunning || l.state == stateAcking }

// ackBatch is the one ack call that is in flight or waiting to be resent.
type ackBatch struct {
	seq      uint64
	acks     []Ack
	failures int
	inFlight bool
	retryAt  Mono // when to resend, while not in flight
}

// Tracker is the worker's lease tracker. Create one with New.
type Tracker struct {
	cfg     Config
	period  time.Duration // heartbeat period, TTL/4
	backoff job.BackoffPolicy
	rng     *rand.Rand

	started  bool
	draining bool
	stopped  bool
	nextSeq  uint64

	leases  map[Ref]*lease
	byID    map[int64]int // tracked leases per job id, in any state
	running int           // leases whose handler has not returned

	// Lease requests: at most one is outstanding.
	reqSeq        uint64
	reqMax        int
	leaseFailures int // consecutive failed requests
	retryPending  bool
	retryAt       Mono

	// Heartbeats.
	hbNext      Mono
	hbInflight  map[uint64][]Ref
	hbRetryUsed bool // the immediate retry of this period was spent

	// Acks: a pending list, and at most one batch in flight or waiting.
	pending      []Ack
	pendingSince Mono
	batch        *ackBatch

	// Releases in flight.
	releasing map[uint64]struct{}

	// Drain.
	drainDeadline Mono
	drainFired    bool

	// minDeadline caches the earliest deadline of a live lease.
	minDeadline Mono
	minKnown    bool // whether a live lease exists
	minDirty    bool // whether the cache must be recomputed
}

// New returns a tracker for cfg, with defaults filled in, or an error that
// wraps job.ErrInvalid. The seed drives the jitter of the lease-request
// backoff and nothing else.
func New(cfg Config, seed uint64) (*Tracker, error) {
	cfg = cfg.WithDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Tracker{
		cfg:        cfg,
		period:     cfg.TTL / 4,
		backoff:    job.BackoffPolicy{Base: cfg.LeaseRetryBase, Cap: cfg.LeaseRetryCap},
		rng:        rand.New(rand.NewPCG(seed, seedStream)),
		leases:     map[Ref]*lease{},
		byID:       map[int64]int{},
		hbInflight: map[uint64][]Ref{},
		releasing:  map[uint64]struct{}{},
	}, nil
}

// Start begins the first period and asks for work. Later calls do nothing.
func (t *Tracker) Start(now Mono) []Effect {
	if t.started || t.draining {
		return nil
	}
	t.started = true
	t.hbNext = now + t.period
	return t.advance(now, nil)
}

// LeasesGranted handles the answer to the Lease request seq, which was sent
// at sentAt and which the broker says waited for the given time before it
// found work. An answer for a request that is not outstanding, or that comes
// while draining, has all its grants discarded and released. Otherwise every
// grant starts a handler, except one whose local deadline has already passed
// on arrival: that one is discarded and released the same way, because a
// handler must never run on a lease the tracker would cancel at once.
//
// A response with more grants than the request's Max, or with a grant whose
// job still has a lease with a running handler, is an *InvariantError. The
// call then changes nothing. A job whose old lease is acking, lost or released
// is not a conflict: see holdsRunning.
//
// After Stopped an answer that arrives late still has its grants discarded and
// released, so that no lease is left to run out. That list has no Arm.
func (t *Tracker) LeasesGranted(now Mono, seq uint64, sentAt Mono, waited time.Duration, grants []Grant) ([]Effect, error) {
	if t.stopped {
		return t.discard(grants), nil
	}
	if t.draining || t.reqSeq == 0 || seq != t.reqSeq {
		return t.advance(now, t.discard(grants)), nil
	}
	if err := t.checkGrants(grants); err != nil {
		return nil, err
	}
	t.reqSeq = 0
	t.leaseFailures = 0
	var out []Effect
	var unrun []Ref
	for _, g := range grants {
		d := t.grantDeadline(now, sentAt, waited, g.Remaining)
		if d <= now {
			out = append(out, Discard{Ref: g.Ref})
			unrun = append(unrun, g.Ref)
			continue
		}
		t.leases[g.Ref] = &lease{ref: g.Ref, attempt: g.Attempt, state: stateRunning, handler: true, deadline: d}
		t.byID[g.Ref.ID]++
		t.running++
		if !t.minDirty && (!t.minKnown || d < t.minDeadline) {
			t.minDeadline, t.minKnown = d, true
		}
		out = append(out, StartHandler{Grant: g})
	}
	return t.advance(now, t.sendRelease(unrun, out)), nil
}

// grantDeadline is the local deadline of a new lease. The broker leased the
// job no earlier than waited after the request was sent, so that instant plus
// the remaining time is a lower bound of the lease's expiry, whatever the
// answer's latency.
func (t *Tracker) grantDeadline(now, sentAt Mono, waited, remaining time.Duration) Mono {
	if flaw.On(flaw.F5) {
		return now + remaining - t.cfg.Margin
	}
	return sentAt + waited + remaining - t.cfg.Margin
}

// checkGrants reports a response that no correct broker and shell can
// produce. It does not change the tracker.
func (t *Tracker) checkGrants(grants []Grant) error {
	if len(grants) > t.reqMax {
		return &InvariantError{Ref: grants[t.reqMax].Ref,
			Msg: fmt.Sprintf("response has %d grants for a request of at most %d", len(grants), t.reqMax)}
	}
	seen := make(map[int64]bool, len(grants))
	for _, g := range grants {
		switch {
		case seen[g.Ref.ID]:
			return &InvariantError{Ref: g.Ref, Msg: "job granted twice in one response"}
		case t.leases[g.Ref] != nil:
			return &InvariantError{Ref: g.Ref, Msg: "lease is already tracked"}
		case t.holdsRunning(g.Ref.ID):
			return &InvariantError{Ref: g.Ref, Msg: "job is already held by a lease with a running handler"}
		}
		seen[g.Ref.ID] = true
	}
	return nil
}

// holdsRunning reports whether a lease of job id is running its handler. Only
// that conflicts with a new grant of the job: it means the broker leased the
// job again while this worker still held it, so the local deadline was wrong.
// Any other old lease may legitimately sit next to the new one:
//   - an acking lease has finished its handler and only waits for its ack. The
//     broker may have applied that ack already, for example a retry with a short
//     delay, and offered the job again while the ack's answer was lost or the
//     resend is pending;
//   - a lost or released lease may still have a handler winding down.
//
// The new lease is tracked under its own Ref. The old one gets Held=false at
// its next heartbeat, or is forgotten when its ack settles.
func (t *Tracker) holdsRunning(id int64) bool {
	if t.byID[id] == 0 {
		return false
	}
	for _, l := range t.leases {
		if l.ref.ID == id && l.state == stateRunning {
			return true
		}
	}
	return false
}

// discard turns grants that will not run into Discard effects and releases
// them.
func (t *Tracker) discard(grants []Grant) []Effect {
	if len(grants) == 0 {
		return nil
	}
	out := make([]Effect, 0, len(grants)+1)
	refs := make([]Ref, len(grants))
	for i, g := range grants {
		out = append(out, Discard{Ref: g.Ref})
		refs[i] = g.Ref
	}
	return t.sendRelease(refs, out)
}

// sendRelease appends SendRelease effects for refs, in chunks of AckBatch.
func (t *Tracker) sendRelease(refs []Ref, out []Effect) []Effect {
	for chunk := range slices.Chunk(refs, t.cfg.AckBatch) {
		seq := t.seq()
		t.releasing[seq] = struct{}{}
		out = append(out, SendRelease{Seq: seq, Refs: slices.Clone(chunk)})
	}
	return out
}

// LeasesFailed handles the failure of the Lease request seq. The next request
// goes out after a full-jitter delay that grows with each consecutive
// failure. A seq that is not outstanding is ignored.
func (t *Tracker) LeasesFailed(now Mono, seq uint64) []Effect {
	if t.stopped || t.reqSeq == 0 || seq != t.reqSeq {
		return nil
	}
	t.reqSeq = 0
	t.leaseFailures = min(t.leaseFailures+1, maxLeaseFailures)
	t.retryAt = now + t.backoff.Delay(int32(t.leaseFailures), t.rng.Float64())
	t.retryPending = true
	return t.advance(now, nil)
}

// HeartbeatDone handles the answer to the heartbeat seq, which was sent at
// sentAt, with one status per ref in the order sent. A held lease's deadline
// moves forward to the answer's remaining time counted from sentAt, and never
// back. A lease that is not held is lost, and a running handler of it is
// cancelled. An answer about a lease that is lost or gone already changes
// nothing.
//
// A seq that is not in flight, or statuses that do not match the refs sent,
// is an *InvariantError. The call then changes nothing.
func (t *Tracker) HeartbeatDone(now Mono, seq uint64, sentAt Mono, statuses []HeartbeatStatus) ([]Effect, error) {
	if t.stopped {
		return nil, nil
	}
	refs, ok := t.hbInflight[seq]
	if !ok {
		return nil, &InvariantError{Msg: fmt.Sprintf("heartbeat answer for seq %d, which is not in flight", seq)}
	}
	if len(statuses) != len(refs) {
		return nil, &InvariantError{Ref: refs[0],
			Msg: fmt.Sprintf("heartbeat answer has %d statuses for %d refs", len(statuses), len(refs))}
	}
	delete(t.hbInflight, seq)
	var out []Effect
	for i, ref := range refs {
		l := t.leases[ref]
		if l == nil || !l.live() {
			continue
		}
		if st := statuses[i]; st.Held {
			l.deadline = max(l.deadline, sentAt+st.Remaining-t.cfg.Margin)
			t.minDirty = true
		} else {
			out = t.markLost(l, CancelLost, out)
		}
	}
	return t.advance(now, out), nil
}

// HeartbeatFailed handles the failure of the heartbeat seq. The first failure
// in a heartbeat period sends that chunk again at once under a new seq. Later
// ones wait for the next firing. A seq that is not in flight is ignored.
func (t *Tracker) HeartbeatFailed(now Mono, seq uint64) []Effect {
	if t.stopped {
		return nil
	}
	refs, ok := t.hbInflight[seq]
	if !ok {
		return nil
	}
	delete(t.hbInflight, seq)
	var out []Effect
	if !t.hbRetryUsed {
		t.hbRetryUsed = true
		var live []Ref
		for _, ref := range refs {
			if l := t.leases[ref]; l != nil && l.live() {
				live = append(live, ref)
			}
		}
		out = t.sendHeartbeat(live, out)
	}
	return t.advance(now, out)
}

// sendHeartbeat appends one SendHeartbeat for refs, unless refs is empty.
func (t *Tracker) sendHeartbeat(refs []Ref, out []Effect) []Effect {
	if len(refs) == 0 {
		return out
	}
	seq := t.seq()
	t.hbInflight[seq] = refs
	return append(out, SendHeartbeat{Seq: seq, Refs: slices.Clone(refs), TTL: t.cfg.TTL})
}

// HandlerDone handles the return of the handler of ref. Its slot is free
// again. A lease released at the drain deadline is forgotten and the outcome
// dropped. Any other lease is acked, even a lost one: the broker's fencing
// decides whether the ack counts.
//
// A ref that is not tracked, or whose handler already returned, is an
// *InvariantError.
func (t *Tracker) HandlerDone(now Mono, ref Ref, out Outcome) ([]Effect, error) {
	if t.stopped {
		return nil, nil
	}
	l := t.leases[ref]
	switch {
	case l == nil:
		return nil, &InvariantError{Ref: ref, Msg: "handler returned for a lease that is not tracked"}
	case !l.handler:
		return nil, &InvariantError{Ref: ref, Msg: "handler returned twice"}
	}
	l.handler = false
	t.running--
	if l.state == stateReleased {
		t.forget(l)
	} else {
		if l.state == stateRunning {
			l.state = stateAcking
		}
		if len(t.pending) == 0 {
			t.pendingSince = now
		}
		t.pending = append(t.pending, Ack{Ref: ref, Attempt: l.attempt, Outcome: out})
	}
	return t.advance(now, nil), nil
}

// AcksDone handles the answer to the ack call seq, with one status per ack in
// the order sent. Every acked lease is forgotten whatever the status, and the
// acks that were waiting go out at once.
//
// A seq that is not in flight, or statuses that do not match the acks sent,
// is an *InvariantError. The call then changes nothing.
func (t *Tracker) AcksDone(now Mono, seq uint64, statuses []AckStatus) ([]Effect, error) {
	if t.stopped {
		return nil, nil
	}
	b := t.batch
	if b == nil || !b.inFlight || b.seq != seq {
		return nil, &InvariantError{Msg: fmt.Sprintf("ack answer for seq %d, which is not in flight", seq)}
	}
	if len(statuses) != len(b.acks) {
		return nil, &InvariantError{Ref: b.acks[0].Ref,
			Msg: fmt.Sprintf("ack answer has %d statuses for %d acks", len(statuses), len(b.acks))}
	}
	t.finishBatch()
	return t.advance(now, t.flushAcks(now, true, nil)), nil
}

// AcksFailed handles the failure of the ack call seq. The same acks go out
// again under a new seq after AckRetryBase doubled for each earlier failure.
// After AckRetries resends have failed too, the leases are forgotten, which
// lets them expire and be delivered again. A seq that is not in flight is
// ignored.
func (t *Tracker) AcksFailed(now Mono, seq uint64) []Effect {
	if t.stopped {
		return nil
	}
	b := t.batch
	if b == nil || !b.inFlight || b.seq != seq {
		return nil
	}
	b.failures++
	if b.failures > t.cfg.AckRetries {
		t.finishBatch()
		return t.advance(now, t.flushAcks(now, true, nil))
	}
	b.inFlight = false
	b.retryAt = now + ackRetryDelay(t.cfg.AckRetryBase, b.failures)
	return t.advance(now, nil)
}

// ackRetryDelay is base doubled k-1 times, at most maxAckRetryDelay.
func ackRetryDelay(base time.Duration, k int) time.Duration {
	d := base
	for i := 1; i < k && d < maxAckRetryDelay; i++ {
		d *= 2
	}
	return min(d, maxAckRetryDelay)
}

// finishBatch forgets the leases of the current ack batch and clears it.
func (t *Tracker) finishBatch() {
	for _, a := range t.batch.acks {
		if l := t.leases[a.Ref]; l != nil {
			t.forget(l)
		}
	}
	t.batch = nil
}

// ReleaseDone handles the end of the release call seq, whether it worked or
// not: release is best effort. A seq that is not in flight is ignored.
func (t *Tracker) ReleaseDone(now Mono, seq uint64) []Effect {
	if _, ok := t.releasing[seq]; !ok {
		return nil
	}
	delete(t.releasing, seq)
	if t.stopped {
		return nil // the release of a late answer's grants
	}
	return t.advance(now, nil)
}

// Tick does whatever is due at now: it gives up on leases past their
// deadline, fires the heartbeat, resends or flushes acks, retries the Lease
// request, and enforces the drain deadline. The shell calls it when the timer
// of the last Arm fires, and may call it at any other instant too.
func (t *Tracker) Tick(now Mono) []Effect {
	if t.stopped || !t.started {
		return nil
	}
	return t.advance(now, nil)
}

// Drain stops asking for work. It abandons the outstanding Lease request and
// waits up to DrainTimeout for handlers to return and for acks to settle,
// then Stopped follows. At the deadline the handlers still running are
// cancelled and their leases released. Handlers that never return are not
// waited for.
func (t *Tracker) Drain(now Mono) []Effect {
	if t.stopped || t.draining {
		return nil
	}
	t.draining = true
	t.drainDeadline = now + t.cfg.DrainTimeout
	t.retryPending = false
	var out []Effect
	if t.reqSeq != 0 {
		out = append(out, AbandonLeaseRequest{Seq: t.reqSeq})
		t.reqSeq = 0
	}
	return t.advance(now, out)
}

// Held lists the leases that the worker may act on at now: those that are not
// lost or released and whose deadline is after now, in ascending job id.
func (t *Tracker) Held(now Mono) []Held {
	ls := t.collect(func(l *lease) bool { return l.live() && l.deadline > now })
	out := make([]Held, len(ls))
	for i, l := range ls {
		out[i] = Held{Ref: l.ref, Deadline: l.deadline}
	}
	return out
}

// advance finishes a call. Whatever the event was, it does everything that is
// due at now or that the event enabled, in a fixed order, and ends with the
// Arm for the next instant at which something is due.
func (t *Tracker) advance(now Mono, out []Effect) []Effect {
	out = t.sweepDeadlines(now, out)
	out = t.fireDrainDeadline(now, out)
	out = t.fireHeartbeat(now, out)
	out = t.fireAckRetry(now, out)
	out = t.flushAcks(now, false, out)
	t.fireLeaseRetry(now)
	out = t.requestLeases(out)
	out = t.stopIfDone(out)
	return t.arm(out)
}

// sweepDeadlines loses every live lease whose deadline has passed.
func (t *Tracker) sweepDeadlines(now Mono, out []Effect) []Effect {
	if d, ok := t.earliestDeadline(); !ok || d > now {
		return out
	}
	for _, l := range t.collect(func(l *lease) bool { return l.live() && l.deadline <= now }) {
		out = t.markLost(l, CancelDeadline, out)
	}
	return out
}

// markLost stops heartbeating l and cancels its handler if it still runs. An
// acking lease keeps its ack.
func (t *Tracker) markLost(l *lease, why CancelReason, out []Effect) []Effect {
	l.state = stateLost
	t.minDirty = true
	if l.handler {
		out = append(out, CancelHandler{Ref: l.ref, Reason: why})
	}
	return out
}

// fireDrainDeadline, once, cancels the handlers still running at the drain
// deadline and releases their leases.
func (t *Tracker) fireDrainDeadline(now Mono, out []Effect) []Effect {
	if !t.draining || t.drainFired || now < t.drainDeadline {
		return out
	}
	t.drainFired = true
	var refs []Ref
	for _, l := range t.collect(func(l *lease) bool { return l.handler }) {
		// A lost lease's handler was cancelled already, and one handler
		// is cancelled at most once.
		if l.state == stateRunning {
			out = append(out, CancelHandler{Ref: l.ref, Reason: CancelDrain})
		}
		l.state = stateReleased
		refs = append(refs, l.ref)
	}
	t.minDirty = true
	return t.sendRelease(refs, out)
}

// fireHeartbeat sends the heartbeat when it is due, unless one is still in
// flight, and schedules the next firing a period later either way.
func (t *Tracker) fireHeartbeat(now Mono, out []Effect) []Effect {
	if now < t.hbNext {
		return out
	}
	t.hbNext = now + t.period
	t.hbRetryUsed = false
	if len(t.hbInflight) > 0 {
		return out
	}
	var refs []Ref
	for _, l := range t.collect((*lease).live) {
		refs = append(refs, l.ref)
	}
	for chunk := range slices.Chunk(refs, t.cfg.AckBatch) {
		out = t.sendHeartbeat(chunk, out)
	}
	return out
}

// fireAckRetry sends the failed ack batch again when its wait is over.
func (t *Tracker) fireAckRetry(now Mono, out []Effect) []Effect {
	b := t.batch
	if b == nil || b.inFlight || now < b.retryAt {
		return out
	}
	b.seq = t.seq()
	b.inFlight = true
	return append(out, SendAcks{Seq: b.seq, Acks: slices.Clone(b.acks)})
}

// flushAcks sends the pending acks as one batch, when no batch is in flight or
// waiting to be resent and either force is set, the list is full, or the
// window of its first ack is over. A list longer than a batch leaves the rest
// for the next flush.
func (t *Tracker) flushAcks(now Mono, force bool, out []Effect) []Effect {
	if t.batch != nil || len(t.pending) == 0 {
		return out
	}
	if !force && len(t.pending) < t.cfg.AckBatch && now < t.pendingSince+t.cfg.AckWindow {
		return out
	}
	n := min(len(t.pending), t.cfg.AckBatch)
	acks := slices.Clone(t.pending[:n])
	t.pending = slices.Delete(t.pending, 0, n)
	t.batch = &ackBatch{seq: t.seq(), acks: acks, inFlight: true}
	return append(out, SendAcks{Seq: t.batch.seq, Acks: slices.Clone(acks)})
}

// fireLeaseRetry ends the backoff wait when it is over.
func (t *Tracker) fireLeaseRetry(now Mono) {
	if t.retryPending && now >= t.retryAt {
		t.retryPending = false
	}
}

// requestLeases asks for work when a slot is free and nothing holds the
// request back: another request outstanding, a backoff wait, or a drain. A
// Lease call carries job payloads, so it asks for at most job.MaxBatch jobs
// however many slots are free. The tracker owns that limit, not the shell,
// so that the simulator sees it too.
func (t *Tracker) requestLeases(out []Effect) []Effect {
	free := t.cfg.Concurrency - t.running
	if !t.started || t.draining || t.reqSeq != 0 || t.retryPending || free <= 0 {
		return out
	}
	t.reqSeq = t.seq()
	t.reqMax = min(free, job.MaxBatch)
	return append(out, RequestLeases{Seq: t.reqSeq, Max: t.reqMax, Wait: t.cfg.LongPoll, TTL: t.cfg.TTL})
}

// stopIfDone emits Stopped, once, when a drain has finished: no ack or
// release is pending or in flight, and no handler runs. Past the drain
// deadline the handlers that never returned are no longer waited for.
func (t *Tracker) stopIfDone(out []Effect) []Effect {
	if !t.draining || t.stopped {
		return out
	}
	if t.batch != nil || len(t.pending) > 0 || len(t.releasing) > 0 {
		return out
	}
	if !t.drainFired && t.running > 0 {
		return out
	}
	t.stopped = true
	return append(out, Stopped{})
}

// arm appends the Arm for the earliest instant at which anything is due.
func (t *Tracker) arm(out []Effect) []Effect {
	if t.stopped || !t.started {
		return out
	}
	at := t.hbNext
	if b := t.batch; b != nil {
		if !b.inFlight {
			at = min(at, b.retryAt)
		}
	} else if len(t.pending) > 0 {
		at = min(at, t.pendingSince+t.cfg.AckWindow)
	}
	if t.retryPending {
		at = min(at, t.retryAt)
	}
	if t.draining && !t.drainFired {
		at = min(at, t.drainDeadline)
	}
	if d, ok := t.earliestDeadline(); ok {
		at = min(at, d)
	}
	return append(out, Arm{At: at})
}

// earliestDeadline returns the earliest deadline of a live lease, and whether
// there is one.
func (t *Tracker) earliestDeadline() (Mono, bool) {
	if t.minDirty {
		t.minDirty, t.minKnown = false, false
		for _, l := range t.leases {
			if l.live() && (!t.minKnown || l.deadline < t.minDeadline) {
				t.minDeadline, t.minKnown = l.deadline, true
			}
		}
	}
	return t.minDeadline, t.minKnown
}

// forget removes a lease from the tracker.
func (t *Tracker) forget(l *lease) {
	delete(t.leases, l.ref)
	if n := t.byID[l.ref.ID] - 1; n > 0 {
		t.byID[l.ref.ID] = n
	} else {
		delete(t.byID, l.ref.ID)
	}
	t.minDirty = true
}

// collect returns the leases that keep accepts, in ascending order of job id
// and then token, so that effects never depend on map order.
func (t *Tracker) collect(keep func(*lease) bool) []*lease {
	var out []*lease
	for _, l := range t.leases {
		if keep(l) {
			out = append(out, l)
		}
	}
	slices.SortFunc(out, func(a, b *lease) int {
		return cmp.Or(cmp.Compare(a.ref.ID, b.ref.ID), cmp.Compare(a.ref.Token, b.ref.Token), strings.Compare(a.ref.Queue, b.ref.Queue))
	})
	return out
}

// seq returns the next sequence number. Every send effect gets its own, and
// the first is 1.
func (t *Tracker) seq() uint64 {
	t.nextSeq++
	return t.nextSeq
}
