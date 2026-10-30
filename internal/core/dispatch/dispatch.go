package dispatch

import (
	"maps"
	"slices"
	"strconv"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
)

const (
	defaultMaxInFlight  = 4
	defaultPollInterval = time.Second
)

// Config configures a Core.
type Config struct {
	// MaxInFlight is the most fetches that run at once for one queue. A
	// value of zero or less selects the default, 4.
	MaxInFlight int
	// PollInterval is how long a queue may go without a fetch while
	// waiters are parked on it. It bounds the delay when a notification is
	// lost. A value of zero or less selects the default, 1s.
	PollInterval time.Duration
}

// Waiter is one long-polling request.
type Waiter struct {
	// ID identifies the waiter. It must not belong to a live waiter.
	ID WaiterID
	// Queue is the queue the worker asked for.
	Queue string
	// Max is the most jobs the worker asked for. A value outside
	// 1..job.MaxBatch is moved to the nearest bound.
	Max int
	// Arrived is the instant the request arrived.
	Arrived Mono
	// Deadline is Arrived plus the wait the worker asked for, capped by the
	// server. A Deadline equal to Arrived, or before it, means: fetch once,
	// then answer.
	Deadline Mono
}

// waiterState is where a waiter is in its life.
type waiterState uint8

const (
	// stateWaiting means the waiter is parked, with no fetch out.
	stateWaiting waiterState = iota + 1
	// stateFetching means a fetch for the waiter is out.
	stateFetching
	// stateGone means the waiter was cancelled while its fetch was out. The
	// entry stays until the fetch comes back, so that the jobs it found can
	// be released.
	stateGone
)

// entry is a waiter and its state.
type entry struct {
	w     Waiter
	state waiterState
	// fetched says that a Fetch was issued for the waiter at least once.
	fetched bool
}

// queueState is what the core knows about one queue.
type queueState struct {
	// waiters holds every live entry of the queue in arrival order: parked,
	// fetching and gone.
	waiters []*entry
	// inFlight counts the entries that are fetching or gone.
	inFlight int
	// knownEmpty says that the last answer found the queue empty, and that
	// nothing since suggests otherwise.
	knownEmpty bool
	// dirty says that a notification arrived after the last fetch started,
	// so an empty answer may be out of date.
	dirty bool
	// lastFetch is the instant the last Fetch was issued.
	lastFetch Mono
	// dueAt is the instant the queue's next scheduled job becomes due, or
	// zero when there is none.
	dueAt Mono
}

// covered reports whether the parked waiter e may be told that there are no
// jobs: a fetch has looked for it, or the queue is known to be empty, which is
// what a waiter arriving at such a queue is told at once. A waiter that is
// not covered is waiting for its turn at a fetch slot.
func (q *queueState) covered(e *entry) bool {
	return e.fetched || q.knownEmpty
}

// oldestWaiting returns the oldest parked entry, or nil.
func (q *queueState) oldestWaiting() *entry {
	for _, e := range q.waiters {
		if e.state == stateWaiting {
			return e
		}
	}
	return nil
}

// remove takes e out of the queue's list.
func (q *queueState) remove(e *entry) {
	i := slices.Index(q.waiters, e)
	switch {
	case i < 0:
	case i == 0:
		q.waiters[0] = nil
		q.waiters = q.waiters[1:]
	default:
		q.waiters = slices.Delete(q.waiters, i, i+1)
	}
}

// Core is the dispatch state of one broker: the long-poll waiters of every
// queue and the fetches in flight for them. Create one with New.
type Core struct {
	cfg      Config
	queues   map[string]*queueState
	waiters  map[WaiterID]*entry
	draining bool
}

// New returns a core with no waiters.
func New(cfg Config) *Core {
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = defaultMaxInFlight
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}
	return &Core{cfg: cfg, queues: map[string]*queueState{}, waiters: map[WaiterID]*entry{}}
}

// Arrive registers a waiter. At most one fetch starts, for the oldest parked
// waiter, which is the new one unless an older one is still parked: none
// starts if the queue is known to be empty or already has MaxInFlight fetches
// out. After Drain the waiter is rejected instead, and a waiter that asked
// for no wait on a queue known to be empty is answered with no jobs at once.
//
// Arrive panics if w.ID belongs to a live waiter, which is a bug in the
// shell.
func (c *Core) Arrive(now Mono, w Waiter) []Effect {
	if _, live := c.waiters[w.ID]; live {
		panic("dispatch: Arrive: duplicate waiter id " + strconv.FormatUint(uint64(w.ID), 10))
	}
	if c.draining {
		return c.arm([]Effect{Respond{Waiter: w.ID, Reason: ReasonRejected}})
	}
	w.Max = min(max(w.Max, 1), job.MaxBatch)
	w.Deadline = max(w.Deadline, w.Arrived)

	q := c.queue(w.Queue)
	e := &entry{w: w, state: stateWaiting}
	q.waiters = append(q.waiters, e)
	c.waiters[w.ID] = e

	effs := c.pump(now, q, nil)
	if e.state == stateWaiting && q.covered(e) && w.Deadline == w.Arrived {
		c.forget(q, e)
		effs = append(effs, Respond{Waiter: w.ID, Reason: ReasonTimeout})
	}
	return c.arm(effs)
}

// Notify reports that jobs may have become ready on a queue. It wakes the
// oldest parked waiter and no more: a notification starts at most one fetch,
// so the wakeups follow the work, and a full batch chains the next waiter.
func (c *Core) Notify(now Mono, queue string) []Effect {
	q, ok := c.queues[queue]
	if !ok {
		// Nobody waits here and nothing is known: the next arrival fetches
		// anyway, so there is nothing to remember.
		return c.arm(nil)
	}
	q.knownEmpty = false
	q.dirty = true
	return c.arm(c.pump(now, q, nil))
}

// DueAt reports that the next scheduled job of a queue becomes due at the
// given instant. It replaces any earlier value, and a zero instant clears
// it. The core wakes the queue's oldest waiter at that instant, from Tick.
func (c *Core) DueAt(now Mono, queue string, at Mono) []Effect {
	q, ok := c.queues[queue]
	if !ok {
		if at == 0 {
			return c.arm(nil)
		}
		q = c.queue(queue)
	}
	q.dueAt = at
	return c.arm(nil)
}

// FetchDone reports the result of the fetch for waiter id: got jobs, or a
// failure, in which case got is ignored. It answers the waiter, releases
// the jobs if the waiter was cancelled meanwhile, and chains one fetch for
// the next parked waiter, but only when the batch was full or a notification
// came in since the last fetch started. A call for a waiter with no fetch out
// changes nothing.
func (c *Core) FetchDone(now Mono, id WaiterID, got int, failed bool) []Effect {
	e, ok := c.waiters[id]
	if !ok || e.state == stateWaiting {
		return c.arm(nil)
	}
	if failed {
		got = 0
	}
	q := c.queues[e.w.Queue]
	q.inFlight--

	var effs []Effect
	switch {
	case e.state == stateGone:
		if got > 0 {
			effs = append(effs, Release{Waiter: id})
		}
		c.forget(q, e)
	case failed:
		effs = append(effs, Respond{Waiter: id, Reason: ReasonUnavailable})
		c.forget(q, e)
	case got > 0:
		// Still delivered while draining: the leases are valid.
		effs = append(effs, Deliver{Waiter: id})
		c.forget(q, e)
	case c.draining:
		effs = append(effs, Respond{Waiter: id, Reason: ReasonDraining})
		c.forget(q, e)
	case now >= e.w.Deadline:
		effs = append(effs, Respond{Waiter: id, Reason: ReasonTimeout})
		c.forget(q, e)
	default:
		e.state = stateWaiting // back in its original place
	}

	full := !failed && got >= e.w.Max
	if !failed {
		switch {
		case full:
			q.knownEmpty = false
		case !q.dirty:
			q.knownEmpty = true
		}
	}
	if full || q.dirty {
		effs = c.pump(now, q, effs)
	}
	return c.arm(effs)
}

// Cancel reports that the client of waiter id went away. A parked waiter is
// forgotten. A waiter whose fetch is out is kept until FetchDone, which then
// releases whatever the fetch found. An unknown id is ignored.
func (c *Core) Cancel(now Mono, id WaiterID) []Effect {
	if e, ok := c.waiters[id]; ok {
		switch e.state {
		case stateWaiting:
			c.forget(c.queues[e.w.Queue], e)
		case stateFetching:
			e.state = stateGone
		case stateGone:
		}
	}
	return c.arm(nil)
}

// Tick is called when the armed instant arrives. For every queue, in name
// order, it times out the covered waiters whose deadline has passed, then
// wakes the queue, with at most one fetch, if a poll interval has passed
// since its last fetch or its due time has arrived. A waiter that is not
// covered, that is one no fetch has looked for on a queue not known to be
// empty, is not timed out: it waits for its fetch.
func (c *Core) Tick(now Mono) []Effect {
	var effs []Effect
	for _, name := range slices.Sorted(maps.Keys(c.queues)) {
		q := c.queues[name]
		effs = c.answerParked(q, func(e *entry) bool { return e.w.Deadline <= now && q.covered(e) }, ReasonTimeout, effs)

		wake := q.oldestWaiting() != nil && now-q.lastFetch >= c.cfg.PollInterval
		if q.dueAt != 0 && q.dueAt <= now {
			q.dueAt = 0
			wake = true
		}
		if wake {
			q.knownEmpty = false
			effs = c.pump(now, q, effs)
		}

		// A queue with no entries and no due time is remembered only for what
		// it knows. After a poll interval that knowledge is stale, and the
		// next arrival would fetch at once anyway.
		if len(q.waiters) == 0 && q.dueAt == 0 && now-q.lastFetch >= c.cfg.PollInterval {
			delete(c.queues, name)
		}
	}
	return c.arm(effs)
}

// Drain starts the shutdown. Every parked waiter is answered with
// ReasonDraining, and later arrivals are rejected. Waiters whose fetch is
// out are answered when it comes back, and no fetch starts again.
func (c *Core) Drain(now Mono) []Effect {
	c.draining = true
	var effs []Effect
	for _, name := range slices.Sorted(maps.Keys(c.queues)) {
		effs = c.answerParked(c.queues[name], func(*entry) bool { return true }, ReasonDraining, effs)
	}
	return c.arm(effs)
}

// Waiting returns how many waiters are parked on the queue, with no fetch
// out for them.
func (c *Core) Waiting(queue string) int {
	q, ok := c.queues[queue]
	if !ok {
		return 0
	}
	var n int
	for _, e := range q.waiters {
		if e.state == stateWaiting {
			n++
		}
	}
	return n
}

// InFlight returns how many fetches are out for the queue, including those
// of waiters that were cancelled meanwhile.
func (c *Core) InFlight(queue string) int {
	if q, ok := c.queues[queue]; ok {
		return q.inFlight
	}
	return 0
}

// queue returns the state of the named queue, creating it if needed.
func (c *Core) queue(name string) *queueState {
	q, ok := c.queues[name]
	if !ok {
		q = &queueState{}
		c.queues[name] = q
	}
	return q
}

// forget drops e from the core.
func (c *Core) forget(q *queueState, e *entry) {
	delete(c.waiters, e.w.ID)
	q.remove(e)
}

// answerParked answers the parked waiters of q for which due is true with
// reason, in arrival order, and forgets them.
func (c *Core) answerParked(q *queueState, due func(*entry) bool, reason Reason, effs []Effect) []Effect {
	kept := q.waiters[:0]
	for _, e := range q.waiters {
		if e.state == stateWaiting && due(e) {
			delete(c.waiters, e.w.ID)
			effs = append(effs, Respond{Waiter: e.w.ID, Reason: reason})
			continue
		}
		kept = append(kept, e)
	}
	clear(q.waiters[len(kept):])
	q.waiters = kept
	return effs
}

// pump starts one fetch for the oldest parked waiter of q, if a fetch slot is
// free and the queue is not known to be empty. A notification that came in
// since the last fetch started counts as news even then. It never starts
// more than one: every event that reaches it, an arrival, a notification, a
// poll, a due time or a finished fetch, is worth one fetch at most, which
// keeps the wakeups proportional to the work. Arrivals under load still fill
// the free slots, one per arrival.
func (c *Core) pump(now Mono, q *queueState, effs []Effect) []Effect {
	if c.draining || q.inFlight >= c.cfg.MaxInFlight || (q.knownEmpty && !q.dirty) {
		return effs
	}
	e := q.oldestWaiting()
	if e == nil {
		return effs
	}
	e.state = stateFetching
	e.fetched = true
	q.inFlight++
	q.lastFetch = now
	q.dirty = false
	return append(effs, Fetch{
		Waiter: e.w.ID,
		Queue:  e.w.Queue,
		Max:    e.w.Max,
		Waited: now - e.w.Arrived,
	})
}

// arm appends the Arm effect: the earliest instant at which Tick has work. A
// parked waiter counts through its deadline once it is covered, because Tick
// does not time out a waiter that is waiting for its fetch. The poll of a
// queue and the due time of a queue count while a waiter is parked on it, and
// the poll only while a fetch slot is free, because a poll cannot start a
// fetch otherwise and would make the shell spin.
func (c *Core) arm(effs []Effect) []Effect {
	var at Mono
	var armed bool
	earliest := func(t Mono) {
		if !armed || t < at {
			at, armed = t, true
		}
	}
	for _, q := range c.queues {
		var parked bool
		for _, e := range q.waiters {
			if e.state == stateWaiting {
				parked = true
				if q.covered(e) {
					earliest(e.w.Deadline)
				}
			}
		}
		if !parked {
			continue
		}
		if q.inFlight < c.cfg.MaxInFlight {
			earliest(q.lastFetch + c.cfg.PollInterval)
		}
		if q.dueAt != 0 {
			earliest(q.dueAt)
		}
	}
	if armed {
		effs = append(effs, Arm{At: at})
	}
	return effs
}
