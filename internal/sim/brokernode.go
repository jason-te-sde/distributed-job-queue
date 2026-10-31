package sim

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/dispatch"
	"github.com/jason-te-sde/distributed-job-queue/internal/core/fair"
	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/core/lease"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/memstore"
)

const (
	// maxWait is the server's cap on a long poll.
	maxWait = 30 * time.Second
	// notifyDelayMin and notifyDelayMax bound how long a store notification
	// takes to reach a broker.
	notifyDelayMin = 100 * time.Microsecond
	notifyDelayMax = 5 * time.Millisecond
	// reapBatch and pruneBatch are the most rows one reap or prune takes.
	reapBatch  = 1000
	pruneBatch = 1000
	// A default for each periodic interval a BrokerConfig leaves at zero.
	defaultReapInterval     = time.Second
	defaultPruneInterval    = time.Minute
	defaultActivityInterval = time.Second
)

// BrokerConfig configures a BrokerNode.
type BrokerConfig struct {
	// ID names the node on the network.
	ID NodeID
	// Clock is the broker's monotonic clock. Its Rate must be positive.
	Clock Clock
	// Scheduler names the tenant scheduler. "drr" is the only one, and the
	// empty string means it.
	Scheduler string
	// PollInterval is the dispatch core's fallback poll; zero takes the
	// core's default.
	PollInterval time.Duration
	// ReapInterval, PruneInterval and ActivityInterval are the periods of
	// the reaper, the pruner and the refresh of each queue's active
	// tenants. Zero takes 1s, 1m and 1s. Each is a whole number of
	// microseconds.
	ReapInterval, PruneInterval, ActivityInterval time.Duration
	// Retention is how long the pruner keeps finished jobs. The zero value
	// takes the production defaults, 24h and 336h, so that a scenario that
	// does not care about pruning never loses a row.
	Retention store.Retention
	// NotifyDropP is the chance that a store notification never reaches
	// this broker.
	NotifyDropP float64
}

// BrokerStats counts what a broker has done. The counts survive a restart.
type BrokerStats struct {
	// FencedAcks counts store outcomes Fenced across Heartbeat, Complete,
	// Fail and Release.
	FencedAcks int
	// Reaps and Prunes count the rows the broker reaped and pruned.
	Reaps, Prunes int
	// NotifyDrops counts notifications that were dropped before reaching
	// the broker.
	NotifyDrops int
	// Unavailable counts store calls that failed because the store was
	// unavailable.
	Unavailable int
}

// reqKey identifies one lease request: the sender and its sequence number.
type reqKey struct {
	from NodeID
	seq  uint64
}

// waiterRec is the broker's record of one long-poll request.
type waiterRec struct {
	req leaseReq
	// fetching says that a fetch for the waiter is out.
	fetching bool
	// cancelled says that the client went away while the fetch was out.
	cancelled bool
	// waited is the Waited of the latest Fetch for the waiter.
	waited time.Duration
	// grants are what that fetch leased.
	grants []store.Grant
}

// BrokerNode is a simulated broker: the real dispatch core and fair planner,
// driven by messages from workers and producers and by its own timers, with a
// store reached through StoreNode. It re-implements, in about as many lines,
// the glue that package broker puts around the same cores.
//
// A crash drops the dispatch core, the fair state, the waiters and the
// timers. Whatever the store committed stays, including leases whose grants
// the broker never delivered; they expire and are reaped.
type BrokerNode struct {
	w   *World
	net *Network
	st  *StoreNode
	cfg BrokerConfig
	rng *Rand

	up    bool
	gen   int // changes with every crash and start; stale callbacks compare it
	timer nodeTimer

	core       *dispatch.Core
	rings      map[string]*fair.DRR // one planner per queue, owned by this node's events
	waiters    map[dispatch.WaiterID]*waiterRec
	byReq      map[reqKey]dispatch.WaiterID
	nextWaiter dispatch.WaiterID
	queues     map[string]struct{} // queues a worker has asked for since the start

	stats BrokerStats
}

// NewBrokerNode starts a broker and attaches it to the network under cfg.ID
// and to the store's notifications. It panics if the id is already attached,
// the clock does not run, the scheduler is unknown, or an interval is not a
// positive whole number of microseconds.
func NewBrokerNode(w *World, net *Network, st *StoreNode, cfg BrokerConfig) *BrokerNode {
	cfg = cfg.withDefaults()
	_ = cfg.Clock.Mono(0) // fail here, not at the first message, if the clock does not run
	if _, taken := net.brokers[cfg.ID]; taken {
		panic("sim: NewBrokerNode called with the id " + string(cfg.ID) + ", which is already attached")
	}
	b := &BrokerNode{w: w, net: net, st: st, cfg: cfg, rng: w.Rand.Fork(), timer: nodeTimer{w: w, clock: cfg.Clock}}
	net.brokers[cfg.ID] = b
	st.subscribe(b.onChange)
	b.start()
	return b
}

// withDefaults fills the zero fields and rejects the invalid ones.
func (c BrokerConfig) withDefaults() BrokerConfig {
	if c.ID == "" {
		panic("sim: NewBrokerNode called with an empty ID")
	}
	switch c.Scheduler {
	case "", "drr":
		c.Scheduler = "drr"
	default:
		panic("sim: NewBrokerNode called with the unknown scheduler " + c.Scheduler)
	}
	for _, p := range []struct {
		field *time.Duration
		def   time.Duration
		name  string
	}{
		{&c.ReapInterval, defaultReapInterval, "ReapInterval"},
		{&c.PruneInterval, defaultPruneInterval, "PruneInterval"},
		{&c.ActivityInterval, defaultActivityInterval, "ActivityInterval"},
	} {
		if *p.field == 0 {
			*p.field = p.def
		}
		if *p.field < 0 || *p.field%time.Microsecond != 0 {
			panic("sim: NewBrokerNode called with " + p.name + " " + p.field.String() + ", want a positive whole number of microseconds")
		}
	}
	if c.Retention == (store.Retention{}) {
		c.Retention = store.Retention{Done: 24 * time.Hour, Dead: 336 * time.Hour}
	}
	return c
}

// Stats returns the counts so far.
func (b *BrokerNode) Stats() BrokerStats {
	return b.stats
}

// Crash stops the broker: it forgets its dispatch core, fair state, waiters
// and timers, and ignores every message and callback until Restart.
func (b *BrokerNode) Crash() {
	b.up = false
	b.gen++
	b.timer.cancel()
	b.core, b.rings, b.waiters, b.byReq, b.queues = nil, nil, nil, nil, nil
}

// Restart brings the broker back with a fresh dispatch core and fair state.
// A broker that is running is crashed first.
func (b *BrokerNode) Restart() {
	if b.up {
		b.Crash()
	}
	b.start()
}

// start builds the broker's state and its periodic work.
func (b *BrokerNode) start() {
	b.up = true
	b.gen++
	b.core = dispatch.New(dispatch.Config{PollInterval: b.cfg.PollInterval})
	b.rings = map[string]*fair.DRR{}
	b.waiters = map[dispatch.WaiterID]*waiterRec{}
	b.byReq = map[reqKey]dispatch.WaiterID{}
	b.queues = map[string]struct{}{}
	b.every(b.cfg.ReapInterval, "broker reap", b.reap)
	b.every(b.cfg.PruneInterval, "broker prune", b.prune)
	b.every(b.cfg.ActivityInterval, "broker activity", b.refreshActivity)
}

// every runs fn once per interval, first at a random point of the first
// interval so that brokers do not tick in step, until the broker crashes.
func (b *BrokerNode) every(interval time.Duration, label string, fn func()) {
	gen := b.gen
	var tick func()
	tick = func() {
		if b.stale(gen) {
			return
		}
		fn()
		b.w.After(interval, label, tick)
	}
	b.w.After(b.rng.Between(0, interval), label, tick)
}

// stale reports whether a callback made in incarnation gen must be ignored.
func (b *BrokerNode) stale(gen int) bool {
	return !b.up || b.gen != gen
}

// mono is the broker's clock reading now.
func (b *BrokerNode) mono() dispatch.Mono {
	return b.cfg.Clock.Mono(b.w.Now)
}

// ring returns the fair planner of a queue, creating it.
func (b *BrokerNode) ring(queue string) *fair.DRR {
	d, ok := b.rings[queue]
	if !ok {
		d = fair.New(fair.Config{})
		b.rings[queue] = d
	}
	return d
}

// send sends a reply or a notice from this broker.
func (b *BrokerNode) send(to NodeID, idempotent bool, label string, deliver func()) {
	b.net.Send(b.cfg.ID, to, idempotent, label, deliver)
}

// call runs a store call whose result is ignored if this incarnation of the
// broker has crashed by the time it ends. A store that is unavailable is
// counted. Any other error is a mistake of the simulator itself, which builds
// every request, so it panics instead of being handled.
func (b *BrokerNode) call(label string, fn func(store.Store) error, done func(err error)) {
	gen := b.gen
	b.st.Call(label, fn, func(err error) {
		if errors.Is(err, store.ErrUnavailable) {
			b.stats.Unavailable++
		} else if err != nil {
			panic(fmt.Sprintf("sim: broker %s: store call %s failed: %v", b.cfg.ID, label, err))
		}
		if !b.stale(gen) {
			done(err)
		}
	})
}

// onChange sees every row the store changes. A job that may be leased right
// now (a new one that is due, a released one, a requeued one) is announced
// to the broker after a short delay, unless the notification is dropped.
func (b *BrokerNode) onChange(ch memstore.Change) {
	var j job.Job
	switch {
	case ch.Op == memstore.OpEnqueue && ch.Inserted && !ch.After.RunAt.After(ch.Now):
		j = ch.After
	case ch.Op == memstore.OpRelease, ch.Op == memstore.OpRequeue:
		j = ch.After
	default:
		return
	}
	if !b.up {
		return
	}
	delay, drop := b.rng.Between(notifyDelayMin, notifyDelayMax), b.rng.Chance(b.cfg.NotifyDropP)
	if drop {
		b.stats.NotifyDrops++
		return
	}
	gen := b.gen
	b.w.After(delay, "broker notify", func() {
		if !b.stale(gen) {
			b.Notify(j.Queue, j.Tenant)
		}
	})
}

// Notify tells the broker that a job of the tenant may be ready on the queue:
// the tenant joins the fair ring and the dispatch core wakes a waiter. The
// store's notifications arrive here through a delay, and a dropped one never
// does. A crashed broker ignores it.
func (b *BrokerNode) Notify(queue, tenant string) {
	if !b.up {
		return
	}
	b.ring(queue).Activate(tenant)
	b.apply(b.core.Notify(b.mono(), queue))
}

// leaseReq is a worker's long-poll request for jobs.
type leaseReq struct {
	from      NodeID
	seq       uint64
	queue     string
	max       int
	wait, ttl time.Duration
	reply     func(leaseResp)
}

// leaseResp answers a leaseReq: grants, an empty answer, or an error.
type leaseResp struct {
	seq uint64
	// waited is how long the broker held the request before it looked for
	// work, on the broker's clock.
	waited time.Duration
	grants []lease.Grant
	err    error
}

// cancelReq tells the broker that the client of a lease request gave up.
type cancelReq struct {
	from NodeID
	seq  uint64
}

// handleLeaseReq registers the request as a waiter of the dispatch core.
func (b *BrokerNode) handleLeaseReq(r leaseReq) {
	if !b.up {
		return
	}
	b.nextWaiter++
	id := b.nextWaiter
	b.waiters[id] = &waiterRec{req: r}
	b.byReq[reqKey{r.from, r.seq}] = id
	b.queues[r.queue] = struct{}{}
	now := b.mono()
	b.apply(b.core.Arrive(now, dispatch.Waiter{
		ID: id, Queue: r.queue, Max: r.max,
		Arrived: now, Deadline: now + min(r.wait, maxWait),
	}))
}

// handleCancel forgets the waiter whose client gave up. When its fetch is
// out, the record stays until the fetch returns and the jobs are released.
func (b *BrokerNode) handleCancel(r cancelReq) {
	if !b.up {
		return
	}
	id, ok := b.byReq[reqKey(r)]
	if !ok {
		return
	}
	rec := b.waiters[id]
	b.apply(b.core.Cancel(b.mono(), id))
	if rec.fetching {
		rec.cancelled = true
	} else {
		b.forget(id)
	}
}

// forget drops a waiter's record.
func (b *BrokerNode) forget(id dispatch.WaiterID) {
	if rec, ok := b.waiters[id]; ok {
		delete(b.byReq, reqKey{rec.req.from, rec.req.seq})
		delete(b.waiters, id)
	}
}

// waiter returns the record of a waiter that the core names. The core never
// names one that the broker has forgotten, so a miss is a bug.
func (b *BrokerNode) waiter(id dispatch.WaiterID) *waiterRec {
	rec, ok := b.waiters[id]
	if !ok {
		panic(fmt.Sprintf("sim: broker %s: the dispatch core named waiter %d, which the broker does not know", b.cfg.ID, id))
	}
	return rec
}

// apply carries out the effects of a dispatch core call.
func (b *BrokerNode) apply(effs []dispatch.Effect) {
	for _, e := range effs {
		switch e := e.(type) {
		case dispatch.Fetch:
			b.startFetch(e)
		case dispatch.Deliver:
			b.deliver(e.Waiter)
		case dispatch.Respond:
			b.respond(e)
		case dispatch.Release:
			b.releaseOrphan(e.Waiter)
		case dispatch.Arm:
			b.arm(e.At)
		}
	}
}

// arm sets the one timer that calls the core's Tick.
func (b *BrokerNode) arm(at dispatch.Mono) {
	gen := b.gen
	b.timer.arm(at, "broker tick", func() {
		if !b.stale(gen) {
			b.apply(b.core.Tick(b.mono()))
		}
	})
}

// startFetch begins a fetch for a waiter. A fetch that finds the queue's
// ring empty first asks the store which tenants have work, as the periodic
// refresh would, so that a job that became due since the last refresh is not
// missed. Otherwise it leases at once.
func (b *BrokerNode) startFetch(f dispatch.Fetch) {
	rec := b.waiter(f.Waiter)
	rec.fetching = true
	rec.waited = f.Waited
	if len(b.ring(f.Queue).Active()) > 0 {
		b.lease(f.Waiter, f.Queue, f.Max)
		return
	}
	var act store.Activity
	b.call("activity", func(s store.Store) (err error) {
		act, err = s.Activity(context.Background(), f.Queue)
		return err
	}, func(err error) {
		if err != nil {
			b.fetchDone(f.Waiter, nil, true, dispatch.AcrossTenants) // nothing was planned
			return
		}
		b.applyActivity(f.Queue, act)
		b.lease(f.Waiter, f.Queue, f.Max)
	})
}

// applyActivity makes the active tenants of a queue the fair ring, and tells
// the core when the queue's next scheduled job falls due.
func (b *BrokerNode) applyActivity(queue string, act store.Activity) {
	b.ring(queue).SetActive(act.Tenants)
	var at dispatch.Mono
	if act.HasNextDue {
		// The core reads a zero instant as "none".
		at = max(b.mono()+act.NextDueIn, 1)
	}
	b.apply(b.core.DueAt(b.mono(), queue, at))
}

// lease plans the fetch of a waiter over the queue's ring and leases the
// planned jobs from the store.
func (b *BrokerNode) lease(id dispatch.WaiterID, queue string, max int) {
	ring := b.ring(queue)
	takes := ring.Plan(max)
	if len(takes) == 0 {
		// The refresh found no tenant with due work. Answer from a fresh
		// event: this can run inside apply, where FetchDone would produce a
		// newer Arm that the outer effect list then overrides.
		gen := b.gen
		b.w.After(0, "broker empty fetch", func() {
			if !b.stale(gen) {
				b.fetchDone(id, nil, false, dispatch.AcrossTenants)
			}
		})
		return
	}
	rec := b.waiter(id)
	req := store.LeaseRequest{Queue: queue, TTL: rec.req.ttl, Worker: string(rec.req.from)}
	for _, tk := range takes {
		req.Takes = append(req.Takes, store.TenantTake{Tenant: tk.Tenant, Limit: tk.Limit})
	}
	var grants []store.Grant
	b.call("lease", func(s store.Store) (err error) {
		grants, err = s.Lease(context.Background(), req)
		return err
	}, func(err error) {
		if err != nil {
			// The plan stays spent. Settling it would have to guess how many
			// jobs each tenant has, and the next refresh corrects the ring.
			b.fetchDone(id, nil, true, dispatch.PerTenant)
			return
		}
		got := make([]int, len(takes))
		for i, tk := range takes {
			for _, g := range grants {
				if g.Job.Tenant == tk.Tenant {
					got[i]++
				}
			}
		}
		ring.Settle(takes, got)
		b.fetchDone(id, grants, false, dispatch.PerTenant)
	})
}

// fetchDone reports the end of a waiter's fetch to the core. The scope says
// what the fetch asked the store for: a take for each tenant of a plan, or no
// plan at all, when the refresh found no tenant with due work and the answer
// is empty for the whole queue.
func (b *BrokerNode) fetchDone(id dispatch.WaiterID, grants []store.Grant, failed bool, scope dispatch.FetchScope) {
	rec := b.waiter(id)
	rec.fetching = false
	rec.grants = grants
	b.apply(b.core.FetchDone(b.mono(), id, len(grants), failed, scope))
	if rec.cancelled {
		b.forget(id)
	}
}

// deliver answers a waiter with the jobs its fetch leased.
func (b *BrokerNode) deliver(id dispatch.WaiterID) {
	rec := b.waiter(id)
	grants := make([]lease.Grant, len(rec.grants))
	for i, g := range rec.grants {
		grants[i] = lease.Grant{
			Ref:       lease.Ref{ID: g.Job.ID, Queue: g.Job.Queue, Token: g.Job.Epoch},
			Attempt:   g.Job.Attempts,
			Remaining: g.Remaining,
		}
	}
	resp := leaseResp{seq: rec.req.seq, waited: rec.waited, grants: grants}
	reply := rec.req.reply
	b.send(rec.req.from, false, "lease-resp", func() { reply(resp) })
	b.forget(id)
}

// respond answers a waiter with no jobs: an empty answer when its wait ran
// out or the broker is draining, which ends the poll without error, and an
// error when the request was rejected or the fetch failed.
func (b *BrokerNode) respond(e dispatch.Respond) {
	rec := b.waiter(e.Waiter)
	resp := leaseResp{seq: rec.req.seq}
	switch e.Reason {
	case dispatch.ReasonTimeout, dispatch.ReasonDraining:
	case dispatch.ReasonRejected, dispatch.ReasonUnavailable:
		resp.err = fmt.Errorf("sim broker %s: lease request answered %v", b.cfg.ID, e.Reason)
	}
	reply := rec.req.reply
	b.send(rec.req.from, false, "lease-resp", func() { reply(resp) })
	b.forget(e.Waiter)
}

// releaseOrphan gives back the jobs that a fetch leased for a client that
// went away. It is best effort: if it fails, the leases expire.
func (b *BrokerNode) releaseOrphan(id dispatch.WaiterID) {
	rec := b.waiter(id)
	refs := make([]store.Ref, len(rec.grants))
	for i, g := range rec.grants {
		refs[i] = store.Ref{ID: g.Job.ID, Queue: g.Job.Queue, Token: g.Job.Epoch}
	}
	b.call("release orphans", func(s store.Store) error {
		out, err := s.Release(context.Background(), refs)
		b.countFenced(out)
		return err
	}, func(error) {})
}

// countFenced adds the Fenced outcomes of a call to the stats.
func (b *BrokerNode) countFenced(outcomes []job.Outcome) {
	for _, o := range outcomes {
		if o == job.Fenced {
			b.stats.FencedAcks++
		}
	}
}

// heartbeatReq extends the leases of a worker.
type heartbeatReq struct {
	from  NodeID
	seq   uint64
	refs  []lease.Ref
	ttl   time.Duration
	reply func(heartbeatResp)
}

// heartbeatResp answers a heartbeatReq: one status per ref, or an error.
type heartbeatResp struct {
	seq      uint64
	statuses []lease.HeartbeatStatus
	err      error
}

// storeRefs converts a worker's lease refs to store refs.
func storeRefs(refs []lease.Ref) []store.Ref {
	out := make([]store.Ref, len(refs))
	for i, r := range refs {
		out[i] = store.Ref{ID: r.ID, Queue: r.Queue, Token: r.Token}
	}
	return out
}

// handleHeartbeat extends the leases and reports which are still held.
func (b *BrokerNode) handleHeartbeat(r heartbeatReq) {
	if !b.up {
		return
	}
	refs := storeRefs(r.refs)
	var res []store.HeartbeatResult
	b.call("heartbeat", func(s store.Store) (err error) {
		res, err = s.Heartbeat(context.Background(), refs, r.ttl)
		for _, x := range res {
			if x.Outcome == job.Fenced {
				b.stats.FencedAcks++
			}
		}
		return err
	}, func(err error) {
		resp := heartbeatResp{seq: r.seq, err: err}
		if err == nil {
			resp.statuses = make([]lease.HeartbeatStatus, len(res))
			for i, x := range res {
				resp.statuses[i] = lease.HeartbeatStatus{Held: x.Outcome == job.Applied, Remaining: x.Remaining}
			}
		}
		b.send(r.from, false, "heartbeat-resp", func() { r.reply(resp) })
	})
}

// ackReq reports finished handlers.
type ackReq struct {
	from  NodeID
	seq   uint64
	acks  []lease.Ack
	reply func(ackResp)
}

// ackResp answers an ackReq: one status per ack, or an error.
type ackResp struct {
	seq      uint64
	statuses []lease.AckStatus
	err      error
}

// ackStatus maps what the store did with an ack to what the worker is told.
func ackStatus(o job.Outcome) lease.AckStatus {
	switch o {
	case job.Applied:
		return lease.AckAccepted
	case job.Duplicate:
		return lease.AckDuplicate
	case job.Fenced, job.Expired, job.NotFound, job.WrongState:
		return lease.AckFenced
	}
	return lease.AckFenced
}

// handleAcks completes the jobs that succeeded and fails the others. A retry
// with no delay of its own waits the default backoff.
func (b *BrokerNode) handleAcks(r ackReq) {
	if !b.up {
		return
	}
	var completes []store.CompleteRequest
	var fails []store.FailRequest
	isComplete := make([]bool, len(r.acks))
	pos := make([]int, len(r.acks)) // index into completes or fails
	for i, a := range r.acks {
		ref := store.Ref{ID: a.Ref.ID, Queue: a.Ref.Queue, Token: a.Ref.Token}
		switch a.Outcome.Kind {
		case lease.OutcomeSuccess:
			isComplete[i], pos[i] = true, len(completes)
			completes = append(completes, store.CompleteRequest{Ref: ref, Result: a.Outcome.Result})
		case lease.OutcomeRetry:
			delay := a.Outcome.RetryAfter
			if delay <= 0 {
				delay = job.DefaultBackoff.Delay(a.Attempt, b.rng.Float64())
			}
			pos[i] = len(fails)
			fails = append(fails, store.FailRequest{Ref: ref, Delay: delay, Error: a.Outcome.Error})
		case lease.OutcomePermanent:
			pos[i] = len(fails)
			fails = append(fails, store.FailRequest{Ref: ref, Permanent: true, Error: a.Outcome.Error})
		}
	}
	var cOut, fOut []job.Outcome
	b.call("ack", func(s store.Store) (err error) {
		if len(completes) > 0 {
			if cOut, err = s.Complete(context.Background(), completes); err != nil {
				return err
			}
		}
		if len(fails) > 0 {
			if fOut, err = s.Fail(context.Background(), fails); err != nil {
				return err
			}
		}
		b.countFenced(cOut)
		b.countFenced(fOut)
		return nil
	}, func(err error) {
		resp := ackResp{seq: r.seq, err: err}
		if err == nil {
			resp.statuses = make([]lease.AckStatus, len(r.acks))
			for i := range r.acks {
				if isComplete[i] {
					resp.statuses[i] = ackStatus(cOut[pos[i]])
				} else {
					resp.statuses[i] = ackStatus(fOut[pos[i]])
				}
			}
		}
		b.send(r.from, false, "ack-resp", func() { r.reply(resp) })
	})
}

// releaseReq gives leases back unrun.
type releaseReq struct {
	from  NodeID
	seq   uint64
	refs  []lease.Ref
	reply func(releaseResp)
}

// releaseResp answers a releaseReq.
type releaseResp struct {
	seq uint64
	err error
}

// handleRelease gives the leases back to the store.
func (b *BrokerNode) handleRelease(r releaseReq) {
	if !b.up {
		return
	}
	refs := storeRefs(r.refs)
	b.call("release", func(s store.Store) error {
		out, err := s.Release(context.Background(), refs)
		b.countFenced(out)
		return err
	}, func(err error) {
		b.send(r.from, false, "release-resp", func() { r.reply(releaseResp{seq: r.seq, err: err}) })
	})
}

// enqueueReq asks the broker to create one job.
type enqueueReq struct {
	from  NodeID
	spec  job.Spec
	reply func(enqueueResp)
}

// enqueueResp answers an enqueueReq with the id of the job.
type enqueueResp struct {
	id  int64
	err error
}

// handleEnqueue creates the job, or finds the one the spec's key names.
func (b *BrokerNode) handleEnqueue(r enqueueReq) {
	if !b.up {
		return
	}
	var res []store.EnqueueResult
	b.call("enqueue", func(s store.Store) (err error) {
		res, err = s.Enqueue(context.Background(), []job.Spec{r.spec})
		return err
	}, func(err error) {
		resp := enqueueResp{err: err}
		if err == nil {
			resp.id = res[0].ID
		}
		b.send(r.from, false, "enqueue-resp", func() { r.reply(resp) })
	})
}

// cancelJobReq asks the broker to cancel a job.
type cancelJobReq struct {
	from   NodeID
	id     int64
	tenant string
	reply  func(cancelJobResp)
}

// cancelJobResp answers a cancelJobReq with what the store did.
type cancelJobResp struct {
	outcome job.Outcome
	err     error
}

// handleCancelJob cancels the job.
func (b *BrokerNode) handleCancelJob(r cancelJobReq) {
	if !b.up {
		return
	}
	var out job.Outcome
	b.call("cancel", func(s store.Store) (err error) {
		out, err = s.Cancel(context.Background(), r.id, r.tenant)
		return err
	}, func(err error) {
		b.send(r.from, false, "cancel-resp", func() { r.reply(cancelJobResp{outcome: out, err: err}) })
	})
}

// reap takes back the leases that ran out.
func (b *BrokerNode) reap() {
	backoff := func(attempts int32) time.Duration {
		return job.DefaultBackoff.Delay(attempts, b.rng.Float64())
	}
	b.call("reap", func(s store.Store) error {
		n, err := s.Reap(context.Background(), reapBatch, backoff)
		b.stats.Reaps += n
		return err
	}, func(error) {})
}

// prune deletes the finished jobs that outlived their retention.
func (b *BrokerNode) prune() {
	b.call("prune", func(s store.Store) error {
		n, err := s.Prune(context.Background(), b.cfg.Retention, pruneBatch)
		b.stats.Prunes += n
		return err
	}, func(error) {})
}

// refreshActivity re-reads which tenants have work on every queue that a
// worker has asked this broker for, so that a tenant whose notification was
// lost joins the ring, and when the next scheduled job falls due. It does not
// skip a queue that has no waiter parked: under a flood the waiters are
// mostly busy fetching, and that is when a lost notification would starve a
// small tenant. A queue that has tenants with work is woken, because the
// dispatch core may hold "known empty" from a fetch that ran while the ring
// was empty, and a refresh that finds due work is the signal that this is
// stale (a delayed job that fell due, a lost notification). The wakeup is
// one fetch at most, and none if the core finds no waiter to serve.
func (b *BrokerNode) refreshActivity() {
	for _, queue := range slices.Sorted(maps.Keys(b.queues)) {
		var act store.Activity
		b.call("activity", func(s store.Store) (err error) {
			act, err = s.Activity(context.Background(), queue)
			return err
		}, func(err error) {
			if err != nil {
				return
			}
			b.applyActivity(queue, act)
			if len(act.Tenants) > 0 {
				b.apply(b.core.Notify(b.mono(), queue))
			}
		})
	}
}

// broker returns the broker attached under id. A message to an id that is
// not a broker is a mistake in the scenario, so it panics.
func (n *Network) broker(id NodeID) *BrokerNode {
	b, ok := n.brokers[id]
	if !ok {
		panic("sim: message sent to " + string(id) + ", which is not a broker attached to the network")
	}
	return b
}
