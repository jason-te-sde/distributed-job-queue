package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/dispatch"
	"github.com/jason-te-sde/distributed-job-queue/internal/core/fair"
	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

const (
	// defaultStoreTimeout bounds every store call the dispatcher makes for
	// a fetch, a release or an activity read. A call that never returns would
	// otherwise hold a waiter, and the shutdown, forever.
	defaultStoreTimeout = 10 * time.Second
	// activityWindow is how long after its last waiter a queue keeps its
	// activity refreshed and its fair-share state. A queue that has been idle
	// longer is forgotten and learned again by its next waiter.
	activityWindow = time.Minute
	// eventBuffer is the capacity of the channel that carries events to the
	// loop. The loop never blocks on anything but its select, so the buffer
	// only smooths bursts.
	eventBuffer = 1024
)

// errDraining is what a Lease that arrives while the broker drains gets.
var errDraining = status.Error(codes.Unavailable, "broker is draining")

// DispatcherConfig configures a Dispatcher. Zero values of MaxInFlight,
// PollInterval, Quantum and ActivityInterval select the defaults of the
// packages that use them: 4, 1s, 8 and 1s.
type DispatcherConfig struct {
	// Scheduler is how a fetch is shared between tenants: "drr" for
	// weighted deficit round robin, "fifo" for arrival order across all
	// tenants.
	Scheduler string
	// Weights maps tenants to their weight under "drr"; a tenant that is
	// absent weighs 1.
	Weights map[string]int64
	// Quantum is the number of jobs a tenant of weight 1 takes per turn
	// under "drr".
	Quantum int64
	// MaxInFlight is the most fetches that run at once for one queue.
	MaxInFlight int
	// PollInterval is how long a queue with waiters may go without a fetch.
	PollInterval time.Duration
	// ActivityInterval is how often the store is asked which tenants of a
	// queue have work and when its next scheduled job falls due.
	ActivityInterval time.Duration
}

// outcome is what a waiter is answered with.
type outcome struct {
	ans LeaseAnswer
	err error
}

// The states of the hand-off between a Lease call and the loop. Exactly one
// side moves a waiter out of waiterWaiting, and that decides who owns the
// jobs fetched for it.
const (
	// waiterWaiting means nobody has answered the waiter and its caller is
	// still there.
	waiterWaiting int32 = iota
	// waiterAnswered means the loop is writing an answer that the caller will
	// read.
	waiterAnswered
	// waiterAbandoned means the caller gave up, and the loop must not answer:
	// whatever it fetched for the waiter goes back to the store.
	waiterAbandoned
)

// waiter is one blocked Lease call. The fields above the line are set before
// the waiter is shared and never change, or are atomic. The ones below are
// the loop's.
type waiter struct {
	id     dispatch.WaiterID
	call   LeaseCall
	answer chan outcome // capacity 1, written once by the loop
	state  atomic.Int32 // waiterWaiting, waiterAnswered or waiterAbandoned

	// -- owned by the loop --

	arrived dispatch.Mono
	// grants are the jobs fetched for the waiter and not yet delivered.
	grants []store.Grant
	// fetching says a fetch for the waiter is out, or waits for the first
	// activity read of its queue.
	fetching bool
	// cancelled says the caller went away while the fetch was out, so that the
	// entry is dropped when the fetch comes back.
	cancelled bool
}

// queueInfo is what the shell knows about one queue, next to the core: the
// fair-share planner and the activity read that fills its ring.
type queueInfo struct {
	// drr is the planner of the queue; nil under "fifo".
	drr *fair.DRR
	// probing says an activity read for the queue is out.
	probing bool
	// parked holds the fetches the core asked for while the ring was empty.
	// They start when the activity read that fills the ring comes back.
	parked []dispatch.Fetch
	// waiters is the number of Lease calls registered on the queue, and
	// lastWaiter the instant the count last fell to zero.
	waiters    int
	lastWaiter dispatch.Mono
}

// The events of the loop. Everything that changes the core or the planners
// reaches the loop as one of these, or through the notify set.
//
//sumtype:decl
type event interface{ isEvent() }

type (
	// evArrive registers a waiter.
	evArrive struct{ w *waiter }
	// evCancel reports that the caller of a waiter has gone.
	evCancel struct{ w *waiter }
	// evDrain starts the drain of the core.
	evDrain struct{}
	// evFetchDone carries the result of a store fetch.
	evFetchDone struct {
		id      dispatch.WaiterID
		queue   string
		planned []fair.Take // what the fair planner asked for; nil for a fetch across tenants
		grants  []store.Grant
		err     error
	}
	// evActivity carries the result of an activity read.
	evActivity struct {
		queue string
		act   store.Activity
		err   error
	}
	// evSettled reports that a release has finished.
	evSettled struct{}
)

func (evArrive) isEvent()    {}
func (evCancel) isEvent()    {}
func (evDrain) isEvent()     {}
func (evFetchDone) isEvent() {}
func (evActivity) isEvent()  {}
func (evSettled) isEvent()   {}

// pendingNotify is what notifications for one queue left since the loop last
// looked.
type pendingNotify struct {
	count   int
	tenants map[string]struct{}
}

// Dispatcher is the shell around dispatch.Core. It owns one goroutine, the
// loop that Run executes, and that goroutine alone touches the core and the
// fair-share planners: callers and the goroutines that talk to the store reach
// it through a channel, and it carries out the core's effects. Lease is the
// only call that blocks, and it blocks on its own waiter.
//
// Fetches, releases and activity reads run in goroutines of their own, each
// bounded by a timeout, and report back to the loop.
type Dispatcher struct {
	st  store.Store
	cfg DispatcherConfig
	m   *Metrics
	log *slog.Logger

	// storeTimeout bounds each store call of the dispatcher. It is a field so
	// that a test can shorten it; nothing else changes it.
	storeTimeout time.Duration
	start        time.Time
	maxInFlight  int

	events chan event
	wake   chan struct{}
	// done is closed when Run returns.
	done     chan struct{}
	draining atomic.Bool
	started  atomic.Bool
	nextID   atomic.Uint64

	// nmu guards the notifications that wait for the loop: the queues
	// notified, how often and for which tenants, and whether a resync was
	// asked for.
	nmu    sync.Mutex
	pend   map[string]*pendingNotify
	resync bool

	// -- owned by the loop goroutine --

	// owner is the id of the goroutine in the loop, recorded in race builds.
	owner   atomic.Uint64
	core    *dispatch.Core
	queues  map[string]*queueInfo
	waiters map[dispatch.WaiterID]*waiter
	// pending counts the goroutines the loop started and has not heard back
	// from.
	pending int
	timer   *time.Timer
}

// NewDispatcher returns a dispatcher over st. Its loop starts when Run is
// called. It panics if st, m or log is nil, or if the scheduler is not "drr"
// or "fifo".
func NewDispatcher(st store.Store, cfg DispatcherConfig, m *Metrics, log *slog.Logger) *Dispatcher {
	switch {
	case st == nil:
		panic("broker.NewDispatcher: store must not be nil")
	case m == nil:
		panic("broker.NewDispatcher: metrics must not be nil")
	case log == nil:
		panic("broker.NewDispatcher: logger must not be nil")
	case cfg.Scheduler != "drr" && cfg.Scheduler != "fifo":
		panic(fmt.Sprintf("broker.NewDispatcher: scheduler must be \"drr\" or \"fifo\", got %q", cfg.Scheduler))
	}
	if cfg.ActivityInterval <= 0 {
		cfg.ActivityInterval = time.Second
	}
	maxInFlight := cfg.MaxInFlight
	if maxInFlight <= 0 {
		maxInFlight = 4
	}
	return &Dispatcher{
		st: st, cfg: cfg, m: m, log: log,
		storeTimeout: defaultStoreTimeout,
		start:        time.Now(),
		maxInFlight:  maxInFlight,
		events:       make(chan event, eventBuffer),
		wake:         make(chan struct{}, 1),
		done:         make(chan struct{}),
		pend:         map[string]*pendingNotify{},
		core:         dispatch.New(dispatch.Config{MaxInFlight: cfg.MaxInFlight, PollInterval: cfg.PollInterval}),
		queues:       map[string]*queueInfo{},
		waiters:      map[dispatch.WaiterID]*waiter{},
	}
}

// Run is the event loop. It returns nil once ctx is done and the dispatcher
// has drained: every waiter answered, the fetches that were in flight finished
// and the jobs fetched for callers that had gone given back. It returns an
// error if it is called a second time, because the dispatcher has one loop.
func (d *Dispatcher) Run(ctx context.Context) error {
	if !d.started.CompareAndSwap(false, true) {
		return errors.New("broker: Dispatcher.Run called twice")
	}
	defer close(d.done)
	d.timer = time.NewTimer(time.Hour)
	d.timer.Stop()
	defer d.timer.Stop()
	activity := time.NewTicker(d.cfg.ActivityInterval)
	defer activity.Stop()

	stopping := ctx.Done()
	stopped := false
	for !stopped || d.pending > 0 {
		select {
		case ev := <-d.events:
			d.onEvent(ev)
		case <-d.wake:
			d.onWake()
		case <-d.timer.C:
			d.onTimer()
		case <-activity.C:
			d.onActivityTick()
		case <-stopping:
			d.onStop()
			stopping, stopped = nil, true
		}
	}
	return nil
}

// Lease registers a waiter for the call and blocks until the loop answers it,
// ctx is done, or the dispatcher has stopped. An answer is a list of jobs,
// possibly empty, or a gRPC status. If ctx ends first the waiter is cancelled
// and the call returns the context's error as a status; jobs that were
// fetched for it in the meantime are given back, never dropped.
func (d *Dispatcher) Lease(ctx context.Context, c LeaseCall) (LeaseAnswer, error) {
	if d.draining.Load() {
		return LeaseAnswer{}, errDraining
	}
	if err := ctx.Err(); err != nil {
		return LeaseAnswer{}, toStatus(err)
	}
	w := &waiter{id: dispatch.WaiterID(d.nextID.Add(1)), call: c, answer: make(chan outcome, 1)}
	if !d.post(evArrive{w}) {
		return LeaseAnswer{}, errDraining
	}
	select {
	case out := <-w.answer:
		return d.finish(ctx, out)
	case <-ctx.Done():
		if w.state.CompareAndSwap(waiterWaiting, waiterAbandoned) {
			d.post(evCancel{w})
			return LeaseAnswer{}, toStatus(ctx.Err())
		}
		// The loop answered first: it is writing the answer, and the jobs in it
		// are ours to deal with.
		return d.finish(ctx, <-w.answer)
	case <-d.done:
		if w.state.CompareAndSwap(waiterWaiting, waiterAbandoned) {
			return LeaseAnswer{}, errDraining
		}
		return d.finish(ctx, <-w.answer)
	}
}

// finish turns the answer the loop wrote into the result of Lease. Jobs for a
// caller whose context has ended since go back to the store: nobody will run
// them, and leaving them to expire would delay them by a lease TTL.
func (d *Dispatcher) finish(ctx context.Context, out outcome) (LeaseAnswer, error) {
	if out.err != nil {
		return LeaseAnswer{}, out.err
	}
	if len(out.ans.Grants) > 0 && ctx.Err() != nil {
		d.releaseGrants(out.ans.Grants)
		return LeaseAnswer{}, toStatus(ctx.Err())
	}
	return out.ans, nil
}

// Notify tells the dispatcher that jobs of the tenant may have become ready on
// the queue. It never blocks: the notification is added to a set that the loop
// empties, so that a burst of them costs the loop one wakeup.
func (d *Dispatcher) Notify(queue, tenant string) {
	select {
	case <-d.done:
		return
	default:
	}
	d.nmu.Lock()
	p := d.pend[queue]
	if p == nil {
		p = &pendingNotify{tenants: map[string]struct{}{}}
		d.pend[queue] = p
	}
	p.count++
	p.tenants[tenant] = struct{}{}
	d.nmu.Unlock()
	d.poke()
}

// Resync tells the dispatcher that notifications may have been lost, as they
// are while the listener is disconnected. Every queue with waiters is woken
// once, and the activity of every queue is read at once.
func (d *Dispatcher) Resync() {
	select {
	case <-d.done:
		return
	default:
	}
	d.nmu.Lock()
	d.resync = true
	d.nmu.Unlock()
	d.poke()
}

// Drain starts the shutdown. From now on Lease returns codes.Unavailable with
// "broker is draining", and the waiters that are parked end with an empty
// answer. A waiter whose fetch is in flight gets the jobs the fetch finds,
// because their leases are valid.
func (d *Dispatcher) Drain() {
	d.draining.Store(true)
	d.post(evDrain{})
}

// poke wakes the loop to look at the notify set.
func (d *Dispatcher) poke() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

// post sends an event to the loop. It reports false if the loop has returned.
func (d *Dispatcher) post(ev event) bool {
	select {
	case d.events <- ev:
		return true
	case <-d.done:
		return false
	}
}

// enter records that the calling goroutine runs the loop, and in a race build
// panics if another goroutine has done so: the core and the planners have
// exactly one owner, and nothing in the types stops a second goroutine from
// calling them.
func (d *Dispatcher) enter() {
	if !raceEnabled {
		return
	}
	id := goroutineID()
	if !d.owner.CompareAndSwap(0, id) && d.owner.Load() != id {
		panic("dispatch core entered from two goroutines")
	}
}

// now is the monotonic instant that the core is given: the time since the
// dispatcher was created, and at least 1ns because the core reads a zero due
// instant as "none".
func (d *Dispatcher) now() dispatch.Mono { return max(time.Since(d.start), 1) }

func (d *Dispatcher) onEvent(ev event) {
	d.enter()
	now := d.now()
	switch ev := ev.(type) {
	case evArrive:
		d.onArrive(now, ev.w)
	case evCancel:
		d.onCancel(now, ev.w)
	case evDrain:
		d.onStop()
	case evFetchDone:
		d.onFetchDone(now, ev)
	case evActivity:
		d.onActivity(now, ev)
	case evSettled:
		d.pending--
	}
}

func (d *Dispatcher) onTimer() {
	d.enter()
	now := d.now()
	d.apply(now, d.core.Tick(now))
}

func (d *Dispatcher) onStop() {
	d.enter()
	d.draining.Store(true)
	now := d.now()
	d.apply(now, d.core.Drain(now))
}

func (d *Dispatcher) onArrive(now dispatch.Mono, w *waiter) {
	qi := d.queue(w.call.Queue)
	w.arrived = now
	d.waiters[w.id] = w
	qi.waiters++
	d.m.waiters.For(w.call.Queue).Inc()
	d.apply(now, d.core.Arrive(now, dispatch.Waiter{
		ID: w.id, Queue: w.call.Queue, Max: w.call.Max,
		Arrived: now, Deadline: now + w.call.Wait,
	}))
}

func (d *Dispatcher) onCancel(now dispatch.Mono, w *waiter) {
	if d.waiters[w.id] != w {
		return // already answered
	}
	effs := d.core.Cancel(now, w.id)
	if w.fetching {
		// The core keeps the entry until the fetch comes back, and so do we.
		w.cancelled = true
	} else {
		d.forget(now, w)
	}
	d.apply(now, effs)
}

func (d *Dispatcher) onFetchDone(now dispatch.Mono, ev evFetchDone) {
	d.pending--
	grants := ev.grants
	result := fetchJobs
	switch {
	case ev.err != nil:
		result = fetchError
		d.log.Warn("lease fetch failed", slog.String("queue", ev.queue), slog.Any("err", ev.err))
		// A store returns no jobs with an error. If one ever does, they are
		// not for anyone: give them back instead of letting the core drop them.
		d.releaseAsync(grants)
		grants = nil
	case len(grants) == 0:
		result = fetchEmpty
	}
	d.m.fetches.For(ev.queue, result).Inc()
	if len(grants) > 0 {
		d.m.leased.For(ev.queue).Add(float64(len(grants)))
	}
	if qi := d.queues[ev.queue]; qi != nil && qi.drr != nil && ev.err == nil {
		settle(qi.drr, ev.planned, grants)
	}

	w := d.waiters[ev.id]
	if w == nil {
		d.log.Error("a fetch came back for a waiter the dispatcher does not know", slog.String("queue", ev.queue))
		d.releaseAsync(grants)
		return
	}
	w.fetching = false
	w.grants = grants
	d.apply(now, d.core.FetchDone(now, ev.id, len(grants), ev.err != nil, fetchScope(ev.planned)))
	if d.waiters[ev.id] == w && w.cancelled {
		d.forget(now, w) // the core dropped it without an effect: nothing was fetched
	}
}

// fetchScope tells the core what a fetch asked the store for: a take for each
// tenant of the fair planner's plan, or a single take across all tenants when
// there was no plan, under "fifo" and with an empty ring. The core needs it
// because a short answer to the first only means that its tenants ran dry.
func fetchScope(planned []fair.Take) dispatch.FetchScope {
	if len(planned) == 0 {
		return dispatch.AcrossTenants
	}
	return dispatch.PerTenant
}

// settle reports what a fetch delivered. A fetch planned by the fair planner
// settles each take against the grants of its tenant, so that a tenant that
// came up short leaves the ring. A fetch across all tenants teaches the ring
// the tenants it found.
func settle(drr *fair.DRR, planned []fair.Take, grants []store.Grant) {
	if len(planned) == 0 {
		var last string
		for i, g := range grants {
			if i == 0 || g.Job.Tenant != last {
				drr.Activate(g.Job.Tenant)
				last = g.Job.Tenant
			}
		}
		return
	}
	index := make(map[string]int, len(planned))
	for i, tk := range planned {
		index[tk.Tenant] = i
	}
	got := make([]int, len(planned))
	for _, g := range grants {
		if i, ok := index[g.Job.Tenant]; ok {
			got[i]++
		}
	}
	drr.Settle(planned, got)
}

func (d *Dispatcher) onActivity(now dispatch.Mono, ev evActivity) {
	d.pending--
	qi := d.queues[ev.queue]
	if qi == nil {
		return // a probe keeps its queue alive, so this does not happen
	}
	qi.probing = false
	// A failed read leaves the ring as it was. If it is empty, the parked
	// fetches go across all tenants, which still makes progress.
	if ev.err != nil {
		d.log.Warn("activity read failed", slog.String("queue", ev.queue), slog.Any("err", ev.err))
	} else {
		if qi.drr != nil {
			qi.drr.SetActive(ev.act.Tenants)
		}
		// A tenant with a job that is ready now, while a waiter is parked,
		// means that a wakeup was lost, or that a scheduled job fell due
		// between two due times. The core holds one due time per queue and a
		// later read replaces it, so this read is also what finds the job
		// whose due time it has just replaced.
		if len(ev.act.Tenants) > 0 {
			d.apply(now, d.core.Notify(now, ev.queue))
		}
		// The due time is never cleared when the queue has no scheduled job:
		// the read can run just before the due time of the last scheduled job
		// passes, and clearing it would lose the wakeup. A due time that
		// outlives its job costs one empty fetch. now is later than the instant
		// the store computed NextDueIn at, so the due time is never early.
		if ev.act.HasNextDue {
			d.apply(now, d.core.DueAt(now, ev.queue, max(now+ev.act.NextDueIn, 1)))
		}
	}
	parked := qi.parked
	qi.parked = nil
	for _, f := range parked {
		d.launchFetch(f, qi)
	}
}

func (d *Dispatcher) onActivityTick() {
	d.enter()
	if d.draining.Load() {
		return
	}
	d.refreshActivity(d.now())
}

// refreshActivity reads the activity of every queue that has had a waiter
// within activityWindow, and forgets the queues that have not.
func (d *Dispatcher) refreshActivity(now dispatch.Mono) {
	for _, name := range slices.Sorted(maps.Keys(d.queues)) {
		qi := d.queues[name]
		if qi.waiters == 0 && !qi.probing && now-qi.lastWaiter > activityWindow {
			delete(d.queues, name)
			continue
		}
		d.probe(name, qi)
	}
}

// probe starts an activity read for the queue unless one is out.
func (d *Dispatcher) probe(queue string, qi *queueInfo) {
	if qi.probing {
		return
	}
	qi.probing = true
	d.pending++
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), d.storeTimeout)
		defer cancel()
		act, err := d.st.Activity(ctx, queue)
		d.post(evActivity{queue: queue, act: act, err: err})
	}()
}

func (d *Dispatcher) onWake() {
	d.enter()
	d.nmu.Lock()
	pend, resync := d.pend, d.resync
	d.pend, d.resync = map[string]*pendingNotify{}, false
	d.nmu.Unlock()

	now := d.now()
	for _, queue := range slices.Sorted(maps.Keys(pend)) {
		qi := d.queues[queue]
		if qi == nil {
			continue // nobody has waited here lately; the next waiter fetches anyway
		}
		p := pend[queue]
		if qi.drr != nil {
			for _, tenant := range slices.Sorted(maps.Keys(p.tenants)) {
				qi.drr.Activate(tenant)
			}
		}
		// Each notification is worth one fetch, and no queue can run more at
		// once than MaxInFlight.
		for range min(p.count, d.maxInFlight) {
			d.apply(now, d.core.Notify(now, queue))
		}
	}
	if resync && !d.draining.Load() {
		for _, queue := range slices.Sorted(maps.Keys(d.queues)) {
			d.apply(now, d.core.Notify(now, queue))
		}
		d.refreshActivity(now)
	}
}

// queue returns the shell's state of the named queue, creating it.
func (d *Dispatcher) queue(name string) *queueInfo {
	qi, ok := d.queues[name]
	if !ok {
		qi = &queueInfo{}
		if d.cfg.Scheduler == "drr" {
			qi.drr = fair.New(fair.Config{QuantumPerWeight: d.cfg.Quantum})
			qi.drr.SetWeights(d.cfg.Weights)
		}
		d.queues[name] = qi
	}
	return qi
}

// apply carries out the effects the core returned from one call, in order. It
// then arms the single timer for the Arm effect, or stops it when there is
// none: the core computes its Arm from its whole state at the end of every
// call, so no Arm means that nothing is waiting for an instant. It must only
// follow a call into the core.
func (d *Dispatcher) apply(now dispatch.Mono, effs []dispatch.Effect) {
	var at dispatch.Mono
	armed := false
	for _, eff := range effs {
		switch eff := eff.(type) {
		case dispatch.Fetch:
			d.onFetch(eff)
		case dispatch.Deliver:
			d.deliver(now, eff.Waiter)
		case dispatch.Respond:
			d.respond(now, eff)
		case dispatch.Release:
			d.releaseWaiter(now, eff.Waiter)
		case dispatch.Arm:
			at, armed = eff.At, true
		}
	}
	if !armed {
		d.timer.Stop()
		return
	}
	d.timer.Reset(max(at-d.now(), 0))
}

// waiterFor returns the waiter of an effect. The core only names waiters it
// was given and that the shell has not forgotten, so a missing one is a bug:
// it is logged and the effect is skipped.
func (d *Dispatcher) waiterFor(id dispatch.WaiterID, effect string) *waiter {
	w := d.waiters[id]
	if w == nil {
		d.log.Error("the core named a waiter the dispatcher does not know", slog.String("effect", effect), slog.Uint64("waiter", uint64(id)))
	}
	return w
}

// onFetch carries out a Fetch effect. Under "drr" a fetch that finds the ring
// empty first reads the queue's activity, which also sets the due time, and
// plans only when the answer is in: the planner shares a batch between the
// tenants in its ring, and an empty ring is the state of a queue that was
// never read, and of one whose tenants all ran dry, in which a job that was
// scheduled or whose notification was lost would otherwise wait for the next
// activity tick.
func (d *Dispatcher) onFetch(f dispatch.Fetch) {
	w := d.waiterFor(f.Waiter, "fetch")
	if w == nil {
		return
	}
	w.fetching = true
	qi := d.queues[f.Queue]
	if qi.drr != nil && len(qi.drr.Active()) == 0 {
		qi.parked = append(qi.parked, f)
		d.probe(f.Queue, qi)
		return
	}
	d.launchFetch(f, qi)
}

// launchFetch plans a fetch on the loop and runs its store call in a
// goroutine of its own. With no tenants in the ring, and under "fifo", the
// plan is a single take across all tenants in arrival order.
func (d *Dispatcher) launchFetch(f dispatch.Fetch, qi *queueInfo) {
	w := d.waiters[f.Waiter]
	var planned []fair.Take
	if qi.drr != nil {
		planned = qi.drr.Plan(f.Max)
	}
	takes := make([]store.TenantTake, len(planned))
	for i, tk := range planned {
		takes[i] = store.TenantTake{Tenant: tk.Tenant, Limit: tk.Limit}
	}
	if len(takes) == 0 {
		takes = []store.TenantTake{{Limit: f.Max}}
	}
	req := store.LeaseRequest{Queue: f.Queue, Takes: takes, TTL: w.call.TTL, Worker: w.call.Worker}
	d.pending++
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), d.storeTimeout)
		defer cancel()
		grants, err := d.st.Lease(ctx, req)
		d.post(evFetchDone{id: f.Waiter, queue: f.Queue, planned: planned, grants: grants, err: err})
	}()
}

// deliver carries out a Deliver effect.
func (d *Dispatcher) deliver(now dispatch.Mono, id dispatch.WaiterID) {
	w := d.waiterFor(id, "deliver")
	if w == nil {
		return
	}
	grants, waited := w.grants, now-w.arrived
	w.grants = nil
	d.forget(now, w)
	d.m.leaseWait.Observe(waited.Seconds())
	d.answer(w, outcome{ans: LeaseAnswer{Grants: grants, Waited: waited}})
}

// respond carries out a Respond effect.
func (d *Dispatcher) respond(now dispatch.Mono, r dispatch.Respond) {
	w := d.waiterFor(r.Waiter, "respond")
	if w == nil {
		return
	}
	d.forget(now, w)
	d.answer(w, respondOutcome(r.Reason, now-w.arrived))
}

// releaseWaiter carries out a Release effect: the waiter is gone and the jobs
// fetched for it go back to the store.
func (d *Dispatcher) releaseWaiter(now dispatch.Mono, id dispatch.WaiterID) {
	w := d.waiterFor(id, "release")
	if w == nil {
		return
	}
	grants := w.grants
	w.grants = nil
	d.forget(now, w)
	d.releaseAsync(grants)
}

// respondOutcome maps the reason for an empty answer to what the caller gets,
// following the shutdown rules of the design: a long poll that is in flight
// when the broker drains ends empty, so that the worker polls again and finds
// another broker, while a request that arrives after the drain started is
// refused.
func respondOutcome(r dispatch.Reason, waited time.Duration) outcome {
	switch r {
	case dispatch.ReasonTimeout, dispatch.ReasonDraining:
		return outcome{ans: LeaseAnswer{Waited: waited}}
	case dispatch.ReasonRejected:
		return outcome{err: errDraining}
	case dispatch.ReasonUnavailable:
		return outcome{err: status.Error(codes.Unavailable, store.ErrUnavailable.Error())}
	}
	return outcome{err: toStatus(fmt.Errorf("dispatcher: unknown reason %v for an empty answer", r))}
}

// answer hands the waiter its outcome, unless its caller has abandoned it, in
// which case the jobs in it go back to the store.
func (d *Dispatcher) answer(w *waiter, out outcome) {
	if !w.state.CompareAndSwap(waiterWaiting, waiterAnswered) {
		d.releaseAsync(out.ans.Grants)
		return
	}
	w.answer <- out
}

// forget drops a waiter from the shell once it has been answered or cancelled.
func (d *Dispatcher) forget(now dispatch.Mono, w *waiter) {
	delete(d.waiters, w.id)
	qi := d.queues[w.call.Queue]
	qi.waiters--
	if qi.waiters == 0 {
		qi.lastWaiter = now
	}
	d.m.waiters.For(w.call.Queue).Dec()
}

// releaseAsync gives jobs back to the store from a goroutine of its own, which
// the loop waits for before it returns.
func (d *Dispatcher) releaseAsync(grants []store.Grant) {
	if len(grants) == 0 {
		return
	}
	d.pending++
	go func() {
		d.releaseGrants(grants)
		d.post(evSettled{})
	}()
}

// releaseGrants gives jobs that were fetched for a caller who is gone back to
// the store, which refunds their attempts. A failure is logged and the leases
// are left to expire, which the reaper handles. It runs in whatever goroutine
// calls it and touches nothing the loop owns.
func (d *Dispatcher) releaseGrants(grants []store.Grant) {
	if len(grants) == 0 {
		return
	}
	refs := make([]store.Ref, len(grants))
	for i, g := range grants {
		refs[i] = store.Ref{ID: g.Job.ID, Queue: g.Job.Queue, Token: g.Job.Epoch}
	}
	ctx, cancel := context.WithTimeout(context.Background(), d.storeTimeout)
	defer cancel()
	outcomes, err := d.st.Release(ctx, refs)
	if err != nil {
		d.log.Warn("release of jobs fetched for a caller that left failed; they expire with their leases",
			slog.String("op", "release"), slog.String("queue", grants[0].Job.Queue), slog.Int("jobs", len(refs)), slog.Any("err", err))
		return
	}
	var released int
	for _, o := range outcomes {
		if o == job.Applied {
			released++
		}
	}
	d.m.orphanReleases.Add(float64(released))
}
