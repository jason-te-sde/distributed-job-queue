package sim

import (
	"fmt"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/core/lease"
)

const (
	// rpcTimeout is how long a worker waits for the answer to a heartbeat,
	// an ack or a release. A lease request waits its long-poll time plus
	// this.
	rpcTimeout = 5 * time.Second
	// cancelDelayMax bounds how long a cooperative handler takes to notice
	// that it was cancelled.
	cancelDelayMax = 10 * time.Millisecond
	// handlerSeedMix separates the handler's random stream from the lease
	// tracker's, which is seeded with the same number.
	handlerSeedMix = 0x2545f4914f6cdd1d
)

// HandlerModel says how simulated handlers behave.
type HandlerModel struct {
	// Duration is the range of an ordinary handler's running time.
	Duration [2]time.Duration
	// LongP is the chance that a handler instead runs for a time drawn
	// from Long, which a scenario sets beyond the TTL.
	LongP float64
	Long  [2]time.Duration
	// RetryP and PermanentP are the chances that a handler that was not
	// cancelled fails, for a retry and for good. Otherwise it succeeds.
	RetryP, PermanentP float64
}

// WorkerConfig configures a WorkerNode.
type WorkerConfig struct {
	// ID names the node on the network, and is the worker id that the
	// store records on a lease, so it must pass job.ValidateWorkerID.
	ID NodeID
	// Queue is the queue the worker takes jobs from. A lease request names
	// one queue, so a scenario with several queues needs a worker for each.
	Queue string
	// Clock is the worker's monotonic clock. Its Rate must be positive.
	Clock Clock
	// Lease configures the worker's lease tracker.
	Lease lease.Config
	// Brokers are the brokers the worker talks to, in the order it rotates
	// through them.
	Brokers []NodeID
	// CooperativeP is the chance that a handler stops soon after it is
	// cancelled. The others run to the end of their drawn time.
	CooperativeP float64
	// Handler says how handlers behave.
	Handler HandlerModel
}

// Hold is a lease that a worker believes it may still act on.
type Hold struct {
	Worker NodeID
	Ref    lease.Ref
	// Deadline is the worker's local deadline for the lease, converted to
	// store time: Epoch plus the virtual time at which the worker's clock
	// reads it.
	Deadline time.Time
}

// rpc is a request in flight from a worker: where it went and when it left,
// on the worker's clock.
type rpc struct {
	broker NodeID
	sentAt lease.Mono
}

// handlerRun is one running simulated handler.
type handlerRun struct {
	grant       lease.Grant
	cooperative bool // it stops when cancelled
	cancelled   bool // it was cancelled and will notice
	done        bool // it returned
}

// WorkerNode is a simulated worker: the real lease tracker, driven by
// messages from brokers, its own timer and handlers that take virtual time.
// It applies RPC timeouts, because the tracker needs every request it sends
// to end with an answer or a failure.
type WorkerNode struct {
	w    *World
	net  *Network
	sink *Sink
	cfg  WorkerConfig

	id           NodeID
	up           bool
	gen          int // changes with every crash and start; stale events compare it
	stalledUntil time.Duration
	rng          *Rand
	tr           *lease.Tracker
	timer        nodeTimer
	rr           int // index of the next broker to ask

	inflight map[uint64]rpc // requests whose answer or timeout is pending, by seq
	runs     map[lease.Ref]*handlerRun
}

// NewWorkerNode starts a worker. Its tracker is seeded with seed, as is its
// handlers' random stream. It panics if the queue is not a valid name, there
// is no broker, the clock does not run or the lease config is invalid.
func NewWorkerNode(w *World, net *Network, sink *Sink, cfg WorkerConfig, seed uint64) *WorkerNode {
	if err := job.ValidateName("queue", cfg.Queue); err != nil {
		panic("sim: NewWorkerNode called with a bad Queue: " + err.Error())
	}
	if len(cfg.Brokers) == 0 {
		panic("sim: NewWorkerNode called with no Brokers")
	}
	_ = cfg.Clock.Mono(0) // fail here, not at the first message, if the clock does not run
	wk := &WorkerNode{w: w, net: net, sink: sink, cfg: cfg, timer: nodeTimer{w: w, clock: cfg.Clock}}
	wk.start(cfg.ID, seed)
	return wk
}

// ID returns the id the worker currently runs under; Restart changes it.
func (wk *WorkerNode) ID() NodeID {
	return wk.id
}

// start builds a fresh tracker and begins asking for work.
func (wk *WorkerNode) start(id NodeID, seed uint64) {
	tr, err := lease.New(wk.cfg.Lease, seed)
	if err != nil {
		panic("sim: NewWorkerNode called with a bad lease config: " + err.Error())
	}
	wk.up = true
	wk.gen++
	wk.id = id
	wk.stalledUntil = 0
	wk.rng = NewRand(seed ^ handlerSeedMix)
	wk.tr = tr
	wk.rr = wk.rng.IntN(len(wk.cfg.Brokers))
	wk.inflight = map[uint64]rpc{}
	wk.runs = map[lease.Ref]*handlerRun{}
	gen := wk.gen
	wk.w.After(0, "worker start", func() {
		wk.deliver(gen, "worker start", func() { wk.apply(wk.tr.Start(wk.mono())) })
	})
}

// Crash kills the worker: its tracker and handlers vanish without sending
// an ack or a release, and it ignores everything until Restart.
func (wk *WorkerNode) Crash() {
	wk.up = false
	wk.gen++
	wk.timer.cancel()
	wk.tr, wk.inflight, wk.runs = nil, nil, nil
}

// Restart brings the worker back under a new id with a fresh tracker seeded
// with seed and no memory of its leases. A worker that is running is crashed
// first.
func (wk *WorkerNode) Restart(newID NodeID, seed uint64) {
	if wk.up {
		wk.Crash()
	}
	wk.start(newID, seed)
}

// Stall freezes the worker until d from now, as a stopped process is
// frozen: every message and timer that reaches it before then is deferred to
// the end of the stall, while its monotonic clock keeps advancing, so its
// leases run out underneath it. Stalling a crashed worker does nothing, and a
// second stall extends the first.
func (wk *WorkerNode) Stall(d time.Duration) {
	if wk.up {
		wk.stalledUntil = max(wk.stalledUntil, wk.w.Now+d)
	}
}

// Holds lists the leases that the worker believes it holds now, in ascending
// job id. A crashed worker holds none.
func (wk *WorkerNode) Holds() []Hold {
	if !wk.up {
		return nil
	}
	held := wk.tr.Held(wk.mono())
	out := make([]Hold, len(held))
	for i, h := range held {
		out[i] = Hold{Worker: wk.id, Ref: h.Ref, Deadline: Epoch.Add(wk.cfg.Clock.Virtual(h.Deadline))}
	}
	return out
}

// mono is the worker's clock reading now.
func (wk *WorkerNode) mono() lease.Mono {
	return wk.cfg.Clock.Mono(wk.w.Now)
}

// deliver runs fn for an event of incarnation gen: never after a crash, and
// not before a stall ends.
func (wk *WorkerNode) deliver(gen int, label string, fn func()) {
	if !wk.up || wk.gen != gen {
		return
	}
	if wk.w.Now < wk.stalledUntil {
		wk.w.At(wk.stalledUntil, label+" (stalled)", func() { wk.deliver(gen, label, fn) })
		return
	}
	fn()
}

// after runs fn once d has passed on the worker's clock.
func (wk *WorkerNode) after(gen int, d time.Duration, label string, fn func()) {
	wk.w.At(afterMono(wk.w, wk.cfg.Clock, d), label, func() { wk.deliver(gen, label, fn) })
}

// must panics on an error from the tracker. Each such error means that the
// shell or a broker broke the protocol, and the simulator exists to find
// that.
func (wk *WorkerNode) must(err error) {
	if err != nil {
		panic(fmt.Sprintf("sim: worker %s: %v", wk.id, err))
	}
}

// nextBroker returns the broker for the next request, rotating through them.
func (wk *WorkerNode) nextBroker() NodeID {
	id := wk.cfg.Brokers[wk.rr%len(wk.cfg.Brokers)]
	wk.rr++
	return id
}

// apply carries out the effects of a tracker call.
func (wk *WorkerNode) apply(effs []lease.Effect) {
	for _, e := range effs {
		switch e := e.(type) {
		case lease.RequestLeases:
			wk.requestLeases(e)
		case lease.AbandonLeaseRequest:
			wk.abandon(e.Seq)
		case lease.StartHandler:
			wk.startHandler(e.Grant)
		case lease.CancelHandler:
			wk.cancelHandler(e.Ref)
		case lease.SendHeartbeat:
			wk.sendHeartbeat(e)
		case lease.SendAcks:
			wk.sendAcks(e)
		case lease.SendRelease:
			wk.sendRelease(e)
		case lease.Arm:
			wk.arm(e.At)
		case lease.Discard, lease.Stopped:
			// A discarded grant's payload is dropped, which the simulator
			// has no payload to do, and its release is a separate effect.
			// Stopped ends a drain, which the simulator never starts.
		}
	}
}

// arm sets the one timer that calls the tracker's Tick.
func (wk *WorkerNode) arm(at lease.Mono) {
	gen := wk.gen
	wk.timer.arm(at, "worker tick", func() {
		wk.deliver(gen, "worker tick", func() { wk.apply(wk.tr.Tick(wk.mono())) })
	})
}

// requestLeases sends a long-poll request to the next broker. If no answer
// comes within the wait plus the RPC timeout, the request fails and the
// broker is told to forget it.
func (wk *WorkerNode) requestLeases(e lease.RequestLeases) {
	gen, from, to := wk.gen, wk.id, wk.nextBroker()
	wk.inflight[e.Seq] = rpc{broker: to, sentAt: wk.mono()}
	req := leaseReq{
		from: from, seq: e.Seq, queue: wk.cfg.Queue, max: e.Max, wait: e.Wait, ttl: e.TTL,
		reply: func(r leaseResp) { wk.deliver(gen, "lease-resp", func() { wk.onLeaseResp(r) }) },
	}
	wk.net.Send(from, to, false, "lease-req", func() { wk.net.broker(to).handleLeaseReq(req) })
	wk.after(gen, e.Wait+rpcTimeout, "lease-timeout", func() {
		if _, pending := wk.inflight[e.Seq]; !pending {
			return
		}
		delete(wk.inflight, e.Seq)
		wk.apply(wk.tr.LeasesFailed(wk.mono(), e.Seq))
		wk.cancelAtBroker(from, to, e.Seq)
	})
}

// abandon drops the lease request seq and tells the broker.
func (wk *WorkerNode) abandon(seq uint64) {
	if rec, pending := wk.inflight[seq]; pending {
		delete(wk.inflight, seq)
		wk.cancelAtBroker(wk.id, rec.broker, seq)
	}
}

// cancelAtBroker tells a broker that the client of request seq gave up.
func (wk *WorkerNode) cancelAtBroker(from, to NodeID, seq uint64) {
	wk.net.Send(from, to, true, "lease-cancel", func() { wk.net.broker(to).handleCancel(cancelReq{from: from, seq: seq}) })
}

// onLeaseResp handles the answer to a lease request. An answer that comes
// after the request timed out still goes to the tracker, which discards the
// grants of a request it no longer waits for and releases them.
func (wk *WorkerNode) onLeaseResp(r leaseResp) {
	rec, pending := wk.inflight[r.seq]
	delete(wk.inflight, r.seq)
	now := wk.mono()
	if r.err != nil {
		if pending {
			wk.apply(wk.tr.LeasesFailed(now, r.seq))
		}
		return
	}
	effs, err := wk.tr.LeasesGranted(now, r.seq, rec.sentAt, r.waited, r.grants)
	wk.must(err)
	wk.apply(effs)
}

// sendHeartbeat sends a heartbeat to the next broker.
func (wk *WorkerNode) sendHeartbeat(e lease.SendHeartbeat) {
	gen, from, to := wk.gen, wk.id, wk.nextBroker()
	wk.inflight[e.Seq] = rpc{broker: to, sentAt: wk.mono()}
	req := heartbeatReq{
		from: from, seq: e.Seq, refs: e.Refs, ttl: e.TTL,
		reply: func(r heartbeatResp) { wk.deliver(gen, "heartbeat-resp", func() { wk.onHeartbeatResp(r) }) },
	}
	wk.net.Send(from, to, true, "heartbeat-req", func() { wk.net.broker(to).handleHeartbeat(req) })
	wk.after(gen, rpcTimeout, "heartbeat-timeout", func() {
		if _, pending := wk.inflight[e.Seq]; pending {
			delete(wk.inflight, e.Seq)
			wk.apply(wk.tr.HeartbeatFailed(wk.mono(), e.Seq))
		}
	})
}

// onHeartbeatResp handles the answer to a heartbeat. The network can deliver
// a request twice, and the broker answers each, so only the first answer
// counts.
func (wk *WorkerNode) onHeartbeatResp(r heartbeatResp) {
	rec, pending := wk.inflight[r.seq]
	if !pending {
		return
	}
	delete(wk.inflight, r.seq)
	if r.err != nil {
		wk.apply(wk.tr.HeartbeatFailed(wk.mono(), r.seq))
		return
	}
	effs, err := wk.tr.HeartbeatDone(wk.mono(), r.seq, rec.sentAt, r.statuses)
	wk.must(err)
	wk.apply(effs)
}

// sendAcks sends a batch of acks to the next broker.
func (wk *WorkerNode) sendAcks(e lease.SendAcks) {
	gen, from, to := wk.gen, wk.id, wk.nextBroker()
	wk.inflight[e.Seq] = rpc{broker: to, sentAt: wk.mono()}
	req := ackReq{
		from: from, seq: e.Seq, acks: e.Acks,
		reply: func(r ackResp) { wk.deliver(gen, "ack-resp", func() { wk.onAckResp(r) }) },
	}
	wk.net.Send(from, to, true, "ack-req", func() { wk.net.broker(to).handleAcks(req) })
	wk.after(gen, rpcTimeout, "ack-timeout", func() {
		if _, pending := wk.inflight[e.Seq]; pending {
			delete(wk.inflight, e.Seq)
			wk.apply(wk.tr.AcksFailed(wk.mono(), e.Seq))
		}
	})
}

// onAckResp handles the answer to an ack batch; only the first counts.
func (wk *WorkerNode) onAckResp(r ackResp) {
	if _, pending := wk.inflight[r.seq]; !pending {
		return
	}
	delete(wk.inflight, r.seq)
	if r.err != nil {
		wk.apply(wk.tr.AcksFailed(wk.mono(), r.seq))
		return
	}
	effs, err := wk.tr.AcksDone(wk.mono(), r.seq, r.statuses)
	wk.must(err)
	wk.apply(effs)
}

// sendRelease sends a release to the next broker. Release is best effort, so
// a timeout ends it like an answer does.
func (wk *WorkerNode) sendRelease(e lease.SendRelease) {
	gen, from, to := wk.gen, wk.id, wk.nextBroker()
	wk.inflight[e.Seq] = rpc{broker: to, sentAt: wk.mono()}
	req := releaseReq{
		from: from, seq: e.Seq, refs: e.Refs,
		reply: func(r releaseResp) { wk.deliver(gen, "release-resp", func() { wk.onReleaseEnd(r.seq) }) },
	}
	wk.net.Send(from, to, true, "release-req", func() { wk.net.broker(to).handleRelease(req) })
	wk.after(gen, rpcTimeout, "release-timeout", func() { wk.onReleaseEnd(e.Seq) })
}

// onReleaseEnd tells the tracker that a release ended, once.
func (wk *WorkerNode) onReleaseEnd(seq uint64) {
	if _, pending := wk.inflight[seq]; !pending {
		return
	}
	delete(wk.inflight, seq)
	wk.apply(wk.tr.ReleaseDone(wk.mono(), seq))
}

// startHandler starts a simulated handler for a grant: it draws how long the
// handler runs and whether it will stop when cancelled.
func (wk *WorkerNode) startHandler(g lease.Grant) {
	m := wk.cfg.Handler
	var d time.Duration
	if wk.rng.Chance(m.LongP) {
		d = wk.rng.Between(m.Long[0], m.Long[1])
	} else {
		d = wk.rng.Between(m.Duration[0], m.Duration[1])
	}
	h := &handlerRun{grant: g, cooperative: wk.rng.Chance(wk.cfg.CooperativeP)}
	wk.runs[g.Ref] = h
	wk.after(wk.gen, d, "handler-done", func() {
		if !h.done {
			wk.finish(h, wk.endOutcome(h))
		}
	})
}

// endOutcome draws what a handler returns when it reaches the end of its
// time. A handler that was cancelled and noticed returns a retry. A success
// first writes to the sink, and a write that the sink rejects as stale turns
// into a retry.
func (wk *WorkerNode) endOutcome(h *handlerRun) lease.Outcome {
	if h.cancelled {
		return lease.Outcome{Kind: lease.OutcomeRetry, Error: "cancelled"}
	}
	m := wk.cfg.Handler
	u := wk.rng.Float64()
	switch {
	case u < m.PermanentP:
		return lease.Outcome{Kind: lease.OutcomePermanent, Error: "handler failed for good"}
	case u < m.PermanentP+m.RetryP:
		return lease.Outcome{Kind: lease.OutcomeRetry, Error: "handler failed"}
	}
	ref := h.grant.Ref
	if !wk.sink.Write(ref.ID, ref.Token) {
		return lease.Outcome{Kind: lease.OutcomeRetry, Error: "fenced by sink"}
	}
	return lease.Outcome{Kind: lease.OutcomeSuccess}
}

// cancelHandler tells a handler to stop. A cooperative one returns a retry
// within cancelDelayMax. An uncooperative one carries on.
func (wk *WorkerNode) cancelHandler(ref lease.Ref) {
	h := wk.runs[ref]
	if h == nil || h.done || h.cancelled || !h.cooperative {
		return
	}
	h.cancelled = true
	wk.after(wk.gen, wk.rng.Between(0, cancelDelayMax), "handler-cancelled", func() {
		if !h.done {
			wk.finish(h, wk.endOutcome(h))
		}
	})
}

// finish ends a handler with an outcome and reports it to the tracker.
func (wk *WorkerNode) finish(h *handlerRun, out lease.Outcome) {
	h.done = true
	delete(wk.runs, h.grant.Ref)
	effs, err := wk.tr.HandlerDone(wk.mono(), h.grant.Ref, out)
	wk.must(err)
	wk.apply(effs)
}
