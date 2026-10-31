package jobqueue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/lease"
	jobqueuev1 "github.com/jason-te-sde/distributed-job-queue/internal/gen/jobqueue/v1"
)

const (
	// callTimeout bounds a heartbeat, an ack and a release. The tracker has
	// no timeout of its own and waits for every call to be answered, so
	// every call needs one.
	callTimeout = 5 * time.Second
	// leaseSlack is added to the long poll to bound a Lease call: the broker
	// answers within the poll, and the slack covers the network and the
	// store.
	leaseSlack = 5 * time.Second
)

// What a handler's context says when it ends, as context.Cause reports it.
var (
	errLeaseLost     = errors.New("lease lost: the broker no longer lists this worker as the holder")
	errLeaseDeadline = errors.New("lease deadline reached: the lease could not be extended in time")
	errDrainTimeout  = errors.New("drain timeout reached: the worker is shutting down")
	errStopped       = errors.New("worker stopped")
)

// cancelCause is the cause that a handler cancelled for reason is told.
func cancelCause(reason lease.CancelReason) error {
	switch reason {
	case lease.CancelLost:
		return errLeaseLost
	case lease.CancelDeadline:
		return errLeaseDeadline
	case lease.CancelDrain:
		return errDrainTimeout
	}
	return fmt.Errorf("handler cancelled: %v", reason)
}

// event is the end of one call or handler that a goroutine reports to the
// loop. The loop is the only goroutine that touches the tracker, so every
// answer comes back to it through the events channel and is applied there.
type event interface {
	// apply tells the tracker what happened at now and returns the effects
	// to carry out. An error is a broken invariant and ends the worker.
	apply(l *workerLoop, now lease.Mono) ([]lease.Effect, error)
}

// leaseAnswer is the end of a Lease call.
type leaseAnswer struct {
	seq    uint64
	sentAt lease.Mono
	resp   *jobqueuev1.LeaseResponse
	err    error
}

// handlerReturned is the end of a handler.
type handlerReturned struct {
	ref lease.Ref
	out lease.Outcome
}

// heartbeatAnswer is the end of a Heartbeat call.
type heartbeatAnswer struct {
	seq    uint64
	sentAt lease.Mono
	n      int // refs sent
	resp   *jobqueuev1.HeartbeatResponse
	err    error
}

// ackAnswer is the end of an Ack call.
type ackAnswer struct {
	seq  uint64
	acks []lease.Ack
	resp *jobqueuev1.AckResponse
	err  error
}

// releaseAnswer is the end of a Release call, whatever its result.
type releaseAnswer struct {
	seq  uint64
	refs []lease.Ref
	err  error
}

// workerLoop is the shell around the lease tracker. Its run method is the
// tracker's single owner goroutine: it alone calls the tracker and reads or
// writes the fields below the line. Calls to the broker and handlers run on
// goroutines of their own and report back through events.
type workerLoop struct {
	log      *slog.Logger
	client   jobqueuev1.JobQueueServiceClient
	queue    string
	workerID string
	handler  Handler
	tracker  *lease.Tracker
	start    time.Time // the instant that lease.Mono counts from

	// values carries what the context of Run carries, without its
	// cancellation: handlers and the calls that settle a drain must outlive
	// the cancellation that starts the drain. rpc is the base of every call
	// to the broker, and ends with the loop.
	values  context.Context
	rpc     context.Context
	stopRPC context.CancelFunc

	events chan event
	// done is closed when the loop has ended, so that goroutines that report
	// to it later do not wait for ever.
	done  chan struct{}
	timer *time.Timer

	// owner is the id of the goroutine in the loop, recorded in race builds.
	owner atomic.Uint64

	// The fields below belong to the loop goroutine.

	// leaseCalls holds the cancel function of every Lease call whose answer
	// has not come back, by Seq.
	leaseCalls map[uint64]context.CancelFunc
	// pending holds the jobs of grants whose handler has not started.
	pending map[lease.Ref]*Job
	// handlers holds the cancel function of every handler that has not
	// reported its end.
	handlers map[lease.Ref]context.CancelCauseFunc
	// calls counts the Lease and Release calls in flight. After the tracker
	// has stopped the loop waits for them, because the answer to a Lease
	// call that arrives late holds jobs that must be given back.
	calls   int
	stopped bool
}

func newWorkerLoop(conn grpc.ClientConnInterface, opts WorkerOptions, handler Handler, tracker *lease.Tracker) *workerLoop {
	return &workerLoop{
		log:        opts.Logger.With(slog.String("worker", opts.WorkerID), slog.String("queue", opts.Queue)),
		client:     jobqueuev1.NewJobQueueServiceClient(conn),
		queue:      opts.Queue,
		workerID:   opts.WorkerID,
		handler:    handler,
		tracker:    tracker,
		events:     make(chan event),
		done:       make(chan struct{}),
		leaseCalls: map[uint64]context.CancelFunc{},
		pending:    map[lease.Ref]*Job{},
		handlers:   map[lease.Ref]context.CancelCauseFunc{},
	}
}

// enter records that the calling goroutine runs the loop, and in a race build
// panics if another goroutine has entered it before. The tracker is not safe
// for concurrent use and has exactly one owner, and nothing in the types stops
// a second goroutine from calling into it, so a race build checks.
func (l *workerLoop) enter() {
	if !raceEnabled {
		return
	}
	id := goroutineID()
	if !l.owner.CompareAndSwap(0, id) && l.owner.Load() != id {
		panic("lease tracker entered from two goroutines")
	}
}

// now is the current instant on the tracker's clock: the monotonic time since
// the loop was built.
func (l *workerLoop) now() lease.Mono { return time.Since(l.start) }

// run is the loop. It returns nil when the drain is complete, and otherwise an
// error that wraps ErrInternal.
func (l *workerLoop) run(ctx context.Context) error {
	l.enter()
	l.start = time.Now()
	l.values = context.WithoutCancel(ctx)
	l.rpc, l.stopRPC = context.WithCancel(l.values)
	l.timer = time.NewTimer(time.Hour) // the first Arm replaces it
	defer func() {
		l.timer.Stop()
		l.stopRPC()
		for _, cancel := range l.handlers {
			cancel(errStopped)
		}
		close(l.done)
	}()
	l.log.Info("worker started")

	draining := ctx.Done()
	effects := l.tracker.Start(l.now())
	for {
		if err := l.perform(effects); err != nil {
			return l.fail(err)
		}
		if l.stopped && l.calls == 0 {
			l.log.Info("worker stopped")
			return nil
		}

		var err error
		select {
		case ev := <-l.events:
			l.enter()
			effects, err = ev.apply(l, l.now())
		case <-l.timer.C:
			l.enter()
			effects = l.tracker.Tick(l.now())
		case <-draining:
			l.enter()
			draining = nil
			l.log.Info("worker draining")
			effects = l.tracker.Drain(l.now())
		}
		if err != nil {
			return l.fail(err)
		}
	}
}

// fail ends the loop on a broken invariant. The error is internal whatever
// raised it, since the tracker's own rules and this shell's are both ours.
func (l *workerLoop) fail(err error) error {
	l.log.Error("worker stopped on a broken invariant", slog.Any("err", err))
	return fmt.Errorf("%w: %w", ErrInternal, err)
}

// perform carries out the effects, in order.
func (l *workerLoop) perform(effects []lease.Effect) error {
	l.enter()
	for _, eff := range effects {
		switch e := eff.(type) {
		case lease.RequestLeases:
			l.requestLeases(e)
		case lease.AbandonLeaseRequest:
			if cancel := l.leaseCalls[e.Seq]; cancel != nil {
				cancel()
			}
		case lease.StartHandler:
			if err := l.startHandler(e.Grant); err != nil {
				return err
			}
		case lease.CancelHandler:
			l.log.Info("cancelling a handler",
				slog.Int64("job_id", e.Ref.ID), slog.Int64("token", e.Ref.Token), slog.String("reason", e.Reason.String()))
			if cancel := l.handlers[e.Ref]; cancel != nil {
				cancel(cancelCause(e.Reason))
			}
		case lease.SendHeartbeat:
			l.sendHeartbeat(e)
		case lease.SendAcks:
			l.sendAcks(e)
		case lease.SendRelease:
			l.sendRelease(e)
		case lease.Discard:
			delete(l.pending, e.Ref) // drops the payload
		case lease.Arm:
			l.timer.Reset(max(e.At-l.now(), 0))
		case lease.Stopped:
			l.stopped = true
		}
	}
	return nil
}

// send reports an event to the loop, unless the loop has ended.
func (l *workerLoop) send(ev event) {
	select {
	case l.events <- ev:
	case <-l.done:
	}
}

// requestLeases starts a Lease call on a goroutine. The call can be cancelled
// through leaseCalls, and gets a deadline of the long poll plus a slack.
func (l *workerLoop) requestLeases(r lease.RequestLeases) {
	ctx, cancel := context.WithCancel(l.rpc)
	l.leaseCalls[r.Seq] = cancel
	l.calls++
	req := &jobqueuev1.LeaseRequest{
		Queue: l.queue, MaxJobs: int32(r.Max), Wait: durationpb.New(r.Wait), Ttl: durationpb.New(r.TTL), WorkerId: l.workerID,
	}
	go func() {
		ctx, stop := context.WithTimeout(ctx, r.Wait+leaseSlack)
		defer stop()
		sentAt := l.now()
		resp, err := l.client.Lease(ctx, req)
		l.send(leaseAnswer{seq: r.Seq, sentAt: sentAt, resp: resp, err: err})
	}()
}

func (e leaseAnswer) apply(l *workerLoop, now lease.Mono) ([]lease.Effect, error) {
	l.calls--
	if cancel := l.leaseCalls[e.seq]; cancel != nil {
		cancel() // frees the call's context
		delete(l.leaseCalls, e.seq)
	}
	if e.err != nil {
		l.logCallFailed("lease", e.err)
		return l.tracker.LeasesFailed(now, e.seq), nil
	}

	jobs := e.resp.GetJobs()
	grants := make([]lease.Grant, len(jobs))
	for i, j := range jobs {
		ref := lease.Ref{ID: j.GetId(), Queue: j.GetQueue(), Token: j.GetToken()}
		grants[i] = lease.Grant{Ref: ref, Attempt: j.GetAttempt(), Remaining: j.GetRemaining().AsDuration()}
		l.pending[ref] = &Job{
			ID: ref.ID, Queue: ref.Queue, Tenant: j.GetTenant(), Payload: j.GetPayload(),
			Attempt: int(j.GetAttempt()), MaxAttempts: int(j.GetMaxAttempts()), EnqueuedAt: timeOf(j.GetEnqueuedAt()),
			token: ref.Token,
		}
	}
	effects, err := l.tracker.LeasesGranted(now, e.seq, e.sentAt, e.resp.GetWaited().AsDuration(), grants)
	if err != nil {
		for _, g := range grants {
			delete(l.pending, g.Ref)
		}
		return nil, err
	}
	return effects, nil
}

// startHandler runs the handler of a grant on a goroutine of its own.
func (l *workerLoop) startHandler(g lease.Grant) error {
	j := l.pending[g.Ref]
	if j == nil {
		return fmt.Errorf("the tracker started a handler for job %d token %d, which was never granted", g.Ref.ID, g.Ref.Token)
	}
	delete(l.pending, g.Ref)
	ctx, cancel := context.WithCancelCause(l.values)
	l.handlers[g.Ref] = cancel
	go l.runHandler(ctx, g.Ref, j)
	return nil
}

// runHandler runs the handler and reports how it ended. A panic is an outcome
// like any other. So is the handler's goroutine ending without a return, which
// runtime.Goexit does, for example through t.FailNow: the deferred function
// still runs, finds nothing to recover, and reports the job as failed, so that
// the slot of the handler is not lost.
func (l *workerLoop) runHandler(ctx context.Context, ref lease.Ref, j *Job) {
	out := failure(lease.OutcomeRetry, "handler ended its goroutine without returning", 0)
	defer func() {
		if r := recover(); r != nil {
			out = panicOutcome(r)
		}
		l.send(handlerReturned{ref: ref, out: out})
	}()
	out = outcomeOf(ctx, j, l.handler(ctx, j))
}

func (e handlerReturned) apply(l *workerLoop, now lease.Mono) ([]lease.Effect, error) {
	if cancel := l.handlers[e.ref]; cancel != nil {
		cancel(errStopped) // frees the handler's context; a cause already set stays
		delete(l.handlers, e.ref)
	}
	return l.tracker.HandlerDone(now, e.ref, e.out)
}

// sendHeartbeat starts a Heartbeat call on a goroutine.
func (l *workerLoop) sendHeartbeat(h lease.SendHeartbeat) {
	req := &jobqueuev1.HeartbeatRequest{Leases: leaseRefs(h.Refs), Ttl: durationpb.New(h.TTL)}
	go func() {
		ctx, cancel := context.WithTimeout(l.rpc, callTimeout)
		defer cancel()
		sentAt := l.now()
		resp, err := l.client.Heartbeat(ctx, req)
		l.send(heartbeatAnswer{seq: h.Seq, sentAt: sentAt, n: len(h.Refs), resp: resp, err: err})
	}()
}

func (e heartbeatAnswer) apply(l *workerLoop, now lease.Mono) ([]lease.Effect, error) {
	if e.err != nil {
		l.logCallFailed("heartbeat", e.err)
		return l.tracker.HeartbeatFailed(now, e.seq), nil
	}
	results := e.resp.GetResults()
	if len(results) != e.n {
		l.logCallFailed("heartbeat", fmt.Errorf("the broker answered %d results for %d leases", len(results), e.n))
		return l.tracker.HeartbeatFailed(now, e.seq), nil
	}
	statuses := make([]lease.HeartbeatStatus, len(results))
	for i, r := range results {
		// Only an explicit HELD keeps a lease. Anything else, a status that
		// this version does not know included, is treated as lost: stopping
		// a handler too early is safe, running it too long is not.
		if r.GetStatus() == jobqueuev1.LeaseStatus_LEASE_STATUS_HELD {
			statuses[i] = lease.HeartbeatStatus{Held: true, Remaining: r.GetRemaining().AsDuration()}
		}
	}
	return l.tracker.HeartbeatDone(now, e.seq, e.sentAt, statuses)
}

// sendAcks starts an Ack call on a goroutine. When it fails the tracker sends
// the same batch again, which is safe: the broker's Complete is idempotent for
// its token, and a Fail for a lease that was lost is fenced.
func (l *workerLoop) sendAcks(a lease.SendAcks) {
	items := make([]*jobqueuev1.AckItem, len(a.Acks))
	for i, ack := range a.Acks {
		items[i] = ackItem(ack)
	}
	go func() {
		ctx, cancel := context.WithTimeout(l.rpc, callTimeout)
		defer cancel()
		resp, err := l.client.Ack(ctx, &jobqueuev1.AckRequest{Items: items})
		l.send(ackAnswer{seq: a.Seq, acks: a.Acks, resp: resp, err: err})
	}()
}

func (e ackAnswer) apply(l *workerLoop, now lease.Mono) ([]lease.Effect, error) {
	if e.err != nil {
		l.logCallFailed("ack", e.err)
		return l.tracker.AcksFailed(now, e.seq), nil
	}
	statuses, err := ackStatuses(e.resp.GetResults(), len(e.acks))
	if err != nil {
		l.logCallFailed("ack", err)
		return l.tracker.AcksFailed(now, e.seq), nil
	}
	for i, s := range statuses {
		if s == lease.AckFenced {
			l.log.Info("an ack was fenced: the lease was no longer this worker's",
				slog.Int64("job_id", e.acks[i].Ref.ID), slog.Int64("token", e.acks[i].Ref.Token))
		}
	}
	return l.tracker.AcksDone(now, e.seq, statuses)
}

// sendRelease starts a Release call on a goroutine. Its answer is only
// counted: a release is best effort, and a lease that is not released runs out.
func (l *workerLoop) sendRelease(r lease.SendRelease) {
	l.calls++
	req := &jobqueuev1.ReleaseRequest{Leases: leaseRefs(r.Refs)}
	go func() {
		ctx, cancel := context.WithTimeout(l.rpc, callTimeout)
		defer cancel()
		_, err := l.client.Release(ctx, req)
		l.send(releaseAnswer{seq: r.Seq, refs: r.Refs, err: err})
	}()
}

func (e releaseAnswer) apply(l *workerLoop, now lease.Mono) ([]lease.Effect, error) {
	l.calls--
	if e.err != nil {
		l.log.Warn("a release failed, the leases will run out instead",
			slog.Int("leases", len(e.refs)), slog.Any("err", e.err))
	}
	return l.tracker.ReleaseDone(now, e.seq), nil
}

// logCallFailed logs the failure of a call of kind op. A Lease call that the
// loop cancelled itself, to drain, is not a failure worth a warning.
func (l *workerLoop) logCallFailed(op string, err error) {
	level := slog.LevelWarn
	if status.Code(err) == codes.Canceled {
		level = slog.LevelDebug
	}
	l.log.Log(l.rpc, level, "a call to the broker failed", slog.String("op", op), slog.Any("err", err))
}

// leaseRefs converts the tracker's lease references to the wire's.
func leaseRefs(refs []lease.Ref) []*jobqueuev1.LeaseRef {
	out := make([]*jobqueuev1.LeaseRef, len(refs))
	for i, r := range refs {
		out[i] = &jobqueuev1.LeaseRef{Id: r.ID, Queue: r.Queue, Token: r.Token}
	}
	return out
}

// ackItem converts one report to the wire's form.
func ackItem(a lease.Ack) *jobqueuev1.AckItem {
	item := &jobqueuev1.AckItem{Lease: &jobqueuev1.LeaseRef{Id: a.Ref.ID, Queue: a.Ref.Queue, Token: a.Ref.Token}}
	if a.Outcome.Kind == lease.OutcomeSuccess {
		item.Outcome = &jobqueuev1.AckItem_Success{Success: &jobqueuev1.Success{Result: a.Outcome.Result}}
		return item
	}
	f := &jobqueuev1.Failure{
		Error: a.Outcome.Error, Permanent: a.Outcome.Kind == lease.OutcomePermanent, Attempt: a.Attempt,
	}
	if a.Outcome.RetryAfter > 0 {
		f.RetryAfter = durationpb.New(a.Outcome.RetryAfter)
	}
	item.Outcome = &jobqueuev1.AckItem_Failure{Failure: f}
	return item
}

// ackStatuses converts the broker's answer to an Ack call of n items. An
// answer of the wrong length, or with a status this version does not know, is
// an error: the caller treats the call as failed and sends the batch again.
func ackStatuses(results []jobqueuev1.AckStatus, n int) ([]lease.AckStatus, error) {
	if len(results) != n {
		return nil, fmt.Errorf("the broker answered %d results for %d acks", len(results), n)
	}
	out := make([]lease.AckStatus, n)
	for i, r := range results {
		switch r {
		case jobqueuev1.AckStatus_ACK_STATUS_ACCEPTED:
			out[i] = lease.AckAccepted
		case jobqueuev1.AckStatus_ACK_STATUS_DUPLICATE:
			out[i] = lease.AckDuplicate
		case jobqueuev1.AckStatus_ACK_STATUS_FENCED:
			out[i] = lease.AckFenced
		case jobqueuev1.AckStatus_ACK_STATUS_UNSPECIFIED:
			return nil, fmt.Errorf("the broker answered no status for ack %d", i)
		default:
			return nil, fmt.Errorf("the broker answered the unknown status %d for ack %d", int32(r), i)
		}
	}
	return out, nil
}
