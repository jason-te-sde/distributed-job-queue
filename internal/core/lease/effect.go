package lease

import (
	"strconv"
	"time"
)

// Mono is a monotonic instant: the time elapsed since the shell started. The
// shell defines it, and the tracker only compares and adds to it.
type Mono = time.Duration

// Ref names one lease: a job and the fencing token under which this worker
// holds it. A redelivery of the same job carries a new token.
type Ref struct {
	// ID is the job id.
	ID int64
	// Queue is the job's queue.
	Queue string
	// Token is the lease's fencing token.
	Token int64
}

// Grant is one job that the broker leased to this worker.
type Grant struct {
	// Ref is the lease.
	Ref Ref
	// Attempt is the attempt number that this lease starts.
	Attempt int32
	// Remaining is the lease time that the broker reported was left when it
	// granted the lease.
	Remaining time.Duration
}

// HeartbeatStatus is the broker's answer about one lease in a heartbeat.
type HeartbeatStatus struct {
	// Held is whether the broker still considers this worker the holder.
	Held bool
	// Remaining is the lease time left at the broker when it answered. It
	// is meaningful only when Held is true.
	Remaining time.Duration
}

// OutcomeKind is how a handler ended.
type OutcomeKind uint8

// The kinds of outcome. The zero OutcomeKind is deliberately none of them.
const (
	// OutcomeSuccess means the handler finished the job.
	OutcomeSuccess OutcomeKind = iota + 1
	// OutcomeRetry means the handler failed and the job should run again.
	OutcomeRetry
	// OutcomePermanent means the handler failed and the job must not run
	// again.
	OutcomePermanent
)

// String returns the lower-case name of the kind, or "outcome_kind(N)" for a
// value that is not a kind.
func (k OutcomeKind) String() string {
	switch k {
	case OutcomeSuccess:
		return "success"
	case OutcomeRetry:
		return "retry"
	case OutcomePermanent:
		return "permanent"
	}
	return "outcome_kind(" + strconv.Itoa(int(k)) + ")"
}

// Outcome is what a handler returned. The tracker carries it to the ack
// without looking inside.
type Outcome struct {
	// Kind says how the handler ended.
	Kind OutcomeKind
	// Result is the handler's result, for a success.
	Result []byte
	// Error is the handler's error message, for a failure.
	Error string
	// RetryAfter is the delay the handler asked for before the next
	// attempt, for a retry.
	RetryAfter time.Duration
}

// Ack reports one finished handler to the broker.
type Ack struct {
	// Ref is the lease.
	Ref Ref
	// Attempt is the attempt that the lease started.
	Attempt int32
	// Outcome is what the handler returned.
	Outcome Outcome
}

// AckStatus is the broker's answer about one ack.
type AckStatus uint8

// The answers to an ack. The zero AckStatus is deliberately none of them.
const (
	// AckAccepted means the ack took effect.
	AckAccepted AckStatus = iota + 1
	// AckDuplicate means the ack had already taken effect.
	AckDuplicate
	// AckFenced means the lease was no longer this worker's.
	AckFenced
)

// String returns the lower-case name of the status, or "ack_status(N)" for a
// value that is not a status.
func (s AckStatus) String() string {
	switch s {
	case AckAccepted:
		return "accepted"
	case AckDuplicate:
		return "duplicate"
	case AckFenced:
		return "fenced"
	}
	return "ack_status(" + strconv.Itoa(int(s)) + ")"
}

// CancelReason is why a handler is told to stop.
type CancelReason uint8

// The reasons for a cancellation. The zero CancelReason is deliberately none
// of them.
const (
	// CancelLost means the broker said the lease is no longer held.
	CancelLost CancelReason = iota + 1
	// CancelDeadline means the local deadline of the lease passed.
	CancelDeadline
	// CancelDrain means the drain timeout passed with the handler running.
	CancelDrain
)

// String returns the lower-case name of the reason, or "cancel_reason(N)" for
// a value that is not a reason.
func (r CancelReason) String() string {
	switch r {
	case CancelLost:
		return "lost"
	case CancelDeadline:
		return "deadline"
	case CancelDrain:
		return "drain"
	}
	return "cancel_reason(" + strconv.Itoa(int(r)) + ")"
}

// Held is a lease that the worker may still act on.
type Held struct {
	// Ref is the lease.
	Ref Ref
	// Deadline is the instant at which the tracker gives up on the lease
	// unless a heartbeat extends it.
	Deadline Mono
}

// InvariantError reports a call that the shell must never make, such as the
// end of a handler that was never started. It means a bug in the shell or
// the broker, not bad input from a user.
type InvariantError struct {
	// Ref is the lease the call was about, or the zero Ref when the call
	// names none.
	Ref Ref
	// Msg says what was wrong.
	Msg string
}

// Error formats the error as "lease tracker invariant broken: job <ID> token
// <Token>: <Msg>".
func (e *InvariantError) Error() string {
	return "lease tracker invariant broken: job " + strconv.FormatInt(e.Ref.ID, 10) +
		" token " + strconv.FormatInt(e.Ref.Token, 10) + ": " + e.Msg
}

// Effect is something the shell must do. Every method of Tracker returns a
// list of them, in the order they must be carried out. An Arm, when present,
// is the last effect. A list with Stopped has no Arm, because the tracker is
// done.
//
// The tracker has no timeout of its own for the calls it sends. The shell
// must eventually answer every RequestLeases, SendHeartbeat, SendAcks and
// SendRelease with the matching Done or Failed method, which means giving
// each RPC a timeout. A call that is never answered stalls the tracker: a
// heartbeat in flight suppresses every later heartbeat, an ack batch in
// flight holds back every later ack, a Lease request in flight holds back
// every later request, and a release in flight keeps a drain from ending.
//
//sumtype:decl
type Effect interface{ isEffect() }

// RequestLeases asks the broker for up to Max jobs, waiting at most Wait for
// work, each leased for TTL. The shell reports the answer with LeasesGranted
// or LeasesFailed under the same Seq.
type RequestLeases struct {
	// Seq identifies the request.
	Seq uint64
	// Max is the most jobs to take: the number of free slots, but never more
	// than job.MaxBatch.
	Max int
	// Wait is how long the broker may hold the request while it has no work.
	Wait time.Duration
	// TTL is the lease length to ask for.
	TTL time.Duration
}

// AbandonLeaseRequest tells the shell to cancel the Lease request Seq. An
// answer that still arrives is handled as a late one.
type AbandonLeaseRequest struct {
	// Seq identifies the request to cancel.
	Seq uint64
}

// StartHandler tells the shell to run the handler of a granted job.
type StartHandler struct {
	// Grant is the job and its lease.
	Grant Grant
}

// CancelHandler tells the shell to cancel a running handler. The handler may
// keep running for a while, and the shell reports its end with HandlerDone.
type CancelHandler struct {
	// Ref is the lease of the handler.
	Ref Ref
	// Reason says why.
	Reason CancelReason
}

// SendHeartbeat extends the leases Refs to TTL. The shell reports the answer
// with HeartbeatDone or HeartbeatFailed under the same Seq.
type SendHeartbeat struct {
	// Seq identifies the call.
	Seq uint64
	// Refs are the leases, at most Config.AckBatch.
	Refs []Ref
	// TTL is the lease length to ask for.
	TTL time.Duration
	// Timeout is the longest the shell may wait for the answer before it
	// reports the call as failed: a heartbeat period. No other heartbeat goes
	// out while this one is in flight, so a call that hangs for longer costs
	// the leases more than the heartbeat it stands for, and at the shortest
	// TTLs the leases run out before it ends. The shell may use less.
	Timeout time.Duration
}

// SendAcks reports finished handlers. The shell reports the answer with
// AcksDone or AcksFailed under the same Seq.
type SendAcks struct {
	// Seq identifies the call.
	Seq uint64
	// Acks are the reports, at most Config.AckBatch.
	Acks []Ack
}

// SendRelease gives leases back unrun or unfinished. It is best effort: the
// shell reports its end with ReleaseDone, whether it worked or not.
type SendRelease struct {
	// Seq identifies the call.
	Seq uint64
	// Refs are the leases, at most Config.AckBatch.
	Refs []Ref
}

// Discard is a grant that will not run: it arrived late, while draining, or
// with its local deadline already passed. The shell drops the job's payload.
// The same call's SendRelease gives the lease back.
type Discard struct {
	// Ref is the lease.
	Ref Ref
}

// Arm sets the shell's one timer to fire at At, replacing any earlier Arm.
// When it fires, the shell calls Tick. An At that is not after the current
// instant means fire at once.
type Arm struct {
	// At is the instant to fire.
	At Mono
}

// Stopped says the drain is complete: no handler is waited for, and nothing
// is pending or in flight. The tracker returns no effects afterwards, with one
// exception: an answer to a Lease request that arrives late still has its
// grants discarded and released, so that no lease is left to run out. That
// answer's effects are Discard and SendRelease, and it has no Arm.
type Stopped struct{}

func (RequestLeases) isEffect()       {}
func (AbandonLeaseRequest) isEffect() {}
func (StartHandler) isEffect()        {}
func (CancelHandler) isEffect()       {}
func (SendHeartbeat) isEffect()       {}
func (SendAcks) isEffect()            {}
func (SendRelease) isEffect()         {}
func (Discard) isEffect()             {}
func (Arm) isEffect()                 {}
func (Stopped) isEffect()             {}
