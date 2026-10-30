package job

import (
	"bytes"
	"slices"
	"strconv"
	"time"
)

// State is where a job is in its life.
type State uint8

// The states of a job. The zero State is deliberately not one of them, so
// that an uninitialized Job is never mistaken for a ready one.
const (
	// StateReady means the job waits for a worker, now or at RunAt.
	StateReady State = iota + 1
	// StateLeased means a worker holds the job until LeaseExpiresAt.
	StateLeased
	// StateSucceeded means a worker completed the job. Terminal.
	StateSucceeded
	// StateDead means the job failed for good: it ran out of attempts or a
	// worker failed it permanently. Terminal, except that Requeue revives it.
	StateDead
	// StateCancelled means the job was cancelled before it finished. Terminal.
	StateCancelled
)

// String returns the lower-case name of the state, or "state(N)" for a
// value that is not a state.
func (s State) String() string {
	switch s {
	case StateReady:
		return "ready"
	case StateLeased:
		return "leased"
	case StateSucceeded:
		return "succeeded"
	case StateDead:
		return "dead"
	case StateCancelled:
		return "cancelled"
	}
	return "state(" + strconv.Itoa(int(s)) + ")"
}

// Terminal reports whether the job has finished: succeeded, dead or
// cancelled.
func (s State) Terminal() bool {
	switch s {
	case StateSucceeded, StateDead, StateCancelled:
		return true
	case StateReady, StateLeased:
		return false
	}
	return false
}

// ParseState is the inverse of State.String for the five state names. Any
// other text is an error that wraps ErrInvalid.
func ParseState(s string) (State, error) {
	for _, st := range [...]State{StateReady, StateLeased, StateSucceeded, StateDead, StateCancelled} {
		if st.String() == s {
			return st, nil
		}
	}
	return 0, &InvalidError{Field: "state", Reason: "unknown state " + strconv.Quote(s)}
}

// Job is one row of the queue. Every time in it is UTC at microsecond
// precision (see NormalizeTime). The transitions share the byte slices
// between the job they receive and the job they return, and never write into
// them. A Job is therefore safe to pass by value only as long as nobody
// writes into its slices: a store that keeps a Job beyond a call, or hands
// one out, must Clone it at that boundary.
type Job struct {
	// ID is the job's identifier, unique within a store.
	ID int64
	// Queue and Tenant name where the job belongs; see ValidateName.
	Queue, Tenant string
	// State is where the job is in its life.
	State State
	// Payload is the opaque work description, at most MaxPayloadBytes.
	Payload []byte
	// RunAt is the earliest time the job may be leased.
	RunAt time.Time
	// Attempts counts the leases granted and not refunded.
	Attempts int32
	// MaxAttempts is the lease budget; the attempt that uses it up and
	// fails sends the job to StateDead.
	MaxAttempts int32
	// Epoch is the lease epoch, the fencing token of the current or last
	// lease. It only ever grows.
	Epoch int64
	// LeaseExpiresAt is the end of the lease; the zero time unless State
	// is StateLeased.
	LeaseExpiresAt time.Time
	// LeasedBy is the worker holding the lease; empty unless State is
	// StateLeased.
	LeasedBy string
	// IdempotencyKey is the enqueue key that identifies this job within its
	// tenant and queue; empty means none.
	IdempotencyKey string
	// LastError is the message of the most recent failure, truncated by
	// TruncateError.
	LastError string
	// Result is what the worker returned on completion, at most
	// MaxResultBytes.
	Result []byte
	// CreatedAt is when the job was enqueued.
	CreatedAt time.Time
	// FinishedAt is when the job reached a terminal state; the zero time
	// unless State.Terminal().
	FinishedAt time.Time
}

// Clone returns a copy of j that shares no memory with it: Payload and Result
// are copied byte for byte, nil staying nil and empty staying empty, and every
// other field is copied by value.
func (j Job) Clone() Job {
	j.Payload = slices.Clone(j.Payload)
	j.Result = slices.Clone(j.Result)
	return j
}

// Equal reports whether a and b hold the same values. It compares the byte
// slices by content, so that nil and empty are the same, and the times by
// instant, so that the zone and monotonic reading do not matter.
func Equal(a, b Job) bool {
	return a.ID == b.ID &&
		a.Queue == b.Queue &&
		a.Tenant == b.Tenant &&
		a.State == b.State &&
		bytes.Equal(a.Payload, b.Payload) &&
		a.RunAt.Equal(b.RunAt) &&
		a.Attempts == b.Attempts &&
		a.MaxAttempts == b.MaxAttempts &&
		a.Epoch == b.Epoch &&
		a.LeaseExpiresAt.Equal(b.LeaseExpiresAt) &&
		a.LeasedBy == b.LeasedBy &&
		a.IdempotencyKey == b.IdempotencyKey &&
		a.LastError == b.LastError &&
		bytes.Equal(a.Result, b.Result) &&
		a.CreatedAt.Equal(b.CreatedAt) &&
		a.FinishedAt.Equal(b.FinishedAt)
}

// Spec is an enqueue request. ValidateSpec says whether it is acceptable.
type Spec struct {
	// Queue and Tenant name where the job goes; see ValidateName.
	Queue, Tenant string
	// Payload is the opaque work description, at most MaxPayloadBytes.
	Payload []byte
	// Delay postpones the job by this long, within [0, MaxDelay].
	Delay time.Duration
	// RunAt schedules the job for an instant. The zero time means not
	// scheduled. It must not be combined with a positive Delay.
	RunAt time.Time
	// MaxAttempts is the lease budget; 0 means DefaultMaxAttempts.
	MaxAttempts int32
	// IdempotencyKey makes the enqueue repeatable: a second request with
	// the same key, tenant and queue yields the first job. Empty means none.
	IdempotencyKey string
}

// Outcome says what an operation did to a job. Only Applied changes it.
type Outcome uint8

// The outcomes of an operation on a job.
const (
	// Applied means the operation took effect.
	Applied Outcome = iota + 1
	// Duplicate means the operation had already taken effect, and
	// repeating it changed nothing.
	Duplicate
	// Fenced means the caller's token no longer matches the job's lease:
	// the lease was lost, or the job is not leased.
	Fenced
	// Expired means the caller's lease ran out before the operation.
	Expired
	// NotFound means no such job exists.
	NotFound
	// WrongState means the job's state does not allow the operation.
	WrongState
)

// String returns the snake-case name of the outcome, or "outcome(N)" for a
// value that is not an outcome.
func (o Outcome) String() string {
	switch o {
	case Applied:
		return "applied"
	case Duplicate:
		return "duplicate"
	case Fenced:
		return "fenced"
	case Expired:
		return "expired"
	case NotFound:
		return "not_found"
	case WrongState:
		return "wrong_state"
	}
	return "outcome(" + strconv.Itoa(int(o)) + ")"
}

// NormalizeTime returns t as UTC at microsecond precision, the form in
// which every time is held in a Job and in the database. It also drops the
// monotonic clock reading. The zero time stays zero.
func NormalizeTime(t time.Time) time.Time {
	return t.UTC().Truncate(time.Microsecond)
}
