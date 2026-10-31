package dispatch

import (
	"strconv"
	"time"
)

// Mono is a monotonic instant chosen by the shell: the time elapsed since the
// shell started. The core never reads a clock; every method takes the current
// instant.
type Mono = time.Duration

// WaiterID identifies one long-polling request. The shell hands out ids and
// never reuses one while the waiter is live.
type WaiterID uint64

// Effect is something the shell must do after a call into the core. Effects
// come back in the order to carry them out, and an Arm, when present, is
// always last.
//
//sumtype:decl
type Effect interface {
	isEffect()
}

// Fetch tells the shell to lease jobs from the store for a waiter. The shell
// keeps the jobs it gets, and reports how many, and what the fetch asked for,
// through FetchDone.
type Fetch struct {
	// Waiter is the waiter the fetch serves.
	Waiter WaiterID
	// Queue is the queue to fetch from.
	Queue string
	// Max is the most jobs the waiter asked for.
	Max int
	// Waited is how long the waiter has been waiting at the moment of the
	// fetch: the instant of the call minus its arrival.
	Waited time.Duration
}

// Deliver tells the shell to answer a waiter with the jobs its fetch found.
type Deliver struct {
	// Waiter is the waiter to answer.
	Waiter WaiterID
}

// Respond tells the shell to answer a waiter with no jobs.
type Respond struct {
	// Waiter is the waiter to answer.
	Waiter WaiterID
	// Reason says why there are no jobs.
	Reason Reason
}

// Release tells the shell that a waiter is gone and that the jobs fetched for
// it must be given back to the store.
type Release struct {
	// Waiter is the waiter that went away.
	Waiter WaiterID
}

// Arm tells the shell when to call Tick next. Only the latest Arm counts:
// the shell keeps one timer and replaces it. An instant that has already
// passed means Tick is due at once.
type Arm struct {
	// At is the instant of the next Tick.
	At Mono
}

func (Fetch) isEffect()   {}
func (Deliver) isEffect() {}
func (Respond) isEffect() {}
func (Release) isEffect() {}
func (Arm) isEffect()     {}

// Reason says why a waiter is answered with no jobs.
type Reason uint8

// The reasons for an empty answer. The zero Reason is deliberately not one of
// them, so that an uninitialized Respond is never mistaken for a real one.
const (
	// ReasonTimeout means the waiter's wait ran out with no jobs.
	ReasonTimeout Reason = iota + 1
	// ReasonDraining means the broker is shutting down and ended the wait.
	ReasonDraining
	// ReasonRejected means the broker is shutting down and refused the
	// request on arrival.
	ReasonRejected
	// ReasonUnavailable means the fetch failed.
	ReasonUnavailable
)

// String returns the lower-case name of the reason, or "reason(N)" for a
// value that is not a reason.
func (r Reason) String() string {
	switch r {
	case ReasonTimeout:
		return "timeout"
	case ReasonDraining:
		return "draining"
	case ReasonRejected:
		return "rejected"
	case ReasonUnavailable:
		return "unavailable"
	}
	return "reason(" + strconv.Itoa(int(r)) + ")"
}
