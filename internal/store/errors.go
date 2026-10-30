package store

import (
	"errors"
	"strconv"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
)

// ErrNotFound is returned when a job does not exist, or belongs to another
// tenant than the caller named.
var ErrNotFound = errors.New("job not found")

// ErrUnavailable is wrapped around failures of the connection to the store,
// which a caller may retry. The caller's own mistakes are job.ErrInvalid and
// are never retried.
var ErrUnavailable = errors.New("store unavailable")

// InternalError reports that the store found a job in a state that its own
// rules rule out. It is a bug in the store, not a mistake of the caller, and
// carries the job's id and state so that whoever is paged can find the row.
type InternalError struct {
	// ID is the job's id.
	ID int64
	// State is the state the job was found in.
	State job.State
	// Msg says which rule was broken.
	Msg string
}

// Error formats the error as "internal invariant broken: job <ID> in state
// <State>: <Msg>".
func (e *InternalError) Error() string {
	return "internal invariant broken: job " + strconv.FormatInt(e.ID, 10) + " in state " + e.State.String() + ": " + e.Msg
}
