package jobqueue

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/core/lease"
)

// Job is a job leased to a worker, as its Handler sees it. The handler may
// read the exported fields freely and must not change them.
type Job struct {
	// ID is the job's id.
	ID int64
	// Queue and Tenant say where the job belongs.
	Queue, Tenant string
	// Payload is the job's input, as the producer enqueued it.
	Payload []byte
	// Attempt is the number of this attempt, counting from 1, and
	// MaxAttempts is how many the job has. A failure on the last attempt
	// makes the job dead.
	Attempt, MaxAttempts int
	// EnqueuedAt is when the job was enqueued.
	EnqueuedAt time.Time

	token  int64
	result []byte
}

// Token returns the fencing token of the lease that this attempt runs under.
// It grows with every lease of the job, so a downstream system that keeps the
// highest token it has seen for a job, and refuses writes under a lower one,
// is protected from a handler that kept running after its lease was lost.
func (j *Job) Token() int64 { return j.token }

// SetResult sets what the broker stores with the job if the handler returns
// nil, which Producer.Get returns as JobInfo.Result. It may be called any
// number of times, and the last call counts; call it before the handler
// returns, from the handler's goroutine. The limit is 64 KiB: a longer result
// makes the job dead with the error "result exceeds 65536 bytes", because no
// retry could make it shorter.
func (j *Job) SetResult(b []byte) { j.result = bytes.Clone(b) }

// Handler runs one job. The context ends when the worker can no longer vouch
// for the lease: the broker said the lease is lost, the lease could not be
// extended in time, or a shutdown outlasted its drain timeout. A handler
// should return promptly then, and context.Cause says which of these it was.
// One that does not is not harmful, only wasteful: the broker fences its
// report.
//
// The result decides what happens to the job. nil means it succeeded. An
// error means it failed and will be tried again after a backoff, unless it is
// the last attempt; RetryAfter and Permanent change that. A panic counts as an
// error that carries the panic value and the stack.
type Handler func(ctx context.Context, job *Job) error

// retryAfterError is an error that asks for a retry no sooner than a delay.
type retryAfterError struct {
	err   error
	after time.Duration
}

func (e *retryAfterError) Error() string { return e.err.Error() }
func (e *retryAfterError) Unwrap() error { return e.err }

// permanentError is an error that says the job must not run again.
type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// RetryAfter makes err, as the result of a Handler, ask for the job to be
// tried again no sooner than d from now, in place of the usual backoff. A d
// below 0 counts as 0, which leaves the backoff to the broker, and one above
// 365 days as 365 days. The error still reads, and unwraps, as err. Like
// errors.Join and the wrappers of fmt.Errorf it passes nil through: wrapping
// no error leaves no error.
func RetryAfter(err error, d time.Duration) error {
	if err == nil {
		return nil
	}
	return &retryAfterError{err: err, after: d}
}

// Permanent makes err, as the result of a Handler, say that the job must not
// be tried again whatever attempts are left: it is dead at once. If an error
// carries both Permanent and RetryAfter, Permanent decides. The error still
// reads, and unwraps, as err. Permanent(nil) is nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// outcomeOf turns what a handler returned for the job into the outcome the
// tracker reports. ctx is the handler's context: if it ended, the cause is
// added to the message of a failure, so that the stored error says why a
// handler gave up.
func outcomeOf(ctx context.Context, j *Job, err error) lease.Outcome {
	if err == nil {
		if len(j.result) > job.MaxResultBytes {
			return failure(lease.OutcomePermanent, fmt.Sprintf("result exceeds %d bytes", job.MaxResultBytes), 0)
		}
		return lease.Outcome{Kind: lease.OutcomeSuccess, Result: j.result}
	}
	msg := err.Error()
	if ctx.Err() != nil {
		msg += " (handler cancelled: " + context.Cause(ctx).Error() + ")"
	}
	var permanent *permanentError
	if errors.As(err, &permanent) {
		return failure(lease.OutcomePermanent, msg, 0)
	}
	var retry *retryAfterError
	if errors.As(err, &retry) {
		return failure(lease.OutcomeRetry, msg, retry.after)
	}
	return failure(lease.OutcomeRetry, msg, 0)
}

// panicOutcome is the outcome of a handler that panicked with r: a failure
// that will be retried, whose message holds the value and the stack. It must
// be called from the deferred function that recovered r, so that the stack is
// the panicking one.
func panicOutcome(r any) lease.Outcome {
	return failure(lease.OutcomeRetry, fmt.Sprintf("panic: %v\n%s", r, debug.Stack()), 0)
}

// failure builds the outcome of a job that did not succeed. The message is
// made fit for the wire and the store, which wants valid UTF-8 without NUL in
// at most 4096 bytes; a handler's error, and a panic's stack, can be anything,
// and one unfit message would fail the whole ack batch that carries it. The
// delay is cut to what the broker accepts: from 0 to 365 days in whole
// microseconds.
func failure(kind lease.OutcomeKind, msg string, retryAfter time.Duration) lease.Outcome {
	retryAfter = min(max(retryAfter, 0), job.MaxDelay)
	return lease.Outcome{
		Kind:       kind,
		Error:      job.TruncateError(msg),
		RetryAfter: retryAfter - retryAfter%time.Microsecond,
	}
}
