package jobqueue

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrNotFound is wrapped by the error of a call that names a job the
	// broker does not have: it never existed, was pruned, or belongs to
	// another tenant.
	ErrNotFound = errors.New("jobqueue: job not found")

	// ErrInvalid is wrapped by the error of a call whose arguments were
	// refused, whether by this package or by the broker. Retrying the same
	// call cannot succeed.
	ErrInvalid = errors.New("jobqueue: invalid argument")
)

// IsRetryable reports whether err came from a call that may succeed if it is
// made again: the broker was unavailable, was out of capacity, or aborted the
// call, or a single attempt ran out of time. It is false for nil, for an
// error that is not a gRPC status, and for an error that wraps a context's
// own: a caller whose deadline has passed gains nothing from another try.
//
// Producer retries exactly these errors by itself. The function is for code
// that makes its own calls on the connection.
func IsRetryable(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	code := status.Code(err)
	return code == codes.Unavailable || code == codes.ResourceExhausted ||
		code == codes.Aborted || code == codes.DeadlineExceeded
}

// callError is the error of a failed call. It names the operation, wraps the
// error of the call itself, so that errors.As and status.FromError still find
// the gRPC status, and also wraps a sentinel of this package when one
// applies, so that errors.Is finds that.
type callError struct {
	op   string
	kind error // ErrNotFound, ErrInvalid, the caller's context error, or nil
	err  error
}

func (e *callError) Error() string { return "jobqueue: " + e.op + ": " + e.err.Error() }

func (e *callError) Unwrap() []error {
	if e.kind == nil {
		return []error{e.err}
	}
	return []error{e.kind, e.err}
}

// callFailed wraps err, the failure of the call op. ctxErr is the error of
// the caller's context if it ended, which says why no further attempt was
// made.
func callFailed(op string, err, ctxErr error) error {
	e := &callError{op: op, err: err, kind: ctxErr}
	if code := status.Code(err); code == codes.NotFound {
		e.kind = ErrNotFound
	} else if code == codes.InvalidArgument {
		e.kind = ErrInvalid
	}
	return e
}
