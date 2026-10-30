package broker

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

// toStatus maps an error of the core, the store or the standard library to
// the gRPC status that a client sees, in this order of precedence:
//
//   - an error that already is a gRPC status stays as it is, so that errors
//     of the dispatcher pass through;
//   - a job.InvalidError, or any other error wrapping job.ErrInvalid, is
//     InvalidArgument with the error's own message, which names the field;
//   - store.ErrNotFound is NotFound;
//   - store.ErrUnavailable is Unavailable;
//   - context.Canceled is Canceled and context.DeadlineExceeded is
//     DeadlineExceeded;
//   - a *store.InternalError, and everything else, is Internal.
//
// Only an InvalidArgument carries the error's text. The others get a fixed
// message, because their errors may hold host names, SQL and row contents that
// are the operator's business and not the caller's. The fixed message of an
// Internal error is "internal error"; toStatusLogged records what it hides.
// toStatus(nil) is nil.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	if s, ok := status.FromError(err); ok {
		return s.Err()
	}
	var invalid *job.InvalidError
	switch {
	case errors.As(err, &invalid), errors.Is(err, job.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, store.ErrNotFound.Error())
	case errors.Is(err, store.ErrUnavailable):
		return status.Error(codes.Unavailable, store.ErrUnavailable.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	}
	return status.Error(codes.Internal, "internal error")
}

// toStatusLogged is toStatus for a handler of operation op: when the error
// becomes Internal, the whole error is logged first, with the job id and state
// if it is a *store.InternalError. The client sees only "internal error".
func toStatusLogged(log *slog.Logger, op string, err error) error {
	out := toStatus(err)
	if out == nil || status.Code(out) != codes.Internal {
		return out
	}
	attrs := []any{slog.String("op", op), slog.Any("err", err)}
	var internal *store.InternalError
	if errors.As(err, &internal) {
		attrs = append(attrs, slog.Int64("job_id", internal.ID), slog.String("state", internal.State.String()))
	}
	log.Error("internal error", attrs...)
	return out
}
