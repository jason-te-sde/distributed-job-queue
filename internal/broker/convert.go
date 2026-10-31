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
//   - an error that carries a gRPC status keeps that status, with the status's
//     own message and never the text of the chain around it, so that errors
//     of the dispatcher pass through; a status of Internal or Unknown, or of
//     OK next to an error, is Internal like any other unexpected error;
//   - a job.InvalidError, or any other error wrapping job.ErrInvalid, is
//     InvalidArgument with the error's own message, which names the field;
//   - store.ErrNotFound is NotFound;
//   - store.ErrUnavailable is Unavailable;
//   - context.Canceled is Canceled and context.DeadlineExceeded is
//     DeadlineExceeded;
//   - a *store.InternalError, and everything else, is Internal.
//
// Only an InvalidArgument carries the error's text, and a status that was
// passed through its own. The others get a fixed message, because their errors
// may hold host names, SQL and row contents that are the operator's business
// and not the caller's. The fixed message of an Internal error is "internal
// error"; toStatusLogged records what it hides. toStatus(nil) is nil.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	var carrier interface{ GRPCStatus() *status.Status }
	if errors.As(err, &carrier) {
		// status.FromError would give a wrapped status the text of the whole
		// chain, which may hold addresses and SQL; ask the inner status itself.
		if s := carrier.GRPCStatus(); s != nil {
			// An error must not become a success, and the text of an internal
			// failure is not for the client.
			if c := s.Code(); c != codes.OK && c != codes.Unknown && c != codes.Internal {
				return s.Err()
			}
		}
		return status.Error(codes.Internal, "internal error")
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
