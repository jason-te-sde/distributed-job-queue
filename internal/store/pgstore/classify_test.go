package pgstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

// retryable is an error that pgconn reports as safe to retry.
type retryable struct{}

func (retryable) Error() string     { return "nothing was sent" }
func (retryable) SafeToRetry() bool { return true }

func TestClassifyNil(t *testing.T) {
	if err := classify("enqueue", nil); err != nil {
		t.Errorf("classify of nil = %v, want nil", err)
	}
}

func TestClassifyConnectionFailures(t *testing.T) {
	for name, err := range map[string]error{
		"a dial error":               &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")},
		"a closed pool":              errors.New("closed pool"),
		"a closed pool, wrapped":     fmt.Errorf("acquire: %w", errors.New("closed pool")),
		"an error safe to retry":     retryable{},
		"a connection exception":     &pgconn.PgError{Code: "08006"},
		"a connection failure":       &pgconn.PgError{Code: "08001"},
		"an administrator shutdown":  &pgconn.PgError{Code: "57P01"},
		"a crash shutdown":           &pgconn.PgError{Code: "57P02"},
		"cannot connect now":         &pgconn.PgError{Code: "57P03"},
		"too many connections":       &pgconn.PgError{Code: "53300"},
		"a wrapped connection error": fmt.Errorf("while sending: %w", &pgconn.PgError{Code: "08P01"}),
		"a deadlock":                 &pgconn.PgError{Code: "40P01"},
		"a serialization failure":    &pgconn.PgError{Code: "40001"},
		"a stream that ended":        io.EOF,
		"a stream that ended early":  io.ErrUnexpectedEOF,
		"an early end, wrapped":      fmt.Errorf("receive message: %w", io.ErrUnexpectedEOF),
	} {
		got := classify("lease", err)
		if !errors.Is(got, store.ErrUnavailable) {
			t.Errorf("%s: classify = %v, want one wrapping store.ErrUnavailable", name, got)
		}
		if errors.Is(got, job.ErrInvalid) {
			t.Errorf("%s: classify = %v, which must not look like a caller's mistake", name, got)
		}
	}
}

func TestClassifyLeavesOtherErrorsAlone(t *testing.T) {
	for name, err := range map[string]error{
		"a unique violation":  &pgconn.PgError{Code: "23505"},
		"a check violation":   &pgconn.PgError{Code: "23514"},
		"a syntax error":      &pgconn.PgError{Code: "42601"},
		"a statement timeout": &pgconn.PgError{Code: "57014"},
		"a plain error":       errors.New("boom"),
	} {
		got := classify("complete", err)
		if errors.Is(got, store.ErrUnavailable) {
			t.Errorf("%s: classify = %v, want an error that is not store.ErrUnavailable", name, got)
		}
		if !errors.Is(got, err) {
			t.Errorf("%s: classify = %v, want one wrapping the original", name, got)
		}
	}
}

// A cancelled or expired context is the caller's doing and not a failure of
// the store. context.DeadlineExceeded is also a net.Error, so the check has to
// come first.
func TestClassifyContextErrors(t *testing.T) {
	for name, err := range map[string]error{
		"canceled":                 context.Canceled,
		"deadline exceeded":        context.DeadlineExceeded,
		"canceled, wrapped":        fmt.Errorf("while reading: %w", context.Canceled),
		"deadline, as a net.Error": &net.OpError{Op: "read", Err: context.DeadlineExceeded},
	} {
		got := classify("get", err)
		if errors.Is(got, store.ErrUnavailable) {
			t.Errorf("%s: classify = %v, which must not wrap store.ErrUnavailable", name, got)
		}
		if !errors.Is(got, context.Canceled) && !errors.Is(got, context.DeadlineExceeded) {
			t.Errorf("%s: classify = %v, want the context error preserved", name, got)
		}
	}
}

func TestClassifyNamesTheOperation(t *testing.T) {
	got := classify("reap", errors.New("boom"))
	if want := "pgstore reap: boom"; got.Error() != want {
		t.Errorf("classify = %q, want %q", got, want)
	}
	got = classify("reap", &pgconn.PgError{Code: "08006", Message: "gone"})
	if !errors.Is(got, store.ErrUnavailable) || got.Error() == "" {
		t.Errorf("classify = %q", got)
	}
}
