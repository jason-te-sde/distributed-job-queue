package broker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

func TestToStatusMapping(t *testing.T) {
	invalid := &job.InvalidError{Field: "queue", Reason: "must not be empty"}
	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
		wantMsg  string // the exact message, or "" to skip the check
		notInMsg string // text that must not reach the client
	}{
		{name: "InvalidError", err: invalid, wantCode: codes.InvalidArgument, wantMsg: "invalid queue: must not be empty"},
		{name: "wrapped InvalidError", err: fmt.Errorf("items[2]: %w", invalid), wantCode: codes.InvalidArgument, wantMsg: "items[2]: invalid queue: must not be empty"},
		{name: "ErrInvalid alone", err: fmt.Errorf("bad thing: %w", job.ErrInvalid), wantCode: codes.InvalidArgument, wantMsg: "bad thing: invalid argument"},
		{name: "ErrNotFound", err: store.ErrNotFound, wantCode: codes.NotFound, wantMsg: "job not found"},
		{name: "wrapped ErrNotFound", err: fmt.Errorf("get 7: %w", store.ErrNotFound), wantCode: codes.NotFound, wantMsg: "job not found"},
		{
			name: "ErrUnavailable hides the driver text", err: fmt.Errorf("%w: dial tcp 10.1.2.3:5432: connect: connection refused", store.ErrUnavailable),
			wantCode: codes.Unavailable, wantMsg: "store unavailable", notInMsg: "10.1.2.3",
		},
		{name: "context.Canceled", err: context.Canceled, wantCode: codes.Canceled, wantMsg: "request canceled"},
		{name: "wrapped context.Canceled", err: fmt.Errorf("fetch: %w", context.Canceled), wantCode: codes.Canceled},
		{name: "context.DeadlineExceeded", err: context.DeadlineExceeded, wantCode: codes.DeadlineExceeded, wantMsg: "deadline exceeded"},
		{name: "wrapped context.DeadlineExceeded", err: fmt.Errorf("fetch: %w", context.DeadlineExceeded), wantCode: codes.DeadlineExceeded},
		{
			name: "InternalError", err: &store.InternalError{ID: 7, State: job.StateLeased, Msg: "row 7 has no lease"},
			wantCode: codes.Internal, wantMsg: "internal error", notInMsg: "row 7",
		},
		{
			name: "wrapped InternalError", err: fmt.Errorf("complete: %w", &store.InternalError{ID: 8, State: job.StateDead, Msg: "boom"}),
			wantCode: codes.Internal, wantMsg: "internal error", notInMsg: "boom",
		},
		{name: "anything else", err: errors.New("pq: relation \"jobs\" does not exist"), wantCode: codes.Internal, wantMsg: "internal error", notInMsg: "relation"},
		{
			name: "an unavailable store whose cause is a deadline", err: fmt.Errorf("%w: %w", store.ErrUnavailable, context.DeadlineExceeded),
			wantCode: codes.Unavailable, wantMsg: "store unavailable",
		},
		{
			name: "an invalid argument beats a cancelled context", err: fmt.Errorf("%w: %w", invalid, context.Canceled),
			wantCode: codes.InvalidArgument,
		},
		{name: "a status passes through", err: status.Error(codes.ResourceExhausted, "slow down"), wantCode: codes.ResourceExhausted, wantMsg: "slow down"},
		{name: "a wrapped status keeps its code", err: fmt.Errorf("dispatch: %w", status.Error(codes.Unavailable, "broker is draining")), wantCode: codes.Unavailable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := toStatus(tc.err)
			if err == nil {
				t.Fatal("toStatus returned nil")
			}
			s, ok := status.FromError(err)
			if !ok {
				t.Fatalf("%v is not a gRPC status", err)
			}
			if s.Code() != tc.wantCode {
				t.Fatalf("code = %v, want %v (message %q)", s.Code(), tc.wantCode, s.Message())
			}
			if tc.wantMsg != "" && s.Message() != tc.wantMsg {
				t.Fatalf("message = %q, want %q", s.Message(), tc.wantMsg)
			}
			if tc.notInMsg != "" && strings.Contains(s.Message(), tc.notInMsg) {
				t.Fatalf("message %q leaks %q", s.Message(), tc.notInMsg)
			}
		})
	}
}

func TestToStatusNil(t *testing.T) {
	if err := toStatus(nil); err != nil {
		t.Fatalf("toStatus(nil) = %v", err)
	}
}

func TestToStatusLoggedLogsInternalErrorsOnly(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Caller mistakes, missing rows and cancelled requests are not the
	// operator's business.
	for _, err := range []error{
		&job.InvalidError{Field: "queue", Reason: "x"}, store.ErrNotFound, store.ErrUnavailable,
		context.Canceled, context.DeadlineExceeded, status.Error(codes.Unavailable, "broker is draining"),
	} {
		if got := toStatusLogged(log, "enqueue", err); got == nil {
			t.Fatalf("toStatusLogged(%v) = nil", err)
		}
	}
	if buf.Len() != 0 {
		t.Fatalf("logged the expected errors: %s", buf.String())
	}

	internal := fmt.Errorf("ack: %w", &store.InternalError{ID: 7, State: job.StateLeased, Msg: "row 7 has no lease"})
	got := toStatusLogged(log, "ack", internal)
	if status.Code(got) != codes.Internal || status.Convert(got).Message() != "internal error" {
		t.Fatalf("toStatusLogged = %v", got)
	}
	line := buf.String()
	for _, want := range []string{"level=ERROR", "op=ack", "job_id=7", "state=leased", "row 7 has no lease"} {
		if !strings.Contains(line, want) {
			t.Errorf("log line lacks %q: %s", want, line)
		}
	}

	buf.Reset()
	got = toStatusLogged(log, "heartbeat", errors.New("kaput"))
	if status.Code(got) != codes.Internal {
		t.Fatalf("toStatusLogged = %v", got)
	}
	line = buf.String()
	if !strings.Contains(line, "op=heartbeat") || !strings.Contains(line, "kaput") || strings.Contains(line, "job_id") {
		t.Errorf("log line = %s", line)
	}
}
