package jobqueue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/core/lease"
)

func TestOutcomeOf(t *testing.T) {
	boom := errors.New("boom")
	cancelled, cancel := context.WithCancelCause(context.Background())
	cancel(errors.New("lease lost: gone"))

	for _, tc := range []struct {
		name       string
		ctx        context.Context
		result     []byte
		err        error
		kind       lease.OutcomeKind
		errContain string
		retryAfter time.Duration
		wantResult string
	}{
		{name: "success", ctx: context.Background(), kind: lease.OutcomeSuccess},
		{name: "success with a result", ctx: context.Background(), result: []byte("42"), kind: lease.OutcomeSuccess, wantResult: "42"},
		{name: "plain error retries", ctx: context.Background(), err: boom, kind: lease.OutcomeRetry, errContain: "boom"},
		{name: "retry after", ctx: context.Background(), err: RetryAfter(boom, 5*time.Second), kind: lease.OutcomeRetry, errContain: "boom", retryAfter: 5 * time.Second},
		{name: "retry after below zero", ctx: context.Background(), err: RetryAfter(boom, -time.Second), kind: lease.OutcomeRetry, errContain: "boom"},
		{name: "retry after beyond the longest delay", ctx: context.Background(), err: RetryAfter(boom, 400*24*time.Hour), kind: lease.OutcomeRetry, errContain: "boom", retryAfter: job.MaxDelay},
		{name: "retry after in whole microseconds", ctx: context.Background(), err: RetryAfter(boom, 1500*time.Nanosecond), kind: lease.OutcomeRetry, errContain: "boom", retryAfter: time.Microsecond},
		{name: "permanent", ctx: context.Background(), err: Permanent(boom), kind: lease.OutcomePermanent, errContain: "boom"},
		{name: "permanent wrapped further", ctx: context.Background(), err: fmt.Errorf("while saving: %w", Permanent(boom)), kind: lease.OutcomePermanent, errContain: "while saving: boom"},
		{name: "permanent wins over retry after, inside", ctx: context.Background(), err: Permanent(RetryAfter(boom, time.Hour)), kind: lease.OutcomePermanent, errContain: "boom"},
		{name: "permanent wins over retry after, outside", ctx: context.Background(), err: RetryAfter(Permanent(boom), time.Hour), kind: lease.OutcomePermanent, errContain: "boom"},
		{name: "cancelled handler names the cause", ctx: cancelled, err: boom, kind: lease.OutcomeRetry, errContain: "boom (handler cancelled: lease lost: gone)"},
		{name: "success after cancellation is still success", ctx: cancelled, kind: lease.OutcomeSuccess},
		{name: "result at the limit", ctx: context.Background(), result: make([]byte, job.MaxResultBytes), kind: lease.OutcomeSuccess, wantResult: string(make([]byte, job.MaxResultBytes))},
		{name: "result over the limit", ctx: context.Background(), result: make([]byte, job.MaxResultBytes+1), kind: lease.OutcomePermanent, errContain: "result exceeds 65536 bytes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			j := &Job{}
			j.SetResult(tc.result)
			got := outcomeOf(tc.ctx, j, tc.err)
			if got.Kind != tc.kind {
				t.Fatalf("Kind = %v, want %v (outcome %+v)", got.Kind, tc.kind, got)
			}
			if tc.errContain != "" && !strings.Contains(got.Error, tc.errContain) {
				t.Errorf("Error = %q, want it to contain %q", got.Error, tc.errContain)
			}
			if got.RetryAfter != tc.retryAfter {
				t.Errorf("RetryAfter = %v, want %v", got.RetryAfter, tc.retryAfter)
			}
			if string(got.Result) != tc.wantResult {
				t.Errorf("Result is %d bytes, want %d", len(got.Result), len(tc.wantResult))
			}
		})
	}
}

func TestRetryAfterAndPermanentOfNilAreNil(t *testing.T) {
	if err := RetryAfter(nil, time.Second); err != nil {
		t.Errorf("RetryAfter(nil) = %v, want nil: wrapping no error leaves no error", err)
	}
	if err := Permanent(nil); err != nil {
		t.Errorf("Permanent(nil) = %v, want nil", err)
	}
}

func TestWrappersKeepTheErrorIdentity(t *testing.T) {
	boom := errors.New("boom")
	if err := RetryAfter(boom, time.Second); !errors.Is(err, boom) || err.Error() != "boom" {
		t.Errorf("RetryAfter(boom) = %v, want an error that is boom and reads as boom", err)
	}
	if err := Permanent(boom); !errors.Is(err, boom) || err.Error() != "boom" {
		t.Errorf("Permanent(boom) = %v, want an error that is boom and reads as boom", err)
	}
}

// The message of a failure travels in a protobuf string field, which must be
// valid UTF-8, and is stored cut to 4096 bytes. A handler's error or panic can
// be neither short nor valid text, and one bad message must not fail the whole
// ack batch that carries it.
func TestOutcomeMessagesAreFitForTheWire(t *testing.T) {
	for name, msg := range map[string]string{
		"invalid UTF-8": "bad \xff\xfe bytes",
		"very long":     strings.Repeat("é", 10000),
		"with NUL":      "a\x00b",
	} {
		t.Run(name, func(t *testing.T) {
			got := outcomeOf(context.Background(), &Job{}, errors.New(msg)).Error
			if !utf8.ValidString(got) {
				t.Errorf("Error %q is not valid UTF-8", got)
			}
			if len(got) > job.MaxErrorBytes {
				t.Errorf("Error is %d bytes, want at most %d", len(got), job.MaxErrorBytes)
			}
			if strings.ContainsRune(got, 0) {
				t.Errorf("Error %q holds a NUL", got)
			}
			if got == "" {
				t.Error("the message was lost")
			}
		})
	}
}

func TestPanicOutcome(t *testing.T) {
	got := panicOutcome(fmt.Errorf("wrapped: %w", errors.New("kaboom")))
	if got.Kind != lease.OutcomeRetry {
		t.Errorf("Kind = %v, want retry", got.Kind)
	}
	if !strings.HasPrefix(got.Error, "panic: wrapped: kaboom\n") || !strings.Contains(got.Error, "goroutine") {
		t.Errorf("Error = %q, want %q and the stack", got.Error, "panic: wrapped: kaboom")
	}
}

func TestGeneratedWorkerIDIsValid(t *testing.T) {
	for _, host := range []string{"", "plain", "has space and ünïcode", strings.Repeat("h", 300)} {
		id := generateWorkerID(host, 4242, 0xdeadbeef)
		if err := job.ValidateWorkerID(id); err != nil {
			t.Errorf("generateWorkerID(%q) = %q: %v", host, id, err)
		}
		if !strings.HasSuffix(id, "-4242-deadbeef") {
			t.Errorf("generateWorkerID(%q) = %q, want it to end in the pid and the random part", host, id)
		}
	}
}
