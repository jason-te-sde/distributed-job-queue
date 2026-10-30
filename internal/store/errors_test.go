package store_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

func TestInternalErrorMessage(t *testing.T) {
	err := &store.InternalError{ID: 42, State: job.StateLeased, Msg: "lease without an expiry"}
	want := "internal invariant broken: job 42 in state leased: lease without an expiry"
	if got := err.Error(); got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestInternalErrorSurvivesWrapping(t *testing.T) {
	wrapped := fmt.Errorf("pgstore reap: %w", &store.InternalError{ID: 7, State: job.StateDead, Msg: "x"})
	var ie *store.InternalError
	if !errors.As(wrapped, &ie) || ie.ID != 7 || ie.State != job.StateDead {
		t.Errorf("errors.As on %v found %+v", wrapped, ie)
	}
	if errors.Is(wrapped, job.ErrInvalid) {
		t.Error("an internal invariant break must not look like a caller's mistake")
	}
}

func TestErrorsAreDistinct(t *testing.T) {
	if errors.Is(store.ErrNotFound, store.ErrUnavailable) || errors.Is(store.ErrUnavailable, job.ErrInvalid) || errors.Is(store.ErrNotFound, job.ErrInvalid) {
		t.Error("ErrNotFound, ErrUnavailable and job.ErrInvalid must be three different errors")
	}
}
