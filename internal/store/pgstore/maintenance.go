package pgstore

import (
	"context"
	"errors"

	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

var errNotImplemented = errors.New("pgstore: not implemented")

// Reap implements store.Store. It is not implemented yet.
func (s *Store) Reap(context.Context, int, store.BackoffFunc) (int, error) {
	return 0, errNotImplemented
}

// Prune implements store.Store. It is not implemented yet.
func (s *Store) Prune(context.Context, store.Retention, int) (int, error) {
	return 0, errNotImplemented
}
