package pgstore

import (
	"context"

	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

// Reap implements store.Store. It is not implemented yet.
func (s *Store) Reap(context.Context, int, store.BackoffFunc) (int, error) {
	return 0, errNotImplemented
}

// Prune implements store.Store. It is not implemented yet.
func (s *Store) Prune(context.Context, store.Retention, int) (int, error) {
	return 0, errNotImplemented
}
