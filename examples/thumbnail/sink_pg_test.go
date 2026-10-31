//go:build pg

package main

import (
	"context"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jason-te-sde/distributed-job-queue/internal/pgtest"
)

// newSink returns a sink on an empty database with the schema applied.
func newSink(t *testing.T) (*Sink, *pgxpool.Pool) {
	t.Helper()
	pool := pgtest.NewEmptyDB(t)
	s := NewSink(pool)
	if err := s.EnsureSchema(t.Context()); err != nil {
		t.Fatal(err)
	}
	return s, pool
}

// stored reads the row of a job.
func stored(t *testing.T, pool *pgxpool.Pool, jobID int64) (token int64, sum string, n int) {
	t.Helper()
	err := pool.QueryRow(t.Context(), "SELECT token, sha256, bytes FROM thumbnails WHERE job_id = $1", jobID).Scan(&token, &sum, &n)
	if err != nil {
		t.Fatalf("read the row of job %d: %v", jobID, err)
	}
	return token, sum, n
}

func TestPGFencedSinkRejectsOlderToken(t *testing.T) {
	s, pool := newSink(t)
	ctx := t.Context()
	const job = 42

	write := func(token int64, sum string, n int, wantApplied bool) {
		t.Helper()
		applied, err := s.Write(ctx, job, token, sum, n)
		if err != nil {
			t.Fatalf("write token %d: %v", token, err)
		}
		if applied != wantApplied {
			t.Fatalf("write token %d: applied = %v, want %v", token, applied, wantApplied)
		}
	}
	check := func(token int64, sum string, n int) {
		t.Helper()
		if gt, gs, gn := stored(t, pool, job); gt != token || gs != sum || gn != n {
			t.Fatalf("stored row = (token %d, %s, %d bytes), want (token %d, %s, %d bytes)", gt, gs, gn, token, sum, n)
		}
	}

	write(2, "sum-two", 200, true)
	check(2, "sum-two", 200)

	write(1, "sum-one", 100, false)
	check(2, "sum-two", 200) // the older holder changed nothing

	write(3, "sum-three", 300, true)
	check(3, "sum-three", 300)

	write(3, "sum-again", 400, false) // a repeat of the same token is not newer
	check(3, "sum-three", 300)
}

func TestPGFencedSinkFencesEachJobSeparately(t *testing.T) {
	s, pool := newSink(t)
	ctx := t.Context()

	if applied, err := s.Write(ctx, 1, 9, "a", 1); err != nil || !applied {
		t.Fatalf("job 1, token 9: applied = %v, err = %v", applied, err)
	}
	// Token 1 is below job 1's token but is the first write of job 2.
	if applied, err := s.Write(ctx, 2, 1, "b", 2); err != nil || !applied {
		t.Fatalf("job 2, token 1: applied = %v, err = %v", applied, err)
	}
	if token, _, _ := stored(t, pool, 1); token != 9 {
		t.Errorf("job 1 has token %d, want 9", token)
	}
}

func TestPGEnsureSchemaIsIdempotentAndSafeConcurrently(t *testing.T) {
	pool := pgtest.NewEmptyDB(t)
	s := NewSink(pool)

	// Replicas that KEDA starts together all apply the schema at once. Two
	// bare CREATE TABLE IF NOT EXISTS statements can collide on the catalog.
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for range 16 {
		wg.Go(func() { errs <- s.EnsureSchema(context.WithoutCancel(t.Context())) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent EnsureSchema: %v", err)
		}
	}

	if applied, err := s.Write(t.Context(), 1, 5, "kept", 1); err != nil || !applied {
		t.Fatalf("write: applied = %v, err = %v", applied, err)
	}
	if err := s.EnsureSchema(t.Context()); err != nil {
		t.Fatalf("a second EnsureSchema: %v", err)
	}
	if token, sum, _ := stored(t, pool, 1); token != 5 || sum != "kept" {
		t.Errorf("EnsureSchema changed an existing row: token %d, sum %q", token, sum)
	}
}
