package main

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// schemaSQL creates the table of the sink.
//
//go:embed schema.sql
var schemaSQL string

// schemaLockKey is the advisory lock that serializes EnsureSchema between
// replicas. The number spells "thumb" in ASCII.
const schemaLockKey = 0x7468756d62

// Sink stores the result of each job in PostgreSQL, fenced by the job's token.
type Sink struct {
	pool *pgxpool.Pool
}

// NewSink returns a sink that writes through pool. The caller closes the pool.
func NewSink(pool *pgxpool.Pool) *Sink { return &Sink{pool: pool} }

// EnsureSchema creates the table if it does not exist. It is safe to call from
// many processes at once: two CREATE TABLE IF NOT EXISTS statements that run
// together can both pass the existence check and then collide in the catalog,
// so the statement runs under an advisory lock that the transaction releases.
func (s *Sink) EnsureSchema(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("apply the schema: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", schemaLockKey); err != nil {
		return fmt.Errorf("apply the schema: lock: %w", err)
	}
	if _, err := tx.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply the schema: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("apply the schema: commit: %w", err)
	}
	return nil
}

// writeSQL stores a result unless a newer lease already has. The WHERE clause
// is the fence: the row is replaced only by a higher token, so a handler that
// kept running after its lease was lost cannot overwrite the work of the
// holder that came after it. The clock is the database's, as every writer
// shares it.
const writeSQL = `
INSERT INTO thumbnails (job_id, token, sha256, bytes, written_at)
VALUES ($1, $2, $3, $4, clock_timestamp())
ON CONFLICT (job_id) DO UPDATE
SET token = EXCLUDED.token, sha256 = EXCLUDED.sha256, bytes = EXCLUDED.bytes, written_at = EXCLUDED.written_at
WHERE thumbnails.token < EXCLUDED.token`

// Write records the thumbnail of a job, which has the SHA-256 sum and n bytes,
// under the fencing token of the lease that made it. applied is false when the
// table already holds a row of the job with the same or a higher token; the
// row is then left as it was.
func (s *Sink) Write(ctx context.Context, jobID, token int64, sum string, n int) (applied bool, err error) {
	tag, err := s.pool.Exec(ctx, writeSQL, jobID, token, sum, n)
	if err != nil {
		return false, fmt.Errorf("write the thumbnail of job %d: %w", jobID, err)
	}
	return tag.RowsAffected() == 1, nil
}
