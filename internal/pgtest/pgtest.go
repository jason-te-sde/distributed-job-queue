// Package pgtest is the test harness for everything that needs a real
// PostgreSQL: it gives each test a private, migrated database whose clock the
// test controls.
//
// It is a helper for test files and imports testing, so only test files may
// import it; the architecture test enforces that. It imports no store package
// except the migrations.
package pgtest

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jason-te-sde/distributed-job-queue/internal/store/pgstore/migrate"
)

// Epoch is what jobqueue.now() returns in a fresh test database and after
// Reset. It is far from the wall clock, so a query that reads the wall clock
// instead of jobqueue.now() cannot pass by accident.
var Epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// cleanupTimeout bounds dropping a test database after the test.
const cleanupTimeout = 30 * time.Second

// databaseSeq numbers the databases one test binary creates.
var databaseSeq atomic.Int64

// DSN returns the connection string of the PostgreSQL server the tests run
// against, from JOBQUEUE_TEST_DSN. It fails the test when the variable is
// unset: pg tests never skip, because a skipped test looks like a passing one.
func DSN(t testing.TB) string {
	t.Helper()
	dsn := os.Getenv("JOBQUEUE_TEST_DSN")
	if dsn == "" {
		t.Fatal("JOBQUEUE_TEST_DSN is not set; pg tests never skip")
	}
	return dsn
}

// NewDB creates a database named jobqueue_test_<pid>_<seq> on the server DSN
// points at, migrates it, installs the test clock and returns a pool on it.
// When the test ends, the pool is closed and the database is dropped, even if
// other connections are still open.
func NewDB(t testing.TB) *pgxpool.Pool {
	t.Helper()
	dsn := DSN(t)
	name := fmt.Sprintf("jobqueue_test_%d_%d", os.Getpid(), databaseSeq.Add(1))
	quoted := pgx.Identifier{name}.Sanitize()

	admin, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatalf("pgtest: connect to the test server: %v", err)
	}
	defer func() { _ = admin.Close(context.WithoutCancel(t.Context())) }()
	if _, err := admin.Exec(t.Context(), "CREATE DATABASE "+quoted); err != nil {
		t.Fatalf("pgtest: create database %s: %v", name, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			t.Errorf("pgtest: connect to drop %s: %v", name, err)
			return
		}
		defer func() { _ = conn.Close(ctx) }()
		if _, err := conn.Exec(ctx, "DROP DATABASE IF EXISTS "+quoted+" WITH (FORCE)"); err != nil {
			t.Errorf("pgtest: drop database %s: %v", name, err)
		}
	})

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("pgtest: parse the DSN: %v", err)
	}
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatalf("pgtest: open a pool on %s: %v", name, err)
	}
	// Registered after the drop, so it runs first: the pool is closed before
	// its database goes away.
	t.Cleanup(pool.Close)

	if _, err := migrate.Up(t.Context(), pool); err != nil {
		t.Fatalf("pgtest: migrate %s: %v", name, err)
	}
	if err := InstallTestClock(t.Context(), pool); err != nil {
		t.Fatalf("pgtest: install the test clock in %s: %v", name, err)
	}
	return pool
}

// InstallTestClock replaces jobqueue.now() with a function that reads a
// one-row table, so a test decides what time it is. It creates the
// jobqueue_test schema, which production databases never have, sets the clock
// to Epoch and redefines the function; the signature is unchanged, so no query
// is affected. The database must already be migrated.
func InstallTestClock(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgtest: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	for _, stmt := range []struct {
		sql  string
		args []any
	}{
		{sql: "CREATE SCHEMA jobqueue_test"},
		{sql: "CREATE TABLE jobqueue_test.clock (t timestamptz NOT NULL)"},
		{sql: "INSERT INTO jobqueue_test.clock (t) VALUES ($1)", args: []any{Epoch}},
		{sql: "CREATE OR REPLACE FUNCTION jobqueue.now() RETURNS timestamptz LANGUAGE sql VOLATILE AS $$ SELECT t FROM jobqueue_test.clock $$"},
	} {
		if _, err := tx.Exec(ctx, stmt.sql, stmt.args...); err != nil {
			return fmt.Errorf("pgtest: %s: %w", stmt.sql, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgtest: commit: %w", err)
	}
	return nil
}

// SetClock makes jobqueue.now() return t. The database keeps microseconds, so
// a t with a finer remainder is refused rather than rounded.
func SetClock(ctx context.Context, pool *pgxpool.Pool, t time.Time) error {
	if t.Nanosecond()%1000 != 0 {
		return fmt.Errorf("pgtest: SetClock(%s): the database keeps microseconds", t.Format(time.RFC3339Nano))
	}
	if _, err := pool.Exec(ctx, "UPDATE jobqueue_test.clock SET t = $1", t.UTC()); err != nil {
		return fmt.Errorf("pgtest: set the clock: %w", err)
	}
	return nil
}

// AdvanceClock moves jobqueue.now() forward by d, which must be a whole number
// of microseconds. A negative d moves it back.
func AdvanceClock(ctx context.Context, pool *pgxpool.Pool, d time.Duration) error {
	if d%time.Microsecond != 0 {
		return fmt.Errorf("pgtest: AdvanceClock(%s): the database keeps microseconds", d)
	}
	if _, err := pool.Exec(ctx, "UPDATE jobqueue_test.clock SET t = t + $1 * interval '1 microsecond'", d.Microseconds()); err != nil {
		return fmt.Errorf("pgtest: advance the clock: %w", err)
	}
	return nil
}

// Reset empties jobqueue.jobs, restarts its identity at 1 and sets the clock
// back to Epoch, so one database can serve many cases.
func Reset(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgtest: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, "TRUNCATE jobqueue.jobs RESTART IDENTITY"); err != nil {
		return fmt.Errorf("pgtest: truncate jobs: %w", err)
	}
	if _, err := tx.Exec(ctx, "UPDATE jobqueue_test.clock SET t = $1", Epoch); err != nil {
		return fmt.Errorf("pgtest: reset the clock: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgtest: commit: %w", err)
	}
	return nil
}
