//go:build pg

package migrate_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jason-te-sde/distributed-job-queue/internal/pgtest"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/pgstore/migrate"
)

var rawSeq atomic.Int64

// newRawDB creates an empty database, which pgtest.NewDB never does because
// it migrates, and returns a pool on it. The database is dropped when the
// test ends.
func newRawDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := pgtest.DSN(t)
	name := fmt.Sprintf("jobqueue_raw_%d_%d", os.Getpid(), rawSeq.Add(1))
	quoted := pgx.Identifier{name}.Sanitize()

	admin, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatalf("connect to the admin database: %v", err)
	}
	defer func() { _ = admin.Close(context.Background()) }()
	if _, err := admin.Exec(t.Context(), "CREATE DATABASE "+quoted); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			t.Errorf("connect to drop %s: %v", name, err)
			return
		}
		defer func() { _ = conn.Close(ctx) }()
		if _, err := conn.Exec(ctx, "DROP DATABASE IF EXISTS "+quoted+" WITH (FORCE)"); err != nil {
			t.Errorf("drop database %s: %v", name, err)
		}
	})

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse JOBQUEUE_TEST_DSN: %v", err)
	}
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatalf("open a pool on %s: %v", name, err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestPGMigrateUpIsIdempotent(t *testing.T) {
	pool := newRawDB(t)
	first, err := migrate.Up(t.Context(), pool)
	if err != nil {
		t.Fatalf("first Up: %v", err)
	}
	if first != migrate.Version {
		t.Fatalf("first Up applied %d migrations, want %d", first, migrate.Version)
	}
	second, err := migrate.Up(t.Context(), pool)
	if err != nil {
		t.Fatalf("second Up: %v", err)
	}
	if second != 0 {
		t.Fatalf("second Up applied %d migrations, want 0", second)
	}
}

func TestPGMigrateConcurrentUp(t *testing.T) {
	const callers = 8
	pool := newRawDB(t)

	var wg sync.WaitGroup
	applied := make([]int, callers)
	errs := make([]error, callers)
	start := make(chan struct{})
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			applied[i], errs[i] = migrate.Up(t.Context(), pool)
		}()
	}
	close(start)
	wg.Wait()

	total := 0
	for i := range callers {
		if errs[i] != nil {
			t.Errorf("caller %d: Up failed: %v", i, errs[i])
		}
		total += applied[i]
	}
	if total != migrate.Version {
		t.Fatalf("callers applied %v migrations in total (%d); each migration must run exactly once, so want %d", applied, total, migrate.Version)
	}
}

func TestPGCheckDetectsMismatch(t *testing.T) {
	pool := newRawDB(t)

	wantMismatch := func(stage string, have int) {
		t.Helper()
		err := migrate.Check(t.Context(), pool)
		var mismatch *migrate.SchemaMismatchError
		if !errors.As(err, &mismatch) {
			t.Fatalf("%s: Check = %v, want a *SchemaMismatchError", stage, err)
		}
		if *mismatch != (migrate.SchemaMismatchError{Have: have, Want: migrate.Version}) {
			t.Fatalf("%s: Check reported %+v, want {Have:%d Want:%d}", stage, *mismatch, have, migrate.Version)
		}
	}

	wantMismatch("unmigrated database", 0)

	if _, err := migrate.Up(t.Context(), pool); err != nil {
		t.Fatalf("Up: %v", err)
	}
	if err := migrate.Check(t.Context(), pool); err != nil {
		t.Fatalf("Check after Up = %v, want nil", err)
	}

	if _, err := pool.Exec(t.Context(), "INSERT INTO jobqueue.schema_migrations VALUES ($1)", migrate.Version+1); err != nil {
		t.Fatalf("record a newer migration: %v", err)
	}
	wantMismatch("database ahead of the binary", migrate.Version+1)
}

func TestPGTestClockControlsNow(t *testing.T) {
	pool := pgtest.NewDB(t)
	now := func() time.Time {
		t.Helper()
		var got time.Time
		if err := pool.QueryRow(t.Context(), "SELECT jobqueue.now()").Scan(&got); err != nil {
			t.Fatalf("SELECT jobqueue.now(): %v", err)
		}
		return got
	}
	wantNow := func(stage string, want time.Time) {
		t.Helper()
		if got := now(); !got.Equal(want) {
			t.Fatalf("%s: jobqueue.now() = %s, want %s", stage, got.UTC().Format(time.RFC3339Nano), want.UTC().Format(time.RFC3339Nano))
		}
	}

	wantNow("fresh database", pgtest.Epoch)

	at := time.Date(2026, 3, 4, 5, 6, 7, 123456000, time.UTC)
	if err := pgtest.SetClock(t.Context(), pool, at); err != nil {
		t.Fatalf("SetClock: %v", err)
	}
	wantNow("after SetClock", at)

	if err := pgtest.AdvanceClock(t.Context(), pool, 1500*time.Millisecond); err != nil {
		t.Fatalf("AdvanceClock: %v", err)
	}
	wantNow("after AdvanceClock(1.5s)", at.Add(1500*time.Millisecond))

	t.Run("rejects values finer than a microsecond", func(t *testing.T) {
		before := now()
		if err := pgtest.AdvanceClock(t.Context(), pool, 1500*time.Nanosecond); err == nil {
			t.Error("AdvanceClock(1500ns) succeeded; the database keeps microseconds, so the remainder would be lost silently")
		}
		if err := pgtest.SetClock(t.Context(), pool, at.Add(time.Nanosecond)); err == nil {
			t.Error("SetClock with a nanosecond component succeeded; the remainder would be lost silently")
		}
		if after := now(); !after.Equal(before) {
			t.Fatalf("a rejected call still moved the clock from %s to %s", before, after)
		}
	})

	t.Run("Reset restores the epoch and empties jobs", func(t *testing.T) {
		insert := func() int64 {
			t.Helper()
			var id int64
			err := pool.QueryRow(t.Context(),
				`INSERT INTO jobqueue.jobs (queue, tenant, state, payload, run_at, attempts, max_attempts, created_at)
				 VALUES ('q', 't', 'ready', '\x', jobqueue.now(), 0, 3, jobqueue.now()) RETURNING id`).Scan(&id)
			if err != nil {
				t.Fatalf("insert a job: %v", err)
			}
			return id
		}
		insert()
		insert()
		if err := pgtest.Reset(t.Context(), pool); err != nil {
			t.Fatalf("Reset: %v", err)
		}
		wantNow("after Reset", pgtest.Epoch)
		var n int
		if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM jobqueue.jobs").Scan(&n); err != nil {
			t.Fatalf("count jobs: %v", err)
		}
		if n != 0 {
			t.Fatalf("%d jobs survived Reset", n)
		}
		if id := insert(); id != 1 {
			t.Fatalf("first id after Reset = %d, want 1: Reset must restart the identity", id)
		}
	})
}

func TestPGConstraintsRejectInconsistentRows(t *testing.T) {
	pool := pgtest.NewDB(t)
	ctx := t.Context()
	at := pgtest.Epoch

	// row is a job that satisfies every constraint; each case breaks one.
	type row struct {
		state        string
		payload      []byte
		attempts     int
		maxAttempts  int
		leaseExpires *time.Time
		leasedBy     *string
		result       []byte
		finishedAt   *time.Time
	}
	worker := "w1"
	valid := func(mutate func(*row)) row {
		r := row{state: "ready", payload: []byte("p"), attempts: 0, maxAttempts: 3}
		mutate(&r)
		return r
	}
	insert := func(r row) error {
		_, err := pool.Exec(ctx,
			`INSERT INTO jobqueue.jobs
			   (queue, tenant, state, payload, run_at, attempts, max_attempts, lease_expires_at, leased_by, result, created_at, finished_at)
			 VALUES ('q', 't', $1, $2, $3, $4, $5, $6, $7, $8, $3, $9)`,
			r.state, r.payload, at, r.attempts, r.maxAttempts, r.leaseExpires, r.leasedBy, r.result, r.finishedAt)
		return err
	}

	accepted := []struct {
		name string
		row  row
	}{
		{"a ready job", valid(func(*row) {})},
		{"a leased job", valid(func(r *row) { r.state, r.attempts, r.leaseExpires, r.leasedBy = "leased", 1, &at, &worker })},
		{"a succeeded job", valid(func(r *row) { r.state, r.attempts, r.finishedAt = "succeeded", 1, &at })},
		{"a dead job", valid(func(r *row) { r.state, r.attempts, r.finishedAt = "dead", 3, &at })},
		{"a cancelled job", valid(func(r *row) { r.state, r.finishedAt = "cancelled", &at })},
		{"an empty payload", valid(func(r *row) { r.payload = []byte{} })},
		{"a payload of 262144 bytes", valid(func(r *row) { r.payload = make([]byte, 262144) })},
		{"a result of 65536 bytes", valid(func(r *row) { r.state, r.finishedAt, r.result = "succeeded", &at, make([]byte, 65536) })},
		{"max_attempts of 1", valid(func(r *row) { r.maxAttempts = 1 })},
		{"max_attempts of 100", valid(func(r *row) { r.maxAttempts = 100 })},
		{"attempts equal to max_attempts", valid(func(r *row) { r.attempts, r.maxAttempts = 3, 3 })},
	}
	for _, c := range accepted {
		t.Run("accepts "+c.name, func(t *testing.T) {
			if err := insert(c.row); err != nil {
				t.Fatalf("insert rejected a consistent row %+v: %v", c.row, err)
			}
		})
	}

	rejected := []struct {
		name       string
		row        row
		constraint string // empty when the constraint is a column check with a generated name
	}{
		{"a leased job without lease_expires_at", valid(func(r *row) { r.state, r.attempts, r.leasedBy = "leased", 1, &worker }), "jobs_leased_iff_lease_set"},
		{"a leased job without leased_by", valid(func(r *row) { r.state, r.attempts, r.leaseExpires = "leased", 1, &at }), "jobs_leased_iff_lease_set"},
		{"a leased job with neither", valid(func(r *row) { r.state, r.attempts = "leased", 1 }), "jobs_leased_iff_lease_set"},
		{"a ready job holding a lease", valid(func(r *row) { r.leaseExpires, r.leasedBy = &at, &worker }), "jobs_leased_iff_lease_set"},
		{"a succeeded job without finished_at", valid(func(r *row) { r.state, r.attempts = "succeeded", 1 }), "jobs_finished_iff_terminal"},
		{"a dead job without finished_at", valid(func(r *row) { r.state, r.attempts = "dead", 3 }), "jobs_finished_iff_terminal"},
		{"a cancelled job without finished_at", valid(func(r *row) { r.state = "cancelled" }), "jobs_finished_iff_terminal"},
		{"a ready job with finished_at", valid(func(r *row) { r.finishedAt = &at }), "jobs_finished_iff_terminal"},
		{"attempts above max_attempts", valid(func(r *row) { r.attempts, r.maxAttempts = 4, 3 }), "jobs_attempts_within_max"},
		{"an unknown state", valid(func(r *row) { r.state = "running" }), ""},
		{"a payload of 262145 bytes", valid(func(r *row) { r.payload = make([]byte, 262145) }), ""},
		{"a result of 65537 bytes", valid(func(r *row) { r.state, r.finishedAt, r.result = "succeeded", &at, make([]byte, 65537) }), ""},
		{"negative attempts", valid(func(r *row) { r.attempts = -1 }), ""},
		{"max_attempts of 0", valid(func(r *row) { r.maxAttempts = 0 }), ""},
		{"max_attempts of 101", valid(func(r *row) { r.maxAttempts = 101 }), ""},
	}
	for _, c := range rejected {
		t.Run("rejects "+c.name, func(t *testing.T) {
			err := insert(c.row)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) {
				t.Fatalf("insert of %+v returned %v, want a PostgreSQL error", c.row, err)
			}
			if pgErr.Code != "23514" {
				t.Fatalf("insert of %+v failed with SQLSTATE %s (%s), want 23514 check_violation", c.row, pgErr.Code, pgErr.Message)
			}
			if c.constraint != "" && pgErr.ConstraintName != c.constraint {
				t.Fatalf("insert of %+v violated %q, want %q", c.row, pgErr.ConstraintName, c.constraint)
			}
		})
	}
}
