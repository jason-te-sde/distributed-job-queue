//go:build pg

package migrate_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jason-te-sde/distributed-job-queue/internal/pgtest"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/pgstore/migrate"
)

func TestPGMigrateUpIsIdempotent(t *testing.T) {
	pool := pgtest.NewEmptyDB(t)
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
	pool := pgtest.NewEmptyDB(t)

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
	pool := pgtest.NewEmptyDB(t)

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
	// describe names the fields that matter, since %+v would print addresses.
	describe := func(r row) string {
		return fmt.Sprintf("{state:%s payload:%dB attempts:%d/%d lease_expires_at set:%t leased_by set:%t result:%dB finished_at set:%t}",
			r.state, len(r.payload), r.attempts, r.maxAttempts, r.leaseExpires != nil, r.leasedBy != nil, len(r.result), r.finishedAt != nil)
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
				t.Fatalf("insert rejected a consistent row %s: %v", describe(c.row), err)
			}
		})
	}

	rejected := []struct {
		name string
		row  row
		// constraints names the checks that may report the violation; nil for a
		// column check, whose name PostgreSQL generates. PostgreSQL reports the
		// first violated check in name order, which is not a contract, so a row
		// that breaks two checks lists both.
		constraints []string
	}{
		{"a leased job without lease_expires_at", valid(func(r *row) { r.state, r.attempts, r.leasedBy = "leased", 1, &worker }), []string{"jobs_leased_iff_lease_set", "jobs_lease_fields_paired"}},
		{"a leased job without leased_by", valid(func(r *row) { r.state, r.attempts, r.leaseExpires = "leased", 1, &at }), []string{"jobs_leased_iff_lease_set", "jobs_lease_fields_paired"}},
		{"a leased job with neither", valid(func(r *row) { r.state, r.attempts = "leased", 1 }), []string{"jobs_leased_iff_lease_set"}},
		{"a ready job holding a lease", valid(func(r *row) { r.leaseExpires, r.leasedBy = &at, &worker }), []string{"jobs_leased_iff_lease_set"}},
		{"a ready job with only leased_by set", valid(func(r *row) { r.leasedBy = &worker }), []string{"jobs_lease_fields_paired"}},
		{"a ready job with only lease_expires_at set", valid(func(r *row) { r.leaseExpires = &at }), []string{"jobs_lease_fields_paired"}},
		{"a succeeded job without finished_at", valid(func(r *row) { r.state, r.attempts = "succeeded", 1 }), []string{"jobs_finished_iff_terminal"}},
		{"a dead job without finished_at", valid(func(r *row) { r.state, r.attempts = "dead", 3 }), []string{"jobs_finished_iff_terminal"}},
		{"a cancelled job without finished_at", valid(func(r *row) { r.state = "cancelled" }), []string{"jobs_finished_iff_terminal"}},
		{"a ready job with finished_at", valid(func(r *row) { r.finishedAt = &at }), []string{"jobs_finished_iff_terminal"}},
		{"attempts above max_attempts", valid(func(r *row) { r.attempts, r.maxAttempts = 4, 3 }), []string{"jobs_attempts_within_max"}},
		{"an unknown state", valid(func(r *row) { r.state = "running" }), nil},
		{"a payload of 262145 bytes", valid(func(r *row) { r.payload = make([]byte, 262145) }), nil},
		{"a result of 65537 bytes", valid(func(r *row) { r.state, r.finishedAt, r.result = "succeeded", &at, make([]byte, 65537) }), nil},
		{"negative attempts", valid(func(r *row) { r.attempts = -1 }), nil},
		{"max_attempts of 0", valid(func(r *row) { r.maxAttempts = 0 }), nil},
		{"max_attempts of 101", valid(func(r *row) { r.maxAttempts = 101 }), nil},
	}
	for _, c := range rejected {
		t.Run("rejects "+c.name, func(t *testing.T) {
			err := insert(c.row)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) {
				t.Fatalf("insert of %s returned %v, want a PostgreSQL error", describe(c.row), err)
			}
			if pgErr.Code != "23514" {
				t.Fatalf("insert of %s failed with SQLSTATE %s (%s), want 23514 check_violation", describe(c.row), pgErr.Code, pgErr.Message)
			}
			if c.constraints != nil && !slices.Contains(c.constraints, pgErr.ConstraintName) {
				t.Fatalf("insert of %s violated %q, want one of %q", describe(c.row), pgErr.ConstraintName, c.constraints)
			}
		})
	}
}

func TestPGJobsIndexesArePinnedAndIdempotencyKeyIsUnique(t *testing.T) {
	pool := pgtest.NewDB(t)
	ctx := t.Context()

	t.Run("the six partial indexes", func(t *testing.T) {
		rows, err := pool.Query(ctx, `
			SELECT c.relname,
			       (SELECT amname FROM pg_am WHERE oid = c.relam),
			       i.indisunique,
			       coalesce((SELECT array_agg(a.attname::text ORDER BY k.ord)
			                   FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
			                   JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum), '{}'),
			       coalesce(pg_get_expr(i.indpred, i.indrelid), '')
			  FROM pg_index i
			  JOIN pg_class c ON c.oid = i.indexrelid
			 WHERE i.indrelid = 'jobqueue.jobs'::regclass AND NOT i.indisprimary
			 ORDER BY c.relname`)
		if err != nil {
			t.Fatalf("read the indexes of jobqueue.jobs: %v", err)
		}
		got, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (string, error) {
			var name, method, predicate string
			var unique bool
			var columns []string
			if err := row.Scan(&name, &method, &unique, &columns, &predicate); err != nil {
				return "", err
			}
			return fmt.Sprintf("%s %s unique=%t (%s) WHERE %s", name, method, unique, strings.Join(columns, ", "), predicate), nil
		})
		if err != nil {
			t.Fatalf("scan the indexes of jobqueue.jobs: %v", err)
		}
		want := []string{
			"jobs_dead btree unique=false (queue, id) WHERE (state = 'dead'::text)",
			"jobs_finished btree unique=false (finished_at, id) WHERE (finished_at IS NOT NULL)",
			"jobs_idempotency btree unique=true (tenant, queue, idempotency_key) WHERE (idempotency_key IS NOT NULL)",
			"jobs_leased_expiry btree unique=false (lease_expires_at, id) WHERE (state = 'leased'::text)",
			"jobs_ready_fifo btree unique=false (queue, run_at, id) WHERE (state = 'ready'::text)",
			"jobs_ready_tenant btree unique=false (queue, tenant, run_at, id) WHERE (state = 'ready'::text)",
		}
		if !slices.Equal(got, want) {
			t.Fatalf("indexes of jobqueue.jobs, besides the primary key:\n got: %q\nwant: %q", got, want)
		}
	})

	insert := func(tenant, queue string, key *string) error {
		_, err := pool.Exec(ctx,
			`INSERT INTO jobqueue.jobs (queue, tenant, state, payload, run_at, attempts, max_attempts, idempotency_key, created_at)
			 VALUES ($1, $2, 'ready', '\x', jobqueue.now(), 0, 3, $3, jobqueue.now())`,
			queue, tenant, key)
		return err
	}
	key := "k1"

	t.Run("the same key twice under one tenant and queue is rejected", func(t *testing.T) {
		if err := insert("t", "q", &key); err != nil {
			t.Fatalf("first insert: %v", err)
		}
		err := insert("t", "q", &key)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) {
			t.Fatalf("second insert returned %v, want a PostgreSQL error", err)
		}
		if pgErr.Code != "23505" || pgErr.ConstraintName != "jobs_idempotency" {
			t.Fatalf("second insert failed with SQLSTATE %s on %q (%s), want 23505 on jobs_idempotency", pgErr.Code, pgErr.ConstraintName, pgErr.Message)
		}
	})

	t.Run("the same key under another tenant or queue is a different job", func(t *testing.T) {
		if err := insert("t2", "q", &key); err != nil {
			t.Fatalf("same key under another tenant: %v", err)
		}
		if err := insert("t", "q2", &key); err != nil {
			t.Fatalf("same key under another queue: %v", err)
		}
	})

	t.Run("jobs without a key never collide", func(t *testing.T) {
		for i := range 3 {
			if err := insert("t", "q", nil); err != nil {
				t.Fatalf("keyless insert %d: %v", i+1, err)
			}
		}
	})
}

// lockHolders counts the sessions holding the advisory lock that serializes Up.
func lockHolders(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	err := pool.QueryRow(t.Context(), `
		SELECT count(*) FROM pg_locks
		 WHERE locktype = 'advisory'
		   AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
		   AND objsubid = 1
		   AND ((classid::bigint << 32) | objid::bigint) = $1`, migrate.LockKey).Scan(&n)
	if err != nil {
		t.Fatalf("read pg_locks: %v", err)
	}
	return n
}

// TestPGJobsTableStorageOptions pins the storage parameters of the jobs table
// and of its TOAST table as they are after every migration, because the soak in
// docs/benchmarks.md depends on them: with autovacuum's cost throttle left on, a
// vacuum of the table took 47 s of the 60 s that autovacuum_naptime allows, and
// the table's size stopped being level.
func TestPGJobsTableStorageOptions(t *testing.T) {
	pool := pgtest.NewEmptyDB(t)
	ctx := t.Context()
	if _, err := migrate.Up(ctx, pool); err != nil {
		t.Fatalf("Up: %v", err)
	}
	options := func(query string) []string {
		t.Helper()
		var got []string
		if err := pool.QueryRow(ctx, query).Scan(&got); err != nil {
			t.Fatalf("read the storage parameters: %v", err)
		}
		slices.Sort(got)
		return got
	}
	check := func(what string, got, want []string) {
		t.Helper()
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("%s has the storage parameters %v, want %v", what, got, want)
		}
	}

	check("jobqueue.jobs",
		options(`SELECT coalesce(reloptions, '{}') FROM pg_class WHERE oid = 'jobqueue.jobs'::regclass`),
		[]string{
			"fillfactor=85",
			"autovacuum_vacuum_scale_factor=0.02",
			"autovacuum_vacuum_threshold=1000",
			"autovacuum_analyze_scale_factor=0.02",
			"autovacuum_vacuum_cost_delay=0",
		})
	check("the TOAST table of jobqueue.jobs",
		options(`SELECT coalesce(t.reloptions, '{}') FROM pg_class c JOIN pg_class t ON t.oid = c.reltoastrelid WHERE c.oid = 'jobqueue.jobs'::regclass`),
		[]string{"autovacuum_vacuum_scale_factor=0.05"})
}

func TestPGUpRollsBackAFailedMigrationAndReleasesTheLock(t *testing.T) {
	pool := pgtest.NewEmptyDB(t)
	ctx := t.Context()

	t.Run("the lock query sees a held lock", func(t *testing.T) {
		if n := lockHolders(t, pool); n != 0 {
			t.Fatalf("%d sessions hold the lock before anything ran", n)
		}
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		defer conn.Release()
		if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrate.LockKey); err != nil {
			t.Fatalf("take the lock: %v", err)
		}
		if n := lockHolders(t, pool); n != 1 {
			t.Fatalf("%d sessions hold the lock while one holds it, want 1: the query cannot tell", n)
		}
		if _, err := conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", migrate.LockKey); err != nil {
			t.Fatalf("release the lock: %v", err)
		}
	})

	good := migrate.NewMigration(1, "0001_probe.sql", "CREATE TABLE jobqueue.probe_one (id int)")
	// The first statement succeeds and the second fails, so the migration is
	// only atomic if the table it created is gone afterwards.
	bad := migrate.NewMigration(2, "0002_boom.sql", "CREATE TABLE jobqueue.probe_two (id int); SELECT 1/0")

	applied, err := migrate.UpWith(ctx, pool, good, bad)
	if err == nil {
		t.Fatal("UpWith succeeded although migration 2 divides by zero")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "22012" {
		t.Fatalf("UpWith returned %v, want an error wrapping SQLSTATE 22012 (division_by_zero)", err)
	}
	if !strings.Contains(err.Error(), "0002_boom.sql") {
		t.Fatalf("the error %q does not name the failing migration", err)
	}
	if applied != 1 {
		t.Fatalf("UpWith reported %d migrations applied, want 1 (the one that committed before the failure)", applied)
	}

	rows, err := pool.Query(ctx, "SELECT version FROM jobqueue.schema_migrations ORDER BY version")
	if err != nil {
		t.Fatalf("read schema_migrations: %v", err)
	}
	versions, err := pgx.CollectRows(rows, pgx.RowTo[int])
	if err != nil {
		t.Fatalf("scan schema_migrations: %v", err)
	}
	if !slices.Equal(versions, []int{1}) {
		t.Fatalf("schema_migrations holds versions %v after the failure, want [1]", versions)
	}

	for table, wantExists := range map[string]bool{"probe_one": true, "probe_two": false} {
		var exists bool
		if err := pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", "jobqueue."+table).Scan(&exists); err != nil {
			t.Fatalf("look up %s: %v", table, err)
		}
		if exists != wantExists {
			t.Fatalf("jobqueue.%s exists = %t after the failure, want %t: a migration must commit or roll back whole", table, exists, wantExists)
		}
	}

	if n := lockHolders(t, pool); n != 0 {
		t.Fatalf("%d sessions still hold the advisory lock after a failed UpWith", n)
	}

	// A second pool has its own sessions, so a lock leaked by the first pool's
	// connection would block it; a session that already held the lock would
	// take it again without waiting, which is why the same pool proves nothing.
	other, err := pgxpool.NewWithConfig(ctx, pool.Config())
	if err != nil {
		t.Fatalf("open a second pool: %v", err)
	}
	t.Cleanup(other.Close)
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	again, err := migrate.UpWith(bounded, other, good)
	if err != nil {
		t.Fatalf("UpWith on another connection: %v (a timeout here means the lock was not released)", err)
	}
	if again != 0 {
		t.Fatalf("UpWith on another connection applied %d migrations, want 0: version 1 was already recorded", again)
	}
}
