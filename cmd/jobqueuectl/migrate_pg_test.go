//go:build pg

package main

import (
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jason-te-sde/distributed-job-queue/internal/pgtest"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/pgstore/migrate"
)

// databaseURL returns a URL for the private database that pool is connected
// to: the test server's URL with the database name replaced.
func databaseURL(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	u, err := url.Parse(pgtest.DSN(t))
	if err != nil {
		t.Fatalf("parse JOBQUEUE_TEST_DSN: %v", err)
	}
	u.Path = "/" + pool.Config().ConnConfig.Database
	return u.String()
}

func TestPGCtlMigrate(t *testing.T) {
	pool := pgtest.NewEmptyDB(t)
	dsn := databaseURL(t, pool)
	version := strconv.Itoa(migrate.Version)

	// A fresh database gets every migration, which is what the database is
	// then asked to confirm.
	r := runCtl(nil, "migrate", "--database-url", dsn)
	if want := "applied=" + version + " version=" + version + "\n"; r.code != 0 || r.stdout != want || r.stderr != "" {
		t.Fatalf("first migrate = exit %d, stdout %q, stderr %q; want exit 0 and %q", r.code, r.stdout, r.stderr, want)
	}
	var recorded int
	if err := pool.QueryRow(t.Context(), "SELECT max(version) FROM jobqueue.schema_migrations").Scan(&recorded); err != nil {
		t.Fatal(err)
	}
	if recorded != migrate.Version {
		t.Fatalf("the database is at version %d after migrate, want %d", recorded, migrate.Version)
	}
	if err := migrate.Check(t.Context(), pool); err != nil {
		t.Fatalf("the migrated database fails the server's start-up check: %v", err)
	}

	// The second run has nothing to do. It takes the URL from the environment.
	env := map[string]string{"JOBQUEUE_DATABASE_URL": dsn}
	r = runCtl(env, "migrate")
	if want := "applied=0 version=" + version + "\n"; r.code != 0 || r.stdout != want || r.stderr != "" {
		t.Fatalf("second migrate = exit %d, stdout %q, stderr %q; want exit 0 and %q", r.code, r.stdout, r.stderr, want)
	}

	r = runCtl(env, "migrate", "--json")
	if want := `{"applied":0,"version":` + version + "}\n"; r.code != 0 || r.stdout != want {
		t.Fatalf("migrate --json = exit %d, stdout %q; want %q", r.code, r.stdout, want)
	}
}

// TestPGCtlMigrateRefusesADatabaseAheadOfTheBinary: a database that a newer
// binary migrated has nothing for Up to apply, and still must not be reported
// as migrated for this one.
func TestPGCtlMigrateRefusesADatabaseAheadOfTheBinary(t *testing.T) {
	pool := pgtest.NewEmptyDB(t)
	dsn := databaseURL(t, pool)
	if r := runCtl(nil, "migrate", "--database-url", dsn); r.code != 0 {
		t.Fatalf("first migrate = exit %d, stderr %q", r.code, r.stderr)
	}
	ahead := migrate.Version + 1
	if _, err := pool.Exec(t.Context(), "INSERT INTO jobqueue.schema_migrations (version) VALUES ($1)", ahead); err != nil {
		t.Fatal(err)
	}

	r := runCtl(nil, "migrate", "--database-url", dsn)
	want := "schema version " + strconv.Itoa(ahead)
	if r.code != 1 || r.stdout != "" || !strings.Contains(r.stderr, want) || !strings.Contains(r.stderr, "ahead") {
		t.Fatalf("migrate of a database at version %d = exit %d, stdout %q, stderr %q; want exit 1 naming %q and that it is ahead",
			ahead, r.code, r.stdout, r.stderr, want)
	}
}
