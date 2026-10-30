package migrate

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Version is the highest embedded migration. It equals the number of files in
// the sql directory, which TestMigrationsAreContiguous enforces.
const Version = 1

// LockKey is the pg_advisory_lock key that serializes Up across processes. It
// is the ASCII bytes of "jobqueue".
const LockKey int64 = 0x6a6f627175657565

// SQLSTATEs that mean "the migration bookkeeping does not exist yet".
const (
	sqlstateUndefinedTable    = "42P01"
	sqlstateInvalidSchemaName = "3F000"
)

// unlockTimeout bounds the release of the advisory lock after the caller's
// context may already be done.
const unlockTimeout = 5 * time.Second

const bootstrapSQL = `
CREATE SCHEMA IF NOT EXISTS jobqueue;
CREATE TABLE IF NOT EXISTS jobqueue.schema_migrations (
    version int PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);`

//go:embed sql/*.sql
var sqlFiles embed.FS

// SchemaMismatchError reports a database whose schema version differs from the
// one the binary was built for.
type SchemaMismatchError struct {
	// Have is the highest version recorded in the database, 0 when none is.
	Have int
	// Want is the version the binary needs, which is Version.
	Want int
}

// Error implements error.
func (e *SchemaMismatchError) Error() string {
	return fmt.Sprintf("database schema version %d, this binary needs %d: run jobqueuectl migrate", e.Have, e.Want)
}

// Files returns the names of the embedded migrations in the order Up applies
// them, for example ["0001_init.sql"].
func Files() []string {
	entries, err := fs.ReadDir(sqlFiles, "sql")
	if err != nil {
		// The directory is embedded at build time and cannot be missing.
		panic("migrate.Files: the embedded sql directory is unreadable: " + err.Error())
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

type migration struct {
	version int
	name    string
	sql     string
}

// load reads every embedded migration, in ascending version order.
func load() ([]migration, error) {
	names := Files()
	out := make([]migration, 0, len(names))
	for _, name := range names {
		if len(name) < 4 {
			return nil, fmt.Errorf("migrate: migration file %q has no four-digit version prefix", name)
		}
		version, err := strconv.Atoi(name[:4])
		if err != nil {
			return nil, fmt.Errorf("migrate: migration file %q has no four-digit version prefix: %w", name, err)
		}
		body, err := fs.ReadFile(sqlFiles, "sql/"+name)
		if err != nil {
			return nil, fmt.Errorf("migrate: read %s: %w", name, err)
		}
		out = append(out, migration{version: version, name: name, sql: string(body)})
	}
	return out, nil
}

// Up applies every embedded migration that the database lacks and returns how
// many it applied; a database that is up to date applies none.
//
// It takes one connection from pool and holds pg_advisory_lock(LockKey) on it
// while it bootstraps the jobqueue schema and jobqueue.schema_migrations and
// then applies the missing versions in ascending order. Each migration runs in
// its own transaction together with its schema_migrations row, so a failure
// leaves the database at the last version that fully applied, and the returned
// count says how many that was. Callers that race wait on the lock and find
// nothing left to apply.
func Up(ctx context.Context, pool *pgxpool.Pool) (applied int, err error) {
	migrations, err := load()
	if err != nil {
		return 0, err
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("migrate: acquire a connection: %w", err)
	}
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", LockKey); err != nil {
		discard(conn)
		return 0, fmt.Errorf("migrate: take the advisory lock: %w", err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), unlockTimeout)
		defer cancel()
		var unlocked bool
		uerr := conn.QueryRow(cleanup, "SELECT pg_advisory_unlock($1)", LockKey).Scan(&unlocked)
		if uerr == nil && !unlocked {
			uerr = errors.New("the session did not hold the lock")
		}
		if uerr != nil {
			// A session-level lock outlives this call. A connection that may still
			// hold it must not return to the pool, where it would block every
			// later caller; closing it ends the session and frees the lock.
			discard(conn)
			err = errors.Join(err, fmt.Errorf("migrate: release the advisory lock: %w", uerr))
			return
		}
		conn.Release()
	}()

	if _, err := conn.Exec(ctx, bootstrapSQL); err != nil {
		return 0, fmt.Errorf("migrate: create the migration bookkeeping: %w", err)
	}
	have, err := appliedVersions(ctx, conn)
	if err != nil {
		return 0, err
	}
	for _, m := range migrations {
		if have[m.version] {
			continue
		}
		if err := apply(ctx, conn, m); err != nil {
			return applied, err
		}
		applied++
	}
	return applied, nil
}

// discard closes a pool connection instead of returning it to the pool.
func discard(conn *pgxpool.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), unlockTimeout)
	defer cancel()
	_ = conn.Hijack().Close(ctx)
}

func appliedVersions(ctx context.Context, conn *pgxpool.Conn) (map[int]bool, error) {
	rows, err := conn.Query(ctx, "SELECT version FROM jobqueue.schema_migrations")
	if err != nil {
		return nil, fmt.Errorf("migrate: read the applied versions: %w", err)
	}
	versions, err := pgx.CollectRows(rows, pgx.RowTo[int])
	if err != nil {
		return nil, fmt.Errorf("migrate: read the applied versions: %w", err)
	}
	have := make(map[int]bool, len(versions))
	for _, v := range versions {
		have[v] = true
	}
	return have, nil
}

// apply runs one migration and records it in the same transaction.
func apply(ctx context.Context, conn *pgxpool.Conn, m migration) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("migrate: begin %s: %w", m.name, err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, m.sql); err != nil {
		return fmt.Errorf("migrate: apply %s: %w", m.name, err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO jobqueue.schema_migrations (version) VALUES ($1)", m.version); err != nil {
		return fmt.Errorf("migrate: record %s: %w", m.name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("migrate: commit %s: %w", m.name, err)
	}
	return nil
}

// Check returns nil when the highest version recorded in the database is
// Version, and a *SchemaMismatchError otherwise. A database without the
// bookkeeping table is at version 0.
func Check(ctx context.Context, pool *pgxpool.Pool) error {
	var have int
	err := pool.QueryRow(ctx, "SELECT coalesce(max(version), 0) FROM jobqueue.schema_migrations").Scan(&have)
	if err != nil {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || (pgErr.Code != sqlstateUndefinedTable && pgErr.Code != sqlstateInvalidSchemaName) {
			return fmt.Errorf("migrate: read the schema version: %w", err)
		}
		have = 0
	}
	if have != Version {
		return &SchemaMismatchError{Have: have, Want: Version}
	}
	return nil
}
