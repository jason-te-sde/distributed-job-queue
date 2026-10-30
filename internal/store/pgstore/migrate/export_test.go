package migrate

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Migration is a migration as Up applies it. Tests build their own to see how
// Up treats one that fails, which no embedded migration may do.
type Migration = migration

// NewMigration returns a migration with the given version, file name and SQL.
func NewMigration(version int, name, sql string) Migration {
	return migration{version: version, name: name, sql: sql}
}

// UpWith is Up over the given migrations instead of the embedded ones. They
// must be in ascending version order.
func UpWith(ctx context.Context, pool *pgxpool.Pool, migrations ...Migration) (int, error) {
	return up(ctx, pool, migrations)
}
