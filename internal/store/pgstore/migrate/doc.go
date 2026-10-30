// Package migrate applies the embedded, forward-only schema migrations that
// the Postgres store needs, and checks that a database is at the version the
// running binary expects.
//
// Migrations are the files in the sql directory, named NNNN_description.sql
// and numbered from 0001 without gaps. Up applies the missing ones in
// ascending order, each in its own transaction together with its row in
// jobqueue.schema_migrations, while it holds a session-level advisory lock, so
// concurrent callers apply each migration exactly once. There is no Down: a
// schema change that must be undone is a new migration. Check is the
// start-up guard that refuses to run against a database at another version.
package migrate
