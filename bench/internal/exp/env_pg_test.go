//go:build pg

package exp

import (
	"testing"
)

func TestPGCaptureEnvReportsEachSettingAsPostgresShowsIt(t *testing.T) {
	dsn, pool := testDatabase(t)
	c, err := StartCluster(t.Context(), ClusterOptions{DatabaseURL: dsn, Brokers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	env := CaptureEnv(t.Context(), pool)
	if env["postgres_version"] == "" {
		t.Fatalf("no postgres_version: %v", env)
	}
	for _, name := range pgSettings {
		var shown string
		if err := pool.QueryRow(t.Context(), "SELECT current_setting($1)", name).Scan(&shown); err != nil {
			t.Fatalf("SHOW %s: %v", name, err)
		}
		// A reader of the committed raw output compares it with what the server
		// prints, so the value must be that, with its unit, and not the number
		// of pg_settings glued to the unit of a different size.
		if got := env["postgres_"+name]; got != shown {
			t.Errorf("postgres_%s = %q, want %q as SHOW prints it", name, got, shown)
		}
	}
	if opts := env["jobs_table_options"]; opts == "" || opts == "no jobs table" {
		t.Errorf("jobs_table_options = %q after the cluster migrated the schema, want the fillfactor and autovacuum options", opts)
	}
}

func TestPGCaptureEnvOnADatabaseWithoutTheSchema(t *testing.T) {
	_, pool := testDatabase(t) // empty: nothing migrated it
	env := CaptureEnv(t.Context(), pool)
	if env["jobs_table_options"] != "no jobs table" {
		t.Errorf("jobs_table_options = %q, want %q: the River experiment runs on a database without the jobs table", env["jobs_table_options"], "no jobs table")
	}
	for key := range env {
		if len(key) > 6 && key[len(key)-6:] == "_error" {
			t.Errorf("CaptureEnv recorded an error %s = %q for a healthy database", key, env[key])
		}
	}
}
