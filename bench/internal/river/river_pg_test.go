//go:build pg

package river

import (
	"net/url"
	"testing"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/bench/internal/exp"
	"github.com/jason-te-sde/distributed-job-queue/internal/pgtest"
)

func TestPGRiverRunsAtAFixedRateAndRemovesItsSchema(t *testing.T) {
	pool := pgtest.NewEmptyDB(t)
	u, err := url.Parse(pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + pool.Config().ConnConfig.Database

	var results []exp.Result
	err = Run(t.Context(), Options{DatabaseURL: u.String(), Rate: 200, Duration: 2 * time.Second}, func(r exp.Result) { results = append(results, r) })
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	r := results[0]
	if r.Experiment != "river" || r.Params["phase"] != "fixed" {
		t.Errorf("experiment %q phase %q, want river/fixed", r.Experiment, r.Params["phase"])
	}
	if r.Metrics["sent"] != 400 || r.Metrics["started"] != 400 {
		t.Errorf("sent %v, started %v; want all 400 jobs of 200/s for 2s to start", r.Metrics["sent"], r.Metrics["started"])
	}
	if r.Quantiles["p50"] <= 0 {
		t.Errorf("no latency quantiles: %v", r.Quantiles)
	}
	if r.Env["postgres_version"] == "" {
		t.Errorf("env lacks the database: %v", r.Env)
	}
	var left bool
	if err := pool.QueryRow(t.Context(), "SELECT to_regnamespace($1) IS NOT NULL", schema).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left {
		t.Errorf("the schema %s is still there after the run", schema)
	}
}
