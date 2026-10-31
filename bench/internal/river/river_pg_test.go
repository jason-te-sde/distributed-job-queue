//go:build pg

package river

import (
	"context"
	"errors"
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

func TestPGRiverCancelledMidProbeStillRemovesItsSchema(t *testing.T) {
	pool := pgtest.NewEmptyDB(t)
	u, err := url.Parse(pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + pool.Config().ConnConfig.Database

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	// A probe of half a minute, interrupted as soon as it is inserting jobs.
	go func() {
		done <- Run(ctx, Options{DatabaseURL: u.String(), Rate: 200, Duration: 30 * time.Second}, func(exp.Result) {})
	}()

	deadline := time.Now().Add(30 * time.Second)
	inserting := func() bool {
		var exists bool
		if err := pool.QueryRow(t.Context(), "SELECT to_regclass($1) IS NOT NULL", schema+".river_job").Scan(&exists); err != nil || !exists {
			return false
		}
		var n int
		return pool.QueryRow(t.Context(), "SELECT count(*) FROM "+schema+".river_job").Scan(&n) == nil && n > 0
	}
	for !inserting() {
		if time.Now().After(deadline) {
			t.Fatal("the probe had inserted no job after 30s")
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Minute):
		t.Fatal("Run did not return after its context was cancelled")
	}
	var left bool
	if err := pool.QueryRow(t.Context(), "SELECT to_regnamespace($1) IS NOT NULL", schema).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left {
		t.Errorf("the schema %s is still there after a cancelled run: the clean-up must not depend on the run's context", schema)
	}
}
