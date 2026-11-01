//go:build pg

package exp

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPGThroughputAtAFixedRateOverThreeBrokers(t *testing.T) {
	dsn, pool := testDatabase(t)
	emit, results := collectResults()
	err := Throughput(t.Context(), ThroughputOptions{
		Setup: Setup{DatabaseURL: dsn, Brokers: 3}, Rate: 200, Duration: 2 * time.Second, Workers: 2, Concurrency: 16,
	}, emit)
	if err != nil {
		t.Fatalf("Throughput: %v", err)
	}
	if len(*results) != 1 {
		t.Fatalf("got %d results, want 1: %+v", len(*results), *results)
	}
	r := (*results)[0]
	if r.Experiment != "throughput" || r.Params["phase"] != "fixed" || r.Params["brokers"] != "3" {
		t.Errorf("experiment/phase/brokers = %q/%q/%q, want throughput/fixed/3", r.Experiment, r.Params["phase"], r.Params["brokers"])
	}
	if r.Metrics["sent"] != 400 || r.Metrics["started"] != 400 {
		t.Errorf("sent %v, started %v; want 400 jobs of 200/s for 2s all started", r.Metrics["sent"], r.Metrics["started"])
	}
	if _, ok := r.Metrics["duplicate_starts"]; !ok {
		t.Errorf("the result does not report duplicate_starts: %v", r.Metrics)
	}
	// Whether the rate was sustained, and the latencies, depend on the machine;
	// this test runs on CI's small runners, so it asserts only that every job that
	// was sent started and that the latency was recorded.
	if r.Quantiles["p50"] <= 0 {
		t.Errorf("p50 = %v, want a recorded latency", r.Quantiles["p50"])
	}
	if r.Env["postgres_version"] == "" || r.Env["postgres_synchronous_commit"] == "" || r.Env["cpu_model"] == "" {
		t.Errorf("env lacks the machine or the database: %v", r.Env)
	}
	if got := countRows(t, pool); got != 0 {
		t.Errorf("the experiment left %d rows in the jobs table, want it to clean up after itself", got)
	}
}

func TestPGThroughputFindMaxRunsEveryPhase(t *testing.T) {
	dsn, _ := testDatabase(t)
	emit, results := collectResults()
	err := Throughput(t.Context(), ThroughputOptions{
		Setup: Setup{DatabaseURL: dsn}, FindMax: true, Lo: 50, Hi: 400, Steps: 2, Duration: time.Second, Workers: 2, Concurrency: 16,
	}, emit)
	if err != nil {
		t.Fatalf("Throughput: %v", err)
	}
	phases := map[string]int{}
	for _, r := range *results {
		phases[r.Params["phase"]]++
	}
	if phases["probe"] != 3 || phases["max"] != 1 || phases["load-50"] != 1 || phases["load-80"] != 1 {
		t.Errorf("phases = %v, want 3 probes, one max, one load-50 and one load-80", phases)
	}
}

func TestPGThroughputCancelledMidProbeLeavesNoRows(t *testing.T) {
	dsn, pool := testDatabase(t)
	slowCommits(t, pool, 300*time.Millisecond)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Throughput(ctx, ThroughputOptions{
			Setup: Setup{DatabaseURL: dsn}, Rate: 100, Duration: time.Minute, Workers: 1, Concurrency: 8,
		}, func(Result) {})
	}()
	awaitRows(t, pool)
	cancel() // some thirty enqueues are in flight, each committing 300ms from now at the latest

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Throughput error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Minute):
		t.Fatal("Throughput did not return within a minute of its context being cancelled")
	}
	if got := countRows(t, pool); got != 0 {
		t.Errorf("%d rows in the jobs table after Throughput returned, want 0: the commits in flight at the cancel landed after the clean-up", got)
	}
}
