//go:build pg

package exp

import (
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
	if r.Metrics["sustained"] != 1 {
		t.Errorf("200/s over three brokers on an idle database was not sustained: %+v", r)
	}
	if p50 := r.Quantiles["p50"]; p50 <= 0 || p50 > time.Second {
		t.Errorf("p50 = %v, want a positive latency under 1s", p50)
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
