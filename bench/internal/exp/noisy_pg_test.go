//go:build pg

package exp

import (
	"testing"
	"time"
)

func TestPGNoisyNeighborFIFOStarvesBAndDRRDoesNot(t *testing.T) {
	waits := map[string]Result{}
	for _, scheduler := range []string{"fifo", "drr"} {
		dsn, pool := testDatabase(t)
		emit, results := collectResults()
		err := Noisy(t.Context(), NoisyOptions{
			Setup: Setup{DatabaseURL: dsn}, Scheduler: scheduler,
			// Capacity is 2 slots over 2ms, 1000 jobs a second, so 3000 flooding
			// jobs take three seconds to drain.
			Flood: 3000, BRate: 20, Duration: 3 * time.Second, ServiceTime: 2 * time.Millisecond, Workers: 1, Concurrency: 2,
		}, emit)
		if err != nil {
			t.Fatalf("Noisy(%s): %v", scheduler, err)
		}
		if len(*results) != 1 {
			t.Fatalf("Noisy(%s) emitted %d results, want 1", scheduler, len(*results))
		}
		r := (*results)[0]
		if r.Experiment != "noisy" || r.Params["scheduler"] != scheduler {
			t.Errorf("experiment %q scheduler %q, want noisy/%s", r.Experiment, r.Params["scheduler"], scheduler)
		}
		if r.Metrics["a_jobs"] != 3000 || r.Metrics["b_jobs"] != 60 {
			t.Errorf("%s: a_jobs %v, b_jobs %v; want 3000 and 60", scheduler, r.Metrics["a_jobs"], r.Metrics["b_jobs"])
		}
		if len(r.CDF) < 50 {
			t.Errorf("%s: the result carries %d CDF points, want a curve to plot", scheduler, len(r.CDF))
		}
		if got := countRows(t, pool); got != 0 {
			t.Errorf("%s: %d rows left in the jobs table", scheduler, got)
		}
		waits[scheduler] = r
	}
	fifo, drr := waits["fifo"].Quantiles, waits["drr"].Quantiles
	if fifo["p99"] < 500*time.Millisecond {
		t.Errorf("under FIFO tenant B's p99 wait is %v, want over 500ms behind a three second flood: the flood did not bite", fifo["p99"])
	}
	if drr["p99"]*4 > fifo["p99"] {
		t.Errorf("tenant B's p99 wait is %v under DRR and %v under FIFO, want DRR at least 4 times better", drr["p99"], fifo["p99"])
	}
}
