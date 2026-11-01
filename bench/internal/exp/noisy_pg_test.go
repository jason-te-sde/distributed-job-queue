//go:build pg

package exp

import (
	"context"
	"errors"
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
		if _, ok := r.Metrics["duplicate_starts"]; !ok {
			t.Errorf("%s: the result does not report duplicate_starts: %v", scheduler, r.Metrics)
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

func TestPGNoisyCancelledMidFloodLeavesNoRows(t *testing.T) {
	dsn, pool := testDatabase(t)
	slowCommits(t, pool, 300*time.Millisecond)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Noisy(ctx, NoisyOptions{
			Setup: Setup{DatabaseURL: dsn}, Scheduler: "drr", Flood: 100000, BRate: 20, Duration: time.Minute,
			ServiceTime: 2 * time.Millisecond, Workers: 1, Concurrency: 2,
		}, func(Result) {})
	}()
	awaitRows(t, pool)
	cancel() // batches of 64 are in flight, each committing 300ms from now at the latest

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Noisy error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Minute):
		t.Fatal("Noisy did not return within a minute of its context being cancelled")
	}
	if got := countRows(t, pool); got != 0 {
		t.Errorf("%d rows in the jobs table after Noisy returned, want 0: the batches in flight at the cancel landed after the clean-up", got)
	}
}
