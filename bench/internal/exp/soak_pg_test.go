//go:build pg

package exp

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestPGSoakSamplesEveryIntervalAndPrunes(t *testing.T) {
	dsn, _ := testDatabase(t)
	emit, results := collectResults()
	err := Soak(t.Context(), SoakOptions{
		Setup: Setup{DatabaseURL: dsn}, Rate: 200, Duration: 6 * time.Second, Interval: time.Second,
		Retention: time.Second, PruneInterval: time.Second, Workers: 2, Concurrency: 16,
	}, emit)
	if err != nil {
		t.Fatalf("Soak: %v", err)
	}
	var samples, summaries []Result
	for _, r := range *results {
		switch r.Params["phase"] {
		case "sample":
			samples = append(samples, r)
		case "summary":
			summaries = append(summaries, r)
		}
	}
	if len(samples) < 5 || len(summaries) != 1 {
		t.Fatalf("got %d samples and %d summaries from 6 intervals of 1s, want at least 5 and 1", len(samples), len(summaries))
	}
	for i, s := range samples {
		if s.Metrics["table_bytes"] <= 0 || s.Metrics["throughput_per_s"] <= 0 {
			t.Errorf("sample %d lacks the table size or throughput: %v", i, s.Metrics)
		}
		if s.Params["retention"] != "1s" {
			t.Errorf("sample %d states retention %q, want 1s: the run must say its pruning is shortened", i, s.Params["retention"])
		}
	}
	sum := summaries[0]
	if sum.Metrics["sent"] != 1200 || sum.Metrics["started"] != 1200 {
		t.Errorf("summary sent %v, started %v; want 1200 each", sum.Metrics["sent"], sum.Metrics["started"])
	}
	// With a retention of a second and a pruner that runs every second, a job is
	// deleted at most about two seconds after it finished, so at 200 jobs a
	// second the table holds some 400 rows however many have been started.
	// Without pruning it would hold every job that was enqueued.
	last := samples[len(samples)-1]
	if rows, started := last.Metrics["rows"], last.Metrics["started_total"]; started < 600 || rows >= 0.8*started {
		t.Errorf("the last sample counts %v rows with %v jobs started, want pruning to have removed over a fifth of them", rows, started)
	}
	if sum.Metrics["table_bytes_max"] < sum.Metrics["table_bytes_final"] || sum.Metrics["table_bytes_final"] <= 0 {
		t.Errorf("summary table sizes: %v", sum.Metrics)
	}
}

func TestPGSoakStopsAtOnceWhenASampleFails(t *testing.T) {
	dsn, pool := testDatabase(t)
	boom := errors.New("the table cannot be read")
	var calls atomic.Int32
	opts := SoakOptions{
		Setup: Setup{DatabaseURL: dsn}, Rate: 50, Interval: 300 * time.Millisecond, Workers: 1, Concurrency: 8,
		// A soak of ten minutes, whose second sample fails.
		Duration: 10 * time.Minute,
		statsFn: func(ctx context.Context, r *rig) (tableStats, error) {
			if calls.Add(1) >= 2 {
				return tableStats{}, boom
			}
			return r.tableStats(ctx)
		},
	}
	done := make(chan error, 1)
	go func() { done <- Soak(t.Context(), opts, func(Result) {}) }()

	select {
	case err := <-done:
		if !errors.Is(err, boom) {
			t.Errorf("Soak error = %v, want it to wrap the failure of the sample", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("Soak was still running 60s after a sample failed, in a soak of ten minutes: the probe was not stopped")
	}
	if got := countRows(t, pool); got != 0 {
		t.Errorf("%d rows left in the jobs table after the failed soak, want it cleaned up", got)
	}
}
