//go:build pg

package exp

import (
	"testing"
	"time"
)

func byVariant(results []Result) map[string]Result {
	out := map[string]Result{}
	for _, r := range results {
		out[r.Params["variant"]] = r
	}
	return out
}

func TestPGAblateBatchAndCoalesceRunBothVariants(t *testing.T) {
	for what, names := range map[string][2]string{
		"batch":    {"max_jobs_1", "max_jobs_64"},
		"coalesce": {"ack_batch_1", "ack_batch_128"},
	} {
		t.Run(what, func(t *testing.T) {
			dsn, _ := testDatabase(t)
			emit, results := collectResults()
			err := Ablate(t.Context(), AblateOptions{What: what, ThroughputOptions: ThroughputOptions{
				Setup: Setup{DatabaseURL: dsn}, Rate: 200, Duration: time.Second, Workers: 2, Concurrency: 16,
			}}, emit)
			if err != nil {
				t.Fatalf("Ablate(%s): %v", what, err)
			}
			got := byVariant(*results)
			for _, name := range names {
				r, ok := got[name]
				if !ok {
					t.Fatalf("no result for the variant %q: %v", name, got)
				}
				if r.Experiment != "ablate" || r.Params["what"] != what {
					t.Errorf("variant %s: experiment %q what %q", name, r.Experiment, r.Params["what"])
				}
				if r.Metrics["started"] != r.Metrics["sent"] || r.Metrics["sent"] != 200 {
					t.Errorf("variant %s: sent %v, started %v; want all 200 started", name, r.Metrics["sent"], r.Metrics["started"])
				}
			}
		})
	}
}

func TestPGAblateNotifyShowsWhatTheListenerBuys(t *testing.T) {
	dsn, _ := testDatabase(t)
	emit, results := collectResults()
	err := Ablate(t.Context(), AblateOptions{What: "notify", ThroughputOptions: ThroughputOptions{
		Setup: Setup{DatabaseURL: dsn}, Rate: 20, Duration: 4 * time.Second, Workers: 1, Concurrency: 8,
	}}, emit)
	if err != nil {
		t.Fatalf("Ablate(notify): %v", err)
	}
	got := byVariant(*results)
	on, off := got["listen_on"], got["listen_off"]
	if on.Metrics["started"] != 80 || off.Metrics["started"] != 80 {
		t.Fatalf("started: on %v, off %v; want 80 each", on.Metrics["started"], off.Metrics["started"])
	}
	// With NOTIFY an idle queue wakes its waiter at once. Without it the waiter
	// finds the job at the next fallback poll, up to a second later, so the
	// median is of the order of half a second.
	if off.Quantiles["p50"] < 100*time.Millisecond {
		t.Errorf("without NOTIFY p50 = %v, want over 100ms: the fallback poll is a second", off.Quantiles["p50"])
	}
	if on.Quantiles["p50"]*5 > off.Quantiles["p50"] {
		t.Errorf("p50 with NOTIFY %v and without %v, want the listener to be at least 5 times faster", on.Quantiles["p50"], off.Quantiles["p50"])
	}
	if on.Params["disable_listen"] != "false" || off.Params["disable_listen"] != "true" {
		t.Errorf("params disable_listen: on %q, off %q", on.Params["disable_listen"], off.Params["disable_listen"])
	}
}
