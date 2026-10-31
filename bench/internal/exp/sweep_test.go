package exp

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/bench/internal/hist"
)

// fakeProbe is a system that sustains any rate under limit. Its outcome at a
// higher rate has a p99 of 3s.
func fakeProbe(limit float64, rates *[]float64) ProbeFunc {
	return func(_ context.Context, rate float64, _, drainWait time.Duration) (*Outcome, error) {
		*rates = append(*rates, rate)
		h := hist.New()
		for range 100 {
			if rate < limit {
				h.Record(5 * time.Millisecond)
			} else {
				h.Record(3 * time.Second)
			}
		}
		return &Outcome{Offered: rate, Sent: 100, Started: 100, Complete: true, Hist: h, LoadSeconds: 1}, nil
	}
}

func collect() (Emit, *[]Result) {
	var got []Result
	return func(r Result) { got = append(got, r) }, &got
}

func byPhase(results []Result, phase string) []Result {
	var out []Result
	for _, r := range results {
		if r.Params["phase"] == phase {
			out = append(out, r)
		}
	}
	return out
}

func TestSweepFindMaxMeasuresAtHalfAndFourFifthsOfTheMaximum(t *testing.T) {
	var rates []float64
	emit, results := collect()
	cfg := SweepConfig{
		Experiment: "throughput", Params: map[string]string{"brokers": "1"}, Env: map[string]string{"os": "test"},
		FindMax: true, Lo: 100, Hi: 10000, Steps: 8, Duration: time.Second,
	}
	if err := Sweep(t.Context(), cfg, fakeProbe(1234, &rates), emit); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	maxes := byPhase(*results, "max")
	if len(maxes) != 1 {
		t.Fatalf("got %d results of phase max, want 1: %d results in all", len(maxes), len(*results))
	}
	maxRate := maxes[0].Metrics["max_rate"]
	if maxRate <= 0 || maxRate > 1234 || maxRate < 1234*0.9 {
		t.Errorf("max_rate = %v, want a sustained rate close under 1234", maxRate)
	}
	if got := byPhase(*results, "probe"); len(got) != 9 {
		t.Errorf("got %d probe results, want 9: the low end and 8 steps", len(got))
	}
	for phase, fraction := range map[string]float64{"load-50": 0.5, "load-80": 0.8} {
		got := byPhase(*results, phase)
		if len(got) != 1 {
			t.Fatalf("got %d results of phase %s, want 1", len(got), phase)
		}
		if want := maxRate * fraction; got[0].Metrics["offered_rate"] != want {
			t.Errorf("%s offered %v, want %v: %v of the maximum", phase, got[0].Metrics["offered_rate"], want, fraction)
		}
		for _, key := range []string{"p50", "p99", "p99.9"} {
			if got[0].Quantiles[key] <= 0 {
				t.Errorf("%s has no %s: %v", phase, key, got[0].Quantiles)
			}
		}
	}
	for i, r := range *results {
		if r.Experiment != "throughput" || r.Params["brokers"] != "1" || r.Env["os"] != "test" {
			t.Errorf("result %d lost the experiment, params or env: %+v", i, r)
		}
		if r.Started.IsZero() {
			t.Errorf("result %d has no start time", i)
		}
	}
	// The last three results are the maximum and the two measured loads.
	n := len(*results)
	if (*results)[n-3].Params["phase"] != "max" || (*results)[n-2].Params["phase"] != "load-50" || (*results)[n-1].Params["phase"] != "load-80" {
		t.Errorf("the last three phases are %q, %q, %q; want max, load-50, load-80",
			(*results)[n-3].Params["phase"], (*results)[n-2].Params["phase"], (*results)[n-1].Params["phase"])
	}
}

func TestSweepFixedRateRunsOnce(t *testing.T) {
	var rates []float64
	emit, results := collect()
	cfg := SweepConfig{Experiment: "throughput", Rate: 750, Duration: time.Second}
	if err := Sweep(t.Context(), cfg, fakeProbe(1000, &rates), emit); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(rates) != 1 || rates[0] != 750 {
		t.Errorf("probed %v, want exactly the one rate 750", rates)
	}
	if len(*results) != 1 || (*results)[0].Params["phase"] != "fixed" {
		t.Fatalf("results = %+v, want one result of phase fixed", *results)
	}
	if got := (*results)[0].Params["rate"]; got != "750" {
		t.Errorf("Params[rate] = %q, want 750", got)
	}
}

func TestSweepUsesTheShortDrainWaitToJudgeAndALongOneToMeasure(t *testing.T) {
	var waits []time.Duration
	probe := func(_ context.Context, rate float64, _, drainWait time.Duration) (*Outcome, error) {
		waits = append(waits, drainWait)
		h := hist.New()
		h.Record(time.Millisecond)
		return &Outcome{Offered: rate, Sent: 1, Started: 1, Complete: true, Hist: h, LoadSeconds: 1}, nil
	}
	emit, _ := collect()
	cfg := SweepConfig{Experiment: "x", FindMax: true, Lo: 10, Hi: 100, Steps: 2, Duration: time.Second}
	// Everything is sustained, so the sweep reports a lower bound.
	if err := Sweep(t.Context(), cfg, probe, emit); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	// 3 probes to judge, 2 measurements.
	if len(waits) != 5 {
		t.Fatalf("%d probes, want 5", len(waits))
	}
	for i, w := range waits[:3] {
		if w != SustainedDrain {
			t.Errorf("probe %d waited %v for the backlog, want %v: that is the bar", i, w, SustainedDrain)
		}
	}
	for i, w := range waits[3:] {
		if w <= SustainedDrain {
			t.Errorf("measurement %d waited %v for the backlog, want longer than the bar so that every sample is counted", i, w)
		}
	}
}

func TestSweepFlagsAMaximumThatIsOnlyALowerBound(t *testing.T) {
	var rates []float64
	emit, results := collect()
	cfg := SweepConfig{Experiment: "x", FindMax: true, Lo: 10, Hi: 100, Steps: 3, Duration: time.Second}
	if err := Sweep(t.Context(), cfg, fakeProbe(1e9, &rates), emit); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	maxes := byPhase(*results, "max")
	if len(maxes) != 1 || maxes[0].Metrics["max_is_lower_bound"] != 1 {
		t.Errorf("max results = %+v, want one with max_is_lower_bound 1: no probe failed, so the range was too low", maxes)
	}
}

func TestSweepStopsWhenTheLowEndFails(t *testing.T) {
	var rates []float64
	emit, results := collect()
	cfg := SweepConfig{Experiment: "x", FindMax: true, Lo: 100, Hi: 1000, Steps: 3, Duration: time.Second}
	err := Sweep(t.Context(), cfg, fakeProbe(50, &rates), emit)
	if !errors.Is(err, ErrLowEndFailed) {
		t.Fatalf("Sweep error = %v, want ErrLowEndFailed", err)
	}
	if got := byPhase(*results, "max"); len(got) != 0 {
		t.Errorf("a sweep whose low end failed reported a maximum: %+v", got)
	}
	if got := byPhase(*results, "probe"); len(got) != 1 {
		t.Errorf("%d probe results, want the one that failed to be on record", len(got))
	}
}

func TestSweepProbeErrorIsAnError(t *testing.T) {
	boom := errors.New("boom")
	probe := func(context.Context, float64, time.Duration, time.Duration) (*Outcome, error) { return nil, boom }
	emit, _ := collect()
	if err := Sweep(t.Context(), SweepConfig{Experiment: "x", Rate: 10, Duration: time.Second}, probe, emit); !errors.Is(err, boom) {
		t.Errorf("Sweep error = %v, want boom", err)
	}
}

func TestSweepRejectsBadConfigurations(t *testing.T) {
	probe := func(context.Context, float64, time.Duration, time.Duration) (*Outcome, error) {
		t.Error("probe was called")
		return nil, nil
	}
	emit, _ := collect()
	for name, cfg := range map[string]SweepConfig{
		"no rate and no find-max": {Experiment: "x", Duration: time.Second},
		"rate and find-max":       {Experiment: "x", Rate: 5, FindMax: true, Lo: 1, Hi: 10, Steps: 2, Duration: time.Second},
		"no duration":             {Experiment: "x", Rate: 5},
		"negative rate":           {Experiment: "x", Rate: -5, Duration: time.Second},
	} {
		if err := Sweep(t.Context(), cfg, probe, emit); err == nil {
			t.Errorf("%s: Sweep returned no error", name)
		}
	}
}

func TestOutcomeMetrics(t *testing.T) {
	h := hist.New()
	h.Record(10 * time.Millisecond)
	o := &Outcome{Offered: 200, Sent: 100, Late: 3, Started: 99, Complete: false, Hist: h, LoadSeconds: 0.5, DrainSeconds: 2, ThroughputPerSec: 190}
	m := o.Metrics()
	for key, want := range map[string]float64{
		"offered_rate": 200, "achieved_rate": 200, "sent": 100, "started": 99, "late_sends": 3,
		"drain_seconds": 2, "throughput_per_s": 190, "sustained": 0, "complete": 0,
	} {
		if got, ok := m[key]; !ok || got != want {
			t.Errorf("Metrics()[%q] = %v (present %v), want %v", key, got, ok, want)
		}
	}
	if got := strconv.FormatFloat(m["late_sends"], 'f', -1, 64); got != "3" {
		t.Errorf("late_sends = %s", got)
	}
}
