package exp

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"strconv"
	"time"
)

// MeasureDrainWait is how long a measurement, as opposed to a judgement of
// whether a rate is sustained, waits for the backlog to start, so that every
// sample is counted.
const MeasureDrainWait = 30 * time.Second

// ProbeFunc runs one probe of a system at a rate: it sets the system up, offers
// the load, waits for the backlog for at most drainWait, tears the system down
// and returns what it measured.
type ProbeFunc func(ctx context.Context, rate float64, duration, drainWait time.Duration) (*Outcome, error)

// SweepConfig describes how a system is probed: at one rate, or at every rate
// that a search for the maximum visits and then at half and at four fifths of
// the maximum.
type SweepConfig struct {
	// Experiment, Params and Env go into every result. Params gets the keys
	// "phase" and "rate" added.
	Experiment string
	Params     map[string]string
	Env        map[string]string
	// FindMax selects the search, over [Lo, Hi] jobs per second in Steps
	// bisections. Otherwise Rate is the one rate to measure.
	FindMax bool
	Rate    float64
	Lo, Hi  float64
	Steps   int
	// Duration is how long each probe offers load.
	Duration time.Duration
	// Log receives one line of progress per probe. It may be nil.
	Log func(format string, args ...any)
}

// Validate checks that the configuration names one way of choosing the rate
// and a positive duration.
func (c SweepConfig) Validate() error {
	switch {
	case c.Duration <= 0:
		return fmt.Errorf("exp: the duration of a probe must be positive, got %v", c.Duration)
	case c.FindMax && c.Rate != 0:
		return errors.New("exp: --rate and --find-max are alternatives")
	case !c.FindMax && !(c.Rate > 0):
		return errors.New("exp: give --rate, or --find-max to search for the maximum")
	}
	return nil
}

// Sweep probes a system as cfg says and emits a result for every probe.
//
// With a fixed rate it emits one result of phase "fixed". With FindMax it emits
// a result of phase "probe" for each rate the bisection tried, one of phase "max"
// with the highest sustained rate as the metric max_rate, and then results of
// phases "load-50" and "load-80" measured at 50% and 80% of it, with the
// latency percentiles at those loads. If every probe was sustained the maximum is
// only a lower bound, which the metric max_is_lower_bound says.
func Sweep(ctx context.Context, cfg SweepConfig, probe ProbeFunc, emit Emit) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	logf := cfg.Log
	if logf == nil {
		logf = func(string, ...any) {}
	}
	run := func(phase string, rate float64, drainWait time.Duration) (*Outcome, error) {
		began := time.Now()
		o, err := probe(ctx, rate, cfg.Duration, drainWait)
		if err != nil {
			return nil, err
		}
		params := maps.Clone(cfg.Params)
		if params == nil {
			params = map[string]string{}
		}
		params["phase"] = phase
		params["rate"] = formatRate(rate)
		emit(Result{
			Experiment: cfg.Experiment, Params: params, Metrics: o.Metrics(), Quantiles: o.Hist.Quantiles(),
			Env: cfg.Env, Started: began, Duration: time.Since(began),
		})
		logf("%s %s: offered %.0f/s, started %d of %d, p99 %v, drain %.2fs, sustained %v",
			cfg.Experiment, phase, rate, o.Started, o.Sent, o.Hist.Quantiles()["p99"], o.DrainSeconds, o.Sustained())
		return o, nil
	}

	if !cfg.FindMax {
		_, err := run("fixed", cfg.Rate, MeasureDrainWait)
		return err
	}

	started := time.Now()
	best, err := FindMax(ctx, func(rate float64) (bool, error) {
		o, err := run("probe", rate, SustainedDrain)
		if err != nil {
			return false, err
		}
		return o.Sustained(), nil
	}, cfg.Lo, cfg.Hi, cfg.Steps)
	lowerBound := errors.Is(err, ErrNeverFailed)
	if err != nil && !lowerBound {
		return err
	}
	params := maps.Clone(cfg.Params)
	if params == nil {
		params = map[string]string{}
	}
	params["phase"] = "max"
	emit(Result{
		Experiment: cfg.Experiment, Params: params,
		Metrics:   map[string]float64{"max_rate": best, "max_is_lower_bound": flag(lowerBound), "steps": float64(cfg.Steps)},
		Quantiles: map[string]time.Duration{}, Env: cfg.Env, Started: started, Duration: time.Since(started),
	})
	logf("%s max: %.0f/s sustained (p99 under %v, backlog done within %v)%s",
		cfg.Experiment, best, SustainedP99, SustainedDrain, map[bool]string{true: "; only a lower bound, no probe failed", false: ""}[lowerBound])

	for _, load := range []struct {
		phase    string
		fraction float64
	}{{"load-50", 0.5}, {"load-80", 0.8}} {
		if _, err := run(load.phase, best*load.fraction, MeasureDrainWait); err != nil {
			return err
		}
	}
	return nil
}

// formatRate writes a rate with at most two decimals and no trailing zeros.
func formatRate(rate float64) string {
	return strconv.FormatFloat(math.Round(rate*100)/100, 'f', -1, 64)
}
