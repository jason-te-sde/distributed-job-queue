package exp

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

// ThroughputOptions configures E1, and the ablations of E2 that measure
// throughput. Every field but the database URL and the choice between Rate and
// FindMax has a default.
type ThroughputOptions struct {
	Setup
	// Rate is the one offered load to measure, in jobs per second. FindMax
	// instead searches [Lo, Hi] for the highest sustained rate in Steps
	// bisections and measures at 50% and 80% of it. The two are alternatives.
	Rate    float64
	FindMax bool
	Lo, Hi  float64
	Steps   int
	// Duration is how long each probe offers load.
	Duration time.Duration
	// PayloadBytes is the size of each job's payload, at least 8.
	PayloadBytes int
	// Workers is the number of workers and Concurrency the handlers each runs.
	Workers, Concurrency int
}

func (o ThroughputOptions) withDefaults() ThroughputOptions {
	if o.Duration == 0 {
		o.Duration = DefaultDuration
	}
	if o.PayloadBytes == 0 {
		o.PayloadBytes = DefaultPayloadBytes
	}
	if o.Workers == 0 {
		o.Workers = DefaultWorkers
	}
	if o.Concurrency == 0 {
		o.Concurrency = DefaultConcurrency
	}
	if o.FindMax {
		if o.Lo == 0 {
			o.Lo = DefaultLo
		}
		if o.Hi == 0 {
			o.Hi = DefaultHi
		}
		if o.Steps == 0 {
			o.Steps = DefaultSteps
		}
	}
	return o
}

func (o ThroughputOptions) validate() error {
	if err := o.Setup.validate(); err != nil {
		return err
	}
	switch {
	case o.PayloadBytes < stampBytes || o.PayloadBytes > maxPayload:
		return fmt.Errorf("--payload-bytes must be between %d and %d, got %d", stampBytes, maxPayload, o.PayloadBytes)
	case o.Workers < 1:
		return fmt.Errorf("--workers must be at least 1, got %d", o.Workers)
	case o.Concurrency < 1 || o.Concurrency > maxConcurrency:
		return fmt.Errorf("--concurrency must be between 1 and %d, got %d", maxConcurrency, o.Concurrency)
	}
	return o.sweep(nil, nil, "").Validate()
}

// params are the inputs of the run as a result states them.
func (o ThroughputOptions) params() map[string]string {
	p := map[string]string{
		"brokers":       strconv.Itoa(o.brokers()),
		"duration":      o.Duration.String(),
		"payload_bytes": strconv.Itoa(o.PayloadBytes),
		"workers":       strconv.Itoa(o.Workers),
		"concurrency":   strconv.Itoa(o.Concurrency),
		"find_max":      strconv.FormatBool(o.FindMax),
	}
	if o.FindMax {
		p["lo"], p["hi"], p["steps"] = formatRate(o.Lo), formatRate(o.Hi), strconv.Itoa(o.Steps)
	}
	return p
}

func (o ThroughputOptions) sweep(env, params map[string]string, experiment string) SweepConfig {
	return SweepConfig{
		Experiment: experiment, Params: params, Env: env,
		FindMax: o.FindMax, Rate: o.Rate, Lo: o.Lo, Hi: o.Hi, Steps: o.Steps, Duration: o.Duration, Log: o.Log,
	}
}

// Throughput is E1: the maximum sustained rate of the queue, and the
// enqueue-to-start latency at fixed loads. The brokers run in this process over
// the database, the workers have empty handlers, and one producer enqueues one
// job per call on an open-loop schedule.
//
// A rate is sustained if the p99 enqueue-to-start latency stays under 1s and the
// backlog is finished within 2s of the end of the load. With FindMax the run
// searches for the highest such rate and then measures the latency percentiles at
// 50% and 80% of it; with Rate it measures that one load.
func Throughput(ctx context.Context, o ThroughputOptions, emit Emit) error {
	o = o.withDefaults()
	if err := o.validate(); err != nil {
		return err
	}
	r, err := openRig(ctx, o.Setup, ClusterOptions{})
	if err != nil {
		return err
	}
	err = Sweep(ctx, o.sweep(r.env, o.params(), "throughput"), r.sdkProbeFor(o, workerSpec{}).run, emit)
	if cerr := r.close(); cerr != nil && err == nil {
		err = cerr
	}
	return err
}

// sdkProbeFor returns the probe of the SDK for the options, with the workers of
// the spec changed to the options' count and concurrency.
func (r *rig) sdkProbeFor(o ThroughputOptions, spec workerSpec) *sdkProbe {
	spec.Count, spec.Concurrency = o.Workers, o.Concurrency
	return &sdkProbe{rig: r, spec: spec, payloadBytes: o.PayloadBytes}
}
