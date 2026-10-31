package exp

import (
	"context"
	"fmt"
)

// AblateOptions configures E2.
type AblateOptions struct {
	ThroughputOptions
	// What is the feature to switch off: "batch", "coalesce" or "notify".
	What string
}

// variant is one setting of an ablation. Spec and Cluster are what the setting
// changes, and Name is how a result names it.
type variant struct {
	Name    string
	Spec    workerSpec
	Cluster ClusterOptions
	Params  map[string]string
}

// variants returns the two settings of the ablation, the feature on first and
// off second.
func variants(what string) ([]variant, error) {
	switch what {
	case "batch":
		// The SDK asks for min(free slots, 64) jobs per Lease call.
		return []variant{
			{Name: "max_jobs_64"},
			{Name: "max_jobs_1", Spec: workerSpec{LeaseMaxJobs: 1}},
		}, nil
	case "coalesce":
		// Acks go out after 2ms or 128 of them; a batch of 1 sends each at once.
		return []variant{
			{Name: "ack_batch_128"},
			{Name: "ack_batch_1", Spec: workerSpec{AckBatch: 1}},
		}, nil
	case "notify":
		return []variant{
			{Name: "listen_on", Params: map[string]string{"disable_listen": "false"}},
			{Name: "listen_off", Cluster: ClusterOptions{DisableListen: true}, Params: map[string]string{"disable_listen": "true"}},
		}, nil
	default:
		return nil, fmt.Errorf("--what must be batch, coalesce or notify, got %q", what)
	}
}

// Ablate is E2: it measures the queue twice, with a feature on and off.
//
//   - batch: the workers take up to 64 jobs per Lease call, or 1.
//   - coalesce: acknowledgements are sent in batches of up to 128 after a 2ms
//     window, or one by one (AckBatch 1).
//   - notify: the idle-queue enqueue-to-start latency with the NOTIFY listener on
//     and with the brokers started with --disable-listen, which leaves only the
//     fallback poll. This one measures one fixed Rate, over a queue that is idle
//     between jobs.
//
// batch and coalesce measure as Throughput does, with Rate or FindMax. Every
// result names its setting as the param "variant".
func Ablate(ctx context.Context, o AblateOptions, emit Emit) error {
	vs, err := variants(o.What)
	if err != nil {
		return err
	}
	if o.What == "notify" {
		// An idle queue: one slow worker and a rate far below what it can take.
		if o.Workers == 0 {
			o.Workers = 1
		}
		if o.Concurrency == 0 {
			o.Concurrency = 8
		}
		if o.FindMax || o.Rate == 0 {
			return fmt.Errorf("--what notify measures idle-queue latency at one fixed rate: give --rate and not --find-max")
		}
	}
	o.ThroughputOptions = o.withDefaults()
	if err := o.validate(); err != nil {
		return err
	}

	// batch and coalesce differ only in the workers, so they share the brokers.
	// notify differs in the brokers.
	var shared *rig
	if o.What != "notify" {
		if shared, err = openRig(ctx, o.Setup, ClusterOptions{}); err != nil {
			return err
		}
		defer func() { _ = shared.close() }()
	}
	for _, v := range vs {
		r := shared
		if r == nil {
			if r, err = openRig(ctx, o.Setup, v.Cluster); err != nil {
				return err
			}
		}
		params := o.params()
		params["what"], params["variant"] = o.What, v.Name
		for k, val := range v.Params {
			params[k] = val
		}
		o.logf("ablate %s: variant %s", o.What, v.Name)
		err = Sweep(ctx, o.sweep(r.env, params, "ablate"), r.sdkProbeFor(o.ThroughputOptions, v.Spec).run, emit)
		if shared == nil {
			if cerr := r.close(); cerr != nil && err == nil {
				err = cerr
			}
		}
		if err != nil {
			return fmt.Errorf("variant %s: %w", v.Name, err)
		}
	}
	return nil
}
