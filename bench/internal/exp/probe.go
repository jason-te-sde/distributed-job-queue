package exp

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/bench/internal/hist"
	"github.com/jason-te-sde/distributed-job-queue/bench/internal/load"
)

const (
	// SustainedP99 is the p99 enqueue-to-start latency that a sustained rate
	// stays under.
	SustainedP99 = time.Second
	// SustainedDrain is how long after the end of the load the backlog may take
	// to finish at a sustained rate.
	SustainedDrain = 2 * time.Second
)

// ProbeConfig describes one open-loop probe.
type ProbeConfig struct {
	// Rate is the offered load in jobs per second, and Duration how long the
	// schedule runs: Rate*Duration jobs are sent.
	Rate     float64
	Duration time.Duration
	// DrainWait is how long, after the last job was sent, to wait for the
	// backlog to start.
	DrainWait time.Duration
	// StartDelay postpones the first job, so that workers have connected
	// before the load begins. Latency is measured from the intended times, which
	// include it, so a delay that is too short shows up as latency.
	StartDelay time.Duration
}

// Outcome is what one probe measured.
type Outcome struct {
	// Offered is the rate asked for. Sent is the number of jobs whose send
	// returned without error, Late how many sends began more than 1ms behind
	// schedule, and Started how many distinct jobs had started when the probe
	// ended.
	Offered float64
	Sent    int
	Late    int
	Started int64
	// Duplicates is the number of starts of a job that had started already. They
	// are not in Started, and the latency of a job is recorded at its first start
	// only. It is read when Await returns, so a duplicate that has not started by
	// then, such as the second job of a retry that commits late, is not in it: it
	// is a lower bound.
	Duplicates int64
	// SendErr is the first error of a send, which stopped the schedule.
	SendErr error
	// Complete is true if every sent job started within the drain wait.
	Complete bool
	// LoadSeconds is the time from the start of the schedule until the last
	// send returned, and DrainSeconds the time from then until the last job
	// started (the whole drain wait if some never did).
	LoadSeconds  float64
	DrainSeconds float64
	// ThroughputPerSec is the number of jobs started per second between the
	// start of the schedule and the last start.
	ThroughputPerSec float64
	// Hist holds the enqueue-to-start latency of every job that started, at its
	// first start.
	Hist *hist.Hist
}

// Sustained reports whether the system kept up with the offered rate: no send
// failed, every job started, the p99 latency was under SustainedP99, and the
// backlog finished within SustainedDrain of the end of the load.
func (o *Outcome) Sustained() bool {
	return o.SendErr == nil && o.Complete && o.Hist != nil &&
		o.Hist.Quantiles()["p99"] < SustainedP99 && o.DrainSeconds <= SustainedDrain.Seconds()
}

// Metrics returns the numbers of the outcome as a result reports them.
func (o *Outcome) Metrics() map[string]float64 {
	achieved := 0.0
	if o.LoadSeconds > 0 {
		achieved = float64(o.Sent) / o.LoadSeconds
	}
	return map[string]float64{
		"offered_rate":     o.Offered,
		"achieved_rate":    achieved,
		"sent":             float64(o.Sent),
		"started":          float64(o.Started),
		"duplicate_starts": float64(o.Duplicates),
		"late_sends":       float64(o.Late),
		"drain_seconds":    o.DrainSeconds,
		"throughput_per_s": o.ThroughputPerSec,
		"complete":         flag(o.Complete),
		"sustained":        flag(o.Sustained()),
	}
}

func flag(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// Probe offers cfg.Rate*cfg.Duration jobs on an open-loop schedule, each sent by
// send with its intended time, and measures when they start through m, whose
// histogram of the tenant it reports. m must be new, and its workers must be
// running and must call m.Observe for every job that starts.
//
// A failed send is not a failure of the probe but its result, in SendErr: an
// overloaded system is expected to fail some. Probe returns an error only when
// ctx ends.
func Probe(ctx context.Context, cfg ProbeConfig, m *Meter, tenant string, send load.SendFunc) (*Outcome, error) {
	return probeFrom(ctx, cfg, time.Now().Add(cfg.StartDelay), m, tenant, send)
}

// probeFrom is Probe for a caller that has fixed the instant the schedule
// starts, because something else is timed against it.
func probeFrom(ctx context.Context, cfg ProbeConfig, start time.Time, m *Meter, tenant string, send load.SendFunc) (*Outcome, error) {
	n := max(1, int(math.Round(cfg.Rate*cfg.Duration.Seconds())))
	sent, late, err := load.Run(ctx, load.Schedule{Start: start, Rate: cfg.Rate}, n, send)
	loadEnd := time.Now()
	if cerr := ctx.Err(); cerr != nil {
		return nil, cerr
	}

	o := &Outcome{
		Offered: cfg.Rate, Sent: sent, Late: late, SendErr: err, Hist: m.Hist(tenant),
		LoadSeconds: loadEnd.Sub(start).Seconds(),
	}
	if err == nil {
		o.Complete = m.Await(ctx, int64(sent), cfg.DrainWait)
		if cerr := ctx.Err(); cerr != nil {
			return nil, cerr
		}
	}
	o.Started = m.StartedTotal()
	o.Duplicates = m.DuplicatesTotal()
	if last := m.LastStart(); !last.IsZero() {
		if span := last.Sub(start).Seconds(); span > 0 {
			o.ThroughputPerSec = float64(o.Started) / span
		}
		o.DrainSeconds = max(0, last.Sub(loadEnd).Seconds())
	}
	if err == nil && !o.Complete {
		o.DrainSeconds = cfg.DrainWait.Seconds()
	}
	if o.Hist == nil {
		return nil, errors.New("exp: the meter does not measure the tenant " + tenant)
	}
	return o, nil
}
