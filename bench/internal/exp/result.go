// Package exp holds the experiments of the benchmark: the harness that measures
// enqueue-to-start latency under an open-loop load, and the six experiments
// built on it.
package exp

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/bench/internal/hist"
)

// Result is the outcome of one measurement, serialized as one line of JSON. The
// names of the units are in the JSON keys, because a bare number in a file that
// is committed as evidence has to say what it counts.
type Result struct {
	// Experiment names the experiment, for example "throughput" or "noisy".
	Experiment string `json:"experiment"`
	// Params are the inputs of this measurement, as the command line gave them
	// and the experiment resolved them.
	Params map[string]string `json:"params"`
	// Metrics are the numbers the experiment reports, in the units its names
	// give: a name ending in _s is seconds, _ms milliseconds, _per_s a rate.
	Metrics map[string]float64 `json:"metrics"`
	// Quantiles are the latency percentiles of the measurement: p50, p90, p99,
	// p99.9 and max, in nanoseconds.
	Quantiles map[string]time.Duration `json:"quantiles_ns"`
	// CDF is the empirical distribution of the latency, for the experiments
	// whose result is drawn. It is omitted otherwise.
	CDF []hist.Point `json:"cdf,omitempty"`
	// Env describes the machine, the toolchain and the database.
	Env map[string]string `json:"env"`
	// Started is when the measurement began, and Duration how long it took, in
	// nanoseconds.
	Started  time.Time     `json:"started"`
	Duration time.Duration `json:"duration_ns"`
}

// Emit receives each result as the experiment produces it, so that a long run
// writes its results while it goes instead of at the end.
type Emit func(Result)

// JSON returns the result as a single line, without a newline.
func (r Result) JSON() ([]byte, error) { return json.Marshal(r) }

// ReadResult reads the JSON lines of the file and returns the last result of
// the experiment. A file written with --out holds one line for every result of
// every run appended to it, and the last is the latest.
func ReadResult(path, experiment string) (Result, error) {
	f, err := os.Open(path)
	if err != nil {
		return Result{}, fmt.Errorf("read results: %w", err)
	}
	defer func() { _ = f.Close() }()

	var found *Result
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), 16<<20)
	for line := 1; sc.Scan(); line++ {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var r Result
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			return Result{}, fmt.Errorf("read results: %s line %d: %w", path, line, err)
		}
		if r.Experiment == experiment {
			found = &r
		}
	}
	if err := sc.Err(); err != nil {
		return Result{}, fmt.Errorf("read results: %s: %w", path, err)
	}
	if found == nil {
		return Result{}, fmt.Errorf("read results: %s has no result of the experiment %q", path, experiment)
	}
	return *found, nil
}

// ErrNoDatabase is returned by experiments that were given no database URL.
var ErrNoDatabase = errors.New("no database URL: pass --database-url or set JOBQUEUE_DATABASE_URL")
