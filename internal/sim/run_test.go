package sim_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/sim"
)

// envInt reads an integer setting from the environment, or returns def when
// the variable is unset.
func envInt(t *testing.T, name string, def int) int {
	t.Helper()
	v, ok := os.LookupEnv(name)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		t.Fatalf("%s=%q, want a non-negative integer", name, v)
	}
	return n
}

// minRoughSeeds is the fewest seeds that the roughness requirement is applied
// to. A run of one seed, as a replay is, cannot be expected to have every
// kind of fault in it.
const minRoughSeeds = 100

// reproduce is the command that runs exactly one seed of TestSimSeeds, for
// any seed, and counts as a pass when the seed passes.
func reproduce(seed int) string {
	return fmt.Sprintf("JOBQUEUE_SIM_SEEDS=1 JOBQUEUE_SIM_SEED_START=%d go test ./internal/sim -run 'TestSimSeeds'", seed)
}

// writeFailure leaves a file that a person can start from: the config, the
// error and the command that replays the seed.
func writeFailure(t *testing.T, seed int, cfg sim.Config, err error) {
	t.Helper()
	dir := filepath.Join("testdata", "failures")
	if mkErr := os.MkdirAll(dir, 0o755); mkErr != nil {
		t.Logf("cannot write the failure file: %v", mkErr)
		return
	}
	body := fmt.Sprintf("seed %d\n\nconfig:\n%+v\n\nerror:\n%v\n\nreproduce:\n%s\n", seed, cfg, err, reproduce(seed))
	path := filepath.Join(dir, "seed-"+strconv.Itoa(seed)+".txt")
	if wErr := os.WriteFile(path, []byte(body), 0o644); wErr != nil {
		t.Logf("cannot write %s: %v", path, wErr)
	}
}

func TestDefaultConfigIsAFunctionOfTheSeed(t *testing.T) {
	a, b := sim.DefaultConfig(77), sim.DefaultConfig(77)
	if fmt.Sprintf("%+v", a) != fmt.Sprintf("%+v", b) {
		t.Errorf("the same seed gave two configs:\n%+v\n%+v", a, b)
	}
	if fmt.Sprintf("%+v", a) == fmt.Sprintf("%+v", sim.DefaultConfig(78)) {
		t.Error("seeds 77 and 78 gave the same config")
	}
}

func TestDefaultConfigStaysInItsRanges(t *testing.T) {
	within := func(t *testing.T, name string, got, lo, hi int) {
		t.Helper()
		if got < lo || got > hi {
			t.Errorf("%s = %d, want %d to %d", name, got, lo, hi)
		}
	}
	seen := map[string]map[int]bool{}
	note := func(name string, v int) {
		if seen[name] == nil {
			seen[name] = map[int]bool{}
		}
		seen[name][v] = true
	}
	for seed := uint64(1); seed <= 300; seed++ {
		c := sim.DefaultConfig(seed)
		if c.Seed != seed {
			t.Fatalf("Seed = %d, want %d", c.Seed, seed)
		}
		within(t, "Brokers", c.Brokers, 1, 3)
		within(t, "Workers", c.Workers, 2, 5)
		within(t, "Concurrency", c.Concurrency, 1, 8)
		within(t, "Jobs", c.Jobs, 50, 300)
		within(t, "Tenants", len(c.Tenants), 1, 4)
		within(t, "Queues", len(c.Queues), 1, 2)
		for name, n := range map[string]int{
			"WorkerCrashes": c.Faults.WorkerCrashes, "WorkerStalls": c.Faults.WorkerStalls,
			"BrokerCrashes": c.Faults.BrokerCrashes, "StoreOutages": c.Faults.StoreOutages, "Partitions": c.Faults.Partitions,
		} {
			within(t, name, n, 0, 3)
			note(name, n)
		}
		note("Brokers", c.Brokers)
		note("Queues", len(c.Queues))
		if c.Workers < len(c.Queues) {
			t.Errorf("seed %d: %d workers for %d queues, so a queue has none", seed, c.Workers, len(c.Queues))
		}
		if c.TTL != 30*time.Second || c.Margin != 3*time.Second || c.Faults.Phase != 10*time.Minute || c.Heal != 30*time.Minute {
			t.Errorf("seed %d: TTL %v, margin %v, phase %v, heal %v", seed, c.TTL, c.Margin, c.Faults.Phase, c.Heal)
		}
		if c.Net.DropP < 0 || c.Net.DropP > 0.05 || c.Net.DupP < 0 || c.Net.DupP > 0.02 || c.NotifyDropP < 0 || c.NotifyDropP > 0.3 {
			t.Errorf("seed %d: DropP %v, DupP %v, NotifyDropP %v out of range", seed, c.Net.DropP, c.Net.DupP, c.NotifyDropP)
		}
		if c.Net.Latency != [2]time.Duration{200 * time.Microsecond, 5 * time.Millisecond} || c.Net.TailP != 0.01 ||
			c.Net.Tail != [2]time.Duration{100 * time.Millisecond, 6 * time.Second} {
			t.Errorf("seed %d: network latencies %+v", seed, c.Net)
		}
		if c.Retention.Done != 2*time.Minute || c.Retention.Dead != 5*time.Minute {
			t.Errorf("seed %d: retention %+v", seed, c.Retention)
		}
	}
	// A range that a generator never leaves its first value of is not drawn.
	for name, vals := range seen {
		if len(vals) < 2 {
			t.Errorf("%s took only the values %v over 300 seeds", name, vals)
		}
	}
}

func TestRunRejectsAConfigThatLeavesAQueueWithoutAWorker(t *testing.T) {
	cfg := sim.DefaultConfig(5)
	cfg.Queues, cfg.Workers = []string{"q1", "q2"}, 1
	_, err := sim.Run(cfg)
	if err == nil || !strings.Contains(err.Error(), "seed 5") || !strings.Contains(err.Error(), "queue") {
		t.Errorf("err = %v, want a setup error that names the seed and the queues", err)
	}
}

func TestRunReportsAPanicAsAnErrorNamingTheSeed(t *testing.T) {
	cfg := sim.DefaultConfig(6)
	cfg.TTL = 500 * time.Millisecond // below the shortest lease, so building a worker panics
	_, err := sim.Run(cfg)
	if err == nil || !strings.Contains(err.Error(), "seed 6") || !strings.Contains(err.Error(), "panic") {
		t.Errorf("err = %v, want an error that names the seed and the panic", err)
	}
}

func TestSimDeterministic(t *testing.T) {
	// Several seeds, and at least one with more than one broker: the order in
	// which nodes are built and ticked is where a map iteration would show.
	seeds := []uint64{8123, 1, 2, 3, 4, 5}
	multiBroker := false
	for _, seed := range seeds {
		multiBroker = multiBroker || sim.DefaultConfig(seed).Brokers >= 2
	}
	if !multiBroker {
		t.Fatalf("none of the seeds %v has two brokers", seeds)
	}
	for _, seed := range seeds {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			a, errA := sim.Run(sim.DefaultConfig(seed))
			b, errB := sim.Run(sim.DefaultConfig(seed))
			if errA != nil || errB != nil {
				t.Fatalf("Run failed: %v, %v", errA, errB)
			}
			if a != b {
				t.Errorf("the same seed gave\n%+v\nand\n%+v", a, b)
			}
			if a.TraceHash == 0 || a.Events == 0 {
				t.Errorf("stats = %+v, want a run that fired events", a)
			}
		})
	}
}

func TestStatsAddCoversEveryField(t *testing.T) {
	// Every field set to 1 on both sides: after Add each count is 2, so a
	// field that Add forgets stays at 1. TraceHash is the one field that is
	// not a count, and Add leaves it alone on purpose.
	var a, b sim.Stats
	for _, s := range []*sim.Stats{&a, &b} {
		v := reflect.ValueOf(s).Elem()
		for i := range v.NumField() {
			f := v.Field(i)
			if f.Kind() == reflect.Int {
				f.SetInt(1)
			} else if f.Kind() == reflect.Uint64 {
				f.SetUint(1)
			} else {
				t.Fatalf("Stats.%s has kind %v, which this test does not know how to set", v.Type().Field(i).Name, f.Kind())
			}
		}
	}
	a.Add(b)
	v := reflect.ValueOf(a)
	for i := range v.NumField() {
		name, f := v.Type().Field(i).Name, v.Field(i)
		switch {
		case name == "TraceHash":
			if f.Uint() != 1 {
				t.Errorf("Add changed TraceHash to %d, want it left alone", f.Uint())
			}
		case f.Int() != 2:
			t.Errorf("after Add, Stats.%s = %d, want 2: Add does not sum it", name, f.Int())
		}
	}
}

func TestRunChecksTheFinalInvariants(t *testing.T) {
	// Far more work than one handler per worker can finish in a minute, and
	// no time to heal: jobs that were confirmed are still waiting at the end,
	// which only the final check can see.
	cfg := sim.DefaultConfig(3)
	cfg.Jobs, cfg.Concurrency = 300, 1
	cfg.Faults.Phase, cfg.Heal = time.Minute, 0
	_, err := sim.Run(cfg)
	var v *sim.Violation
	if !errors.As(err, &v) || v.Invariant != "L1" {
		t.Fatalf("err = %v, want a *sim.Violation of L1 inside the error", err)
	}
	if !strings.HasPrefix(err.Error(), "seed 3: ") {
		t.Errorf("err = %q, want it to start with the seed", err.Error())
	}
}

func TestSimSeeds(t *testing.T) {
	n, start := envInt(t, "JOBQUEUE_SIM_SEEDS", 500), envInt(t, "JOBQUEUE_SIM_SEED_START", 1)
	var mu sync.Mutex
	var total sim.Stats
	ran := 0 // the seeds that ran and passed; a -run filter can leave out most of n
	t.Run("seeds", func(t *testing.T) {
		for s := start; s < start+n; s++ {
			t.Run(strconv.Itoa(s), func(t *testing.T) {
				t.Parallel()
				cfg := sim.DefaultConfig(uint64(s))
				st, err := sim.Run(cfg)
				if err != nil {
					writeFailure(t, s, cfg, err)
					t.Fatalf("%v\nreproduce: %s", err, reproduce(s))
				}
				mu.Lock()
				total.Add(st)
				ran++
				mu.Unlock()
			})
		}
	})
	if t.Failed() {
		return
	}
	t.Logf("%d seeds ran, from %d: %+v", ran, start, total)
	if ran < minRoughSeeds {
		t.Logf("fewer than %d seeds ran, so the roughness of the batch is not checked", minRoughSeeds)
		return
	}
	// The batch must have been rough enough to mean something: a system
	// that is left alone passes every safety check.
	counters := map[string]int{
		"DuplicateDeliveries": total.DuplicateDeliveries, "FencedAcks": total.FencedAcks,
		"SinkRejections": total.SinkRejections, "Reaps": total.Reaps, "Prunes": total.Prunes,
		"WorkerCrashes": total.WorkerCrashes, "WorkerStalls": total.WorkerStalls, "BrokerCrashes": total.BrokerCrashes,
		"StoreOutages": total.StoreOutages, "Partitions": total.Partitions, "NotifyDrops": total.NotifyDrops,
	}
	for _, name := range []string{"DuplicateDeliveries", "FencedAcks", "SinkRejections", "Reaps", "Prunes",
		"WorkerCrashes", "WorkerStalls", "BrokerCrashes", "StoreOutages", "Partitions", "NotifyDrops"} {
		if counters[name] <= 0 {
			t.Errorf("the batch of %d seeds that ran, from %d, had %s = %d, want more than 0, so it was not rough enough", ran, start, name, counters[name])
		}
	}
}

// regressionSeeds reads testdata/regression_seeds.txt: one "<seed> # <note>"
// per line, blank lines and lines that start with # ignored.
func regressionSeeds(t *testing.T) []uint64 {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "regression_seeds.txt"))
	if err != nil {
		t.Fatalf("reading the regression seeds: %v", err)
	}
	var seeds []uint64
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		field, note, hasNote := strings.Cut(line, "#")
		seed, err := strconv.ParseUint(strings.TrimSpace(field), 10, 64)
		if err != nil || !hasNote || strings.TrimSpace(note) == "" {
			t.Fatalf("regression_seeds.txt line %d is %q, want \"<seed> # <note>\"", i+1, line)
		}
		seeds = append(seeds, seed)
	}
	if len(seeds) == 0 || seeds[0] != 8123 {
		t.Fatalf("regression_seeds.txt lists %v, want it to start with 8123", seeds)
	}
	return seeds
}

func TestSimRegressionSeeds(t *testing.T) {
	for _, seed := range regressionSeeds(t) {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			t.Parallel()
			if _, err := sim.Run(sim.DefaultConfig(seed)); err != nil {
				t.Fatalf("%v\nreproduce: go test ./internal/sim -run 'TestSimRegressionSeeds/^%d$'", err, seed)
			}
		})
	}
}
