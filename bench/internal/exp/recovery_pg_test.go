//go:build pg

package exp

import (
	"strings"
	"testing"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
)

func TestPGRecoveryKillWorkerRestartsEveryHeldJobWithinTheBound(t *testing.T) {
	dsn, pool := testDatabase(t)
	emit, results := collectResults()
	err := Recovery(t.Context(), RecoveryOptions{
		Setup: Setup{DatabaseURL: dsn}, Kill: "worker", K: 8, TTL: time.Second, Margin: 200 * time.Millisecond, Exe: buildBench(t),
	}, emit)
	if err != nil {
		t.Fatalf("Recovery(worker): %v", err)
	}
	if len(*results) != 1 {
		t.Fatalf("emitted %d results, want 1", len(*results))
	}
	r := (*results)[0]
	if r.Experiment != "recovery" || r.Params["kill"] != "worker" {
		t.Errorf("experiment %q kill %q, want recovery/worker", r.Experiment, r.Params["kill"])
	}
	if r.Metrics["k"] != 8 || r.Metrics["restarted"] != 8 {
		t.Errorf("k %v, restarted %v; want all 8 held jobs to restart", r.Metrics["k"], r.Metrics["restarted"])
	}
	// The lease of a killed worker runs out between three quarters of a TTL and
	// a whole one after the kill, since it was last heartbeat at most a
	// quarter of a TTL before.
	all, bound := r.Metrics["time_to_all_restart_s"], r.Metrics["bound_s"]
	if all < 0.7 {
		t.Errorf("all jobs restarted %.2fs after the kill, before the leases could have run out (TTL 1s)", all)
	}
	if all > bound {
		t.Errorf("all jobs restarted %.2fs after the kill, over the bound of %.2fs", all, bound)
	}
	// The first backoff is what the reaper applies, not a number of this file's.
	if want := job.DefaultBackoff.Ceiling(1).Seconds(); r.Metrics["first_backoff_s"] != want {
		t.Errorf("first_backoff_s = %v, want %v, the ceiling of the default backoff after the first attempt", r.Metrics["first_backoff_s"], want)
	}
	if bound != r.Metrics["ttl_s"]+r.Metrics["reap_interval_s"]+r.Metrics["first_backoff_s"]+r.Metrics["dispatch_s"] {
		t.Errorf("bound_s %v is not the sum of its parts: %v", bound, r.Metrics)
	}
	if r.Metrics["within_bound"] != 1 {
		t.Errorf("within_bound = %v with %.2fs against a bound of %.2fs", r.Metrics["within_bound"], all, bound)
	}
	if r.Quantiles["max"] <= 0 {
		t.Errorf("no restart delay quantiles: %v", r.Quantiles)
	}
	var leased int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM jobqueue.jobs WHERE state = 'leased'").Scan(&leased); err != nil {
		t.Fatal(err)
	}
	if leased != 0 && countRows(t, pool) != 0 {
		t.Errorf("%d jobs still leased after the experiment: it must clean up", leased)
	}
}

func TestPGRecoveryKillBrokerLosesNoJob(t *testing.T) {
	dsn, _ := testDatabase(t)
	emit, results := collectResults()
	err := Recovery(t.Context(), RecoveryOptions{
		Setup: Setup{DatabaseURL: dsn}, Kill: "broker", Rate: 100, Duration: 6 * time.Second,
	}, emit)
	if err != nil {
		t.Fatalf("Recovery(broker): %v", err)
	}
	if len(*results) != 1 {
		t.Fatalf("emitted %d results, want 1", len(*results))
	}
	r := (*results)[0]
	if r.Params["kill"] != "broker" || r.Params["brokers"] != "3" {
		t.Errorf("kill %q brokers %q, want broker/3", r.Params["kill"], r.Params["brokers"])
	}
	if !strings.Contains(r.Params["mode"], "connections reset through a proxy") || !strings.Contains(r.Params["mode"], "broker process stays up") {
		t.Errorf("params[mode] = %q, want it to say what was killed", r.Params["mode"])
	}
	if r.Metrics["connections_reset"] < 1 {
		t.Errorf("connections_reset = %v: the kill reset nothing, so the run injected no fault", r.Metrics["connections_reset"])
	}
	if r.Metrics["errors_before_kill"] != 0 {
		t.Errorf("errors_before_kill = %v, want 0: the workers saw errors with all three brokers up", r.Metrics["errors_before_kill"])
	}
	if r.Metrics["sent"] != 600 || r.Metrics["started"] != 600 {
		t.Errorf("sent %v, started %v; want all 600 jobs to start despite the dead broker", r.Metrics["sent"], r.Metrics["started"])
	}
	if errs := r.Metrics["errors_after_kill"]; errs > 0 && r.Metrics["error_window_s"] < 0 {
		t.Errorf("errors_after_kill %v with a window of %v", errs, r.Metrics["error_window_s"])
	}
}
