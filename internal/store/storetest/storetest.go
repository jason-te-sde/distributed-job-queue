// Package storetest is the conformance suite that every store.Store must
// pass. It pins the contract that the store package documents: what each
// operation does to a job, which arguments are rejected, and where the time
// boundaries lie.
//
// The suite runs against a Harness, which owns a fresh store and the clock
// the store reads. The in-memory store and the PostgreSQL store each supply
// a harness, so a divergence between them shows up as a failing subtest
// rather than as a difference nobody noticed.
//
// The package imports testing, so only test files may import it; the
// architecture test enforces that.
package storetest

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

// Harness is a fresh, empty store together with the clock it reads. The
// clock starts at 2026-01-01T00:00:00Z and moves only through Advance, so a
// test can place every instant exactly.
type Harness interface {
	// Store returns the store under test.
	Store() store.Store
	// Now returns the store's current time.
	Now() time.Time
	// Advance moves the store's clock forward by d, a whole number of
	// microseconds.
	Advance(d time.Duration)
}

// NewHarness returns a fresh, empty Harness. The suite calls it once per
// subtest, with that subtest's t.
type NewHarness func(t *testing.T) Harness

// Run runs every group of the suite in order: RunEnqueue, RunLease and
// RunAck. The groups are separate so that a store can adopt them one at a
// time.
func Run(t *testing.T, nh NewHarness) {
	RunEnqueue(t, nh)
	RunLease(t, nh)
	RunAck(t, nh)
}

// subtest is the body of one named conformance check.
type subtest func(t *testing.T, h Harness)

// run runs fn as the subtest called name, on a fresh harness.
func run(t *testing.T, nh NewHarness, name string, fn subtest) {
	t.Run(name, func(t *testing.T) { fn(t, nh(t)) })
}

// badNames are names that must be rejected, as a queue and as a tenant.
func badNames() []string {
	return []string{"", "a b", "q\t1", "-lead", "x\x00", strings.Repeat("a", job.MaxNameBytes+1), "ünï"}
}

// newSpec returns a valid spec with a small payload.
func newSpec(queue, tenant string) job.Spec {
	return job.Spec{Queue: queue, Tenant: tenant, Payload: []byte("payload")}
}

// validSpecs returns n valid specs.
func validSpecs(n int) []job.Spec {
	specs := make([]job.Spec, n)
	for i := range specs {
		specs[i] = newSpec("q1", "t1")
	}
	return specs
}

// enqueue enqueues specs, which must be accepted, and returns the ids.
func enqueue(t *testing.T, h Harness, specs ...job.Spec) []int64 {
	t.Helper()
	res, err := h.Store().Enqueue(t.Context(), specs)
	if err != nil {
		t.Fatalf("Enqueue of %d specs: %v", len(specs), err)
	}
	if len(res) != len(specs) {
		t.Fatalf("Enqueue of %d specs returned %d results", len(specs), len(res))
	}
	ids := make([]int64, len(res))
	for i, r := range res {
		ids[i] = r.ID
	}
	return ids
}

// get returns job id, which must exist.
func get(t *testing.T, h Harness, id int64) job.Job {
	t.Helper()
	j, err := h.Store().Get(t.Context(), id, "")
	if err != nil {
		t.Fatalf("Get(%d): %v", id, err)
	}
	return j
}

// snapshot returns every row, which the store must be able to list.
func snapshot(t *testing.T, h Harness) []job.Job {
	t.Helper()
	sn, ok := h.Store().(store.Snapshotter)
	if !ok {
		t.Fatalf("%T does not implement store.Snapshotter", h.Store())
	}
	rows, err := sn.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	return rows
}

// wantRows fails unless the store holds exactly n rows.
func wantRows(t *testing.T, h Harness, n int, when string) {
	t.Helper()
	if rows := snapshot(t, h); len(rows) != n {
		t.Errorf("%s: the store holds %d rows, want %d", when, len(rows), n)
	}
}

// leaseRequest asks for jobs of queue with the given takes, for 30 seconds
// as worker "w1".
func leaseRequest(queue string, takes ...store.TenantTake) store.LeaseRequest {
	return store.LeaseRequest{Queue: queue, Takes: takes, TTL: 30 * time.Second, Worker: "w1"}
}

// leaseTakes leases with the given takes; the request must be valid.
func leaseTakes(t *testing.T, h Harness, queue string, takes ...store.TenantTake) []store.Grant {
	t.Helper()
	grants, err := h.Store().Lease(t.Context(), leaseRequest(queue, takes...))
	if err != nil {
		t.Fatalf("Lease(%s, %v): %v", queue, takes, err)
	}
	return grants
}

// leaseFrom leases up to limit jobs of one tenant; tenant "" leases in FIFO
// order across tenants.
func leaseFrom(t *testing.T, h Harness, queue, tenant string, limit int) []store.Grant {
	t.Helper()
	return leaseTakes(t, h, queue, store.TenantTake{Tenant: tenant, Limit: limit})
}

// leaseOne enqueues s and leases it.
func leaseOne(t *testing.T, h Harness, s job.Spec) store.Grant {
	t.Helper()
	id := enqueue(t, h, s)[0]
	grants := leaseFrom(t, h, s.Queue, s.Tenant, 1)
	if len(grants) != 1 || grants[0].Job.ID != id {
		t.Fatalf("leasing the job just enqueued as %d returned %v", id, grants)
	}
	return grants[0]
}

// refOf is the reference with which the holder of g acknowledges it.
func refOf(g store.Grant) store.Ref {
	return store.Ref{ID: g.Job.ID, Queue: g.Job.Queue, Token: g.Job.Epoch}
}

// failRetryably fails the job of ref for a retry at once, which must succeed.
func failRetryably(t *testing.T, h Harness, ref store.Ref) {
	t.Helper()
	if got := failOne(t, h, store.FailRequest{Ref: ref, Error: "retry"}); got != job.Applied {
		t.Fatalf("Fail(%v) = %v, want applied", ref, got)
	}
}

// relet takes g's job away from its holder by failing it for an immediate
// retry, and leases it again to someone else. The token of g is stale
// afterwards.
func relet(t *testing.T, h Harness, g store.Grant) store.Grant {
	t.Helper()
	failRetryably(t, h, refOf(g))
	again := leaseFrom(t, h, g.Job.Queue, g.Job.Tenant, 1)
	if len(again) != 1 || again[0].Job.ID != g.Job.ID {
		t.Fatalf("leasing job %d again returned %v", g.Job.ID, again)
	}
	return again[0]
}

// heartbeatOne extends one lease.
func heartbeatOne(t *testing.T, h Harness, ref store.Ref, ttl time.Duration) store.HeartbeatResult {
	t.Helper()
	res, err := h.Store().Heartbeat(t.Context(), []store.Ref{ref}, ttl)
	if err != nil || len(res) != 1 {
		t.Fatalf("Heartbeat(%v, %v) = %v, %v", ref, ttl, res, err)
	}
	return res[0]
}

// completeOne completes one job.
func completeOne(t *testing.T, h Harness, ref store.Ref, result []byte) job.Outcome {
	t.Helper()
	out, err := h.Store().Complete(t.Context(), []store.CompleteRequest{{Ref: ref, Result: result}})
	if err != nil || len(out) != 1 {
		t.Fatalf("Complete(%v) = %v, %v", ref, out, err)
	}
	return out[0]
}

// failOne fails one job.
func failOne(t *testing.T, h Harness, req store.FailRequest) job.Outcome {
	t.Helper()
	out, err := h.Store().Fail(t.Context(), []store.FailRequest{req})
	if err != nil || len(out) != 1 {
		t.Fatalf("Fail(%v) = %v, %v", req.Ref, out, err)
	}
	return out[0]
}

// releaseOne releases one job.
func releaseOne(t *testing.T, h Harness, ref store.Ref) job.Outcome {
	t.Helper()
	out, err := h.Store().Release(t.Context(), []store.Ref{ref})
	if err != nil || len(out) != 1 {
		t.Fatalf("Release(%v) = %v, %v", ref, out, err)
	}
	return out[0]
}

// wantInvalid fails unless err wraps job.ErrInvalid.
func wantInvalid(t *testing.T, err error, what string) {
	t.Helper()
	if !errors.Is(err, job.ErrInvalid) {
		t.Errorf("%s: error = %v, want one wrapping job.ErrInvalid", what, err)
	}
}

// wantOutcome fails unless got is want.
func wantOutcome(t *testing.T, got, want job.Outcome, what string) {
	t.Helper()
	if got != want {
		t.Errorf("%s: outcome = %v, want %v", what, got, want)
	}
}

// wantState fails unless job id is in state.
func wantState(t *testing.T, h Harness, id int64, state job.State, when string) job.Job {
	t.Helper()
	j := get(t, h, id)
	if j.State != state {
		t.Errorf("%s: job %d is %v, want %v", when, id, j.State, state)
	}
	return j
}

// wantTime fails unless got is the instant want.
func wantTime(t *testing.T, got, want time.Time, what string) {
	t.Helper()
	if !got.Equal(want) {
		t.Errorf("%s = %v, want %v", what, got, want)
	}
}

// ids lists the ids of grants in order.
func ids(grants []store.Grant) []int64 {
	out := make([]int64, len(grants))
	for i, g := range grants {
		out[i] = g.Job.ID
	}
	return out
}
