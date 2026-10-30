package storetest

import (
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

// RunLease runs the conformance subtests of Lease.
func RunLease(t *testing.T, nh NewHarness) {
	run(t, nh, "LeaseRespectsRunAt", leaseRespectsRunAt)
	run(t, nh, "LeaseOrdersByRunAtThenIDWithinTenant", leaseOrdersByRunAtThenIDWithinTenant)
	run(t, nh, "LeaseHonorsPerTenantLimits", leaseHonorsPerTenantLimits)
	run(t, nh, "LeaseGrantsAreOrderedByRunAtThenIDAcrossTakes", leaseGrantsAreOrderedByRunAtThenIDAcrossTakes)
	run(t, nh, "LeaseFIFOAcrossTenants", leaseFIFOAcrossTenants)
	run(t, nh, "LeaseIncrementsEpochAndAttempts", leaseIncrementsEpochAndAttempts)
	run(t, nh, "LeaseSetsExpiryAndRemaining", leaseSetsExpiryAndRemaining)
	run(t, nh, "LeaseRejectsInvalidRequests", leaseRejectsInvalidRequests)
	run(t, nh, "LeaseSkipsOtherQueues", leaseSkipsOtherQueues)
}

func leaseRespectsRunAt(t *testing.T, h Harness) {
	s := newSpec("q1", "t1")
	s.Delay = 10 * time.Second
	id := enqueue(t, h, s)[0]
	if got := leaseFrom(t, h, "q1", "t1", 5); len(got) != 0 {
		t.Fatalf("leased %v before the job's run_at", ids(got))
	}
	h.Advance(10*time.Second - time.Microsecond)
	if got := leaseFrom(t, h, "q1", "t1", 5); len(got) != 0 {
		t.Fatalf("leased %v one microsecond before the job's run_at", ids(got))
	}
	h.Advance(time.Microsecond)
	got := leaseFrom(t, h, "q1", "t1", 5)
	if !slices.Equal(ids(got), []int64{id}) {
		t.Errorf("leased %v at exactly run_at, want [%d]", ids(got), id)
	}
}

func leaseOrdersByRunAtThenIDWithinTenant(t *testing.T, h Harness) {
	// Delays in seconds, by position 0..4. All are due after 5 seconds, and
	// the order is then by run_at, with the id breaking the tie of positions
	// 1 and 3.
	var specs []job.Spec
	for _, d := range []int{3, 0, 1, 0, 2} {
		s := newSpec("q1", "t1")
		s.Delay = time.Duration(d) * time.Second
		specs = append(specs, s)
	}
	id := enqueue(t, h, specs...)
	h.Advance(5 * time.Second)
	if got, want := ids(leaseFrom(t, h, "q1", "t1", 3)), []int64{id[1], id[3], id[2]}; !slices.Equal(got, want) {
		t.Errorf("the first three leased = %v, want %v", got, want)
	}
	if got, want := ids(leaseFrom(t, h, "q1", "t1", 10)), []int64{id[4], id[0]}; !slices.Equal(got, want) {
		t.Errorf("the rest leased = %v, want %v", got, want)
	}
}

func leaseHonorsPerTenantLimits(t *testing.T, h Harness) {
	var specs []job.Spec
	for range 5 {
		specs = append(specs, newSpec("q1", "a"), newSpec("q1", "b"))
	}
	id := enqueue(t, h, specs...) // a is at even positions 0..8, b at odd ones
	grants := leaseTakes(t, h, "q1", store.TenantTake{Tenant: "a", Limit: 2}, store.TenantTake{Tenant: "b", Limit: 3})
	if got, want := ids(grants), []int64{id[0], id[1], id[2], id[3], id[5]}; !slices.Equal(got, want) {
		t.Errorf("leased %v, want %v: 2 of tenant a and 3 of tenant b, in (run_at, id) order", got, want)
	}
	for _, g := range grants {
		if g.Job.State != job.StateLeased {
			t.Errorf("job %d came back %v, want leased", g.Job.ID, g.Job.State)
		}
	}
}

func leaseGrantsAreOrderedByRunAtThenIDAcrossTakes(t *testing.T, h Harness) {
	// Tenant a's job has the lowest id but runs last. Tenants b and c run at
	// the same instant, c with the lower id. The takes are asked for in the
	// order a, b, c, which is neither the order of the ids nor of the run_at
	// values, so the grants can only come back right if they are sorted by
	// (run_at, id) across the takes.
	later := newSpec("q1", "a")
	later.Delay = 10 * time.Second
	id := enqueue(t, h, later, newSpec("q1", "c"), newSpec("q1", "b"))
	h.Advance(10 * time.Second)
	grants := leaseTakes(t, h, "q1",
		store.TenantTake{Tenant: "a", Limit: 1}, store.TenantTake{Tenant: "b", Limit: 1}, store.TenantTake{Tenant: "c", Limit: 1})
	if got, want := ids(grants), []int64{id[1], id[2], id[0]}; !slices.Equal(got, want) {
		t.Errorf("grants = %v, want %v: by (run_at, id) across takes, not by take or by id", got, want)
	}
}

func leaseFIFOAcrossTenants(t *testing.T, h Harness) {
	id := enqueue(t, h, newSpec("q1", "a"), newSpec("q1", "b"), newSpec("q1", "a"))
	got := ids(leaseFrom(t, h, "q1", "", 2))
	if want := []int64{id[0], id[1]}; !slices.Equal(got, want) {
		t.Errorf("a FIFO take of 2 leased %v, want the first two by id %v", got, want)
	}
}

func leaseIncrementsEpochAndAttempts(t *testing.T, h Harness) {
	first := leaseOne(t, h, newSpec("q1", "t1"))
	if first.Job.Epoch != 1 || first.Job.Attempts != 1 {
		t.Fatalf("after the first lease Epoch = %d, Attempts = %d, want 1 and 1", first.Job.Epoch, first.Job.Attempts)
	}
	second := relet(t, h, first)
	if second.Job.Epoch != 2 || second.Job.Attempts != 2 {
		t.Errorf("after the second lease Epoch = %d, Attempts = %d, want 2 and 2", second.Job.Epoch, second.Job.Attempts)
	}
	if got := get(t, h, first.Job.ID); got.Epoch != 2 || got.Attempts != 2 {
		t.Errorf("stored Epoch = %d, Attempts = %d, want 2 and 2", got.Epoch, got.Attempts)
	}
}

func leaseSetsExpiryAndRemaining(t *testing.T, h Harness) {
	id := enqueue(t, h, newSpec("q1", "t1"))[0]
	req := leaseRequest("q1", store.TenantTake{Tenant: "t1", Limit: 1})
	req.TTL = 45 * time.Second
	req.Worker = "worker-7"
	grants, err := h.Store().Lease(t.Context(), req)
	if err != nil || len(grants) != 1 {
		t.Fatalf("Lease = %v, %v", grants, err)
	}
	g := grants[0]
	if g.Remaining != req.TTL {
		t.Errorf("Remaining = %v, want %v", g.Remaining, req.TTL)
	}
	wantTime(t, g.Job.LeaseExpiresAt, h.Now().Add(req.TTL), "LeaseExpiresAt")
	if g.Job.LeasedBy != "worker-7" || g.Job.State != job.StateLeased {
		t.Errorf("granted job is %v leased by %q, want leased by worker-7", g.Job.State, g.Job.LeasedBy)
	}
	if stored := get(t, h, id); !job.Equal(stored, g.Job) {
		t.Errorf("stored job %+v differs from the granted one %+v", stored, g.Job)
	}
}

func leaseRejectsInvalidRequests(t *testing.T, h Harness) {
	enqueue(t, h, newSpec("q1", "a"), newSpec("q1", "b"))
	take := func(tenant string, limit int) store.TenantTake { return store.TenantTake{Tenant: tenant, Limit: limit} }
	type edit func(r *store.LeaseRequest)
	cases := map[string]edit{
		"TTL 0":                               func(r *store.LeaseRequest) { r.TTL = 0 },
		"TTL below the minimum":               func(r *store.LeaseRequest) { r.TTL = job.MinTTL - time.Microsecond },
		"TTL above the maximum":               func(r *store.LeaseRequest) { r.TTL = job.MaxTTL + time.Microsecond },
		"TTL with a sub-microsecond part":     func(r *store.LeaseRequest) { r.TTL = 5*time.Second + time.Nanosecond },
		"takes summing to 65":                 func(r *store.LeaseRequest) { r.Takes = []store.TenantTake{take("a", 30), take("b", 35)} },
		"one take of 65":                      func(r *store.LeaseRequest) { r.Takes = []store.TenantTake{take("a", 65)} },
		"a negative limit hiding a large one": func(r *store.LeaseRequest) { r.Takes = []store.TenantTake{take("a", 70), take("b", -10)} },
		"a zero limit":                        func(r *store.LeaseRequest) { r.Takes = []store.TenantTake{take("a", 0)} },
		"no takes":                            func(r *store.LeaseRequest) { r.Takes = nil },
		"an empty worker":                     func(r *store.LeaseRequest) { r.Worker = "" },
		"a worker with a space":               func(r *store.LeaseRequest) { r.Worker = "w 1" },
		"an empty tenant next to another":     func(r *store.LeaseRequest) { r.Takes = []store.TenantTake{take("", 1), take("b", 1)} },
	}
	for _, name := range badNames() {
		cases["tenant "+strconv.Quote(name)+" in a take"] = func(r *store.LeaseRequest) {
			r.Takes = []store.TenantTake{take("a", 1), take(name, 1)}
		}
		cases["queue "+strconv.Quote(name)] = func(r *store.LeaseRequest) { r.Queue = name }
	}
	for name, apply := range cases {
		req := leaseRequest("q1", take("a", 1), take("b", 1))
		apply(&req)
		_, err := h.Store().Lease(t.Context(), req)
		wantInvalid(t, err, name)
	}
	for _, j := range snapshot(t, h) {
		if j.State != job.StateReady {
			t.Errorf("job %d is %v after only invalid requests, want ready", j.ID, j.State)
		}
	}
	if got := leaseTakes(t, h, "q1", take("", job.MaxBatch)); len(got) != 2 {
		t.Errorf("a valid FIFO request of %d leased %d jobs, want 2", job.MaxBatch, len(got))
	}
}

func leaseSkipsOtherQueues(t *testing.T, h Harness) {
	ids1 := enqueue(t, h, newSpec("q1", "t1"), newSpec("q2", "t1"), newSpec("q1", "t1"))
	if got := ids(leaseFrom(t, h, "q2", "t1", 10)); !slices.Equal(got, []int64{ids1[1]}) {
		t.Errorf("leasing queue q2 returned %v, want only [%d]", got, ids1[1])
	}
	if got := ids(leaseFrom(t, h, "q2", "", 10)); len(got) != 0 {
		t.Errorf("a FIFO take on queue q2 returned %v after its only job was leased", got)
	}
	if got := ids(leaseFrom(t, h, "q1", "", 10)); !slices.Equal(got, []int64{ids1[0], ids1[2]}) {
		t.Errorf("leasing queue q1 returned %v, want [%d %d]", got, ids1[0], ids1[2])
	}
}
