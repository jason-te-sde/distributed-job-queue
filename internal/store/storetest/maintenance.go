package storetest

import (
	"errors"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

// RunMaintenance runs the conformance subtests of Cancel, Requeue, Reap,
// Prune, Purge, ListDead, Stats and Activity.
func RunMaintenance(t *testing.T, nh NewHarness) {
	run(t, nh, "CancelReadyAndLeased", cancelReadyAndLeased)
	run(t, nh, "CancelTerminalIsWrongState", cancelTerminalIsWrongState)
	run(t, nh, "CancelTwiceIsDuplicate", cancelTwiceIsDuplicate)
	run(t, nh, "CancelThenHeartbeatFenced", cancelThenHeartbeatFenced)
	run(t, nh, "CancelTenantScope", cancelTenantScope)
	run(t, nh, "ReapExpiredRetriesWithBackoff", reapExpiredRetriesWithBackoff)
	run(t, nh, "ReapAtExactExpiry", reapAtExactExpiry)
	run(t, nh, "ReapExhaustedGoesDead", reapExhaustedGoesDead)
	run(t, nh, "ReapVisitsInIDOrderAndHonorsLimit", reapVisitsInIDOrderAndHonorsLimit)
	run(t, nh, "ReapSkipsLive", reapSkipsLive)
	run(t, nh, "ReapRejectsTheWholeCallOnAnInvalidBackoff", reapRejectsTheWholeCallOnAnInvalidBackoff)
	run(t, nh, "RequeueDead", requeueDead)
	run(t, nh, "RequeueNonDeadWrongState", requeueNonDeadWrongState)
	run(t, nh, "PruneHonorsRetention", pruneHonorsRetention)
	run(t, nh, "PruneDeadUsesDeadRetention", pruneDeadUsesDeadRetention)
	run(t, nh, "PruneLimit", pruneLimit)
	run(t, nh, "EnqueueKeyReusableAfterPrune", enqueueKeyReusableAfterPrune)
	run(t, nh, "PurgeTerminalOnly", purgeTerminalOnly)
	run(t, nh, "ListDeadPaginates", listDeadPaginates)
	run(t, nh, "ListDeadReturnsCopies", listDeadReturnsCopies)
	run(t, nh, "StatsCountsPerTenant", statsCountsPerTenant)
	run(t, nh, "ActivityListsTenantsWithDueWork", activityListsTenantsWithDueWork)
	run(t, nh, "ActivityNextDueCoversTenantsWithDueWork", activityNextDueCoversTenantsWithDueWork)
	run(t, nh, "ActivityListsOnlyTenantsThatLeaseCanServe", activityListsOnlyTenantsThatLeaseCanServe)
	run(t, nh, "MaintenanceRejectsInvalidArguments", maintenanceRejectsInvalidArguments)
}

// retention is the default retention of the queue: 24 hours for succeeded
// and cancelled jobs, 14 days for dead ones.
func retention() store.Retention {
	return store.Retention{Done: 24 * time.Hour, Dead: 336 * time.Hour}
}

// secondsPerAttempt is the backoff the Reap subtests use: one second for
// each attempt used.
func secondsPerAttempt(attempts int32) time.Duration { return time.Duration(attempts) * time.Second }

// cancelOne cancels one job.
func cancelOne(t *testing.T, h Harness, id int64, tenant string) job.Outcome {
	t.Helper()
	out, err := h.Store().Cancel(t.Context(), id, tenant)
	if err != nil {
		t.Fatalf("Cancel(%d, %q): %v", id, tenant, err)
	}
	return out
}

// requeueOne requeues one job.
func requeueOne(t *testing.T, h Harness, id int64) job.Outcome {
	t.Helper()
	out, err := h.Store().Requeue(t.Context(), id)
	if err != nil {
		t.Fatalf("Requeue(%d): %v", id, err)
	}
	return out
}

// reap reaps with the one-second-per-attempt backoff and returns how many
// jobs it took back.
func reap(t *testing.T, h Harness, limit int) int {
	t.Helper()
	n, err := h.Store().Reap(t.Context(), limit, secondsPerAttempt)
	if err != nil {
		t.Fatalf("Reap(%d): %v", limit, err)
	}
	return n
}

// prune prunes with the default retention and returns how many rows it
// deleted.
func prune(t *testing.T, h Harness, limit int) int {
	t.Helper()
	n, err := h.Store().Prune(t.Context(), retention(), limit)
	if err != nil {
		t.Fatalf("Prune(%d): %v", limit, err)
	}
	return n
}

// wantCount fails unless got is want.
func wantCount(t *testing.T, got, want int, what string) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %d, want %d", what, got, want)
	}
}

// succeeded enqueues a job for q1 and t1 and completes it, returning its id.
func succeeded(t *testing.T, h Harness) int64 {
	t.Helper()
	g := leaseOne(t, h, newSpec("q1", "t1"))
	wantOutcome(t, completeOne(t, h, refOf(g), nil), job.Applied, "Complete")
	return g.Job.ID
}

// dead enqueues a job for q1 and t1 and fails it for good, returning its id.
func dead(t *testing.T, h Harness) int64 {
	t.Helper()
	g := leaseOne(t, h, newSpec("q1", "t1"))
	wantOutcome(t, failOne(t, h, store.FailRequest{Ref: refOf(g), Permanent: true, Error: "boom"}), job.Applied, "Fail")
	return g.Job.ID
}

func cancelReadyAndLeased(t *testing.T, h Harness) {
	ready := enqueue(t, h, newSpec("q1", "t1"))[0]
	h.Advance(time.Second)
	wantOutcome(t, cancelOne(t, h, ready, ""), job.Applied, "Cancel of a ready job")
	j := wantState(t, h, ready, job.StateCancelled, "after cancelling a ready job")
	wantTime(t, j.FinishedAt, h.Now(), "FinishedAt of the cancelled ready job")

	g := leaseOne(t, h, newSpec("q1", "t1"))
	h.Advance(time.Second)
	wantOutcome(t, cancelOne(t, h, g.Job.ID, "t1"), job.Applied, "Cancel of a leased job")
	j = wantState(t, h, g.Job.ID, job.StateCancelled, "after cancelling a leased job")
	if j.LeasedBy != "" || !j.LeaseExpiresAt.IsZero() {
		t.Errorf("cancelled job still has LeasedBy %q, LeaseExpiresAt %v", j.LeasedBy, j.LeaseExpiresAt)
	}
	wantTime(t, j.FinishedAt, h.Now(), "FinishedAt of the cancelled leased job")
}

func cancelTerminalIsWrongState(t *testing.T, h Harness) {
	done, failed := succeeded(t, h), dead(t, h)
	wantOutcome(t, cancelOne(t, h, done, ""), job.WrongState, "Cancel of a succeeded job")
	wantOutcome(t, cancelOne(t, h, failed, ""), job.WrongState, "Cancel of a dead job")
	wantState(t, h, done, job.StateSucceeded, "after the refused Cancel")
	wantState(t, h, failed, job.StateDead, "after the refused Cancel")
}

func cancelTwiceIsDuplicate(t *testing.T, h Harness) {
	id := enqueue(t, h, newSpec("q1", "t1"))[0]
	wantOutcome(t, cancelOne(t, h, id, ""), job.Applied, "first Cancel")
	h.Advance(time.Second)
	wantOutcome(t, cancelOne(t, h, id, ""), job.Duplicate, "second Cancel")
	wantTime(t, get(t, h, id).FinishedAt, h.Now().Add(-time.Second), "FinishedAt after the second Cancel")
}

func cancelThenHeartbeatFenced(t *testing.T, h Harness) {
	g := leaseOne(t, h, newSpec("q1", "t1"))
	wantOutcome(t, cancelOne(t, h, g.Job.ID, ""), job.Applied, "Cancel")
	wantOutcome(t, heartbeatOne(t, h, refOf(g), 30*time.Second).Outcome, job.Fenced, "Heartbeat after Cancel")
	wantOutcome(t, completeOne(t, h, refOf(g), nil), job.Fenced, "Complete after Cancel")
	wantState(t, h, g.Job.ID, job.StateCancelled, "after the fenced calls")
}

func cancelTenantScope(t *testing.T, h Harness) {
	id := enqueue(t, h, newSpec("q1", "t1"))[0]
	wantOutcome(t, cancelOne(t, h, id, "other"), job.NotFound, "Cancel under another tenant")
	wantState(t, h, id, job.StateReady, "after Cancel under another tenant")
	wantOutcome(t, cancelOne(t, h, id+100, ""), job.NotFound, "Cancel of an unknown id")
	wantOutcome(t, cancelOne(t, h, id, "t1"), job.Applied, "Cancel under the job's tenant")
}

func reapExpiredRetriesWithBackoff(t *testing.T, h Harness) {
	g := leaseOne(t, h, newSpec("q1", "t1"))
	h.Advance(31 * time.Second)
	wantCount(t, reap(t, h, 10), 1, "Reap")
	j := wantState(t, h, g.Job.ID, job.StateReady, "after Reap")
	wantTime(t, j.RunAt, h.Now().Add(time.Second), "RunAt: now plus the backoff of 1 attempt")
	if j.LastError != "lease expired" || j.LeasedBy != "" || !j.LeaseExpiresAt.IsZero() || j.Attempts != 1 {
		t.Errorf("reaped job has LastError %q, LeasedBy %q, LeaseExpiresAt %v, Attempts %d; want %q, no lease and 1 attempt",
			j.LastError, j.LeasedBy, j.LeaseExpiresAt, j.Attempts, "lease expired")
	}
	wantOutcome(t, completeOne(t, h, refOf(g), nil), job.Fenced, "Complete with the reaped lease's token")
}

func reapAtExactExpiry(t *testing.T, h Harness) {
	g := leaseOne(t, h, newSpec("q1", "t1"))
	h.Advance(30*time.Second - time.Microsecond)
	wantCount(t, reap(t, h, 10), 0, "Reap one microsecond before expiry")
	wantState(t, h, g.Job.ID, job.StateLeased, "one microsecond before expiry")
	h.Advance(time.Microsecond)
	wantCount(t, reap(t, h, 10), 1, "Reap exactly at expiry")
	wantState(t, h, g.Job.ID, job.StateReady, "at expiry")
}

func reapExhaustedGoesDead(t *testing.T, h Harness) {
	s := newSpec("q1", "t1")
	s.MaxAttempts = 1
	g := leaseOne(t, h, s)
	h.Advance(31 * time.Second)
	wantCount(t, reap(t, h, 10), 1, "Reap")
	j := wantState(t, h, g.Job.ID, job.StateDead, "after reaping the last attempt")
	wantTime(t, j.FinishedAt, h.Now(), "FinishedAt")
	if j.LastError != "lease expired" {
		t.Errorf("LastError = %q, want %q", j.LastError, "lease expired")
	}
}

func reapVisitsInIDOrderAndHonorsLimit(t *testing.T, h Harness) {
	// Job k belongs to tenant k, and is failed k-1 times for an immediate
	// retry, so that its attempts tell the order of the backoff calls. Then
	// each job is leased once more, the highest id first and a second apart,
	// so that the leases expire in the opposite order of the ids. Ascending id
	// is therefore not the order of (expiry, id).
	tenants := []string{"t1", "t2", "t3", "t4", "t5"}
	var specs []job.Spec
	for _, tenant := range tenants {
		specs = append(specs, newSpec("q1", tenant))
	}
	id := enqueue(t, h, specs...)
	for i, tenant := range tenants {
		for range i {
			failRetryably(t, h, refOf(leaseExactlyOne(t, h, "q1", tenant)))
		}
	}
	for i := len(tenants) - 1; i >= 0; i-- {
		if got := leaseFrom(t, h, "q1", tenants[i], 1); len(got) != 1 || got[0].Job.Attempts != int32(i+1) {
			t.Fatalf("test setup: the last lease of job %d = %v, want one grant with %d attempts", id[i], got, i+1)
		}
		if i > 0 {
			h.Advance(time.Second)
		}
	}
	h.Advance(30 * time.Second) // the lease that was granted last expires exactly now

	var calls []int32
	backoff := func(attempts int32) time.Duration {
		calls = append(calls, attempts)
		return secondsPerAttempt(attempts)
	}
	n, err := h.Store().Reap(t.Context(), 3, backoff)
	if err != nil || n != 3 {
		t.Fatalf("Reap with limit 3 = %d, %v; want 3", n, err)
	}
	if want := []int32{1, 2, 3}; !slices.Equal(calls, want) {
		t.Errorf("backoff was called with %v, want %v: the attempts of the three lowest ids, in id order, although the leases of the highest ids expired first", calls, want)
	}
	for i, state := range []job.State{job.StateReady, job.StateReady, job.StateReady, job.StateLeased, job.StateLeased} {
		wantState(t, h, id[i], state, "after Reap with limit 3")
	}
	wantTime(t, get(t, h, id[2]).RunAt, h.Now().Add(3*time.Second), "RunAt of the third job: now plus its 3 attempts of backoff")

	calls = nil
	n, err = h.Store().Reap(t.Context(), 3, backoff)
	if err != nil || n != 2 {
		t.Fatalf("the next Reap = %d, %v; want 2", n, err)
	}
	if want := []int32{4, 5}; !slices.Equal(calls, want) {
		t.Errorf("the next Reap called backoff with %v, want %v", calls, want)
	}
}

func reapSkipsLive(t *testing.T, h Harness) {
	live := leaseOne(t, h, newSpec("q1", "t1"))
	done := succeeded(t, h)
	ready := enqueue(t, h, newSpec("q1", "t1"))[0]
	h.Advance(29 * time.Second)
	n, err := h.Store().Reap(t.Context(), 10, func(int32) time.Duration {
		t.Error("backoff was called although no lease has expired")
		return 0
	})
	if err != nil || n != 0 {
		t.Fatalf("Reap = %d, %v; want 0", n, err)
	}
	wantState(t, h, live.Job.ID, job.StateLeased, "after Reap of a live lease")
	wantState(t, h, done, job.StateSucceeded, "after Reap")
	wantState(t, h, ready, job.StateReady, "after Reap")
}

func reapRejectsTheWholeCallOnAnInvalidBackoff(t *testing.T, h Harness) {
	enqueue(t, h, newSpec("q1", "t1"), newSpec("q1", "t1"))
	if got := leaseFrom(t, h, "q1", "t1", 2); len(got) != 2 {
		t.Fatalf("test setup: leased %d jobs, want 2", len(got))
	}
	h.Advance(31 * time.Second) // both leases have run out
	before := snapshot(t, h)
	for name, bad := range map[string]time.Duration{
		"a negative delay":        -time.Second,
		"a sub-microsecond delay": time.Second + time.Nanosecond,
	} {
		asked := 0
		_, err := h.Store().Reap(t.Context(), 10, func(int32) time.Duration {
			asked++
			if asked == 1 {
				return time.Second
			}
			return bad
		})
		wantInvalid(t, err, "Reap with a valid delay for the first row and "+name+" for the second")
		after := snapshot(t, h)
		if len(after) != len(before) {
			t.Fatalf("Reap with %s changed the number of rows from %d to %d", name, len(before), len(after))
		}
		for i := range before {
			if !job.Equal(before[i], after[i]) {
				t.Errorf("Reap with %s changed job %d from %+v to %+v: an invalid delay must reject the call before any row changes",
					name, before[i].ID, before[i], after[i])
			}
		}
	}
}

func requeueDead(t *testing.T, h Harness) {
	id := dead(t, h)
	h.Advance(10 * time.Second)
	wantOutcome(t, requeueOne(t, h, id), job.Applied, "Requeue")
	j := wantState(t, h, id, job.StateReady, "after Requeue")
	if j.Attempts != 0 || j.LastError != "boom" || !j.FinishedAt.IsZero() || len(j.Result) != 0 {
		t.Errorf("requeued job has Attempts %d, LastError %q, FinishedAt %v, Result %q; want 0, the old error, no finish and no result",
			j.Attempts, j.LastError, j.FinishedAt, j.Result)
	}
	wantTime(t, j.RunAt, h.Now(), "RunAt")
	if got := leaseFrom(t, h, "q1", "t1", 1); len(got) != 1 || got[0].Job.ID != id {
		t.Errorf("the requeued job was not leasable: %v", ids(got))
	}
}

func requeueNonDeadWrongState(t *testing.T, h Harness) {
	done := succeeded(t, h)
	leased := leaseOne(t, h, newSpec("q1", "t2")).Job.ID
	cancelled := enqueue(t, h, newSpec("q1", "t3"))[0]
	wantOutcome(t, cancelOne(t, h, cancelled, ""), job.Applied, "Cancel")
	ready := enqueue(t, h, newSpec("q1", "t1"))[0]
	for name, id := range map[string]int64{"ready": ready, "leased": leased, "succeeded": done, "cancelled": cancelled} {
		wantOutcome(t, requeueOne(t, h, id), job.WrongState, "Requeue of a "+name+" job")
	}
	wantOutcome(t, requeueOne(t, h, 9999), job.NotFound, "Requeue of an unknown id")
	wantState(t, h, done, job.StateSucceeded, "after the refused Requeue")
}

func pruneHonorsRetention(t *testing.T, h Harness) {
	done := succeeded(t, h)
	cancelled := enqueue(t, h, newSpec("q1", "t1"))[0]
	wantOutcome(t, cancelOne(t, h, cancelled, ""), job.Applied, "Cancel")
	h.Advance(24 * time.Hour)
	wantCount(t, prune(t, h, 10), 0, "Prune exactly at the retention")
	h.Advance(time.Microsecond)
	wantCount(t, prune(t, h, 10), 2, "Prune one microsecond past the retention")
	for _, id := range []int64{done, cancelled} {
		if _, err := h.Store().Get(t.Context(), id, ""); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("Get(%d) after Prune: error = %v, want store.ErrNotFound", id, err)
		}
	}
}

func pruneDeadUsesDeadRetention(t *testing.T, h Harness) {
	id := dead(t, h)
	h.Advance(24*time.Hour + time.Microsecond)
	wantCount(t, prune(t, h, 10), 0, "Prune of a dead job past the retention of succeeded ones")
	h.Advance(312*time.Hour - time.Microsecond) // 336h since it died
	wantCount(t, prune(t, h, 10), 0, "Prune of a dead job exactly at the dead retention")
	h.Advance(time.Microsecond)
	wantCount(t, prune(t, h, 10), 1, "Prune of a dead job past the dead retention")
	if _, err := h.Store().Get(t.Context(), id, ""); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Get after Prune: error = %v, want store.ErrNotFound", err)
	}
}

func pruneLimit(t *testing.T, h Harness) {
	var finished []int64
	for range 5 {
		finished = append(finished, succeeded(t, h))
	}
	live := leaseOne(t, h, newSpec("q1", "t1")).Job.ID
	ready := enqueue(t, h, newSpec("q1", "t1"))[0]
	h.Advance(25 * time.Hour)
	wantCount(t, prune(t, h, 2), 2, "Prune with limit 2")
	left := rowIDs(snapshot(t, h))
	if want := []int64{finished[2], finished[3], finished[4], live, ready}; !slices.Equal(left, want) {
		t.Errorf("rows left after Prune with limit 2 = %v, want %v: the lowest ids go first, and unfinished jobs stay", left, want)
	}
	wantCount(t, prune(t, h, 10), 3, "the next Prune")
	wantRows(t, h, 2, "after both Prunes")
}

func enqueueKeyReusableAfterPrune(t *testing.T, h Harness) {
	s := newSpec("q1", "t1")
	s.IdempotencyKey = "k"
	g := leaseOne(t, h, s)
	wantOutcome(t, completeOne(t, h, refOf(g), nil), job.Applied, "Complete")
	again, err := h.Store().Enqueue(t.Context(), []job.Spec{s})
	if err != nil || again[0].ID != g.Job.ID || !again[0].Deduplicated {
		t.Fatalf("Enqueue of the key while its job exists = %v, %v; want the same id, deduplicated", again, err)
	}
	h.Advance(24*time.Hour + time.Microsecond)
	wantCount(t, prune(t, h, 10), 1, "Prune")
	res, err := h.Store().Enqueue(t.Context(), []job.Spec{s})
	if err != nil || len(res) != 1 {
		t.Fatalf("Enqueue after Prune = %v, %v", res, err)
	}
	if res[0].Deduplicated || res[0].ID <= g.Job.ID {
		t.Errorf("Enqueue of the key after its job was pruned = %+v, want a new id above %d, not deduplicated", res[0], g.Job.ID)
	}
}

func purgeTerminalOnly(t *testing.T, h Harness) {
	s := h.Store()
	ctx := t.Context()
	before := h.Now()
	// Queue q1 gets two succeeded jobs, one dead job and one ready job;
	// queue q2 gets one succeeded job.
	var done []int64
	for range 2 {
		done = append(done, succeeded(t, h))
	}
	deadID := dead(t, h)
	ready := enqueue(t, h, newSpec("q1", "t1"))[0]
	other := leaseOne(t, h, newSpec("q2", "t1"))
	wantOutcome(t, completeOne(t, h, refOf(other), nil), job.Applied, "Complete")
	h.Advance(time.Second)

	for _, state := range []job.State{job.StateReady, job.StateLeased, 0} {
		_, err := s.Purge(ctx, store.PurgeRequest{Queue: "q1", State: state, FinishedBefore: h.Now().Add(time.Hour)})
		wantInvalid(t, err, "Purge of state "+state.String())
	}
	_, err := s.Purge(ctx, store.PurgeRequest{Queue: "a b", State: job.StateSucceeded, FinishedBefore: h.Now()})
	wantInvalid(t, err, "Purge of a bad queue name")
	wantRows(t, h, 5, "after the rejected Purges")

	n, err := s.Purge(ctx, store.PurgeRequest{Queue: "q1", State: job.StateSucceeded, FinishedBefore: before})
	if err != nil || n != 0 {
		t.Fatalf("Purge of jobs finished before the instant they finished = %d, %v; want 0: the bound is exclusive", n, err)
	}
	n, err = s.Purge(ctx, store.PurgeRequest{Queue: "q1", State: job.StateSucceeded, FinishedBefore: h.Now().Add(time.Microsecond)})
	if err != nil || n != 2 {
		t.Fatalf("Purge of succeeded jobs of q1 = %d, %v; want 2", n, err)
	}
	left := rowIDs(snapshot(t, h))
	if want := []int64{deadID, ready, other.Job.ID}; !slices.Equal(left, want) {
		t.Errorf("rows left after Purge = %v, want %v (done were %v): only succeeded jobs of q1 go", left, want, done)
	}
	n, err = s.Purge(ctx, store.PurgeRequest{Queue: "q1", State: job.StateDead, FinishedBefore: h.Now().Add(time.Microsecond)})
	if err != nil || n != 1 {
		t.Errorf("Purge of dead jobs of q1 = %d, %v; want 1", n, err)
	}
}

func listDeadPaginates(t *testing.T, h Harness) {
	var specs []job.Spec
	for range 5 {
		specs = append(specs, newSpec("q1", "t1"))
	}
	first := enqueue(t, h, specs...)
	grants := leaseFrom(t, h, "q1", "", 5)
	var fails []store.FailRequest
	for _, g := range grants {
		fails = append(fails, store.FailRequest{Ref: refOf(g), Permanent: true, Error: "boom"})
	}
	if _, err := h.Store().Fail(t.Context(), fails); err != nil {
		t.Fatal(err)
	}
	// Neighbours that ListDead must skip: a ready job of q1 and a dead job
	// of q2, both with higher ids.
	enqueue(t, h, newSpec("q1", "t1"))
	other := leaseOne(t, h, newSpec("q2", "t1"))
	wantOutcome(t, failOne(t, h, store.FailRequest{Ref: refOf(other), Permanent: true}), job.Applied, "Fail")

	list := func(after int64, limit int) []int64 {
		t.Helper()
		rows, err := h.Store().ListDead(t.Context(), "q1", after, limit)
		if err != nil {
			t.Fatalf("ListDead(q1, %d, %d): %v", after, limit, err)
		}
		out := make([]int64, len(rows))
		for i, r := range rows {
			if r.State != job.StateDead || r.Queue != "q1" {
				t.Errorf("ListDead returned job %d in state %v of queue %s", r.ID, r.State, r.Queue)
			}
			out[i] = r.ID
		}
		return out
	}
	if got, want := list(0, 2), []int64{first[0], first[1]}; !slices.Equal(got, want) {
		t.Errorf("first page = %v, want %v", got, want)
	}
	if got, want := list(first[1], 2), []int64{first[2], first[3]}; !slices.Equal(got, want) {
		t.Errorf("second page = %v, want %v", got, want)
	}
	if got, want := list(first[3], 2), []int64{first[4]}; !slices.Equal(got, want) {
		t.Errorf("third page = %v, want %v", got, want)
	}
	if got := list(first[4], 2); len(got) != 0 {
		t.Errorf("page after the last = %v, want none", got)
	}
}

func listDeadReturnsCopies(t *testing.T, h Harness) {
	s := newSpec("q1", "t1")
	s.Payload = []byte("original-payload")
	g := leaseOne(t, h, s)
	failOne(t, h, store.FailRequest{Ref: refOf(g), Permanent: true})
	rows, err := h.Store().ListDead(t.Context(), "q1", 0, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("ListDead = %v, %v", rows, err)
	}
	for i := range rows[0].Payload {
		rows[0].Payload[i] = '!'
	}
	if got := get(t, h, g.Job.ID).Payload; string(got) != "original-payload" {
		t.Errorf("after the caller changed the bytes ListDead returned, the stored payload is %q", got)
	}
}

func statsCountsPerTenant(t *testing.T, h Harness) {
	// Tenant a gets a leased, a dead and a succeeded job; tenant c only a
	// succeeded one. The jobs that stay ready are enqueued afterwards, three
	// seconds apart, and the clock then moves four seconds.
	enqueue(t, h, newSpec("q1", "a"), newSpec("q1", "a"), newSpec("q1", "a"), newSpec("q1", "c"))
	grants := leaseTakes(t, h, "q1", store.TenantTake{Tenant: "a", Limit: 3}, store.TenantTake{Tenant: "c", Limit: 1})
	if len(grants) != 4 {
		t.Fatalf("leased %d jobs, want 4", len(grants))
	}
	wantOutcome(t, failOne(t, h, store.FailRequest{Ref: refOf(grants[1]), Permanent: true}), job.Applied, "Fail")
	wantOutcome(t, completeOne(t, h, refOf(grants[2]), nil), job.Applied, "Complete")
	wantOutcome(t, completeOne(t, h, refOf(grants[3]), nil), job.Applied, "Complete")

	enqueue(t, h, newSpec("q1", "a"))
	delayed := newSpec("q1", "a")
	delayed.Delay = 30 * time.Second
	enqueue(t, h, delayed, newSpec("q2", "z"))
	h.Advance(3 * time.Second)
	atNow := newSpec("q1", "b")
	atNow.Delay = 4 * time.Second // due exactly when the clock has moved 4s more
	enqueue(t, h, newSpec("q1", "a"), newSpec("q1", "b"), atNow)
	h.Advance(4 * time.Second)

	got, err := h.Store().Stats(t.Context(), "q1")
	if err != nil {
		t.Fatal(err)
	}
	want := []store.TenantStats{
		{Tenant: "a", Ready: 2, Scheduled: 1, Leased: 1, Dead: 1, OldestReadyAge: 7 * time.Second},
		{Tenant: "b", Ready: 2, OldestReadyAge: 4 * time.Second},
	}
	if !slices.Equal(got, want) {
		t.Errorf("Stats(q1) =\n  %+v\nwant\n  %+v\n(tenant c holds only a succeeded job and is not listed; queue q2 is not counted)", got, want)
	}
	if empty, err := h.Store().Stats(t.Context(), "q3"); err != nil || len(empty) != 0 {
		t.Errorf("Stats of an empty queue = %v, %v; want none", empty, err)
	}
}

func activityListsTenantsWithDueWork(t *testing.T, h Harness) {
	s := h.Store()
	if got, err := s.Activity(t.Context(), "q1"); err != nil || len(got.Tenants) != 0 || got.HasNextDue {
		t.Fatalf("Activity of an empty queue = %+v, %v; want nothing", got, err)
	}
	// Tenants a and d have only scheduled work, b and c have due work. The
	// scheduled jobs are enqueued latest first, so that the one due soonest
	// is not the first or the last to be seen.
	late, soon := newSpec("q1", "d"), newSpec("q1", "a")
	late.Delay, soon.Delay = 90*time.Second, 30*time.Second
	enqueue(t, h, newSpec("q1", "c"), late, newSpec("q1", "b"), soon, newSpec("q2", "z"))

	activity := func(when string) store.Activity {
		t.Helper()
		got, err := s.Activity(t.Context(), "q1")
		if err != nil {
			t.Fatalf("Activity %s: %v", when, err)
		}
		return got
	}
	if got := activity("at the start"); !slices.Equal(got.Tenants, []string{"b", "c"}) || !got.HasNextDue || got.NextDueIn != 30*time.Second {
		t.Errorf("Activity = %+v, want tenants [b c] and the next job due in 30s", got)
	}
	h.Advance(10 * time.Second)
	if got := activity("10s later"); got.NextDueIn != 20*time.Second {
		t.Errorf("Activity 10s later = %+v, want the next job due in 20s", got)
	}
	h.Advance(20 * time.Second)
	got := activity("when one scheduled job is due exactly now")
	if !slices.Equal(got.Tenants, []string{"a", "b", "c"}) || !got.HasNextDue || got.NextDueIn != 60*time.Second {
		t.Errorf("Activity when a's job is due exactly now = %+v, want tenants [a b c] and the next job due in 60s", got)
	}
	h.Advance(60 * time.Second)
	if got := activity("when everything is due"); !slices.Equal(got.Tenants, []string{"a", "b", "c", "d"}) || got.HasNextDue {
		t.Errorf("Activity when everything is due = %+v, want tenants [a b c d] and nothing scheduled", got)
	}
}

func activityNextDueCoversTenantsWithDueWork(t *testing.T, h Harness) {
	// Tenant b has due work and a job scheduled in 10s; tenant a has only a
	// job scheduled in 30s. The next job to fall due is b's, so NextDueIn
	// looks at the whole queue, not only at tenants without due work.
	soon, late := newSpec("q1", "b"), newSpec("q1", "a")
	soon.Delay, late.Delay = 10*time.Second, 30*time.Second
	enqueue(t, h, newSpec("q1", "b"), soon, late)

	activity := func(when string) store.Activity {
		t.Helper()
		got, err := h.Store().Activity(t.Context(), "q1")
		if err != nil {
			t.Fatalf("Activity %s: %v", when, err)
		}
		return got
	}
	if got := activity("at the start"); !slices.Equal(got.Tenants, []string{"b"}) || !got.HasNextDue || got.NextDueIn != 10*time.Second {
		t.Errorf("Activity = %+v, want tenants [b] and the next job due in 10s: the scheduled job of a tenant with due work counts", got)
	}
	h.Advance(10 * time.Second)
	if got := activity("when b's scheduled job is due"); !slices.Equal(got.Tenants, []string{"b"}) || !got.HasNextDue || got.NextDueIn != 20*time.Second {
		t.Errorf("Activity 10s later = %+v, want tenants [b] and the next job due in 20s", got)
	}
}

// activityListsOnlyTenantsThatLeaseCanServe pins the agreement that the
// dispatcher's fair sharing relies on. It plans a fetch as one take per tenant
// that Activity lists, and treats a short answer as proof that the short
// tenants ran dry. That chain of fetches ends only if a listed tenant has
// something to give: with no concurrent writer, every tenant that Activity
// lists must get a grant from a per-tenant Lease at the same instant, and a
// tenant that it does not list must get none.
func activityListsOnlyTenantsThatLeaseCanServe(t *testing.T, h Harness) {
	const delay = 20 * time.Second
	scheduled := func(tenant string) job.Spec {
		s := newSpec("q1", tenant)
		s.Delay = delay
		return s
	}
	// a has two due jobs; b has one that is not yet due; c has a due job and
	// one that is not; d has one that is leased below; f has one that is
	// cancelled below; e has a due job in another queue.
	id := enqueue(t, h,
		newSpec("q1", "a"), newSpec("q1", "a"),
		scheduled("b"),
		newSpec("q1", "c"), scheduled("c"),
		newSpec("q1", "d"),
		newSpec("q1", "f"),
		newSpec("q2", "e"),
	)
	if g := leaseFrom(t, h, "q1", "d", 1); len(g) != 1 {
		t.Fatalf("leasing tenant d's job returned %d grants, want 1", len(g))
	}
	if out := cancelOne(t, h, id[6], "f"); out != job.Applied {
		t.Fatalf("Cancel of tenant f's job = %v, want applied", out)
	}

	// One take of one job for every tenant, listed or not.
	var takes []store.TenantTake
	for _, tenant := range []string{"a", "b", "c", "d", "e", "f"} {
		takes = append(takes, store.TenantTake{Tenant: tenant, Limit: 1})
	}
	check := func(when string, want ...string) {
		t.Helper()
		act, err := h.Store().Activity(t.Context(), "q1")
		if err != nil {
			t.Fatalf("Activity %s: %v", when, err)
		}
		var served []string
		for _, g := range leaseTakes(t, h, "q1", takes...) {
			served = append(served, g.Job.Tenant)
		}
		slices.Sort(served)
		if !slices.Equal(act.Tenants, want) {
			t.Errorf("%s: Activity lists %v, want %v", when, act.Tenants, want)
		}
		for _, tenant := range act.Tenants {
			if !slices.Contains(served, tenant) {
				t.Errorf("%s: Activity lists tenant %s, but a per-tenant Lease at the same instant served none of its jobs (it served %v)", when, tenant, served)
			}
		}
		for _, tenant := range served {
			if !slices.Contains(act.Tenants, tenant) {
				t.Errorf("%s: a per-tenant Lease served tenant %s, which Activity does not list (%v)", when, tenant, act.Tenants)
			}
		}
	}
	check("at the start", "a", "c")
	h.Advance(delay - time.Microsecond)
	check("one microsecond before b's job and c's second job are due", "a")
	h.Advance(time.Microsecond)
	check("when b's job and c's second job are due exactly now", "b", "c")
	check("when nothing is left")
}

func maintenanceRejectsInvalidArguments(t *testing.T, h Harness) {
	s := h.Store()
	ctx := t.Context()
	g := leaseOne(t, h, newSpec("q1", "t1"))
	h.Advance(31 * time.Second) // its lease has run out, so Reap has a row to visit
	noBackoff := func(int32) time.Duration { return time.Second }

	_, err := s.Reap(ctx, 0, noBackoff)
	wantInvalid(t, err, "Reap with limit 0")
	_, err = s.Reap(ctx, -1, noBackoff)
	wantInvalid(t, err, "Reap with a negative limit")
	_, err = s.Reap(ctx, 10, nil)
	wantInvalid(t, err, "Reap without a backoff function")
	_, err = s.Reap(ctx, 10, func(int32) time.Duration { return time.Second + time.Nanosecond })
	wantInvalid(t, err, "Reap with a sub-microsecond backoff")
	_, err = s.Reap(ctx, 10, func(int32) time.Duration { return -time.Second })
	wantInvalid(t, err, "Reap with a negative backoff")
	wantState(t, h, g.Job.ID, job.StateLeased, "after the rejected Reaps")

	_, err = s.Prune(ctx, retention(), 0)
	wantInvalid(t, err, "Prune with limit 0")
	_, err = s.Prune(ctx, store.Retention{Done: -time.Second, Dead: time.Hour}, 10)
	wantInvalid(t, err, "Prune with a negative retention")
	_, err = s.Prune(ctx, store.Retention{Done: time.Hour, Dead: time.Hour + time.Nanosecond}, 10)
	wantInvalid(t, err, "Prune with a sub-microsecond retention")

	_, err = s.ListDead(ctx, "q1", 0, 0)
	wantInvalid(t, err, "ListDead with limit 0")
	_, err = s.ListDead(ctx, "q1", 0, -1)
	wantInvalid(t, err, "ListDead with a negative limit")
	for _, name := range badNames() {
		quoted := strconv.Quote(name)
		_, err = s.ListDead(ctx, name, 0, 10)
		wantInvalid(t, err, "ListDead of queue "+quoted)
		_, err = s.Stats(ctx, name)
		wantInvalid(t, err, "Stats of queue "+quoted)
		_, err = s.Activity(ctx, name)
		wantInvalid(t, err, "Activity of queue "+quoted)
		if name == "" {
			continue // an empty tenant means any tenant
		}
		_, err = s.Get(ctx, g.Job.ID, name)
		wantInvalid(t, err, "Get under tenant "+quoted)
		_, err = s.Cancel(ctx, g.Job.ID, name)
		wantInvalid(t, err, "Cancel under tenant "+quoted)
	}
	wantState(t, h, g.Job.ID, job.StateLeased, "after the rejected calls")
}

// rowIDs lists the ids of jobs in order.
func rowIDs(jobs []job.Job) []int64 {
	out := make([]int64, len(jobs))
	for i, j := range jobs {
		out[i] = j.ID
	}
	return out
}
