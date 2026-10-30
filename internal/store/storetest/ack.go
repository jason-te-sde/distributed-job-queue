package storetest

import (
	"bytes"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

// RunAck runs the conformance subtests of Heartbeat, Complete, Fail and
// Release, and of the rules that hold for every call that returns a job.
func RunAck(t *testing.T, nh NewHarness) {
	run(t, nh, "HeartbeatExtendsAndReportsRemaining", heartbeatExtendsAndReportsRemaining)
	run(t, nh, "HeartbeatAtExpiryIsExpired", heartbeatAtExpiryIsExpired)
	run(t, nh, "HeartbeatStaleTokenIsFenced", heartbeatStaleTokenIsFenced)
	run(t, nh, "HeartbeatWrongQueueIsNotFound", heartbeatWrongQueueIsNotFound)
	run(t, nh, "CompleteThenDuplicate", completeThenDuplicate)
	run(t, nh, "CompleteAfterExpiryBeforeReapSucceeds", completeAfterExpiryBeforeReapSucceeds)
	run(t, nh, "CompleteStaleTokenFenced", completeStaleTokenFenced)
	run(t, nh, "CompleteResultLimit", completeResultLimit)
	run(t, nh, "FailRetryableSchedulesRetry", failRetryableSchedulesRetry)
	run(t, nh, "FailExhaustedGoesDead", failExhaustedGoesDead)
	run(t, nh, "FailPermanentGoesDead", failPermanentGoesDead)
	run(t, nh, "FailTruncatesError", failTruncatesError)
	run(t, nh, "ReleaseRefundsAttempt", releaseRefundsAttempt)
	run(t, nh, "BatchResultsAlignWithRequests", batchResultsAlignWithRequests)
	run(t, nh, "HeartbeatResultsAlignWithRequests", heartbeatResultsAlignWithRequests)
	run(t, nh, "FailResultsAlignWithRequests", failResultsAlignWithRequests)
	run(t, nh, "ReleaseResultsAlignWithRequests", releaseResultsAlignWithRequests)
	run(t, nh, "AckBatchLimit", ackBatchLimit)
	run(t, nh, "AckRejectsInvalidArguments", ackRejectsInvalidArguments)
	run(t, nh, "ReturnedBytesAreCopies", returnedBytesAreCopies)
}

func heartbeatExtendsAndReportsRemaining(t *testing.T, h Harness) {
	g := leaseOne(t, h, newSpec("q1", "t1"))
	h.Advance(10 * time.Second)
	res := heartbeatOne(t, h, refOf(g), 30*time.Second)
	wantOutcome(t, res.Outcome, job.Applied, "Heartbeat")
	if res.Remaining != 30*time.Second {
		t.Errorf("Remaining = %v, want 30s", res.Remaining)
	}
	wantTime(t, get(t, h, g.Job.ID).LeaseExpiresAt, h.Now().Add(30*time.Second), "LeaseExpiresAt after the heartbeat")
}

func heartbeatAtExpiryIsExpired(t *testing.T, h Harness) {
	// Both jobs are leased for 30s at the same instant. One microsecond
	// before that lease ends, the first is extended; at the instant it ends,
	// the second can no longer be.
	enqueue(t, h, newSpec("q1", "t1"), newSpec("q1", "t1"))
	grants := leaseFrom(t, h, "q1", "t1", 2)
	if len(grants) != 2 {
		t.Fatalf("leased %d jobs, want 2", len(grants))
	}
	leased := h.Now()
	h.Advance(30*time.Second - time.Microsecond)
	live := heartbeatOne(t, h, refOf(grants[0]), 5*time.Second)
	wantOutcome(t, live.Outcome, job.Applied, "Heartbeat one microsecond before expiry")
	h.Advance(time.Microsecond)
	dead := heartbeatOne(t, h, refOf(grants[1]), 5*time.Second)
	wantOutcome(t, dead.Outcome, job.Expired, "Heartbeat exactly at expiry")
	if dead.Remaining != 0 {
		t.Errorf("Remaining of an expired heartbeat = %v, want 0", dead.Remaining)
	}
	j := wantState(t, h, grants[1].Job.ID, job.StateLeased, "after the expired heartbeat")
	wantTime(t, j.LeaseExpiresAt, leased.Add(30*time.Second), "LeaseExpiresAt after the expired heartbeat")
}

func heartbeatStaleTokenIsFenced(t *testing.T, h Harness) {
	old := leaseOne(t, h, newSpec("q1", "t1"))
	current := relet(t, h, old)
	res := heartbeatOne(t, h, refOf(old), 30*time.Second)
	wantOutcome(t, res.Outcome, job.Fenced, "Heartbeat with the token of the previous lease")
	if res.Remaining != 0 {
		t.Errorf("Remaining of a fenced heartbeat = %v, want 0", res.Remaining)
	}
	wantTime(t, get(t, h, old.Job.ID).LeaseExpiresAt, current.Job.LeaseExpiresAt, "LeaseExpiresAt after the fenced heartbeat")
	wantOutcome(t, heartbeatOne(t, h, refOf(current), 30*time.Second).Outcome, job.Applied, "Heartbeat with the current token")
}

func heartbeatWrongQueueIsNotFound(t *testing.T, h Harness) {
	g := leaseOne(t, h, newSpec("q1", "t1"))
	wrong := refOf(g)
	wrong.Queue = "q2"
	wantOutcome(t, heartbeatOne(t, h, wrong, 30*time.Second).Outcome, job.NotFound, "Heartbeat naming another queue")
	unknown := refOf(g)
	unknown.ID += 100
	wantOutcome(t, heartbeatOne(t, h, unknown, 30*time.Second).Outcome, job.NotFound, "Heartbeat of an unknown id")
	wantOutcome(t, heartbeatOne(t, h, refOf(g), 30*time.Second).Outcome, job.Applied, "Heartbeat with the right queue")
}

func completeThenDuplicate(t *testing.T, h Harness) {
	g := leaseOne(t, h, newSpec("q1", "t1"))
	wantOutcome(t, completeOne(t, h, refOf(g), []byte("first")), job.Applied, "Complete")
	j := wantState(t, h, g.Job.ID, job.StateSucceeded, "after Complete")
	if string(j.Result) != "first" || j.LeasedBy != "" || !j.LeaseExpiresAt.IsZero() {
		t.Errorf("completed job has Result %q, LeasedBy %q, LeaseExpiresAt %v; want the result and no lease", j.Result, j.LeasedBy, j.LeaseExpiresAt)
	}
	wantTime(t, j.FinishedAt, h.Now(), "FinishedAt")

	h.Advance(time.Second)
	wantOutcome(t, completeOne(t, h, refOf(g), []byte("second")), job.Duplicate, "Complete again")
	j = get(t, h, g.Job.ID)
	if string(j.Result) != "first" {
		t.Errorf("Result after the duplicate Complete = %q, want the first one", j.Result)
	}
	wantTime(t, j.FinishedAt, h.Now().Add(-time.Second), "FinishedAt after the duplicate Complete")
}

func completeAfterExpiryBeforeReapSucceeds(t *testing.T, h Harness) {
	g := leaseOne(t, h, newSpec("q1", "t1"))
	h.Advance(31 * time.Second)
	wantOutcome(t, completeOne(t, h, refOf(g), nil), job.Applied, "Complete after the lease ran out")
	wantState(t, h, g.Job.ID, job.StateSucceeded, "after Complete")
}

func completeStaleTokenFenced(t *testing.T, h Harness) {
	old := leaseOne(t, h, newSpec("q1", "t1"))
	current := relet(t, h, old)
	wantOutcome(t, completeOne(t, h, refOf(old), []byte("late")), job.Fenced, "Complete with the token of the previous lease")
	j := wantState(t, h, old.Job.ID, job.StateLeased, "after the fenced Complete")
	if j.Epoch != current.Job.Epoch || len(j.Result) != 0 {
		t.Errorf("job has Epoch %d and Result %q after the fenced Complete, want %d and none", j.Epoch, j.Result, current.Job.Epoch)
	}
}

func completeResultLimit(t *testing.T, h Harness) {
	g := leaseOne(t, h, newSpec("q1", "t1"))
	_, err := h.Store().Complete(t.Context(), []store.CompleteRequest{{Ref: refOf(g), Result: make([]byte, job.MaxResultBytes+1)}})
	wantInvalid(t, err, "a result one byte over the limit")
	wantState(t, h, g.Job.ID, job.StateLeased, "after the rejected Complete")
	wantOutcome(t, completeOne(t, h, refOf(g), make([]byte, job.MaxResultBytes)), job.Applied, "Complete with a result at the limit")
	if got := len(get(t, h, g.Job.ID).Result); got != job.MaxResultBytes {
		t.Errorf("stored result is %d bytes, want %d", got, job.MaxResultBytes)
	}
}

func failRetryableSchedulesRetry(t *testing.T, h Harness) {
	g := leaseOne(t, h, newSpec("q1", "t1"))
	out := failOne(t, h, store.FailRequest{Ref: refOf(g), Delay: 7 * time.Second, Error: "boom"})
	wantOutcome(t, out, job.Applied, "Fail")
	j := wantState(t, h, g.Job.ID, job.StateReady, "after a retryable Fail")
	wantTime(t, j.RunAt, h.Now().Add(7*time.Second), "RunAt")
	if j.LastError != "boom" || j.LeasedBy != "" || !j.LeaseExpiresAt.IsZero() || !j.FinishedAt.IsZero() {
		t.Errorf("job has LastError %q, LeasedBy %q, LeaseExpiresAt %v, FinishedAt %v; want the error and no lease or finish",
			j.LastError, j.LeasedBy, j.LeaseExpiresAt, j.FinishedAt)
	}
	if j.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1: a failure does not refund the attempt", j.Attempts)
	}
}

func failExhaustedGoesDead(t *testing.T, h Harness) {
	s := newSpec("q1", "t1")
	s.MaxAttempts = 1
	g := leaseOne(t, h, s)
	wantOutcome(t, failOne(t, h, store.FailRequest{Ref: refOf(g), Error: "boom"}), job.Applied, "Fail")
	j := wantState(t, h, g.Job.ID, job.StateDead, "after failing the last attempt")
	wantTime(t, j.FinishedAt, h.Now(), "FinishedAt")
}

func failPermanentGoesDead(t *testing.T, h Harness) {
	g := leaseOne(t, h, newSpec("q1", "t1"))
	wantOutcome(t, failOne(t, h, store.FailRequest{Ref: refOf(g), Permanent: true, Error: "no"}), job.Applied, "Fail")
	j := wantState(t, h, g.Job.ID, job.StateDead, "after a permanent Fail")
	if j.LastError != "no" {
		t.Errorf("LastError = %q, want %q", j.LastError, "no")
	}
}

func failTruncatesError(t *testing.T, h Harness) {
	g := leaseOne(t, h, newSpec("q1", "t1"))
	// Three-byte characters, so that byte 4096 falls inside one.
	msg := strings.Repeat("€", 1666) + "ab"
	if len(msg) != 5000 {
		t.Fatalf("test setup: message is %d bytes, want 5000", len(msg))
	}
	wantOutcome(t, failOne(t, h, store.FailRequest{Ref: refOf(g), Error: msg}), job.Applied, "Fail")
	got := get(t, h, g.Job.ID).LastError
	if len(got) > job.MaxErrorBytes || !utf8.ValidString(got) || !strings.HasPrefix(msg, got) || len(got) < job.MaxErrorBytes-3 {
		t.Errorf("stored error is %d bytes (valid UTF-8: %t), want a prefix of the message of at most %d bytes cut on a character boundary",
			len(got), utf8.ValidString(got), job.MaxErrorBytes)
	}
}

func releaseRefundsAttempt(t *testing.T, h Harness) {
	g := leaseOne(t, h, newSpec("q1", "t1"))
	h.Advance(5 * time.Second)
	wantOutcome(t, releaseOne(t, h, refOf(g)), job.Applied, "Release")
	j := wantState(t, h, g.Job.ID, job.StateReady, "after Release")
	if j.Attempts != 0 || j.LeasedBy != "" || !j.LeaseExpiresAt.IsZero() {
		t.Errorf("released job has Attempts %d, LeasedBy %q, LeaseExpiresAt %v; want the attempt refunded and no lease", j.Attempts, j.LeasedBy, j.LeaseExpiresAt)
	}
	wantTime(t, j.RunAt, h.Now(), "RunAt")
	again := leaseFrom(t, h, "q1", "t1", 1)
	if len(again) != 1 || again[0].Job.Attempts != 1 || again[0].Job.Epoch != 2 {
		t.Errorf("leasing the released job again returned %+v, want Attempts 1 and Epoch 2", again)
	}
	wantOutcome(t, releaseOne(t, h, refOf(g)), job.Fenced, "Release with the token of the previous lease")
}

func batchResultsAlignWithRequests(t *testing.T, h Harness) {
	valid := leaseOne(t, h, newSpec("q1", "t1"))
	stale := leaseOne(t, h, newSpec("q1", "t1"))
	staleRef := refOf(stale)
	relet(t, h, stale)
	unknown := store.Ref{ID: 9999, Queue: "q1", Token: 1}
	wrongQueue := refOf(valid)
	wrongQueue.Queue = "q2"

	out, err := h.Store().Complete(t.Context(), []store.CompleteRequest{
		{Ref: refOf(valid)}, {Ref: staleRef}, {Ref: unknown}, {Ref: wrongQueue},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []job.Outcome{job.Applied, job.Fenced, job.NotFound, job.NotFound}
	if len(out) != len(want) {
		t.Fatalf("Complete returned %d outcomes for 4 requests: %v", len(out), out)
	}
	for i := range want {
		wantOutcome(t, out[i], want[i], "item "+strconv.Itoa(i))
	}
}

// mixedRefs are four references with four different fates.
type mixedRefs struct {
	// valid names a live lease.
	valid store.Ref
	// stale carries the token of a lease that was taken away.
	stale store.Ref
	// unknown names a job that does not exist.
	unknown store.Ref
	// wrongQueue names a live lease, but in a queue the job is not in.
	wrongQueue store.Ref
}

// newMixedRefs leases the jobs behind a mixedRefs.
func newMixedRefs(t *testing.T, h Harness) mixedRefs {
	t.Helper()
	valid := leaseOne(t, h, newSpec("q1", "t1"))
	stale := leaseOne(t, h, newSpec("q1", "t1"))
	staleRef := refOf(stale)
	relet(t, h, stale)
	other := leaseOne(t, h, newSpec("q1", "t1"))
	wrong := refOf(other)
	wrong.Queue = "q2"
	return mixedRefs{
		valid:      refOf(valid),
		stale:      staleRef,
		unknown:    store.Ref{ID: other.Job.ID + 1000, Queue: "q1", Token: 1},
		wrongQueue: wrong,
	}
}

// wantOutcomes fails unless got is want, item by item.
func wantOutcomes(t *testing.T, got, want []job.Outcome, call string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Errorf("%s returned %v, want %v: one outcome per item, in request order", call, got, want)
	}
}

func heartbeatResultsAlignWithRequests(t *testing.T, h Harness) {
	m := newMixedRefs(t, h)
	res, err := h.Store().Heartbeat(t.Context(), []store.Ref{m.unknown, m.valid, m.wrongQueue, m.stale}, 40*time.Second)
	if err != nil || len(res) != 4 {
		t.Fatalf("Heartbeat = %v, %v; want 4 results", res, err)
	}
	got := []job.Outcome{res[0].Outcome, res[1].Outcome, res[2].Outcome, res[3].Outcome}
	wantOutcomes(t, got, []job.Outcome{job.NotFound, job.Applied, job.NotFound, job.Fenced}, "Heartbeat")
	if want := []time.Duration{0, 40 * time.Second, 0, 0}; res[0].Remaining != want[0] || res[1].Remaining != want[1] || res[2].Remaining != want[2] || res[3].Remaining != want[3] {
		t.Errorf("Remaining = [%v %v %v %v], want %v: positive only for the applied item", res[0].Remaining, res[1].Remaining, res[2].Remaining, res[3].Remaining, want)
	}
}

func failResultsAlignWithRequests(t *testing.T, h Harness) {
	m := newMixedRefs(t, h)
	var reqs []store.FailRequest
	for _, r := range []store.Ref{m.stale, m.wrongQueue, m.valid, m.unknown} {
		reqs = append(reqs, store.FailRequest{Ref: r, Error: "boom"})
	}
	got, err := h.Store().Fail(t.Context(), reqs)
	if err != nil {
		t.Fatal(err)
	}
	wantOutcomes(t, got, []job.Outcome{job.Fenced, job.NotFound, job.Applied, job.NotFound}, "Fail")
	wantState(t, h, m.valid.ID, job.StateReady, "after Fail")
	wantState(t, h, m.stale.ID, job.StateLeased, "after the fenced Fail")
}

func releaseResultsAlignWithRequests(t *testing.T, h Harness) {
	m := newMixedRefs(t, h)
	got, err := h.Store().Release(t.Context(), []store.Ref{m.wrongQueue, m.stale, m.unknown, m.valid})
	if err != nil {
		t.Fatal(err)
	}
	wantOutcomes(t, got, []job.Outcome{job.NotFound, job.Fenced, job.NotFound, job.Applied}, "Release")
	wantState(t, h, m.valid.ID, job.StateReady, "after Release")
	wantState(t, h, m.stale.ID, job.StateLeased, "after the fenced Release")
}

func ackBatchLimit(t *testing.T, h Harness) {
	refs := func(n int) []store.Ref {
		out := make([]store.Ref, n)
		for i := range out {
			out[i] = store.Ref{ID: int64(1000 + i), Queue: "q1", Token: 1}
		}
		return out
	}
	completes := func(n int) []store.CompleteRequest {
		out := make([]store.CompleteRequest, n)
		for i, r := range refs(n) {
			out[i] = store.CompleteRequest{Ref: r}
		}
		return out
	}
	fails := func(n int) []store.FailRequest {
		out := make([]store.FailRequest, n)
		for i, r := range refs(n) {
			out[i] = store.FailRequest{Ref: r}
		}
		return out
	}
	s := h.Store()
	ctx := t.Context()

	if res, err := s.Heartbeat(ctx, refs(job.MaxAckBatch), time.Second); err != nil || len(res) != job.MaxAckBatch {
		t.Errorf("Heartbeat of %d refs = %d results, %v; want them all answered", job.MaxAckBatch, len(res), err)
	}
	_, err := s.Heartbeat(ctx, refs(job.MaxAckBatch+1), time.Second)
	wantInvalid(t, err, "Heartbeat of 129 refs")
	_, err = s.Complete(ctx, completes(job.MaxAckBatch+1))
	wantInvalid(t, err, "Complete of 129 requests")
	_, err = s.Fail(ctx, fails(job.MaxAckBatch+1))
	wantInvalid(t, err, "Fail of 129 requests")
	_, err = s.Release(ctx, refs(job.MaxAckBatch+1))
	wantInvalid(t, err, "Release of 129 refs")

	_, err = s.Heartbeat(ctx, nil, time.Second)
	wantInvalid(t, err, "Heartbeat of no refs")
	_, err = s.Complete(ctx, nil)
	wantInvalid(t, err, "Complete of no requests")
	_, err = s.Fail(ctx, nil)
	wantInvalid(t, err, "Fail of no requests")
	_, err = s.Release(ctx, nil)
	wantInvalid(t, err, "Release of no refs")
}

func ackRejectsInvalidArguments(t *testing.T, h Harness) {
	good := leaseOne(t, h, newSpec("q1", "t1"))
	ref := refOf(good)
	badRef := ref
	badRef.Queue = "a b"
	s := h.Store()
	ctx := t.Context()

	_, err := s.Heartbeat(ctx, []store.Ref{ref}, 0)
	wantInvalid(t, err, "Heartbeat with TTL 0")
	_, err = s.Heartbeat(ctx, []store.Ref{ref}, job.MaxTTL+time.Microsecond)
	wantInvalid(t, err, "Heartbeat with a TTL over the maximum")
	_, err = s.Heartbeat(ctx, []store.Ref{ref}, 5*time.Second+time.Nanosecond)
	wantInvalid(t, err, "Heartbeat with a sub-microsecond TTL")
	_, err = s.Heartbeat(ctx, []store.Ref{ref, badRef}, 5*time.Second)
	wantInvalid(t, err, "Heartbeat with one bad queue name among the refs")

	_, err = s.Complete(ctx, []store.CompleteRequest{{Ref: ref}, {Ref: badRef}})
	wantInvalid(t, err, "Complete with one bad queue name among the requests")
	_, err = s.Release(ctx, []store.Ref{ref, badRef})
	wantInvalid(t, err, "Release with one bad queue name among the refs")
	for name, delay := range map[string]time.Duration{
		"a negative delay":         -time.Second,
		"a delay over the maximum": job.MaxDelay + time.Microsecond,
		"a sub-microsecond delay":  time.Second + time.Nanosecond,
	} {
		_, err = s.Fail(ctx, []store.FailRequest{{Ref: ref}, {Ref: ref, Delay: delay}})
		wantInvalid(t, err, "Fail with "+name+" in its second item")
	}
	_, err = s.Fail(ctx, []store.FailRequest{{Ref: ref}, {Ref: badRef}})
	wantInvalid(t, err, "Fail with one bad queue name among the requests")

	// Rejected calls change nothing: the first item of each was valid.
	j := wantState(t, h, good.Job.ID, job.StateLeased, "after the rejected calls")
	if j.Epoch != good.Job.Epoch || !j.LeaseExpiresAt.Equal(good.Job.LeaseExpiresAt) {
		t.Errorf("job changed to %+v after only rejected calls", j)
	}
}

func returnedBytesAreCopies(t *testing.T, h Harness) {
	const payload, result = "original-payload", "original-result"
	mutate := func(b []byte) {
		for i := range b {
			b[i] = '!'
		}
	}
	var id int64
	wantStored := func(when string) {
		t.Helper()
		j := get(t, h, id)
		if string(j.Payload) != payload || string(j.Result) != wantResult(j.State, result) {
			t.Errorf("%s: stored Payload %q, Result %q; want %q and %q", when, j.Payload, j.Result, payload, wantResult(j.State, result))
		}
	}

	buf := []byte(payload)
	s := newSpec("q1", "t1")
	s.Payload = buf
	id = enqueue(t, h, s)[0]
	mutate(buf)
	wantStored("after the caller reused its Spec.Payload buffer")

	j := get(t, h, id)
	mutate(j.Payload)
	wantStored("after the caller changed the bytes Get returned")

	for _, row := range snapshot(t, h) {
		mutate(row.Payload)
	}
	wantStored("after the caller changed the bytes Snapshot returned")

	grants := leaseFrom(t, h, "q1", "t1", 1)
	if len(grants) != 1 {
		t.Fatalf("leased %d jobs, want 1", len(grants))
	}
	mutate(grants[0].Job.Payload)
	wantStored("after the caller changed the bytes a Grant carried")

	resBuf := []byte(result)
	wantOutcome(t, completeOne(t, h, refOf(grants[0]), resBuf), job.Applied, "Complete")
	mutate(resBuf)
	wantStored("after the caller reused its CompleteRequest.Result buffer")

	j = get(t, h, id)
	mutate(j.Result)
	mutate(j.Payload)
	wantStored("after the caller changed the Result bytes Get returned")

	for _, row := range snapshot(t, h) {
		mutate(row.Result)
	}
	wantStored("after the caller changed the Result bytes Snapshot returned")
	if !bytes.Equal(get(t, h, id).Result, []byte(result)) {
		t.Error("the stored result differs from the one completed")
	}
}

// wantResult is the result a job in state should hold: the completed one
// once it has succeeded, none before.
func wantResult(state job.State, result string) string {
	if state == job.StateSucceeded {
		return result
	}
	return ""
}
