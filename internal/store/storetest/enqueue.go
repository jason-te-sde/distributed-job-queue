package storetest

import (
	"bytes"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

// RunEnqueue runs the conformance subtests of Enqueue, Get and Snapshot.
func RunEnqueue(t *testing.T, nh NewHarness) {
	run(t, nh, "EnqueueAssignsIncreasingIDs", enqueueAssignsIncreasingIDs)
	run(t, nh, "EnqueueDefaultsMaxAttempts", enqueueDefaultsMaxAttempts)
	run(t, nh, "EnqueueDelaySetsRunAt", enqueueDelaySetsRunAt)
	run(t, nh, "EnqueueRunAtInPastIsClampedToNow", enqueueRunAtInPastIsClampedToNow)
	run(t, nh, "EnqueueRejectsDelayAndRunAtTogether", enqueueRejectsDelayAndRunAtTogether)
	run(t, nh, "EnqueueIdempotencyKeyDeduplicates", enqueueIdempotencyKeyDeduplicates)
	run(t, nh, "EnqueueIdempotencyKeyIsScopedByTenantAndQueue", enqueueIdempotencyKeyIsScopedByTenantAndQueue)
	run(t, nh, "EnqueueBatchDedupWithinBatch", enqueueBatchDedupWithinBatch)
	run(t, nh, "EnqueueDeduplicatedItemConsumesAnID", enqueueDeduplicatedItemConsumesAnID)
	run(t, nh, "EnqueueSizeLimits", enqueueSizeLimits)
	run(t, nh, "EnqueueRejectsBadNames", enqueueRejectsBadNames)
	run(t, nh, "GetReturnsJobAndNotFound", getReturnsJobAndNotFound)
	run(t, nh, "GetTenantScope", getTenantScope)
	run(t, nh, "SnapshotOrderedByID", snapshotOrderedByID)
}

func enqueueAssignsIncreasingIDs(t *testing.T, h Harness) {
	got := enqueue(t, h, newSpec("q1", "t1"), newSpec("q1", "t2"), newSpec("q2", "t1"))
	for i := 1; i < len(got); i++ {
		if got[i] <= got[i-1] {
			t.Fatalf("ids %v are not strictly increasing", got)
		}
	}
	res, err := h.Store().Enqueue(t.Context(), []job.Spec{newSpec("q1", "t1")})
	if err != nil || res[0].ID <= got[len(got)-1] || res[0].Deduplicated {
		t.Errorf("a later call returned %v, %v; want an id above %d, not deduplicated", res, err, got[len(got)-1])
	}
}

func enqueueDefaultsMaxAttempts(t *testing.T, h Harness) {
	id := enqueue(t, h, newSpec("q1", "t1"))[0]
	if got := get(t, h, id).MaxAttempts; got != 25 {
		t.Errorf("MaxAttempts of a spec that left it at 0 = %d, want 25", got)
	}
}

func enqueueDelaySetsRunAt(t *testing.T, h Harness) {
	s := newSpec("q1", "t1")
	s.Delay = 5 * time.Second
	j := get(t, h, enqueue(t, h, s)[0])
	wantTime(t, j.RunAt, h.Now().Add(5*time.Second), "RunAt of a job delayed by 5s")
	wantTime(t, j.CreatedAt, h.Now(), "CreatedAt")
	if j.State != job.StateReady {
		t.Errorf("state = %v, want ready", j.State)
	}
}

func enqueueRunAtInPastIsClampedToNow(t *testing.T, h Harness) {
	h.Advance(time.Hour)
	s := newSpec("q1", "t1")
	s.RunAt = h.Now().Add(-30 * time.Minute)
	j := get(t, h, enqueue(t, h, s)[0])
	wantTime(t, j.RunAt, h.Now(), "RunAt of a job scheduled in the past")
}

func enqueueRejectsDelayAndRunAtTogether(t *testing.T, h Harness) {
	s := newSpec("q1", "t1")
	s.Delay = time.Second
	s.RunAt = h.Now().Add(time.Minute)
	_, err := h.Store().Enqueue(t.Context(), []job.Spec{s})
	wantInvalid(t, err, "a spec with both Delay and RunAt")
	wantRows(t, h, 0, "after the rejected call")
}

func enqueueIdempotencyKeyDeduplicates(t *testing.T, h Harness) {
	s := newSpec("q1", "t1")
	s.IdempotencyKey = "k"
	first, err := h.Store().Enqueue(t.Context(), []job.Spec{s})
	if err != nil || len(first) != 1 || first[0].Deduplicated {
		t.Fatalf("first Enqueue = %v, %v; want one result, not deduplicated", first, err)
	}
	second, err := h.Store().Enqueue(t.Context(), []job.Spec{s})
	if err != nil || len(second) != 1 {
		t.Fatalf("second Enqueue = %v, %v", second, err)
	}
	if second[0].ID != first[0].ID || !second[0].Deduplicated {
		t.Errorf("second Enqueue = %+v, want id %d with Deduplicated", second[0], first[0].ID)
	}
	wantRows(t, h, 1, "after enqueueing the same key twice")
}

func enqueueIdempotencyKeyIsScopedByTenantAndQueue(t *testing.T, h Harness) {
	var specs []job.Spec
	for _, where := range [][2]string{{"q1", "t1"}, {"q1", "t2"}, {"q2", "t1"}} {
		s := newSpec(where[0], where[1])
		s.IdempotencyKey = "k"
		specs = append(specs, s)
	}
	res, err := h.Store().Enqueue(t.Context(), specs)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[int64]bool{}
	for i, r := range res {
		if r.Deduplicated || seen[r.ID] {
			t.Errorf("item %d (%s, %s) = %+v: the key must not collide across tenants or queues", i, specs[i].Queue, specs[i].Tenant, r)
		}
		seen[r.ID] = true
	}
	wantRows(t, h, 3, "after enqueueing one key under three scopes")
}

func enqueueBatchDedupWithinBatch(t *testing.T, h Harness) {
	s := newSpec("q1", "t1")
	s.IdempotencyKey = "k"
	res, err := h.Store().Enqueue(t.Context(), []job.Spec{s, s})
	if err != nil || len(res) != 2 {
		t.Fatalf("Enqueue = %v, %v", res, err)
	}
	want := []store.EnqueueResult{{ID: res[0].ID}, {ID: res[0].ID, Deduplicated: true}}
	if res[0] != want[0] || res[1] != want[1] {
		t.Errorf("results = %v, want %v", res, want)
	}
	wantRows(t, h, 1, "after a batch holding one key twice")
}

func enqueueDeduplicatedItemConsumesAnID(t *testing.T, h Harness) {
	s := newSpec("q1", "t1")
	s.IdempotencyKey = "k"
	first := enqueue(t, h, s)[0]
	enqueue(t, h, s) // deduplicated, and still takes an id
	next := enqueue(t, h, newSpec("q1", "t1"))[0]
	if next != first+2 {
		t.Errorf("the id after a deduplicated item = %d, want %d: every item consumes one id", next, first+2)
	}
}

func enqueueSizeLimits(t *testing.T, h Harness) {
	for _, n := range []int{0, job.MaxPayloadBytes} {
		s := newSpec("q1", "t1")
		s.Payload = make([]byte, n)
		id := enqueue(t, h, s)[0]
		if got := len(get(t, h, id).Payload); got != n {
			t.Errorf("a payload of %d bytes came back with %d", n, got)
		}
	}

	big := newSpec("q1", "t1")
	big.Payload = bytes.Repeat([]byte{'x'}, job.MaxPayloadBytes+1)
	_, err := h.Store().Enqueue(t.Context(), []job.Spec{big})
	wantInvalid(t, err, "a payload one byte over the limit")
	_, err = h.Store().Enqueue(t.Context(), []job.Spec{newSpec("q1", "t1"), big})
	wantInvalid(t, err, "a batch of a valid spec and an oversized one")
	wantRows(t, h, 2, "after the oversized payloads were rejected")

	_, err = h.Store().Enqueue(t.Context(), validSpecs(job.MaxBatch+1))
	wantInvalid(t, err, "a batch of 65 items")
	_, err = h.Store().Enqueue(t.Context(), nil)
	wantInvalid(t, err, "an empty batch")
	wantRows(t, h, 2, "after the oversized batches were rejected")

	if _, err := h.Store().Enqueue(t.Context(), validSpecs(job.MaxBatch)); err != nil {
		t.Errorf("a batch of exactly %d items: %v", job.MaxBatch, err)
	}
	wantRows(t, h, 2+job.MaxBatch, "after a batch at the limit")
}

func enqueueRejectsBadNames(t *testing.T, h Harness) {
	for _, name := range badNames() {
		_, err := h.Store().Enqueue(t.Context(), []job.Spec{newSpec(name, "t1")})
		wantInvalid(t, err, "queue "+strconv.Quote(name))
		_, err = h.Store().Enqueue(t.Context(), []job.Spec{newSpec("q1", name)})
		wantInvalid(t, err, "tenant "+strconv.Quote(name))
	}
	wantRows(t, h, 0, "after every bad name was rejected")
}

func getReturnsJobAndNotFound(t *testing.T, h Harness) {
	s := newSpec("q1", "t1")
	s.Payload = []byte("hello")
	s.IdempotencyKey = "key"
	s.MaxAttempts = 3
	id := enqueue(t, h, s)[0]
	j := get(t, h, id)
	if j.ID != id || j.Queue != "q1" || j.Tenant != "t1" || string(j.Payload) != "hello" ||
		j.IdempotencyKey != "key" || j.MaxAttempts != 3 || j.State != job.StateReady || j.Attempts != 0 || j.Epoch != 0 {
		t.Errorf("Get(%d) = %+v, want the job as enqueued", id, j)
	}
	_, err := h.Store().Get(t.Context(), id+100, "")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Get of an unknown id: error = %v, want store.ErrNotFound", err)
	}
}

func getTenantScope(t *testing.T, h Harness) {
	id := enqueue(t, h, newSpec("q1", "t1"))[0]
	if _, err := h.Store().Get(t.Context(), id, "other"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Get under another tenant: error = %v, want store.ErrNotFound", err)
	}
	if j, err := h.Store().Get(t.Context(), id, "t1"); err != nil || j.ID != id {
		t.Errorf("Get under the job's tenant = %v, %v", j.ID, err)
	}
	if j, err := h.Store().Get(t.Context(), id, ""); err != nil || j.ID != id {
		t.Errorf(`Get under tenant "" = %v, %v`, j.ID, err)
	}
}

func snapshotOrderedByID(t *testing.T, h Harness) {
	enqueue(t, h, newSpec("q2", "t2"), newSpec("q1", "t1"))
	enqueue(t, h, newSpec("q1", "t3"))
	rows := snapshot(t, h)
	if len(rows) != 3 {
		t.Fatalf("Snapshot returned %d rows, want 3", len(rows))
	}
	for i := 1; i < len(rows); i++ {
		if rows[i].ID <= rows[i-1].ID {
			t.Errorf("Snapshot ids %d then %d are not ascending", rows[i-1].ID, rows[i].ID)
		}
	}
}
