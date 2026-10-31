package sim

import (
	"testing"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/memstore"
)

// TestBrokerReleasesJobsFetchedForAClientThatLeft cancels a long-poll request
// while the broker's fetch for it is in flight, after the store has leased
// the job and before the broker has the answer. The job must go back to the
// store with its attempt refunded, and the client must hear nothing.
func TestBrokerReleasesJobsFetchedForAClientThatLeft(t *testing.T) {
	w := NewWorld(7)
	net := NewNetwork(w, NetConfig{Latency: [2]time.Duration{time.Millisecond, time.Millisecond}})
	var b *BrokerNode
	var changes []memstore.Change
	cancelled := false
	st := NewStoreNode(w, func(ch memstore.Change) {
		changes = append(changes, ch)
		if ch.Op == memstore.OpLease && !cancelled {
			cancelled = true
			b.handleCancel(cancelReq{from: "w1", seq: 1})
		}
	})
	b = NewBrokerNode(w, net, st, BrokerConfig{ID: "b1", Clock: Clock{Rate: 1}})
	replies := 0
	b.handleEnqueue(enqueueReq{from: "p1", spec: job.Spec{Queue: "q", Tenant: "t"}, reply: func(enqueueResp) {}})
	// Let the job be committed before the worker asks.
	for w.Now < time.Second {
		w.Step()
	}
	b.handleLeaseReq(leaseReq{from: "w1", seq: 1, queue: "q", max: 4, wait: 5 * time.Second, ttl: 30 * time.Second,
		reply: func(leaseResp) { replies++ }})
	for w.Now < 20*time.Second {
		w.Step()
	}

	if !cancelled {
		t.Fatal("the store never leased the job, so the cancel did not race the fetch")
	}
	var ops []memstore.Op
	for _, ch := range changes {
		ops = append(ops, ch.Op)
	}
	jobs, err := st.Mem().Snapshot(t.Context())
	if err != nil || len(jobs) != 1 {
		t.Fatalf("snapshot = %v, %v", jobs, err)
	}
	j := jobs[0]
	if j.State != job.StateReady || j.Attempts != 0 {
		t.Errorf("job is %v with %d attempts after changes %v, want ready with the attempt refunded", j.State, j.Attempts, ops)
	}
	if replies != 0 {
		t.Errorf("the client that left got %d replies, want none", replies)
	}
	if got := b.Stats().FencedAcks; got != 0 {
		t.Errorf("FencedAcks = %d, want the release applied", got)
	}
	if _, err := st.Mem().Get(t.Context(), j.ID, ""); err != nil {
		t.Errorf("get: %v (store.ErrNotFound is %v)", err, store.ErrNotFound)
	}
}
