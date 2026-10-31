//go:build pg

package exp

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	jobqueue "github.com/jason-te-sde/distributed-job-queue"
	jobqueuev1 "github.com/jason-te-sde/distributed-job-queue/internal/gen/jobqueue/v1"
)

func TestPGClusterServesProducersAndWorkersAcrossBrokers(t *testing.T) {
	dsn, pool := testDatabase(t)
	c, err := StartCluster(t.Context(), ClusterOptions{DatabaseURL: dsn, Brokers: 3})
	if err != nil {
		t.Fatalf("StartCluster: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	if got := len(c.Addrs()); got != 3 {
		t.Fatalf("Addrs() has %d addresses, want 3", got)
	}

	conn, err := c.Dial()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	producer := jobqueue.NewProducer(conn, jobqueue.ProducerOptions{})
	res, err := producer.Enqueue(t.Context(), jobqueue.EnqueueParams{Queue: "q", Payload: []byte("x")})
	if err != nil {
		t.Fatalf("Enqueue through the cluster: %v", err)
	}
	if got := countRows(t, pool); got != 1 {
		t.Errorf("the jobs table has %d rows after one enqueue, want 1 (job %d): the cluster must have migrated and used the database", got, res.ID)
	}

	if err := c.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Errorf("a second Close returned %v, want nil", err)
	}
}

func TestPGClusterKillBrokerResetsItsConnectionsOnly(t *testing.T) {
	dsn, _ := testDatabase(t)
	c, err := StartCluster(t.Context(), ClusterOptions{DatabaseURL: dsn, Brokers: 3, Killable: true})
	if err != nil {
		t.Fatalf("StartCluster: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	conn, err := c.Dial()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	producer := jobqueue.NewProducer(conn, jobqueue.ProducerOptions{Retry: jobqueue.RetryPolicy{Attempts: 5, Base: time.Millisecond, Cap: 5 * time.Millisecond}})
	for range 9 { // enough calls that round robin has used every broker and connected to each
		if _, err := producer.Enqueue(t.Context(), jobqueue.EnqueueParams{Queue: "q"}); err != nil {
			t.Fatalf("Enqueue before the kill: %v", err)
		}
	}
	reset, err := c.KillBroker(0)
	if err != nil {
		t.Fatalf("KillBroker: %v", err)
	}
	if reset < 1 {
		t.Errorf("KillBroker reset %d connections, want at least the producer's", reset)
	}
	// The producer retries on a connection to a surviving broker.
	for range 6 {
		if _, err := producer.Enqueue(t.Context(), jobqueue.EnqueueParams{Queue: "q"}); err != nil {
			t.Fatalf("Enqueue after the kill of one broker of three: %v", err)
		}
	}
	if _, err := (&Cluster{}).KillBroker(0); err == nil {
		t.Error("KillBroker on a cluster that is not killable returned no error")
	}
}

// callRecorder is a client interceptor that notes the size of what each Lease
// and Ack call asks for.
type callRecorder struct {
	mu       sync.Mutex
	maxJobs  []int32
	ackItems []int
}

func (c *callRecorder) intercept(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	c.mu.Lock()
	switch r := req.(type) {
	case *jobqueuev1.LeaseRequest:
		c.maxJobs = append(c.maxJobs, r.GetMaxJobs())
	case *jobqueuev1.AckRequest:
		c.ackItems = append(c.ackItems, len(r.GetItems()))
	}
	c.mu.Unlock()
	return invoker(ctx, method, req, reply, cc, opts...)
}

func TestPGStartWorkersAppliesTheLeaseCapAndTheAckBatch(t *testing.T) {
	run := func(t *testing.T, spec workerSpec) *callRecorder {
		t.Helper()
		dsn, pool := testDatabase(t)
		c, err := StartCluster(t.Context(), ClusterOptions{DatabaseURL: dsn, Brokers: 1})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		conn, err := c.Dial()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		const n = 40
		if err := enqueueN(t.Context(), jobqueue.NewProducer(conn, jobqueue.ProducerOptions{}), "q", n); err != nil {
			t.Fatal(err)
		}
		rec := &callRecorder{}
		spec.Count, spec.Concurrency, spec.Observe = 1, 16, rec.intercept
		stop, err := startWorkers(t.Context(), c, "q", spec, func(context.Context, *jobqueue.Job) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(30 * time.Second)
		for {
			var done int
			if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM jobqueue.jobs WHERE state = 'succeeded'").Scan(&done); err != nil {
				t.Fatal(err)
			}
			if done == n {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%d of %d jobs succeeded within 30s", done, n)
			}
			time.Sleep(10 * time.Millisecond)
		}
		if err := stop(); err != nil {
			t.Fatalf("stop: %v", err)
		}
		return rec
	}

	capped := run(t, workerSpec{LeaseMaxJobs: 1, AckBatch: 1})
	if len(capped.maxJobs) == 0 || len(capped.ackItems) == 0 {
		t.Fatalf("the worker made %d Lease calls and %d Ack calls", len(capped.maxJobs), len(capped.ackItems))
	}
	for _, m := range capped.maxJobs {
		if m != 1 {
			t.Fatalf("a Lease call asked for %d jobs under a cap of 1 (all: %v)", m, capped.maxJobs)
		}
	}
	for _, n := range capped.ackItems {
		if n != 1 {
			t.Fatalf("an Ack call carried %d items under AckBatch 1 (all: %v)", n, capped.ackItems)
		}
	}

	// The control: without the cap the worker asks for as many jobs as it has
	// free slots, so a cap of 1 above is not just how a small test behaves.
	free := run(t, workerSpec{})
	most := int32(0)
	for _, m := range free.maxJobs {
		most = max(most, m)
	}
	if most < 2 {
		t.Errorf("without a cap no Lease call asked for more than one job (asked: %v)", free.maxJobs)
	}
}
