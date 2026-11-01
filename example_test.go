package jobqueue_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	jobqueue "github.com/jason-te-sde/distributed-job-queue"
)

// The examples have no Output comment: they need a broker, so go test
// compiles them and does not run them.

// A producer enqueues a job with an idempotency key, which makes the call safe
// to repeat: enqueuing the same key again returns the first job.
func ExampleProducer_Enqueue() {
	ctx := context.Background()

	conn, err := jobqueue.Dial("127.0.0.1:7070", jobqueue.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	producer := jobqueue.NewProducer(conn, jobqueue.ProducerOptions{Tenant: "acme"})
	res, err := producer.Enqueue(ctx, jobqueue.EnqueueParams{
		Queue:          "thumbnails",
		Payload:        []byte(`{"seed": 7, "size": 1024}`),
		IdempotencyKey: "thumbnail-7",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("job %d, deduplicated: %t\n", res.ID, res.Deduplicated)

	info, err := producer.Get(ctx, res.ID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("job %d is %s after %d of %d attempts\n", info.ID, info.State, info.Attempts, info.MaxAttempts)
}

// A worker runs a handler on each job it leases. The handler returns nil to
// complete the job, an error to retry it after a backoff, RetryAfter to choose
// the delay, or Permanent to send the job to the dead state.
func ExampleNewWorker() {
	conn, err := jobqueue.Dial("127.0.0.1:7070", jobqueue.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	worker := jobqueue.NewWorker(conn, jobqueue.WorkerOptions{Queue: "thumbnails", Concurrency: 16})
	err = worker.Handle(func(ctx context.Context, j *jobqueue.Job) error {
		// j.Token() is the fencing token of this lease. Send it with every
		// write to a downstream system, and have that system refuse a write
		// whose token is lower than one it has already seen for the job.
		fmt.Printf("job %d, attempt %d, token %d: %s\n", j.ID, j.Attempt, j.Token(), j.Payload)
		j.SetResult([]byte("done"))
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// Run returns after ctx ends and the running handlers have finished or
	// been given back.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := worker.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
