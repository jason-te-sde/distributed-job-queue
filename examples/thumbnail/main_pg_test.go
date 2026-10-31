//go:build pg

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	jobqueue "github.com/jason-te-sde/distributed-job-queue"
	"github.com/jason-te-sde/distributed-job-queue/internal/broker"
	"github.com/jason-te-sde/distributed-job-queue/internal/pgtest"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/pgstore/migrate"
)

// syncBuffer is a writer that the command's goroutine and the test share.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// databaseURL returns the URL of the database that pool is connected to, on
// the server of the test DSN.
func databaseURL(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	u, err := url.Parse(pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + pool.Config().ConnConfig.Database
	return u.String()
}

// startBroker runs a broker on a fresh, migrated database, on a loopback port,
// until the test ends. extra are more flags of the broker. It returns the
// address of the gRPC API.
func startBroker(t *testing.T, extra ...string) string {
	t.Helper()
	pool := pgtest.NewEmptyDB(t)
	if _, err := migrate.Up(t.Context(), pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	args := append([]string{
		"--database-url=" + databaseURL(t, pool), "--grpc-addr=127.0.0.1:0", "--http-addr=127.0.0.1:0", "--shutdown-grace=2s",
	}, extra...)
	cfg, err := broker.ParseFlags(args, func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	b, err := broker.New(t.Context(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("start the broker: %v", err)
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("the broker stopped with %v", err)
		}
	})
	return b.GRPCAddr().String()
}

// eventually polls cond until it reports done, and fails the test if it does
// not within the time. cond also says what it saw, which the failure shows.
func eventually(t *testing.T, what string, within time.Duration, cond func() (done bool, seen string)) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		done, seen := cond()
		if done {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within %v; last seen: %s", what, within, seen)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// endToEnd runs the two commands against a broker, as a deployment does, and
// checks what lands in the sink.
//
// Before the worker starts, the test enqueues two jobs of its own through the
// SDK, which the worker must fail for good: one whose payload cannot be
// rendered, and one whose sink row a newer lease has already written. Then it
// produces three jobs with the command, starts the worker, and waits for the
// worker to finish all five: the produced ones succeeded, the other two dead.
func endToEnd(t *testing.T, producer *jobqueue.Producer, addr string, produceArgs, workArgs []string, workEnv map[string]string) {
	t.Helper()
	ctx := t.Context()

	sinkPool := pgtest.NewEmptyDB(t)
	sink := NewSink(sinkPool)
	if err := sink.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	enqueue := func(payload string) int64 {
		t.Helper()
		res, err := producer.Enqueue(ctx, jobqueue.EnqueueParams{Queue: queueName, Payload: []byte(payload), MaxAttempts: 5})
		if err != nil {
			t.Fatal(err)
		}
		return res.ID
	}
	badID := enqueue(`{"seed":1,"size":10}`)
	fencedID := enqueue(`{"seed":2,"size":64}`)
	const newerToken = 1 << 40
	if applied, err := sink.Write(ctx, fencedID, newerToken, "newer-holder", 1); err != nil || !applied {
		t.Fatalf("seed the sink with a newer lease: applied = %v, err = %v", applied, err)
	}

	var out, errs syncBuffer
	if status := run(ctx, append([]string{"produce", "--addr", addr, "--count", "3"}, produceArgs...), env(nil), &out, &errs); status != 0 {
		t.Fatalf("produce exited with %d: %s", status, errs.String())
	}
	if !strings.Contains(out.String(), "enqueued 3 jobs") {
		t.Errorf("produce said %q, want it to report 3 jobs", out.String())
	}

	workEnv["JOBQUEUE_DATABASE_URL"] = databaseURL(t, sinkPool)
	workCtx, stop := context.WithCancel(ctx)
	defer stop()
	var workOut, workErrs syncBuffer
	status := make(chan int, 1)
	go func() {
		status <- run(workCtx, append([]string{"work", "--addr", addr, "--concurrency", "2"}, workArgs...), env(workEnv), &workOut, &workErrs)
	}()

	jobInfo := func(id int64) *jobqueue.JobInfo {
		info, err := producer.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return info
	}
	// producedIDs are the jobs the produce command made. The sink has a row
	// for each of them once the worker has written it, and the row of the
	// fenced job was seeded above, so it is left out.
	producedIDs := func() []int64 {
		rows, err := sinkPool.Query(ctx, "SELECT job_id FROM thumbnails WHERE job_id <> $1 ORDER BY job_id", fencedID)
		if err != nil {
			t.Fatal(err)
		}
		ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			t.Fatal(err)
		}
		return ids
	}
	// A job is finished when the broker says so, which is later than the row
	// being written: the handler must also return nil. A handler that wrote
	// the row and then failed would leave every other assertion true.
	eventually(t, "the worker to finish every job", 30*time.Second, func() (bool, string) {
		ids := producedIDs()
		states := map[string]string{"bad job": jobInfo(badID).State, "fenced job": jobInfo(fencedID).State}
		for _, id := range ids {
			states[fmt.Sprintf("produced job %d", id)] = jobInfo(id).State
		}
		seen := fmt.Sprintf("%d of 3 produced jobs have a row; states %v", len(ids), states)
		if len(ids) != 3 || states["bad job"] != "dead" || states["fenced job"] != "dead" {
			return false, seen
		}
		for _, id := range ids {
			if states[fmt.Sprintf("produced job %d", id)] != "succeeded" {
				return false, seen
			}
		}
		return true, seen
	})
	stop()
	select {
	case s := <-status:
		if s != 0 {
			t.Errorf("work exited with %d after its context ended: %s", s, workErrs.String())
		}
	case <-time.After(30 * time.Second):
		t.Fatal("work did not exit within 30s of its context ending")
	}

	// The three produced jobs are the seeds 0, 1 and 2 at the source size, and
	// each wrote the SHA-256 of its PNG.
	want := make([]string, 3)
	for seed := range want {
		_, want[seed], _ = Thumbnail(Payload{Seed: int64(seed), Size: sourceSize}) // checked by TestThumbnailDeterministic
	}
	rowsOut, err := sinkPool.Query(ctx, "SELECT sha256, token FROM thumbnails WHERE job_id <> $1 ORDER BY sha256", fencedID)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for rowsOut.Next() {
		var sum string
		var token int64
		if err := rowsOut.Scan(&sum, &token); err != nil {
			t.Fatal(err)
		}
		if token < 1 {
			t.Errorf("a row has token %d, want a fencing token of at least 1", token)
		}
		got = append(got, sum)
	}
	if err := rowsOut.Err(); err != nil {
		t.Fatal(err)
	}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("sink holds the sums %v, want %v", got, want)
	}

	var token int64
	var sum string
	if err := sinkPool.QueryRow(ctx, "SELECT token, sha256 FROM thumbnails WHERE job_id = $1", fencedID).Scan(&token, &sum); err != nil {
		t.Fatal(err)
	}
	if token != newerToken || sum != "newer-holder" {
		t.Errorf("the superseded write changed the row: token %d, sum %q", token, sum)
	}

	for _, id := range producedIDs() {
		if info := jobInfo(id); info.State != "succeeded" || info.Attempts != 1 || info.LastError != "" {
			t.Errorf("produced job %d: state %q, attempts %d, last error %q; want succeeded at the first attempt", id, info.State, info.Attempts, info.LastError)
		}
	}
	for _, c := range []struct {
		name string
		id   int64
		want string
	}{{"unrenderable payload", badID, "invalid payload"}, {"superseded write", fencedID, "superseded by a newer lease"}} {
		info := jobInfo(c.id)
		if info.Attempts != 1 || !strings.Contains(info.LastError, c.want) {
			t.Errorf("%s: attempts = %d, last error %q; want one attempt and an error with %q", c.name, info.Attempts, info.LastError, c.want)
		}
	}
}

func TestPGProduceAndWorkInsecure(t *testing.T) {
	addr := startBroker(t)
	conn, err := jobqueue.Dial(addr, jobqueue.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	endToEnd(t, jobqueue.NewProducer(conn, jobqueue.ProducerOptions{}), addr,
		[]string{"--insecure"}, []string{"--insecure"}, map[string]string{})
}

func TestPGProduceAndWorkOverTLSWithTokens(t *testing.T) {
	cert, key := testCert(t)
	const producerToken, workerToken = "producer-secret", "worker-secret"
	hash := func(token string) string { h := sha256.Sum256([]byte(token)); return hex.EncodeToString(h[:]) }
	tokens := filepath.Join(t.TempDir(), "tokens")
	lines := fmt.Sprintf("producer tenant=acme %s\nworker queues=%s %s\n", hash(producerToken), queueName, hash(workerToken))
	if err := os.WriteFile(tokens, []byte(lines), 0o600); err != nil {
		t.Fatal(err)
	}
	addr := startBroker(t, "--tls-cert="+cert, "--tls-key="+key, "--tokens-file="+tokens)

	pemBytes, err := os.ReadFile(cert)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(pemBytes)
	var conn *grpc.ClientConn
	if conn, err = jobqueue.Dial(addr, jobqueue.WithTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}), jobqueue.WithToken(producerToken)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// Without a token the broker refuses the command, so the token the
	// commands send below is what lets them in.
	var out, errs syncBuffer
	if status := run(t.Context(), []string{"produce", "--addr", addr, "--tls-ca", cert}, env(nil), &out, &errs); status != 1 || !strings.Contains(strings.ToLower(errs.String()), "unauthenticated") {
		t.Errorf("produce without a token: status %d, stderr %q; want 1 and an unauthenticated error", status, errs.String())
	}

	// The producer's token comes from a flag and the worker's from the
	// environment, so both ways are used.
	endToEnd(t, jobqueue.NewProducer(conn, jobqueue.ProducerOptions{}), addr,
		[]string{"--tls-ca", cert, "--token", producerToken, "--tenant", "acme"},
		[]string{"--tls-ca", cert},
		map[string]string{"JOBQUEUE_TOKEN": workerToken})
}
