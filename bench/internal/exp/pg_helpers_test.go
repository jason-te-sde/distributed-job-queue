//go:build pg

package exp

import (
	"context"
	"net/url"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jason-te-sde/distributed-job-queue/internal/pgtest"
)

// testDatabase creates an empty database on the server that JOBQUEUE_TEST_DSN
// names, which the experiment migrates itself, and returns its URL and a pool
// on it. The database is dropped when the test ends.
func testDatabase(t *testing.T) (string, *pgxpool.Pool) {
	t.Helper()
	pool := pgtest.NewEmptyDB(t)
	u, err := url.Parse(pgtest.DSN(t))
	if err != nil {
		t.Fatalf("parse JOBQUEUE_TEST_DSN: %v", err)
	}
	u.Path = "/" + pool.Config().ConnConfig.Database
	return u.String(), pool
}

// countRows is the number of rows in the jobs table, whatever their queue.
func countRows(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM jobqueue.jobs").Scan(&n); err != nil {
		t.Fatalf("count the jobs: %v", err)
	}
	return n
}

// buildBench compiles the jobqueue-bench command, which the recovery
// experiment runs as the worker process, and returns the path of the binary.
func buildBench(t *testing.T) string {
	t.Helper()
	exe := filepath.Join(t.TempDir(), "jobqueue-bench")
	out, err := exec.Command("go", "build", "-o", exe, "github.com/jason-te-sde/distributed-job-queue/bench/cmd/jobqueue-bench").CombinedOutput()
	if err != nil {
		t.Fatalf("go build jobqueue-bench: %v\n%s", err, out)
	}
	return exe
}

// collectResults returns an Emit that appends to the slice it also returns.
func collectResults() (Emit, *[]Result) { return collect() }
