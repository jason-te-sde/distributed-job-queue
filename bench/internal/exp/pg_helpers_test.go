//go:build pg

package exp

import (
	"context"
	"fmt"
	"net/url"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jason-te-sde/distributed-job-queue/internal/pgtest"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/pgstore/migrate"
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

// slowCommits makes every transaction that inserts into the jobs table take
// delay to commit, once per transaction however many rows it inserts, and keeps
// it from being stopped. The delay is in a deferred constraint trigger, so it
// runs at COMMIT, and the rows are invisible to everybody else until it has
// passed. A client that gives up in the meantime sends a cancel, which would
// end the sleep and abort the transaction; the trigger swallows it and sleeps
// the delay again. That models the end of a real commit, which is past the
// point where it can be stopped and which a client cannot wait for, with a window
// that is wide enough to hit every time. The schema is migrated first, because
// the experiments find it migrated already and do not touch it again.
func slowCommits(t *testing.T, pool *pgxpool.Pool, delay time.Duration) {
	t.Helper()
	if _, err := migrate.Up(t.Context(), pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	for _, stmt := range []string{
		fmt.Sprintf(`CREATE FUNCTION bench_test_slow_commit() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN
				IF current_setting('bench_test.slept', true) IS DISTINCT FROM 'y' THEN
					PERFORM set_config('bench_test.slept', 'y', true);
					BEGIN
						PERFORM pg_sleep(%[1]f);
					EXCEPTION WHEN query_canceled THEN
						PERFORM pg_sleep(%[1]f);
					END;
				END IF;
				RETURN NULL;
			END $$`, delay.Seconds()),
		`CREATE CONSTRAINT TRIGGER bench_test_slow_commit AFTER INSERT ON jobqueue.jobs
			DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION bench_test_slow_commit()`,
	} {
		if _, err := pool.Exec(t.Context(), stmt); err != nil {
			t.Fatalf("install the slow commit: %v", err)
		}
	}
}

// awaitRows waits until the jobs table holds a row, which means that commits are
// landing and the load is well under way.
func awaitRows(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for countRows(t, pool) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no job was committed within 30s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
