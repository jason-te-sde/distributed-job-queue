//go:build pg

package main

import (
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jason-te-sde/distributed-job-queue/bench/internal/exp"
	"github.com/jason-te-sde/distributed-job-queue/internal/pgtest"
)

// TestPGThroughputCommandPrintsAJSONLine is the smoke run of the CI job at a
// size that fits a test: one JSON line on stdout, a summary on stderr, and the
// line appended to --out.
func TestPGThroughputCommandPrintsAJSONLine(t *testing.T) {
	pool := pgtest.NewEmptyDB(t)
	u, err := url.Parse(pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + pool.Config().ConnConfig.Database
	out := filepath.Join(t.TempDir(), "smoke.json")

	code, stdout, stderr := execute(t, noEnv, "throughput", "--database-url", u.String(), "--rate", "100", "--duration", "2s", "--out", out)
	if code != 0 {
		t.Fatalf("exit code %d: %s", code, stderr)
	}
	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	if len(lines) != 1 {
		t.Fatalf("stdout has %d lines, want one JSON line: %q", len(lines), stdout)
	}
	var r exp.Result
	if err := json.Unmarshal([]byte(lines[0]), &r); err != nil {
		t.Fatalf("stdout is not a result: %v\n%s", err, lines[0])
	}
	if r.Experiment != "throughput" || r.Metrics["started"] != 200 || r.Metrics["sent"] != 200 {
		t.Errorf("result = %s: want throughput, with all 200 jobs sent and started", lines[0])
	}
	if !strings.Contains(stderr, "throughput") || !strings.Contains(stderr, "p99=") {
		t.Errorf("stderr lacks the human summary: %q", stderr)
	}
	if data, err := os.ReadFile(out); err != nil || strings.TrimSpace(string(data)) != lines[0] {
		t.Errorf("--out file = %q, %v; want the same line as stdout", data, err)
	}
}
