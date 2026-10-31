package chaos

// This file has no build tag: what it holds is pure, and runs under a plain
// go test ./..., not only in the chaos job.

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// excusedExit reports whether the exit of a process that nobody ended is
// excused, which only a broker that failed before it had ever been ready can
// be, and only with the ordinary error exit, status 1, that jobqueue-server
// uses for an error at start (the database is out, for one). A signal, a panic
// (status 2), any other status, and any exit at all of a broker that had been
// serving or of a worker is a crash.
func excusedExit(isBroker, wasReady bool, err error) bool {
	if !isBroker || wasReady {
		return false
	}
	var exit *exec.ExitError
	return errors.As(err, &exit) && exit.ExitCode() == 1
}

// describeExit says how a process ended, from the error of cmd.Wait.
func describeExit(err error) string {
	var exit *exec.ExitError
	switch {
	case err == nil:
		return "exit status 0"
	case errors.As(err, &exit):
		return exit.String()
	}
	return err.Error()
}

// lastLine returns the last non-empty line of a log, cut to 300 bytes, for
// the record of an exit.
func lastLine(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return "(the log could not be read: " + err.Error() + ")"
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	last := lines[len(lines)-1]
	if len(last) > 300 {
		last = last[:300] + "..."
	}
	return last
}

// exitOf runs a shell command and returns the error of its Wait.
func exitOf(t *testing.T, script string) error {
	t.Helper()
	err := exec.Command("sh", "-c", script).Run()
	var exit *exec.ExitError
	if err != nil && !errors.As(err, &exit) {
		t.Fatalf("run %q: %v", script, err)
	}
	return err
}

// TestExcusedExit pins which exits of a process that nobody ended are excused:
// only the ordinary error exit of a broker that had not been ready yet.
func TestExcusedExit(t *testing.T) {
	for _, tc := range []struct {
		name               string
		isBroker, wasReady bool
		script             string
		want               bool
	}{
		{"broker that failed at boot", true, false, "exit 1", true},
		{"broker that panicked at boot", true, false, "exit 2", false},
		{"broker killed by a signal at boot", true, false, "kill -9 $$", false},
		{"broker that ended without an error at boot", true, false, "exit 0", false},
		{"broker with another status at boot", true, false, "exit 3", false},
		{"broker that failed after serving", true, true, "exit 1", false},
		{"worker that failed", false, false, "exit 1", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := excusedExit(tc.isBroker, tc.wasReady, exitOf(t, tc.script)); got != tc.want {
				t.Errorf("excusedExit(broker=%v, ready=%v, %q) = %v, want %v", tc.isBroker, tc.wasReady, tc.script, got, tc.want)
			}
		})
	}
}

// TestDescribeExit pins the words of an exit in the records.
func TestDescribeExit(t *testing.T) {
	for _, tc := range []struct{ script, want string }{
		{"exit 0", "exit status 0"},
		{"exit 1", "exit status 1"},
		{"exit 2", "exit status 2"},
		{"kill -9 $$", "signal: killed"},
	} {
		if got := describeExit(exitOf(t, tc.script)); got != tc.want {
			t.Errorf("describeExit(%q) = %q, want %q", tc.script, got, tc.want)
		}
	}
	if got := describeExit(errors.New("boom")); got != "boom" {
		t.Errorf("describeExit of a plain error = %q, want boom", got)
	}
}

// TestLastLine pins that the record of an exit carries the last line of the
// log, and not an error of its own when the log is missing.
func TestLastLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "broker.log")
	if err := os.WriteFile(path, []byte("=== start 1\nfirst\njobqueue-server: open the database: timeout\n\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got, want := lastLine(path), "jobqueue-server: open the database: timeout"; got != want {
		t.Errorf("lastLine = %q, want %q", got, want)
	}
	long := filepath.Join(dir, "long.log")
	if err := os.WriteFile(long, []byte(strings.Repeat("x", 1000)+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := lastLine(long); len(got) != 303 || !strings.HasSuffix(got, "...") {
		t.Errorf("a long line is cut to 300 bytes and an ellipsis, got %d bytes", len(got))
	}
	if got := lastLine(filepath.Join(dir, "missing.log")); !strings.HasPrefix(got, "(the log could not be read") {
		t.Errorf("lastLine of a missing file = %q", got)
	}
}
