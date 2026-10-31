package chaos

// This file has no build tag: what it holds is pure, and runs under a plain
// go test ./..., not only in the chaos job.

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// The lines that jobqueue-server prints when it cannot reach its database
// while it starts begin in one of two ways: cmd/jobqueue-server prefixes the
// error of broker.New with its name, and broker.New returns either the error
// of opening the database or the error of reading the schema version.
const (
	// bootOpenPrefix starts the error of opening the database: broker.New says
	// "open the database" and pgstore.Open says "pgstore open", and wraps what
	// the driver said.
	bootOpenPrefix = "jobqueue-server: open the database: pgstore open: "
	// bootCheckPrefix starts the error of reading the schema version, which
	// migrate.Check returns with the driver's error as it is, so no "store
	// unavailable" is in it. A database that is reachable but wrong has other
	// lines: a version that does not match (and a table that is missing, which
	// Check takes for version 0) prints "database schema version ...".
	bootCheckPrefix = "jobqueue-server: migrate: read the schema version: "
)

// unreachableMarkers are the parts of such an error that say that the database
// could not be reached, or was not accepting connections yet, and not that the
// broker was told something wrong. pgstore says "store unavailable" for every
// failure to connect, a wrong password and a missing database included, so
// that phrase cannot tell them apart; the cause that follows it can:
//
//   - "dial error": the connection was refused, or timed out, or had no route;
//   - "EOF" and "connection reset": the server, or a proxy, closed it;
//   - "context deadline exceeded": the connect timeout of the URL ran out, as
//     it does when nothing answers;
//   - SQLSTATE 57P0x, 53300 and 08xxx: the server is starting up or shutting
//     down, has no connection to give, or the connection failed.
//
// A password that is wrong (SQLSTATE 28P01), a database that does not exist
// (3D000) and a role that may not read the schema table (42501) carry none of
// them.
var unreachableMarkers = []string{
	"dial error", "EOF", "connection reset", "context deadline exceeded",
	"SQLSTATE 57P0", "SQLSTATE 53300", "SQLSTATE 08",
}

// databaseUnreachable reports whether last, the last line of a broker's log,
// says that the broker exited because it could not reach its database, either
// when it opened it or when it read the schema version a moment later. Any
// other error at start is a bug, whether of the broker or of the test: a
// schema that is not the binary's, a flag or a configuration, TLS or tokens, a
// port that is taken, a wrong password, a role without rights.
func databaseUnreachable(last string) bool {
	for _, prefix := range []string{bootOpenPrefix, bootCheckPrefix} {
		if cause, ok := strings.CutPrefix(last, prefix); ok {
			return slices.ContainsFunc(unreachableMarkers, func(m string) bool { return strings.Contains(cause, m) })
		}
	}
	return false
}

// excusedExit reports whether the exit of a process that nobody ended is
// excused, which only a broker that failed before it had ever been ready can
// be, with the ordinary error exit, status 1, and with a last line of its log
// that says the database could not be reached. cmd/jobqueue-server exits with
// status 1 for every error at start, so the status alone proves nothing. A
// signal, a panic (status 2), any other status, any other error at start, and
// any exit at all of a broker that had been serving or of a worker is a crash.
func excusedExit(isBroker, wasReady bool, err error, last string) bool {
	if !isBroker || wasReady {
		return false
	}
	var exit *exec.ExitError
	return errors.As(err, &exit) && exit.ExitCode() == 1 && databaseUnreachable(last)
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

// lastLine returns the last line of a log that has any text, whole: the
// classification of a broker's exit reads it.
func lastLine(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return "(the log could not be read: " + err.Error() + ")"
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	return lines[len(lines)-1]
}

// clip cuts s to n bytes and an ellipsis, for a record.
func clip(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
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

// The last lines of a broker's log, as jobqueue-server prints them for the
// errors at start that matter here. Most were produced by running the built
// server against the condition (a closed port, a blackholed proxy, a toxiproxy
// limit_data and reset_peer toxic, a wrong password, a missing database, a
// role without rights on the schema, a bad flag, a taken port). The ones
// marked composed put the real prefix on a real cause that was seen in another
// phase, or on the text of the PostgreSQL error.
const (
	lineRefused    = "jobqueue-server: open the database: pgstore open: store unavailable: failed to connect to `user=postgres database=exp_ok`: 127.0.0.1:1 (127.0.0.1): dial error: dial tcp 127.0.0.1:1: connect: connection refused"
	lineTimeout    = "jobqueue-server: open the database: pgstore open: failed to connect to `user=postgres database=exp_ok`: 127.0.0.1:61498 (127.0.0.1): failed to receive message: timeout: context deadline exceeded"
	lineEOF        = "jobqueue-server: open the database: pgstore open: store unavailable: failed to connect to `user=postgres database=chaos_3_1793458886`: 127.0.0.1:63506 (127.0.0.1): failed to receive message: unexpected EOF"
	lineOpenEOF    = "jobqueue-server: open the database: pgstore open: store unavailable: unexpected EOF"
	lineOpenReset  = "jobqueue-server: open the database: pgstore open: store unavailable: failed to connect to `user=postgres database=exp_ok`: 127.0.0.1:54402 (127.0.0.1): failed to receive message: read tcp 127.0.0.1:54404->127.0.0.1:54402: read: connection reset by peer"
	lineStartup    = "jobqueue-server: open the database: pgstore open: store unavailable: failed to connect to `user=postgres database=chaos_3_1`: 127.0.0.1:5432 (127.0.0.1): server error: FATAL: the database system is starting up (SQLSTATE 57P03)" // composed
	lineCheckEOF   = "jobqueue-server: migrate: read the schema version: unexpected EOF"
	lineCheckReset = "jobqueue-server: migrate: read the schema version: failed to receive message: read tcp 127.0.0.1:54404->127.0.0.1:54402: read: connection reset by peer"                                         // composed
	lineCheckAdmin = "jobqueue-server: migrate: read the schema version: FATAL: terminating connection due to administrator command (SQLSTATE 57P01)"                                                                  // composed
	lineCheckDown  = "jobqueue-server: migrate: read the schema version: failed to connect to `user=postgres database=exp_ok`: 127.0.0.1:1 (127.0.0.1): dial error: dial tcp 127.0.0.1:1: connect: connection refused" // composed

	lineSchema      = "jobqueue-server: database schema version 0, this binary needs 1: run jobqueuectl migrate"
	lineSchemaAhead = "jobqueue-server: database schema version 2, this binary needs 1: run jobqueuectl migrate" // composed
	linePassword    = "jobqueue-server: open the database: pgstore open: store unavailable: failed to connect to `user=postgres database=exp_ok`: 127.0.0.1:55432 (127.0.0.1): failed SASL auth: FATAL: password authentication failed for user \"postgres\" (SQLSTATE 28P01)"
	lineNoDB        = "jobqueue-server: open the database: pgstore open: store unavailable: failed to connect to `user=postgres database=nope`: 127.0.0.1:55432 (127.0.0.1): server error: FATAL: database \"nope\" does not exist (SQLSTATE 3D000)"
	lineNoRights    = "jobqueue-server: migrate: read the schema version: ERROR: permission denied for schema jobqueue (SQLSTATE 42501)"
	lineBadDSN      = "jobqueue-server: open the database: pgstore open: cannot parse `not a url`: failed to parse as keyword/value (invalid keyword/value)"
	lineFlag        = "jobqueue-server: --reap-interval must be positive, got 0s"
	lineUnknown     = "jobqueue-server: flag provided but not defined: -nope"
	lineNoURL       = "jobqueue-server: no database URL: pass --database-url or set JOBQUEUE_DATABASE_URL"
	lineTLS         = "jobqueue-server: load the TLS certificate and key: open /nonexistent/c.pem: no such file or directory"
	lineTokens      = "jobqueue-server: tokens file: open /nonexistent/tokens: no such file or directory"
	lineBind        = "jobqueue-server: listen on the gRPC address: listen tcp 127.0.0.1:61520: bind: address already in use"
)

// TestDatabaseUnreachable pins which last lines of a broker's log say that the
// database could not be reached, whether while opening it or while reading the
// schema version, and which are errors of another kind: a schema mismatch, a
// flag, TLS, tokens, a port that is taken and a role without rights are all
// status 1 as well, and a wrong password and a missing database are reported
// as "store unavailable" like a refused connection.
func TestDatabaseUnreachable(t *testing.T) {
	for _, tc := range []struct {
		name string
		line string
		want bool
	}{
		{"open: connection refused", lineRefused, true},
		{"open: connect timeout", lineTimeout, true},
		{"open: connection closed while connecting", lineEOF, true},
		{"open: connection closed", lineOpenEOF, true},
		{"open: connection reset", lineOpenReset, true},
		{"open: server starting up", lineStartup, true},
		{"schema read: connection closed", lineCheckEOF, true},
		{"schema read: connection reset", lineCheckReset, true},
		{"schema read: server shut down", lineCheckAdmin, true},
		{"schema read: connection refused", lineCheckDown, true},

		{"schema version mismatch, or a table that is missing", lineSchema, false},
		{"schema version ahead of the binary", lineSchemaAhead, false},
		{"wrong password", linePassword, false},
		{"database does not exist", lineNoDB, false},
		{"schema read: role without rights", lineNoRights, false},
		{"unparsable URL", lineBadDSN, false},
		{"invalid flag value", lineFlag, false},
		{"unknown flag", lineUnknown, false},
		{"no database URL", lineNoURL, false},
		{"TLS files", lineTLS, false},
		{"tokens file", lineTokens, false},
		{"port taken", lineBind, false},
		{"empty log", "", false},
		{"log that could not be read", "(the log could not be read: open x: no such file or directory)", false},
		{"a log line of the broker", `{"time":"2026-10-31T02:55:07.114194-07:00","level":"INFO","msg":"broker started"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := databaseUnreachable(tc.line); got != tc.want {
				t.Errorf("databaseUnreachable(%q) = %v, want %v", tc.line, got, tc.want)
			}
		})
	}
}

// TestExcusedExit pins which exits of a process that nobody ended are excused:
// only the status 1 of a broker that had not been ready yet, and only when the
// last line of its log says that the database could not be reached.
func TestExcusedExit(t *testing.T) {
	for _, tc := range []struct {
		name               string
		isBroker, wasReady bool
		script, last       string
		want               bool
	}{
		{"broker that could not reach its database at boot", true, false, "exit 1", lineRefused, true},
		{"broker whose database timed out at boot", true, false, "exit 1", lineTimeout, true},
		{"broker that lost its database while reading the schema version", true, false, "exit 1", lineCheckEOF, true},
		{"status 1 at boot with a role that may not read the schema", true, false, "exit 1", lineNoRights, false},
		{"status 1 at boot with a schema version ahead", true, false, "exit 1", lineSchemaAhead, false},
		{"panic while reading the schema version", true, false, "exit 2", lineCheckEOF, false},
		{"status 1 at boot with a schema mismatch", true, false, "exit 1", lineSchema, false},
		{"status 1 at boot with a configuration error", true, false, "exit 1", lineFlag, false},
		{"status 1 at boot with a wrong password", true, false, "exit 1", linePassword, false},
		{"status 1 at boot with a port taken", true, false, "exit 1", lineBind, false},
		{"status 1 at boot with no line to read", true, false, "exit 1", "", false},
		{"broker that panicked at boot", true, false, "exit 2", lineRefused, false},
		{"broker killed by a signal at boot", true, false, "kill -9 $$", lineRefused, false},
		{"broker that ended without an error at boot", true, false, "exit 0", lineRefused, false},
		{"broker with another status at boot", true, false, "exit 3", lineRefused, false},
		{"broker that failed after serving", true, true, "exit 1", lineRefused, false},
		{"worker that failed", false, false, "exit 1", lineRefused, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := excusedExit(tc.isBroker, tc.wasReady, exitOf(t, tc.script), tc.last); got != tc.want {
				t.Errorf("excusedExit(broker=%v, ready=%v, %q, %q) = %v, want %v", tc.isBroker, tc.wasReady, tc.script, tc.last, got, tc.want)
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
// log, whole, and not an error of its own when the log is missing.
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
	whole := strings.Repeat("x", 1000)
	if err := os.WriteFile(long, []byte(whole+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := lastLine(long); got != whole {
		t.Errorf("a long last line is cut to %d bytes: the classification would read half of it", len(got))
	}
	if got := lastLine(filepath.Join(dir, "missing.log")); !strings.HasPrefix(got, "(the log could not be read") {
		t.Errorf("lastLine of a missing file = %q", got)
	}
}

// TestClip pins how a record is shortened.
func TestClip(t *testing.T) {
	if got := clip("short", 10); got != "short" {
		t.Errorf("clip of a short string = %q", got)
	}
	if got := clip(strings.Repeat("x", 20), 10); got != strings.Repeat("x", 10)+"..." {
		t.Errorf("clip of a long string = %q", got)
	}
}
