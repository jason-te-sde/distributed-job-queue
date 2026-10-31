package main

import (
	"bytes"
	"strings"
	"testing"
)

// runArgs runs the command with the given arguments and no environment, and
// returns the exit status and what it wrote.
func runArgs(args ...string) (status int, stdout, stderr string) {
	var out, errOut bytes.Buffer
	status = run(args, func(string) string { return "" }, &out, &errOut)
	return status, out.String(), errOut.String()
}

func TestVersionFlagPrintsTheVersionAndExits(t *testing.T) {
	// --version is read before the flags are parsed, because the parser
	// rejects flags it does not know.
	for _, args := range [][]string{{"--version"}, {"-version"}, {"--grpc-addr=:1", "--version"}} {
		status, stdout, stderr := runArgs(args...)
		if status != 0 || stdout != version+"\n" || stderr != "" {
			t.Errorf("run(%v) = %d, %q, %q; want 0, %q, \"\"", args, status, stdout, stderr, version+"\n")
		}
	}
	if version != "dev" {
		t.Errorf("version defaults to %q, want \"dev\": a build sets it with -ldflags -X main.version=...", version)
	}
}

func TestHelpPrintsTheUsageAndSucceeds(t *testing.T) {
	status, stdout, stderr := runArgs("-h")
	if status != 0 || stderr != "" {
		t.Errorf("run(-h) = %d, stderr %q; want 0 and nothing on stderr", status, stderr)
	}
	for _, want := range []string{"-grpc-addr", "-shutdown-grace", "-version"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("the usage lacks %s: %q", want, stdout)
		}
	}
}

func TestFailuresExitWithStatus1AndOneErrorLine(t *testing.T) {
	for name, tc := range map[string]struct {
		args []string
		want string
	}{
		"unknown flag":      {[]string{"--no-such-flag"}, "no-such-flag"},
		"no database":       {[]string{"--grpc-addr=127.0.0.1:0"}, "database URL"},
		"bad scheduler":     {[]string{"--database-url=postgres://x/y", "--grpc-addr=127.0.0.1:0", "--scheduler=lifo"}, "--scheduler"},
		"unreachable":       {[]string{"--database-url=postgres://127.0.0.1:1/x?sslmode=disable&connect_timeout=1", "--grpc-addr=127.0.0.1:0"}, "jobqueue-server"},
		"unreadable tokens": {[]string{"--database-url=postgres://x/y", "--grpc-addr=127.0.0.1:0", "--tokens-file=/nonexistent/tokens"}, "tokens file"},
	} {
		t.Run(name, func(t *testing.T) {
			status, stdout, stderr := runArgs(tc.args...)
			if status != 1 || stdout != "" {
				t.Fatalf("run = %d, stdout %q; want status 1 and nothing on stdout", status, stdout)
			}
			if !strings.Contains(stderr, tc.want) {
				t.Errorf("stderr = %q, want it to mention %q", stderr, tc.want)
			}
			if lines := strings.Count(strings.TrimRight(stderr, "\n"), "\n") + 1; lines != 1 {
				t.Errorf("stderr has %d lines, want one error line: %q", lines, stderr)
			}
		})
	}
}
