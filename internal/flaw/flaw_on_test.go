//go:build jobqueue_flaws

package flaw

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

// probeEnv marks a run of this test binary as the subprocess of
// TestOnReadsEnv.
const probeEnv = "JOBQUEUE_FLAW_PROBE"

// TestFlawProbe prints which flaws init switched on. It does nothing unless
// TestOnReadsEnv started this binary with probeEnv set: init reads
// JOBQUEUE_FLAW once, so each value to check needs a process of its own.
func TestFlawProbe(t *testing.T) {
	if os.Getenv(probeEnv) == "" {
		t.Skip("runs only as a subprocess of TestOnReadsEnv")
	}
	var active []string
	for _, id := range All() {
		if On(id) {
			active = append(active, string(id))
		}
	}
	fmt.Printf("ACTIVE=%s\n", strings.Join(active, ","))
}

// probe re-executes the test binary with JOBQUEUE_FLAW set to value and
// returns what the probe printed, the subprocess's stderr and its exit error.
func probe(t *testing.T, value string) (active []string, stderr string, err error) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestFlawProbe$")
	cmd.Env = append(os.Environ(), probeEnv+"=1", "JOBQUEUE_FLAW="+value)
	var stdout, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &errOut
	err = cmd.Run()
	for line := range strings.Lines(stdout.String()) {
		if rest, ok := strings.CutPrefix(line, "ACTIVE="); ok {
			if rest = strings.TrimSpace(rest); rest != "" {
				active = strings.Split(rest, ",")
			}
		}
	}
	return active, errOut.String(), err
}

func TestOnReadsEnv(t *testing.T) {
	t.Run("empty value switches nothing on", func(t *testing.T) {
		t.Parallel()
		active, stderr, err := probe(t, "")
		if err != nil {
			t.Fatalf("subprocess failed: %v\n%s", err, stderr)
		}
		if len(active) != 0 {
			t.Errorf("active flaws = %v, want none", active)
		}
	})
	for _, id := range All() {
		t.Run("only "+string(id), func(t *testing.T) {
			t.Parallel()
			active, stderr, err := probe(t, string(id))
			if err != nil {
				t.Fatalf("subprocess failed: %v\n%s", err, stderr)
			}
			if want := []string{string(id)}; !slices.Equal(active, want) {
				t.Errorf("active flaws = %v, want %v", active, want)
			}
		})
	}
	for _, value := range []string{"nope", "f8"} {
		t.Run("unknown "+value, func(t *testing.T) {
			t.Parallel()
			_, stderr, err := probe(t, value)
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() == 0 {
				t.Fatalf("subprocess error = %v, want a non-zero exit", err)
			}
			if want := "unknown JOBQUEUE_FLAW " + value; !strings.Contains(stderr, want) {
				t.Errorf("stderr does not contain %q:\n%s", want, stderr)
			}
		})
	}
}
