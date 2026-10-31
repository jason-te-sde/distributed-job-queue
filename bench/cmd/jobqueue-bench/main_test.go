package main

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/bench/internal/exp"
	"github.com/jason-te-sde/distributed-job-queue/bench/internal/hist"
)

func noEnv(string) string { return "" }

// execute runs the command line and returns what it exited with and wrote.
func execute(t *testing.T, getenv func(string) string, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code = run(context.Background(), args, &out, &errOut, getenv)
	return code, out.String(), errOut.String()
}

func TestRunWithNoCommandPrintsTheCommands(t *testing.T) {
	code, _, stderr := execute(t, noEnv)
	if code != 2 {
		t.Errorf("exit code = %d, want 2", code)
	}
	for _, name := range []string{"throughput", "ablate", "noisy", "recovery", "soak", "river", "worker", "plot"} {
		if !strings.Contains(stderr, name) {
			t.Errorf("the usage does not list %q:\n%s", name, stderr)
		}
	}
}

func TestRunHelpIsNotAnError(t *testing.T) {
	for _, arg := range []string{"help", "-h", "--help"} {
		code, stdout, _ := execute(t, noEnv, arg)
		if code != 0 || !strings.Contains(stdout, "throughput") {
			t.Errorf("%s: exit code %d, stdout %q; want 0 and the list of commands", arg, code, stdout)
		}
	}
}

func TestRunUnknownCommand(t *testing.T) {
	code, _, stderr := execute(t, noEnv, "latency")
	if code != 2 || !strings.Contains(stderr, `"latency"`) {
		t.Errorf("exit code %d, stderr %q; want 2 and the name of the command", code, stderr)
	}
}

func TestEverySubcommandHasHelp(t *testing.T) {
	for _, tc := range []struct{ name, flag string }{
		{"throughput", "find-max"}, {"ablate", "what"}, {"noisy", "scheduler"}, {"recovery", "kill"},
		{"soak", "retention"}, {"river", "find-max"}, {"worker", "addr"}, {"plot", "in"},
	} {
		code, _, stderr := execute(t, noEnv, tc.name, "-h")
		if code != 0 || !strings.Contains(stderr, "\n  -"+tc.flag) {
			t.Errorf("%s -h: exit code %d, stderr %q; want 0 and a usage that lists the flag %s", tc.name, code, stderr, tc.flag)
		}
	}
}

func TestSubcommandFlagErrorsExitWithTwo(t *testing.T) {
	for _, args := range [][]string{
		{"throughput", "--rate", "fast"},
		{"throughput", "--no-such-flag"},
		{"throughput", "stray"},
		{"noisy", "--flood", "many"},
		{"plot", "--in"},
	} {
		if code, _, _ := execute(t, noEnv, args...); code != 2 {
			t.Errorf("%v: exit code = %d, want 2", args, code)
		}
	}
}

func TestExperimentWithoutADatabaseSaysSo(t *testing.T) {
	for _, args := range [][]string{
		{"throughput", "--rate", "10"},
		{"ablate", "--what", "batch", "--rate", "10"},
		{"noisy", "--scheduler", "drr"},
		{"recovery", "--kill", "worker"},
		{"soak", "--rate", "10"},
		{"river", "--rate", "10"},
	} {
		code, _, stderr := execute(t, noEnv, args...)
		if code != 1 || !strings.Contains(stderr, "JOBQUEUE_DATABASE_URL") {
			t.Errorf("%v: exit code %d, stderr %q; want 1 and a message that names JOBQUEUE_DATABASE_URL", args, code, stderr)
		}
	}
}

func TestDatabaseURLComesFromTheEnvironmentWhenNoFlagIsGiven(t *testing.T) {
	// The URL is one that nothing listens on, so the run fails when it tries to
	// connect: with a message about the connection, not about a missing URL.
	env := func(k string) string {
		if k == "JOBQUEUE_DATABASE_URL" {
			return "postgres://127.0.0.1:1/none?connect_timeout=1"
		}
		return ""
	}
	code, _, stderr := execute(t, env, "throughput", "--rate", "10")
	if code != 1 || strings.Contains(stderr, "no database URL") {
		t.Errorf("exit code %d, stderr %q; want the environment's URL to be used", code, stderr)
	}
	// The flag wins over the environment.
	code, _, stderr = execute(t, env, "throughput", "--rate", "10", "--database-url", "not a url")
	if code != 1 || !strings.Contains(stderr, "parse the database URL") {
		t.Errorf("exit code %d, stderr %q; want the flag's URL to be the one parsed", code, stderr)
	}
}

func TestWorkerNeedsItsAddress(t *testing.T) {
	code, _, stderr := execute(t, noEnv, "worker", "--queue", "q")
	if code != 1 || !strings.Contains(stderr, "--addr") {
		t.Errorf("exit code %d, stderr %q; want 1 and the missing --addr named", code, stderr)
	}
}

func TestSinkWritesJSONToStdoutASummaryToStderrAndAppendsToTheFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.json")
	result := func(phase string) exp.Result {
		h := hist.New()
		h.Record(5 * time.Millisecond)
		return exp.Result{
			Experiment: "throughput", Params: map[string]string{"phase": phase, "rate": "100"},
			Metrics: map[string]float64{"offered_rate": 100}, Quantiles: h.Quantiles(), Env: map[string]string{}, Started: time.Now(),
		}
	}
	for run := 1; run <= 2; run++ {
		var out, errOut bytes.Buffer
		s, err := newSink(&out, &errOut, path)
		if err != nil {
			t.Fatal(err)
		}
		s.emit(result("probe"))
		s.emit(result("max"))
		if err := s.close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		lines := strings.Split(strings.TrimSpace(out.String()), "\n")
		if len(lines) != 2 {
			t.Fatalf("stdout has %d lines, want one JSON line per result: %q", len(lines), out.String())
		}
		for _, line := range lines {
			var r exp.Result
			if err := json.Unmarshal([]byte(line), &r); err != nil {
				t.Fatalf("stdout line %q is not a result: %v", line, err)
			}
		}
		if got := errOut.String(); !strings.Contains(got, "throughput") || !strings.Contains(got, "phase=max") || !strings.Contains(got, "p99=") {
			t.Errorf("the summary on stderr = %q, want the experiment, the phase and the quantiles", got)
		}
		if strings.Contains(errOut.String(), `{"experiment"`) {
			t.Errorf("stderr holds JSON: %q", errOut.String())
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Count(string(data), "\n"); got != 2*run {
			t.Errorf("after run %d the file has %d lines, want %d: --out appends", run, got, 2*run)
		}
	}
}

func TestSinkReportsAFileThatCannotBeOpened(t *testing.T) {
	_, err := newSink(io.Discard, io.Discard, filepath.Join(t.TempDir(), "missing-dir", "out.json"))
	if err == nil {
		t.Error("newSink on a path in a missing directory returned no error")
	}
}

// noisyResult is the result of a noisy run under the scheduler, whose CDF
// climbs from lo to hi.
func noisyResult(scheduler, flood string, lo, hi time.Duration) exp.Result {
	h := hist.New()
	for i := range 300 {
		h.Record(lo + (hi-lo)*time.Duration(i)/300)
	}
	return exp.Result{
		Experiment: "noisy",
		Params: map[string]string{
			"scheduler": scheduler, "flood": flood, "b_rate": "10", "duration": "30s", "service_time": "2ms",
			"workers": "2", "concurrency": "4", "brokers": "1", "payload_bytes": "64",
		},
		Quantiles: h.Quantiles(), CDF: h.CDF(), Env: map[string]string{}, Started: time.Now(),
	}
}

func writeResult(t *testing.T, dir, name string, r exp.Result) string {
	t.Helper()
	line, err := r.JSON()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, append(line, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestPlotCommandWritesAWellFormedSVGOfTwoRuns(t *testing.T) {
	dir := t.TempDir()
	fifo := writeResult(t, dir, "noisy-fifo.json", noisyResult("fifo", "100000", time.Second, 25*time.Second))
	drr := writeResult(t, dir, "noisy-drr.json", noisyResult("drr", "100000", time.Millisecond, 40*time.Millisecond))
	out := filepath.Join(dir, "docs", "benchmarks", "noisy-neighbor.svg")

	code, _, stderr := execute(t, noEnv, "plot", "--in", fifo, "--in", drr, "--out", out)
	if code != 0 {
		t.Fatalf("exit code %d: %s", code, stderr)
	}
	svg, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("the SVG was not written: %v", err)
	}
	dec := xml.NewDecoder(bytes.NewReader(svg))
	paths, text := 0, strings.Builder{}
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("the SVG is not well-formed: %v", err)
		}
		switch tok := tok.(type) {
		case xml.StartElement:
			if tok.Name.Local == "path" {
				paths++
			}
		case xml.CharData:
			text.Write(tok)
		}
	}
	if paths != 2 {
		t.Errorf("%d series in the SVG, want 2", paths)
	}
	for _, want := range []string{"FIFO", "DRR", "100000", "10/s"} {
		if !strings.Contains(text.String(), want) {
			t.Errorf("the SVG text lacks %q: %q", want, text.String())
		}
	}
}

func TestPlotCommandRefusesRunsThatDifferInMoreThanTheScheduler(t *testing.T) {
	dir := t.TempDir()
	fifo := writeResult(t, dir, "fifo.json", noisyResult("fifo", "100000", time.Second, 25*time.Second))
	drr := writeResult(t, dir, "drr.json", noisyResult("drr", "5000", time.Millisecond, 40*time.Millisecond))
	code, _, stderr := execute(t, noEnv, "plot", "--in", fifo, "--in", drr, "--out", filepath.Join(dir, "x.svg"))
	if code != 1 || !strings.Contains(stderr, "flood") {
		t.Errorf("exit code %d, stderr %q; want 1 and the differing param, flood, named", code, stderr)
	}
	if _, err := os.Stat(filepath.Join(dir, "x.svg")); err == nil {
		t.Error("an SVG was written for runs that cannot be compared")
	}
}

func TestPlotCommandRefusesTwoRunsOfTheSameScheduler(t *testing.T) {
	dir := t.TempDir()
	a := writeResult(t, dir, "a.json", noisyResult("drr", "100000", time.Millisecond, 40*time.Millisecond))
	b := writeResult(t, dir, "b.json", noisyResult("drr", "100000", time.Millisecond, 50*time.Millisecond))
	code, _, stderr := execute(t, noEnv, "plot", "--in", a, "--in", b, "--out", filepath.Join(dir, "x.svg"))
	if code != 1 || !strings.Contains(stderr, "scheduler") {
		t.Errorf("exit code %d, stderr %q; want 1 and the scheduler named", code, stderr)
	}
}

func TestPlotCommandNeedsTwoInputsAndAnOutput(t *testing.T) {
	dir := t.TempDir()
	one := writeResult(t, dir, "one.json", noisyResult("fifo", "1", time.Millisecond, time.Second))
	for name, args := range map[string][]string{
		"one input":       {"plot", "--in", one, "--out", filepath.Join(dir, "x.svg")},
		"no output":       {"plot", "--in", one, "--in", one},
		"no inputs":       {"plot", "--out", filepath.Join(dir, "x.svg")},
		"a missing input": {"plot", "--in", one, "--in", filepath.Join(dir, "missing.json"), "--out", filepath.Join(dir, "x.svg")},
	} {
		if code, _, _ := execute(t, noEnv, args...); code == 0 {
			t.Errorf("%s: exit code 0, want a failure", name)
		}
	}
}

func TestPlotCommandRefusesAResultWithoutACDF(t *testing.T) {
	dir := t.TempDir()
	bare := noisyResult("fifo", "1", time.Millisecond, time.Second)
	bare.CDF = nil
	a := writeResult(t, dir, "a.json", bare)
	b := writeResult(t, dir, "b.json", noisyResult("drr", "1", time.Millisecond, time.Second))
	code, _, stderr := execute(t, noEnv, "plot", "--in", a, "--in", b, "--out", filepath.Join(dir, "x.svg"))
	if code != 1 || !strings.Contains(stderr, "CDF") {
		t.Errorf("exit code %d, stderr %q; want 1 and the missing CDF named", code, stderr)
	}
}
