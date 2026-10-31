package exp

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/bench/internal/hist"
)

func sampleResult() Result {
	h := hist.New()
	for i := 1; i <= 100; i++ {
		h.Record(time.Duration(i) * time.Millisecond)
	}
	return Result{
		Experiment: "noisy",
		Params:     map[string]string{"scheduler": "drr", "flood": "100000"},
		Metrics:    map[string]float64{"b_jobs": 300},
		Quantiles:  h.Quantiles(),
		CDF:        h.CDF(),
		Env:        map[string]string{"os": "darwin", "arch": "arm64"},
		Started:    time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC),
		Duration:   31 * time.Second,
	}
}

func TestResultJSONRoundTrip(t *testing.T) {
	want := sampleResult()
	line, err := want.JSON()
	if err != nil {
		t.Fatalf("JSON: %v", err)
	}
	if bytes.ContainsRune(line, '\n') {
		t.Errorf("JSON holds a newline, want a single line: %q", line)
	}
	var got Result
	if err := json.Unmarshal(line, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Experiment != want.Experiment || got.Params["flood"] != "100000" || got.Metrics["b_jobs"] != 300 ||
		got.Quantiles["p99"] != want.Quantiles["p99"] || got.Env["arch"] != "arm64" ||
		!got.Started.Equal(want.Started) || got.Duration != want.Duration || len(got.CDF) != len(want.CDF) {
		t.Errorf("round trip changed the result:\n got %+v\nwant %+v", got, want)
	}
}

func TestResultJSONNamesItsUnits(t *testing.T) {
	line, err := sampleResult().JSON()
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{`"quantiles_ns"`, `"duration_ns"`, `"wait_ns"`} {
		if !strings.Contains(string(line), key) {
			t.Errorf("JSON has no %s: a reader of a bare number must be told its unit: %s", key, line)
		}
	}
}

func TestReadResultTakesTheLastOfAnExperiment(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "runs.json")
	var buf bytes.Buffer
	first, second, other := sampleResult(), sampleResult(), sampleResult()
	first.Params = map[string]string{"scheduler": "fifo", "run": "first"}
	second.Params = map[string]string{"scheduler": "fifo", "run": "second"}
	other.Experiment = "throughput"
	other.Params = map[string]string{"run": "other"}
	for _, r := range []Result{first, second, other} {
		line, err := r.JSON()
		if err != nil {
			t.Fatal(err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := ReadResult(path, "noisy")
	if err != nil {
		t.Fatalf("ReadResult: %v", err)
	}
	if got.Params["run"] != "second" {
		t.Errorf("ReadResult returned run %q, want the last result of the experiment, second", got.Params["run"])
	}
	if _, err := ReadResult(path, "soak"); err == nil {
		t.Error("ReadResult found a result of an experiment that is not in the file")
	}
	if _, err := ReadResult(filepath.Join(dir, "missing.json"), "noisy"); err == nil {
		t.Error("ReadResult of a missing file returned no error")
	}
}

func TestReadResultNamesTheBadLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(path, []byte("{\"experiment\":\"noisy\"}\nnot json\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := ReadResult(path, "noisy")
	if err == nil || !strings.Contains(err.Error(), "line 2") {
		t.Errorf("ReadResult error = %v, want it to name line 2", err)
	}
}
