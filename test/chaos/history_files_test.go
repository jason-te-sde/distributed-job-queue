package chaos

// This file has no build tag: what it holds is pure, and runs under a plain
// go test ./..., not only in the chaos job.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/jason-te-sde/distributed-job-queue/internal/history"
)

// tornTail is the unfinished last line of a history file.
type tornTail struct {
	// File is the base name of the file.
	File string
	// Bytes is the size of the unfinished line.
	Bytes int
}

// dropTornTails removes the unfinished last line from every history file in
// files, which maps base names to contents, and returns the files without
// them. A worker killed with SIGKILL can die in the middle of a write, so a
// torn tail is expected in the file of a process that was killed, and nowhere
// else: the files named in killed are the ones whose process was. Their torn
// tails are tolerated; every other torn tail is refused, which means that
// nothing explains it and the caller must fail. Both kinds are dropped from
// the returned contents, so that the rest of the history can still be read
// and checked. Nothing else is repaired.
func dropTornTails(files map[string][]byte, killed map[string]bool) (kept map[string][]byte, tolerated, refused []tornTail) {
	kept = make(map[string][]byte, len(files))
	for _, name := range slices.Sorted(maps.Keys(files)) {
		data, dropped := dropTornTail(files[name])
		kept[name] = data
		switch {
		case dropped == 0:
		case killed[name]:
			tolerated = append(tolerated, tornTail{File: name, Bytes: dropped})
		default:
			refused = append(refused, tornTail{File: name, Bytes: dropped})
		}
	}
	return kept, tolerated, refused
}

// loadHistory reads the history files, *.jsonl, of the directory dir, drops
// their torn tails as dropTornTails says and parses the rest with
// history.ReadAll, which refuses anything else that is not an event. It
// returns the events with the tails that were tolerated and the ones that were
// refused; the caller fails the test for the latter. The files are read in
// the order of their names.
func loadHistory(dir string, killed map[string]bool) (events []history.Event, tolerated, refused []tornTail, err error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("list the history files: %w", err)
	}
	files := make(map[string][]byte, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("read %s: %w", path, err)
		}
		files[filepath.Base(path)] = data
	}
	kept, tolerated, refused := dropTornTails(files, killed)
	names := slices.Sorted(maps.Keys(kept))
	readers := make([]io.Reader, len(names))
	for i, name := range names {
		readers[i] = bytes.NewReader(kept[name])
	}
	events, err = history.ReadAll(readers...)
	if err != nil {
		return nil, tolerated, refused, fmt.Errorf("read the history: %w (files in order: %s)", err, strings.Join(names, ", "))
	}
	return events, tolerated, refused, nil
}

// dropTornTail removes an unfinished last line from data and returns the
// rest and the size of what it removed. A last line without its newline that
// is a whole JSON value is kept: a reader can parse it.
func dropTornTail(data []byte) (kept []byte, dropped int) {
	if len(data) == 0 || data[len(data)-1] == '\n' {
		return data, 0
	}
	cut := bytes.LastIndexByte(data, '\n') + 1
	if json.Valid(data[cut:]) {
		return data, 0
	}
	return data[:cut], len(data) - cut
}

// TestDropTornTail pins what dropTornTail repairs: an unfinished last line
// goes, and nothing else does.
func TestDropTornTail(t *testing.T) {
	const whole = `{"at":"2026-10-31T00:00:00Z","kind":"handler_start","id":1}`
	for _, tc := range []struct {
		name        string
		in          string
		wantKept    string
		wantDropped int
	}{
		{"empty file", "", "", 0},
		{"ends in a newline", whole + "\n", whole + "\n", 0},
		{"one torn line", `{"at":"2026-10`, "", len(`{"at":"2026-10`)},
		{"torn line after whole lines", whole + "\n" + whole + "\n" + `{"at":`, whole + "\n" + whole + "\n", len(`{"at":`)},
		{"whole last line without a newline", whole, whole, 0},
		{"whole last line without a newline after others", whole + "\n" + whole, whole + "\n" + whole, 0},
		{"a torn line in the middle is not repaired", `{"at":` + "\n" + whole + "\n", `{"at":` + "\n" + whole + "\n", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kept, dropped := dropTornTail([]byte(tc.in))
			if string(kept) != tc.wantKept || dropped != tc.wantDropped {
				t.Errorf("dropTornTail(%q) = (%q, %d), want (%q, %d)", tc.in, kept, dropped, tc.wantKept, tc.wantDropped)
			}
		})
	}
}

// TestDropTornTailsAttributesTearsToKilledProcesses pins who may have a torn
// tail: only a process that was killed. The same tear in the file of a
// process that was not killed is refused and named, and a killed process's
// file that is whole is no reason for a complaint.
func TestDropTornTailsAttributesTearsToKilledProcesses(t *testing.T) {
	const (
		whole = `{"at":"2026-10-31T00:00:00Z","kind":"handler_start","id":1}` + "\n"
		torn  = `{"at":"2026-10`
	)
	files := map[string][]byte{
		"producer.jsonl":     []byte(whole + torn),
		"worker-w0-g1.jsonl": []byte(whole + torn),
		"worker-w1-g1.jsonl": []byte(whole + torn),
		"worker-w2-g1.jsonl": []byte(whole),
		"worker-w3-g1.jsonl": []byte(whole + torn),
	}
	killed := map[string]bool{
		"worker-w0-g1.jsonl": true,
		"worker-w2-g1.jsonl": true,
		"worker-w9-g1.jsonl": true, // killed before it wrote a file: no file, no tear
	}

	kept, tolerated, refused := dropTornTails(files, killed)

	wantTolerated := []tornTail{{File: "worker-w0-g1.jsonl", Bytes: len(torn)}}
	if !slices.Equal(tolerated, wantTolerated) {
		t.Errorf("tolerated = %v, want %v", tolerated, wantTolerated)
	}
	wantRefused := []tornTail{
		{File: "producer.jsonl", Bytes: len(torn)},
		{File: "worker-w1-g1.jsonl", Bytes: len(torn)},
		{File: "worker-w3-g1.jsonl", Bytes: len(torn)},
	}
	if !slices.Equal(refused, wantRefused) {
		t.Errorf("refused = %v, want %v", refused, wantRefused)
	}
	for name, data := range kept {
		if string(data) != whole {
			t.Errorf("%s kept as %q, want the whole line only", name, data)
		}
	}
	if len(kept) != len(files) {
		t.Errorf("kept %d files, want %d: a refused file is still read", len(kept), len(files))
	}
}

// TestLoadHistoryAttributesTearsToKilledProcesses pins the same through the
// files of a directory: the events of a killed worker's file survive its torn
// tail, a torn tail in the file of a worker that was not killed is refused by
// name, and a line that is broken anywhere else is an error.
func TestLoadHistoryAttributesTearsToKilledProcesses(t *testing.T) {
	const (
		line = `{"at":"2026-10-31T00:00:00Z","kind":"handler_start","id":1}` + "\n"
		torn = `{"at":"2026-10`
	)
	write := func(dir, name, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	dir := t.TempDir()
	write(dir, "producer.jsonl", line+line)
	write(dir, "worker-w0-g1.jsonl", line+torn)
	write(dir, "worker-w1-g1.jsonl", line+torn)
	events, tolerated, refused, err := loadHistory(dir, map[string]bool{"worker-w0-g1.jsonl": true})
	if err != nil {
		t.Fatalf("loadHistory: %v", err)
	}
	if len(events) != 4 {
		t.Errorf("read %d events, want the 4 whole lines of the three files", len(events))
	}
	if want := []tornTail{{File: "worker-w0-g1.jsonl", Bytes: len(torn)}}; !slices.Equal(tolerated, want) {
		t.Errorf("tolerated = %v, want %v", tolerated, want)
	}
	if want := []tornTail{{File: "worker-w1-g1.jsonl", Bytes: len(torn)}}; !slices.Equal(refused, want) {
		t.Errorf("refused = %v, want %v: the file of a worker that was not killed has a tear nothing explains", refused, want)
	}

	broken := t.TempDir()
	write(broken, "worker-w0-g1.jsonl", torn+"\n"+line)
	if _, _, _, err := loadHistory(broken, map[string]bool{"worker-w0-g1.jsonl": true}); err == nil ||
		!strings.Contains(err.Error(), "worker-w0-g1.jsonl") {
		t.Errorf("a broken line in the middle of a killed worker's file: err = %v, want an error naming the file", err)
	}
}
