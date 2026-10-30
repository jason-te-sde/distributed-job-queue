package history

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// at returns the instant s seconds after t0.
func at(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }

func enqueued(s int, key string, id int64) Event {
	return Event{At: at(s), Kind: EnqueueConfirmed, Key: key, ID: id}
}

func cancelled(s int, id int64) Event { return Event{At: at(s), Kind: CancelConfirmed, ID: id} }

func started(s int, id, token int64, worker string) Event {
	return Event{At: at(s), Kind: HandlerStart, ID: id, Token: token, Worker: worker}
}

func sinkWrite(s int, id, token int64, applied bool) Event {
	return Event{At: at(s), Kind: SinkWrite, ID: id, Token: token, Worker: "w1", Applied: applied}
}

func succeeded(id, epoch int64) FinalJob {
	return FinalJob{ID: id, State: job.StateSucceeded, Epoch: epoch}
}

// invariants returns the invariant of every violation, in order.
func invariants(r Report) []string {
	var out []string
	for _, v := range r.Violations {
		out = append(out, v.Invariant)
	}
	return out
}

func TestCheckAlarms(t *testing.T) {
	tests := []struct {
		name      string
		events    []Event
		final     []FinalJob
		sink      []SinkRow
		invariant string
		jobID     int64
	}{
		{
			name:      "a confirmed id missing from the final state breaks S1",
			events:    []Event{enqueued(1, "k1", 1), enqueued(2, "k2", 2)},
			final:     []FinalJob{succeeded(1, 1)},
			sink:      []SinkRow{{JobID: 1, Token: 1}},
			invariant: "S1",
			jobID:     2,
		},
		{
			name:      "two ids for one key break S7",
			events:    []Event{enqueued(1, "k", 1), enqueued(2, "k", 2)},
			final:     []FinalJob{succeeded(1, 1), succeeded(2, 1)},
			sink:      []SinkRow{{JobID: 1, Token: 1}, {JobID: 2, Token: 1}},
			invariant: "S7",
			jobID:     1,
		},
		{
			name: "a succeeded job whose sink token is older than its epoch breaks S4",
			events: []Event{
				enqueued(1, "k", 1),
				started(2, 1, 1, "w1"), started(3, 1, 2, "w2"),
				sinkWrite(4, 1, 1, true),
			},
			final:     []FinalJob{succeeded(1, 2)},
			sink:      []SinkRow{{JobID: 1, Token: 1}},
			invariant: "S4",
			jobID:     1,
		},
		{
			name: "an applied sink write that goes backwards breaks S4",
			events: []Event{
				enqueued(1, "k", 1),
				sinkWrite(2, 1, 2, true), sinkWrite(3, 1, 1, true),
			},
			final:     []FinalJob{succeeded(1, 2)},
			sink:      []SinkRow{{JobID: 1, Token: 2}},
			invariant: "S4",
			jobID:     1,
		},
		{
			name:      "a confirmed job left ready breaks L1",
			events:    []Event{enqueued(1, "k", 1)},
			final:     []FinalJob{{ID: 1, State: job.StateReady}},
			invariant: "L1",
			jobID:     1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := Check(tc.events, tc.final, tc.sink)
			if len(r.Violations) != 1 {
				t.Fatalf("want exactly one violation, got %d: %+v", len(r.Violations), r.Violations)
			}
			v := r.Violations[0]
			if v.Invariant != tc.invariant || v.JobID != tc.jobID {
				t.Errorf("violation = %+v, want invariant %s on job %d", v, tc.invariant, tc.jobID)
			}
			if v.Detail == "" {
				t.Error("a violation must say what was found")
			}
		})
	}
}

func TestCheckAcceptsGoodHistory(t *testing.T) {
	// Job 1 is delivered twice: worker w1 loses its lease, w2 finishes the
	// job at token 2, and then w1's zombie write at token 1 is rejected by
	// the sink. Job 2 is a plain run, job 3 is cancelled, job 4 ran out of
	// attempts.
	events := []Event{
		enqueued(1, "k1", 1), enqueued(1, "k2", 2), enqueued(1, "k3", 3), enqueued(1, "k4", 4),
		started(2, 1, 1, "w1"),
		started(3, 1, 2, "w2"),
		sinkWrite(4, 1, 2, true),
		{At: at(5), Kind: HandlerEnd, ID: 1, Token: 2, Worker: "w2", Result: "ok"},
		sinkWrite(6, 1, 1, false),
		{At: at(7), Kind: HandlerEnd, ID: 1, Token: 1, Worker: "w1", Result: "ok"},
		started(8, 2, 1, "w1"),
		sinkWrite(9, 2, 1, true),
		{At: at(10), Kind: HandlerEnd, ID: 2, Token: 1, Worker: "w1", Result: "ok"},
		cancelled(11, 3),
		started(12, 4, 1, "w1"),
		{At: at(13), Kind: HandlerEnd, ID: 4, Token: 1, Worker: "w1", Result: "error"},
	}
	final := []FinalJob{
		succeeded(1, 2), succeeded(2, 1),
		{ID: 3, State: job.StateCancelled, Epoch: 0},
		{ID: 4, State: job.StateDead, Epoch: 1},
	}
	sink := []SinkRow{{JobID: 1, Token: 2}, {JobID: 2, Token: 1}}

	r := Check(events, final, sink)
	if len(r.Violations) != 0 {
		t.Fatalf("a good history must have no violations: %+v", r.Violations)
	}
	if r.DuplicateDeliveries != 1 {
		t.Errorf("DuplicateDeliveries = %d, want 1", r.DuplicateDeliveries)
	}
}

func TestCheckCounts(t *testing.T) {
	events := []Event{
		enqueued(1, "k1", 1), enqueued(1, "k2", 2),
		started(2, 1, 1, "w1"), started(3, 1, 2, "w2"), started(4, 1, 3, "w3"),
		started(5, 2, 1, "w1"),
		sinkWrite(6, 1, 3, true), sinkWrite(7, 1, 2, false),
		cancelled(8, 2),
	}
	r := Check(events, []FinalJob{succeeded(1, 3), {ID: 2, State: job.StateCancelled}}, []SinkRow{{JobID: 1, Token: 3}})
	want := map[Kind]int{EnqueueConfirmed: 2, HandlerStart: 4, SinkWrite: 2, CancelConfirmed: 1}
	if len(r.Counts) != len(want) {
		t.Errorf("Counts = %v, want %v", r.Counts, want)
	}
	for k, n := range want {
		if r.Counts[k] != n {
			t.Errorf("Counts[%s] = %d, want %d", k, r.Counts[k], n)
		}
	}
	if r.DuplicateDeliveries != 2 {
		t.Errorf("DuplicateDeliveries = %d, want 2 (three starts of job 1 and one of job 2)", r.DuplicateDeliveries)
	}
	if len(r.Violations) != 0 {
		t.Errorf("unexpected violations: %+v", r.Violations)
	}
}

func TestCheckSucceededJobWithoutSinkRowBreaksS4(t *testing.T) {
	r := Check([]Event{enqueued(1, "k", 1)}, []FinalJob{succeeded(1, 1)}, nil)
	if got := invariants(r); !slices.Equal(got, []string{"S4"}) {
		t.Fatalf("invariants = %v, want [S4]: %+v", got, r.Violations)
	}
}

func TestCheckSucceededJobNeedsOnlyOneSinkRowAtItsEpoch(t *testing.T) {
	r := Check([]Event{enqueued(1, "k", 1)}, []FinalJob{succeeded(1, 2)}, []SinkRow{{JobID: 1, Token: 1}, {JobID: 1, Token: 2}})
	if len(r.Violations) != 0 {
		t.Fatalf("unexpected violations: %+v", r.Violations)
	}
}

func TestCheckOrdersSinkWritesByTimeNotBySlicePosition(t *testing.T) {
	good := []Event{enqueued(1, "k", 1), sinkWrite(3, 1, 2, true), sinkWrite(2, 1, 1, true)}
	bad := []Event{enqueued(1, "k", 1), sinkWrite(2, 1, 2, true), sinkWrite(3, 1, 1, true)}
	final := []FinalJob{succeeded(1, 2)}
	sink := []SinkRow{{JobID: 1, Token: 2}}

	if r := Check(good, final, sink); len(r.Violations) != 0 {
		t.Errorf("token 1 at 2s then token 2 at 3s is in order, whatever the slice says: %+v", r.Violations)
	}
	if r := Check(bad, final, sink); !slices.Equal(invariants(r), []string{"S4"}) {
		t.Errorf("token 2 at 2s then token 1 at 3s goes backwards, whatever the slice says: %+v", r.Violations)
	}
}

func TestCheckBackwardsWriteIsPerJob(t *testing.T) {
	// A higher token on another job is not "an earlier write for the same job".
	events := []Event{
		enqueued(1, "k1", 1), enqueued(1, "k2", 2),
		sinkWrite(2, 1, 5, true), sinkWrite(3, 2, 1, true),
	}
	r := Check(events, []FinalJob{succeeded(1, 5), succeeded(2, 1)}, []SinkRow{{JobID: 1, Token: 5}, {JobID: 2, Token: 1}})
	if len(r.Violations) != 0 {
		t.Fatalf("unexpected violations: %+v", r.Violations)
	}
}

func TestCheckAnAppliedWriteAtTheSameTokenIsNotBackwards(t *testing.T) {
	// The same delivery may write twice; only a strictly lower token is a
	// step back.
	events := []Event{enqueued(1, "k", 1), sinkWrite(2, 1, 2, true), sinkWrite(3, 1, 2, true)}
	r := Check(events, []FinalJob{succeeded(1, 2)}, []SinkRow{{JobID: 1, Token: 2}})
	if len(r.Violations) != 0 {
		t.Fatalf("unexpected violations: %+v", r.Violations)
	}
}

func TestCheckRejectedLowerTokenWriteIsNotBackwards(t *testing.T) {
	events := []Event{enqueued(1, "k", 1), sinkWrite(2, 1, 3, true), sinkWrite(3, 1, 2, false)}
	r := Check(events, []FinalJob{succeeded(1, 3)}, []SinkRow{{JobID: 1, Token: 3}})
	if len(r.Violations) != 0 {
		t.Fatalf("a rejected write is the sink doing its job: %+v", r.Violations)
	}
}

func TestCheckCancelledJobsAreExemptFromL1(t *testing.T) {
	tests := []struct {
		name   string
		events []Event
		state  job.State
		want   []string
	}{
		{"cancelled after a confirmed cancel", []Event{enqueued(1, "k", 1), cancelled(2, 1)}, job.StateCancelled, nil},
		{"left ready although the cancel was confirmed", []Event{enqueued(1, "k", 1), cancelled(2, 1)}, job.StateReady, nil},
		{"cancelled without any confirmed cancel", []Event{enqueued(1, "k", 1)}, job.StateCancelled, []string{"L1"}},
		{"left leased", []Event{enqueued(1, "k", 1)}, job.StateLeased, []string{"L1"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := Check(tc.events, []FinalJob{{ID: 1, State: tc.state}}, nil)
			if got := invariants(r); !slices.Equal(got, tc.want) {
				t.Errorf("invariants = %v, want %v: %+v", got, tc.want, r.Violations)
			}
		})
	}
}

func TestCheckIgnoresJobsThatWereNeverConfirmed(t *testing.T) {
	// The enqueue acknowledgement of job 9 was lost, so it was never
	// confirmed: its final state is nobody's business but the sink's.
	events := []Event{enqueued(1, "k", 1)}
	final := []FinalJob{succeeded(1, 1), {ID: 9, State: job.StateReady}, {ID: 10, State: job.StateDead}}
	r := Check(events, final, []SinkRow{{JobID: 1, Token: 1}})
	if len(r.Violations) != 0 {
		t.Fatalf("unexpected violations: %+v", r.Violations)
	}
}

func TestCheckConfirmingOneJobTwiceIsNotAViolation(t *testing.T) {
	// A retried enqueue with the same key gets the same id back.
	events := []Event{enqueued(1, "k", 1), enqueued(2, "k", 1)}
	r := Check(events, []FinalJob{succeeded(1, 1)}, []SinkRow{{JobID: 1, Token: 1}})
	if len(r.Violations) != 0 {
		t.Fatalf("unexpected violations: %+v", r.Violations)
	}

	// And a missing id is reported once, however often it was confirmed.
	r = Check(events, nil, nil)
	if got := invariants(r); !slices.Equal(got, []string{"S1"}) {
		t.Fatalf("invariants = %v, want [S1]: %+v", got, r.Violations)
	}
}

func TestCheckEmptyKeyMeansNoKey(t *testing.T) {
	events := []Event{enqueued(1, "", 1), enqueued(2, "", 2)}
	final := []FinalJob{succeeded(1, 1), succeeded(2, 1)}
	r := Check(events, final, []SinkRow{{JobID: 1, Token: 1}, {JobID: 2, Token: 1}})
	if len(r.Violations) != 0 {
		t.Fatalf("unexpected violations: %+v", r.Violations)
	}
}

func TestCheckReportsOneS7ViolationPerKey(t *testing.T) {
	events := []Event{enqueued(1, "k", 1), enqueued(2, "k", 2), enqueued(3, "k", 3), enqueued(4, "k", 2)}
	final := []FinalJob{succeeded(1, 1), succeeded(2, 1), succeeded(3, 1)}
	sink := []SinkRow{{JobID: 1, Token: 1}, {JobID: 2, Token: 1}, {JobID: 3, Token: 1}}
	r := Check(events, final, sink)
	if got := invariants(r); !slices.Equal(got, []string{"S7"}) {
		t.Fatalf("invariants = %v, want [S7]: %+v", got, r.Violations)
	}
	for _, id := range []string{"1", "2", "3"} {
		if !strings.Contains(r.Violations[0].Detail, id) {
			t.Errorf("detail %q does not name id %s", r.Violations[0].Detail, id)
		}
	}
}

func TestCheckReportsEveryRuleThatIsBroken(t *testing.T) {
	events := []Event{enqueued(1, "k", 1), enqueued(2, "k", 2), enqueued(3, "k3", 3), enqueued(4, "k4", 4)}
	final := []FinalJob{{ID: 1, State: job.StateReady}, succeeded(2, 1), succeeded(3, 1)}
	r := Check(events, final, []SinkRow{{JobID: 2, Token: 1}})
	want := []string{"S1", "S7", "S4", "L1"}
	if got := invariants(r); !slices.Equal(got, want) {
		t.Fatalf("invariants = %v, want %v: %+v", got, want, r.Violations)
	}
}

func TestCheckLeavesItsInputsAlone(t *testing.T) {
	events := []Event{enqueued(1, "k", 1), sinkWrite(3, 1, 2, true), sinkWrite(2, 1, 1, true)}
	before := slices.Clone(events)
	Check(events, []FinalJob{succeeded(1, 2)}, []SinkRow{{JobID: 1, Token: 2}})
	if !slices.Equal(events, before) {
		t.Errorf("Check reordered or changed its events: %+v", events)
	}
}

func TestRecorderRoundTrip(t *testing.T) {
	want := []Event{
		{At: time.Date(2026, 3, 4, 5, 6, 7, 123456789, time.UTC), Kind: EnqueueConfirmed, Key: "k \"1\" <&>", ID: 1},
		{At: at(2), Kind: CancelConfirmed, ID: 2},
		{At: at(3), Kind: HandlerStart, ID: 1, Token: 4, Worker: "w-1"},
		{At: at(4), Kind: HandlerEnd, ID: 1, Token: 4, Worker: "w-1", Result: "ok"},
		{At: at(5), Kind: SinkWrite, ID: 1, Token: 4, Worker: "w-1", Applied: true},
		{At: at(6), Kind: SinkWrite, ID: 1, Token: 3, Worker: "w-2"},
	}
	var buf bytes.Buffer
	rec := NewRecorder(&buf)
	for _, e := range want {
		if err := rec.Record(e); err != nil {
			t.Fatalf("Record(%+v): %v", e, err)
		}
	}
	if lines := strings.Count(buf.String(), "\n"); lines != len(want) {
		t.Errorf("the file has %d lines, want one per event (%d)", lines, len(want))
	}

	got, err := ReadAll(&buf)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("read %d events, want %d", len(got), len(want))
	}
	for i := range want {
		if !got[i].At.Equal(want[i].At) {
			t.Errorf("event %d: At = %v, want %v", i, got[i].At, want[i].At)
		}
		got[i].At, want[i].At = time.Time{}, time.Time{}
		if got[i] != want[i] {
			t.Errorf("event %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestRecorderFlushesEveryEvent(t *testing.T) {
	// A process killed after Record returned must not take the event with it,
	// so the bytes have to be in the writer, not in a buffer in front of it.
	var buf bytes.Buffer
	rec := NewRecorder(&buf)
	for i := range 3 {
		if err := rec.Record(enqueued(i, "k", 1)); err != nil {
			t.Fatal(err)
		}
		if lines := strings.Count(buf.String(), "\n"); lines != i+1 {
			t.Fatalf("after %d events the writer holds %d lines", i+1, lines)
		}
	}
}

func TestRecorderIsSafeForConcurrentUse(t *testing.T) {
	const goroutines, perGoroutine = 16, 200
	var buf bytes.Buffer // not safe for concurrent use: only the Recorder's lock protects it
	rec := NewRecorder(&buf)

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			for i := range perGoroutine {
				e := Event{At: at(i), Kind: HandlerStart, ID: int64(i + 1), Token: 1, Worker: fmt.Sprintf("w%d", g)}
				if err := rec.Record(e); err != nil {
					t.Errorf("Record: %v", err)
					return
				}
			}
		})
	}
	wg.Wait()

	got, err := ReadAll(&buf)
	if err != nil {
		t.Fatalf("ReadAll: lines were interleaved or lost: %v", err)
	}
	if len(got) != goroutines*perGoroutine {
		t.Fatalf("read %d events, want %d", len(got), goroutines*perGoroutine)
	}
	seen := make(map[string]int)
	for _, e := range got {
		seen[fmt.Sprintf("%s/%d", e.Worker, e.ID)]++
	}
	if len(seen) != goroutines*perGoroutine {
		t.Errorf("%d distinct (worker, id) pairs, want %d: an event was duplicated or lost", len(seen), goroutines*perGoroutine)
	}
}

func TestRecorderRefusesAnUnknownKind(t *testing.T) {
	var buf bytes.Buffer
	rec := NewRecorder(&buf)
	for _, k := range []Kind{"", "enqueue", "ENQUEUE_CONFIRMED"} {
		if err := rec.Record(Event{At: at(1), Kind: k}); err == nil {
			t.Errorf("Record accepted kind %q", k)
		}
	}
	if buf.Len() != 0 {
		t.Errorf("a refused event left %q in the file", buf.String())
	}
}

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestRecorderReportsWriteErrors(t *testing.T) {
	boom := errors.New("disk full")
	rec := NewRecorder(failingWriter{err: boom})
	err := rec.Record(enqueued(1, "k", 1))
	if !errors.Is(err, boom) {
		t.Fatalf("Record = %v, want an error wrapping %v", err, boom)
	}
	if err := rec.Record(enqueued(2, "k", 1)); !errors.Is(err, boom) {
		t.Errorf("a recorder whose writer failed must keep failing, got %v", err)
	}
}

func TestReadAllConcatenatesFilesInOrder(t *testing.T) {
	var a, b bytes.Buffer
	ra, rb := NewRecorder(&a), NewRecorder(&b)
	for _, e := range []Event{enqueued(1, "a", 1), enqueued(2, "a", 2)} {
		if err := ra.Record(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := rb.Record(enqueued(0, "b", 3)); err != nil {
		t.Fatal(err)
	}

	got, err := ReadAll(&a, &bytes.Buffer{}, &b)
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for _, e := range got {
		ids = append(ids, e.ID)
	}
	if !slices.Equal(ids, []int64{1, 2, 3}) {
		t.Errorf("ids = %v, want [1 2 3]: files are concatenated in argument order", ids)
	}

	if got, err := ReadAll(); err != nil || len(got) != 0 {
		t.Errorf("ReadAll() = %v, %v, want nothing", got, err)
	}
}

func TestReadAllAcceptsALastLineWithoutNewline(t *testing.T) {
	got, err := ReadAll(strings.NewReader(`{"at":"2026-01-01T00:00:00Z","kind":"cancel_confirmed","id":5}`))
	if err != nil || len(got) != 1 || got[0].ID != 5 || got[0].Kind != CancelConfirmed {
		t.Fatalf("ReadAll = %+v, %v", got, err)
	}
}

func TestReadAllRejectsMalformedLine(t *testing.T) {
	const good = `{"at":"2026-01-01T00:00:00Z","kind":"enqueue_confirmed","key":"k","id":1}` + "\n"
	tests := []struct {
		name string
		line string
	}{
		{"not JSON", "this is not json"},
		{"a torn line from a killed process", `{"at":"2026-01-01T00:00:00Z","kind":"enqu`},
		{"a blank line", ""},
		{"JSON that is not an object", "[1,2,3]"},
		{"null", "null"},
		{"an object without a kind", `{"at":"2026-01-01T00:00:00Z","id":1}`},
		{"an unknown kind", `{"at":"2026-01-01T00:00:00Z","kind":"teleport","id":1}`},
		{"a field of the wrong type", `{"at":"2026-01-01T00:00:00Z","kind":"handler_start","id":"seven"}`},
		{"two events on one line", `{"kind":"cancel_confirmed","id":1} {"kind":"cancel_confirmed","id":2}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// The bad line is line 3 of the second of three files.
			files := []io.Reader{
				strings.NewReader(good),
				strings.NewReader(good + good + tc.line + "\n" + good),
				strings.NewReader(good),
			}
			got, err := ReadAll(files...)
			if err == nil {
				t.Fatalf("ReadAll accepted %q and returned %d events", tc.line, len(got))
			}
			for _, want := range []string{"file index 1", "line 3"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not name %q", err, want)
				}
			}
			if got != nil {
				t.Errorf("ReadAll returned %d events together with an error", len(got))
			}
		})
	}
}

type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

func TestReadAllReportsReadErrors(t *testing.T) {
	boom := errors.New("input/output error")
	_, err := ReadAll(strings.NewReader(""), errReader{err: boom})
	if !errors.Is(err, boom) {
		t.Fatalf("ReadAll = %v, want an error wrapping %v", err, boom)
	}
	if !strings.Contains(err.Error(), "file index 1") {
		t.Errorf("error %q does not name the file", err)
	}
}
