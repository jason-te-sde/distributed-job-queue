package job

import (
	"errors"
	"testing"
	"time"
)

func TestStateStringAndParseRoundTrip(t *testing.T) {
	names := map[State]string{
		StateReady: "ready", StateLeased: "leased", StateSucceeded: "succeeded",
		StateDead: "dead", StateCancelled: "cancelled",
	}
	if len(names) != 5 {
		t.Fatal("the table must cover all five states")
	}
	for s, name := range names {
		if got := s.String(); got != name {
			t.Errorf("State(%d).String() = %q, want %q", uint8(s), got, name)
		}
		back, err := ParseState(name)
		if err != nil || back != s {
			t.Errorf("ParseState(%q) = %v, %v; want %v", name, back, err, s)
		}
	}
	for _, bad := range []string{"bogus", "", "READY", "ready ", "state(1)"} {
		if _, err := ParseState(bad); !errors.Is(err, ErrInvalid) {
			t.Errorf("ParseState(%q) = %v, want ErrInvalid", bad, err)
		}
	}
	if got := State(9).String(); got != "state(9)" {
		t.Errorf("State(9).String() = %q", got)
	}
	if got := State(0).String(); got != "state(0)" {
		t.Errorf("the zero State must not look like a real state: %q", got)
	}
}

func TestStateTerminal(t *testing.T) {
	want := map[State]bool{
		StateReady: false, StateLeased: false,
		StateSucceeded: true, StateDead: true, StateCancelled: true,
	}
	for s, terminal := range want {
		if got := s.Terminal(); got != terminal {
			t.Errorf("%v.Terminal() = %v, want %v", s, got, terminal)
		}
	}
	if State(0).Terminal() || State(9).Terminal() {
		t.Error("an undefined state must not be terminal")
	}
}

func TestOutcomeString(t *testing.T) {
	want := map[Outcome]string{
		Applied: "applied", Duplicate: "duplicate", Fenced: "fenced",
		Expired: "expired", NotFound: "not_found", WrongState: "wrong_state",
	}
	for o, name := range want {
		if got := o.String(); got != name {
			t.Errorf("Outcome(%d).String() = %q, want %q", uint8(o), got, name)
		}
	}
	if got := Outcome(0).String(); got != "outcome(0)" {
		t.Errorf("Outcome(0).String() = %q", got)
	}
	if got := Outcome(42).String(); got != "outcome(42)" {
		t.Errorf("Outcome(42).String() = %q", got)
	}
}

// fullJob returns a job with every field set to a distinct non-zero value.
// It uses an unkeyed literal on purpose: adding a field to Job breaks the
// build here, which is the reminder to extend Equal and the table in
// TestEqualNoticesEveryField.
func fullJob() Job {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return Job{
		1, "queue", "tenant", StateLeased, []byte("payload"), at, 2, 3, 4,
		at.Add(time.Minute), "worker", "key", "last error", []byte("result"),
		at.Add(-time.Hour), at.Add(time.Hour),
	}
}

func TestEqualNoticesEveryField(t *testing.T) {
	if !Equal(fullJob(), fullJob()) {
		t.Fatal("two identical jobs are not Equal")
	}
	mutations := map[string]func(*Job){
		"ID":             func(j *Job) { j.ID++ },
		"Queue":          func(j *Job) { j.Queue += "x" },
		"Tenant":         func(j *Job) { j.Tenant += "x" },
		"State":          func(j *Job) { j.State = StateReady },
		"Payload":        func(j *Job) { j.Payload = []byte("other") },
		"RunAt":          func(j *Job) { j.RunAt = j.RunAt.Add(time.Microsecond) },
		"Attempts":       func(j *Job) { j.Attempts++ },
		"MaxAttempts":    func(j *Job) { j.MaxAttempts++ },
		"Epoch":          func(j *Job) { j.Epoch++ },
		"LeaseExpiresAt": func(j *Job) { j.LeaseExpiresAt = j.LeaseExpiresAt.Add(time.Microsecond) },
		"LeasedBy":       func(j *Job) { j.LeasedBy += "x" },
		"IdempotencyKey": func(j *Job) { j.IdempotencyKey += "x" },
		"LastError":      func(j *Job) { j.LastError += "x" },
		"Result":         func(j *Job) { j.Result = []byte("other") },
		"CreatedAt":      func(j *Job) { j.CreatedAt = j.CreatedAt.Add(time.Microsecond) },
		"FinishedAt":     func(j *Job) { j.FinishedAt = j.FinishedAt.Add(time.Microsecond) },
	}
	if len(mutations) != 16 {
		t.Fatalf("the table covers %d fields, Job has 16", len(mutations))
	}
	for field, mutate := range mutations {
		a, b := fullJob(), fullJob()
		mutate(&b)
		if Equal(a, b) || Equal(b, a) {
			t.Errorf("Equal ignores %s", field)
		}
	}
}

func TestEqualTreatsNilAndEmptyBytesAlike(t *testing.T) {
	a, b := fullJob(), fullJob()
	a.Payload, a.Result = nil, nil
	b.Payload, b.Result = []byte{}, []byte{}
	if !Equal(a, b) || !Equal(b, a) {
		t.Fatal("nil and empty byte slices must be Equal")
	}
}

func TestEqualComparesInstantsNotRepresentations(t *testing.T) {
	a, b := fullJob(), fullJob()
	zone := time.FixedZone("plus5", 5*60*60)
	b.RunAt = b.RunAt.In(zone)
	b.CreatedAt = b.CreatedAt.In(zone)
	if !Equal(a, b) {
		t.Fatal("the same instants in another zone must be Equal")
	}
}

func TestNormalizeTimeIsUTCMicroseconds(t *testing.T) {
	zone := time.FixedZone("plus5", 5*60*60)
	in := time.Date(2026, 3, 4, 15, 30, 45, 1500, zone)
	got := NormalizeTime(in)
	if got.Location() != time.UTC {
		t.Errorf("location = %v, want UTC", got.Location())
	}
	want := time.Date(2026, 3, 4, 10, 30, 45, 1000, time.UTC)
	if !got.Equal(want) || got.Nanosecond() != 1000 {
		t.Errorf("got %v (ns=%d), want %v", got, got.Nanosecond(), want)
	}
	if !NormalizeTime(time.Time{}).IsZero() {
		t.Error("the zero time must stay zero so that IsZero keeps meaning \"unset\"")
	}
}

func TestCloneSharesNoMemoryWithTheOriginal(t *testing.T) {
	orig := fullJob()
	clone := orig.Clone()
	if !Equal(orig, clone) {
		t.Fatalf("Clone changed a value: got %+v, want %+v", clone, orig)
	}
	clone.Payload[0] ^= 0xff
	clone.Result[0] ^= 0xff
	if string(orig.Payload) != "payload" || string(orig.Result) != "result" {
		t.Fatalf("writing into the clone's bytes changed the original: payload %q, result %q", orig.Payload, orig.Result)
	}
	if Equal(orig, clone) {
		t.Fatal("the clone's bytes were not independent: the write did not show up in the clone")
	}

	// And the other way round: a write into the original leaves the clone alone.
	clone = orig.Clone()
	orig.Payload[0] ^= 0xff
	orig.Result[0] ^= 0xff
	if string(clone.Payload) != "payload" || string(clone.Result) != "result" {
		t.Fatalf("writing into the original's bytes changed the clone: payload %q, result %q", clone.Payload, clone.Result)
	}
}

func TestCloneKeepsNilAndEmptyBytesAsTheyAre(t *testing.T) {
	j := fullJob()
	j.Payload, j.Result = nil, []byte{}
	c := j.Clone()
	if c.Payload != nil {
		t.Errorf("a nil Payload became %#v", c.Payload)
	}
	if c.Result == nil || len(c.Result) != 0 {
		t.Errorf("an empty Result became %#v", c.Result)
	}
}

func TestCloneCopiesTheOtherFieldsByValue(t *testing.T) {
	j := fullJob()
	c := j.Clone()
	c.ID, c.Queue, c.State, c.Attempts, c.LeasedBy = 99, "other", StateDead, 99, "other"
	c.RunAt = c.RunAt.Add(time.Hour)
	if !Equal(j, fullJob()) {
		t.Fatalf("changing the clone's fields changed the original: %+v", j)
	}
}
