package job

import (
	"slices"
	"strings"
	"testing"
	"time"
)

var (
	matrixNow       = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	matrixTTL       = 30 * time.Second
	matrixToken     = int64(7)
	matrixRetryWait = 5 * time.Second
)

// snapshot returns a copy of j that shares no memory with it, so that a
// later write into j's byte slices shows up as a difference.
func snapshot(j Job) Job {
	j.Payload = slices.Clone(j.Payload)
	j.Result = slices.Clone(j.Result)
	return j
}

// wantShape fails unless j keeps the invariants that tie the lease and
// finish fields to the state.
func wantShape(t *testing.T, j Job) {
	t.Helper()
	leased := j.State == StateLeased
	if leased != !j.LeaseExpiresAt.IsZero() || leased != (j.LeasedBy != "") {
		t.Errorf("state %v with LeaseExpiresAt %v and LeasedBy %q: the lease fields are set exactly when the job is leased",
			j.State, j.LeaseExpiresAt, j.LeasedBy)
	}
	if j.State.Terminal() != !j.FinishedAt.IsZero() {
		t.Errorf("state %v with FinishedAt %v: FinishedAt is set exactly when the job is terminal", j.State, j.FinishedAt)
	}
}

// matrixRow is a start row of the transition matrix.
type matrixRow struct {
	name string
	job  Job
}

// matrixRows returns the nine start rows. Every row carries a payload, and
// the finished ones a result, so that a write into either is visible.
func matrixRows() []matrixRow {
	base := Job{
		ID: 1, Queue: "q", Tenant: "t", Payload: []byte("payload"),
		RunAt: matrixNow, MaxAttempts: 25, Epoch: matrixToken, CreatedAt: matrixNow.Add(-time.Hour),
	}
	with := func(f func(*Job)) Job {
		j := snapshot(base)
		f(&j)
		return j
	}
	leased := func(j *Job, expires time.Time, epoch int64) {
		j.State, j.Attempts, j.Epoch = StateLeased, 1, epoch
		j.LeaseExpiresAt, j.LeasedBy = expires, "w1"
	}
	finished := func(j *Job, state State, epoch int64) {
		j.State, j.Attempts, j.Epoch = state, 1, epoch
		j.FinishedAt = matrixNow.Add(-time.Second)
		if state == StateSucceeded {
			j.Result = []byte("result")
		}
	}
	return []matrixRow{
		{"ready, RunAt <= now", with(func(j *Job) { j.State = StateReady })},
		{"ready, RunAt > now", with(func(j *Job) { j.State, j.RunAt = StateReady, matrixNow.Add(time.Microsecond) })},
		{"leased, token match, live", with(func(j *Job) { leased(j, matrixNow.Add(10*time.Second), matrixToken) })},
		{"leased, token match, expired", with(func(j *Job) { leased(j, matrixNow.Add(-time.Microsecond), matrixToken) })},
		{"leased, token mismatch, live", with(func(j *Job) { leased(j, matrixNow.Add(10*time.Second), 8) })},
		{"succeeded, Epoch == token", with(func(j *Job) { finished(j, StateSucceeded, matrixToken) })},
		{"succeeded, Epoch != token", with(func(j *Job) { finished(j, StateSucceeded, 8) })},
		{"dead", with(func(j *Job) { finished(j, StateDead, matrixToken) })},
		{"cancelled", with(func(j *Job) { finished(j, StateCancelled, matrixToken) })},
	}
}

// matrixOp is one of the eight operations, with fixed arguments.
type matrixOp struct {
	name string
	run  func(Job) (Job, Outcome)
}

func matrixOps() []matrixOp {
	return []matrixOp{
		{"Lease", func(j Job) (Job, Outcome) { return Lease(j, matrixNow, matrixTTL, "w2") }},
		{"Heartbeat", func(j Job) (Job, Outcome) { return Heartbeat(j, matrixToken, matrixNow, matrixTTL) }},
		{"Complete", func(j Job) (Job, Outcome) { return Complete(j, matrixToken, matrixNow, []byte("done")) }},
		{"Fail", func(j Job) (Job, Outcome) { return Fail(j, matrixToken, matrixNow, false, matrixRetryWait, "boom") }},
		{"Release", func(j Job) (Job, Outcome) { return Release(j, matrixToken, matrixNow) }},
		{"Cancel", func(j Job) (Job, Outcome) { return Cancel(j, matrixNow) }},
		{"Reap", func(j Job) (Job, Outcome) { return Reap(j, matrixNow, matrixRetryWait) }},
		{"Requeue", func(j Job) (Job, Outcome) { return Requeue(j, matrixNow) }},
	}
}

// cell is the expected result of one operation on one row: the outcome and
// the state afterwards. A zero state means the job keeps its state.
type cell struct {
	outcome Outcome
	state   State
}

func applied(s State) cell { return cell{outcome: Applied, state: s} }

var (
	fenced     = cell{outcome: Fenced}
	wrongState = cell{outcome: WrongState}
	duplicate  = cell{outcome: Duplicate}
	expired    = cell{outcome: Expired}
)

// matrixWant is the transition table of the design, row by row, in the
// column order Lease, Heartbeat, Complete, Fail, Release, Cancel, Reap,
// Requeue. It is written out in full on purpose: the test must not derive
// it from the code it checks.
var matrixWant = map[string][8]cell{
	"ready, RunAt <= now":          {applied(StateLeased), fenced, fenced, fenced, fenced, applied(StateCancelled), wrongState, wrongState},
	"ready, RunAt > now":           {wrongState, fenced, fenced, fenced, fenced, applied(StateCancelled), wrongState, wrongState},
	"leased, token match, live":    {wrongState, applied(StateLeased), applied(StateSucceeded), applied(StateReady), applied(StateReady), applied(StateCancelled), wrongState, wrongState},
	"leased, token match, expired": {wrongState, expired, applied(StateSucceeded), applied(StateReady), applied(StateReady), applied(StateCancelled), applied(StateReady), wrongState},
	"leased, token mismatch, live": {wrongState, fenced, fenced, fenced, fenced, applied(StateCancelled), wrongState, wrongState},
	"succeeded, Epoch == token":    {wrongState, fenced, duplicate, fenced, fenced, wrongState, wrongState, wrongState},
	"succeeded, Epoch != token":    {wrongState, fenced, fenced, fenced, fenced, wrongState, wrongState, wrongState},
	"dead":                         {wrongState, fenced, fenced, fenced, fenced, wrongState, wrongState, applied(StateReady)},
	"cancelled":                    {wrongState, fenced, fenced, fenced, fenced, duplicate, wrongState, wrongState},
}

func TestTransitionMatrix(t *testing.T) {
	rows, ops := matrixRows(), matrixOps()
	if len(rows) != 9 || len(ops) != 8 || len(matrixWant) != 9 {
		t.Fatalf("the matrix is %d rows by %d operations with %d expectations, want 9 by 8 with 9", len(rows), len(ops), len(matrixWant))
	}
	for _, row := range rows {
		want, ok := matrixWant[row.name]
		if !ok {
			t.Fatalf("no expectation for row %q", row.name)
		}
		for i, op := range ops {
			t.Run(row.name+"/"+op.name, func(t *testing.T) {
				out, outcome := op.run(snapshot(row.job))
				if outcome != want[i].outcome {
					t.Fatalf("outcome = %v, want %v", outcome, want[i].outcome)
				}
				wantState := want[i].state
				if wantState == 0 {
					wantState = row.job.State
				}
				if out.State != wantState {
					t.Fatalf("state = %v, want %v", out.State, wantState)
				}
				wantShape(t, out)
				if out.Epoch < row.job.Epoch {
					t.Errorf("Epoch went from %d down to %d", row.job.Epoch, out.Epoch)
				}
			})
		}
	}
}

func TestNonAppliedReturnsInputUnchanged(t *testing.T) {
	checked := 0
	for _, row := range matrixRows() {
		for i, op := range matrixOps() {
			if matrixWant[row.name][i].outcome == Applied {
				continue
			}
			in := snapshot(row.job)
			out, outcome := op.run(in)
			if outcome == Applied || !Equal(out, in) {
				t.Errorf("%s/%s: outcome %v, out %+v, want %+v unchanged", row.name, op.name, outcome, out, in)
			}
			checked++
		}
	}
	if checked != 57 {
		t.Fatalf("checked %d non-applied cells, want 57: the matrix has 72 cells and 15 of them are applied", checked)
	}
}

func TestTransitionsDoNotMutateInput(t *testing.T) {
	for _, row := range matrixRows() {
		for _, op := range matrixOps() {
			in := row.job
			before := snapshot(in)
			op.run(in)
			if !Equal(in, before) {
				t.Errorf("%s/%s changed its argument: %+v, was %+v", row.name, op.name, in, before)
			}
		}
	}
	// The result handed to Complete is stored as given and must not be
	// written to either.
	result := []byte("result")
	Complete(matrixRows()[2].job, matrixToken, matrixNow, result)
	if string(result) != "result" {
		t.Errorf("Complete wrote into the caller's result: %q", result)
	}
}

func TestTransitionBoundaries(t *testing.T) {
	ready := matrixRows()[0].job // RunAt == matrixNow
	if _, o := Lease(ready, matrixNow, matrixTTL, "w"); o != Applied {
		t.Errorf("Lease at now == RunAt = %v, want applied", o)
	}
	if _, o := Lease(ready, matrixNow.Add(-time.Microsecond), matrixTTL, "w"); o != WrongState {
		t.Errorf("Lease 1us before RunAt = %v, want wrong_state", o)
	}

	live := matrixRows()[2].job // LeaseExpiresAt == matrixNow + 10s
	expires := live.LeaseExpiresAt
	if _, o := Heartbeat(live, matrixToken, expires, matrixTTL); o != Expired {
		t.Errorf("Heartbeat at now == LeaseExpiresAt = %v, want expired", o)
	}
	if _, o := Heartbeat(live, matrixToken, expires.Add(-time.Microsecond), matrixTTL); o != Applied {
		t.Errorf("Heartbeat 1us before expiry = %v, want applied", o)
	}
	if _, o := Reap(live, expires, matrixRetryWait); o != Applied {
		t.Errorf("Reap at now == LeaseExpiresAt = %v, want applied", o)
	}
	if _, o := Reap(live, expires.Add(-time.Microsecond), matrixRetryWait); o != WrongState {
		t.Errorf("Reap 1us before expiry = %v, want wrong_state", o)
	}
	if _, o := Complete(live, matrixToken, expires.Add(time.Hour), nil); o != Applied {
		t.Errorf("Complete long after expiry = %v, want applied: expiry is ignored", o)
	}

	const done, dead = 24 * time.Hour, 336 * time.Hour
	rows := matrixRows()
	for _, tc := range []struct {
		name string
		job  Job
		keep time.Duration
	}{
		{"succeeded", rows[5].job, done},
		{"cancelled", rows[8].job, done},
		{"dead", rows[7].job, dead},
	} {
		at := func(age time.Duration) bool { return Prunable(tc.job, tc.job.FinishedAt.Add(age), done, dead) }
		if at(tc.keep) {
			t.Errorf("%s: Prunable at age == retention = true, want false", tc.name)
		}
		if !at(tc.keep + time.Microsecond) {
			t.Errorf("%s: Prunable 1us past retention = false, want true", tc.name)
		}
		if at(0) || at(tc.keep-time.Microsecond) {
			t.Errorf("%s: Prunable inside retention = true, want false", tc.name)
		}
	}
	for _, row := range []matrixRow{rows[0], rows[2]} {
		if Prunable(row.job, matrixNow.Add(1000*dead), done, dead) {
			t.Errorf("%s: a job that has not finished is never prunable", row.name)
		}
	}
}

func TestLeaseIncrementsEpochAndAttempts(t *testing.T) {
	in := matrixRows()[0].job
	in.Epoch, in.Attempts = 3, 0
	out, o := Lease(in, matrixNow, matrixTTL, "worker-9")
	if o != Applied {
		t.Fatalf("outcome = %v", o)
	}
	if out.Epoch != 4 || out.Attempts != 1 {
		t.Errorf("Epoch %d Attempts %d, want 4 and 1", out.Epoch, out.Attempts)
	}
	if want := matrixNow.Add(matrixTTL); !out.LeaseExpiresAt.Equal(want) {
		t.Errorf("LeaseExpiresAt = %v, want %v", out.LeaseExpiresAt, want)
	}
	if out.LeasedBy != "worker-9" || out.State != StateLeased {
		t.Errorf("LeasedBy %q State %v", out.LeasedBy, out.State)
	}
}

func TestHeartbeatExtendsFromNow(t *testing.T) {
	in := matrixRows()[2].job
	now := matrixNow.Add(4 * time.Second)
	out, o := Heartbeat(in, matrixToken, now, matrixTTL)
	if o != Applied {
		t.Fatalf("outcome = %v", o)
	}
	if want := now.Add(matrixTTL); !out.LeaseExpiresAt.Equal(want) {
		t.Errorf("LeaseExpiresAt = %v, want %v", out.LeaseExpiresAt, want)
	}
	want := in
	want.LeaseExpiresAt = out.LeaseExpiresAt
	if !Equal(out, want) {
		t.Errorf("Heartbeat changed more than LeaseExpiresAt: %+v", out)
	}
}

func TestHeartbeatMayShortenTheLease(t *testing.T) {
	// The lease is set to now+ttl, not extended by ttl: a heartbeat with a
	// shorter ttl moves the expiry earlier. The store, not this function,
	// decides whether that is acceptable.
	in := matrixRows()[2].job // expires in 10s
	out, o := Heartbeat(in, matrixToken, matrixNow, time.Second)
	if o != Applied || !out.LeaseExpiresAt.Equal(matrixNow.Add(time.Second)) {
		t.Fatalf("outcome %v, LeaseExpiresAt %v", o, out.LeaseExpiresAt)
	}
}

func TestCompleteStoresResultAndClearsTheLease(t *testing.T) {
	in := matrixRows()[2].job
	in.LastError = "earlier failure"
	out, o := Complete(in, matrixToken, matrixNow, []byte("done"))
	if o != Applied {
		t.Fatalf("outcome = %v", o)
	}
	want := in
	want.State, want.Result, want.FinishedAt = StateSucceeded, []byte("done"), matrixNow
	want.LeaseExpiresAt, want.LeasedBy = time.Time{}, ""
	if !Equal(out, want) {
		t.Errorf("got %+v, want %+v", out, want)
	}
}

func TestCompleteDoesNotReplaceAnEarlierResult(t *testing.T) {
	succeeded := matrixRows()[5].job // Epoch == token
	out, o := Complete(succeeded, matrixToken, matrixNow, []byte("another result"))
	if o != Duplicate || string(out.Result) != "result" {
		t.Errorf("outcome %v, result %q: a second completion must not replace the first", o, out.Result)
	}
}

func TestFailRetriesWithinBudget(t *testing.T) {
	in := matrixRows()[2].job
	in.Attempts = 24 // one attempt left
	out, o := Fail(in, matrixToken, matrixNow, false, matrixRetryWait, "boom")
	if o != Applied {
		t.Fatalf("outcome = %v", o)
	}
	want := in
	want.State, want.LastError = StateReady, "boom"
	want.RunAt = matrixNow.Add(matrixRetryWait)
	want.LeaseExpiresAt, want.LeasedBy = time.Time{}, ""
	if !Equal(out, want) {
		t.Errorf("got %+v, want %+v", out, want)
	}
}

func TestFailExhaustsBudget(t *testing.T) {
	in := matrixRows()[2].job
	in.Attempts = in.MaxAttempts
	out, o := Fail(in, matrixToken, matrixNow, false, matrixRetryWait, "boom")
	if o != Applied {
		t.Fatalf("outcome = %v", o)
	}
	if out.State != StateDead || !out.FinishedAt.Equal(matrixNow) || out.LastError != "boom" {
		t.Errorf("State %v FinishedAt %v LastError %q, want dead, now and boom", out.State, out.FinishedAt, out.LastError)
	}
	wantShape(t, out)
}

func TestFailPermanentGoesDeadEarly(t *testing.T) {
	in := matrixRows()[2].job
	in.Attempts = 1
	out, o := Fail(in, matrixToken, matrixNow, true, matrixRetryWait, "bad input")
	if o != Applied || out.State != StateDead || !out.FinishedAt.Equal(matrixNow) {
		t.Fatalf("outcome %v State %v FinishedAt %v", o, out.State, out.FinishedAt)
	}
	if out.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1 unchanged", out.Attempts)
	}
	wantShape(t, out)
}

func TestFailStoresATruncatedMessage(t *testing.T) {
	in := matrixRows()[2].job
	out, _ := Fail(in, matrixToken, matrixNow, false, 0, strings.Repeat("é", 3000)+"\x00")
	if out.LastError != TruncateError(strings.Repeat("é", 3000)) || len(out.LastError) > MaxErrorBytes {
		t.Errorf("LastError has %d bytes, want the TruncateError form", len(out.LastError))
	}
}

func TestReleaseRefundsAttempt(t *testing.T) {
	in := matrixRows()[2].job
	in.Attempts = 3
	in.LastError = "earlier failure"
	out, o := Release(in, matrixToken, matrixNow)
	if o != Applied {
		t.Fatalf("outcome = %v", o)
	}
	want := in
	want.State, want.Attempts, want.RunAt = StateReady, 2, matrixNow
	want.LeaseExpiresAt, want.LeasedBy = time.Time{}, ""
	if !Equal(out, want) {
		t.Errorf("got %+v, want %+v", out, want)
	}
}

func TestCancelClearsTheLease(t *testing.T) {
	for _, i := range []int{0, 2} { // ready and leased
		in := matrixRows()[i].job
		out, o := Cancel(in, matrixNow)
		if o != Applied || out.State != StateCancelled || !out.FinishedAt.Equal(matrixNow) {
			t.Fatalf("row %d: outcome %v State %v FinishedAt %v", i, o, out.State, out.FinishedAt)
		}
		wantShape(t, out)
		if out.Epoch != in.Epoch || out.Attempts != in.Attempts {
			t.Errorf("row %d: Epoch %d Attempts %d, want %d and %d unchanged", i, out.Epoch, out.Attempts, in.Epoch, in.Attempts)
		}
	}
}

func TestReapRecordsLeaseExpired(t *testing.T) {
	in := matrixRows()[3].job // leased, expired
	out, o := Reap(in, matrixNow, matrixRetryWait)
	if o != Applied {
		t.Fatalf("outcome = %v", o)
	}
	if out.LastError != "lease expired" || !out.RunAt.Equal(matrixNow.Add(matrixRetryWait)) || out.State != StateReady {
		t.Errorf("LastError %q RunAt %v State %v", out.LastError, out.RunAt, out.State)
	}
	wantShape(t, out)

	in.Attempts = in.MaxAttempts
	out, o = Reap(in, matrixNow, matrixRetryWait)
	if o != Applied || out.State != StateDead || out.LastError != "lease expired" {
		t.Errorf("Reap with the budget spent: outcome %v State %v LastError %q, want dead", o, out.State, out.LastError)
	}
}

func TestRequeueResetsAttemptsKeepsEpochAndError(t *testing.T) {
	in := matrixRows()[7].job
	in.Attempts, in.Epoch, in.LastError, in.Result = 25, 9, "boom", []byte("stale")
	out, o := Requeue(in, matrixNow.Add(time.Hour))
	if o != Applied {
		t.Fatalf("outcome = %v", o)
	}
	want := in
	want.State, want.Attempts, want.RunAt = StateReady, 0, matrixNow.Add(time.Hour)
	want.FinishedAt, want.Result = time.Time{}, nil
	if !Equal(out, want) {
		t.Errorf("got %+v, want %+v", out, want)
	}
	if out.Epoch != 9 || out.LastError != "boom" {
		t.Errorf("Epoch %d LastError %q, want 9 and boom kept", out.Epoch, out.LastError)
	}
	wantShape(t, out)
}

func TestNewSetsTheInitialFields(t *testing.T) {
	s := Spec{Queue: "q", Tenant: "t", Payload: []byte("p"), IdempotencyKey: "k"}
	got := New(s, 42, matrixNow)
	want := Job{
		ID: 42, Queue: "q", Tenant: "t", State: StateReady, Payload: []byte("p"),
		RunAt: matrixNow, MaxAttempts: DefaultMaxAttempts, IdempotencyKey: "k", CreatedAt: matrixNow,
	}
	if !Equal(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
	s.MaxAttempts = 7
	if got := New(s, 1, matrixNow); got.MaxAttempts != 7 {
		t.Errorf("MaxAttempts = %d, want the requested 7", got.MaxAttempts)
	}
}

func TestNewClampsScheduledRunAtToNow(t *testing.T) {
	s := Spec{Queue: "q", Tenant: "t", RunAt: matrixNow.Add(-time.Hour)}
	if got := New(s, 1, matrixNow); !got.RunAt.Equal(matrixNow) {
		t.Errorf("RunAt in the past = %v, want now", got.RunAt)
	}
	s.RunAt = matrixNow.Add(time.Hour)
	if got := New(s, 1, matrixNow); !got.RunAt.Equal(matrixNow.Add(time.Hour)) {
		t.Errorf("RunAt in the future = %v, want it kept", got.RunAt)
	}
	s = Spec{Queue: "q", Tenant: "t", Delay: 5 * time.Second}
	if got := New(s, 1, matrixNow); !got.RunAt.Equal(matrixNow.Add(5 * time.Second)) {
		t.Errorf("Delay 5s: RunAt = %v, want now+5s", got.RunAt)
	}
}

func TestNewNormalizesEveryTime(t *testing.T) {
	zone := time.FixedZone("plus5", 5*60*60)
	now := time.Date(2026, 1, 1, 5, 0, 0, 1500, zone) // 00:00:00.0000015 UTC
	runAt := time.Date(2026, 1, 1, 6, 0, 0, 2500, zone)
	got := New(Spec{Queue: "q", Tenant: "t", RunAt: runAt}, 1, now)
	for name, ts := range map[string]time.Time{"CreatedAt": got.CreatedAt, "RunAt": got.RunAt} {
		if ts.Location() != time.UTC || ts.Nanosecond()%1000 != 0 {
			t.Errorf("%s = %v: not UTC at microsecond precision", name, ts)
		}
	}
	if want := time.Date(2026, 1, 1, 0, 0, 0, 1000, time.UTC); !got.CreatedAt.Equal(want) {
		t.Errorf("CreatedAt = %v, want %v", got.CreatedAt, want)
	}
	if want := time.Date(2026, 1, 1, 1, 0, 0, 2000, time.UTC); !got.RunAt.Equal(want) {
		t.Errorf("RunAt = %v, want %v", got.RunAt, want)
	}
}
