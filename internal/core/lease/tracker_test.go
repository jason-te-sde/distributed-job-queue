package lease

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
)

const (
	ms  = time.Millisecond
	sec = time.Second
)

// testConfig is the configuration of every test unless it says otherwise:
// four slots, a 30s lease with a 3s margin, and a 2ms ack window. The other
// fields take their defaults inside New.
func testConfig() Config {
	return Config{Concurrency: 4, TTL: 30 * sec, Margin: 3 * sec, AckWindow: 2 * ms}
}

// newTracker builds a tracker from testConfig after applying the mutations.
func newTracker(t *testing.T, mutate ...func(*Config)) *Tracker {
	t.Helper()
	cfg := testConfig()
	for _, m := range mutate {
		m(&cfg)
	}
	tr, err := New(cfg, 1)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return tr
}

// ref returns the lease reference of job id. Its token is 100 + id, so a
// failure message names both numbers.
func ref(id int64) Ref { return Ref{ID: id, Queue: "q", Token: 100 + id} }

// grant returns a grant of job id with a full 30s remaining.
func grant(id int64) Grant { return Grant{Ref: ref(id), Attempt: 1, Remaining: 30 * sec} }

func grants(ids ...int64) []Grant {
	out := make([]Grant, len(ids))
	for i, id := range ids {
		out[i] = grant(id)
	}
	return out
}

// all returns the effects of type T in effs, in order.
func all[T Effect](effs []Effect) []T {
	var out []T
	for _, e := range effs {
		if v, ok := e.(T); ok {
			out = append(out, v)
		}
	}
	return out
}

// only returns the one effect of type T in effs and fails if there is none
// or more than one.
func only[T Effect](t *testing.T, effs []Effect) T {
	t.Helper()
	got := all[T](effs)
	if len(got) != 1 {
		t.Fatalf("want exactly one %T in %v, got %d", *new(T), effs, len(got))
	}
	return got[0]
}

// none fails if effs holds an effect of type T.
func none[T Effect](t *testing.T, effs []Effect) {
	t.Helper()
	if got := all[T](effs); len(got) != 0 {
		t.Fatalf("want no %T in %v, got %v", *new(T), effs, got)
	}
}

// wantEffects fails unless got is exactly want. Effects are compared by their
// Go-syntax form, which shows the type name, every field and the contents of
// every slice, so two effects of different types that print alike cannot
// pass for each other.
func wantEffects(t *testing.T, got []Effect, want ...Effect) {
	t.Helper()
	if goSyntax(got) != goSyntax(want) {
		t.Fatalf("effects = %v, want %v", got, want)
	}
}

// goSyntax prints effs in Go syntax, and an empty list the same way whether it
// is nil or not.
func goSyntax(effs []Effect) string {
	if len(effs) == 0 {
		return "[]"
	}
	return fmt.Sprintf("%#v", effs)
}

// armOf returns the instant of the Arm that ends effs and fails if effs does
// not end with one.
func armOf(t *testing.T, effs []Effect) Mono {
	t.Helper()
	if len(effs) == 0 {
		t.Fatalf("no effects, want a trailing Arm")
	}
	a, ok := effs[len(effs)-1].(Arm)
	if !ok {
		t.Fatalf("the last effect of %v is not an Arm", effs)
	}
	return a.At
}

// wantInvariant fails unless err is an *InvariantError about want, with the
// documented message.
func wantInvariant(t *testing.T, err error, want Ref) {
	t.Helper()
	var ie *InvariantError
	if !errors.As(err, &ie) {
		t.Fatalf("error = %v, want an *InvariantError", err)
	}
	if ie.Ref != want {
		t.Fatalf("InvariantError.Ref = %+v, want %+v", ie.Ref, want)
	}
	prefix := fmt.Sprintf("lease tracker invariant broken: job %d token %d: ", want.ID, want.Token)
	if !strings.HasPrefix(err.Error(), prefix) || ie.Msg == "" {
		t.Fatalf("error text %q does not start with %q and carry a message", err.Error(), prefix)
	}
}

// startAndGrant starts tr at instant 0 and answers its first Lease request at
// instant at with grants of the given jobs, as if the request had been sent at
// 0 and had waited 0. It returns the effects of the answer.
func startAndGrant(t *testing.T, tr *Tracker, at Mono, ids ...int64) []Effect {
	t.Helper()
	first := only[RequestLeases](t, tr.Start(0))
	effs, err := tr.LeasesGranted(at, first.Seq, 0, 0, grants(ids...))
	if err != nil {
		t.Fatalf("LeasesGranted: %v", err)
	}
	return effs
}

// startAndGrantInBatches starts tr at instant 0 and answers its Lease
// requests at instant at with the given jobs, as many per response as each
// request allows, which is at most job.MaxBatch.
func startAndGrantInBatches(t *testing.T, tr *Tracker, at Mono, ids ...int64) {
	t.Helper()
	req := only[RequestLeases](t, tr.Start(0))
	for len(ids) > 0 {
		n := min(len(ids), req.Max)
		effs, err := tr.LeasesGranted(at, req.Seq, 0, 0, grants(ids[:n]...))
		if err != nil {
			t.Fatalf("LeasesGranted: %v", err)
		}
		if ids = ids[n:]; len(ids) > 0 {
			req = only[RequestLeases](t, effs)
		}
	}
}

// allHeld answers every ref of hb as held with a full 30s remaining.
func allHeld(hb SendHeartbeat) []HeartbeatStatus {
	out := make([]HeartbeatStatus, len(hb.Refs))
	for i := range out {
		out[i] = HeartbeatStatus{Held: true, Remaining: 30 * sec}
	}
	return out
}

// heartbeatRound ticks at instant at, expects a heartbeat, and answers it as
// all held 10ms later. It returns the effects of the Tick.
func heartbeatRound(t *testing.T, tr *Tracker, at Mono) []Effect {
	t.Helper()
	effs := tr.Tick(at)
	hb := only[SendHeartbeat](t, effs)
	if _, err := tr.HeartbeatDone(at+10*ms, hb.Seq, at, allHeld(hb)); err != nil {
		t.Fatalf("HeartbeatDone: %v", err)
	}
	return effs
}

// heldDeadline returns the local deadline of job id. It reads Held at instant
// 0, so it sees every lease that is not lost or released.
func heldDeadline(t *testing.T, tr *Tracker, id int64) Mono {
	t.Helper()
	for _, h := range tr.Held(0) {
		if h.Ref == ref(id) {
			return h.Deadline
		}
	}
	t.Fatalf("job %d is not held: %v", id, tr.Held(0))
	return 0
}

func heldIDs(tr *Tracker, now Mono) []int64 {
	var ids []int64
	for _, h := range tr.Held(now) {
		ids = append(ids, h.Ref.ID)
	}
	return ids
}

func TestStartRequestsFullConcurrency(t *testing.T) {
	tr := newTracker(t)
	wantEffects(t, tr.Start(0),
		RequestLeases{Seq: 1, Max: 4, Wait: 20 * sec, TTL: 30 * sec},
		Arm{At: 7500 * ms})
}

func TestGrantDeadlineAnchorsAtSendTime(t *testing.T) {
	tr := newTracker(t)
	first := only[RequestLeases](t, tr.Start(0))
	g := Grant{Ref: ref(1), Attempt: 1, Remaining: 30 * sec}
	effs, err := tr.LeasesGranted(3*sec, first.Seq, 100*ms, 2*sec, []Grant{g})
	if err != nil {
		t.Fatalf("LeasesGranted: %v", err)
	}
	if sh := only[StartHandler](t, effs); sh.Grant != g {
		t.Fatalf("StartHandler.Grant = %+v, want %+v", sh.Grant, g)
	}
	// sentAt + waited + Remaining - Margin = 100ms + 2s + 30s - 3s.
	held := tr.Held(3 * sec)
	if len(held) != 1 || held[0].Deadline != 29*sec+100*ms {
		t.Fatalf("Held = %+v, want one lease with deadline 29.1s", held)
	}
}

func TestDeadlineIgnoresResponseLatency(t *testing.T) {
	// The same grant, answered 10s after it was sent. The deadline must not
	// move with the answer's arrival: flaw F5 anchors it at the arrival and
	// makes it 10.1s + 30s - 3s = 37.1s.
	tr := newTracker(t)
	first := only[RequestLeases](t, tr.Start(0))
	if _, err := tr.LeasesGranted(100*ms+10*sec, first.Seq, 100*ms, 2*sec, grants(1)); err != nil {
		t.Fatalf("LeasesGranted: %v", err)
	}
	held := tr.Held(10 * sec)
	if len(held) != 1 || held[0].Deadline != 29*sec+100*ms {
		t.Fatalf("Held = %+v, want one lease with deadline 29.1s", held)
	}
}

func TestHeartbeatEveryQuarterTTL(t *testing.T) {
	tr := newTracker(t)
	startAndGrant(t, tr, 100*ms, 1, 2)
	seen := map[uint64]bool{}
	for i, at := range []Mono{7500 * ms, 15 * sec, 22500 * ms} {
		effs := heartbeatRound(t, tr, at)
		hb := only[SendHeartbeat](t, effs)
		if !slices.Equal(hb.Refs, []Ref{ref(1), ref(2)}) || hb.TTL != 30*sec {
			t.Fatalf("heartbeat %d = %+v, want both refs and a 30s TTL", i, hb)
		}
		if seen[hb.Seq] {
			t.Fatalf("heartbeat %d reuses seq %d", i, hb.Seq)
		}
		seen[hb.Seq] = true
		if got, want := armOf(t, effs), at+7500*ms; got != want {
			t.Fatalf("Arm after the heartbeat at %v = %v, want %v", at, got, want)
		}
	}
}

func TestHeartbeatExtendsDeadlineMonotonically(t *testing.T) {
	tr := newTracker(t)
	startAndGrant(t, tr, 100*ms, 1)
	heartbeatRound(t, tr, 7500*ms) // sent 7.5s, remaining 30s: 7.5s + 30s - 3s
	if got := heldDeadline(t, tr, 1); got != 34500*ms {
		t.Fatalf("deadline = %v, want 34.5s", got)
	}
	// A reply whose heartbeat was sent later but reports less must not shrink it.
	hb := only[SendHeartbeat](t, tr.Tick(15*sec))
	stale := []HeartbeatStatus{{Held: true, Remaining: 5 * sec}}
	if _, err := tr.HeartbeatDone(15*sec+10*ms, hb.Seq, 15*sec, stale); err != nil {
		t.Fatalf("HeartbeatDone: %v", err)
	}
	if got := heldDeadline(t, tr, 1); got != 34500*ms {
		t.Fatalf("deadline after a smaller reply = %v, want 34.5s", got)
	}
}

func TestHeartbeatLostCancelsHandler(t *testing.T) {
	tr := newTracker(t)
	startAndGrant(t, tr, 100*ms, 1, 2)
	hb := only[SendHeartbeat](t, tr.Tick(7500*ms))
	statuses := []HeartbeatStatus{{Held: false}, {Held: true, Remaining: 30 * sec}}
	effs, err := tr.HeartbeatDone(7600*ms, hb.Seq, 7500*ms, statuses)
	if err != nil {
		t.Fatalf("HeartbeatDone: %v", err)
	}
	if got := all[CancelHandler](effs); len(got) != 1 || got[0] != (CancelHandler{Ref: ref(1), Reason: CancelLost}) {
		t.Fatalf("CancelHandler = %v, want one for job 1 with reason lost", got)
	}
	next := only[SendHeartbeat](t, tr.Tick(15*sec))
	if !slices.Equal(next.Refs, []Ref{ref(2)}) {
		t.Fatalf("next heartbeat refs = %v, want only job 2", next.Refs)
	}
	if ids := heldIDs(tr, 8*sec); !slices.Equal(ids, []int64{2}) {
		t.Fatalf("Held = %v, want only job 2", ids)
	}
}

func TestTwoLostHeartbeatsSurvive(t *testing.T) {
	tr := newTracker(t)
	startAndGrant(t, tr, 0, 1) // deadline 27s
	failRound := func(at Mono) {
		hb := only[SendHeartbeat](t, tr.Tick(at))
		retry := only[SendHeartbeat](t, tr.HeartbeatFailed(at, hb.Seq))
		none[SendHeartbeat](t, tr.HeartbeatFailed(at, retry.Seq))
	}
	for at := 500 * ms; at <= 22*sec; at += 500 * ms {
		if at == 7500*ms || at == 15*sec {
			failRound(at)
		} else {
			none[SendHeartbeat](t, tr.Tick(at))
		}
		if ids := heldIDs(tr, at); !slices.Equal(ids, []int64{1}) {
			t.Fatalf("Held(%v) = %v, want job 1", at, ids)
		}
	}
	heartbeatRound(t, tr, 22500*ms)
	if ids := heldIDs(tr, 22500*ms); !slices.Equal(ids, []int64{1}) {
		t.Fatalf("Held(22.5s) = %v, want job 1", ids)
	}
	if got := heldDeadline(t, tr, 1); got != 49500*ms {
		t.Fatalf("deadline = %v, want 22.5s + 30s - 3s = 49.5s", got)
	}
}

func TestHeartbeatFailureRetriesOnceImmediately(t *testing.T) {
	tr := newTracker(t)
	startAndGrant(t, tr, 100*ms, 1, 2)
	hb := only[SendHeartbeat](t, tr.Tick(7500*ms))

	effs := tr.HeartbeatFailed(7600*ms, hb.Seq)
	retry := only[SendHeartbeat](t, effs)
	if retry.Seq == hb.Seq || !slices.Equal(retry.Refs, hb.Refs) || retry.TTL != hb.TTL {
		t.Fatalf("retry = %+v, want the same refs and TTL as %+v under a new seq", retry, hb)
	}
	if got := armOf(t, effs); got != 15*sec {
		t.Fatalf("Arm after the retry = %v, want the next period at 15s", got)
	}
	// A second failure in the same period waits for the next firing.
	none[SendHeartbeat](t, tr.HeartbeatFailed(7700*ms, retry.Seq))
	// The next period gets its own immediate retry.
	next := only[SendHeartbeat](t, tr.Tick(15*sec))
	only[SendHeartbeat](t, tr.HeartbeatFailed(15*sec+50*ms, next.Seq))
}

func TestHeartbeatChunksByAckBatch(t *testing.T) {
	tr := newTracker(t, func(c *Config) { c.Concurrency = 5; c.AckBatch = 2 })
	startAndGrant(t, tr, 100*ms, 1, 2, 3, 4, 5)
	beats := all[SendHeartbeat](tr.Tick(7500 * ms))
	if len(beats) != 3 {
		t.Fatalf("got %d heartbeat chunks, want 3", len(beats))
	}
	wantRefs := [][]Ref{{ref(1), ref(2)}, {ref(3), ref(4)}, {ref(5)}}
	for i, hb := range beats {
		if !slices.Equal(hb.Refs, wantRefs[i]) {
			t.Fatalf("chunk %d refs = %v, want %v", i, hb.Refs, wantRefs[i])
		}
		for _, other := range beats[:i] {
			if other.Seq == hb.Seq {
				t.Fatalf("chunks %v share a seq", beats)
			}
		}
	}
	// Only the failed chunk is re-sent, and only for the first failure.
	retry := only[SendHeartbeat](t, tr.HeartbeatFailed(7600*ms, beats[1].Seq))
	if !slices.Equal(retry.Refs, wantRefs[1]) {
		t.Fatalf("retry refs = %v, want %v", retry.Refs, wantRefs[1])
	}
	none[SendHeartbeat](t, tr.HeartbeatFailed(7600*ms, beats[2].Seq))
}

func TestMissedHeartbeatFiringMovesNextFiring(t *testing.T) {
	tr := newTracker(t)
	startAndGrant(t, tr, 100*ms, 1)
	hb := only[SendHeartbeat](t, tr.Tick(7500*ms)) // never answered in time

	effs := tr.Tick(15 * sec)
	none[SendHeartbeat](t, effs)
	if got := armOf(t, effs); got != 22500*ms {
		t.Fatalf("Arm after a missed firing = %v, want 22.5s", got)
	}
	// The late reply does not bring a heartbeat forward.
	late, err := tr.HeartbeatDone(16*sec, hb.Seq, 7500*ms, allHeld(hb))
	if err != nil {
		t.Fatalf("HeartbeatDone: %v", err)
	}
	none[SendHeartbeat](t, late)
	only[SendHeartbeat](t, tr.Tick(22500*ms))
}

func TestLocalDeadlineCancelsHandler(t *testing.T) {
	tr := newTracker(t)
	startAndGrant(t, tr, 0, 1) // deadline 27s
	only[SendHeartbeat](t, tr.Tick(7500*ms))
	for _, at := range []Mono{15 * sec, 22500 * ms} {
		none[SendHeartbeat](t, tr.Tick(at)) // the first heartbeat never returns
	}
	if ids := heldIDs(tr, 26999*ms); !slices.Equal(ids, []int64{1}) {
		t.Fatalf("Held(26.999s) = %v, want job 1", ids)
	}
	effs := tr.Tick(27 * sec)
	if got := all[CancelHandler](effs); len(got) != 1 || got[0] != (CancelHandler{Ref: ref(1), Reason: CancelDeadline}) {
		t.Fatalf("CancelHandler = %v, want one for job 1 with reason deadline", got)
	}
	if got := tr.Held(27 * sec); len(got) != 0 {
		t.Fatalf("Held(27s) = %+v, want none", got)
	}
	// The lease is lost, so a second Tick does not cancel it again.
	none[CancelHandler](t, tr.Tick(28*sec))
}

func TestHandlerDoneCoalescesAcks(t *testing.T) {
	tr := newTracker(t)
	startAndGrant(t, tr, 100*ms, 1, 2, 3)
	done := func(at Mono, id int64) []Effect {
		effs, err := tr.HandlerDone(at, ref(id), Outcome{Kind: OutcomeSuccess, Result: []byte{byte(id)}})
		if err != nil {
			t.Fatalf("HandlerDone(%d): %v", id, err)
		}
		return effs
	}
	first := done(sec, 1)
	none[SendAcks](t, first)
	if got := armOf(t, first); got != sec+2*ms {
		t.Fatalf("Arm after the first completion = %v, want 1.002s", got)
	}
	none[SendAcks](t, done(sec+500*time.Microsecond, 2))
	none[SendAcks](t, done(sec+ms, 3))
	none[SendAcks](t, tr.Tick(sec+2*ms-time.Nanosecond))

	sa := only[SendAcks](t, tr.Tick(sec+2*ms))
	if len(sa.Acks) != 3 {
		t.Fatalf("got %d acks, want 3: %+v", len(sa.Acks), sa.Acks)
	}
	for i, a := range sa.Acks {
		id := int64(i + 1)
		if a.Ref != ref(id) || a.Attempt != 1 || a.Outcome.Kind != OutcomeSuccess || !slices.Equal(a.Outcome.Result, []byte{byte(id)}) {
			t.Fatalf("ack %d = %+v, want job %d, attempt 1 and its outcome", i, a, id)
		}
	}
}

func TestFullAckBatchFlushesAtOnce(t *testing.T) {
	tr := newTracker(t, func(c *Config) { c.Concurrency = 200 })
	ids := make([]int64, 200)
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	startAndGrantInBatches(t, tr, 100*ms, ids...)
	for _, id := range ids[:127] {
		effs, err := tr.HandlerDone(sec, ref(id), Outcome{Kind: OutcomeSuccess})
		if err != nil {
			t.Fatalf("HandlerDone(%d): %v", id, err)
		}
		none[SendAcks](t, effs)
	}
	effs, err := tr.HandlerDone(sec, ref(128), Outcome{Kind: OutcomeSuccess})
	if err != nil {
		t.Fatalf("HandlerDone(128): %v", err)
	}
	sa := only[SendAcks](t, effs)
	if len(sa.Acks) != 128 || sa.Acks[0].Ref != ref(1) || sa.Acks[127].Ref != ref(128) {
		t.Fatalf("SendAcks has %d acks, want jobs 1..128", len(sa.Acks))
	}
}

func TestAckBatchInFlightHoldsBackTheRest(t *testing.T) {
	tr := newTracker(t, func(c *Config) { c.Concurrency = 200 })
	ids := make([]int64, 200)
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	startAndGrantInBatches(t, tr, 100*ms, ids...)
	var batch SendAcks
	for _, id := range ids {
		effs, err := tr.HandlerDone(sec, ref(id), Outcome{Kind: OutcomeSuccess})
		if err != nil {
			t.Fatalf("HandlerDone(%d): %v", id, err)
		}
		if id == 128 {
			batch = only[SendAcks](t, effs)
		} else {
			none[SendAcks](t, effs)
		}
	}
	// The window passes while the batch is in flight: nothing else goes out.
	none[SendAcks](t, tr.Tick(sec+10*ms))
	rest, err := tr.AcksDone(sec+20*ms, batch.Seq, make([]AckStatus, len(batch.Acks)))
	if err != nil {
		t.Fatalf("AcksDone: %v", err)
	}
	sa := only[SendAcks](t, rest)
	if len(sa.Acks) != 72 || sa.Acks[0].Ref != ref(129) || sa.Acks[71].Ref != ref(200) {
		t.Fatalf("remaining SendAcks has %d acks, want jobs 129..200", len(sa.Acks))
	}
}

func TestAcksDoneFlushesWaitingAcksAtOnce(t *testing.T) {
	tr := newTracker(t, func(c *Config) { c.AckBatch = 2 })
	startAndGrant(t, tr, 100*ms, 1, 2, 3)
	var batch SendAcks
	for _, id := range []int64{1, 2} {
		effs, err := tr.HandlerDone(sec, ref(id), Outcome{Kind: OutcomeSuccess})
		if err != nil {
			t.Fatalf("HandlerDone(%d): %v", id, err)
		}
		if id == 2 {
			batch = only[SendAcks](t, effs)
		}
	}
	// A third ack waits for a batch that is in flight. Its own window of 2ms
	// is not over when the answer comes, and it still goes out at once.
	if _, err := tr.HandlerDone(sec+10*ms, ref(3), Outcome{Kind: OutcomeSuccess}); err != nil {
		t.Fatalf("HandlerDone(3): %v", err)
	}
	effs, err := tr.AcksDone(sec+11*ms, batch.Seq, []AckStatus{AckAccepted, AckAccepted})
	if err != nil {
		t.Fatalf("AcksDone: %v", err)
	}
	if sa := only[SendAcks](t, effs); len(sa.Acks) != 1 || sa.Acks[0].Ref != ref(3) {
		t.Fatalf("SendAcks = %+v, want the ack of job 3", sa)
	}
}

func TestAcksDoneForgetsEveryAckedLease(t *testing.T) {
	tr := newTracker(t)
	startAndGrant(t, tr, 100*ms, 1, 2, 3)
	for id := int64(1); id <= 3; id++ {
		if _, err := tr.HandlerDone(sec, ref(id), Outcome{Kind: OutcomeSuccess}); err != nil {
			t.Fatalf("HandlerDone(%d): %v", id, err)
		}
	}
	sa := only[SendAcks](t, tr.Tick(sec+2*ms))
	statuses := []AckStatus{AckAccepted, AckDuplicate, AckFenced}
	if _, err := tr.AcksDone(sec+10*ms, sa.Seq, statuses); err != nil {
		t.Fatalf("AcksDone: %v", err)
	}
	if got := tr.Held(sec + 10*ms); len(got) != 0 {
		t.Fatalf("Held = %+v, want none after every ack settled", got)
	}
	none[SendHeartbeat](t, tr.Tick(7500*ms))
}

func TestAckingLeaseKeepsHeartbeating(t *testing.T) {
	tr := newTracker(t)
	startAndGrant(t, tr, 100*ms, 1)
	if _, err := tr.HandlerDone(7*sec, ref(1), Outcome{Kind: OutcomeSuccess}); err != nil {
		t.Fatalf("HandlerDone: %v", err)
	}
	only[SendAcks](t, tr.Tick(7*sec+2*ms)) // in flight at 7.5s
	hb := only[SendHeartbeat](t, tr.Tick(7500*ms))
	if !slices.Equal(hb.Refs, []Ref{ref(1)}) {
		t.Fatalf("heartbeat refs = %v, want the acking job 1", hb.Refs)
	}
	if ids := heldIDs(tr, 7500*ms); !slices.Equal(ids, []int64{1}) {
		t.Fatalf("Held = %v, want the acking job 1", ids)
	}
}

func TestAckFailureForALeaseNoLongerHeldRetriesWithBackoffThenDrops(t *testing.T) {
	tr := newTracker(t)
	startAndGrant(t, tr, 100*ms, 1)
	hb := only[SendHeartbeat](t, tr.Tick(7500*ms))
	if _, err := tr.HeartbeatDone(7600*ms, hb.Seq, 7500*ms, []HeartbeatStatus{{Held: false}}); err != nil {
		t.Fatalf("HeartbeatDone: %v", err)
	}
	if _, err := tr.HandlerDone(8*sec, ref(1), Outcome{Kind: OutcomeRetry, Error: "boom", RetryAfter: sec}); err != nil {
		t.Fatalf("HandlerDone: %v", err)
	}
	sent := only[SendAcks](t, tr.Tick(8*sec+2*ms))
	now := 8*sec + 2*ms
	for k, delay := range []time.Duration{100 * ms, 200 * ms, 400 * ms, 800 * ms, 1600 * ms} {
		effs := tr.AcksFailed(now, sent.Seq)
		none[SendAcks](t, effs)
		if got := armOf(t, effs); got != now+delay {
			t.Fatalf("failure %d: Arm = %v, want %v", k+1, got, now+delay)
		}
		none[SendAcks](t, tr.Tick(now+delay-time.Nanosecond))
		now += delay
		resend := only[SendAcks](t, tr.Tick(now))
		if resend.Seq == sent.Seq || fmt.Sprint(resend.Acks) != fmt.Sprint(sent.Acks) {
			t.Fatalf("retry %d = %+v, want the acks of %+v under a new seq", k+1, resend, sent)
		}
		sent = resend
	}
	// The sixth failure exceeds AckRetries = 5, and the broker denied the
	// lease: it is forgotten.
	effs := tr.AcksFailed(now, sent.Seq)
	none[SendAcks](t, effs)
	none[SendAcks](t, tr.Tick(now+10*sec))
	if got := tr.Held(now); len(got) != 0 {
		t.Fatalf("Held = %+v, want the abandoned lease gone", got)
	}
	none[SendHeartbeat](t, tr.Tick(now+20*sec))
}

// An ack whose lease the tracker still holds is worth sending for as long as
// it holds it: given up, the job would run again once the lease expired. The
// lease of job 1 is held until its deadline at 27s (30s granted at 0, less the
// 3s margin), so the batch is sent again past AckRetries, every 2s once the
// doubled wait reaches AckRetryCap, and forgotten at the first failure after
// 27s, even one that arrives before the tick at the deadline.
func TestAckFailureKeepsResendingWhileTheLeaseIsHeld(t *testing.T) {
	tr := newTracker(t)
	startAndGrant(t, tr, 100*ms, 1)
	if _, err := tr.HandlerDone(sec, ref(1), Outcome{Kind: OutcomeSuccess}); err != nil {
		t.Fatalf("HandlerDone: %v", err)
	}
	sent := only[SendAcks](t, tr.Tick(sec+2*ms))
	now := sec + 2*ms
	waits := []time.Duration{100 * ms, 200 * ms, 400 * ms, 800 * ms, 1600 * ms}
	for range 11 { // the resends at 6.102s, 8.102s, ..., 26.102s
		waits = append(waits, 2*sec)
	}
	for k, wait := range waits {
		none[SendAcks](t, tr.AcksFailed(now, sent.Seq))
		none[SendAcks](t, tr.Tick(now+wait-time.Nanosecond))
		now += wait
		resend := only[SendAcks](t, tr.Tick(now))
		if resend.Seq == sent.Seq || fmt.Sprint(resend.Acks) != fmt.Sprint(sent.Acks) {
			t.Fatalf("resend %d at %v = %+v, want the acks of %+v under a new seq", k+1, now, resend, sent)
		}
		sent = resend
	}
	if now != 26*sec+102*ms {
		t.Fatalf("the last resend went out at %v, want 26.102s", now)
	}
	// The resend at 26.102s fails at 27.5s, after the lease's deadline and
	// before any tick: it is forgotten.
	none[SendAcks](t, tr.AcksFailed(27*sec+500*ms, sent.Seq))
	none[SendAcks](t, tr.Tick(40*sec))
	if got := tr.Held(27*sec + 500*ms); len(got) != 0 {
		t.Fatalf("Held = %+v, want the abandoned lease gone", got)
	}
}

// After a drain's deadline the count alone decides, so that the drain ends on
// time although the lease is held until 27s. The drain starts at 1s with a 5s
// timeout: the sixth failure, at 5.102s, is before its deadline and sends the
// batch again; the seventh, at 7.102s, is after it and ends the drain.
func TestAckFailureAfterTheDrainDeadlineDropsAtTheRetryCount(t *testing.T) {
	tr := newTracker(t, func(c *Config) { c.DrainTimeout = 5 * sec })
	startAndGrant(t, tr, 100*ms, 1)
	tr.Drain(sec)
	if _, err := tr.HandlerDone(2*sec, ref(1), Outcome{Kind: OutcomeSuccess}); err != nil {
		t.Fatalf("HandlerDone: %v", err)
	}
	sent := only[SendAcks](t, tr.Tick(2*sec+2*ms))
	now := 2*sec + 2*ms
	for k, wait := range []time.Duration{100 * ms, 200 * ms, 400 * ms, 800 * ms, 1600 * ms, 2 * sec} {
		none[Stopped](t, tr.AcksFailed(now, sent.Seq))
		now += wait
		resend := only[SendAcks](t, tr.Tick(now))
		if resend.Seq == sent.Seq {
			t.Fatalf("resend %d at %v reused seq %d", k+1, now, sent.Seq)
		}
		sent = resend
	}
	if now != 7*sec+102*ms {
		t.Fatalf("the last resend went out at %v, want 7.102s", now)
	}
	wantEffects(t, tr.AcksFailed(now, sent.Seq), Stopped{})
}

func TestRequestsMoreWhenSlotsFree(t *testing.T) {
	tr := newTracker(t)
	effs := startAndGrant(t, tr, 100*ms, 1, 2, 3, 4)
	none[RequestLeases](t, effs)
	done, err := tr.HandlerDone(sec, ref(2), Outcome{Kind: OutcomeSuccess})
	if err != nil {
		t.Fatalf("HandlerDone: %v", err)
	}
	if got := only[RequestLeases](t, done); got != (RequestLeases{Seq: 2, Max: 1, Wait: 20 * sec, TTL: 30 * sec}) {
		t.Fatalf("request = %+v, want Seq 2, Max 1", got)
	}
}

func TestOneRequestIsOutstandingAtATime(t *testing.T) {
	tr := newTracker(t)
	first := only[RequestLeases](t, tr.Start(0))
	effs, err := tr.LeasesGranted(sec, first.Seq, 0, 0, grants(1))
	if err != nil {
		t.Fatalf("LeasesGranted: %v", err)
	}
	if got := only[RequestLeases](t, effs); got.Max != 3 {
		t.Fatalf("request Max = %d, want the 3 free slots", got.Max)
	}
	// A slot frees while that request is outstanding: no second request.
	done, err := tr.HandlerDone(2*sec, ref(1), Outcome{Kind: OutcomeSuccess})
	if err != nil {
		t.Fatalf("HandlerDone: %v", err)
	}
	none[RequestLeases](t, done)
}

func TestEmptyGrantRequestsAgain(t *testing.T) {
	tr := newTracker(t)
	first := only[RequestLeases](t, tr.Start(0))
	effs, err := tr.LeasesGranted(20*sec, first.Seq, 0, 20*sec, nil)
	if err != nil {
		t.Fatalf("LeasesGranted: %v", err)
	}
	if got := only[RequestLeases](t, effs); got != (RequestLeases{Seq: 2, Max: 4, Wait: 20 * sec, TTL: 30 * sec}) {
		t.Fatalf("request = %+v, want Seq 2, Max 4", got)
	}
}

func TestLeaseRequestIsClampedToMaxBatch(t *testing.T) {
	// A Lease call carries payloads, so the broker takes at most MaxBatch
	// jobs in one. The tracker asks for no more, whatever the free slots.
	tr := newTracker(t, func(c *Config) { c.Concurrency = 200 })
	req := only[RequestLeases](t, tr.Start(0))
	if req.Max != job.MaxBatch {
		t.Fatalf("first request Max = %d, want %d with 200 slots free", req.Max, job.MaxBatch)
	}
	// A response with more grants than the clamped Max is a bug, even though
	// slots were free for them.
	over := make([]int64, job.MaxBatch+1)
	for i := range over {
		over[i] = int64(i + 1)
	}
	_, err := tr.LeasesGranted(sec, req.Seq, 0, 0, grants(over...))
	wantInvariant(t, err, ref(job.MaxBatch+1))

	running := 0
	for _, wantMax := range []int{64, 64, 64, 8} { // 200 = 3*64 + 8
		if req.Max != wantMax {
			t.Fatalf("request with %d running: Max = %d, want %d", running, req.Max, wantMax)
		}
		ids := make([]int64, req.Max)
		for i := range ids {
			ids[i] = int64(running + i + 1)
		}
		effs, err := tr.LeasesGranted(sec, req.Seq, 0, 0, grants(ids...))
		if err != nil {
			t.Fatalf("LeasesGranted with %d running: %v", running, err)
		}
		running += len(ids)
		if running == 200 {
			none[RequestLeases](t, effs) // every slot is taken
			break
		}
		req = only[RequestLeases](t, effs)
	}
	// Slots that free up are refilled in requests of at most MaxBatch too.
	for id := int64(1); id <= 100; id++ {
		effs, err := tr.HandlerDone(2*sec, ref(id), Outcome{Kind: OutcomeSuccess})
		if err != nil {
			t.Fatalf("HandlerDone(%d): %v", id, err)
		}
		for _, r := range all[RequestLeases](effs) {
			if r.Max < 1 || r.Max > job.MaxBatch {
				t.Fatalf("request Max = %d after %d handlers returned, want 1..%d", r.Max, id, job.MaxBatch)
			}
		}
	}
}

func TestGrantPastItsDeadlineIsDiscardedAndReleased(t *testing.T) {
	tr := newTracker(t)
	req := only[RequestLeases](t, tr.Start(0))
	// Job 1 was leased for 3s of which the margin takes all, so its local
	// deadline (sentAt + waited + 3s - 3s = 0) is long past on arrival at 5s.
	// Job 2 has a full lease.
	stale := Grant{Ref: ref(1), Attempt: 1, Remaining: 3 * sec}
	effs, err := tr.LeasesGranted(5*sec, req.Seq, 0, 0, []Grant{stale, grant(2)})
	if err != nil {
		t.Fatalf("LeasesGranted: %v", err)
	}
	if got := all[Discard](effs); len(got) != 1 || got[0].Ref != ref(1) {
		t.Fatalf("Discard = %v, want job 1 only", got)
	}
	if got := all[StartHandler](effs); len(got) != 1 || got[0].Grant.Ref != ref(2) {
		t.Fatalf("StartHandler = %v, want job 2 only", got)
	}
	rel := only[SendRelease](t, effs)
	if !slices.Equal(rel.Refs, []Ref{ref(1)}) {
		t.Fatalf("SendRelease refs = %v, want job 1", rel.Refs)
	}
	none[CancelHandler](t, effs)
	// Only job 2 holds a slot, so the next request asks for the other three.
	if got := only[RequestLeases](t, effs); got.Max != 3 {
		t.Fatalf("request Max = %d, want 3", got.Max)
	}
	if ids := heldIDs(tr, 5*sec); !slices.Equal(ids, []int64{2}) {
		t.Fatalf("Held = %v, want only job 2", ids)
	}
	// The discarded lease is never heartbeated, and no handler ever returns
	// for it.
	hb := only[SendHeartbeat](t, tr.Tick(7500*ms))
	if !slices.Equal(hb.Refs, []Ref{ref(2)}) {
		t.Fatalf("heartbeat refs = %v, want only job 2", hb.Refs)
	}
	if _, err := tr.HandlerDone(8*sec, ref(1), Outcome{Kind: OutcomeSuccess}); err == nil {
		t.Fatalf("HandlerDone for a discarded grant returned no error")
	}
}

func TestGrantDeadlineBoundaryIsDiscarded(t *testing.T) {
	// The deadline of a grant sent at 0 with 30s remaining is 27s: a grant
	// that arrives at that instant is discarded, one just before it starts.
	for _, tc := range []struct {
		name  string
		at    Mono
		start bool
	}{
		{"one nanosecond before the deadline", 27*sec - time.Nanosecond, true},
		{"at the deadline", 27 * sec, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := newTracker(t)
			req := only[RequestLeases](t, tr.Start(0))
			effs, err := tr.LeasesGranted(tc.at, req.Seq, 0, 0, grants(1))
			if err != nil {
				t.Fatalf("LeasesGranted: %v", err)
			}
			if got := len(all[StartHandler](effs)) == 1; got != tc.start {
				t.Fatalf("handler started = %v, want %v in %v", got, tc.start, effs)
			}
			if got := len(all[Discard](effs)) == 1; got == tc.start {
				t.Fatalf("grant discarded = %v, want %v in %v", got, !tc.start, effs)
			}
		})
	}
}

// failedLeaseDelays fails k consecutive Lease requests of a tracker with the
// given seed and returns the delay before each next request. The tracker has
// a one hour TTL so that no heartbeat gets in the way of the retry timer.
func failedLeaseDelays(t *testing.T, seed uint64, k int) []time.Duration {
	t.Helper()
	cfg := testConfig()
	cfg.TTL = time.Hour
	tr, err := New(cfg, seed)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	req := only[RequestLeases](t, tr.Start(0))
	now := Mono(0)
	var delays []time.Duration
	for i := 1; i <= k; i++ {
		effs := tr.LeasesFailed(now, req.Seq)
		none[RequestLeases](t, effs)
		at := armOf(t, effs)
		ceiling := min(5*sec, 100*ms<<(i-1))
		if at < now || at >= now+ceiling {
			t.Fatalf("failure %d: retry at %v, want within [%v, %v)", i, at, now, now+ceiling)
		}
		if at > now {
			none[RequestLeases](t, tr.Tick(at-time.Nanosecond))
		}
		req = only[RequestLeases](t, tr.Tick(at))
		if req.Max != 4 {
			t.Fatalf("retry Max = %d, want 4", req.Max)
		}
		delays = append(delays, at-now)
		now = at
	}
	return delays
}

func TestLeaseFailureBacksOffWithJitter(t *testing.T) {
	a := failedLeaseDelays(t, 7, 10)
	if b := failedLeaseDelays(t, 7, 10); !slices.Equal(a, b) {
		t.Fatalf("the same seed gave %v and %v", a, b)
	}
	if c := failedLeaseDelays(t, 8, 10); slices.Equal(a, c) {
		t.Fatalf("seeds 7 and 8 gave the same delays %v", a)
	}
}

func TestLeaseBackoffResetsAfterAResponse(t *testing.T) {
	cfg := testConfig()
	cfg.TTL = time.Hour
	tr, err := New(cfg, 3)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	req := only[RequestLeases](t, tr.Start(0))
	now := Mono(0)
	for range 6 { // the sixth ceiling is 3.2s
		at := armOf(t, tr.LeasesFailed(now, req.Seq))
		now = at
		req = only[RequestLeases](t, tr.Tick(at))
	}
	// An empty response is a response: the next failure starts over at 100ms.
	effs, err := tr.LeasesGranted(now+sec, req.Seq, now, sec, nil)
	if err != nil {
		t.Fatalf("LeasesGranted: %v", err)
	}
	req = only[RequestLeases](t, effs)
	now += sec
	if at := armOf(t, tr.LeasesFailed(now, req.Seq)); at >= now+100*ms {
		t.Fatalf("first failure after a response retries at %v, want below %v", at, now+100*ms)
	}
}

func TestLeasesFailedForAnotherSeqIsIgnored(t *testing.T) {
	tr := newTracker(t)
	req := only[RequestLeases](t, tr.Start(0))
	if effs := tr.LeasesFailed(sec, req.Seq+1); len(effs) != 0 {
		t.Fatalf("effects = %v, want none for a seq that is not outstanding", effs)
	}
	// The real request is still outstanding.
	effs := tr.LeasesFailed(sec, req.Seq)
	none[RequestLeases](t, effs)
	armOf(t, effs)
}

// heartbeatUntil answers every heartbeat from the first period up to and
// including instant until, so that no lease runs out its deadline.
func heartbeatUntil(t *testing.T, tr *Tracker, from, until Mono) {
	t.Helper()
	for at := from; at <= until; at += 7500 * ms {
		heartbeatRound(t, tr, at)
	}
}

func TestDrainWaitsThenReleases(t *testing.T) {
	tr := newTracker(t)
	startAndGrant(t, tr, 100*ms, 1) // request 2 is now outstanding
	heartbeatRound(t, tr, 7500*ms)

	wantEffects(t, tr.Drain(10*sec), AbandonLeaseRequest{Seq: 2}, Arm{At: 15 * sec})
	heartbeatUntil(t, tr, 15*sec, 30*sec) // the drain deadline is 35s

	effs := tr.Tick(35 * sec)
	if got := all[CancelHandler](effs); len(got) != 1 || got[0] != (CancelHandler{Ref: ref(1), Reason: CancelDrain}) {
		t.Fatalf("CancelHandler = %v, want one for job 1 with reason drain", got)
	}
	rel := only[SendRelease](t, effs)
	if !slices.Equal(rel.Refs, []Ref{ref(1)}) {
		t.Fatalf("SendRelease refs = %v, want job 1", rel.Refs)
	}
	none[Stopped](t, effs)
	// The handler never returns. Stopped follows the release alone.
	wantEffects(t, tr.ReleaseDone(35*sec+10*ms, rel.Seq), Stopped{})
	// Nothing is left to do, even if the handler returns late.
	if effs, err := tr.HandlerDone(40*sec, ref(1), Outcome{Kind: OutcomeSuccess}); len(effs) != 0 || err != nil {
		t.Fatalf("HandlerDone after Stopped = %v, %v, want nothing", effs, err)
	}
	if effs := tr.Tick(60 * sec); len(effs) != 0 {
		t.Fatalf("Tick after Stopped = %v, want nothing", effs)
	}
}

func TestDrainWithNothingRunningStopsImmediately(t *testing.T) {
	tr := newTracker(t)
	tr.Start(0)
	wantEffects(t, tr.Drain(sec), AbandonLeaseRequest{Seq: 1}, Stopped{})
	if effs := tr.Drain(2 * sec); len(effs) != 0 {
		t.Fatalf("a second Drain = %v, want nothing", effs)
	}
}

func TestDrainStopsWhenHandlersReturnAndAcksSettle(t *testing.T) {
	tr := newTracker(t)
	startAndGrant(t, tr, 100*ms, 1)
	tr.Drain(sec)
	none[RequestLeases](t, tr.Tick(2*sec))

	done, err := tr.HandlerDone(2*sec, ref(1), Outcome{Kind: OutcomeSuccess})
	if err != nil {
		t.Fatalf("HandlerDone: %v", err)
	}
	none[RequestLeases](t, done) // draining: the freed slot is not refilled
	none[Stopped](t, done)       // the ack is still to be sent
	sa := only[SendAcks](t, tr.Tick(2*sec+2*ms))
	effs, err := tr.AcksDone(2*sec+10*ms, sa.Seq, []AckStatus{AckAccepted})
	if err != nil {
		t.Fatalf("AcksDone: %v", err)
	}
	wantEffects(t, effs, Stopped{})
}

func TestDrainWaitsForAnAckThatIsRetrying(t *testing.T) {
	tr := newTracker(t)
	startAndGrant(t, tr, 100*ms, 1)
	tr.Drain(sec)
	if _, err := tr.HandlerDone(2*sec, ref(1), Outcome{Kind: OutcomeSuccess}); err != nil {
		t.Fatalf("HandlerDone: %v", err)
	}
	sa := only[SendAcks](t, tr.Tick(2*sec+2*ms))
	effs := tr.AcksFailed(2*sec+10*ms, sa.Seq)
	none[Stopped](t, effs)
	retry := only[SendAcks](t, tr.Tick(2*sec+110*ms))
	done, err := tr.AcksDone(2*sec+120*ms, retry.Seq, []AckStatus{AckAccepted})
	if err != nil {
		t.Fatalf("AcksDone: %v", err)
	}
	wantEffects(t, done, Stopped{})
}

func TestLateGrantIsDiscardedAndReleased(t *testing.T) {
	tr := newTracker(t)
	startAndGrant(t, tr, 100*ms, 1) // job 1 runs; request 2 is outstanding
	wantEffects(t, tr.Drain(5*sec), AbandonLeaseRequest{Seq: 2}, Arm{At: 7500 * ms})

	effs, err := tr.LeasesGranted(6*sec, 2, 100*ms, 0, grants(2, 3))
	if err != nil {
		t.Fatalf("LeasesGranted: %v", err)
	}
	if got := all[Discard](effs); len(got) != 2 || got[0].Ref != ref(2) || got[1].Ref != ref(3) {
		t.Fatalf("Discard = %v, want jobs 2 and 3", got)
	}
	none[StartHandler](t, effs)
	rel := only[SendRelease](t, effs)
	if !slices.Equal(rel.Refs, []Ref{ref(2), ref(3)}) {
		t.Fatalf("SendRelease refs = %v, want jobs 2 and 3", rel.Refs)
	}
	// Job 1 still runs, so the release alone does not stop the tracker.
	none[Stopped](t, tr.ReleaseDone(6*sec+10*ms, rel.Seq))
}

func TestLateGrantsBlockStopUntilReleased(t *testing.T) {
	tr := newTracker(t)
	startAndGrant(t, tr, 100*ms, 1) // request 2 is outstanding
	tr.Drain(sec)
	if _, err := tr.HandlerDone(2*sec, ref(1), Outcome{Kind: OutcomeSuccess}); err != nil {
		t.Fatalf("HandlerDone: %v", err)
	}
	sa := only[SendAcks](t, tr.Tick(2*sec+2*ms))
	// The abandoned request answers anyway, while an ack is in flight.
	effs, err := tr.LeasesGranted(2*sec+10*ms, 2, 100*ms, 0, grants(2))
	if err != nil {
		t.Fatalf("LeasesGranted: %v", err)
	}
	rel := only[SendRelease](t, effs)
	// The ack settles first, but the release is still in flight.
	none[Stopped](t, mustAcks(t, tr, 2*sec+20*ms, sa))
	wantEffects(t, tr.ReleaseDone(2*sec+30*ms, rel.Seq), Stopped{})
}

func mustAcks(t *testing.T, tr *Tracker, now Mono, sa SendAcks) []Effect {
	t.Helper()
	effs, err := tr.AcksDone(now, sa.Seq, make([]AckStatus, len(sa.Acks)))
	if err != nil {
		t.Fatalf("AcksDone: %v", err)
	}
	return effs
}

func TestStaleSeqGrantIsDiscardedAndReleased(t *testing.T) {
	tr := newTracker(t)
	first := only[RequestLeases](t, tr.Start(0))
	effs, err := tr.LeasesGranted(sec, first.Seq+40, 0, 0, grants(5))
	if err != nil {
		t.Fatalf("LeasesGranted: %v", err)
	}
	if got := all[Discard](effs); len(got) != 1 || got[0].Ref != ref(5) {
		t.Fatalf("Discard = %v, want job 5", got)
	}
	if rel := only[SendRelease](t, effs); !slices.Equal(rel.Refs, []Ref{ref(5)}) {
		t.Fatalf("SendRelease refs = %v, want job 5", rel.Refs)
	}
	none[RequestLeases](t, effs) // the real request is still outstanding
	// The real answer is still accepted.
	real, err := tr.LeasesGranted(2*sec, first.Seq, 0, 0, grants(6))
	if err != nil {
		t.Fatalf("LeasesGranted: %v", err)
	}
	only[StartHandler](t, real)
}

func TestHandlerDoneAfterReleaseIsDropped(t *testing.T) {
	tr := newTracker(t)
	startAndGrant(t, tr, 100*ms, 1)
	heartbeatUntil(t, tr, 7500*ms, 30*sec)
	tr.Drain(10 * sec) // the drain deadline is 35s
	rel := only[SendRelease](t, tr.Tick(35*sec))

	effs, err := tr.HandlerDone(36*sec, ref(1), Outcome{Kind: OutcomeSuccess})
	if err != nil {
		t.Fatalf("HandlerDone after release: %v", err)
	}
	none[SendAcks](t, effs)
	none[SendAcks](t, tr.Tick(37*sec))
	if got := tr.Held(36 * sec); len(got) != 0 {
		t.Fatalf("Held = %+v, want none", got)
	}
	wantEffects(t, tr.ReleaseDone(37*sec, rel.Seq), Stopped{})
}

func TestLostLeaseStillAcksWhenHandlerReturns(t *testing.T) {
	tr := newTracker(t)
	startAndGrant(t, tr, 100*ms, 1)
	hb := only[SendHeartbeat](t, tr.Tick(7500*ms))
	if _, err := tr.HeartbeatDone(7600*ms, hb.Seq, 7500*ms, []HeartbeatStatus{{Held: false}}); err != nil {
		t.Fatalf("HeartbeatDone: %v", err)
	}
	if _, err := tr.HandlerDone(8*sec, ref(1), Outcome{Kind: OutcomeSuccess}); err != nil {
		t.Fatalf("HandlerDone: %v", err)
	}
	sa := only[SendAcks](t, tr.Tick(8*sec+2*ms))
	if len(sa.Acks) != 1 || sa.Acks[0].Ref != ref(1) {
		t.Fatalf("SendAcks = %+v, want the ack of the lost job 1", sa)
	}
	// A lost lease is not heartbeated again, acking or not.
	none[SendHeartbeat](t, tr.Tick(15*sec))
}

func TestAckingLeaseThatIsLostKeepsItsAck(t *testing.T) {
	tr := newTracker(t)
	startAndGrant(t, tr, 100*ms, 1)
	if _, err := tr.HandlerDone(7*sec, ref(1), Outcome{Kind: OutcomeSuccess}); err != nil {
		t.Fatalf("HandlerDone: %v", err)
	}
	sa := only[SendAcks](t, tr.Tick(7*sec+2*ms))
	hb := only[SendHeartbeat](t, tr.Tick(7500*ms))
	effs, err := tr.HeartbeatDone(7600*ms, hb.Seq, 7500*ms, []HeartbeatStatus{{Held: false}})
	if err != nil {
		t.Fatalf("HeartbeatDone: %v", err)
	}
	none[CancelHandler](t, effs) // the handler already returned
	if got := tr.Held(7600 * ms); len(got) != 0 {
		t.Fatalf("Held = %+v, want the lost lease gone", got)
	}
	// The ack in flight is untouched, and its answer forgets the lease.
	if _, err := tr.AcksDone(8*sec, sa.Seq, []AckStatus{AckFenced}); err != nil {
		t.Fatalf("AcksDone: %v", err)
	}
}

func TestRedeliveredJobIsAcceptedWhileTheOldLeaseIsLost(t *testing.T) {
	tr := newTracker(t)
	startAndGrant(t, tr, 100*ms, 1) // request 2 is outstanding
	hb := only[SendHeartbeat](t, tr.Tick(7500*ms))
	if _, err := tr.HeartbeatDone(7600*ms, hb.Seq, 7500*ms, []HeartbeatStatus{{Held: false}}); err != nil {
		t.Fatalf("HeartbeatDone: %v", err)
	}
	// The lease of job 1 is lost but its handler still runs. The broker may
	// hand the same job to this worker again under a new token.
	again := Grant{Ref: Ref{ID: 1, Queue: "q", Token: 201}, Attempt: 2, Remaining: 30 * sec}
	effs, err := tr.LeasesGranted(8*sec, 2, 7*sec, 0, []Grant{again})
	if err != nil {
		t.Fatalf("LeasesGranted for a redelivered job: %v", err)
	}
	if sh := only[StartHandler](t, effs); sh.Grant != again {
		t.Fatalf("StartHandler = %+v, want %+v", sh, again)
	}
	// Both leases hold a slot until their own handler returns.
	req := only[RequestLeases](t, effs)
	if req.Max != 2 {
		t.Fatalf("request Max = %d, want 2 free slots of 4", req.Max)
	}
	// The new lease is live, so a third grant of job 1 is a bug.
	_, err = tr.LeasesGranted(9*sec, req.Seq, 8*sec, 0, []Grant{{Ref: Ref{ID: 1, Queue: "q", Token: 301}, Attempt: 3, Remaining: 30 * sec}})
	wantInvariant(t, err, Ref{ID: 1, Queue: "q", Token: 301})
}

func TestRedeliveredJobIsAcceptedWhileTheOldAckIsPending(t *testing.T) {
	again := Grant{Ref: Ref{ID: 1, Queue: "q", Token: 201}, Attempt: 2, Remaining: 30 * sec}
	oldRef := ref(1)

	// finish returns the handler of job 1 with a retry that asks for 10ms.
	finish := func(t *testing.T, tr *Tracker, at Mono) {
		t.Helper()
		out := Outcome{Kind: OutcomeRetry, Error: "boom", RetryAfter: 10 * ms}
		if _, err := tr.HandlerDone(at, oldRef, out); err != nil {
			t.Fatalf("HandlerDone: %v", err)
		}
	}
	// redeliver answers the outstanding request 2 (sent at 100ms) with the
	// same job under a new token and checks that it starts.
	redeliver := func(t *testing.T, tr *Tracker, at Mono) []Effect {
		t.Helper()
		effs, err := tr.LeasesGranted(at, 2, 100*ms, 0, []Grant{again})
		if err != nil {
			t.Fatalf("LeasesGranted for a redelivered job: %v", err)
		}
		if sh := only[StartHandler](t, effs); sh.Grant != again {
			t.Fatalf("StartHandler = %+v, want %+v", sh, again)
		}
		none[Discard](t, effs)
		// The old handler returned, so only the new lease holds a slot, and
		// the answered request is replaced by a new one.
		if req := only[RequestLeases](t, effs); req.Max != 3 {
			t.Fatalf("request Max = %d, want 3 free slots", req.Max)
		}
		return effs
	}
	heldRefs := func(tr *Tracker, now Mono) []Ref {
		var refs []Ref
		for _, h := range tr.Held(now) {
			refs = append(refs, h.Ref)
		}
		return refs
	}

	t.Run("ack waits for a resend", func(t *testing.T) {
		// The retry reached the broker, which applied it and offered the job
		// again, but the ack's reply was lost: a resend is pending while the
		// long poll hands the job back under token 201.
		tr := newTracker(t)
		startAndGrant(t, tr, 100*ms, 1)
		finish(t, tr, sec)
		sa := only[SendAcks](t, tr.Tick(sec+2*ms))
		none[SendAcks](t, tr.AcksFailed(sec+10*ms, sa.Seq))
		redeliver(t, tr, sec+50*ms)
		if got := heldRefs(tr, sec+50*ms); !slices.Equal(got, []Ref{oldRef, again.Ref}) {
			t.Fatalf("Held = %v, want the old lease and the new one", got)
		}
		// The resend settles the old lease and leaves the new one alone.
		resend := only[SendAcks](t, tr.Tick(sec+110*ms))
		if _, err := tr.AcksDone(sec+120*ms, resend.Seq, []AckStatus{AckDuplicate}); err != nil {
			t.Fatalf("AcksDone: %v", err)
		}
		if got := heldRefs(tr, sec+120*ms); !slices.Equal(got, []Ref{again.Ref}) {
			t.Fatalf("Held after the ack settled = %v, want only the new lease", got)
		}
		hb := only[SendHeartbeat](t, tr.Tick(7500*ms))
		if !slices.Equal(hb.Refs, []Ref{again.Ref}) {
			t.Fatalf("heartbeat refs = %v, want only the new lease", hb.Refs)
		}
	})

	t.Run("old lease is denied by the next heartbeat", func(t *testing.T) {
		tr := newTracker(t)
		startAndGrant(t, tr, 100*ms, 1)
		finish(t, tr, sec)
		sa := only[SendAcks](t, tr.Tick(sec+2*ms))
		none[SendAcks](t, tr.AcksFailed(sec+10*ms, sa.Seq))
		redeliver(t, tr, sec+50*ms)
		// Both leases are heartbeated. The broker fences the old token.
		effs := tr.Tick(7500 * ms)
		hb := only[SendHeartbeat](t, effs)
		if !slices.Equal(hb.Refs, []Ref{oldRef, again.Ref}) {
			t.Fatalf("heartbeat refs = %v, want both leases", hb.Refs)
		}
		statuses := []HeartbeatStatus{{Held: false}, {Held: true, Remaining: 30 * sec}}
		done, err := tr.HeartbeatDone(7600*ms, hb.Seq, 7500*ms, statuses)
		if err != nil {
			t.Fatalf("HeartbeatDone: %v", err)
		}
		none[CancelHandler](t, done) // the old handler returned long ago
		if got := heldRefs(tr, 7600*ms); !slices.Equal(got, []Ref{again.Ref}) {
			t.Fatalf("Held = %v, want only the new lease", got)
		}
		// The old ack still goes out and settles by itself.
		resend := only[SendAcks](t, effs)
		if _, err := tr.AcksDone(7700*ms, resend.Seq, []AckStatus{AckFenced}); err != nil {
			t.Fatalf("AcksDone: %v", err)
		}
		if got := heldRefs(tr, 7700*ms); !slices.Equal(got, []Ref{again.Ref}) {
			t.Fatalf("Held after the ack settled = %v, want only the new lease", got)
		}
	})

	t.Run("ack still in the pending list", func(t *testing.T) {
		tr := newTracker(t)
		startAndGrant(t, tr, 100*ms, 1)
		finish(t, tr, sec)
		redeliver(t, tr, sec+500*time.Microsecond) // inside the 2ms ack window
		if got := heldRefs(tr, sec+ms); !slices.Equal(got, []Ref{oldRef, again.Ref}) {
			t.Fatalf("Held = %v, want the old lease and the new one", got)
		}
	})
}

func TestGrantsAfterStoppedAreDiscardedAndReleased(t *testing.T) {
	tr := newTracker(t)
	tr.Start(0)
	wantEffects(t, tr.Drain(sec), AbandonLeaseRequest{Seq: 1}, Stopped{})
	// The abandoned request answers after all. No handler may start, and no
	// lease may be left to run out: the answer's grants are released, with no
	// Arm because the tracker is done.
	effs, err := tr.LeasesGranted(2*sec, 1, 0, 0, grants(1, 2))
	if err != nil {
		t.Fatalf("LeasesGranted after Stopped: %v", err)
	}
	rel := only[SendRelease](t, effs)
	wantEffects(t, effs, Discard{Ref: ref(1)}, Discard{Ref: ref(2)}, SendRelease{Seq: rel.Seq, Refs: []Ref{ref(1), ref(2)}})
	if effs := tr.ReleaseDone(2*sec+10*ms, rel.Seq); len(effs) != 0 {
		t.Fatalf("ReleaseDone after Stopped = %v, want nothing", effs)
	}
	// Everything else stays silent.
	if effs := tr.Tick(3 * sec); len(effs) != 0 {
		t.Fatalf("Tick after Stopped = %v, want nothing", effs)
	}
	if effs, err := tr.LeasesGranted(3*sec, 1, 0, 0, nil); len(effs) != 0 || err != nil {
		t.Fatalf("an empty answer after Stopped = %v, %v, want nothing", effs, err)
	}
}

func TestSmallestLegalTTLSurvivesTwoLostHeartbeats(t *testing.T) {
	// TTL 12.001s is the shortest that is valid with a 3s margin. Its period
	// is 3.00025s and the deadline is 12.001s - 3s = 9.001s, just after the
	// third heartbeat at 9.00075s. A TTL of exactly four margins would put
	// the third heartbeat at the very instant the sweep cancels the lease.
	tr := newTracker(t, func(c *Config) { c.TTL = 12001 * ms })
	first := only[RequestLeases](t, tr.Start(0))
	g := Grant{Ref: ref(1), Attempt: 1, Remaining: 12001 * ms}
	if _, err := tr.LeasesGranted(0, first.Seq, 0, 0, []Grant{g}); err != nil {
		t.Fatalf("LeasesGranted: %v", err)
	}
	period := 12001 * ms / 4
	for k := Mono(1); k <= 2; k++ {
		hb := only[SendHeartbeat](t, tr.Tick(k*period))
		retry := only[SendHeartbeat](t, tr.HeartbeatFailed(k*period, hb.Seq))
		none[SendHeartbeat](t, tr.HeartbeatFailed(k*period, retry.Seq))
	}
	effs := tr.Tick(3 * period)
	none[CancelHandler](t, effs)
	hb := only[SendHeartbeat](t, effs)
	if !slices.Equal(hb.Refs, []Ref{ref(1)}) {
		t.Fatalf("third heartbeat refs = %v, want job 1 still held", hb.Refs)
	}
}

func TestHeartbeatCallsAreBoundedByOnePeriod(t *testing.T) {
	// A heartbeat in flight suppresses every later one, so a call that the
	// shell lets hang for longer than a period costs the lease more than the
	// one heartbeat it replaces. Each call, the retry of a failed one
	// included, carries the period as the longest the shell may wait.
	for name, ttl := range map[string]time.Duration{"the smallest legal TTL": 12001 * ms, "the default TTL": 30 * sec} {
		t.Run(name, func(t *testing.T) {
			tr := newTracker(t, func(c *Config) { c.TTL = ttl })
			first := only[RequestLeases](t, tr.Start(0))
			if _, err := tr.LeasesGranted(0, first.Seq, 0, 0, []Grant{{Ref: ref(1), Attempt: 1, Remaining: ttl}}); err != nil {
				t.Fatalf("LeasesGranted: %v", err)
			}
			period := ttl / 4
			hb := only[SendHeartbeat](t, tr.Tick(period))
			if hb.Timeout != period {
				t.Errorf("heartbeat Timeout = %v, want the period %v", hb.Timeout, period)
			}
			retry := only[SendHeartbeat](t, tr.HeartbeatFailed(period+ms, hb.Seq))
			if retry.Timeout != period {
				t.Errorf("retried heartbeat Timeout = %v, want the period %v", retry.Timeout, period)
			}
		})
	}
}

func TestInvariantErrors(t *testing.T) {
	tr := newTracker(t)
	first := only[RequestLeases](t, tr.Start(0))

	// More grants than the request asked for. The error names the first one
	// beyond Max, and the call changes nothing.
	_, err := tr.LeasesGranted(sec, first.Seq, 0, 0, grants(1, 2, 3, 4, 5))
	wantInvariant(t, err, ref(5))
	_, err = tr.LeasesGranted(sec, first.Seq, 0, 0, grants(7, 7))
	wantInvariant(t, err, ref(7))
	effs, err := tr.LeasesGranted(sec, first.Seq, 0, 0, grants(1))
	if err != nil {
		t.Fatalf("a valid response after rejected ones: %v", err)
	}
	req := only[RequestLeases](t, effs)

	// A grant of a job that is already held.
	_, err = tr.LeasesGranted(2*sec, req.Seq, sec, 0, grants(1))
	wantInvariant(t, err, ref(1))

	// A handler that was never started, and one that returns twice.
	_, err = tr.HandlerDone(3*sec, ref(42), Outcome{Kind: OutcomeSuccess})
	wantInvariant(t, err, ref(42))
	if _, err = tr.HandlerDone(3*sec, ref(1), Outcome{Kind: OutcomeSuccess}); err != nil {
		t.Fatalf("HandlerDone: %v", err)
	}
	_, err = tr.HandlerDone(3*sec, ref(1), Outcome{Kind: OutcomeSuccess})
	wantInvariant(t, err, ref(1))

	// A reply whose statuses do not match what was sent.
	effs = tr.Tick(7500 * ms) // the window of the ack at 3s is long over
	hb := only[SendHeartbeat](t, effs)
	sa := only[SendAcks](t, effs)
	_, err = tr.HeartbeatDone(8*sec, hb.Seq, 7500*ms, []HeartbeatStatus{{Held: true}, {Held: true}})
	wantInvariant(t, err, ref(1))
	_, err = tr.AcksDone(8*sec, sa.Seq, nil)
	wantInvariant(t, err, ref(1))
}

func TestRepliesForUnknownSeqs(t *testing.T) {
	tr := newTracker(t)
	startAndGrant(t, tr, 100*ms, 1)
	// Heartbeat and ack replies are never abandoned, so a seq that is not in
	// flight is a bug in the shell. The other calls cannot report it and
	// ignore the stale input.
	if _, err := tr.HeartbeatDone(sec, 999, 0, nil); err == nil {
		t.Fatalf("HeartbeatDone for an unknown seq returned no error")
	} else if !errors.As(err, new(*InvariantError)) {
		t.Fatalf("HeartbeatDone error = %v, want an *InvariantError", err)
	}
	if _, err := tr.AcksDone(sec, 999, nil); !errors.As(err, new(*InvariantError)) {
		t.Fatalf("AcksDone error = %v, want an *InvariantError", err)
	}
	if effs := tr.HeartbeatFailed(sec, 999); len(effs) != 0 {
		t.Fatalf("HeartbeatFailed = %v, want nothing", effs)
	}
	if effs := tr.AcksFailed(sec, 999); len(effs) != 0 {
		t.Fatalf("AcksFailed = %v, want nothing", effs)
	}
	if effs := tr.ReleaseDone(sec, 999); len(effs) != 0 {
		t.Fatalf("ReleaseDone = %v, want nothing", effs)
	}
}

func TestEnumNames(t *testing.T) {
	for _, tc := range []struct{ got, want string }{
		{OutcomeSuccess.String(), "success"},
		{OutcomeRetry.String(), "retry"},
		{OutcomePermanent.String(), "permanent"},
		{OutcomeKind(0).String(), "outcome_kind(0)"},
		{AckAccepted.String(), "accepted"},
		{AckDuplicate.String(), "duplicate"},
		{AckFenced.String(), "fenced"},
		{AckStatus(9).String(), "ack_status(9)"},
		{CancelLost.String(), "lost"},
		{CancelDeadline.String(), "deadline"},
		{CancelDrain.String(), "drain"},
		{CancelReason(0).String(), "cancel_reason(0)"},
	} {
		if tc.got != tc.want {
			t.Errorf("String() = %q, want %q", tc.got, tc.want)
		}
	}
}

func TestConfigWithDefaults(t *testing.T) {
	got := Config{}.WithDefaults()
	want := Config{
		Concurrency:    1,
		TTL:            30 * sec,
		Margin:         3 * sec,
		LongPoll:       20 * sec,
		AckWindow:      2 * ms,
		AckBatch:       128,
		DrainTimeout:   25 * sec,
		AckRetries:     5,
		AckRetryBase:   100 * ms,
		AckRetryCap:    2 * sec,
		LeaseRetryBase: 100 * ms,
		LeaseRetryCap:  5 * sec,
	}
	if got != want {
		t.Fatalf("Config{}.WithDefaults() = %+v, want %+v", got, want)
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("the defaults do not validate: %v", err)
	}
	set := Config{Concurrency: 9, TTL: time.Minute, Margin: sec, LongPoll: 5 * sec, AckBatch: 7}
	kept := set.WithDefaults()
	if kept.Concurrency != 9 || kept.TTL != time.Minute || kept.Margin != sec || kept.LongPoll != 5*sec || kept.AckBatch != 7 {
		t.Fatalf("WithDefaults overwrote a field that was set: %+v", kept)
	}
}

func TestConfigValidate(t *testing.T) {
	base := testConfig().WithDefaults()
	for _, tc := range []struct {
		name   string
		mutate func(*Config)
		field  string // empty means valid
	}{
		{"defaults", func(*Config) {}, ""},
		{"ttl at exactly four margins", func(c *Config) { c.TTL = 12 * sec }, "ttl"},
		{"ttl just above four margins", func(c *Config) { c.TTL = 12001 * ms }, ""},
		{"ttl below four margins", func(c *Config) { c.TTL = 11 * sec }, "ttl"},
		{"margin too large to multiply by four", func(c *Config) { c.Margin = 1 << 62 }, "ttl"},
		{"ttl below the minimum", func(c *Config) { c.TTL = 500 * ms; c.Margin = 100 * ms }, "ttl"},
		{"ttl above the maximum", func(c *Config) { c.TTL = 2 * time.Hour }, "ttl"},
		{"ttl not in whole microseconds", func(c *Config) { c.TTL = 30*sec + 1 }, "ttl"},
		{"zero concurrency", func(c *Config) { c.Concurrency = 0 }, "concurrency"},
		{"concurrency at the limit", func(c *Config) { c.Concurrency = 10000 }, ""},
		{"concurrency past the limit", func(c *Config) { c.Concurrency = 10001 }, "concurrency"},
		{"negative margin", func(c *Config) { c.Margin = -sec }, "margin"},
		{"long poll at the limit", func(c *Config) { c.LongPoll = 30 * sec }, ""},
		{"long poll past the limit", func(c *Config) { c.LongPoll = 31 * sec }, "long_poll"},
		{"negative long poll", func(c *Config) { c.LongPoll = -sec }, "long_poll"},
		{"negative ack window", func(c *Config) { c.AckWindow = -ms }, "ack_window"},
		{"ack batch at the limit", func(c *Config) { c.AckBatch = job.MaxAckBatch }, ""},
		{"ack batch past the limit", func(c *Config) { c.AckBatch = job.MaxAckBatch + 1 }, "ack_batch"},
		{"zero ack batch", func(c *Config) { c.AckBatch = 0 }, "ack_batch"},
		{"negative drain timeout", func(c *Config) { c.DrainTimeout = -sec }, "drain_timeout"},
		{"negative ack retries", func(c *Config) { c.AckRetries = -1 }, "ack_retries"},
		{"negative ack retry base", func(c *Config) { c.AckRetryBase = -ms }, "ack_retry_base"},
		{"negative ack retry cap", func(c *Config) { c.AckRetryCap = -ms }, "ack_retry_cap"},
		{"negative lease retry base", func(c *Config) { c.LeaseRetryBase = -ms }, "lease_retry_base"},
		{"negative lease retry cap", func(c *Config) { c.LeaseRetryCap = -ms }, "lease_retry_cap"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mutate(&cfg)
			err := cfg.Validate()
			if tc.field == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			var inv *job.InvalidError
			if !errors.Is(err, job.ErrInvalid) || !errors.As(err, &inv) || inv.Field != tc.field {
				t.Fatalf("Validate() = %v, want job.ErrInvalid naming %q", err, tc.field)
			}
			if !strings.Contains(err.Error(), tc.field) {
				t.Fatalf("error text %q does not name %q", err.Error(), tc.field)
			}
		})
	}
}

func TestNewAppliesDefaultsAndValidates(t *testing.T) {
	tr, err := New(Config{}, 1)
	if err != nil {
		t.Fatalf("New(Config{}) = %v, want the defaults to apply", err)
	}
	wantEffects(t, tr.Start(0),
		RequestLeases{Seq: 1, Max: 1, Wait: 20 * sec, TTL: 30 * sec},
		Arm{At: 7500 * ms})
	if _, err := New(Config{TTL: 11 * sec, Margin: 3 * sec}, 1); !errors.Is(err, job.ErrInvalid) {
		t.Fatalf("New with TTL 11s and margin 3s = %v, want job.ErrInvalid", err)
	}
}
