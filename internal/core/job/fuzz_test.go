package job

import (
	"testing"
	"time"
)

// byteSource hands out the fuzz input one byte at a time and returns zeros
// once it runs dry, so that every input decodes to some run.
type byteSource struct{ b []byte }

func (s *byteSource) next() byte {
	if len(s.b) == 0 {
		return 0
	}
	c := s.b[0]
	s.b = s.b[1:]
	return c
}

// fuzzStart is the instant a fuzzed run starts at.
var fuzzStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// decodeStart reads a start job in any state. Epoch, Attempts and MaxAttempts
// are small so that the interesting boundaries come up often. The lease and
// finish fields are set to match the state, because the property under test
// is that the transitions keep them matching.
func decodeStart(src *byteSource) Job {
	j := Job{
		ID: 1, Queue: "q", Tenant: "t", Payload: []byte("payload"),
		State:       State(src.next()%5) + StateReady,
		Epoch:       int64(src.next() % 16),
		Attempts:    int32(src.next() % 6),
		MaxAttempts: int32(src.next() % 6),
		RunAt:       fuzzStart.Add(time.Duration(int8(src.next())) * time.Second),
		CreatedAt:   fuzzStart.Add(-time.Hour),
	}
	leaseOffset := time.Duration(int8(src.next())) * time.Second
	switch j.State {
	case StateLeased:
		j.LeasedBy, j.LeaseExpiresAt = "w", fuzzStart.Add(leaseOffset)
	case StateSucceeded, StateDead, StateCancelled:
		j.FinishedAt = fuzzStart.Add(leaseOffset)
		j.Result = []byte("result")
	case StateReady:
	}
	return j
}

// reachable reports whether j satisfies the invariants that every job built
// by New and moved by the transitions satisfies. A ready job has attempts
// left, because Fail sends one with none left to dead; a job that has been
// leased has at least one attempt; none has more than its budget.
func reachable(j Job) bool {
	if j.MaxAttempts < 1 || j.Attempts < 0 || j.Attempts > j.MaxAttempts {
		return false
	}
	switch j.State {
	case StateReady:
		return j.Attempts < j.MaxAttempts
	case StateLeased, StateSucceeded, StateDead:
		return j.Attempts >= 1
	case StateCancelled:
		return true
	}
	return false
}

// fuzzOps is the number of operation kinds: the eight transitions and
// Prunable.
const fuzzOps = 9

// applyFuzzOp runs operation kind on j. It returns ok == false for Prunable,
// which does not return a job.
func applyFuzzOp(kind byte, j Job, now time.Time, token int64, p byte) (out Job, o Outcome, ok bool) {
	ttl := time.Duration(1+p%4) * time.Second
	delay := time.Duration(p>>3%4) * time.Second
	switch kind % fuzzOps {
	case 0:
		out, o = Lease(j, now, ttl, "w2")
	case 1:
		out, o = Heartbeat(j, token, now, ttl)
	case 2:
		out, o = Complete(j, token, now, []byte("done"))
	case 3:
		out, o = Fail(j, token, now, p&4 != 0, delay, "boom")
	case 4:
		out, o = Release(j, token, now)
	case 5:
		out, o = Cancel(j, now)
	case 6:
		out, o = Reap(j, now, delay)
	case 7:
		out, o = Requeue(j, now)
	default:
		Prunable(j, now, ttl, delay)
		return j, 0, false
	}
	return out, o, true
}

// fuzzSeed encodes a start row followed by every operation once with the
// matching token and no time step.
func fuzzSeed(state State, epoch, attempts, maxAttempts byte, runOffset, leaseOffset int8) []byte {
	b := []byte{byte(state - StateReady), epoch, attempts, maxAttempts, byte(runOffset), byte(leaseOffset)}
	for kind := byte(0); kind < fuzzOps; kind++ {
		b = append(b, kind, 0x80, 0, 0)
	}
	return b
}

func FuzzTransitions(f *testing.F) {
	// The nine rows of the matrix, with epoch 7 and fuzzStart as now.
	f.Add(fuzzSeed(StateReady, 7, 0, 5, 0, 0))
	f.Add(fuzzSeed(StateReady, 7, 0, 5, 1, 0))
	f.Add(fuzzSeed(StateLeased, 7, 1, 5, 0, 10))
	f.Add(fuzzSeed(StateLeased, 7, 1, 5, 0, -1))
	f.Add(fuzzSeed(StateLeased, 8, 1, 5, 0, 10))
	f.Add(fuzzSeed(StateSucceeded, 7, 1, 5, 0, -1))
	f.Add(fuzzSeed(StateSucceeded, 8, 1, 5, 0, -1))
	f.Add(fuzzSeed(StateDead, 7, 5, 5, 0, -1))
	f.Add(fuzzSeed(StateCancelled, 7, 1, 5, 0, -1))
	f.Fuzz(func(t *testing.T, data []byte) {
		src := &byteSource{b: data}
		cur := decodeStart(src)
		clean := reachable(cur)
		now := fuzzStart
		for step := 0; step < 32 && len(src.b) >= 4; step++ {
			kind, tok, dt, p := src.next(), src.next(), src.next(), src.next()
			now = now.Add(time.Duration(dt%8) * time.Second)
			token := int64(tok % 16)
			if tok&0x80 != 0 {
				token = cur.Epoch // often present the token that would match
			}
			before := snapshot(cur)
			out, o, ok := applyFuzzOp(kind, cur, now, token, p)
			if !Equal(cur, before) {
				t.Fatalf("step %d: operation %d changed its argument: %+v, was %+v", step, kind%fuzzOps, cur, before)
			}
			if !ok {
				continue
			}
			op := kind % fuzzOps
			isLease := op == 0
			revived := op == 7 && cur.State == StateDead && out.State == StateReady
			switch {
			case isLease && o == Applied && out.Epoch != cur.Epoch+1:
				t.Fatalf("step %d: Lease moved Epoch from %d to %d", step, cur.Epoch, out.Epoch)
			case !isLease && out.Epoch != cur.Epoch:
				t.Fatalf("step %d: operation %d moved Epoch from %d to %d", step, op, cur.Epoch, out.Epoch)
			case o != Applied && !Equal(out, cur):
				t.Fatalf("step %d: operation %d returned %v but changed the job: %+v, was %+v", step, op, o, out, cur)
			case cur.State.Terminal() && !Equal(out, cur) && !revived:
				t.Fatalf("step %d: a %v job became %+v through operation %d; only Requeue may revive a dead one", step, cur.State, out, op)
			}
			if (out.State == StateLeased) != (out.LeasedBy != "") || (out.State == StateLeased) == out.LeaseExpiresAt.IsZero() {
				t.Fatalf("step %d: lease fields do not match state %v: %+v", step, out.State, out)
			}
			if out.State.Terminal() == out.FinishedAt.IsZero() {
				t.Fatalf("step %d: FinishedAt does not match state %v: %+v", step, out.State, out)
			}
			if clean && !reachable(out) {
				t.Fatalf("step %d: operation %d took a reachable job to an unreachable one (Attempts %d of %d, state %v): %+v",
					step, op, out.Attempts, out.MaxAttempts, out.State, out)
			}
			cur = out
		}
	})
}
