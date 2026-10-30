package history

import (
	"fmt"
	"slices"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
)

// FinalJob is a job as the jobs table holds it after the run.
type FinalJob struct {
	// ID is the job's id.
	ID int64
	// State is where the job ended.
	State job.State
	// Epoch is the job's final lease epoch.
	Epoch int64
}

// SinkRow is a row of the sink table after the run: the token of the write
// the sink accepted last for the job.
type SinkRow struct {
	// JobID is the job the row belongs to.
	JobID int64
	// Token is the fencing token the sink stored.
	Token int64
}

// Violation is one broken invariant.
type Violation struct {
	// Invariant is the name of the rule: S1, S4, S7 or L1.
	Invariant string
	// Detail says what was found.
	Detail string
	// JobID is the job concerned; for S7 it is the first id the key got.
	JobID int64
}

// Report is the result of Check.
type Report struct {
	// Violations lists every broken invariant, rule by rule: S1, S7, S4
	// (the final state first, then the write order), L1. Within a rule they
	// follow the order of the confirmations, the keys, final, or the sink
	// writes by time, whichever the rule walks.
	Violations []Violation
	// Counts is the number of events of each kind.
	Counts map[Kind]int
	// DuplicateDeliveries counts the HandlerStart events beyond the first
	// for each job. Redelivery is allowed; the count says how hard the run
	// pushed on it.
	DuplicateDeliveries int
}

// The names of the invariants Check reports.
const (
	invS1 = "S1" // no confirmed enqueue is lost
	invS4 = "S4" // the sink only moves forward and ends at the final epoch
	invS7 = "S7" // an idempotency key names one job
	invL1 = "L1" // every confirmed job finishes
)

// Check applies the invariants to a history, the final state of the jobs
// table and the contents of the sink table, and reports what is broken.
//
//   - S1: every EnqueueConfirmed id is in final. Chaos runs keep their jobs
//     for longer than the run, so nothing is pruned.
//   - S7: every EnqueueConfirmed with a given key has the same id. An empty
//     key means none. The event carries no tenant or queue, so a run must
//     use keys that are unique across them.
//   - S4: every succeeded job in final has a sink row whose token is its
//     final epoch, and no applied SinkWrite has a token lower than an
//     earlier applied write for the same job. Earlier means a smaller At;
//     writes with equal At keep their order in events.
//   - L1: every confirmed job in final that no CancelConfirmed covers is
//     succeeded or dead.
//
// A confirmed id that is missing from final breaks S1 only. Check does not
// modify its arguments.
func Check(events []Event, final []FinalJob, sink []SinkRow) Report {
	rep := Report{Counts: make(map[Kind]int)}

	finalByID := make(map[int64]FinalJob, len(final))
	for _, f := range final {
		finalByID[f.ID] = f
	}
	sinkTokens := make(map[int64][]int64)
	for _, s := range sink {
		sinkTokens[s.JobID] = append(sinkTokens[s.JobID], s.Token)
	}

	var (
		confirmed []int64 // distinct confirmed ids, in order of first confirmation
		seen      = make(map[int64]bool)
		keys      []string // distinct non-empty keys, in order of first appearance
		keyIDs    = make(map[string][]int64)
		cancelled = make(map[int64]bool)
		starts    = make(map[int64]int)
		applied   []Event
	)
	for _, e := range events {
		rep.Counts[e.Kind]++
		switch e.Kind {
		case EnqueueConfirmed:
			if !seen[e.ID] {
				seen[e.ID] = true
				confirmed = append(confirmed, e.ID)
			}
			if e.Key != "" {
				ids, known := keyIDs[e.Key]
				if !known {
					keys = append(keys, e.Key)
				}
				if !slices.Contains(ids, e.ID) {
					keyIDs[e.Key] = append(ids, e.ID)
				}
			}
		case CancelConfirmed:
			cancelled[e.ID] = true
		case HandlerStart:
			starts[e.ID]++
		case SinkWrite:
			if e.Applied {
				applied = append(applied, e)
			}
		case HandlerEnd:
		}
	}
	for _, n := range starts {
		rep.DuplicateDeliveries += n - 1
	}

	add := func(invariant string, id int64, format string, args ...any) {
		rep.Violations = append(rep.Violations, Violation{Invariant: invariant, JobID: id, Detail: fmt.Sprintf(format, args...)})
	}

	for _, id := range confirmed {
		if _, ok := finalByID[id]; !ok {
			add(invS1, id, "job %d was confirmed as enqueued but is not in the final state", id)
		}
	}

	for _, key := range keys {
		if ids := keyIDs[key]; len(ids) > 1 {
			add(invS7, ids[0], "key %q was confirmed with ids %v", key, ids)
		}
	}

	for _, f := range final {
		if f.State != job.StateSucceeded || slices.Contains(sinkTokens[f.ID], f.Epoch) {
			continue
		}
		if tokens := sinkTokens[f.ID]; len(tokens) == 0 {
			add(invS4, f.ID, "job %d succeeded at epoch %d but the sink has no row for it", f.ID, f.Epoch)
		} else {
			add(invS4, f.ID, "job %d succeeded at epoch %d but the sink holds token %v", f.ID, f.Epoch, tokens)
		}
	}

	slices.SortStableFunc(applied, func(a, b Event) int { return a.At.Compare(b.At) })
	highest := make(map[int64]int64)
	for _, e := range applied {
		prev, ok := highest[e.ID]
		if ok && e.Token < prev {
			add(invS4, e.ID, "job %d: the sink applied token %d at %s after it applied token %d",
				e.ID, e.Token, e.At.Format("2006-01-02T15:04:05.000000Z07:00"), prev)
			continue
		}
		highest[e.ID] = e.Token
	}

	for _, id := range confirmed {
		f, ok := finalByID[id]
		if !ok || cancelled[id] {
			continue
		}
		if f.State != job.StateSucceeded && f.State != job.StateDead {
			add(invL1, id, "confirmed job %d ended %s, want succeeded or dead", id, f.State)
		}
	}

	return rep
}
