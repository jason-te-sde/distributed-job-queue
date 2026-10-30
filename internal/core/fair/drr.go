package fair

import (
	"maps"
	"math"
	"slices"

	"github.com/jason-te-sde/distributed-job-queue/internal/flaw"
)

// defaultQuantum is the quantum per unit of weight used when Config leaves
// it unset.
const defaultQuantum = 8

// Config configures a DRR.
type Config struct {
	// QuantumPerWeight is the number of jobs a tenant of weight 1 may take in
	// one turn. A value of zero or less selects the default, 8.
	QuantumPerWeight int64
}

// Take asks for up to Limit jobs of one tenant.
type Take struct {
	// Tenant is the tenant name.
	Tenant string
	// Limit is the number of jobs to fetch for the tenant.
	Limit int
}

// DRR is a deficit round robin planner. Tenants sit in a ring and are served
// in ring order. A tenant's turn starts by crediting it Q*weight jobs and
// lasts until it has used them, and a turn that a batch cuts short stays open
// for the next Plan call.
//
// A DRR is not safe for concurrent use: the caller serializes all calls.
type DRR struct {
	quantum  int64
	ring     []string
	deficit  map[string]int64
	weight   map[string]int64
	cursor   int  // index in ring of the tenant whose turn is open or next
	turnOpen bool // whether ring[cursor] has been credited for this turn
}

// New returns a planner with an empty ring.
func New(cfg Config) *DRR {
	q := cfg.QuantumPerWeight
	if q <= 0 {
		q = defaultQuantum
	}
	return &DRR{quantum: q, deficit: map[string]int64{}, weight: map[string]int64{}}
}

// SetWeights replaces the tenant weights. Tenants absent from w weigh 1, and
// so does any tenant whose value is below 1. The planner keeps its own copy
// of w. A change takes effect from each tenant's next turn.
func (d *DRR) SetWeights(w map[string]int64) {
	d.weight = maps.Clone(w)
}

// SetActive makes tenants the ring. Tenants already in the ring keep their
// order and their turn, tenants no longer listed leave the ring and lose
// their deficit, and tenants not yet in the ring join at the tail in the order
// given.
func (d *DRR) SetActive(tenants []string) {
	keep := make(map[string]bool, len(tenants))
	for _, t := range tenants {
		keep[t] = true
	}
	for _, t := range slices.Clone(d.ring) {
		if !keep[t] {
			d.remove(t)
		}
	}
	for _, t := range tenants {
		d.Activate(t)
	}
}

// Activate appends tenant to the tail of the ring unless it is already there.
func (d *DRR) Activate(tenant string) {
	if !slices.Contains(d.ring, tenant) {
		d.ring = append(d.ring, tenant)
	}
}

// Active returns the tenants in ring order. The caller may modify the result.
func (d *DRR) Active() []string {
	return slices.Clone(d.ring)
}

// Plan shares max jobs between the tenants in the ring and returns one Take
// per tenant that gets any, in the order the tenants were first served. It
// returns nil when max is not positive or the ring is empty.
//
// The result does not depend on how the jobs are asked for: planning n jobs in
// one call grants each tenant what planning one job n times would.
func (d *DRR) Plan(max int) []Take {
	if max <= 0 || len(d.ring) == 0 {
		return nil
	}
	var takes []Take
	index := map[string]int{} // tenant -> position in takes
	for max > 0 && len(d.ring) > 0 {
		t := d.ring[d.cursor]
		if !d.turnOpen {
			d.deficit[t] += d.quantumFor(t)
			d.turnOpen = true
		}
		n := min(d.deficit[t], int64(max))
		i, seen := index[t]
		if !seen {
			i = len(takes)
			index[t] = i
			takes = append(takes, Take{Tenant: t})
		}
		takes[i].Limit += int(n)
		d.deficit[t] -= n
		max -= int(n)
		if d.deficit[t] == 0 {
			d.advance()
		}
	}
	// Flaw F8 drops the unused part of a turn that the batch cut short, so
	// the share of a tenant depends on how the jobs were asked for.
	if flaw.On(flaw.F8) && d.turnOpen {
		if t := d.ring[d.cursor]; d.deficit[t] > 0 {
			d.deficit[t] = 0
			d.advance()
		}
	}
	return takes
}

// Settle reports how many jobs each take really delivered, got[i] for
// takes[i]. The takes come from an earlier Plan, not necessarily the latest:
// several fetches can be in flight, and their plans may be settled in any
// order. A tenant that delivered fewer jobs than asked has run out of work:
// it leaves the ring and loses its deficit until it is activated again.
//
// Settle panics if got and takes differ in length.
func (d *DRR) Settle(takes []Take, got []int) {
	if len(got) != len(takes) {
		panic("fair: DRR.Settle needs one count per take")
	}
	for i, tk := range takes {
		if got[i] < tk.Limit {
			d.remove(tk.Tenant)
		}
	}
}

// quantumFor returns the jobs a turn of tenant is worth. A tenant with no
// weight, or one below 1, weighs 1. The product saturates at the largest
// int64 so that an absurd weight cannot wrap around to a negative grant.
func (d *DRR) quantumFor(tenant string) int64 {
	w := max(d.weight[tenant], 1)
	if w > math.MaxInt64/d.quantum {
		return math.MaxInt64
	}
	return w * d.quantum
}

// advance ends the open turn and moves to the next tenant in the ring.
func (d *DRR) advance() {
	d.cursor = (d.cursor + 1) % len(d.ring)
	d.turnOpen = false
}

// remove takes tenant out of the ring, if it is there, and drops its
// deficit. The cursor keeps pointing at the same next tenant: it moves back
// when the removed tenant sat before it and wraps at the end of the ring. If
// the removed tenant held the cursor, its open turn ends with it.
func (d *DRR) remove(tenant string) {
	delete(d.deficit, tenant)
	i := slices.Index(d.ring, tenant)
	if i < 0 {
		return
	}
	d.ring = slices.Delete(d.ring, i, i+1)
	switch {
	case i < d.cursor:
		d.cursor--
	case i == d.cursor:
		d.turnOpen = false
	}
	if d.cursor >= len(d.ring) {
		d.cursor = 0
	}
}
