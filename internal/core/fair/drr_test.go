package fair

import (
	"maps"
	"math"
	"slices"
	"strings"
	"testing"

	"pgregory.net/rapid"
)

// newPlanner returns a planner with quantum q whose ring holds tenants in
// the given order, all at the default weight.
func newPlanner(q int64, tenants ...string) *DRR {
	d := New(Config{QuantumPerWeight: q})
	d.SetActive(tenants)
	return d
}

// wantPlan fails unless Plan(max) returns exactly want.
func wantPlan(t *testing.T, d *DRR, max int, want []Take) {
	t.Helper()
	if got := d.Plan(max); !slices.Equal(got, want) {
		t.Fatalf("Plan(%d) = %v, want %v", max, got, want)
	}
}

func wantActive(t *testing.T, d *DRR, want ...string) {
	t.Helper()
	if got := d.Active(); !slices.Equal(got, want) {
		t.Fatalf("Active() = %v, want %v", got, want)
	}
}

func TestPlanSingleTenantGetsWholeBatch(t *testing.T) {
	d := newPlanner(8, "a")
	wantPlan(t, d, 64, []Take{{"a", 64}})
}

func TestPlanHonorsWeights(t *testing.T) {
	d := newPlanner(8, "a", "b")
	d.SetWeights(map[string]int64{"a": 1, "b": 3})
	wantPlan(t, d, 32, []Take{{"a", 8}, {"b", 24}})
}

func TestPlanCarriesDeficitAcrossCalls(t *testing.T) {
	d := newPlanner(8, "a", "b")
	d.SetWeights(map[string]int64{"a": 1, "b": 3})
	wantPlan(t, d, 5, []Take{{"a", 5}})
	wantPlan(t, d, 5, []Take{{"a", 3}, {"b", 2}})
	wantPlan(t, d, 30, []Take{{"b", 22}, {"a", 8}})
}

func TestPlanMergesWrappedTurns(t *testing.T) {
	d := newPlanner(1, "a", "b")
	wantPlan(t, d, 5, []Take{{"a", 3}, {"b", 2}})
}

func TestPlanZeroAndEmpty(t *testing.T) {
	d := newPlanner(8, "a")
	if got := d.Plan(0); got != nil {
		t.Errorf("Plan(0) = %v, want nil", got)
	}
	if got := d.Plan(-3); got != nil {
		t.Errorf("Plan(-3) = %v, want nil", got)
	}
	if got := New(Config{}).Plan(10); got != nil {
		t.Errorf("Plan(10) on an empty ring = %v, want nil", got)
	}
}

func TestDefaultQuantumIsEight(t *testing.T) {
	for _, q := range []int64{0, -1} {
		d := New(Config{QuantumPerWeight: q})
		d.SetActive([]string{"a", "b"})
		// One call of 8 shows the quantum: a larger call would merge turns.
		wantPlan(t, d, 8, []Take{{"a", 8}})
		wantPlan(t, d, 8, []Take{{"b", 8}})
	}
}

func TestDefaultWeightIsOne(t *testing.T) {
	d := newPlanner(8, "a", "b")
	d.SetWeights(map[string]int64{"a": 0, "b": -5})
	wantPlan(t, d, 16, []Take{{"a", 8}, {"b", 8}})

	// A tenant absent from the map weighs 1 as well.
	d = newPlanner(8, "a", "b")
	d.SetWeights(map[string]int64{"a": 3})
	wantPlan(t, d, 32, []Take{{"a", 24}, {"b", 8}})
}

func TestSetWeightsReplacesPreviousWeights(t *testing.T) {
	d := newPlanner(8, "a", "b")
	d.SetWeights(map[string]int64{"a": 3})
	d.SetWeights(map[string]int64{"b": 2})
	wantPlan(t, d, 24, []Take{{"a", 8}, {"b", 16}})

	// The planner keeps its own copy of the map.
	w := map[string]int64{"a": 2}
	d.SetWeights(w)
	w["a"] = 100
	wantPlan(t, d, 24, []Take{{"a", 16}, {"b", 8}})
}

func TestPlanSaturatesHugeWeights(t *testing.T) {
	d := newPlanner(8, "a")
	d.SetWeights(map[string]int64{"a": math.MaxInt64})
	for range 2 {
		wantPlan(t, d, 10, []Take{{"a", 10}})
	}
}

func TestSettleRemovesExhaustedTenant(t *testing.T) {
	d := newPlanner(8, "a", "b")
	takes := d.Plan(16)
	if want := []Take{{"a", 8}, {"b", 8}}; !slices.Equal(takes, want) {
		t.Fatalf("Plan(16) = %v, want %v", takes, want)
	}
	d.Settle(takes, []int{3, 8})
	wantActive(t, d, "b")
	wantPlan(t, d, 8, []Take{{"b", 8}})
}

func TestSettleKeepsCursorOnNextTenant(t *testing.T) {
	d := newPlanner(1, "a", "b", "c")
	takes := d.Plan(2) // a and b took their turns; c is next.
	d.Settle(takes, []int{0, 1})
	wantActive(t, d, "b", "c")
	wantPlan(t, d, 1, []Take{{"c", 1}})
}

func TestSettleWrapsWhenTheLastTenantAtTheCursorLeaves(t *testing.T) {
	d := newPlanner(1, "a", "b")
	wantPlan(t, d, 1, []Take{{"a", 1}}) // b is next.
	d.Settle([]Take{{"b", 1}}, []int{0})
	wantActive(t, d, "a")
	wantPlan(t, d, 1, []Take{{"a", 1}})
}

func TestSettleClosesTheRemovedTenantsOpenTurn(t *testing.T) {
	d := newPlanner(8, "a", "b", "c")
	takes := d.Plan(3) // a's turn stays open with 5 left.
	d.Settle(takes, []int{0})
	wantActive(t, d, "b", "c")
	// b is next and starts a fresh turn: it must not inherit a's open turn.
	wantPlan(t, d, 8, []Take{{"b", 8}})
}

func TestSettleKeepsTenantsThatGotTheirFullLimit(t *testing.T) {
	d := newPlanner(8, "a", "b")
	takes := d.Plan(16)
	d.Settle(takes, []int{8, 8})
	wantActive(t, d, "a", "b")
	wantPlan(t, d, 16, []Take{{"a", 8}, {"b", 8}})
}

func TestSettlePanicsOnLengthMismatch(t *testing.T) {
	defer func() {
		msg, _ := recover().(string)
		if !strings.Contains(msg, "Settle") {
			t.Errorf("panic value = %q, want a message naming Settle", msg)
		}
	}()
	newPlanner(8, "a").Settle([]Take{{"a", 1}}, nil)
}

func TestActivateAppendsAndSetActiveKeepsOrder(t *testing.T) {
	d := New(Config{})
	d.SetActive([]string{"b", "a"})
	d.Activate("c")
	d.Activate("a") // already present: no change
	wantActive(t, d, "b", "a", "c")
	d.SetActive([]string{"c", "b"})
	wantActive(t, d, "b", "c")

	// New tenants join in the order given; a repeated name counts once.
	d.SetActive([]string{"d", "c", "a", "d", "b"})
	wantActive(t, d, "b", "c", "d", "a")
}

func TestActiveReturnsACopy(t *testing.T) {
	d := newPlanner(8, "a", "b")
	d.Active()[0] = "x"
	wantActive(t, d, "a", "b")
}

func TestSetActiveDropsTheDeficitOfRemovedTenants(t *testing.T) {
	d := newPlanner(8, "a", "b")
	wantPlan(t, d, 3, []Take{{"a", 3}}) // a keeps 5 of its turn.
	d.SetActive([]string{"b"})
	d.SetActive([]string{"b", "a"})
	wantPlan(t, d, 8, []Take{{"b", 8}})
	// a returns with a fresh quantum, not 5 + 8.
	wantPlan(t, d, 16, []Take{{"a", 8}, {"b", 8}})
}

func TestSetActiveKeepsTheTurnOfASurvivingTenant(t *testing.T) {
	d := newPlanner(8, "a", "b", "c")
	wantPlan(t, d, 11, []Take{{"a", 8}, {"b", 3}}) // b keeps 5 of its turn.
	d.SetActive([]string{"c", "b"})
	wantPlan(t, d, 5, []Take{{"b", 5}})
	wantPlan(t, d, 8, []Take{{"c", 8}})
}

// scenario is a generated planner setup: tenants with weights, a quantum and
// a sequence of batch sizes whose sum stays within 2000.
type scenario struct {
	quantum int64
	names   []string
	weights []int64
	batches []int
}

func drawScenario(rt *rapid.T) scenario {
	n := rapid.IntRange(1, 6).Draw(rt, "tenants")
	names := make([]string, n)
	for i := range names {
		names[i] = "t" + string(rune('0'+i))
	}
	return scenario{
		quantum: rapid.Int64Range(1, 8).Draw(rt, "quantum"),
		names:   names,
		weights: rapid.SliceOfN(rapid.Int64Range(1, 5), n, n).Draw(rt, "weights"),
		batches: rapid.SliceOfN(rapid.IntRange(1, 64), 1, 31).Draw(rt, "batches"),
	}
}

// planner returns a planner for the scenario with every tenant active.
func (s scenario) planner() *DRR {
	d := newPlanner(s.quantum, s.names...)
	w := make(map[string]int64, len(s.names))
	for i, name := range s.names {
		w[name] = s.weights[i]
	}
	d.SetWeights(w)
	return d
}

// add folds one Plan result into the cumulative counts and returns the
// number of jobs it granted. Every Take must name a tenant once and grant
// at least one job.
func add(rt *rapid.T, counts map[string]int, takes []Take) int {
	seen := map[string]bool{}
	total := 0
	for _, tk := range takes {
		if seen[tk.Tenant] || tk.Limit < 1 {
			rt.Fatalf("Plan result %v repeats a tenant or grants nothing", takes)
		}
		seen[tk.Tenant] = true
		counts[tk.Tenant] += tk.Limit
		total += tk.Limit
	}
	return total
}

func TestPlanBatchIndependence(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		s := drawScenario(rt)
		batched, single := s.planner(), s.planner()
		inBatches, oneByOne := map[string]int{}, map[string]int{}
		for i, size := range s.batches {
			if got := add(rt, inBatches, batched.Plan(size)); got != size {
				rt.Fatalf("batch %d: Plan(%d) granted %d", i, size, got)
			}
			for range size {
				add(rt, oneByOne, single.Plan(1))
			}
			if !maps.Equal(inBatches, oneByOne) {
				rt.Fatalf("after batch %d (size %d): batches gave %v, one job at a time gave %v",
					i, size, inBatches, oneByOne)
			}
		}
	})
}

// TestPlanShareBound pins the fairness guarantee. With every tenant always
// active, a tenant i that has received G_i of N jobs in total, with weight
// w_i out of a total W, satisfies |G_i*W - N*w_i| <= Q*w_i*W after every call.
func TestPlanShareBound(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		s := drawScenario(rt)
		d := s.planner()
		var total int64
		for _, w := range s.weights {
			total += w
		}
		counts := map[string]int{}
		granted := 0
		for i, size := range s.batches {
			granted += add(rt, counts, d.Plan(size))
			for j, name := range s.names {
				w := s.weights[j]
				drift := int64(counts[name])*total - int64(granted)*w
				bound := s.quantum * w * total
				if drift > bound || drift < -bound {
					rt.Fatalf("after batch %d: tenant %s has %d of %d jobs, drift %d exceeds bound %d (weights %v, quantum %d)",
						i, name, counts[name], granted, drift, bound, s.weights, s.quantum)
				}
			}
		}
	})
}
