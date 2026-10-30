package sim_test

import (
	"encoding/binary"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/sim"
)

// drain fires events until the queue is empty and returns how many fired.
func drain(w *sim.World) int {
	n := 0
	for w.Step() {
		n++
	}
	return n
}

// advanceTo fires events until virtual time reaches d. It fails the test if
// the queue empties first.
func advanceTo(t *testing.T, w *sim.World, d time.Duration) {
	t.Helper()
	reached := false
	w.At(d, "advance", func() { reached = true })
	for !reached {
		if !w.Step() {
			t.Fatalf("queue emptied before virtual time reached %v", d)
		}
	}
}

// mustPanic runs f and returns the panic message, failing the test when f
// does not panic with a string.
func mustPanic(t *testing.T, f func()) (msg string) {
	t.Helper()
	defer func() {
		r := recover()
		s, ok := r.(string)
		if !ok {
			t.Fatalf("recovered %v (%T), want a string panic", r, r)
		}
		msg = s
	}()
	f()
	return ""
}

func TestEventsRunInTimeThenSeqOrder(t *testing.T) {
	w := sim.NewWorld(1)
	var got []string
	record := func(label string) func() {
		return func() { got = append(got, label+"@"+w.Now.String()) }
	}
	// Scheduled out of time order; b1..b3 share a time and must keep the
	// order in which they were scheduled.
	w.At(30*time.Millisecond, "c", record("c"))
	w.At(10*time.Millisecond, "b1", record("b1"))
	w.At(10*time.Millisecond, "b2", func() {
		record("b2")()
		// Scheduled while b2 runs, for the same instant: it goes after
		// b3, which was queued earlier.
		w.At(w.Now, "b4", record("b4"))
	})
	w.At(10*time.Millisecond, "b3", record("b3"))
	w.At(0, "a", record("a"))
	w.After(20*time.Millisecond, "after", record("after"))

	if w.Now != 0 {
		t.Fatalf("Now = %v before any Step, want 0", w.Now)
	}
	n := drain(w)
	want := []string{"a@0s", "b1@10ms", "b2@10ms", "b3@10ms", "b4@10ms", "after@20ms", "c@30ms"}
	if !slices.Equal(got, want) {
		t.Errorf("order = %v, want %v", got, want)
	}
	if n != len(want) {
		t.Errorf("fired %d events, want %d", n, len(want))
	}
	if w.Step() {
		t.Error("Step() = true on an empty queue, want false")
	}
	if w.Now != 30*time.Millisecond {
		t.Errorf("Now = %v after the last event, want 30ms", w.Now)
	}
}

func TestAfterIsRelativeToNow(t *testing.T) {
	w := sim.NewWorld(1)
	var at time.Duration
	w.At(5*time.Second, "outer", func() {
		w.After(2*time.Second, "inner", func() { at = w.Now })
	})
	drain(w)
	if at != 7*time.Second {
		t.Errorf("inner event ran at %v, want 7s", at)
	}
}

func TestSchedulingInThePastPanics(t *testing.T) {
	w := sim.NewWorld(1)
	w.At(10*time.Millisecond, "first", func() {})
	w.Step()

	if msg := mustPanic(t, func() { w.At(5*time.Millisecond, "late", func() {}) }); msg != "sim: event scheduled in the past: late" {
		t.Errorf("At panic = %q", msg)
	}
	if msg := mustPanic(t, func() { w.After(-time.Nanosecond, "negative", func() {}) }); msg != "sim: event scheduled in the past: negative" {
		t.Errorf("After panic = %q", msg)
	}
	// The present is not the past, and a refused call leaves nothing queued.
	if w.Step() {
		t.Error("a refused schedule left an event in the queue")
	}
	ran := false
	w.At(w.Now, "now", func() { ran = true })
	if !w.Step() || !ran {
		t.Error("an event scheduled for exactly Now did not run")
	}
}

func TestTraceHashCoversTimeSeqAndLabel(t *testing.T) {
	noop := func() {}
	hashOf := func(schedule func(w *sim.World)) uint64 {
		w := sim.NewWorld(1)
		schedule(w)
		drain(w)
		return w.TraceHash()
	}
	base := hashOf(func(w *sim.World) {
		w.At(time.Millisecond, "x", noop)
		w.At(5*time.Millisecond, "y", noop)
	})
	if again := hashOf(func(w *sim.World) {
		w.At(time.Millisecond, "x", noop)
		w.At(5*time.Millisecond, "y", noop)
	}); again != base {
		t.Errorf("the same events gave %x and %x", base, again)
	}
	if empty := hashOf(func(*sim.World) {}); empty == base {
		t.Error("a world that fired nothing hashes like one that fired events")
	}
	variants := map[string]func(w *sim.World){
		"different time": func(w *sim.World) {
			w.At(time.Millisecond, "x", noop)
			w.At(6*time.Millisecond, "y", noop)
		},
		"different label": func(w *sim.World) {
			w.At(time.Millisecond, "x", noop)
			w.At(5*time.Millisecond, "z", noop)
		},
		// The same (time, label) sequence fires, but y was scheduled first
		// and so carries the lower Seq.
		"different seq": func(w *sim.World) {
			w.At(5*time.Millisecond, "y", noop)
			w.At(time.Millisecond, "x", noop)
		},
	}
	for name, schedule := range variants {
		if got := hashOf(schedule); got == base {
			t.Errorf("%s: hash unchanged (%x)", name, got)
		}
	}
}

// TestTraceHashDistinguishesEventBoundaries pins that the hashed encoding is
// injective: a label that contains the bytes of a second event's (time, seq)
// must not hash like two events.
func TestTraceHashDistinguishesEventBoundaries(t *testing.T) {
	noop := func() {}
	// The (time, seq) of the second event below: 5ms and seq 1, as two
	// little-endian 64-bit words.
	header := binary.LittleEndian.AppendUint64(nil, uint64(5*time.Millisecond))
	header = binary.LittleEndian.AppendUint64(header, 1)

	two := sim.NewWorld(1)
	two.At(time.Millisecond, "x", noop)
	two.At(5*time.Millisecond, "y", noop)
	drain(two)

	one := sim.NewWorld(1)
	one.At(time.Millisecond, "x"+string(header)+"y", noop)
	drain(one)

	if one.TraceHash() == two.TraceHash() {
		t.Error("one event with a crafted label hashes like two events")
	}
}

// lossy is the network the toy scenario and the rate tests run over.
var lossy = sim.NetConfig{
	DropP:   0.1,
	DupP:    0.05,
	Latency: [2]time.Duration{time.Millisecond, 5 * time.Millisecond},
	TailP:   0.01,
	Tail:    [2]time.Duration{time.Second, 5 * time.Second},
}

// pingPong runs a toy scenario: three nodes exchange 1000 pings over a lossy
// network, each starting at a random time, and every delivered ping is
// answered with a pong. It returns the trace hash and the number of events
// that fired.
func pingPong(seed uint64) (hash uint64, fired int) {
	w := sim.NewWorld(seed)
	net := sim.NewNetwork(w, lossy)
	nodes := []sim.NodeID{"a", "b", "c"}
	rngs := make([]*sim.Rand, len(nodes))
	for i := range rngs {
		rngs[i] = w.Rand.Fork()
	}
	for i := range 1000 {
		from := i % len(nodes)
		w.After(w.Rand.Between(0, time.Second), "ping-start", func() {
			to := (from + 1 + rngs[from].IntN(len(nodes)-1)) % len(nodes)
			net.Send(nodes[from], nodes[to], true, "ping", func() {
				net.Send(nodes[to], nodes[from], true, "pong", func() {})
			})
		})
	}
	fired = drain(w)
	return w.TraceHash(), fired
}

func TestSameSeedSameTrace(t *testing.T) {
	h1, fired1 := pingPong(8123)
	h2, fired2 := pingPong(8123)
	if h1 != h2 || fired1 != fired2 {
		t.Errorf("seed 8123 twice: hash %x vs %x, events %d vs %d", h1, h2, fired1, fired2)
	}
	if fired1 <= 1000 {
		t.Errorf("only %d events fired: the scenario did not exercise the network", fired1)
	}
	if h3, _ := pingPong(8124); h3 == h1 {
		t.Errorf("seeds 8123 and 8124 gave the same hash %x", h1)
	}
}

func TestNetworkRates(t *testing.T) {
	const sends = 100_000
	run := func(seed uint64, idempotent bool) (sim.NetStats, int) {
		w := sim.NewWorld(seed)
		net := sim.NewNetwork(w, lossy)
		delivered := 0
		for range sends {
			net.Send("a", "b", idempotent, "msg", func() { delivered++ })
		}
		drain(w)
		return net.Stats(), delivered
	}

	t.Run("idempotent", func(t *testing.T) {
		s, delivered := run(101, true)
		if s.Sent != sends {
			t.Errorf("Sent = %d, want %d", s.Sent, sends)
		}
		if s.Dropped < 9000 || s.Dropped > 11000 {
			t.Errorf("Dropped = %d, want within [9000, 11000] (expected 10000)", s.Dropped)
		}
		// A duplicate needs a message that was not dropped: the expected
		// count is 100000 * 0.9 * 0.05 = 4500.
		if s.Duplicated < 4000 || s.Duplicated > 6000 {
			t.Errorf("Duplicated = %d, want within [4000, 6000] (expected 4500)", s.Duplicated)
		}
		if s.Partitioned != 0 {
			t.Errorf("Partitioned = %d, want 0", s.Partitioned)
		}
		if want := s.Sent - s.Dropped + s.Duplicated; delivered != want {
			t.Errorf("delivered %d, want Sent-Dropped+Duplicated = %d", delivered, want)
		}
	})

	t.Run("non-idempotent", func(t *testing.T) {
		s, delivered := run(102, false)
		if s.Dropped < 9000 || s.Dropped > 11000 {
			t.Errorf("Dropped = %d, want within [9000, 11000] (expected 10000)", s.Dropped)
		}
		if s.Duplicated != 0 {
			t.Errorf("Duplicated = %d, want 0: only idempotent messages may be duplicated", s.Duplicated)
		}
		if want := s.Sent - s.Dropped; delivered != want {
			t.Errorf("delivered %d, want Sent-Dropped = %d", delivered, want)
		}
	})
}

func TestNetworkLatencyStaysInConfiguredRanges(t *testing.T) {
	const sends = 20_000
	cfg := sim.NetConfig{
		Latency: [2]time.Duration{2 * time.Millisecond, 8 * time.Millisecond},
		TailP:   0.05,
		Tail:    [2]time.Duration{time.Second, 4 * time.Second},
	}
	w := sim.NewWorld(7)
	net := sim.NewNetwork(w, cfg)
	var typical, tail int
	for range sends {
		sentAt := w.Now
		net.Send("a", "b", false, "msg", func() {
			d := w.Now - sentAt
			switch {
			case d >= cfg.Latency[0] && d <= cfg.Latency[1]:
				typical++
			case d >= cfg.Tail[0] && d <= cfg.Tail[1]:
				tail++
			default:
				t.Errorf("latency %v is in neither configured range", d)
			}
			if d%time.Microsecond != 0 {
				t.Errorf("latency %v is not a whole number of microseconds", d)
			}
		})
	}
	drain(w)
	if typical+tail != sends {
		t.Fatalf("delivered %d messages, want %d", typical+tail, sends)
	}
	// Expected 1000 tail messages, standard deviation about 31.
	if tail < 800 || tail > 1200 {
		t.Errorf("%d tail latencies, want within [800, 1200] (expected 1000)", tail)
	}
}

// fixedLatency is a lossless network where every message takes 5ms.
var fixedLatency = sim.NetConfig{Latency: [2]time.Duration{5 * time.Millisecond, 5 * time.Millisecond}}

func TestPartitionDropsUntilHealedOrExpired(t *testing.T) {
	t.Run("blocks both directions until it expires", func(t *testing.T) {
		w := sim.NewWorld(1)
		net := sim.NewNetwork(w, fixedLatency)
		var got []string
		send := func(from, to sim.NodeID) {
			net.Send(from, to, false, "m", func() { got = append(got, string(from)+">"+string(to)) })
		}
		net.Partition("a", "b", 100*time.Millisecond)
		send("a", "b")
		send("b", "a")
		send("a", "c") // a different pair is unaffected
		advanceTo(t, w, 99*time.Millisecond)
		send("a", "b") // still inside the partition
		advanceTo(t, w, 100*time.Millisecond)
		send("a", "b") // `until` itself is already healed
		send("b", "a")
		drain(w)

		want := []string{"a>c", "a>b", "b>a"}
		if !slices.Equal(got, want) {
			t.Errorf("delivered %v, want %v", got, want)
		}
		if s := net.Stats(); s.Sent != 6 || s.Partitioned != 3 || s.Dropped != 0 {
			t.Errorf("stats = %+v, want Sent 6, Partitioned 3, Dropped 0", s)
		}
	})

	t.Run("Heal lifts it early", func(t *testing.T) {
		w := sim.NewWorld(1)
		net := sim.NewNetwork(w, fixedLatency)
		delivered := 0
		count := func() { delivered++ }
		net.Partition("a", "b", time.Hour)
		net.Partition("c", "d", time.Hour)
		net.Send("a", "b", false, "m", count)
		advanceTo(t, w, time.Second)
		net.Heal()
		net.Send("a", "b", false, "m", count)
		net.Send("d", "c", false, "m", count)
		drain(w)
		if delivered != 2 {
			t.Errorf("delivered %d messages, want 2 (only the one sent before Heal is lost)", delivered)
		}
		if s := net.Stats(); s.Partitioned != 1 {
			t.Errorf("Partitioned = %d, want 1", s.Partitioned)
		}
	})

	t.Run("a message already in flight is delivered", func(t *testing.T) {
		w := sim.NewWorld(1)
		net := sim.NewNetwork(w, fixedLatency)
		delivered := 0
		net.Send("a", "b", false, "m", func() { delivered++ })
		net.Partition("a", "b", time.Hour)
		drain(w)
		if delivered != 1 {
			t.Errorf("delivered %d, want 1: a partition only drops messages sent while it lasts", delivered)
		}
	})

	t.Run("a shorter partition does not shorten a longer one", func(t *testing.T) {
		w := sim.NewWorld(1)
		net := sim.NewNetwork(w, fixedLatency)
		delivered := 0
		net.Partition("a", "b", time.Second)
		net.Partition("b", "a", time.Millisecond)
		advanceTo(t, w, 10*time.Millisecond)
		net.Send("a", "b", false, "m", func() { delivered++ })
		drain(w)
		if delivered != 0 {
			t.Errorf("delivered %d, want 0: the longer partition still holds", delivered)
		}
	})
}

func TestHealStopsRandomFaults(t *testing.T) {
	const sends = 20_000
	w := sim.NewWorld(3)
	net := sim.NewNetwork(w, lossy)
	net.Heal()
	delivered := 0
	for range sends {
		net.Send("a", "b", true, "m", func() { delivered++ })
	}
	drain(w)
	if s := net.Stats(); s.Dropped != 0 || s.Duplicated != 0 {
		t.Errorf("stats after Heal = %+v, want no drops and no duplicates", s)
	}
	if delivered != sends {
		t.Errorf("delivered %d, want %d", delivered, sends)
	}
	// TailP is zero too: every message used the typical range, so the last
	// one landed within it.
	if w.Now > lossy.Latency[1] {
		t.Errorf("virtual time reached %v: a tail latency was used after Heal", w.Now)
	}
}

func TestNewNetworkRejectsBadLatencyRanges(t *testing.T) {
	w := sim.NewWorld(1)
	cases := map[string]sim.NetConfig{
		"inverted typical range": {Latency: [2]time.Duration{5 * time.Millisecond, time.Millisecond}},
		"inverted tail range":    {Tail: [2]time.Duration{time.Second, time.Millisecond}},
		"negative latency":       {Latency: [2]time.Duration{-time.Millisecond, time.Millisecond}},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			msg := mustPanic(t, func() { sim.NewNetwork(w, cfg) })
			if !strings.HasPrefix(msg, "sim: NewNetwork") {
				t.Errorf("panic = %q, want it to name NewNetwork", msg)
			}
		})
	}
}

func TestClockRoundTrip(t *testing.T) {
	for _, offset := range []time.Duration{0, 250 * time.Millisecond, -2 * time.Second} {
		for _, rate := range []float64{1 - 1e-4, 1, 1 + 1e-4} {
			c := sim.Clock{Offset: offset, Rate: rate}
			for _, v := range []time.Duration{0, time.Second, time.Hour} {
				got := c.Virtual(c.Mono(v))
				diff := got - v
				if diff < -time.Microsecond || diff > time.Microsecond {
					t.Errorf("offset %v rate %v: Virtual(Mono(%v)) = %v, off by %v", offset, rate, v, got, diff)
				}
				if got%time.Microsecond != 0 {
					t.Errorf("offset %v rate %v: Virtual(Mono(%v)) = %v, not a whole number of microseconds", offset, rate, v, got)
				}
			}
		}
	}
}

func TestClockMonoAppliesOffsetThenRate(t *testing.T) {
	fast := sim.Clock{Offset: 5 * time.Millisecond, Rate: 1 + 1e-4}
	// An hour of virtual time is 360ms longer on a clock that runs 0.01% fast.
	if got, want := fast.Mono(time.Hour), 5*time.Millisecond+time.Hour+360*time.Millisecond; got != want {
		t.Errorf("fast.Mono(1h) = %v, want %v", got, want)
	}
	slow := sim.Clock{Offset: -time.Second, Rate: 1 - 1e-4}
	if got, want := slow.Mono(time.Hour), -time.Second+time.Hour-360*time.Millisecond; got != want {
		t.Errorf("slow.Mono(1h) = %v, want %v", got, want)
	}
	if got := fast.Mono(0); got != fast.Offset {
		t.Errorf("Mono(0) = %v, want the offset %v", got, fast.Offset)
	}
	// Virtual undoes the drift: 3.6s of mono time past the offset on the fast
	// clock is a little less than 3.6s of virtual time.
	if got, want := fast.Virtual(fast.Offset+3600*time.Millisecond), 3599640*time.Microsecond; got != want {
		t.Errorf("fast.Virtual(offset+3.6s) = %v, want %v", got, want)
	}
}

func TestClockWithNonPositiveRatePanics(t *testing.T) {
	for _, rate := range []float64{0, -1} {
		c := sim.Clock{Rate: rate}
		if msg := mustPanic(t, func() { c.Mono(time.Second) }); !strings.HasPrefix(msg, "sim: Clock.Mono") {
			t.Errorf("rate %v: Mono panic = %q", rate, msg)
		}
		if msg := mustPanic(t, func() { c.Virtual(time.Second) }); !strings.HasPrefix(msg, "sim: Clock.Virtual") {
			t.Errorf("rate %v: Virtual panic = %q", rate, msg)
		}
	}
}

func TestStoreTimeIsMicroseconds(t *testing.T) {
	w := sim.NewWorld(1)
	if got := w.StoreTime(); !got.Equal(sim.Epoch) {
		t.Errorf("StoreTime() at Now=0 = %v, want Epoch %v", got, sim.Epoch)
	}
	if want := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC); !sim.Epoch.Equal(want) {
		t.Errorf("Epoch = %v, want %v", sim.Epoch, want)
	}

	// 1.234567891s: the last three digits are below the store's precision.
	w.At(1234567891*time.Nanosecond, "e", func() {})
	w.Step()
	got := w.StoreTime()
	if want := sim.Epoch.Add(1234567 * time.Microsecond); !got.Equal(want) {
		t.Errorf("StoreTime() = %v, want %v", got, want)
	}
	if got.Nanosecond()%1000 != 0 {
		t.Errorf("StoreTime() has %d ns, want a whole number of microseconds", got.Nanosecond())
	}
	if got.Location() != time.UTC {
		t.Errorf("StoreTime() location = %v, want UTC", got.Location())
	}
}

func TestRandMatchesPCGSeededAsDocumented(t *testing.T) {
	const seed = 424242
	ref := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	r := sim.NewRand(seed)
	for i := range 100 {
		if got, want := r.Float64(), ref.Float64(); got != want {
			t.Fatalf("draw %d: Float64 = %v, want %v", i, got, want)
		}
		if got, want := r.IntN(1000), ref.IntN(1000); got != want {
			t.Fatalf("draw %d: IntN = %v, want %v", i, got, want)
		}
	}
	if got, want := sim.NewWorld(seed).Rand.IntN(1<<30), sim.NewRand(seed).IntN(1<<30); got != want {
		t.Errorf("NewWorld(seed).Rand draws %d, NewRand(seed) draws %d", got, want)
	}
}

func TestRandChance(t *testing.T) {
	r := sim.NewRand(5)
	for range 1000 {
		if r.Chance(0) {
			t.Fatal("Chance(0) = true")
		}
		if !r.Chance(1) {
			t.Fatal("Chance(1) = false")
		}
	}
	hits := 0
	for range 100_000 {
		if r.Chance(0.25) {
			hits++
		}
	}
	// Expected 25000, standard deviation about 137.
	if hits < 24000 || hits > 26000 {
		t.Errorf("Chance(0.25) hit %d of 100000, want within [24000, 26000]", hits)
	}
}

func TestRandBetween(t *testing.T) {
	r := sim.NewRand(6)
	lo, hi := 3*time.Millisecond, 3*time.Millisecond+10*time.Microsecond
	seen := map[time.Duration]bool{}
	for range 10_000 {
		d := r.Between(lo, hi)
		if d < lo || d > hi {
			t.Fatalf("Between(%v, %v) = %v, out of range", lo, hi, d)
		}
		if d%time.Microsecond != 0 {
			t.Fatalf("Between(%v, %v) = %v, not a whole number of microseconds", lo, hi, d)
		}
		seen[d] = true
	}
	// Both ends are reachable: 11 whole-microsecond values in [lo, hi].
	if len(seen) != 11 {
		t.Errorf("saw %d distinct values, want all 11 (ends included)", len(seen))
	}
	if got := r.Between(time.Second, time.Second); got != time.Second {
		t.Errorf("Between(1s, 1s) = %v, want 1s", got)
	}
	if msg := mustPanic(t, func() { r.Between(time.Second, time.Millisecond) }); !strings.HasPrefix(msg, "sim: Rand.Between") {
		t.Errorf("inverted Between panic = %q", msg)
	}
}

func TestRandForkIsReproducibleAndIndependent(t *testing.T) {
	draws := func(r *sim.Rand) []int {
		out := make([]int, 8)
		for i := range out {
			out[i] = r.IntN(1 << 30)
		}
		return out
	}
	p1, p2 := sim.NewRand(9), sim.NewRand(9)
	c1, c2 := p1.Fork(), p2.Fork()
	if a, b := draws(c1), draws(c2); !slices.Equal(a, b) {
		t.Errorf("forks of equal parents differ: %v vs %v", a, b)
	}
	if a, b := draws(p1), draws(p2); !slices.Equal(a, b) {
		t.Errorf("parents differ after forking: %v vs %v", a, b)
	}
	// Two forks of one parent are different streams, and a fork is not a
	// copy of its parent.
	parent := sim.NewRand(9)
	f1, f2 := parent.Fork(), parent.Fork()
	if slices.Equal(draws(f1), draws(f2)) {
		t.Error("two forks of the same parent produce the same stream")
	}
	if slices.Equal(draws(sim.NewRand(9).Fork()), draws(sim.NewRand(9))) {
		t.Error("a fork reproduces its parent's stream")
	}
}
