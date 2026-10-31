package dispatch

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
)

const (
	sec       = time.Second
	testQueue = "jobs"
)

// wt returns a waiter on testQueue that arrived at the given instant and
// waits at most 20s for jobs.
func wt(id WaiterID, arrived Mono) Waiter {
	return Waiter{ID: id, Queue: testQueue, Max: 8, Arrived: arrived, Deadline: arrived + 20*sec}
}

// on returns w moved to another queue.
func on(queue string, w Waiter) Waiter {
	w.Queue = queue
	return w
}

// describe renders one effect for failure messages. The switch lists every
// effect, so adding an effect type fails the lint here as well.
func describe(e Effect) string {
	switch e := e.(type) {
	case Fetch:
		return fmt.Sprintf("Fetch{w%d %s max=%d waited=%s}", e.Waiter, e.Queue, e.Max, e.Waited)
	case Deliver:
		return fmt.Sprintf("Deliver{w%d}", e.Waiter)
	case Respond:
		return fmt.Sprintf("Respond{w%d %s}", e.Waiter, e.Reason)
	case Release:
		return fmt.Sprintf("Release{w%d}", e.Waiter)
	case Arm:
		return fmt.Sprintf("Arm{%s}", e.At)
	}
	return fmt.Sprintf("%T", e)
}

func describeAll(effs []Effect) string {
	parts := make([]string, len(effs))
	for i, e := range effs {
		parts[i] = describe(e)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// wantEffects fails unless effs is exactly want, in order.
func wantEffects(t *testing.T, effs []Effect, want ...Effect) {
	t.Helper()
	if !slices.Equal(effs, want) {
		t.Fatalf("effects = %s, want %s", describeAll(effs), describeAll(want))
	}
}

// fetches returns the Fetch effects in order.
func fetches(effs []Effect) []Fetch {
	var out []Fetch
	for _, e := range effs {
		if f, ok := e.(Fetch); ok {
			out = append(out, f)
		}
	}
	return out
}

// respond returns the reason of the Respond effect for id, if there is one.
func respond(effs []Effect, id WaiterID) (Reason, bool) {
	for _, e := range effs {
		if r, ok := e.(Respond); ok && r.Waiter == id {
			return r.Reason, true
		}
	}
	return 0, false
}

// armAt returns the instant of the Arm effect, which must be the last effect.
func armAt(t *testing.T, effs []Effect) (Mono, bool) {
	t.Helper()
	for i, e := range effs {
		if a, ok := e.(Arm); ok {
			if i != len(effs)-1 {
				t.Fatalf("Arm is not the last effect: %s", describeAll(effs))
			}
			return a.At, true
		}
	}
	return 0, false
}

func wantArm(t *testing.T, effs []Effect, at Mono) {
	t.Helper()
	got, ok := armAt(t, effs)
	if !ok || got != at {
		t.Fatalf("effects = %s, want an Arm at %s", describeAll(effs), at)
	}
}

func wantNoArm(t *testing.T, effs []Effect) {
	t.Helper()
	if at, ok := armAt(t, effs); ok {
		t.Fatalf("unexpected Arm at %s in %s", at, describeAll(effs))
	}
}

func wantCounts(t *testing.T, c *Core, queue string, waiting, inFlight int) {
	t.Helper()
	if w, f := c.Waiting(queue), c.InFlight(queue); w != waiting || f != inFlight {
		t.Fatalf("Waiting, InFlight of %q = %d, %d, want %d, %d", queue, w, f, waiting, inFlight)
	}
}

func mustPanic(t *testing.T, contains string, f func()) {
	t.Helper()
	defer func() {
		t.Helper()
		r := recover()
		if r == nil {
			t.Fatal("did not panic")
		}
		if msg := fmt.Sprint(r); !strings.Contains(msg, contains) {
			t.Fatalf("panic = %q, want it to contain %q", msg, contains)
		}
	}()
	f()
}

// parked returns a core whose testQueue is known empty and has the given
// waiters parked on it, in order, all arrived at 0. The first waiter is
// fetched and answers empty, the others arrive afterwards.
func parked(t *testing.T, cfg Config, ids ...WaiterID) *Core {
	t.Helper()
	c := New(cfg)
	for i, id := range ids {
		effs := c.Arrive(0, wt(id, 0))
		if i == 0 {
			wantEffects(t, effs, Fetch{Waiter: id, Queue: testQueue, Max: 8})
			c.FetchDone(0, id, 0, false, AcrossTenants)
		} else if len(fetches(effs)) != 0 {
			t.Fatalf("waiter %d fetched on a known empty queue", id)
		}
	}
	wantCounts(t, c, testQueue, len(ids), 0)
	return c
}

func TestArriveFetchesImmediately(t *testing.T) {
	c := New(Config{})
	effs := c.Arrive(0, wt(1, 0))
	wantEffects(t, effs, Fetch{Waiter: 1, Queue: testQueue, Max: 8, Waited: 0})
	wantCounts(t, c, testQueue, 0, 1)
}

func TestArriveOnKnownEmptyQueueWaits(t *testing.T) {
	c := New(Config{})
	c.Arrive(0, wt(1, 0))
	effs := c.FetchDone(0, 1, 0, false, AcrossTenants)
	wantEffects(t, effs, Arm{At: sec}) // waiter 1 waits again; the poll is the earliest instant
	effs = c.Arrive(0, wt(2, 0))
	wantEffects(t, effs, Arm{At: sec})
	wantCounts(t, c, testQueue, 2, 0)
}

func TestNotifyWakesOldestWaiter(t *testing.T) {
	c := parked(t, Config{}, 1, 2)
	effs := c.Notify(0, testQueue)
	wantEffects(t, effs, Fetch{Waiter: 1, Queue: testQueue, Max: 8}, Arm{At: sec}) // waiter 2 waits on
	wantCounts(t, c, testQueue, 1, 1)
}

func TestNotifyStartsOneFetchPerNotification(t *testing.T) {
	// Four fetch slots and three parked waiters: each notification still
	// wakes one waiter, the oldest, so wakeups follow the work.
	c := parked(t, Config{MaxInFlight: 4}, 1, 2, 3)
	wantEffects(t, c.Notify(0, testQueue), Fetch{Waiter: 1, Queue: testQueue, Max: 8}, Arm{At: sec})
	wantEffects(t, c.Notify(0, testQueue), Fetch{Waiter: 2, Queue: testQueue, Max: 8}, Arm{At: sec})
	wantEffects(t, c.Notify(0, testQueue), Fetch{Waiter: 3, Queue: testQueue, Max: 8})
	wantEffects(t, c.Notify(0, testQueue)) // nobody is left to wake
	wantCounts(t, c, testQueue, 0, 3)
}

func TestPollStartsOneFetch(t *testing.T) {
	c := parked(t, Config{}, 1, 2, 3)
	effs := c.Tick(sec)
	wantEffects(t, effs, Fetch{Waiter: 1, Queue: testQueue, Max: 8, Waited: sec}, Arm{At: 2 * sec})
	wantCounts(t, c, testQueue, 2, 1)
}

func TestDueWakeStartsOneFetch(t *testing.T) {
	c := parked(t, Config{PollInterval: time.Hour}, 1, 2, 3)
	wantArm(t, c.DueAt(0, testQueue, 5*sec), 5*sec)
	effs := c.Tick(5 * sec)
	wantEffects(t, effs, Fetch{Waiter: 1, Queue: testQueue, Max: 8, Waited: 5 * sec}, Arm{At: 5*sec + time.Hour})
	wantCounts(t, c, testQueue, 2, 1)
}

func TestPollAndDueTogetherStartOneFetch(t *testing.T) {
	c := parked(t, Config{}, 1, 2, 3)
	c.DueAt(0, testQueue, sec)
	effs := c.Tick(sec)
	wantEffects(t, effs, Fetch{Waiter: 1, Queue: testQueue, Max: 8, Waited: sec}, Arm{At: 2 * sec})
}

func TestArriveFetchesForTheOldestParkedWaiter(t *testing.T) {
	// A notification woke waiter 1 and left 2 and 3 parked on an open queue.
	// An arrival gets the next fetch slot in line behind them, not ahead.
	c := parked(t, Config{}, 1, 2, 3)
	c.Notify(0, testQueue)
	effs := c.Arrive(0, wt(4, 0))
	wantEffects(t, effs, Fetch{Waiter: 2, Queue: testQueue, Max: 8}, Arm{At: sec})
	wantCounts(t, c, testQueue, 2, 2)
}

func TestNotifyOnIdleQueueIsHarmless(t *testing.T) {
	c := New(Config{})
	wantEffects(t, c.Notify(0, "nobody-waits-here"))
	if n := len(c.queues); n != 0 {
		t.Fatalf("Notify created state for %d queues, want 0", n)
	}
	// The queue still fetches at once when its first waiter arrives.
	wantEffects(t, c.Arrive(0, wt(1, 0)), Fetch{Waiter: 1, Queue: testQueue, Max: 8})
}

func TestFullFetchChainsToNextWaiter(t *testing.T) {
	c := New(Config{MaxInFlight: 1})
	c.Arrive(0, wt(1, 0))
	if effs := c.Arrive(0, wt(2, 0)); len(fetches(effs)) != 0 {
		t.Fatalf("waiter 2 fetched while the only slot was busy: %s", describeAll(effs))
	}
	effs := c.FetchDone(sec, 1, 8, false, AcrossTenants)
	wantEffects(t, effs, Deliver{Waiter: 1}, Fetch{Waiter: 2, Queue: testQueue, Max: 8, Waited: sec})
	wantCounts(t, c, testQueue, 0, 1)
}

func TestPartialFetchStopsChain(t *testing.T) {
	c := New(Config{MaxInFlight: 1})
	c.Arrive(0, wt(1, 0))
	c.Arrive(0, wt(2, 0))
	effs := c.FetchDone(0, 1, 3, false, AcrossTenants)
	wantEffects(t, effs, Deliver{Waiter: 1}, Arm{At: sec})
	wantCounts(t, c, testQueue, 1, 0)
}

func TestFullBatchChainsExactlyOneWaiter(t *testing.T) {
	c := parked(t, Config{}, 1, 2, 3, 4)
	c.Notify(0, testQueue) // fetches 1
	c.Notify(0, testQueue) // fetches 2
	effs := c.FetchDone(0, 1, 8, false, AcrossTenants)
	wantEffects(t, effs, Deliver{Waiter: 1}, Fetch{Waiter: 3, Queue: testQueue, Max: 8}, Arm{At: sec})
	effs = c.FetchDone(0, 3, 8, false, AcrossTenants)
	wantEffects(t, effs, Deliver{Waiter: 3}, Fetch{Waiter: 4, Queue: testQueue, Max: 8})
}

func TestFullBatchReopensQueueClosedByPartialOne(t *testing.T) {
	// Two fetches are out. The partial answer closes the queue, so no waiter
	// is chained. The full answer that follows reopens it and chains one.
	c := parked(t, Config{}, 1, 2, 3, 4)
	c.Notify(0, testQueue) // fetches 1
	c.Notify(0, testQueue) // fetches 2
	effs := c.FetchDone(0, 2, 1, false, AcrossTenants)
	wantEffects(t, effs, Deliver{Waiter: 2}, Arm{At: sec})
	effs = c.FetchDone(0, 1, 8, false, AcrossTenants)
	wantEffects(t, effs, Deliver{Waiter: 1}, Fetch{Waiter: 3, Queue: testQueue, Max: 8}, Arm{At: sec})
	wantCounts(t, c, testQueue, 1, 1)
}

func TestShortFetchClosesTheQueueOnlyIfItAskedAcrossTenants(t *testing.T) {
	// The same short answer, 3 jobs of the 8 asked for, with a second waiter
	// parked behind the only fetch slot. Across tenants it is the end of the
	// queue. Per tenant it is only the end of the tenants that came up short.
	t.Run("across tenants", func(t *testing.T) {
		c := New(Config{MaxInFlight: 1})
		c.Arrive(0, wt(1, 0))
		c.Arrive(0, wt(2, 0))
		wantEffects(t, c.FetchDone(0, 1, 3, false, AcrossTenants), Deliver{Waiter: 1}, Arm{At: sec})
		wantCounts(t, c, testQueue, 1, 0)
		// The queue is known empty: a new waiter parks without a fetch.
		wantEffects(t, c.Arrive(0, wt(3, 0)), Arm{At: sec})
		wantCounts(t, c, testQueue, 2, 0)
	})
	t.Run("per tenant", func(t *testing.T) {
		c := New(Config{MaxInFlight: 1})
		c.Arrive(0, wt(1, 0))
		c.Arrive(0, wt(2, 0))
		wantEffects(t, c.FetchDone(0, 1, 3, false, PerTenant),
			Deliver{Waiter: 1}, Fetch{Waiter: 2, Queue: testQueue, Max: 8})
		wantCounts(t, c, testQueue, 0, 1)
		// The queue is open: a new waiter takes the next free slot, in line.
		wantEffects(t, c.FetchDone(0, 2, 8, false, PerTenant), Deliver{Waiter: 2})
		wantEffects(t, c.Arrive(0, wt(3, 0)), Fetch{Waiter: 3, Queue: testQueue, Max: 8})
	})
}

func TestEmptyPerTenantFetchTriesAgainUntilOneAcrossTenantsLooks(t *testing.T) {
	// Every tenant of a per-tenant fetch came up empty. That proves nothing
	// about the tenants the fetch did not name, so the waiter, still within
	// its deadline, is fetched for again. The shell's ring is empty by now, so
	// the next fetch looks across tenants, and only its empty answer closes
	// the queue: the chain ends there.
	c := New(Config{})
	c.Arrive(0, wt(1, 0))
	wantEffects(t, c.FetchDone(sec, 1, 0, false, PerTenant),
		Fetch{Waiter: 1, Queue: testQueue, Max: 8, Waited: sec})
	wantCounts(t, c, testQueue, 0, 1)
	wantEffects(t, c.FetchDone(sec, 1, 0, false, AcrossTenants), Arm{At: 2 * sec})
	wantCounts(t, c, testQueue, 1, 0)
}

func TestShortPerTenantFetchPastTheDeadlineTimesOutAndStillChains(t *testing.T) {
	// An empty answer ends a waiter whose deadline has passed, as always. The
	// fetch it leaves open still serves the next waiter.
	c := New(Config{MaxInFlight: 1})
	c.Arrive(0, wt(1, 0))
	c.Arrive(0, Waiter{ID: 2, Queue: testQueue, Max: 8, Arrived: 0, Deadline: time.Hour})
	wantEffects(t, c.FetchDone(20*sec, 1, 0, false, PerTenant),
		Respond{Waiter: 1, Reason: ReasonTimeout}, Fetch{Waiter: 2, Queue: testQueue, Max: 8, Waited: 20 * sec})
}

func TestFailedFetchIgnoresTheScope(t *testing.T) {
	// A failure says nothing about the tenants either way: nothing chains,
	// whatever the fetch asked for. The poll serves the next waiter.
	for _, scope := range []FetchScope{AcrossTenants, PerTenant} {
		c := New(Config{MaxInFlight: 1})
		c.Arrive(0, wt(1, 0))
		c.Arrive(0, wt(2, 0))
		wantEffects(t, c.FetchDone(0, 1, 0, true, scope), Respond{Waiter: 1, Reason: ReasonUnavailable}, Arm{At: sec})
		wantCounts(t, c, testQueue, 1, 0)
	}
}

func TestFetchDoneRejectsAnUnknownScope(t *testing.T) {
	c := New(Config{})
	c.Arrive(0, wt(1, 0))
	mustPanic(t, "FetchDone: unknown scope 0", func() { c.FetchDone(0, 1, 0, false, 0) })
	mustPanic(t, "FetchDone: unknown scope 3", func() { c.FetchDone(0, 1, 0, false, PerTenant+1) })
	wantCounts(t, c, testQueue, 0, 1) // the refused calls changed nothing
}

func TestFailedFetchDoesNotChainAndThePollServesTheNextWaiter(t *testing.T) {
	c := New(Config{MaxInFlight: 1})
	c.Arrive(0, wt(1, 0))
	c.Arrive(0, wt(2, 0))
	effs := c.FetchDone(0, 1, 0, true, AcrossTenants)
	wantEffects(t, effs, Respond{Waiter: 1, Reason: ReasonUnavailable}, Arm{At: sec})

	// Waiter 2 was never fetched for. The poll gives it its fetch.
	effs = c.Tick(sec)
	wantEffects(t, effs, Fetch{Waiter: 2, Queue: testQueue, Max: 8, Waited: sec})
}

func TestFailedFetchLetsParkedWaitersTimeOut(t *testing.T) {
	// A failed fetch does not close the queue. The waiters parked behind it
	// have had the store's chance all the same, so they time out on schedule
	// instead of waiting for one poll per waiter while the store is down.
	c := New(Config{MaxInFlight: 1, PollInterval: time.Hour})
	c.Arrive(0, wt(1, 0))
	w := wt(2, 0)
	w.Deadline = 5 * sec
	wantEffects(t, c.Arrive(0, w))
	wantEffects(t, c.Tick(5*sec)) // still waiting for its turn at the slot

	wantEffects(t, c.FetchDone(5*sec, 1, 0, true, AcrossTenants), Respond{Waiter: 1, Reason: ReasonUnavailable}, Arm{At: 5 * sec})
	wantEffects(t, c.Tick(5*sec), Respond{Waiter: 2, Reason: ReasonTimeout})
}

func TestNeverFetchedWaiterIsNotTimedOutByTick(t *testing.T) {
	// The only fetch slot is busy, so a waiter that asked for no wait is
	// parked without a fetch. Tick must not tell it "nothing" before a fetch
	// has looked for it: it gets its one fetch when the slot frees.
	c := New(Config{MaxInFlight: 1})
	wantEffects(t, c.Arrive(0, wt(1, 0)), Fetch{Waiter: 1, Queue: testQueue, Max: 8})
	w := wt(2, 0)
	w.Deadline = w.Arrived
	wantEffects(t, c.Arrive(0, w)) // parked: nothing to fetch with, nothing to arm
	wantEffects(t, c.Tick(5*sec))  // far past its deadline: still not answered

	effs := c.FetchDone(5*sec, 1, 8, false, AcrossTenants)
	wantEffects(t, effs, Deliver{Waiter: 1}, Fetch{Waiter: 2, Queue: testQueue, Max: 8, Waited: 5 * sec})
	wantEffects(t, c.FetchDone(5*sec, 2, 0, false, AcrossTenants), Respond{Waiter: 2, Reason: ReasonTimeout})
	wantCounts(t, c, testQueue, 0, 0)
}

func TestFetchedWaiterTimesOutWhileAnotherFetchHasTheQueueOpen(t *testing.T) {
	// Waiters 1 and 2 have both been fetched for and wait on a queue known to
	// be empty. A poll opens the queue with a fetch for waiter 1. Waiter 2
	// has been looked for, so its deadline still counts.
	c := New(Config{})
	c.Arrive(0, wt(1, 0))
	c.FetchDone(0, 1, 0, false, AcrossTenants)
	c.Notify(0, testQueue) // fetches 1
	w := wt(2, 0)
	w.Deadline = 1500 * time.Millisecond
	wantEffects(t, c.Arrive(0, w), Fetch{Waiter: 2, Queue: testQueue, Max: 8})
	c.FetchDone(0, 1, 0, false, AcrossTenants)
	c.FetchDone(0, 2, 0, false, AcrossTenants)

	effs := c.Tick(sec)
	wantEffects(t, effs, Fetch{Waiter: 1, Queue: testQueue, Max: 8, Waited: sec}, Arm{At: 1500 * time.Millisecond})
	wantEffects(t, c.Tick(1500*time.Millisecond), Respond{Waiter: 2, Reason: ReasonTimeout})
}

func TestParkedWaiterIsTimedOutOnceTheQueueIsKnownEmpty(t *testing.T) {
	// A waiter parked behind a busy slot is covered as soon as the queue
	// is known to be empty: that is what a waiter arriving at such a queue
	// is told at once.
	c := New(Config{MaxInFlight: 1, PollInterval: time.Hour})
	c.Arrive(0, wt(1, 0))
	w := wt(2, 0)
	w.Deadline = 5 * sec
	wantEffects(t, c.Arrive(0, w))
	wantEffects(t, c.Tick(5*sec)) // the queue is open: not answered

	wantArm(t, c.FetchDone(5*sec, 1, 0, false, AcrossTenants), 5*sec)
	wantEffects(t, c.Tick(5*sec), Respond{Waiter: 2, Reason: ReasonTimeout}, Arm{At: 20 * sec})
}

func TestInFlightBounded(t *testing.T) {
	c := New(Config{})
	var n int
	for id := WaiterID(1); id <= 10; id++ {
		n += len(fetches(c.Arrive(0, wt(id, 0))))
	}
	if n != 4 {
		t.Fatalf("10 arrivals started %d fetches, want 4", n)
	}
	wantCounts(t, c, testQueue, 6, 4)
}

func TestInFlightBoundIsConfigurable(t *testing.T) {
	c := New(Config{MaxInFlight: 2})
	var n int
	for id := WaiterID(1); id <= 5; id++ {
		n += len(fetches(c.Arrive(0, wt(id, 0))))
	}
	if n != 2 {
		t.Fatalf("5 arrivals started %d fetches, want 2", n)
	}
}

func TestConfigDefaults(t *testing.T) {
	for _, cfg := range []Config{{}, {MaxInFlight: -1, PollInterval: -time.Second}} {
		c := New(cfg)
		if c.cfg.MaxInFlight != 4 || c.cfg.PollInterval != sec {
			t.Fatalf("New(%+v) uses %+v, want 4 and 1s", cfg, c.cfg)
		}
	}
	c := New(Config{MaxInFlight: 7, PollInterval: 3 * sec})
	if c.cfg.MaxInFlight != 7 || c.cfg.PollInterval != 3*sec {
		t.Fatalf("explicit config changed: %+v", c.cfg)
	}
}

func TestNotifyDuringFetchMarksDirty(t *testing.T) {
	c := New(Config{})
	c.Arrive(0, wt(1, 0))
	wantEffects(t, c.Notify(0, testQueue)) // nobody is waiting: nothing to wake
	effs := c.FetchDone(0, 1, 0, false, AcrossTenants)
	wantEffects(t, effs, Fetch{Waiter: 1, Queue: testQueue, Max: 8}) // the empty answer may predate the job

	// Without the notification the same answer parks the waiter.
	c = New(Config{})
	c.Arrive(0, wt(1, 0))
	wantEffects(t, c.FetchDone(0, 1, 0, false, AcrossTenants), Arm{At: sec})
}

func TestRefetchAfterNotificationLeavesQueueOpen(t *testing.T) {
	// The empty answer was outdated by a notification, so the queue is not
	// known to be empty: a waiter that arrives next fetches at once.
	c := New(Config{})
	c.Arrive(0, wt(1, 0))
	c.Notify(0, testQueue)
	wantEffects(t, c.FetchDone(0, 1, 0, false, AcrossTenants), Fetch{Waiter: 1, Queue: testQueue, Max: 8})
	wantEffects(t, c.Arrive(0, wt(2, 0)), Fetch{Waiter: 2, Queue: testQueue, Max: 8})

	// Once an answer comes back empty with no notification since, it closes.
	c.FetchDone(0, 1, 0, false, AcrossTenants)
	c.FetchDone(0, 2, 0, false, AcrossTenants)
	wantEffects(t, c.Arrive(0, wt(3, 0)), Arm{At: sec})
}

func TestTimeoutRespondsAtDeadline(t *testing.T) {
	c := New(Config{PollInterval: time.Hour})
	c.Arrive(0, wt(1, 0))
	wantArm(t, c.FetchDone(0, 1, 0, false, AcrossTenants), 20*sec)

	effs := c.Tick(20*sec - time.Microsecond)
	if _, ok := respond(effs, 1); ok {
		t.Fatalf("responded before the deadline: %s", describeAll(effs))
	}
	wantArm(t, effs, 20*sec)

	effs = c.Tick(20 * sec)
	wantEffects(t, effs, Respond{Waiter: 1, Reason: ReasonTimeout})
	wantCounts(t, c, testQueue, 0, 0)
}

func TestTimeoutAnswersExpiredWaitersInArrivalOrder(t *testing.T) {
	c := New(Config{PollInterval: time.Hour})
	c.Arrive(0, wt(1, 0))
	c.FetchDone(0, 1, 0, false, AcrossTenants)
	for _, id := range []WaiterID{2, 3} {
		w := wt(id, 0)
		w.Deadline = 5 * sec
		c.Arrive(0, w)
	}
	effs := c.Tick(5 * sec)
	wantEffects(t, effs,
		Respond{Waiter: 2, Reason: ReasonTimeout},
		Respond{Waiter: 3, Reason: ReasonTimeout},
		Arm{At: 20 * sec},
	)
	wantCounts(t, c, testQueue, 1, 0)
}

func TestFetchDoneAfterDeadlineRespondsTimeout(t *testing.T) {
	c := New(Config{})
	w := wt(1, 0)
	w.Deadline = 3 * sec
	c.Arrive(0, w)
	effs := c.FetchDone(3*sec, 1, 0, false, AcrossTenants)
	wantEffects(t, effs, Respond{Waiter: 1, Reason: ReasonTimeout})
	// One instant earlier the same answer parks the waiter again.
	c = New(Config{})
	c.Arrive(0, w)
	effs = c.FetchDone(3*sec-time.Microsecond, 1, 0, false, AcrossTenants)
	if _, ok := respond(effs, 1); ok {
		t.Fatalf("responded before the deadline: %s", describeAll(effs))
	}
	wantCounts(t, c, testQueue, 1, 0)
}

func TestFetchDoneWithJobsAfterDeadlineStillDelivers(t *testing.T) {
	c := New(Config{})
	w := wt(1, 0)
	w.Deadline = 3 * sec
	c.Arrive(0, w)
	wantEffects(t, c.FetchDone(9*sec, 1, 2, false, AcrossTenants), Deliver{Waiter: 1})
}

func TestWaitZeroFetchesOnceThenResponds(t *testing.T) {
	c := New(Config{})
	w := wt(1, 5*sec)
	w.Deadline = w.Arrived
	wantEffects(t, c.Arrive(5*sec, w), Fetch{Waiter: 1, Queue: testQueue, Max: 8})
	wantEffects(t, c.FetchDone(5*sec, 1, 0, false, AcrossTenants), Respond{Waiter: 1, Reason: ReasonTimeout})
	wantCounts(t, c, testQueue, 0, 0)
}

func TestWaitZeroOnKnownEmptyQueueRespondsAtOnce(t *testing.T) {
	c := parked(t, Config{}, 1)
	w := wt(2, 0)
	w.Deadline = w.Arrived
	effs := c.Arrive(0, w)
	wantEffects(t, effs, Respond{Waiter: 2, Reason: ReasonTimeout}, Arm{At: sec})
	wantCounts(t, c, testQueue, 1, 0)
}

func TestWaitZeroArriveSeesAKnownEmptyQueueGoneStale(t *testing.T) {
	// Knowledge that the queue is empty is refreshed by a notification and by
	// Tick, and Tick runs only while a waiter is parked. A worker that
	// arrives after the queue went quiet must not be answered from knowledge
	// that a poll or a due time would have refreshed.
	for _, tc := range []struct {
		name     string
		poll     time.Duration
		dueAt    Mono
		tick     bool // the waiter times out in a Tick; otherwise it is cancelled
		arriveAt Mono
		want     []Effect
	}{
		{
			name: "due time passed", poll: time.Hour, dueAt: 5 * sec, tick: true, arriveAt: 10 * sec,
			want: []Effect{Fetch{Waiter: 2, Queue: testQueue, Max: 8}},
		},
		{
			name: "poll interval passed after a lost notification", poll: sec, arriveAt: 60 * sec,
			want: []Effect{Fetch{Waiter: 2, Queue: testQueue, Max: 8}},
		},
		{
			name: "due time still ahead", poll: time.Hour, dueAt: 20 * sec, tick: true, arriveAt: 10 * sec,
			want: []Effect{Respond{Waiter: 2, Reason: ReasonTimeout}},
		},
		{
			name: "knowledge still fresh", poll: sec, arriveAt: sec - time.Microsecond,
			want: []Effect{Respond{Waiter: 2, Reason: ReasonTimeout}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := New(Config{PollInterval: tc.poll})
			w := wt(1, 0)
			w.Deadline = sec / 2
			c.Arrive(0, w)
			c.FetchDone(0, 1, 0, false, AcrossTenants) // the queue is known empty
			if tc.dueAt != 0 {
				c.DueAt(0, testQueue, tc.dueAt)
			}
			if tc.tick {
				// The waiter times out and nothing is armed any more.
				wantEffects(t, c.Tick(sec), Respond{Waiter: 1, Reason: ReasonTimeout})
			} else {
				wantEffects(t, c.Cancel(0, 1)) // no Tick: the queue state is kept as it is
			}

			zero := wt(2, tc.arriveAt)
			zero.Deadline = zero.Arrived
			wantEffects(t, c.Arrive(tc.arriveAt, zero), tc.want...)
		})
	}
}

func TestWaitZeroDeliversWhatTheFetchFound(t *testing.T) {
	c := New(Config{})
	w := wt(1, 0)
	w.Deadline = w.Arrived
	c.Arrive(0, w)
	wantEffects(t, c.FetchDone(0, 1, 1, false, AcrossTenants), Deliver{Waiter: 1})
}

func TestPollRefetchesAfterInterval(t *testing.T) {
	c := New(Config{})
	c.Arrive(5*sec, wt(1, 5*sec))
	c.FetchDone(5*sec, 1, 0, false, AcrossTenants)

	effs := c.Tick(5*sec + 999*time.Millisecond)
	if len(fetches(effs)) != 0 {
		t.Fatalf("polled early: %s", describeAll(effs))
	}
	effs = c.Tick(6 * sec)
	wantEffects(t, effs, Fetch{Waiter: 1, Queue: testQueue, Max: 8, Waited: sec})

	// The poll restarts the interval.
	c.FetchDone(6*sec, 1, 0, false, AcrossTenants)
	if effs := c.Tick(6*sec + 999*time.Millisecond); len(fetches(effs)) != 0 {
		t.Fatalf("polled early after a poll: %s", describeAll(effs))
	}
	if effs := c.Tick(7 * sec); len(fetches(effs)) != 1 {
		t.Fatalf("did not poll after the interval: %s", describeAll(effs))
	}
}

func TestPollIntervalIsConfigurable(t *testing.T) {
	c := New(Config{PollInterval: 250 * time.Millisecond})
	c.Arrive(0, wt(1, 0))
	c.FetchDone(0, 1, 0, false, AcrossTenants)
	if effs := c.Tick(249 * time.Millisecond); len(fetches(effs)) != 0 {
		t.Fatalf("polled early: %s", describeAll(effs))
	}
	if effs := c.Tick(250 * time.Millisecond); len(fetches(effs)) != 1 {
		t.Fatalf("did not poll: %s", describeAll(effs))
	}
}

func TestDueAtWakes(t *testing.T) {
	for _, tc := range []struct {
		poll time.Duration
		arm  Mono
	}{
		{poll: sec, arm: sec},          // the poll comes first
		{poll: 10 * sec, arm: 5 * sec}, // the due time comes first
	} {
		c := New(Config{PollInterval: tc.poll})
		c.Arrive(0, wt(1, 0))
		c.FetchDone(0, 1, 0, false, AcrossTenants)
		wantArm(t, c.DueAt(0, testQueue, 5*sec), tc.arm)
	}

	// With the poll out of the way, only the due time wakes the waiter.
	c := New(Config{PollInterval: time.Hour})
	c.Arrive(0, wt(1, 0))
	c.FetchDone(0, 1, 0, false, AcrossTenants)
	wantEffects(t, c.DueAt(0, testQueue, 5*sec), Arm{At: 5 * sec})
	if effs := c.Tick(5*sec - time.Microsecond); len(fetches(effs)) != 0 {
		t.Fatalf("woke before the due time: %s", describeAll(effs))
	}
	effs := c.Tick(5 * sec)
	wantEffects(t, effs, Fetch{Waiter: 1, Queue: testQueue, Max: 8, Waited: 5 * sec})

	// The due time is spent: the next answer parks the waiter until its deadline.
	wantEffects(t, c.FetchDone(5*sec, 1, 0, false, AcrossTenants), Arm{At: 20 * sec})
}

func TestDueAtReplacesTheEarlierValue(t *testing.T) {
	c := New(Config{PollInterval: time.Hour})
	c.Arrive(0, wt(1, 0))
	c.FetchDone(0, 1, 0, false, AcrossTenants)
	wantArm(t, c.DueAt(0, testQueue, 5*sec), 5*sec)
	wantArm(t, c.DueAt(0, testQueue, 9*sec), 9*sec) // later, and it replaces
	wantArm(t, c.DueAt(0, testQueue, 2*sec), 2*sec)
	wantArm(t, c.DueAt(0, testQueue, 0), 20*sec) // zero clears
}

func TestDueAtInThePastWakesAtTheNextTick(t *testing.T) {
	c := New(Config{PollInterval: time.Hour})
	c.Arrive(sec, wt(1, sec))
	c.FetchDone(sec, 1, 0, false, AcrossTenants)
	wantEffects(t, c.DueAt(2*sec, testQueue, sec), Arm{At: sec})
	wantEffects(t, c.Tick(2*sec), Fetch{Waiter: 1, Queue: testQueue, Max: 8, Waited: sec})
}

func TestWakeWithNoFreeSlotMarksTheQueueDirty(t *testing.T) {
	// A due time or a poll that finds every fetch slot busy is not lost: the
	// fetch that comes back chains the next waiter instead of closing the
	// queue on an answer that may predate the wake.
	for _, tc := range []struct {
		name  string
		poll  time.Duration
		dueAt Mono
		tick  Mono
	}{
		{name: "due time", poll: time.Hour, dueAt: 2 * sec, tick: 2 * sec},
		{name: "poll", poll: sec, tick: 2 * sec},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := New(Config{MaxInFlight: 1, PollInterval: tc.poll})
			first := wt(1, 0)
			first.Deadline = 3 * sec
			c.Arrive(0, first)
			c.Arrive(0, wt(2, 0)) // parked behind the busy slot
			if tc.dueAt != 0 {
				c.DueAt(0, testQueue, tc.dueAt)
			}
			wantEffects(t, c.Tick(tc.tick)) // woken, but no slot: nothing to start

			effs := c.FetchDone(3*sec, 1, 0, false, AcrossTenants)
			wantEffects(t, effs,
				Respond{Waiter: 1, Reason: ReasonTimeout},
				Fetch{Waiter: 2, Queue: testQueue, Max: 8, Waited: 3 * sec},
			)
		})
	}

	// Without the wake the empty answer closes the queue, and waiter 2 waits.
	c := New(Config{MaxInFlight: 1, PollInterval: time.Hour})
	first := wt(1, 0)
	first.Deadline = 3 * sec
	c.Arrive(0, first)
	c.Arrive(0, wt(2, 0))
	effs := c.FetchDone(3*sec, 1, 0, false, AcrossTenants)
	if len(fetches(effs)) != 0 {
		t.Fatalf("fetched without a wake: %s", describeAll(effs))
	}
}

func TestCancelWhileWaitingRemoves(t *testing.T) {
	c := parked(t, Config{PollInterval: time.Hour}, 1, 2)
	wantEffects(t, c.Cancel(0, 1), Arm{At: 20 * sec}) // waiter 2 remains
	wantCounts(t, c, testQueue, 1, 0)
	wantEffects(t, c.Cancel(0, 2))
	wantCounts(t, c, testQueue, 0, 0)

	// A cancelled waiter is never answered, whatever happens afterwards.
	effs := c.Notify(0, testQueue)
	wantEffects(t, effs)
	if _, ok := respond(c.Tick(30*sec), 1); ok {
		t.Fatal("a cancelled waiter was answered")
	}
}

func TestCancelDuringFetchReleases(t *testing.T) {
	c := New(Config{})
	c.Arrive(0, wt(1, 0))
	c.Arrive(0, wt(2, 0))
	wantEffects(t, c.Cancel(0, 1))
	wantEffects(t, c.Cancel(0, 2))
	wantCounts(t, c, testQueue, 0, 2) // the fetches are still outstanding

	wantEffects(t, c.FetchDone(0, 1, 3, false, AcrossTenants), Release{Waiter: 1})
	wantCounts(t, c, testQueue, 0, 1)
	wantEffects(t, c.FetchDone(0, 2, 0, false, AcrossTenants)) // nothing was fetched: nothing to release
	wantCounts(t, c, testQueue, 0, 0)

	// Both waiters are forgotten.
	wantEffects(t, c.Cancel(0, 1))
	wantEffects(t, c.FetchDone(0, 1, 3, false, AcrossTenants))
	wantCounts(t, c, testQueue, 0, 0)
}

func TestCancelledFetchStillChainsOnFullBatch(t *testing.T) {
	// The cancelled waiter's fetch still tells the queue what it holds: a
	// full batch for a vanished client chains the next waiter.
	c := New(Config{MaxInFlight: 1})
	c.Arrive(0, wt(1, 0))
	c.Arrive(0, wt(2, 0))
	c.Cancel(0, 1)
	effs := c.FetchDone(0, 1, 8, false, AcrossTenants)
	wantEffects(t, effs, Release{Waiter: 1}, Fetch{Waiter: 2, Queue: testQueue, Max: 8})
}

func TestCancelAfterAnswerIsIgnored(t *testing.T) {
	c := New(Config{})
	c.Arrive(0, wt(1, 0))
	wantEffects(t, c.FetchDone(0, 1, 2, false, AcrossTenants), Deliver{Waiter: 1})
	wantEffects(t, c.Cancel(0, 1))
	wantEffects(t, c.Cancel(0, 99))
}

func TestFetchFailureRespondsUnavailable(t *testing.T) {
	c := New(Config{})
	c.Arrive(0, wt(1, 0))
	effs := c.FetchDone(0, 1, 0, true, AcrossTenants)
	wantEffects(t, effs, Respond{Waiter: 1, Reason: ReasonUnavailable})
	wantCounts(t, c, testQueue, 0, 0)

	// A failure says nothing about the queue: the next waiter fetches at once.
	wantEffects(t, c.Arrive(0, wt(2, 0)), Fetch{Waiter: 2, Queue: testQueue, Max: 8})
}

func TestFetchFailureReleasesGrantsOfACancelledWaiter(t *testing.T) {
	// A failure report that still lists jobs must not leave them to expire.
	c := New(Config{})
	c.Arrive(0, wt(1, 0))
	wantEffects(t, c.FetchDone(0, 1, 5, true, AcrossTenants), Respond{Waiter: 1, Reason: ReasonUnavailable})

	c.Arrive(0, wt(2, 0))
	c.Cancel(0, 2)
	wantEffects(t, c.FetchDone(0, 2, 5, true, AcrossTenants), Release{Waiter: 2})

	// A failure with nothing fetched releases nothing.
	c.Arrive(0, wt(3, 0))
	c.Cancel(0, 3)
	wantEffects(t, c.FetchDone(0, 3, 0, true, AcrossTenants))
}

func TestFetchFailureKeepsKnownEmpty(t *testing.T) {
	c := New(Config{MaxInFlight: 2, PollInterval: time.Hour})
	c.Arrive(0, wt(1, 0))
	c.Arrive(0, wt(2, 0))
	c.FetchDone(0, 1, 0, false, AcrossTenants) // the queue is known empty; waiter 1 waits
	c.Arrive(0, wt(3, 0))
	wantCounts(t, c, testQueue, 2, 1)

	// Waiter 2's fetch fails. That says nothing new about the queue, so the
	// waiters keep waiting.
	effs := c.FetchDone(0, 2, 0, true, AcrossTenants)
	wantEffects(t, effs, Respond{Waiter: 2, Reason: ReasonUnavailable}, Arm{At: 20 * sec})
}

func TestDrainAnswersEveryWaiter(t *testing.T) {
	// B (waiter 1) is fetching and A (waiter 2) is waiting.
	c := parked(t, Config{MaxInFlight: 1}, 1, 2)
	c.Notify(0, testQueue)
	wantCounts(t, c, testQueue, 1, 1)

	wantEffects(t, c.Drain(sec), Respond{Waiter: 2, Reason: ReasonDraining})
	wantEffects(t, c.Arrive(sec, wt(3, sec)), Respond{Waiter: 3, Reason: ReasonRejected})

	// The grants of the fetch in flight are valid, so they are delivered.
	wantEffects(t, c.FetchDone(sec, 1, 2, false, AcrossTenants), Deliver{Waiter: 1})
	wantCounts(t, c, testQueue, 0, 0)
	wantEffects(t, c.Notify(sec, testQueue))
	wantEffects(t, c.Tick(30*sec))
}

func TestDrainThenEmptyFetchRespondsDraining(t *testing.T) {
	c := New(Config{})
	c.Arrive(0, wt(1, 0))
	c.Drain(0)
	wantEffects(t, c.FetchDone(0, 1, 0, false, AcrossTenants), Respond{Waiter: 1, Reason: ReasonDraining})
	wantCounts(t, c, testQueue, 0, 0)
}

func TestDrainThenFailedFetchRespondsUnavailable(t *testing.T) {
	c := New(Config{})
	c.Arrive(0, wt(1, 0))
	c.Drain(0)
	wantEffects(t, c.FetchDone(0, 1, 0, true, AcrossTenants), Respond{Waiter: 1, Reason: ReasonUnavailable})
}

func TestDrainReleasesCancelledFetches(t *testing.T) {
	c := New(Config{})
	c.Arrive(0, wt(1, 0))
	c.Cancel(0, 1)
	c.Drain(0)
	wantEffects(t, c.FetchDone(0, 1, 2, false, AcrossTenants), Release{Waiter: 1})
}

func TestDrainIsIdempotent(t *testing.T) {
	c := parked(t, Config{}, 1)
	if effs := c.Drain(0); len(effs) != 1 {
		t.Fatalf("first Drain = %s, want one Respond", describeAll(effs))
	}
	wantEffects(t, c.Drain(0))
}

func TestFetchWaitedIsSinceArrival(t *testing.T) {
	c := New(Config{})
	c.Arrive(sec, wt(1, sec))
	c.FetchDone(sec, 1, 0, false, AcrossTenants)
	effs := c.Notify(4*sec, testQueue)
	wantEffects(t, effs, Fetch{Waiter: 1, Queue: testQueue, Max: 8, Waited: 3 * sec})
}

func TestArmIsEarliestPendingInstant(t *testing.T) {
	c := New(Config{PollInterval: 10 * sec})
	wantNoArm(t, c.Notify(0, "a"))

	w1 := on("a", wt(1, 0))
	w1.Deadline = 30 * sec
	c.Arrive(0, w1)
	wantArm(t, c.FetchDone(0, 1, 0, false, AcrossTenants), 10*sec) // the poll of "a"

	w2 := on("b", wt(2, 2*sec))
	w2.Deadline = 6 * sec
	c.Arrive(2*sec, w2)
	wantArm(t, c.FetchDone(2*sec, 2, 0, false, AcrossTenants), 6*sec) // the deadline of waiter 2

	wantArm(t, c.DueAt(2*sec, "a", 3*sec), 3*sec) // a due time of "a"

	wantArm(t, c.DueAt(2*sec, "c", sec), 3*sec) // no waiter on "c": it adds nothing to arm
	wantArm(t, c.Cancel(2*sec, 2), 3*sec)
	wantArm(t, c.DueAt(2*sec, "a", 0), 10*sec) // back to the poll of "a"
	wantNoArm(t, c.Cancel(2*sec, 1))
}

func TestArmIgnoresFetchingWaiters(t *testing.T) {
	c := New(Config{})
	wantEffects(t, c.Arrive(0, wt(1, 0)), Fetch{Waiter: 1, Queue: testQueue, Max: 8})
	wantEffects(t, c.DueAt(0, testQueue, sec))
}

func TestArmDoesNotSpinWhileFetchesSaturate(t *testing.T) {
	c := New(Config{MaxInFlight: 1})
	c.Arrive(0, wt(1, 0))
	wantEffects(t, c.Arrive(0, wt(2, 0))) // the poll could not start a fetch: nothing to arm

	// A Tick far past the poll interval cannot start a fetch either, and it
	// leaves nothing armed.
	wantEffects(t, c.Tick(5*sec))

	// The wake was not lost: the fetch that comes back chains the next waiter.
	effs := c.FetchDone(5*sec, 1, 0, false, AcrossTenants)
	wantEffects(t, effs, Fetch{Waiter: 1, Queue: testQueue, Max: 8, Waited: 5 * sec})

	// With no Tick in between, the empty answer closes the queue, and once
	// the slot is free the overdue poll is due at once.
	c = New(Config{MaxInFlight: 1})
	c.Arrive(0, wt(1, 0))
	c.Arrive(0, wt(2, 0))
	wantEffects(t, c.FetchDone(5*sec, 1, 0, false, AcrossTenants), Arm{At: sec})
	wantEffects(t, c.Tick(5*sec), Fetch{Waiter: 1, Queue: testQueue, Max: 8, Waited: 5 * sec})
}

// parkedOnQueues returns a core with four waiters parked on three queues, in
// arrival order: 1 on "b", 2 on "a", 3 on "c" and 4 on "a". All of them
// arrived at 0 and give up at deadline. The poll never fires.
func parkedOnQueues(t *testing.T, deadline Mono) *Core {
	t.Helper()
	c := New(Config{PollInterval: time.Hour})
	for _, p := range []struct {
		id    WaiterID
		queue string
	}{{1, "b"}, {2, "a"}, {3, "c"}, {4, "a"}} {
		w := on(p.queue, wt(p.id, 0))
		w.Deadline = deadline
		if len(fetches(c.Arrive(0, w))) > 0 {
			c.FetchDone(0, p.id, 0, false, AcrossTenants)
		}
	}
	return c
}

func TestTickEffectsFollowQueueNameOrder(t *testing.T) {
	// Map iteration order is random; the effects must not be.
	for range 20 {
		c := parkedOnQueues(t, 5*sec)
		wantEffects(t, c.Tick(5*sec),
			Respond{Waiter: 2, Reason: ReasonTimeout},
			Respond{Waiter: 4, Reason: ReasonTimeout},
			Respond{Waiter: 1, Reason: ReasonTimeout},
			Respond{Waiter: 3, Reason: ReasonTimeout},
		)
	}
}

func TestDrainEffectsFollowQueueNameOrder(t *testing.T) {
	for range 20 {
		c := parkedOnQueues(t, 20*sec)
		wantEffects(t, c.Drain(0),
			Respond{Waiter: 2, Reason: ReasonDraining},
			Respond{Waiter: 4, Reason: ReasonDraining},
			Respond{Waiter: 1, Reason: ReasonDraining},
			Respond{Waiter: 3, Reason: ReasonDraining},
		)
	}
}

func TestIdleQueueStateIsPruned(t *testing.T) {
	c := New(Config{})
	c.Arrive(0, wt(1, 0))
	c.FetchDone(0, 1, 0, false, AcrossTenants)
	c.Cancel(0, 1)
	if len(c.queues) != 1 {
		t.Fatalf("queues = %d before the prune, want 1", len(c.queues))
	}
	c.Tick(sec - time.Microsecond) // the knowledge is still fresh
	if len(c.queues) != 1 {
		t.Fatalf("queues = %d, want the fresh state kept", len(c.queues))
	}
	c.Tick(sec)
	if len(c.queues) != 0 || len(c.waiters) != 0 {
		t.Fatalf("queues, waiters = %d, %d after the prune, want 0, 0", len(c.queues), len(c.waiters))
	}
}

func TestPruneKeepsQueuesWithPendingWork(t *testing.T) {
	c := New(Config{})
	c.DueAt(0, "scheduled", 10*sec)
	c.Arrive(0, wt(1, 0)) // fetching
	c.Arrive(0, on("other", wt(2, 0)))
	c.FetchDone(0, 2, 0, false, AcrossTenants) // waiting
	c.Tick(5 * sec)                            // "other" polls; waiter 1 still fetches
	if len(c.queues) != 3 {
		t.Fatalf("queues = %d, want 3 (due time, fetch in flight, waiter)", len(c.queues))
	}
	c.Cancel(5*sec, 2)
	c.FetchDone(5*sec, 2, 0, false, AcrossTenants)
	c.FetchDone(5*sec, 1, 0, false, AcrossTenants)
	c.Cancel(5*sec, 1)
	c.Tick(9 * sec)
	if len(c.queues) != 1 {
		t.Fatalf("queues = %d, want only the one with a pending due time", len(c.queues))
	}
	c.Tick(10 * sec) // the due time passes and is spent
	if len(c.queues) != 0 {
		t.Fatalf("queues = %d, want 0 once nothing is pending", len(c.queues))
	}
}

func TestArriveClampsMax(t *testing.T) {
	for _, tc := range []struct{ asked, want int }{
		{asked: 0, want: 1},
		{asked: -5, want: 1},
		{asked: 1, want: 1},
		{asked: job.MaxBatch, want: job.MaxBatch},
		{asked: job.MaxBatch + 1, want: job.MaxBatch},
		{asked: 1 << 30, want: job.MaxBatch},
	} {
		c := New(Config{})
		w := wt(1, 0)
		w.Max = tc.asked
		fs := fetches(c.Arrive(0, w))
		if len(fs) != 1 || fs[0].Max != tc.want {
			t.Fatalf("Max %d: fetches = %v, want one with Max %d", tc.asked, fs, tc.want)
		}
	}
}

func TestArriveTreatsDeadlineBeforeArrivalAsFetchOnce(t *testing.T) {
	c := New(Config{})
	w := wt(1, 5*sec)
	w.Deadline = sec
	c.Arrive(5*sec, w)
	wantEffects(t, c.FetchDone(5*sec, 1, 0, false, AcrossTenants), Respond{Waiter: 1, Reason: ReasonTimeout})
}

func TestArriveRejectsDuplicateID(t *testing.T) {
	c := New(Config{})
	c.Arrive(0, wt(1, 0))
	mustPanic(t, "dispatch: Arrive: duplicate waiter id 1", func() { c.Arrive(0, wt(1, 0)) })
	// An id that was answered is free again.
	c.FetchDone(0, 1, 8, false, AcrossTenants)
	wantEffects(t, c.Arrive(0, wt(1, 0)), Fetch{Waiter: 1, Queue: testQueue, Max: 8})
}

func TestFetchDoneForUnknownOrIdleWaiterIsIgnored(t *testing.T) {
	c := parked(t, Config{}, 1)
	wantEffects(t, c.FetchDone(0, 99, 3, false, AcrossTenants), Arm{At: sec}) // unknown id
	wantEffects(t, c.FetchDone(0, 1, 3, false, AcrossTenants), Arm{At: sec})  // waiter 1 has no fetch out
	wantCounts(t, c, testQueue, 1, 0)
}

func TestWaitingAndInFlightOfUnknownQueue(t *testing.T) {
	c := New(Config{})
	wantCounts(t, c, "nowhere", 0, 0)
}

func TestQueuesAreIndependent(t *testing.T) {
	c := New(Config{MaxInFlight: 1})
	wantEffects(t, c.Arrive(0, on("a", wt(1, 0))), Fetch{Waiter: 1, Queue: "a", Max: 8})
	wantEffects(t, c.Arrive(0, on("b", wt(2, 0))), Fetch{Waiter: 2, Queue: "b", Max: 8})
	wantCounts(t, c, "a", 0, 1)
	wantCounts(t, c, "b", 0, 1)
	c.FetchDone(0, 1, 0, false, AcrossTenants) // waiter 1 waits on a
	if effs := c.Notify(0, "b"); len(fetches(effs)) != 0 {
		t.Fatalf("a notification for b started a fetch on a: %s", describeAll(effs))
	}
	wantEffects(t, c.Notify(0, "a"), Fetch{Waiter: 1, Queue: "a", Max: 8})
}

func TestReasonString(t *testing.T) {
	for _, tc := range []struct {
		r    Reason
		want string
	}{
		{ReasonTimeout, "timeout"},
		{ReasonDraining, "draining"},
		{ReasonRejected, "rejected"},
		{ReasonUnavailable, "unavailable"},
		{Reason(0), "reason(0)"},
		{Reason(200), "reason(200)"},
	} {
		if got := tc.r.String(); got != tc.want {
			t.Fatalf("Reason(%d).String() = %q, want %q", uint8(tc.r), got, tc.want)
		}
	}
}
