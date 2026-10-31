//go:build pg

package pgstore_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"pgregory.net/rapid"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/pgtest"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/memstore"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/pgstore"
)

// The differential test runs random sequences of operations against the
// in-memory store, which is the reference model, and against pgstore, and
// fails on the first call that the two answer differently. The conformance
// suite pins what the contract says; this test finds what nobody thought to
// write down.
//
// PostgreSQL sequences skip values (a deduplicated item consumes one, and so
// does an insert that rolls back), and the reference store's ids need not start
// at 1, so the two stores' ids are not equal. They are related by idMap, a
// bijection that must keep their order, built from the ids that Enqueue
// returns for new jobs.

// unknownID is an id that neither store ever hands out. idMap passes it
// through unchanged, so both stores are asked about the same missing job.
const unknownID = int64(1) << 40

// diffQueues and diffTenants are the names the test draws from. Two queues
// make a ref with the wrong queue possible, and three tenants make tenant
// scopes that match, differ and repeat.
var (
	diffQueues  = []string{"q1", "q2"}
	diffTenants = []string{"a", "b", "c"}
)

// idMap relates the reference store's job ids to pgstore's.
type idMap struct {
	pg              map[int64]int64
	lastMem, lastPG int64
}

func newIDMap() *idMap { return &idMap{pg: map[int64]int64{}} }

// add records that the reference store and pgstore named the same new job
// mem and pg. It fails if either id was seen before or if the pair would
// break the order of ids: the later job must have the larger id in both.
func (m *idMap) add(mem, pg int64) error {
	if _, dup := m.pg[mem]; dup {
		return fmt.Errorf("the reference store returned id %d for a new job twice", mem)
	}
	if mem <= m.lastMem || pg <= m.lastPG {
		return fmt.Errorf("new job ids (reference %d, pgstore %d) do not extend the order of the earlier ones (reference %d, pgstore %d)",
			mem, pg, m.lastMem, m.lastPG)
	}
	m.pg[mem] = pg
	m.lastMem, m.lastPG = mem, pg
	return nil
}

// lookup returns pgstore's id for the reference store's id, if a job with it
// was ever returned by Enqueue.
func (m *idMap) lookup(mem int64) (int64, bool) {
	pg, ok := m.pg[mem]
	return pg, ok
}

// of returns pgstore's id for the reference store's id, and passes an id that
// was never handed out through unchanged.
func (m *idMap) of(mem int64) int64 {
	if pg, ok := m.pg[mem]; ok {
		return pg
	}
	return mem
}

// enqueued is a job the test has created, which it may cancel or requeue.
type enqueued struct {
	id            int64
	queue, tenant string
}

// granted is a job the test has leased, with the epoch of its latest lease.
type granted struct {
	id            int64
	queue, tenant string
	epoch         int64
}

// diffHarness drives both stores with the same arguments.
type diffHarness struct {
	ctx  context.Context
	pool *pgxpool.Pool
	mem  *memstore.Store
	pg   *pgstore.Store
	now  time.Time // the reference store's clock

	ids      *idMap
	enqueued []enqueued
	granted  []granted
}

func newDiffHarness(ctx context.Context, pool *pgxpool.Pool, pg *pgstore.Store) *diffHarness {
	h := &diffHarness{ctx: ctx, pool: pool, pg: pg, now: pgtest.Epoch, ids: newIDMap()}
	h.mem = memstore.New(memstore.Options{Clock: func() time.Time { return h.now }})
	return h
}

// failedAlike compares the errors of one call on both stores. It returns true
// when both rejected the call as invalid, so that there are no results to
// compare, and false when both succeeded. Any other pair fails the test.
func (h *diffHarness) failedAlike(rt *rapid.T, op string, memErr, pgErr error) bool {
	switch {
	case memErr == nil && pgErr == nil:
		return false
	case errors.Is(memErr, job.ErrInvalid) && errors.Is(pgErr, job.ErrInvalid):
		return true
	}
	rt.Fatalf("%s: errors differ: reference store %v, pgstore %v", op, memErr, pgErr)
	return false
}

// mapJob returns a reference store job under pgstore's id.
func (h *diffHarness) mapJob(rt *rapid.T, j job.Job) job.Job {
	pg, ok := h.ids.lookup(j.ID)
	if !ok {
		rt.Fatalf("the reference store holds job %d, which no Enqueue result named", j.ID)
	}
	j.ID = pg
	return j
}

// sameJob fails unless the reference store's job, under pgstore's id, equals
// pgstore's.
func (h *diffHarness) sameJob(rt *rapid.T, what string, mem, pg job.Job) {
	rt.Helper()
	want := h.mapJob(rt, mem)
	if !job.Equal(want, pg) {
		rt.Fatalf("%s differs\nreference store (id mapped): %+v\npgstore:                     %+v", what, want, pg)
	}
}

// oneIn is true about once in n draws, and shrinks towards false.
func oneIn(rt *rapid.T, n int, label string) bool {
	return rapid.IntRange(1, n).Draw(rt, label) == n
}

// describeSpec is a short form of a spec for the log: the payload, which can
// be 256 KiB, is left out.
func describeSpec(sp job.Spec) string {
	return fmt.Sprintf("{%s/%s key=%q delay=%s run_at=%s max=%d payload=%dB}",
		sp.Queue, sp.Tenant, sp.IdempotencyKey, sp.Delay, sp.RunAt.Format(time.RFC3339Nano), sp.MaxAttempts, len(sp.Payload))
}

func describeSpecs(specs []job.Spec) string {
	parts := make([]string, len(specs))
	for i, sp := range specs {
		parts[i] = describeSpec(sp)
	}
	return strings.Join(parts, " ")
}

// drawSpec draws a valid spec. A spec has a delay or an absolute run time,
// never both.
func (h *diffHarness) drawSpec(rt *rapid.T) job.Spec {
	sp := job.Spec{
		Queue:          rapid.SampledFrom(diffQueues).Draw(rt, "queue"),
		Tenant:         rapid.SampledFrom(diffTenants).Draw(rt, "tenant"),
		Payload:        rapid.SliceOfN(rapid.Byte(), 0, 6).Draw(rt, "payload"),
		MaxAttempts:    rapid.SampledFrom([]int32{0, 1, 2, 3}).Draw(rt, "max_attempts"),
		IdempotencyKey: rapid.SampledFrom([]string{"", "k1", "k2", "k3", "k4"}).Draw(rt, "key"),
	}
	if oneIn(rt, 4, "scheduled_at") {
		// Before now (which becomes now), at now, ahead, and ahead by a
		// fraction of a microsecond (which the stores drop).
		sp.RunAt = h.now.Add(rapid.SampledFrom([]time.Duration{-time.Second, 0, 3 * time.Second, 3*time.Second + 999}).Draw(rt, "run_at_offset"))
	} else {
		sp.Delay = rapid.SampledFrom([]time.Duration{0, time.Second, 5 * time.Second}).Draw(rt, "delay")
	}
	return sp
}

// invalidSpecs are ways to break a valid spec. Each is rejected by
// job.ValidateSpec, so both stores must refuse the whole call.
var invalidSpecs = []func(sp *job.Spec){
	func(sp *job.Spec) { sp.Queue = "" },
	func(sp *job.Spec) { sp.Tenant = "a b" },
	func(sp *job.Spec) { sp.Payload = make([]byte, job.MaxPayloadBytes+1) },
	func(sp *job.Spec) { sp.Delay = -time.Second },
	func(sp *job.Spec) { sp.Delay = time.Nanosecond },
	func(sp *job.Spec) { sp.Delay, sp.RunAt = time.Second, pgtest.Epoch.Add(time.Hour) },
	func(sp *job.Spec) { sp.RunAt = time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC) },
	func(sp *job.Spec) { sp.MaxAttempts = job.MaxMaxAttempts + 1 },
	func(sp *job.Spec) { sp.MaxAttempts = -1 },
	func(sp *job.Spec) { sp.IdempotencyKey = "k\x00" },
}

// refs draws one to most refs of jobs that have been leased. The token is the
// latest epoch about half the time and one off it otherwise, and about one ref
// in five names the wrong queue, so that stale and foreign refs are in most
// calls. About one in twenty names a job that does not exist. It returns the
// refs for the reference store and, translated, for pgstore. It skips the
// action when nothing has been leased yet.
func (h *diffHarness) refs(rt *rapid.T, most int) (mem, pg []store.Ref) {
	if len(h.granted) == 0 {
		rt.SkipNow()
	}
	n := rapid.IntRange(1, most).Draw(rt, "refs")
	for range n {
		g := h.granted[rapid.IntRange(0, len(h.granted)-1).Draw(rt, "job")]
		r := store.Ref{ID: g.id, Queue: g.queue, Token: g.epoch}
		switch rapid.SampledFrom([]int64{0, 0, -1, 1}).Draw(rt, "token_offset") {
		case -1:
			r.Token--
		case 1:
			r.Token++
		}
		if oneIn(rt, 5, "wrong_queue") {
			r.Queue = otherQueue(r.Queue)
		}
		if oneIn(rt, 20, "unknown_job") {
			r.ID = unknownID
		}
		mem = append(mem, r)
		r.ID = h.ids.of(r.ID)
		pg = append(pg, r)
	}
	return mem, pg
}

func otherQueue(q string) string {
	if q == diffQueues[0] {
		return diffQueues[1]
	}
	return diffQueues[0]
}

// wantOutcomes fails unless both stores gave the same outcome for every item.
func wantOutcomes(rt *rapid.T, op string, mem, pg []job.Outcome) {
	rt.Helper()
	if !slices.Equal(mem, pg) {
		rt.Fatalf("%s: outcomes differ\nreference store: %v\npgstore:         %v", op, mem, pg)
	}
}

// enqueue sends one to four specs, one of which is invalid about once in ten
// calls.
func (h *diffHarness) enqueue(rt *rapid.T) {
	specs := make([]job.Spec, rapid.IntRange(1, 4).Draw(rt, "items"))
	for i := range specs {
		specs[i] = h.drawSpec(rt)
	}
	if oneIn(rt, 10, "invalid_item") {
		i := rapid.IntRange(0, len(specs)-1).Draw(rt, "invalid_at")
		rapid.SampledFrom(invalidSpecs).Draw(rt, "invalid_kind")(&specs[i])
	}
	rt.Logf("enqueue %s", describeSpecs(specs))

	memRes, memErr := h.mem.Enqueue(h.ctx, specs)
	pgRes, pgErr := h.pg.Enqueue(h.ctx, specs)
	if h.failedAlike(rt, "enqueue", memErr, pgErr) {
		return
	}
	if len(memRes) != len(specs) || len(pgRes) != len(specs) {
		rt.Fatalf("enqueue of %d specs: reference store returned %d results, pgstore %d", len(specs), len(memRes), len(pgRes))
	}
	for i, sp := range specs {
		m, p := memRes[i], pgRes[i]
		if m.Deduplicated != p.Deduplicated {
			rt.Fatalf("enqueue item %d %s: deduplicated is %v in the reference store, %v in pgstore", i, describeSpec(sp), m.Deduplicated, p.Deduplicated)
		}
		if m.Deduplicated {
			if want, ok := h.ids.lookup(m.ID); !ok || want != p.ID {
				rt.Fatalf("enqueue item %d %s: deduplicated to job %d in the reference store, which is pgstore job %d (known: %v), but to job %d in pgstore",
					i, describeSpec(sp), m.ID, want, ok, p.ID)
			}
			continue
		}
		if err := h.ids.add(m.ID, p.ID); err != nil {
			rt.Fatalf("enqueue item %d %s: %v", i, describeSpec(sp), err)
		}
		h.enqueued = append(h.enqueued, enqueued{id: m.ID, queue: sp.Queue, tenant: sp.Tenant})
	}
}

// lease takes in FIFO order or from one to three tenants, a tenant possibly
// more than once.
func (h *diffHarness) lease(rt *rapid.T) {
	req := store.LeaseRequest{
		Queue:  rapid.SampledFrom(diffQueues).Draw(rt, "queue"),
		TTL:    rapid.SampledFrom([]time.Duration{5 * time.Second, 30 * time.Second}).Draw(rt, "ttl"),
		Worker: "w",
	}
	if rapid.Bool().Draw(rt, "fifo") {
		req.Takes = []store.TenantTake{{Limit: rapid.IntRange(1, 4).Draw(rt, "limit")}}
	} else {
		for range rapid.IntRange(1, 3).Draw(rt, "takes") {
			req.Takes = append(req.Takes, store.TenantTake{
				Tenant: rapid.SampledFrom(diffTenants).Draw(rt, "tenant"),
				Limit:  rapid.IntRange(1, 4).Draw(rt, "limit"),
			})
		}
	}
	rt.Logf("lease %+v", req)

	memGrants, memErr := h.mem.Lease(h.ctx, req)
	pgGrants, pgErr := h.pg.Lease(h.ctx, req)
	if h.failedAlike(rt, "lease", memErr, pgErr) {
		return
	}
	if len(memGrants) != len(pgGrants) {
		rt.Fatalf("lease %+v: the reference store granted %d jobs, pgstore %d\nreference store: %+v\npgstore: %+v",
			req, len(memGrants), len(pgGrants), memGrants, pgGrants)
	}
	for i, m := range memGrants {
		p := pgGrants[i]
		h.sameJob(rt, fmt.Sprintf("grant %d of lease %+v", i, req), m.Job, p.Job)
		if m.Remaining != p.Remaining {
			rt.Fatalf("grant %d of lease %+v: remaining is %s in the reference store, %s in pgstore", i, req, m.Remaining, p.Remaining)
		}
		h.noteGrant(m.Job)
	}
}

// noteGrant records the epoch of a job that has just been leased.
func (h *diffHarness) noteGrant(j job.Job) {
	g := granted{id: j.ID, queue: j.Queue, tenant: j.Tenant, epoch: j.Epoch}
	for i := range h.granted {
		if h.granted[i].id == j.ID {
			h.granted[i] = g
			return
		}
	}
	h.granted = append(h.granted, g)
}

func (h *diffHarness) heartbeat(rt *rapid.T) {
	memRefs, pgRefs := h.refs(rt, 3)
	ttl := rapid.SampledFrom([]time.Duration{5 * time.Second, 30 * time.Second}).Draw(rt, "ttl")
	rt.Logf("heartbeat %v ttl %s", memRefs, ttl)

	memRes, memErr := h.mem.Heartbeat(h.ctx, memRefs, ttl)
	pgRes, pgErr := h.pg.Heartbeat(h.ctx, pgRefs, ttl)
	if h.failedAlike(rt, "heartbeat", memErr, pgErr) {
		return
	}
	if !slices.Equal(memRes, pgRes) {
		rt.Fatalf("heartbeat %v ttl %s: results differ\nreference store: %+v\npgstore:         %+v", memRefs, ttl, memRes, pgRes)
	}
}

func (h *diffHarness) complete(rt *rapid.T) {
	memRefs, pgRefs := h.refs(rt, 3)
	memReqs, pgReqs := make([]store.CompleteRequest, len(memRefs)), make([]store.CompleteRequest, len(memRefs))
	for i := range memRefs {
		result := rapid.SliceOfN(rapid.Byte(), 0, 8).Draw(rt, "result")
		memReqs[i] = store.CompleteRequest{Ref: memRefs[i], Result: result}
		pgReqs[i] = store.CompleteRequest{Ref: pgRefs[i], Result: result}
	}
	rt.Logf("complete %+v", memReqs)

	memOut, memErr := h.mem.Complete(h.ctx, memReqs)
	pgOut, pgErr := h.pg.Complete(h.ctx, pgReqs)
	if h.failedAlike(rt, "complete", memErr, pgErr) {
		return
	}
	wantOutcomes(rt, fmt.Sprintf("complete %+v", memReqs), memOut, pgOut)
}

// failMessages include an empty one, one with a NUL, and one that is too long
// and has multi-byte characters at the cut.
var failMessages = []string{"", "boom", "bad\x00byte", strings.Repeat("é", job.MaxErrorBytes)}

func (h *diffHarness) fail(rt *rapid.T) {
	memRefs, pgRefs := h.refs(rt, 3)
	memReqs, pgReqs := make([]store.FailRequest, len(memRefs)), make([]store.FailRequest, len(memRefs))
	for i := range memRefs {
		r := store.FailRequest{
			Permanent: rapid.Bool().Draw(rt, "permanent"),
			Delay:     rapid.SampledFrom([]time.Duration{0, time.Second, 3 * time.Second}).Draw(rt, "delay"),
			Error:     rapid.SampledFrom(failMessages).Draw(rt, "error"),
		}
		r.Ref = memRefs[i]
		memReqs[i] = r
		r.Ref = pgRefs[i]
		pgReqs[i] = r
	}
	rt.Logf("fail %+v", memRefs)

	memOut, memErr := h.mem.Fail(h.ctx, memReqs)
	pgOut, pgErr := h.pg.Fail(h.ctx, pgReqs)
	if h.failedAlike(rt, "fail", memErr, pgErr) {
		return
	}
	wantOutcomes(rt, fmt.Sprintf("fail %v", memRefs), memOut, pgOut)
}

func (h *diffHarness) release(rt *rapid.T) {
	memRefs, pgRefs := h.refs(rt, 3)
	rt.Logf("release %v", memRefs)

	memOut, memErr := h.mem.Release(h.ctx, memRefs)
	pgOut, pgErr := h.pg.Release(h.ctx, pgRefs)
	if h.failedAlike(rt, "release", memErr, pgErr) {
		return
	}
	wantOutcomes(rt, fmt.Sprintf("release %v", memRefs), memOut, pgOut)
}

// drawJob draws the id of a job that exists or, about once in ten calls, one
// that does not, and a tenant scope for it: any tenant, the job's, or another.
func (h *diffHarness) drawJob(rt *rapid.T) (id int64, tenant string) {
	if len(h.enqueued) == 0 {
		rt.SkipNow()
	}
	e := h.enqueued[rapid.IntRange(0, len(h.enqueued)-1).Draw(rt, "job")]
	id, tenant = e.id, e.tenant
	switch rapid.IntRange(0, 2).Draw(rt, "scope") {
	case 0:
		tenant = ""
	case 2:
		tenant = diffTenants[(slices.Index(diffTenants, e.tenant)+1)%len(diffTenants)]
	}
	if oneIn(rt, 10, "unknown_job") {
		id = unknownID
	}
	return id, tenant
}

func (h *diffHarness) cancel(rt *rapid.T) {
	id, tenant := h.drawJob(rt)
	rt.Logf("cancel %d tenant %q", id, tenant)

	memOut, memErr := h.mem.Cancel(h.ctx, id, tenant)
	pgOut, pgErr := h.pg.Cancel(h.ctx, h.ids.of(id), tenant)
	if h.failedAlike(rt, "cancel", memErr, pgErr) {
		return
	}
	wantOutcomes(rt, fmt.Sprintf("cancel %d tenant %q", id, tenant), []job.Outcome{memOut}, []job.Outcome{pgOut})
}

func (h *diffHarness) requeue(rt *rapid.T) {
	id, _ := h.drawJob(rt)
	rt.Logf("requeue %d", id)

	memOut, memErr := h.mem.Requeue(h.ctx, id)
	pgOut, pgErr := h.pg.Requeue(h.ctx, h.ids.of(id))
	if h.failedAlike(rt, "requeue", memErr, pgErr) {
		return
	}
	wantOutcomes(rt, fmt.Sprintf("requeue %d", id), []job.Outcome{memOut}, []job.Outcome{pgOut})
}

// reap asks each store to reap with a backoff of one second per attempt, and
// compares not only how many rows it took back but the attempts counts that it
// asked about, which show the rows it visited and the order.
func (h *diffHarness) reap(rt *rapid.T) {
	limit := rapid.IntRange(1, 5).Draw(rt, "limit")
	var memAsked, pgAsked []int32
	backoff := func(asked *[]int32) store.BackoffFunc {
		return func(attempts int32) time.Duration {
			*asked = append(*asked, attempts)
			return time.Duration(attempts) * time.Second
		}
	}
	rt.Logf("reap limit %d", limit)

	memN, memErr := h.mem.Reap(h.ctx, limit, backoff(&memAsked))
	pgN, pgErr := h.pg.Reap(h.ctx, limit, backoff(&pgAsked))
	if h.failedAlike(rt, "reap", memErr, pgErr) {
		return
	}
	if memN != pgN {
		rt.Fatalf("reap limit %d: the reference store reaped %d jobs, pgstore %d", limit, memN, pgN)
	}
	if !slices.Equal(memAsked, pgAsked) {
		rt.Fatalf("reap limit %d: backoff was asked about attempts %v by the reference store, %v by pgstore", limit, memAsked, pgAsked)
	}
}

func (h *diffHarness) prune(rt *rapid.T) {
	retention := store.Retention{Done: 10 * time.Second, Dead: 20 * time.Second}
	limit := rapid.IntRange(1, 5).Draw(rt, "limit")
	rt.Logf("prune limit %d", limit)

	memN, memErr := h.mem.Prune(h.ctx, retention, limit)
	pgN, pgErr := h.pg.Prune(h.ctx, retention, limit)
	if h.failedAlike(rt, "prune", memErr, pgErr) {
		return
	}
	if memN != pgN {
		rt.Fatalf("prune limit %d: the reference store deleted %d jobs, pgstore %d", limit, memN, pgN)
	}
}

// advance moves both clocks by the same amount.
func (h *diffHarness) advance(rt *rapid.T) {
	d := rapid.SampledFrom([]time.Duration{time.Microsecond, time.Second, 5 * time.Second, 30 * time.Second}).Draw(rt, "by")
	rt.Logf("advance %s", d)
	h.now = h.now.Add(d)
	if err := pgtest.AdvanceClock(h.ctx, h.pool, d); err != nil {
		rt.Fatalf("advance the database clock: %v", err)
	}
}

// checkSnapshots runs after every step. It compares every row of the two
// stores, which also checks that the ids stay in the same order, and then what
// the read and admin calls say about each queue.
func (h *diffHarness) checkSnapshots(rt *rapid.T) {
	memRows, err := h.mem.Snapshot(h.ctx)
	if err != nil {
		rt.Fatalf("snapshot of the reference store: %v", err)
	}
	pgRows, err := h.pg.Snapshot(h.ctx)
	if err != nil {
		rt.Fatalf("snapshot of pgstore: %v", err)
	}
	if len(memRows) != len(pgRows) {
		rt.Fatalf("the reference store holds %d jobs, pgstore %d", len(memRows), len(pgRows))
	}
	for i := range memRows {
		h.sameJob(rt, fmt.Sprintf("job at position %d of the snapshot", i), memRows[i], pgRows[i])
	}
	for _, q := range diffQueues {
		h.checkQueue(rt, q)
	}
}

// checkQueue compares Stats, Activity and ListDead for one queue.
func (h *diffHarness) checkQueue(rt *rapid.T, q string) {
	memStats, err := h.mem.Stats(h.ctx, q)
	if err != nil {
		rt.Fatalf("stats of the reference store: %v", err)
	}
	pgStats, err := h.pg.Stats(h.ctx, q)
	if err != nil {
		rt.Fatalf("stats of pgstore: %v", err)
	}
	if !slices.Equal(memStats, pgStats) {
		rt.Fatalf("stats of %s differ\nreference store: %+v\npgstore:         %+v", q, memStats, pgStats)
	}

	memAct, err := h.mem.Activity(h.ctx, q)
	if err != nil {
		rt.Fatalf("activity of the reference store: %v", err)
	}
	pgAct, err := h.pg.Activity(h.ctx, q)
	if err != nil {
		rt.Fatalf("activity of pgstore: %v", err)
	}
	if !slices.Equal(memAct.Tenants, pgAct.Tenants) || memAct.HasNextDue != pgAct.HasNextDue || memAct.NextDueIn != pgAct.NextDueIn {
		rt.Fatalf("activity of %s differs\nreference store: %+v\npgstore:         %+v", q, memAct, pgAct)
	}

	memDead, err := h.mem.ListDead(h.ctx, q, 0, 3)
	if err != nil {
		rt.Fatalf("dead jobs of the reference store: %v", err)
	}
	pgDead, err := h.pg.ListDead(h.ctx, q, 0, 3)
	if err != nil {
		rt.Fatalf("dead jobs of pgstore: %v", err)
	}
	if len(memDead) != len(pgDead) {
		rt.Fatalf("the first dead jobs of %s: the reference store lists %d, pgstore %d", q, len(memDead), len(pgDead))
	}
	for i := range memDead {
		h.sameJob(rt, fmt.Sprintf("dead job %d of %s", i, q), memDead[i], pgDead[i])
	}
}

// TestPGDifferential holds pgstore to the reference store: whatever sequence
// of calls and clock movements rapid draws, both stores must answer every call
// alike and hold the same rows afterwards.
func TestPGDifferential(t *testing.T) {
	pool := pgtest.NewDB(t)
	pg := pgstore.New(pool)
	rapid.Check(t, func(rt *rapid.T) {
		if err := pgtest.Reset(t.Context(), pool); err != nil {
			rt.Fatal(err)
		}
		h := newDiffHarness(t.Context(), pool, pg)
		// Two Repeat passes, not one. Each draws its own length, and the second
		// goes on from the state the first left, so a check makes a longer and
		// more varied history. The flaw that needs the longest one is F3-pg,
		// Reap ignoring max_attempts: a job has to be leased, to run out of
		// lease, and to be reaped with its attempts used up. Measured with
		// one check per run, one pass finds it in 23 of 300 runs (8%) and two
		// passes in 111 of 600 (18.5%; two samples of 300). The flaws job
		// relies on a check finding each flaw often enough that its many
		// checks never miss; see scripts/check-flaws.sh.
		for range 2 {
			rt.Repeat(map[string]func(*rapid.T){
				"":          h.checkSnapshots,
				"enqueue":   h.enqueue,
				"lease":     h.lease,
				"heartbeat": h.heartbeat,
				"complete":  h.complete,
				"fail":      h.fail,
				"release":   h.release,
				"cancel":    h.cancel,
				"requeue":   h.requeue,
				"reap":      h.reap,
				"prune":     h.prune,
				"advance":   h.advance,
			})
		}
	})
}
