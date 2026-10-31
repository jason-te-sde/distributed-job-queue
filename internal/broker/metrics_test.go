package broker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/memstore"
)

// metricValue reads one series of a counter or a gauge from g; a series that
// does not exist reads as 0.
func metricValue(t testing.TB, g prometheus.Gatherer, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
	metric:
		for _, m := range mf.GetMetric() {
			got := map[string]string{}
			for _, l := range m.GetLabel() {
				got[l.GetName()] = l.GetValue()
			}
			if len(got) != len(labels) {
				continue
			}
			for k, v := range labels {
				if got[k] != v {
					continue metric
				}
			}
			switch {
			case m.GetCounter() != nil:
				return m.GetCounter().GetValue()
			case m.GetGauge() != nil:
				return m.GetGauge().GetValue()
			}
			t.Fatalf("metric %s is neither a counter nor a gauge", name)
		}
	}
	return 0
}

// seriesCount returns how many series the named metric has in g.
func seriesCount(t testing.TB, g prometheus.Gatherer, name string) int {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range families {
		if mf.GetName() == name {
			return len(mf.GetMetric())
		}
	}
	return 0
}

func TestQueueLabelCapsCardinality(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	for i := range maxQueueLabels {
		q := fmt.Sprintf("q%03d", i)
		if got := m.queues.Label(q); got != q {
			t.Fatalf("queueLabel(%q) = %q, want the name itself: it is among the first %d", q, got, maxQueueLabels)
		}
	}
	if got := m.queues.Label("one-too-many"); got != otherQueueLabel {
		t.Fatalf("queueLabel of the %dth name = %q, want %q", maxQueueLabels+1, got, otherQueueLabel)
	}
	if got := m.queues.Label("q000"); got != "q000" {
		t.Errorf("queueLabel(q000) = %q after the cap was reached, want q000: a name keeps its label", got)
	}
	if got := m.queues.Label("one-too-many"); got != otherQueueLabel {
		t.Errorf("queueLabel(one-too-many) = %q on the second call, want %q", got, otherQueueLabel)
	}
}

func TestEnqueueMetricsUseTheQueueLabelCap(t *testing.T) {
	ts := newTestServer(t, testOpts{noAuth: true})
	// maxQueueLabels queues fill the labels; the next two share "other".
	for i := range maxQueueLabels + 2 {
		queue := fmt.Sprintf("q%03d", i)
		it := item(queue, "t")
		it.IdempotencyKey = "k-" + queue
		if _, err := ts.client.Enqueue(ts.anon(), enqueueReq(it)); err != nil {
			t.Fatalf("Enqueue into %s: %v", queue, err)
		}
	}
	// The key of the last queue names its job again.
	last := item(fmt.Sprintf("q%03d", maxQueueLabels+1), "t")
	last.IdempotencyKey = "k-" + last.Queue
	if _, err := ts.client.Enqueue(ts.anon(), enqueueReq(last)); err != nil {
		t.Fatal(err)
	}

	if got := ts.counter("jobqueue_jobs_enqueued_total", map[string]string{"queue": "q000"}); got != 1 {
		t.Errorf("enqueued{queue=q000} = %v, want 1", got)
	}
	if got := ts.counter("jobqueue_jobs_enqueued_total", map[string]string{"queue": otherQueueLabel}); got != 2 {
		t.Errorf("enqueued{queue=other} = %v, want 2", got)
	}
	if got := ts.counter("jobqueue_jobs_deduplicated_total", map[string]string{"queue": otherQueueLabel}); got != 1 {
		t.Errorf("deduplicated{queue=other} = %v, want 1", got)
	}
	if n := seriesCount(t, ts.reg, "jobqueue_jobs_enqueued_total"); n != maxQueueLabels+1 {
		t.Errorf("jobqueue_jobs_enqueued_total has %d series, want %d", n, maxQueueLabels+1)
	}
}

func TestMetricsRegistersEveryName(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	// One observation of each metric, so that every series exists.
	m.enqueued.For("q").Inc()
	m.deduplicated.For("q").Inc()
	m.leased.For("q").Inc()
	m.acks.WithLabelValues(opComplete, resultAccepted).Inc()
	m.reaped.Inc()
	m.pruned.Inc()
	m.orphanReleases.Inc()
	m.fetches.For("q", fetchJobs).Inc()
	m.waiters.For("q").Inc()
	m.leaseWait.Observe(0.01)
	m.observeStoreCall("lease", 0.001)
	m.setQueueJobs("q", queueJobCounts{ready: 1})
	m.notifyReconnects.Inc()
	m.notifyMalformed.Inc()
	m.SetBuildInfo("v1.2.3")

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string][]string{} // name -> label names of its first series
	for _, mf := range families {
		var labels []string
		for _, l := range mf.GetMetric()[0].GetLabel() {
			labels = append(labels, l.GetName())
		}
		slices.Sort(labels)
		got[mf.GetName()] = labels
	}
	want := map[string][]string{ // name -> label names
		"jobqueue_jobs_enqueued_total":     {"queue"},
		"jobqueue_jobs_deduplicated_total": {"queue"},
		"jobqueue_jobs_leased_total":       {"queue"},
		"jobqueue_acks_total":              {"op", "result"},
		"jobqueue_jobs_reaped_total":       nil,
		"jobqueue_jobs_pruned_total":       nil,
		"jobqueue_orphan_releases_total":   nil,
		"jobqueue_dispatch_fetches_total":  {"queue", "result"},
		"jobqueue_waiters":                 {"queue"},
		"jobqueue_lease_wait_seconds":      nil,
		"jobqueue_store_call_seconds":      {"op"},
		"jobqueue_queue_jobs":              {"queue", "state"},
		"jobqueue_notify_reconnects_total": nil,
		"jobqueue_notify_malformed_total":  nil,
		"jobqueue_build_info":              {"go_version", "version"},
	}
	for name, labels := range want {
		gotLabels, ok := got[name]
		if !ok {
			t.Errorf("metric %s is not registered", name)
			continue
		}
		if !slices.Equal(gotLabels, labels) {
			t.Errorf("metric %s has labels %v, want %v", name, gotLabels, labels)
		}
	}
	for name := range got {
		if _, ok := want[name]; !ok && strings.HasPrefix(name, "jobqueue_") {
			t.Errorf("metric %s is registered but not in the list of the design", name)
		}
	}
	if _, ok := got["go_goroutines"]; !ok {
		t.Error("the Go collector is not registered: go_goroutines is missing")
	}
	var process bool
	for name := range got {
		process = process || strings.HasPrefix(name, "process_")
	}
	if !process {
		t.Error("the process collector is not registered: no process_ metric")
	}

	if v := metricValue(t, reg, "jobqueue_build_info", map[string]string{"version": "v1.2.3", "go_version": runtime.Version()}); v != 1 {
		t.Errorf("build_info = %v, want 1", v)
	}
	// A second SetBuildInfo replaces the first: there is one build.
	m.SetBuildInfo("v2")
	if n := seriesCount(t, reg, "jobqueue_build_info"); n != 1 {
		t.Errorf("build_info has %d series after a second SetBuildInfo, want 1", n)
	}
}

func TestHistogramBucketsSpanTheDocumentedRanges(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	m.leaseWait.Observe(0.01)
	m.observeStoreCall("lease", 0.001)
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	bounds := map[string][]float64{}
	for _, mf := range families {
		h := mf.GetMetric()[0].GetHistogram()
		if h == nil {
			continue
		}
		for _, b := range h.GetBucket() {
			bounds[mf.GetName()] = append(bounds[mf.GetName()], b.GetUpperBound())
		}
	}
	for name, want := range map[string]struct{ first, last float64 }{
		"jobqueue_lease_wait_seconds": {0.001, 32.768}, // 1ms doubling to about 32s
		"jobqueue_store_call_seconds": {0.0001, 10},    // 100µs up to 10s
	} {
		b := bounds[name]
		if len(b) == 0 {
			t.Fatalf("no buckets for %s", name)
		}
		if math.Abs(b[0]-want.first) > 1e-9 || math.Abs(b[len(b)-1]-want.last) > 1e-9 {
			t.Errorf("%s buckets run from %v to %v, want %v to %v", name, b[0], b[len(b)-1], want.first, want.last)
		}
	}
}

func TestTimedStoreObservesEveryStoreMethod(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	clock := &fakeClock{now: t0}
	mem := memstore.New(memstore.Options{Clock: clock.Now})
	st := timedStore{Store: mem, m: m}
	ctx := t.Context()

	res, err := st.Enqueue(ctx, []job.Spec{{Queue: "q", Tenant: "t", Payload: []byte("p")}})
	if err != nil {
		t.Fatal(err)
	}
	id := res[0].ID
	grants, err := st.Lease(ctx, store.LeaseRequest{Queue: "q", Takes: []store.TenantTake{{Limit: 1}}, TTL: time.Minute, Worker: "w"})
	if err != nil || len(grants) != 1 {
		t.Fatalf("Lease = %v, %v", grants, err)
	}
	ref := store.Ref{ID: id, Queue: "q", Token: grants[0].Job.Epoch}
	calls := []struct {
		op string
		do func() error
	}{
		{"heartbeat", func() error { _, err := st.Heartbeat(ctx, []store.Ref{ref}, time.Minute); return err }},
		{"fail", func() error {
			_, err := st.Fail(ctx, []store.FailRequest{{Ref: ref, Permanent: true, Error: "x"}})
			return err
		}},
		{"requeue", func() error { _, err := st.Requeue(ctx, id); return err }},
		{"get", func() error { _, err := st.Get(ctx, id, ""); return err }},
		{"list_dead", func() error { _, err := st.ListDead(ctx, "q", 0, 10); return err }},
		{"stats", func() error { _, err := st.Stats(ctx, "q"); return err }},
		{"activity", func() error { _, err := st.Activity(ctx, "q"); return err }},
		{"cancel", func() error { _, err := st.Cancel(ctx, id, "t"); return err }},
		{"reap", func() error { _, err := st.Reap(ctx, 10, func(int32) time.Duration { return 0 }); return err }},
		{"prune", func() error {
			_, err := st.Prune(ctx, store.Retention{Done: time.Hour, Dead: time.Hour}, 10)
			return err
		}},
		{"purge", func() error {
			_, err := st.Purge(ctx, store.PurgeRequest{Queue: "q", State: job.StateSucceeded, FinishedBefore: t0})
			return err
		}},
	}
	// Complete and Release need a leased job of their own.
	res, err = st.Enqueue(ctx, []job.Spec{{Queue: "q2", Tenant: "t", Payload: []byte("p")}, {Queue: "q2", Tenant: "t", Payload: []byte("p")}})
	if err != nil {
		t.Fatal(err)
	}
	grants, err = st.Lease(ctx, store.LeaseRequest{Queue: "q2", Takes: []store.TenantTake{{Limit: 2}}, TTL: time.Minute, Worker: "w"})
	if err != nil || len(grants) != 2 {
		t.Fatalf("Lease = %v, %v", grants, err)
	}
	calls = append(calls, []struct {
		op string
		do func() error
	}{
		{"complete", func() error {
			_, err := st.Complete(ctx, []store.CompleteRequest{{Ref: store.Ref{ID: res[0].ID, Queue: "q2", Token: grants[0].Job.Epoch}}})
			return err
		}},
		{"release", func() error {
			_, err := st.Release(ctx, []store.Ref{{ID: res[1].ID, Queue: "q2", Token: grants[1].Job.Epoch}})
			return err
		}},
	}...)
	for _, c := range calls {
		if err := c.do(); err != nil {
			t.Fatalf("%s: %v", c.op, err)
		}
	}

	// Every method of the interface, and nothing else, has a series, each
	// observed once (enqueue and lease twice, because the setup made two calls).
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var ops []string
	for _, mf := range families {
		if mf.GetName() != "jobqueue_store_call_seconds" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			ops = append(ops, metric.GetLabel()[0].GetValue())
			want := uint64(1)
			if op := metric.GetLabel()[0].GetValue(); op == "enqueue" || op == "lease" {
				want = 2
			}
			if n := metric.GetHistogram().GetSampleCount(); n != want {
				t.Errorf("op %q was observed %d times, want %d", metric.GetLabel()[0].GetValue(), n, want)
			}
		}
	}
	slices.Sort(ops)
	if methods := reflect.TypeFor[store.Store]().NumMethod(); len(ops) != methods {
		t.Errorf("store_call_seconds has %d ops %v, but store.Store has %d methods: a new method needs a timed wrapper", len(ops), ops, methods)
	}
}

func TestStatsRefresherSetsQueueJobs(t *testing.T) {
	clock := &fakeClock{now: t0}
	mem := memstore.New(memstore.Options{Clock: clock.Now})
	ctx := t.Context()
	specs := []job.Spec{
		{Queue: "q", Tenant: "a", Payload: []byte("p")},
		{Queue: "q", Tenant: "b", Payload: []byte("p")},
		{Queue: "q", Tenant: "b", Payload: []byte("p")},
		{Queue: "q", Tenant: "b", Payload: []byte("p"), Delay: time.Hour},
		{Queue: "q", Tenant: "a", Payload: []byte("p")},
	}
	if _, err := mem.Enqueue(ctx, specs); err != nil {
		t.Fatal(err)
	}
	// Lease the first job, kill the second.
	grants, err := mem.Lease(ctx, store.LeaseRequest{Queue: "q", Takes: []store.TenantTake{{Limit: 2}}, TTL: time.Minute, Worker: "w"})
	if err != nil || len(grants) != 2 {
		t.Fatalf("Lease = %v, %v", grants, err)
	}
	g := grants[1].Job
	if _, err := mem.Fail(ctx, []store.FailRequest{{Ref: store.Ref{ID: g.ID, Queue: "q", Token: g.Epoch}, Permanent: true, Error: "x"}}); err != nil {
		t.Fatal(err)
	}

	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	m.queues.Label("q") // the broker has seen the queue
	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		runStatsRefresher(runCtx, mem, 5*time.Millisecond, m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runDone:
		case <-time.After(10 * time.Second):
			t.Error("the stats refresher did not return after its context ended")
		}
	})
	for state, want := range map[string]float64{"ready": 2, "scheduled": 1, "leased": 1, "dead": 1} {
		labels := map[string]string{"queue": "q", "state": state}
		waitFor(t, "queue_jobs "+state, quickly, func() bool { return metricValue(t, reg, "jobqueue_queue_jobs", labels) == want })
	}
}

func TestStatsRefresherSkipsQueuesWithoutALabel(t *testing.T) {
	// Past the cap a queue shares "other" with every other late queue, so its
	// gauge could not be told apart: the refresher leaves such queues out.
	clock := &fakeClock{now: t0}
	mem := memstore.New(memstore.Options{Clock: clock.Now})
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	for i := range maxQueueLabels {
		m.queues.Label(fmt.Sprintf("q%03d", i))
	}
	if _, err := mem.Enqueue(t.Context(), []job.Spec{{Queue: "late", Tenant: "t", Payload: []byte("p")}}); err != nil {
		t.Fatal(err)
	}
	if got := m.queues.Names(); len(got) != maxQueueLabels || slices.Contains(got, "late") {
		t.Fatalf("Names() has %d names, including late: %v", len(got), slices.Contains(got, "late"))
	}
	refreshQueueJobs(t.Context(), mem, m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if n := seriesCount(t, reg, "jobqueue_queue_jobs"); n != maxQueueLabels*4 {
		t.Errorf("queue_jobs has %d series, want %d (4 states of each of the %d labelled queues)", n, maxQueueLabels*4, maxQueueLabels)
	}
	if got := metricValue(t, reg, "jobqueue_queue_jobs", map[string]string{"queue": otherQueueLabel, "state": "ready"}); got != 0 {
		t.Errorf("queue_jobs{queue=other} = %v, want no series", got)
	}
}

// queueLabelValues returns, for every metric family that has a "queue" label,
// the distinct values of that label.
func queueLabelValues(t testing.TB, g prometheus.Gatherer) map[string]map[string]struct{} {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]map[string]struct{}{}
	for _, mf := range families {
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() != "queue" {
					continue
				}
				if out[mf.GetName()] == nil {
					out[mf.GetName()] = map[string]struct{}{}
				}
				out[mf.GetName()][l.GetValue()] = struct{}{}
			}
		}
	}
	return out
}

// wantQueueLabelsWithinTheCap fails for every family with more than
// maxQueueLabels+1 values of its queue label.
func wantQueueLabelsWithinTheCap(t testing.TB, g prometheus.Gatherer) {
	t.Helper()
	for name, values := range queueLabelValues(t, g) {
		if len(values) > maxQueueLabels+1 {
			t.Errorf("%s has %d distinct queue labels, want at most %d: every queue label must go through the cap", name, len(values), maxQueueLabels+1)
		}
	}
}
