package broker

import (
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
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
		if got := m.queueLabel(q); got != q {
			t.Fatalf("queueLabel(%q) = %q, want the name itself: it is among the first %d", q, got, maxQueueLabels)
		}
	}
	if got := m.queueLabel("one-too-many"); got != otherQueueLabel {
		t.Fatalf("queueLabel of the %dth name = %q, want %q", maxQueueLabels+1, got, otherQueueLabel)
	}
	if got := m.queueLabel("q000"); got != "q000" {
		t.Errorf("queueLabel(q000) = %q after the cap was reached, want q000: a name keeps its label", got)
	}
	if got := m.queueLabel("one-too-many"); got != otherQueueLabel {
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
