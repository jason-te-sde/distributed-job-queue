// Package queuelabel keeps the "queue" label of Prometheus series bounded.
//
// A queue name comes from a request, so a series labelled with it could be
// created without bound by anyone who may enqueue. A Cap hands the first few
// distinct names a label value of their own and every later name a shared one.
// Counters and Gauges are vectors whose first label is the queue; their
// raw vectors are unexported, so the only way to reach a series is a method
// that takes the queue name and applies the cap. Code outside this package
// cannot label a series with a queue name without it.
package queuelabel

import (
	"maps"
	"slices"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Cap decides the label value of each queue name: the name itself for each of
// the first max distinct names it sees, and a shared value for every later
// one. A name keeps its label for the life of the Cap. It is safe for
// concurrent use.
type Cap struct {
	max   int
	other string

	mu    sync.Mutex
	named map[string]struct{}
}

// NewCap returns a Cap that gives max distinct queue names their own label
// value and every other name the value other.
func NewCap(max int, other string) *Cap {
	return &Cap{max: max, other: other, named: map[string]struct{}{}}
}

// Label returns the label value for a queue name.
func (c *Cap) Label(queue string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.named[queue]; ok {
		return queue
	}
	if len(c.named) >= c.max {
		return c.other
	}
	c.named[queue] = struct{}{}
	return queue
}

// Names returns, in order, the queue names that have a label value of their
// own. Past the cap a queue shares a value with others, so no series of its
// own can be kept for it.
func (c *Cap) Names() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Sorted(maps.Keys(c.named))
}

// Counters is a counter vector labelled by queue and then by any extra labels.
type Counters struct {
	vec *prometheus.CounterVec
	cap *Cap
}

// NewCounters creates and registers, through f, a counter vector whose labels
// are "queue" followed by extra. Its series are reached by For.
func NewCounters(f promauto.Factory, c *Cap, opts prometheus.CounterOpts, extra ...string) *Counters {
	return &Counters{vec: f.NewCounterVec(opts, append([]string{"queue"}, extra...)), cap: c}
}

// For returns the counter of a queue, with the values of the extra labels. The
// queue label is the cap's value for the name. It panics, as prometheus does,
// if the number of extra values is not the number of extra labels.
func (c *Counters) For(queue string, extra ...string) prometheus.Counter {
	return c.vec.WithLabelValues(append([]string{c.cap.Label(queue)}, extra...)...)
}

// Gauges is a gauge vector labelled by queue and then by any extra labels.
type Gauges struct {
	vec *prometheus.GaugeVec
	cap *Cap
}

// NewGauges creates and registers, through f, a gauge vector whose labels are
// "queue" followed by extra. Its series are reached by For.
func NewGauges(f promauto.Factory, c *Cap, opts prometheus.GaugeOpts, extra ...string) *Gauges {
	return &Gauges{vec: f.NewGaugeVec(opts, append([]string{"queue"}, extra...)), cap: c}
}

// For returns the gauge of a queue, with the values of the extra labels. The
// queue label is the cap's value for the name. It panics, as prometheus does,
// if the number of extra values is not the number of extra labels.
func (g *Gauges) For(queue string, extra ...string) prometheus.Gauge {
	return g.vec.WithLabelValues(append([]string{g.cap.Label(queue)}, extra...)...)
}
