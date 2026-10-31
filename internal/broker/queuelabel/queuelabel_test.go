package queuelabel

import (
	"fmt"
	"slices"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

func TestCapGivesTheFirstNamesTheirOwnLabel(t *testing.T) {
	c := NewCap(3, "other")
	for _, q := range []string{"a", "b", "c"} {
		if got := c.Label(q); got != q {
			t.Errorf("Label(%q) = %q, want the name itself", q, got)
		}
	}
	if got := c.Label("d"); got != "other" {
		t.Errorf("Label of the fourth name = %q, want other", got)
	}
	if got := c.Label("a"); got != "a" {
		t.Errorf("Label(a) after the cap was reached = %q, want a: a name keeps its label", got)
	}
	if got := c.Label("d"); got != "other" {
		t.Errorf("Label(d) the second time = %q, want other", got)
	}
	if got, want := c.Names(), []string{"a", "b", "c"}; !slices.Equal(got, want) {
		t.Errorf("Names() = %v, want %v: only names with a label of their own", got, want)
	}
}

func TestCountersAndGaugesApplyTheCapAndKeepTheQueueFirst(t *testing.T) {
	reg := prometheus.NewRegistry()
	f := promauto.With(reg)
	c := NewCap(1, "other")
	counters := NewCounters(f, c, prometheus.CounterOpts{Name: "things_total", Help: "h"}, "result")
	gauges := NewGauges(f, c, prometheus.GaugeOpts{Name: "level", Help: "h"})

	counters.For("a", "ok").Inc()
	counters.For("b", "ok").Inc()
	counters.For("c", "ok").Add(2)
	gauges.For("b").Set(5)

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for _, mf := range families {
		for _, m := range mf.GetMetric() {
			var key string
			for _, l := range m.GetLabel() {
				key += fmt.Sprintf("%s=%s,", l.GetName(), l.GetValue())
			}
			v := m.GetCounter().GetValue()
			if m.GetGauge() != nil {
				v = m.GetGauge().GetValue()
			}
			got[mf.GetName()+"{"+key+"}"] = v
		}
	}
	want := map[string]float64{
		"things_total{queue=a,result=ok,}":     1,
		"things_total{queue=other,result=ok,}": 3, // b and c share the label
		"level{queue=other,}":                  5,
	}
	if len(got) != len(want) {
		t.Errorf("series = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("series %s = %v, want %v (all: %v)", k, got[k], v, got)
		}
	}
}
