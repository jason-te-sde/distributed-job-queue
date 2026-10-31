package broker

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// get calls a path on the handler and returns the status and the trimmed body.
func get(h http.Handler, path string) (int, string) {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code, strings.TrimSpace(rec.Body.String())
}

func wantStatus(t *testing.T, h http.Handler, path string, code int, body string) {
	t.Helper()
	if gotCode, gotBody := get(h, path); gotCode != code || gotBody != body {
		t.Errorf("GET %s = %d %q, want %d %q", path, gotCode, gotBody, code, body)
	}
}

func TestReadyTransitions(t *testing.T) {
	clock := &fakeClock{now: t0}
	h := NewHealth()
	h.now = clock.Now
	handler := h.Handler()

	wantStatus(t, handler, "/readyz", 503, "database not checked")
	wantStatus(t, handler, "/healthz", 200, "ok")

	h.SetDB(true, nil)
	wantStatus(t, handler, "/readyz", 200, "ready")

	clock.Advance(4900 * time.Millisecond)
	wantStatus(t, handler, "/readyz", 200, "ready")
	clock.Advance(200 * time.Millisecond) // the report is now 5.1s old
	wantStatus(t, handler, "/readyz", 503, "database check is stale")

	// A failed check is not ready, and does not repeat the error: it may name
	// hosts.
	h.SetDB(false, errors.New("connect to 10.1.2.3: refused"))
	wantStatus(t, handler, "/readyz", 503, "database unavailable")

	h.SetDB(true, nil)
	wantStatus(t, handler, "/readyz", 200, "ready")

	h.SetDraining()
	wantStatus(t, handler, "/readyz", 503, "draining")
	wantStatus(t, handler, "/healthz", 200, "ok") // the process is alive while it drains

	// Draining wins over a database that is fine, and stays when it reports again.
	h.SetDB(true, nil)
	wantStatus(t, handler, "/readyz", 503, "draining")
}

func TestHealthServesMetricsOnlyOnceGivenAGatherer(t *testing.T) {
	h := NewHealth()
	if code, _ := get(h.Handler(), "/metrics"); code != http.StatusNotFound {
		t.Errorf("GET /metrics without a gatherer = %d, want 404", code)
	}

	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	m.enqueuedFor("q").Add(3)
	h.SetGatherer(reg)
	code, body := get(h.Handler(), "/metrics")
	if code != http.StatusOK {
		t.Fatalf("GET /metrics = %d, want 200", code)
	}
	for _, want := range []string{`jobqueue_jobs_enqueued_total{queue="q"} 3`, "go_goroutines"} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}
}
