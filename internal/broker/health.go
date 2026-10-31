package broker

import (
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// dbStaleAfter is how old a report of the database check may be before it no
// longer counts: a pinger that stopped is not a database that is fine.
const dbStaleAfter = 5 * time.Second

// dbReport is the result of one database check and the time it was made.
type dbReport struct {
	at time.Time
	ok bool
}

// Health is what the HTTP endpoint answers its probes with. The process is
// alive as long as it answers at all, so /healthz never fails, not even while
// the broker drains: killing it then would cut the calls that the grace
// period is for. /readyz is the one that says whether to send it work: it
// fails while the broker drains, and while the database has not been checked
// recently or failed its last check.
//
// The endpoint carries no authentication, as probes and scrapers do not send
// tokens.
type Health struct {
	// now is the clock that dates the reports; a test replaces it.
	now      func() time.Time
	db       atomic.Pointer[dbReport]
	draining atomic.Bool
	// gatherer is the registry that /metrics serves; see SetGatherer.
	gatherer prometheus.Gatherer
}

// NewHealth returns a Health that has not heard from the database yet, so that
// /readyz fails until the first report.
func NewHealth() *Health { return &Health{now: time.Now} }

// SetDB records the result of a database check: ok is whether it succeeded.
// The error is the caller's to log: /readyz says only that the database is
// unavailable, because the error may name hosts and users.
func (h *Health) SetDB(ok bool, _ error) {
	h.db.Store(&dbReport{at: h.now(), ok: ok})
}

// SetDraining makes /readyz fail from now on, so that load balancers stop
// sending the broker new calls while the ones it has finish.
func (h *Health) SetDraining() { h.draining.Store(true) }

// SetGatherer makes /metrics serve g. It must be called before Handler.
func (h *Health) SetGatherer(g prometheus.Gatherer) { h.gatherer = g }

// ready says whether the broker should receive work, and why not.
func (h *Health) ready() (bool, string) {
	if h.draining.Load() {
		return false, "draining"
	}
	r := h.db.Load()
	switch {
	case r == nil:
		return false, "database not checked"
	case h.now().Sub(r.at) > dbStaleAfter:
		return false, "database check is stale"
	case !r.ok:
		return false, "database unavailable"
	}
	return true, "ready"
}

// Handler serves /healthz, which answers 200 "ok" while the process runs;
// /readyz, which answers 200 "ready" or 503 with the reason; and, when a
// gatherer was set, /metrics.
func (h *Health) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeText(w, http.StatusOK, "ok")
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if ok, reason := h.ready(); !ok {
			writeText(w, http.StatusServiceUnavailable, reason)
			return
		}
		writeText(w, http.StatusOK, "ready")
	})
	if h.gatherer != nil {
		mux.Handle("/metrics", promhttp.HandlerFor(h.gatherer, promhttp.HandlerOpts{}))
	}
	return mux
}

func writeText(w http.ResponseWriter, code int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(code)
	_, _ = w.Write([]byte(body + "\n"))
}
