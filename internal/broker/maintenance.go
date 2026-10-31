package broker

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

const (
	// statsInterval is how often the jobs-by-state gauges are refreshed.
	statsInterval = 15 * time.Second
	// pingInterval is how often the database is checked for /readyz. A report
	// goes stale after 5s, so two checks fit before one does.
	pingInterval = 2 * time.Second
	// dbPingTimeout bounds one database check.
	dbPingTimeout = 2 * time.Second
	// statsTimeout bounds one Stats call of the refresher.
	statsTimeout = 10 * time.Second
)

// RunReaper takes back expired leases until ctx is done. Every `every` it calls
// Reap for up to batch jobs, delaying each retry by the default backoff for the
// job's attempts with a draw from a generator seeded with seed; a call that
// took back a full batch is followed at once by another, because more are
// probably waiting. A failed call is logged and the loop carries on. The
// reaper runs on every broker: the store's Reap locks the rows it takes, so
// several of them do not take the same job twice.
func RunReaper(ctx context.Context, st store.Store, every time.Duration, batch int, seed uint64, m *Metrics, log *slog.Logger) {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	// The store calls this synchronously inside Reap, so the generator is only
	// ever used from this goroutine.
	backoff := func(attempts int32) time.Duration { return job.DefaultBackoff.Delay(attempts, rng.Float64()) }
	runWhileFull(ctx, every, batch, func(ctx context.Context) (int, error) {
		n, err := st.Reap(ctx, batch, backoff)
		if err != nil {
			log.Warn("reap failed", slog.String("op", "reap"), slog.Any("err", err))
			return 0, err
		}
		if n > 0 {
			m.reaped.Add(float64(n))
			log.Info("took back expired leases", slog.String("op", "reap"), slog.Int("jobs", n))
		}
		return n, nil
	})
}

// RunPruner deletes finished jobs that outlived their retention until ctx is
// done. Every `every` it calls Prune for up to batch jobs, and a call that
// deleted a full batch is followed at once by another. A failed call is logged
// and the loop carries on.
func RunPruner(ctx context.Context, st store.Store, every time.Duration, batch int, r store.Retention, m *Metrics, log *slog.Logger) {
	runWhileFull(ctx, every, batch, func(ctx context.Context) (int, error) {
		n, err := st.Prune(ctx, r, batch)
		if err != nil {
			log.Warn("prune failed", slog.String("op", "prune"), slog.Any("err", err))
			return 0, err
		}
		if n > 0 {
			m.pruned.Add(float64(n))
			log.Info("deleted finished jobs", slog.String("op", "prune"), slog.Int("jobs", n))
		}
		return n, nil
	})
}

// runWhileFull calls pass on every tick until ctx is done. A pass that returns
// batch repeats at once; one that returns less, or fails, waits for the next
// tick.
func runWhileFull(ctx context.Context, every time.Duration, batch int, pass func(context.Context) (int, error)) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for ctx.Err() == nil {
			if n, err := pass(ctx); err != nil || n < batch {
				break
			}
		}
	}
}

// runStatsRefresher refreshes the jobs-by-state gauges now and then every
// `every`, until ctx is done.
func runStatsRefresher(ctx context.Context, st store.Store, every time.Duration, m *Metrics, log *slog.Logger) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		refreshQueueJobs(ctx, st, m, log)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// refreshQueueJobs reads the stats of every queue that has a label of its own
// and sets its gauges. The store cannot list queues, so the queues are the ones
// this broker has seen.
func refreshQueueJobs(ctx context.Context, st store.Store, m *Metrics, log *slog.Logger) {
	for _, queue := range m.labeledQueues() {
		if ctx.Err() != nil {
			return
		}
		callCtx, cancel := context.WithTimeout(ctx, statsTimeout)
		stats, err := st.Stats(callCtx, queue)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				log.Warn("stats refresh failed", slog.String("op", "stats"), slog.String("queue", queue), slog.Any("err", err))
			}
			continue
		}
		var c queueJobCounts
		for _, s := range stats {
			c.ready += s.Ready
			c.scheduled += s.Scheduled
			c.leased += s.Leased
			c.dead += s.Dead
		}
		m.setQueueJobs(queue, c)
	}
}

// pinger is what the database check needs of a connection pool.
type pinger interface {
	Ping(ctx context.Context) error
}

// runPinger checks the database now and then every `every` until ctx is done,
// and reports each result to h. It logs when the answer changes, not on every
// check.
func runPinger(ctx context.Context, p pinger, h *Health, every time.Duration, log *slog.Logger) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	healthy := true
	for {
		callCtx, cancel := context.WithTimeout(ctx, dbPingTimeout)
		err := p.Ping(callCtx)
		cancel()
		if ctx.Err() != nil {
			return
		}
		h.SetDB(err == nil, err)
		if ok := err == nil; ok != healthy {
			healthy = ok
			if ok {
				log.Info("the database answers again")
			} else {
				log.Warn("the database does not answer", slog.Any("err", err))
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
