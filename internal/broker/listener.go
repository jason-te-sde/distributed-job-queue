package broker

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/jason-te-sde/distributed-job-queue/internal/store/pgstore"
)

const (
	// listenerAppName is the application name of the listener's connection,
	// which is how an operator, and the tests, find it in pg_stat_activity.
	listenerAppName = "jobqueue-listener"
	// The delay before a reconnect is drawn uniformly below a ceiling that
	// starts at reconnectBase, doubles with each failure and stops at
	// reconnectCap.
	reconnectBase = 100 * time.Millisecond
	reconnectCap  = 5 * time.Second
	// connectTimeout bounds opening the connection, and pingTimeout one
	// liveness check.
	connectTimeout = 10 * time.Second
	pingTimeout    = 5 * time.Second
	// defaultPingEvery is how long the connection may be silent before it is
	// checked. A connection that a firewall dropped without a reset would
	// otherwise leave the listener deaf, with nothing but the fallback poll to
	// find work.
	defaultPingEvery = 30 * time.Second
)

// Listener turns the notifications of the store into Notify calls on a
// dispatcher. It holds one dedicated connection, not one from the pool, because
// LISTEN belongs to a session and a pooled session is shared and recycled.
//
// A notification is only a hint that work may exist, and the connection loses
// them while it is down, so a reconnect ends with a Resync of the dispatcher.
type Listener struct {
	url string
	d   *Dispatcher
	m   *Metrics
	log *slog.Logger

	// rng draws the reconnect jitter; only Run uses it.
	rng *rand.Rand
	// backoff returns the delay before the reconnect that follows the given
	// number of consecutive failures. A test replaces it.
	backoff func(attempt int) time.Duration
	// pingEvery is how long the connection may be idle before it is checked.
	pingEvery time.Duration
	// listens counts the times the connection has started to listen.
	listens atomic.Int64
}

// NewListener returns a listener that will connect to the database at url and
// feed d. Its jitter comes from a generator seeded with the clock, because
// brokers that lose the database together should not come back together.
func NewListener(url string, d *Dispatcher, m *Metrics, log *slog.Logger) *Listener {
	seed := uint64(time.Now().UnixNano())
	l := &Listener{
		url: url, d: d, m: m, log: log,
		rng:       rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		pingEvery: defaultPingEvery,
	}
	l.backoff = func(attempt int) time.Duration { return reconnectDelay(attempt, l.rng.Float64()) }
	return l
}

// reconnectDelay is the full-jitter delay after the given number of consecutive
// failures, for a uniform draw u in [0, 1): u times a ceiling of 100ms, 200ms,
// 400ms and so on up to 5s.
func reconnectDelay(attempt int, u float64) time.Duration {
	ceiling := reconnectCap
	if attempt < 6 { // 100ms << 6 is already over the cap; this also keeps the shift small
		ceiling = min(reconnectBase<<max(attempt, 0), reconnectCap)
	}
	return time.Duration(float64(ceiling) * u)
}

// Run connects, listens and forwards notifications until ctx is done, when it
// returns nil. Any error, in connecting or listening or while waiting,
// counts as a reconnect: the listener backs off with full jitter and starts
// again, and after each LISTEN it asks the dispatcher to resync.
func (l *Listener) Run(ctx context.Context) error {
	attempt := 0
	for ctx.Err() == nil {
		listened, err := l.session(ctx)
		if ctx.Err() != nil {
			return nil
		}
		l.m.notifyReconnects.Inc()
		if listened {
			attempt = 0 // the connection worked, so this is a new outage
		}
		delay := l.backoff(attempt)
		attempt++
		l.log.Warn("notify listener lost its connection", slog.Any("err", err), slog.Duration("retry_in", delay))
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil
		}
	}
	return nil
}

// session is one connection: connect, LISTEN, then forward notifications until
// something fails. It always returns an error, and says whether LISTEN had
// succeeded by then.
func (l *Listener) session(ctx context.Context) (listened bool, err error) {
	cfg, err := pgx.ParseConfig(l.url)
	if err != nil {
		return false, fmt.Errorf("parse the database url: %w", err)
	}
	cfg.RuntimeParams["application_name"] = listenerAppName
	connectCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	conn, err := pgx.ConnectConfig(connectCtx, cfg)
	cancel()
	if err != nil {
		return false, fmt.Errorf("connect: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), pingTimeout)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{pgstore.NotifyChannel}.Sanitize()); err != nil {
		return false, fmt.Errorf("listen: %w", err)
	}
	l.listens.Add(1)
	// Whatever was enqueued while the connection was down was announced to
	// nobody.
	l.d.Resync()

	for {
		waitCtx, cancel := context.WithTimeout(ctx, l.pingEvery)
		n, err := conn.WaitForNotification(waitCtx)
		cancel()
		if n != nil {
			l.forward(n)
		}
		switch {
		case err == nil:
		case ctx.Err() != nil:
			return true, ctx.Err()
		case pgconn.Timeout(err):
			// Idle for pingEvery: make sure the connection is still there.
			pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
			err := conn.Ping(pingCtx)
			cancel()
			if err != nil {
				return true, fmt.Errorf("ping: %w", err)
			}
		default:
			return true, fmt.Errorf("wait for a notification: %w", err)
		}
	}
}

// forward hands one notification to the dispatcher. Anyone with access to the
// database can send one, so a payload that is not a queue and a tenant is
// counted and dropped.
func (l *Listener) forward(n *pgconn.Notification) {
	queue, tenant, ok := pgstore.ParseNotifyPayload(n.Payload)
	if !ok {
		l.m.notifyMalformed.Inc()
		l.log.Debug("malformed notification", slog.String("channel", n.Channel), slog.Int("bytes", len(n.Payload)))
		return
	}
	l.d.Notify(queue, tenant)
}
