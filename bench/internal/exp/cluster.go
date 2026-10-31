package exp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"

	jobqueue "github.com/jason-te-sde/distributed-job-queue"
	"github.com/jason-te-sde/distributed-job-queue/internal/broker"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/pgstore/migrate"
)

const (
	// benchPoolConns is the size of the pool that the harness itself uses for
	// setup, clean-up and sampling.
	benchPoolConns = 4
	// clusterShutdownGrace is how long a broker waits for running calls when the
	// cluster is closed. The workers have stopped by then, so nothing waits.
	clusterShutdownGrace = 2 * time.Second
)

// ClusterOptions describes the brokers of an experiment. They are the real
// brokers, internal/broker.Broker, running in this process on loopback ports
// over one PostgreSQL database; the zero value of every field except
// DatabaseURL is the broker's own default.
type ClusterOptions struct {
	// DatabaseURL is the PostgreSQL database. The schema is migrated if it is
	// not already.
	DatabaseURL string
	// Brokers is the number of brokers, at least 1.
	Brokers int
	// MaxDBConns is the size of each broker's database pool.
	MaxDBConns int32
	// Scheduler is "drr" or "fifo".
	Scheduler string
	// DisableListen turns the NOTIFY listener off, which leaves the fallback
	// poll.
	DisableListen bool
	// Retention and PruneInterval set how long finished jobs are kept and how
	// often they are deleted; a zero Retention keeps the broker's default.
	Retention     store.Retention
	PruneInterval time.Duration
	// Killable puts each broker behind a proxy that KillBroker can cut.
	Killable bool
}

// Cluster is a set of brokers that share a database.
type Cluster struct {
	pool    *pgxpool.Pool
	cfg     broker.Config
	brokers []*broker.Broker
	done    []chan error
	proxies []*proxy
	cancel  context.CancelFunc

	closeOnce sync.Once
	closeErr  error
}

// quietLog is the logger of the brokers: errors only, to stderr, so that a
// failure inside a broker is seen and the access log does not drown the output.
func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// discardLog is the logger of the workers.
func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// OpenDB opens a small pool on the database and migrates its schema.
func OpenDB(ctx context.Context, url string) (*pgxpool.Pool, error) {
	if url == "" {
		return nil, ErrNoDatabase
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse the database URL: %w", err)
	}
	cfg.MaxConns = benchPoolConns
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open the database: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("reach the database: %w", err)
	}
	if _, err := migrate.Up(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate the database: %w", err)
	}
	return pool, nil
}

// StartCluster migrates the database and starts the brokers. The caller must
// Close the cluster, after its workers have stopped.
func StartCluster(ctx context.Context, o ClusterOptions) (*Cluster, error) {
	if o.Brokers < 1 {
		return nil, fmt.Errorf("a cluster needs at least one broker, got %d", o.Brokers)
	}
	pool, err := OpenDB(ctx, o.DatabaseURL)
	if err != nil {
		return nil, err
	}
	cfg, err := broker.ParseFlags([]string{
		"--grpc-addr=127.0.0.1:0", "--http-addr=127.0.0.1:0", "--database-url=" + o.DatabaseURL,
	}, nil)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("broker configuration: %w", err)
	}
	cfg.DisableListen = o.DisableListen
	cfg.ShutdownGrace = clusterShutdownGrace
	if o.MaxDBConns > 0 {
		cfg.MaxDBConns = o.MaxDBConns
	}
	if o.Scheduler != "" {
		cfg.Scheduler = o.Scheduler
	}
	if o.Retention.Done > 0 {
		cfg.Retention = o.Retention
	}
	if o.PruneInterval > 0 {
		cfg.PruneInterval = o.PruneInterval
	}
	if err := cfg.Validate(); err != nil {
		pool.Close()
		return nil, fmt.Errorf("broker configuration: %w", err)
	}

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	c := &Cluster{pool: pool, cfg: cfg, cancel: cancel}
	for range o.Brokers {
		b, err := broker.New(ctx, cfg, quietLog())
		if err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("start broker %d: %w", len(c.brokers)+1, err)
		}
		done := make(chan error, 1)
		go func() { done <- b.Run(runCtx) }()
		c.brokers = append(c.brokers, b)
		c.done = append(c.done, done)
		if o.Killable {
			p, err := newProxy(b.GRPCAddr().String())
			if err != nil {
				_ = c.Close()
				return nil, fmt.Errorf("proxy broker %d: %w", len(c.brokers), err)
			}
			c.proxies = append(c.proxies, p)
		}
	}
	return c, nil
}

// Pool is the harness's own pool on the database, for setup, clean-up and
// sampling. The cluster closes it.
func (c *Cluster) Pool() *pgxpool.Pool { return c.pool }

// Config is the configuration that every broker of the cluster runs with.
func (c *Cluster) Config() broker.Config { return c.cfg }

// Addrs are the addresses clients connect to: the brokers', or their proxies'.
func (c *Cluster) Addrs() []string {
	addrs := make([]string, len(c.brokers))
	for i, b := range c.brokers {
		if len(c.proxies) > 0 {
			addrs[i] = c.proxies[i].Addr()
		} else {
			addrs[i] = b.GRPCAddr().String()
		}
	}
	return addrs
}

// Dial opens a plaintext connection that spreads calls over every broker
// round-robin, through the SDK's Dial, with extra options after the cluster's
// own. The caller closes it.
func (c *Cluster) Dial(opts ...jobqueue.DialOption) (*grpc.ClientConn, error) {
	r := manual.NewBuilderWithScheme("bench")
	var addrs []resolver.Address
	for _, a := range c.Addrs() {
		addrs = append(addrs, resolver.Address{Addr: a})
	}
	r.InitialState(resolver.State{Addresses: addrs})
	all := append([]jobqueue.DialOption{jobqueue.WithInsecure(), jobqueue.WithGRPCOptions(grpc.WithResolvers(r))}, opts...)
	return jobqueue.Dial("bench:///brokers", all...)
}

// KillBroker cuts broker i off from its clients: every connection through its
// proxy is reset and no new one is accepted, while the broker itself keeps
// running. It returns the number of client connections reset. The cluster must
// have been started Killable.
func (c *Cluster) KillBroker(i int) (int, error) {
	if len(c.proxies) == 0 {
		return 0, errors.New("the cluster was not started Killable")
	}
	if i < 0 || i >= len(c.proxies) {
		return 0, fmt.Errorf("there is no broker %d among %d", i, len(c.proxies))
	}
	return c.proxies[i].Kill(), nil
}

// Close stops the brokers, after they have drained, and releases the proxies and
// the pool. It returns the first error a broker ended with, and may be called
// more than once.
func (c *Cluster) Close() error {
	c.closeOnce.Do(func() {
		for _, p := range c.proxies {
			p.Close()
		}
		c.cancel()
		for _, done := range c.done {
			if err := <-done; err != nil && c.closeErr == nil {
				c.closeErr = err
			}
		}
		c.pool.Close()
	})
	return c.closeErr
}
