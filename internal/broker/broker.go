package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"github.com/jason-te-sde/distributed-job-queue/internal/gen/externalscaler"
	jobqueuev1 "github.com/jason-te-sde/distributed-job-queue/internal/gen/jobqueue/v1"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/pgstore"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/pgstore/migrate"
)

const (
	// maxMessageBytes is the largest gRPC message in either direction: 64 jobs
	// of 256 KiB, with room for the rest of the response.
	maxMessageBytes = 20 << 20
	// httpShutdownTimeout bounds closing the health endpoint at the end of a
	// shutdown.
	httpShutdownTimeout = 5 * time.Second
	// sdkPingEvery is how often the SDK pings a connection while a call is in
	// flight (keepalive.ClientParameters.Time of 30s, with a timeout of 10s).
	sdkPingEvery = 30 * time.Second
)

// keepaliveEnforcement is the policy for the pings that clients send. gRPC's
// default admits a ping only every 5 minutes, and closes a connection whose
// client sends a third one early, so the SDK's ping every sdkPingEvery would
// cost a worker its connection and every call on it. A MinTime of 10s admits
// that ping, and PermitWithoutStream admits it while the connection is idle,
// which a worker's is between long polls; a client that pings faster than that
// still earns its strikes.
func keepaliveEnforcement() keepalive.EnforcementPolicy {
	return keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}
}

// Broker is the whole server: the gRPC API over a PostgreSQL store, the
// dispatcher and listener that serve long polls, the reaper and pruner, the
// HTTP endpoint with health checks and metrics, and, when ScalerAddr is set,
// the autoscaler service for KEDA. New builds it and binds its ports, and Run
// serves until its context ends and then shuts down in order.
type Broker struct {
	cfg  Config
	log  *slog.Logger
	seed uint64

	pool       *pgxpool.Pool
	st         store.Store
	reg        *prometheus.Registry
	metrics    *Metrics
	server     *Server
	dispatcher *Dispatcher
	listener   *Listener // nil when Config.DisableListen is set
	health     *Health

	grpcServer *grpc.Server
	httpServer *http.Server
	grpcLis    net.Listener
	httpLis    net.Listener

	// scaler, scalerServer and scalerLis are nil unless Config.ScalerAddr is
	// set. The scaler is served by a gRPC server of its own, so that the
	// authentication interceptor of grpcServer does not reach it.
	scaler       *Scaler
	scalerServer *grpc.Server
	scalerLis    net.Listener
}

// loadAuth reads the tokens file named by path. The result is nil, which makes
// every caller an admin, if and only if path is empty. A file that cannot be
// read or parsed is an error, never an open API.
func loadAuth(path string) (*Authenticator, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("tokens file: %w", err)
	}
	defer func() { _ = f.Close() }()
	auth, err := LoadTokens(f)
	if err != nil {
		return nil, err
	}
	return auth, nil
}

// New validates cfg and builds a broker. It checks everything that can be
// checked without the database first (the tokens file, the TLS files), then
// connects, and refuses to go on if the schema is not the one this binary was
// built for: the error is a *migrate.SchemaMismatchError, unchanged. Finally
// it binds the gRPC and HTTP addresses, so that GRPCAddr and HTTPAddr are
// known, and a listener on port 0 has its port, before Run. The scaler address
// is bound last, and only if it is set.
//
// The caller must call Run, which releases what New acquired. The version
// that jobqueue_build_info reports is "dev" until SetVersion says otherwise.
func New(ctx context.Context, cfg Config, log *slog.Logger) (*Broker, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	auth, err := loadAuth(cfg.TokensFile)
	if err != nil {
		return nil, err
	}
	if cfg.TokensFile != "" && auth == nil {
		return nil, errors.New("tokens file configured but no authenticator built: refusing to serve unauthenticated")
	}
	var creds credentials.TransportCredentials
	if cfg.TLSCert != "" {
		if creds, err = credentials.NewServerTLSFromFile(cfg.TLSCert, cfg.TLSKey); err != nil {
			return nil, fmt.Errorf("load the TLS certificate and key: %w", err)
		}
	}

	pool, err := pgstore.Open(ctx, cfg.DatabaseURL, cfg.MaxDBConns)
	if err != nil {
		return nil, fmt.Errorf("open the database: %w", err)
	}
	if err := migrate.Check(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}

	b := &Broker{cfg: cfg, log: log, pool: pool, reg: prometheus.NewRegistry(), health: NewHealth()}
	// The jitter of retries and the reconnect delays differ from broker to
	// broker, so brokers that lose the database together do not come back
	// together.
	b.seed = uint64(time.Now().UnixNano())
	b.metrics = NewMetrics(b.reg)
	b.metrics.SetBuildInfo("dev")
	b.health.SetGatherer(b.reg)
	b.st = timedStore{Store: pgstore.New(pool), m: b.metrics}
	b.dispatcher = NewDispatcher(b.st, DispatcherConfig{
		Scheduler: cfg.Scheduler, Weights: cfg.TenantWeights, Quantum: cfg.Quantum,
		MaxInFlight: cfg.MaxInFlight, PollInterval: cfg.PollInterval, ActivityInterval: cfg.ActivityInterval,
	}, b.metrics, log)
	b.server = NewServer(b.st, b.dispatcher, b.metrics, log, b.seed)
	if !cfg.DisableListen {
		b.listener = NewListener(cfg.DatabaseURL, b.dispatcher, b.metrics, log)
	}

	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(UnaryAuth(auth)),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge: cfg.MaxConnectionAge, MaxConnectionAgeGrace: cfg.MaxConnectionAgeGrace,
		}),
		grpc.KeepaliveEnforcementPolicy(keepaliveEnforcement()),
		grpc.MaxRecvMsgSize(maxMessageBytes),
		grpc.MaxSendMsgSize(maxMessageBytes),
	}
	if creds != nil {
		opts = append(opts, grpc.Creds(creds))
	}
	b.grpcServer = grpc.NewServer(opts...)
	jobqueuev1.RegisterJobQueueServiceServer(b.grpcServer, b.server)
	b.httpServer = &http.Server{Handler: b.health.Handler(), ReadHeaderTimeout: 10 * time.Second}
	if cfg.ScalerAddr != "" {
		// The scaler is served on a port of its own, without UnaryAuth: KEDA
		// cannot send bearer tokens. Nothing here puts it behind the
		// interceptor above, and the deployment's network policy is what
		// restricts who can reach the port. It is served without TLS, which
		// is how KEDA dials an external scaler unless the trigger asks
		// for TLS.
		b.scaler = NewScaler(b.st, 0)
		b.scaler.log = log
		b.scalerServer = grpc.NewServer()
		externalscaler.RegisterExternalScalerServer(b.scalerServer, b.scaler)
	}

	if b.grpcLis, err = net.Listen("tcp", cfg.GRPCAddr); err != nil {
		pool.Close()
		return nil, fmt.Errorf("listen on the gRPC address: %w", err)
	}
	if b.httpLis, err = net.Listen("tcp", cfg.HTTPAddr); err != nil {
		_ = b.grpcLis.Close()
		pool.Close()
		return nil, fmt.Errorf("listen on the HTTP address: %w", err)
	}
	if b.scaler != nil {
		if b.scalerLis, err = net.Listen("tcp", cfg.ScalerAddr); err != nil {
			_ = b.grpcLis.Close()
			_ = b.httpLis.Close()
			pool.Close()
			return nil, fmt.Errorf("listen on the scaler address: %w", err)
		}
	}
	return b, nil
}

// SetVersion sets the version that jobqueue_build_info reports.
func (b *Broker) SetVersion(version string) { b.metrics.SetBuildInfo(version) }

// GRPCAddr returns the address the gRPC API is bound to.
func (b *Broker) GRPCAddr() net.Addr { return b.grpcLis.Addr() }

// HTTPAddr returns the address the health and metrics endpoint is bound to.
func (b *Broker) HTTPAddr() net.Addr { return b.httpLis.Addr() }

// ScalerAddr returns the address the autoscaler service is bound to, or nil
// if Config.ScalerAddr was empty and there is none.
func (b *Broker) ScalerAddr() net.Addr {
	if b.scalerLis == nil {
		return nil
	}
	return b.scalerLis.Addr()
}

// Run serves until ctx is done, then shuts down and returns nil; it returns an
// error only if a component failed on its own, after the same shutdown.
//
// The shutdown goes in this order: /readyz starts to fail, so that the
// endpoint is taken out of rotation; Lease calls are refused, long polls end
// empty and the streams of the autoscaler end; the gRPC server and the
// autoscaler server stop accepting calls and wait for the running ones,
// workers still being able to heartbeat and acknowledge, for at most
// ShutdownGrace between them, after which they cut the rest off; the
// background loops stop; the health endpoint closes; and the database pool is
// released.
func (b *Broker) Run(ctx context.Context) error {
	// The loops outlive ctx: they run until the calls that need them are done.
	bg, stopBackground := context.WithCancel(context.WithoutCancel(ctx))
	defer stopBackground()

	var wg sync.WaitGroup
	failed := make(chan error, 8)
	start := func(name string, run func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := run(); err != nil {
				select {
				case failed <- fmt.Errorf("%s: %w", name, err):
				default:
				}
			}
		}()
	}
	cfg := b.cfg
	start("gRPC server", func() error { return b.grpcServer.Serve(b.grpcLis) })
	if b.scalerServer != nil {
		start("scaler server", func() error { return b.scalerServer.Serve(b.scalerLis) })
	}
	start("HTTP server", func() error {
		if err := b.httpServer.Serve(b.httpLis); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	start("dispatcher", func() error { return b.dispatcher.Run(bg) })
	if b.listener != nil { // nil under DisableListen, which leaves only the fallback poll
		start("notify listener", func() error { return b.listener.Run(bg) })
	}
	start("reaper", func() error {
		RunReaper(bg, b.st, cfg.ReapInterval, cfg.ReapBatch, b.seed, b.metrics, b.log)
		return nil
	})
	start("pruner", func() error {
		RunPruner(bg, b.st, cfg.PruneInterval, cfg.PruneBatch, cfg.Retention, b.metrics, b.log)
		return nil
	})
	start("stats refresher", func() error {
		runStatsRefresher(bg, b.st, statsInterval, b.metrics, b.log)
		return nil
	})
	start("database pinger", func() error {
		runPinger(bg, b.pool, b.health, pingInterval, b.log)
		return nil
	})
	attrs := []any{slog.String("grpc", b.GRPCAddr().String()), slog.String("http", b.HTTPAddr().String())}
	if addr := b.ScalerAddr(); addr != nil {
		attrs = append(attrs, slog.String("scaler", addr.String()))
	}
	b.log.Info("broker started", attrs...)

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-failed:
		b.log.Error("a component failed, shutting down", slog.Any("err", runErr))
	}

	b.log.Info("shutting down")
	b.health.SetDraining()
	b.server.SetDraining()
	b.dispatcher.Drain()
	if b.scaler != nil {
		b.scaler.Drain()
	}
	b.stopGRPC()
	stopBackground()
	b.stopHTTP()
	wg.Wait()
	b.pool.Close()
	b.log.Info("broker stopped")
	return runErr
}

// stopGRPC stops the gRPC server and, if there is one, the autoscaler server
// gracefully, which waits for running calls, and cuts them off if they outlast
// ShutdownGrace. The two stop side by side, so that together they have one
// grace period and not two.
func (b *Broker) stopGRPC() {
	servers := []*grpc.Server{b.grpcServer}
	if b.scalerServer != nil {
		servers = append(servers, b.scalerServer)
	}
	var stopping sync.WaitGroup
	for _, srv := range servers {
		stopping.Go(srv.GracefulStop)
	}
	done := make(chan struct{})
	go func() {
		stopping.Wait()
		close(done)
	}()
	timer := time.NewTimer(b.cfg.ShutdownGrace)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		b.log.Warn("calls outlasted the shutdown grace period, cutting them off", slog.Duration("grace", b.cfg.ShutdownGrace))
		for _, srv := range servers {
			srv.Stop()
		}
		<-done
	}
}

// stopHTTP closes the health endpoint.
func (b *Broker) stopHTTP() {
	ctx, cancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
	defer cancel()
	if err := b.httpServer.Shutdown(ctx); err != nil {
		_ = b.httpServer.Close()
	}
}
