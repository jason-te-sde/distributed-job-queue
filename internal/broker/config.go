package broker

import (
	"errors"
	"flag"
	"fmt"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

// databaseURLEnv names the environment variable that supplies DatabaseURL
// when --database-url is not given. The URL usually carries a password, so
// it is read from the environment only after flag parsing: a default shown by
// --help would print it.
const databaseURLEnv = "JOBQUEUE_DATABASE_URL"

// The bounds of a tenant's weight. An unbounded weight would let one tenant
// take every lease and starve the others.
const (
	minTenantWeight int64 = 1
	maxTenantWeight int64 = 1000
)

// Config is everything the server is told at start. ParseFlags fills it and
// Validate checks it; the flag of each field is the kebab-case of its name,
// so Config.MaxConnectionAgeGrace is --max-connection-age-grace.
type Config struct {
	// GRPCAddr and HTTPAddr are the listen addresses of the gRPC API and of
	// the HTTP endpoint that serves health checks and metrics. ScalerAddr
	// is the listen address of the autoscaler's gRPC service; empty
	// disables it. That service is plaintext and has no authentication,
	// because KEDA cannot send tokens: it is meant to be reachable only by
	// KEDA, through a NetworkPolicy, and Validate makes no demand on the
	// address.
	GRPCAddr, HTTPAddr, ScalerAddr string
	// DatabaseURL is the PostgreSQL connection URL, from --database-url or
	// else the JOBQUEUE_DATABASE_URL environment variable.
	DatabaseURL string
	// MaxDBConns is the size of the database connection pool.
	MaxDBConns int32
	// MaxNotifyConns is the most connections of that pool that the calls
	// which notify (enqueue, release and requeue) may hold at once. Their
	// transactions queue on the lock that PostgreSQL takes for each committing
	// NOTIFY, and unbounded they would take every connection and leave none for
	// leasing. Zero, the default, derives the bound from MaxDBConns: all but a
	// reserve of max(2, MaxDBConns/4), which leaves at least 2 connections to
	// the other calls from a pool of 3 up, 1 in a pool of 2, and none in a pool
	// of 1. A set value must leave one connection to them, so it is at most
	// MaxDBConns minus 1; in a pool of 1, which cannot spare one, it is 0 or 1.
	MaxNotifyConns int32
	// TokensFile names the file of token hashes that LoadTokens reads. TLSCert
	// and TLSKey name the certificate and key of the gRPC listener.
	TokensFile, TLSCert, TLSKey string
	// Insecure allows serving a non-loopback address without TLS and
	// tokens.
	Insecure bool
	// DisableListen stops the broker from listening for notifications of new
	// jobs, so that it finds work only by its fallback poll. It exists to
	// measure what the notifications buy; production leaves it off.
	DisableListen bool
	// Scheduler is how leases are shared between tenants: "drr" for
	// weighted deficit round robin, "fifo" for arrival order.
	Scheduler string
	// TenantWeights maps tenants to their weight under "drr"; a tenant that
	// is absent weighs 1.
	TenantWeights map[string]int64
	// ReapInterval and PruneInterval are how often expired leases are taken
	// back and finished jobs are deleted.
	ReapInterval, PruneInterval time.Duration
	// PollInterval is the fallback poll that finds work when no
	// notification arrives. ActivityInterval is how often the dispatcher
	// asks the store which tenants have work.
	PollInterval, ActivityInterval time.Duration
	// ReapBatch and PruneBatch are the most rows one reap or prune call
	// takes.
	ReapBatch, PruneBatch int
	// Retention is how long finished jobs are kept.
	Retention store.Retention
	// MaxInFlight is the most store fetches the dispatcher runs at once for
	// one queue.
	MaxInFlight int
	// Quantum is the number of jobs a tenant of weight 1 takes per turn
	// under "drr".
	Quantum int64
	// MaxConnectionAge is how long a client connection lives before the
	// server asks it to reconnect, which re-balances clients across brokers.
	// MaxConnectionAgeGrace is how long it then waits for running calls.
	MaxConnectionAge, MaxConnectionAgeGrace time.Duration
	// ShutdownGrace is how long a shutdown waits for running calls before it
	// closes them.
	ShutdownGrace time.Duration
	// LogLevel is "debug", "info", "warn" or "error". LogFormat is "json" or
	// "text".
	LogLevel, LogFormat string
}

// ParseFlags reads the flags of jobqueue-server from args, which excludes the
// program name, and returns the configuration. getenv looks up an
// environment variable; it may be nil. The result is not yet validated:
// Validate does that.
//
// A request for help comes back as an error that wraps flag.ErrHelp and
// carries the usage text. Any other failure comes back as the error that the
// flag package reports.
func ParseFlags(args []string, getenv func(string) string) (Config, error) {
	var c Config
	fs := flag.NewFlagSet("jobqueue-server", flag.ContinueOnError)
	var usage strings.Builder
	fs.SetOutput(&usage)

	fs.StringVar(&c.GRPCAddr, "grpc-addr", ":7070", "listen address of the gRPC API")
	fs.StringVar(&c.HTTPAddr, "http-addr", ":7071", "listen address of the health and metrics endpoint")
	fs.StringVar(&c.ScalerAddr, "scaler-addr", "", "listen address of the autoscaler service, which is unauthenticated plaintext gRPC meant to be reachable only by KEDA through a NetworkPolicy; empty disables it")
	fs.StringVar(&c.DatabaseURL, "database-url", "", "PostgreSQL connection URL; defaults to $"+databaseURLEnv)
	c.MaxDBConns = 20
	fs.Var((*int32Value)(&c.MaxDBConns), "max-db-conns", "size of the database connection pool")
	fs.Var((*int32Value)(&c.MaxNotifyConns), "max-notify-conns", "most pool connections that enqueue, release and requeue may hold at once, so that leasing finds one; 0 derives it, leaving a reserve of max(2, max-db-conns/4); at most max-db-conns minus 1, except that a pool of 1 allows 0 or 1")
	fs.StringVar(&c.TokensFile, "tokens-file", "", "file of token hashes that authenticate callers")
	fs.StringVar(&c.TLSCert, "tls-cert", "", "TLS certificate file of the gRPC listener")
	fs.StringVar(&c.TLSKey, "tls-key", "", "TLS key file of the gRPC listener")
	fs.BoolVar(&c.Insecure, "insecure", false, "serve a non-loopback address without TLS and tokens")
	fs.BoolVar(&c.DisableListen, "disable-listen", false, "do not listen for notifications of new jobs; find work by polling only (for measurements)")
	fs.StringVar(&c.Scheduler, "scheduler", "drr", "how leases are shared between tenants: drr or fifo")
	fs.Var(&weightsValue{dst: &c.TenantWeights}, "tenant-weights", "weights of tenants under drr, as tenant=weight,tenant=weight; others weigh 1")
	fs.DurationVar(&c.ReapInterval, "reap-interval", time.Second, "how often expired leases are taken back")
	fs.DurationVar(&c.PruneInterval, "prune-interval", time.Minute, "how often finished jobs are deleted")
	fs.DurationVar(&c.PollInterval, "poll-interval", time.Second, "fallback poll for work when no notification arrives")
	fs.DurationVar(&c.ActivityInterval, "activity-interval", time.Second, "how often the dispatcher asks which tenants have work")
	fs.IntVar(&c.ReapBatch, "reap-batch", 1000, "most expired leases taken back per call")
	fs.IntVar(&c.PruneBatch, "prune-batch", 1000, "most finished jobs deleted per call")
	fs.DurationVar(&c.Retention.Done, "retention-done", 24*time.Hour, "how long succeeded and cancelled jobs are kept")
	fs.DurationVar(&c.Retention.Dead, "retention-dead", 336*time.Hour, "how long dead jobs are kept")
	fs.IntVar(&c.MaxInFlight, "max-in-flight", 4, "most store fetches in flight per queue")
	fs.Int64Var(&c.Quantum, "quantum", 8, "jobs a tenant of weight 1 takes per turn under drr")
	fs.DurationVar(&c.MaxConnectionAge, "max-connection-age", 5*time.Minute, "age at which a client connection is asked to reconnect")
	fs.DurationVar(&c.MaxConnectionAgeGrace, "max-connection-age-grace", 30*time.Second, "how long running calls may outlive max-connection-age")
	fs.DurationVar(&c.ShutdownGrace, "shutdown-grace", 25*time.Second, "how long a shutdown waits for running calls")
	fs.StringVar(&c.LogLevel, "log-level", "info", "log level: debug, info, warn or error")
	fs.StringVar(&c.LogFormat, "log-format", "json", "log format: json or text")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return Config{}, fmt.Errorf("%w\n%s", err, strings.TrimRight(usage.String(), "\n"))
		}
		return Config{}, err
	}
	if fs.NArg() > 0 {
		return Config{}, fmt.Errorf("unexpected argument %q: jobqueue-server takes flags only", fs.Arg(0))
	}
	if c.DatabaseURL == "" && getenv != nil {
		c.DatabaseURL = getenv(databaseURLEnv)
	}
	return c, nil
}

// int32Value is a flag.Value for an int32.
type int32Value int32

func (v *int32Value) Set(s string) error {
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return err
	}
	*v = int32Value(n)
	return nil
}

func (v *int32Value) String() string {
	if v == nil {
		return "0"
	}
	return strconv.FormatInt(int64(*v), 10)
}

// weightsValue is a flag.Value for --tenant-weights. A later occurrence of
// the flag replaces an earlier one, as it does for every other flag.
type weightsValue struct{ dst *map[string]int64 }

func (v *weightsValue) Set(s string) error {
	m, err := parseTenantWeights(s)
	if err != nil {
		return err
	}
	*v.dst = m
	return nil
}

// String formats the weights as Set reads them, in tenant order. The flag
// package also calls it on a zero weightsValue to find the default.
func (v *weightsValue) String() string {
	if v == nil || v.dst == nil {
		return ""
	}
	tenants := make([]string, 0, len(*v.dst))
	for t := range *v.dst {
		tenants = append(tenants, t)
	}
	slices.Sort(tenants)
	parts := make([]string, len(tenants))
	for i, t := range tenants {
		parts[i] = t + "=" + strconv.FormatInt((*v.dst)[t], 10)
	}
	return strings.Join(parts, ",")
}

// parseTenantWeights reads "tenant=weight,tenant=weight". The empty string is
// no weights. A tenant must be a valid name and appear once, and a weight
// must lie within [minTenantWeight, maxTenantWeight].
func parseTenantWeights(s string) (map[string]int64, error) {
	if s == "" {
		return nil, nil
	}
	weights := map[string]int64{}
	for item := range strings.SplitSeq(s, ",") {
		tenant, weight, ok := strings.Cut(item, "=")
		if !ok {
			return nil, fmt.Errorf("tenant weight %q: want <tenant>=<weight>", item)
		}
		if err := job.ValidateName("tenant", tenant); err != nil {
			return nil, fmt.Errorf("tenant weight %q: %w", item, err)
		}
		w, err := strconv.ParseInt(weight, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("tenant weight %q: the weight must be a whole number", item)
		}
		if err := checkWeight(tenant, w); err != nil {
			return nil, err
		}
		if _, dup := weights[tenant]; dup {
			return nil, fmt.Errorf("tenant weight %q: tenant %q is given more than once", item, tenant)
		}
		weights[tenant] = w
	}
	return weights, nil
}

func checkWeight(tenant string, w int64) error {
	if w < minTenantWeight || w > maxTenantWeight {
		return fmt.Errorf("weight of tenant %q must be between %d and %d, got %d", tenant, minTenantWeight, maxTenantWeight, w)
	}
	return nil
}

// Validate checks the configuration as a whole and returns the first rule it
// breaks. It checks that the scheduler, log level and log format are ones the
// server knows; that every interval, grace, retention, batch, in-flight limit,
// quantum and pool size is positive and that a retention is a whole number of
// microseconds, as the store requires; that every tenant weight lies in
// [1, 1000]; that a database URL is present; that the TLS certificate and key
// come together; and that the gRPC address is a host and a port.
//
// It also refuses to serve an address that is not loopback (127.0.0.1, ::1 or
// localhost; a bare ":7070" is not loopback) unless TLS and a tokens file are
// both configured or Insecure is set, so that a missing flag cannot expose an
// unauthenticated queue to the network.
func (c Config) Validate() error {
	if c.DatabaseURL == "" {
		return errors.New("no database URL: pass --database-url or set " + databaseURLEnv)
	}
	if c.Scheduler != "drr" && c.Scheduler != "fifo" {
		return fmt.Errorf("--scheduler must be drr or fifo, got %q", c.Scheduler)
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("--log-level must be debug, info, warn or error, got %q", c.LogLevel)
	}
	switch c.LogFormat {
	case "json", "text":
	default:
		return fmt.Errorf("--log-format must be json or text, got %q", c.LogFormat)
	}

	for _, d := range []struct {
		flag  string
		value time.Duration
	}{
		{"reap-interval", c.ReapInterval},
		{"prune-interval", c.PruneInterval},
		{"poll-interval", c.PollInterval},
		{"activity-interval", c.ActivityInterval},
		{"retention-done", c.Retention.Done},
		{"retention-dead", c.Retention.Dead},
		{"max-connection-age", c.MaxConnectionAge},
		{"max-connection-age-grace", c.MaxConnectionAgeGrace},
		{"shutdown-grace", c.ShutdownGrace},
	} {
		if d.value <= 0 {
			return fmt.Errorf("--%s must be positive, got %v", d.flag, d.value)
		}
	}
	for _, d := range []struct {
		flag  string
		value time.Duration
	}{{"retention-done", c.Retention.Done}, {"retention-dead", c.Retention.Dead}} {
		if d.value%time.Microsecond != 0 {
			return fmt.Errorf("--%s must be a whole number of microseconds, got %v", d.flag, d.value)
		}
	}
	for _, n := range []struct {
		flag  string
		value int64
	}{
		{"max-db-conns", int64(c.MaxDBConns)},
		{"reap-batch", int64(c.ReapBatch)},
		{"prune-batch", int64(c.PruneBatch)},
		{"max-in-flight", int64(c.MaxInFlight)},
		{"quantum", c.Quantum},
	} {
		if n.value < 1 {
			return fmt.Errorf("--%s must be at least 1, got %d", n.flag, n.value)
		}
	}
	// A bound as large as the pool would turn itself off, so it must leave one
	// connection to the other calls. A pool of 1 cannot spare one: 0 and 1 are
	// its only values.
	if ceiling := max(1, c.MaxDBConns-1); c.MaxNotifyConns < 0 || c.MaxNotifyConns > ceiling {
		return fmt.Errorf("--max-notify-conns must be 0 (derive it) or between 1 and %d, which leaves a connection of --max-db-conns=%d "+
			"to the calls that do not notify (a pool of 1 allows 0 or 1), got %d", ceiling, c.MaxDBConns, c.MaxNotifyConns)
	}

	for _, tenant := range slices.Sorted(maps.Keys(c.TenantWeights)) {
		if err := job.ValidateName("tenant", tenant); err != nil {
			return fmt.Errorf("--tenant-weights: %w", err)
		}
		if err := checkWeight(tenant, c.TenantWeights[tenant]); err != nil {
			return fmt.Errorf("--tenant-weights: %w", err)
		}
	}

	if c.TLSCert != "" && c.TLSKey == "" {
		return errors.New("--tls-cert needs --tls-key: pass both or neither")
	}
	if c.TLSKey != "" && c.TLSCert == "" {
		return errors.New("--tls-key needs --tls-cert: pass both or neither")
	}

	host, port, err := net.SplitHostPort(c.GRPCAddr)
	if err != nil {
		return fmt.Errorf("--grpc-addr %q is not a host:port address: %w", c.GRPCAddr, err)
	}
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return fmt.Errorf("--grpc-addr %q: the port must be a number from 0 to 65535", c.GRPCAddr)
	}
	secured := c.TLSCert != "" && c.TokensFile != ""
	if !isLoopbackHost(host) && !secured && !c.Insecure {
		return fmt.Errorf("refusing to serve %s without TLS and tokens; pass --insecure to override", c.GRPCAddr)
	}
	return nil
}

// isLoopbackHost reports whether host is one of the three names that mean
// this machine only. The list is deliberately short: any other name could
// reach a network, and the empty host of ":7070" listens on every interface.
func isLoopbackHost(host string) bool {
	return host == "127.0.0.1" || host == "::1" || host == "localhost"
}
