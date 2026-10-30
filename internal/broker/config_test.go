package broker

import (
	"errors"
	"flag"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

func noEnv(string) string { return "" }

func envWith(key, value string) func(string) string {
	return func(k string) string {
		if k == key {
			return value
		}
		return ""
	}
}

// validConfig returns a Config that passes Validate: a loopback listener, so
// that no TLS or tokens are needed, and every other setting at its default.
func validConfig(t *testing.T) Config {
	t.Helper()
	c, err := ParseFlags([]string{"--grpc-addr=127.0.0.1:7070", "--database-url=postgres://db/jobs"}, noEnv)
	if err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate(defaults on loopback): %v", err)
	}
	return c
}

func TestParseFlagsDefaults(t *testing.T) {
	got, err := ParseFlags(nil, noEnv)
	if err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}
	want := Config{
		GRPCAddr:              ":7070",
		HTTPAddr:              ":7071",
		ScalerAddr:            "",
		MaxDBConns:            20,
		Scheduler:             "drr",
		ReapInterval:          time.Second,
		PruneInterval:         time.Minute,
		PollInterval:          time.Second,
		ActivityInterval:      time.Second,
		ReapBatch:             1000,
		PruneBatch:            1000,
		Retention:             store.Retention{Done: 24 * time.Hour, Dead: 336 * time.Hour},
		MaxInFlight:           4,
		Quantum:               8,
		MaxConnectionAge:      5 * time.Minute,
		MaxConnectionAgeGrace: 30 * time.Second,
		ShutdownGrace:         25 * time.Second,
		LogLevel:              "info",
		LogFormat:             "json",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("defaults differ\n got: %+v\nwant: %+v", got, want)
	}
}

// TestParseFlagsEveryFlag sets every flag to a value of its own, so a flag
// bound to the wrong field, or spelled differently from the Helm chart's
// arguments, fails here.
func TestParseFlagsEveryFlag(t *testing.T) {
	args := []string{
		"--grpc-addr=127.0.0.1:1",
		"--http-addr=127.0.0.1:2",
		"--scaler-addr=127.0.0.1:3",
		"--database-url=postgres://flag/db",
		"--max-db-conns=7",
		"--tokens-file=/etc/tokens",
		"--tls-cert=/etc/tls.crt",
		"--tls-key=/etc/tls.key",
		"--insecure",
		"--scheduler=fifo",
		"--tenant-weights=a=4,b=1",
		"--reap-interval=2s",
		"--prune-interval=3m",
		"--poll-interval=4s",
		"--activity-interval=5s",
		"--reap-batch=11",
		"--prune-batch=12",
		"--retention-done=13h",
		"--retention-dead=14h",
		"--max-in-flight=15",
		"--quantum=16",
		"--max-connection-age=17m",
		"--max-connection-age-grace=18s",
		"--shutdown-grace=19s",
		"--log-level=debug",
		"--log-format=text",
	}
	got, err := ParseFlags(args, noEnv)
	if err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}
	want := Config{
		GRPCAddr:              "127.0.0.1:1",
		HTTPAddr:              "127.0.0.1:2",
		ScalerAddr:            "127.0.0.1:3",
		DatabaseURL:           "postgres://flag/db",
		MaxDBConns:            7,
		TokensFile:            "/etc/tokens",
		TLSCert:               "/etc/tls.crt",
		TLSKey:                "/etc/tls.key",
		Insecure:              true,
		Scheduler:             "fifo",
		TenantWeights:         map[string]int64{"a": 4, "b": 1},
		ReapInterval:          2 * time.Second,
		PruneInterval:         3 * time.Minute,
		PollInterval:          4 * time.Second,
		ActivityInterval:      5 * time.Second,
		ReapBatch:             11,
		PruneBatch:            12,
		Retention:             store.Retention{Done: 13 * time.Hour, Dead: 14 * time.Hour},
		MaxInFlight:           15,
		Quantum:               16,
		MaxConnectionAge:      17 * time.Minute,
		MaxConnectionAgeGrace: 18 * time.Second,
		ShutdownGrace:         19 * time.Second,
		LogLevel:              "debug",
		LogFormat:             "text",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("config differs\n got: %+v\nwant: %+v", got, want)
	}
}

func TestParseFlagsEnvDatabaseURL(t *testing.T) {
	const envName = "JOBQUEUE_DATABASE_URL"
	tests := []struct {
		name string
		args []string
		env  func(string) string
		want string
	}{
		{"env alone", nil, envWith(envName, "postgres://env/db"), "postgres://env/db"},
		{"flag beats env", []string{"--database-url=postgres://flag/db"}, envWith(envName, "postgres://env/db"), "postgres://flag/db"},
		{"neither", nil, noEnv, ""},
		{"no getenv at all", nil, nil, ""},
		{"another variable is ignored", nil, envWith("DATABASE_URL", "postgres://other/db"), ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, err := ParseFlags(tc.args, tc.env)
			if err != nil {
				t.Fatal(err)
			}
			if c.DatabaseURL != tc.want {
				t.Fatalf("DatabaseURL = %q, want %q", c.DatabaseURL, tc.want)
			}
		})
	}
}

func TestParseTenantWeights(t *testing.T) {
	tests := []struct {
		value   string
		want    map[string]int64
		wantErr string // a fragment of the error, empty when the value must parse
	}{
		{value: "a=4,b=1", want: map[string]int64{"a": 4, "b": 1}},
		{value: "a=1", want: map[string]int64{"a": 1}},
		{value: "a=1000", want: map[string]int64{"a": 1000}},
		{value: "", want: nil},
		{value: "a", wantErr: "<tenant>=<weight>"},
		{value: "a=0", wantErr: "between 1 and 1000"},
		{value: "a=x", wantErr: "weight"},
		{value: "a=-3", wantErr: "between 1 and 1000"},
		{value: "a=1001", wantErr: "between 1 and 1000"},
		{value: "a=99999999999999999999", wantErr: "weight"},
		{value: "a=", wantErr: "weight"},
		{value: "=4", wantErr: "tenant"},
		{value: "a=1,a=2", wantErr: "more than once"},
		{value: "a=1,", wantErr: "<tenant>=<weight>"},
		{value: "-lead=2", wantErr: "tenant"},
		{value: "a b=2", wantErr: "tenant"},
		{value: "a=1=2", wantErr: "weight"},
	}
	for _, tc := range tests {
		t.Run(tc.value, func(t *testing.T) {
			c, err := ParseFlags([]string{"--tenant-weights=" + tc.value}, noEnv)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("ParseFlags accepted %q: %v", tc.value, c.TenantWeights)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not mention %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseFlags(%q): %v", tc.value, err)
			}
			if !reflect.DeepEqual(c.TenantWeights, tc.want) {
				t.Fatalf("TenantWeights = %v, want %v", c.TenantWeights, tc.want)
			}
		})
	}
}

func TestParseFlagsRejects(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"unknown flag", []string{"--no-such-flag"}},
		{"positional argument", []string{"serve"}},
		{"argument after the flags", []string{"--insecure", "extra"}},
		{"bad duration", []string{"--reap-interval=soon"}},
		{"bad integer", []string{"--reap-batch=many"}},
		{"bad int32", []string{"--max-db-conns=lots"}},
		{"int32 overflow", []string{"--max-db-conns=3000000000"}},
		{"bad bool", []string{"--insecure=maybe"}},
		{"missing value", []string{"--grpc-addr"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseFlags(tc.args, noEnv); err == nil {
				t.Fatalf("ParseFlags(%v) succeeded", tc.args)
			}
		})
	}
}

func TestParseFlagsHelp(t *testing.T) {
	_, err := ParseFlags([]string{"-h"}, noEnv)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("err = %v, want flag.ErrHelp", err)
	}
	for _, name := range []string{"grpc-addr", "tenant-weights", "max-connection-age-grace", "insecure"} {
		if !strings.Contains(err.Error(), "-"+name) {
			t.Errorf("usage does not list -%s:\n%v", name, err)
		}
	}
}

func TestValidateRefusesInsecureNonLoopback(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string // empty when the config must be accepted
	}{
		{"wildcard port only, no TLS", func(c *Config) { c.GRPCAddr = ":7070" }, "--insecure"},
		{"wildcard port, insecure", func(c *Config) { c.GRPCAddr = ":7070"; c.Insecure = true }, ""},
		{"wildcard port, TLS and tokens", func(c *Config) {
			c.GRPCAddr = ":7070"
			c.TLSCert, c.TLSKey, c.TokensFile = "c.pem", "k.pem", "tokens"
		}, ""},
		{"wildcard port, TLS without tokens", func(c *Config) {
			c.GRPCAddr = ":7070"
			c.TLSCert, c.TLSKey = "c.pem", "k.pem"
		}, "--insecure"},
		{"wildcard port, tokens without TLS", func(c *Config) {
			c.GRPCAddr = ":7070"
			c.TokensFile = "tokens"
		}, "--insecure"},
		{"IPv4 loopback", func(c *Config) { c.GRPCAddr = "127.0.0.1:7070" }, ""},
		{"IPv6 loopback", func(c *Config) { c.GRPCAddr = "[::1]:7070" }, ""},
		{"localhost", func(c *Config) { c.GRPCAddr = "localhost:7070" }, ""},
		{"loopback with an ephemeral port", func(c *Config) { c.GRPCAddr = "127.0.0.1:0" }, ""},
		{"all IPv4 interfaces", func(c *Config) { c.GRPCAddr = "0.0.0.0:7070" }, "--insecure"},
		{"all IPv6 interfaces", func(c *Config) { c.GRPCAddr = "[::]:7070" }, "--insecure"},
		{"a private address", func(c *Config) { c.GRPCAddr = "10.1.2.3:7070" }, "--insecure"},
		{"a host name", func(c *Config) { c.GRPCAddr = "queue.example.com:7070" }, "--insecure"},
		{"another loopback address is not on the list", func(c *Config) { c.GRPCAddr = "127.0.0.2:7070" }, "--insecure"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfig(t)
			tc.mutate(&c)
			err := c.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("Validate accepted the config")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not mention %q", err, tc.wantErr)
			}
			want := "refusing to serve " + c.GRPCAddr + " without TLS and tokens; pass --insecure to override"
			if err.Error() != want {
				t.Fatalf("error = %q, want %q", err, want)
			}
		})
	}
}

func TestValidateRejectsBadScheduler(t *testing.T) {
	for _, s := range []string{"", "round-robin", "DRR", "drr ", "fifo,drr"} {
		c := validConfig(t)
		c.Scheduler = s
		err := c.Validate()
		if err == nil {
			t.Errorf("Validate accepted scheduler %q", s)
			continue
		}
		if !strings.Contains(err.Error(), "scheduler") {
			t.Errorf("scheduler %q: error %q does not name the flag", s, err)
		}
	}
	for _, s := range []string{"drr", "fifo"} {
		c := validConfig(t)
		c.Scheduler = s
		if err := c.Validate(); err != nil {
			t.Errorf("Validate rejected scheduler %q: %v", s, err)
		}
	}
}

func TestValidateRejects(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string // a fragment naming the offending flag
	}{
		{"no database URL", func(c *Config) { c.DatabaseURL = "" }, "database-url"},
		{"zero max-db-conns", func(c *Config) { c.MaxDBConns = 0 }, "max-db-conns"},
		{"negative max-db-conns", func(c *Config) { c.MaxDBConns = -1 }, "max-db-conns"},
		{"zero reap-interval", func(c *Config) { c.ReapInterval = 0 }, "reap-interval"},
		{"negative prune-interval", func(c *Config) { c.PruneInterval = -time.Second }, "prune-interval"},
		{"zero poll-interval", func(c *Config) { c.PollInterval = 0 }, "poll-interval"},
		{"zero activity-interval", func(c *Config) { c.ActivityInterval = 0 }, "activity-interval"},
		{"zero retention-done", func(c *Config) { c.Retention.Done = 0 }, "retention-done"},
		{"negative retention-dead", func(c *Config) { c.Retention.Dead = -time.Hour }, "retention-dead"},
		{"sub-microsecond retention-done", func(c *Config) { c.Retention.Done = time.Hour + time.Nanosecond }, "retention-done"},
		{"sub-microsecond retention-dead", func(c *Config) { c.Retention.Dead = time.Hour + time.Nanosecond }, "retention-dead"},
		{"zero max-connection-age", func(c *Config) { c.MaxConnectionAge = 0 }, "max-connection-age"},
		{"zero max-connection-age-grace", func(c *Config) { c.MaxConnectionAgeGrace = 0 }, "max-connection-age-grace"},
		{"zero shutdown-grace", func(c *Config) { c.ShutdownGrace = 0 }, "shutdown-grace"},
		{"zero reap-batch", func(c *Config) { c.ReapBatch = 0 }, "reap-batch"},
		{"negative prune-batch", func(c *Config) { c.PruneBatch = -5 }, "prune-batch"},
		{"zero max-in-flight", func(c *Config) { c.MaxInFlight = 0 }, "max-in-flight"},
		{"zero quantum", func(c *Config) { c.Quantum = 0 }, "quantum"},
		{"weight below 1", func(c *Config) { c.TenantWeights = map[string]int64{"a": 0} }, "tenant-weights"},
		{"weight above 1000", func(c *Config) { c.TenantWeights = map[string]int64{"a": 1001} }, "tenant-weights"},
		{"weight for a bad tenant name", func(c *Config) { c.TenantWeights = map[string]int64{"a b": 2} }, "tenant-weights"},
		{"certificate without key", func(c *Config) { c.TLSCert = "c.pem" }, "tls-key"},
		{"key without certificate", func(c *Config) { c.TLSKey = "k.pem" }, "tls-cert"},
		{"gRPC address without a port", func(c *Config) { c.GRPCAddr = "127.0.0.1" }, "grpc-addr"},
		{"gRPC address that is empty", func(c *Config) { c.GRPCAddr = "" }, "grpc-addr"},
		{"gRPC address with an empty port", func(c *Config) { c.GRPCAddr = "127.0.0.1:" }, "grpc-addr"},
		{"gRPC address with a port out of range", func(c *Config) { c.GRPCAddr = "127.0.0.1:70000" }, "grpc-addr"},
		{"unknown log level", func(c *Config) { c.LogLevel = "loud" }, "log-level"},
		{"unknown log format", func(c *Config) { c.LogFormat = "xml" }, "log-format"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfig(t)
			tc.mutate(&c)
			err := c.Validate()
			if err == nil {
				t.Fatal("Validate accepted the config")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not mention %q", err, tc.wantErr)
			}
		})
	}
}

func TestValidateAcceptsLimits(t *testing.T) {
	c := validConfig(t)
	c.TenantWeights = map[string]int64{"a": 1, "b": 1000}
	c.Retention = store.Retention{Done: time.Microsecond, Dead: time.Microsecond}
	c.TLSCert, c.TLSKey = "c.pem", "k.pem"
	for _, level := range []string{"debug", "info", "warn", "error"} {
		for _, format := range []string{"json", "text"} {
			c.LogLevel, c.LogFormat = level, format
			if err := c.Validate(); err != nil {
				t.Errorf("Validate(%s, %s): %v", level, format, err)
			}
		}
	}
}

// FuzzParseFlags feeds arbitrary argument vectors to ParseFlags. It never
// panics, and a configuration that both ParseFlags and Validate accept obeys
// every rule of Validate, restated here so that the check does not trust the
// code it checks.
func FuzzParseFlags(f *testing.F) {
	for _, seed := range []string{
		"",
		"--grpc-addr=127.0.0.1:7070",
		"--grpc-addr=127.0.0.1:0\x00--scheduler=fifo\x00--tenant-weights=a=4,b=1",
		"--grpc-addr=:7070\x00--insecure",
		"--grpc-addr=:7070\x00--tls-cert=c\x00--tls-key=k\x00--tokens-file=t",
		"--grpc-addr=[::1]:1\x00--reap-interval=1ms\x00--retention-done=1us\x00--retention-dead=2h",
		"--grpc-addr=localhost:9\x00--max-db-conns=2147483647\x00--quantum=1\x00--log-level=warn\x00--log-format=text",
		"-h",
		"--tenant-weights=a=0",
		"--tenant-weights=a=1001",
		"--max-db-conns=-1",
		"--\x00--grpc-addr=127.0.0.1:1",
		"positional",
		"--grpc-addr=127.0.0.1:99999",
		"--retention-done=1ns",
	} {
		f.Add(seed)
	}
	env := envWith("JOBQUEUE_DATABASE_URL", "postgres://env/db")
	f.Fuzz(func(t *testing.T, input string) {
		var args []string
		if input != "" {
			args = strings.Split(input, "\x00")
		}
		c, err := ParseFlags(args, env)
		if err != nil {
			return
		}
		if err := c.Validate(); err != nil {
			return
		}
		checkConfigRules(t, c)
	})
}

func checkConfigRules(t *testing.T, c Config) {
	t.Helper()
	if c.Scheduler != "drr" && c.Scheduler != "fifo" {
		t.Errorf("scheduler %q is neither drr nor fifo", c.Scheduler)
	}
	if c.DatabaseURL == "" {
		t.Error("database URL is empty")
	}
	for name, d := range map[string]time.Duration{
		"reap-interval": c.ReapInterval, "prune-interval": c.PruneInterval,
		"poll-interval": c.PollInterval, "activity-interval": c.ActivityInterval,
		"retention-done": c.Retention.Done, "retention-dead": c.Retention.Dead,
		"max-connection-age": c.MaxConnectionAge, "max-connection-age-grace": c.MaxConnectionAgeGrace,
		"shutdown-grace": c.ShutdownGrace,
	} {
		if d <= 0 {
			t.Errorf("%s = %v is not positive", name, d)
		}
	}
	for name, d := range map[string]time.Duration{"retention-done": c.Retention.Done, "retention-dead": c.Retention.Dead} {
		if d%time.Microsecond != 0 {
			t.Errorf("%s = %v is not a whole number of microseconds", name, d)
		}
	}
	for name, n := range map[string]int64{
		"max-db-conns": int64(c.MaxDBConns), "reap-batch": int64(c.ReapBatch), "prune-batch": int64(c.PruneBatch),
		"max-in-flight": int64(c.MaxInFlight), "quantum": c.Quantum,
	} {
		if n < 1 {
			t.Errorf("%s = %d is below 1", name, n)
		}
	}
	for tenant, w := range c.TenantWeights {
		if w < 1 || w > 1000 {
			t.Errorf("weight of %q = %d is outside [1, 1000]", tenant, w)
		}
	}
	if (c.TLSCert == "") != (c.TLSKey == "") {
		t.Errorf("TLS certificate %q and key %q must be set together", c.TLSCert, c.TLSKey)
	}
	host, port, err := net.SplitHostPort(c.GRPCAddr)
	if err != nil {
		t.Errorf("gRPC address %q is not host:port: %v", c.GRPCAddr, err)
	}
	if n, err := strconv.ParseUint(port, 10, 16); err != nil {
		t.Errorf("gRPC port %q is not a port number: %v (%d)", port, err, n)
	}
	loopback := host == "127.0.0.1" || host == "::1" || host == "localhost"
	secured := c.TLSCert != "" && c.TLSKey != "" && c.TokensFile != ""
	if !loopback && !secured && !c.Insecure {
		t.Errorf("%q is served without TLS and tokens and without --insecure", c.GRPCAddr)
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		t.Errorf("log level %q is unknown", c.LogLevel)
	}
	switch c.LogFormat {
	case "json", "text":
	default:
		t.Errorf("log format %q is unknown", c.LogFormat)
	}
}
