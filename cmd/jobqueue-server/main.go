// Command jobqueue-server runs a broker of the job queue: the gRPC API over a
// PostgreSQL database, with health checks and metrics on an HTTP port.
//
// It is configured by flags (see --help) and by the environment variable
// JOBQUEUE_DATABASE_URL, and it shuts down gracefully on SIGTERM or an
// interrupt. A build sets the version that --version prints with
//
//	go build -ldflags "-X main.version=1.2.3" ./cmd/jobqueue-server
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jason-te-sde/distributed-job-queue/internal/broker"
)

// version is what --version prints. It is "dev" unless the linker sets it.
var version = "dev"

func main() {
	os.Exit(run(os.Args[1:], os.Getenv, os.Stdout, os.Stderr))
}

// run is the command, apart from the process: it returns the exit status. Help
// and the version go to stdout and succeed; a failure writes one line to stderr
// and returns 1.
func run(args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	// The flag parser rejects what it does not know, so the version is looked
	// for first.
	for _, a := range args {
		if a == "--version" || a == "-version" {
			_, _ = fmt.Fprintln(stdout, version)
			return 0
		}
	}

	cfg, err := broker.ParseFlags(args, getenv)
	if errors.Is(err, flag.ErrHelp) {
		_, usage, _ := strings.Cut(err.Error(), "\n") // the text after "flag: help requested"
		_, _ = fmt.Fprintln(stdout, usage)
		_, _ = fmt.Fprintln(stdout, "  -version\n    \tprint the version and exit")
		return 0
	}
	if err != nil {
		return fail(stderr, err)
	}
	if err := cfg.Validate(); err != nil {
		return fail(stderr, err)
	}

	log := newLogger(stderr, cfg.LogLevel, cfg.LogFormat)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	// The first signal starts the graceful shutdown. Handing the signals back
	// to the runtime then makes a second SIGTERM or Ctrl-C end the process at
	// once, for an operator who does not want to wait out the grace period.
	context.AfterFunc(ctx, stop)

	b, err := broker.New(ctx, cfg, log)
	if err != nil {
		return fail(stderr, err)
	}
	b.SetVersion(version)
	log.Info("starting", slog.String("version", version))
	if err := b.Run(ctx); err != nil {
		return fail(stderr, err)
	}
	return 0
}

// fail writes the error as one line and returns the exit status of a failure.
func fail(stderr io.Writer, err error) int {
	_, _ = fmt.Fprintf(stderr, "jobqueue-server: %s\n", strings.Join(strings.Fields(err.Error()), " "))
	return 1
}

// newLogger builds the logger the configuration asks for. The level and the
// format were validated, so an unknown one cannot get here.
func newLogger(w io.Writer, level, format string) *slog.Logger {
	var lvl slog.Level
	_ = lvl.UnmarshalText([]byte(level))
	opts := &slog.HandlerOptions{Level: lvl}
	if format == "text" {
		return slog.New(slog.NewTextHandler(w, opts))
	}
	return slog.New(slog.NewJSONHandler(w, opts))
}
