// Command jobqueuectl is the admin and operations command line of the job
// queue. It enqueues, inspects, cancels and requeues jobs through the
// broker's gRPC API, applies the database migrations, and mints the tokens
// that the broker's tokens file lists.
//
// The commands print one result each: text meant for a person, or one JSON
// object with --json. The exit status is 0 for success, 1 for a failure (the
// broker refused, could not be reached, or the database failed) and 2 for a
// usage error, which is found before anything is sent.
//
// Run jobqueuectl help for the commands and the flags they share. A build sets
// the version that the version command prints with
//
//	go build -ldflags "-X main.version=1.2.3" ./cmd/jobqueuectl
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	jobqueuev1 "github.com/jason-te-sde/distributed-job-queue/internal/gen/jobqueue/v1"
)

// version is what the version command prints. It is "dev" unless the linker
// sets it.
var version = "dev"

// The exit statuses of the command.
const (
	exitOK    = 0
	exitError = 1
	exitUsage = 2
)

// Where the flags that every command shares get their values when the command
// line does not give them.
const (
	defaultAddr    = "dns:///localhost:7070"
	defaultTimeout = 10 * time.Second
	addrEnv        = "JOBQUEUE_ADDR"
	tokenEnv       = "JOBQUEUE_TOKEN"
	// maxMessageBytes is the largest gRPC message the broker sends or accepts:
	// a page of 64 jobs of 256 KiB, with room for the rest of the response.
	maxMessageBytes = 20 << 20
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, os.Getenv))
}

// run is the command apart from the process: it returns the exit status.
// getenv looks up an environment variable.
func run(args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	e := &env{stdout: stdout, stderr: stderr, getenv: getenv, g: newGlobals(getenv)}
	return e.finish(e.dispatch(args))
}

// env is what a command runs with: where it writes, how it reads the
// environment, and the flags that every command shares.
type env struct {
	stdout, stderr io.Writer
	getenv         func(string) string
	g              *globals
	// cmd is the command being run, once the arguments have named one. A usage
	// error names its synopsis.
	cmd *command
}

// globals are the flags that every command accepts, before or after its name.
type globals struct {
	addr, token, tlsCA string
	insecure, json     bool
	timeout            time.Duration
}

// newGlobals returns the shared flags set to what the environment gives, or
// else to their defaults.
func newGlobals(getenv func(string) string) *globals {
	g := &globals{addr: defaultAddr, timeout: defaultTimeout, token: getenv(tokenEnv)}
	if v := getenv(addrEnv); v != "" {
		g.addr = v
	}
	return g
}

// register adds the shared flags to fs, which every command line goes
// through: once for the flags in front of the command name, and once more in
// the FlagSet of the command itself for the flags behind it. Both write to the
// same globals, so a flag given twice takes its last value. The current value
// is each flag's default, which is what makes the second registration keep what
// the first one read.
func (g *globals) register(fs *flag.FlagSet) {
	fs.StringVar(&g.addr, "addr", g.addr, "gRPC `host:port` of the broker (env "+addrEnv+")")
	fs.Var(secret{&g.token}, "token", "bearer `token` (env "+tokenEnv+"); a flag shows in the process list, so prefer the environment")
	fs.BoolVar(&g.insecure, "insecure", g.insecure, "connect without TLS; the token, if any, then travels in clear text")
	fs.StringVar(&g.tlsCA, "tls-ca", g.tlsCA, "PEM `file` of the CA that signed the broker's certificate (default: the system roots)")
	fs.BoolVar(&g.json, "json", g.json, "print one JSON object per result")
	fs.DurationVar(&g.timeout, "timeout", g.timeout, "limit for the whole command")
}

// validate checks the shared flags against each other.
func (g *globals) validate() error {
	switch {
	case g.timeout <= 0:
		return usagef("--timeout must be positive, got %s", g.timeout)
	case g.insecure && g.tlsCA != "":
		return usagef("--insecure and --tls-ca contradict each other: pick one")
	}
	return nil
}

// secret is a string flag whose value never appears in help output: the flag
// package prints a flag's default, and the default of a token or a database URL
// is whatever the environment holds.
type secret struct{ p *string }

func (s secret) String() string     { return "" }
func (s secret) Set(v string) error { *s.p = v; return nil }

// usageError is a mistake in the command line. It is found before anything is
// sent, and it makes the exit status 2.
type usageError struct{ msg string }

func (e *usageError) Error() string { return e.msg }

func usagef(format string, args ...any) error {
	return &usageError{msg: fmt.Sprintf(format, args...)}
}

// helpRequest is a request for help. It is not a failure: its text goes to
// standard output and the exit status is 0.
type helpRequest struct{ text string }

func (h *helpRequest) Error() string { return "help requested" }

// finish turns what a command returned into the exit status, writing the
// message of a failure as one line to standard error.
func (e *env) finish(err error) int {
	var (
		help  *helpRequest
		usage *usageError
	)
	switch {
	case err == nil:
		return exitOK
	case errors.As(err, &help):
		_, _ = io.WriteString(e.stdout, help.text)
		return exitOK
	case errors.As(err, &usage):
		_, _ = fmt.Fprintf(e.stderr, "jobqueuectl: %s\n", clean(usage.msg))
		if e.cmd != nil {
			_, _ = fmt.Fprintf(e.stderr, "usage: jobqueuectl %s\n", e.cmd.synopsis)
		} else {
			_, _ = fmt.Fprintln(e.stderr, "run 'jobqueuectl help' for the commands")
		}
		return exitUsage
	default:
		_, _ = fmt.Fprintf(e.stderr, "jobqueuectl: %s\n", clean(err.Error()))
		return exitError
	}
}

// command is one subcommand: the words that name it, its synopsis for usage
// text, and the function that runs it with the arguments behind its name.
type command struct {
	name     string
	synopsis string
	summary  string
	run      func(e *env, args []string) error
	// hidden commands run but are not listed in the usage text.
	hidden bool
}

// commands is every command, in the order the usage text lists them. A
// command named by two words, such as "dead list", is found by both.
var commands = []command{
	{name: "enqueue", run: cmdEnqueue,
		synopsis: "enqueue --queue Q [--tenant T] [--delay D | --run-at RFC3339] [--max-attempts N] [--key K] (--payload @file | --payload-string S)",
		summary:  "add a job; prints its id"},
	{name: "get", run: cmdGet, synopsis: "get <id>", summary: "show a job"},
	{name: "cancel", run: cmdCancel, synopsis: "cancel <id>", summary: "stop a job that has not finished"},
	{name: "stats", run: cmdStats, synopsis: "stats --queue Q", summary: "count a queue's jobs per tenant"},
	{name: "dead list", run: cmdDeadList, synopsis: "dead list --queue Q [--after ID] [--limit N]", summary: "list a queue's dead jobs, a page at a time"},
	{name: "requeue", run: cmdRequeue, synopsis: "requeue <id>", summary: "give a dead job another run"},
	{name: "purge", run: cmdPurge,
		synopsis: "purge --queue Q --state succeeded|dead|cancelled --before RFC3339",
		summary:  "delete the jobs of a state that finished before a time"},
	{name: "migrate", run: cmdMigrate, synopsis: "migrate --database-url URL", summary: "apply the database migrations (env " + databaseURLEnv + ")"},
	{name: "token new", run: cmdTokenNew,
		synopsis: "token new --role producer --tenant T | --role worker --queues a,b | --role admin",
		summary:  "mint a token and the tokens-file line that lists it; the token is shown only once"},
	{name: "version", run: cmdVersion, synopsis: "version", summary: "print the version of this build"},
	// sleep exists for the preStop hook of the Helm chart, which runs in an
	// image that has no shell and so no sleep of its own.
	{name: "sleep", run: cmdSleep, synopsis: "sleep <duration>", hidden: true},
}

// dispatch reads the flags in front of the command name, finds the command,
// and runs it with the rest.
func (e *env) dispatch(args []string) error {
	fs := newFlagSet("jobqueuectl")
	e.g.register(fs)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return &helpRequest{text: usageText()}
		}
		return usagef("%v", err)
	}
	rest := fs.Args()
	switch {
	case len(rest) == 0:
		return usagef("missing command")
	case rest[0] == "help":
		return &helpRequest{text: usageText()}
	}

	for i := range commands {
		c := &commands[i]
		words := strings.Fields(c.name)
		if len(rest) >= len(words) && slices.Equal(rest[:len(words)], words) {
			e.cmd = c
			return c.run(e, rest[len(words):])
		}
	}
	// A group name without a known second word gets a better message than
	// "unknown command".
	var subs []string
	for _, c := range commands {
		if group, sub, ok := strings.Cut(c.name, " "); ok && group == rest[0] {
			subs = append(subs, sub)
		}
	}
	if len(subs) > 0 {
		return usagef("%s needs one of: %s", rest[0], strings.Join(subs, ", "))
	}
	return usagef("unknown command %q", rest[0])
}

// newFlagSet returns a FlagSet that reports its errors through the return
// value of Parse and prints nothing itself: finish decides what is printed
// and where.
func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Usage = func() {}
	return fs
}

// flagSet returns the FlagSet of a command, with the shared flags in it. The
// command adds its own and passes the set to parse.
func (e *env) flagSet() *flag.FlagSet {
	fs := newFlagSet(e.cmd.name)
	e.g.register(fs)
	return fs
}

// parse reads args into fs and returns the arguments that are not flags. Flags
// and arguments may come in any order, so that "get 5 --json" works as well as
// "get --json 5"; a "--" that is not the value of a flag ends the flags.
func (e *env) parse(fs *flag.FlagSet, args []string) ([]string, error) {
	var positional []string
	for {
		if err := fs.Parse(args); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return nil, &helpRequest{text: e.commandUsage(fs)}
			}
			return nil, usagef("%v", err)
		}
		rest := fs.Args()
		if len(rest) == 0 {
			break
		}
		if endsAtTerminator(fs, args[:len(args)-len(rest)]) {
			positional = append(positional, rest...)
			break
		}
		positional = append(positional, rest[0])
		args = rest[1:]
	}
	if err := e.g.validate(); err != nil {
		return nil, err
	}
	return positional, nil
}

// endsAtTerminator reports whether read, the arguments that fs has just taken
// as flags, ends with a "--" that ended them. A "--" that is the value of a
// flag, as in "--key --", ends nothing, and which of the two it is depends on
// how many arguments each flag before it takes, so this reads the arguments the
// way the flag package does: a flag written with "=" or a boolean flag stands
// alone, any other takes the next argument as its value.
func endsAtTerminator(fs *flag.FlagSet, read []string) bool {
	for i := 0; i < len(read); i++ {
		if read[i] == "--" {
			return i == len(read)-1
		}
		name, _, hasValue := strings.Cut(strings.TrimLeft(read[i], "-"), "=")
		if hasValue {
			continue
		}
		f := fs.Lookup(name)
		if f == nil {
			continue // fs.Parse has accepted these arguments, so this cannot happen
		}
		if b, ok := f.Value.(interface{ IsBoolFlag() bool }); ok && b.IsBoolFlag() {
			continue
		}
		i++ // the next argument is this flag's value, whatever it looks like
	}
	return false
}

// commandUsage is the help text of the running command: its synopsis and all
// of its flags.
func (e *env) commandUsage(fs *flag.FlagSet) string {
	var b strings.Builder
	fmt.Fprintf(&b, "usage: jobqueuectl %s\n\nflags:\n", e.cmd.synopsis)
	b.WriteString(defaults(fs))
	return b.String()
}

// defaults lists the flags of fs with their defaults, as PrintDefaults does,
// but with the double dash that this command's documentation uses; the flag
// package accepts both spellings.
func defaults(fs *flag.FlagSet) string {
	var b strings.Builder
	fs.SetOutput(&b)
	fs.PrintDefaults()
	fs.SetOutput(io.Discard)
	lines := strings.SplitAfter(b.String(), "\n")
	for i, l := range lines {
		if rest, ok := strings.CutPrefix(l, "  -"); ok {
			lines[i] = "  --" + rest
		}
	}
	return strings.Join(lines, "")
}

// usageText is the help text of the whole command.
func usageText() string {
	var b strings.Builder
	b.WriteString("usage: jobqueuectl [global flags] <command> [flags]\n\ncommands:\n")
	for _, c := range commands {
		if !c.hidden {
			fmt.Fprintf(&b, "  %s\n      %s\n", c.synopsis, c.summary)
		}
	}
	b.WriteString("  help\n      show this text\n\n")
	b.WriteString("global flags, accepted before and after the command name:\n")
	fs := newFlagSet("jobqueuectl")
	// Defaults are shown as they are without an environment.
	newGlobals(func(string) string { return "" }).register(fs)
	b.WriteString(defaults(fs))
	return b.String()
}

// setFlags returns the names of the flags that the command line gave, which
// tells "--delay 0s" from no --delay.
func setFlags(fs *flag.FlagSet) map[string]bool {
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	return set
}

// session is a connection to the broker and what a command needs to use it.
type session struct {
	client jobqueuev1.JobQueueServiceClient
	// ctx ends at --timeout and carries the token.
	ctx   context.Context
	close func()
}

// dial prepares a session. The connection itself is made by the first call, so
// a broker that is down is an error of that call.
func (e *env) dial() (*session, error) {
	creds, err := e.transportCredentials()
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(e.g.addr,
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageBytes), grpc.MaxCallSendMsgSize(maxMessageBytes)))
	if err != nil {
		return nil, fmt.Errorf("--addr %q: %w", e.g.addr, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), e.g.timeout)
	if e.g.token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+e.g.token)
	}
	return &session{
		client: jobqueuev1.NewJobQueueServiceClient(conn),
		ctx:    ctx,
		close:  func() { cancel(); _ = conn.Close() },
	}, nil
}

// transportCredentials is TLS against the system roots, or against the CA of
// --tls-ca, unless --insecure turns TLS off.
func (e *env) transportCredentials() (credentials.TransportCredentials, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	switch {
	case e.g.insecure:
		return insecure.NewCredentials(), nil
	case e.g.tlsCA != "":
		pem, err := os.ReadFile(e.g.tlsCA)
		if err != nil {
			return nil, fmt.Errorf("--tls-ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("--tls-ca: no PEM certificate in %s", e.g.tlsCA)
		}
		cfg.RootCAs = pool
	}
	return credentials.NewTLS(cfg), nil
}

// rpcError is the error of a failed call as the command reports it: the gRPC
// status code and the broker's message, without the transport's wrapping.
func rpcError(err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	return fmt.Errorf("%s: %s", st.Code(), st.Message())
}
