package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jason-te-sde/distributed-job-queue/internal/broker"
	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	jobqueuev1 "github.com/jason-te-sde/distributed-job-queue/internal/gen/jobqueue/v1"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/memstore"
)

// epoch is the time of every test broker until a test moves its clock.
var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// fakeClock is the clock of a test broker's store.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// noLease is the dispatcher of a test broker: the commands never lease.
type noLease struct{}

func (noLease) Lease(context.Context, broker.LeaseCall) (broker.LeaseAnswer, error) {
	return broker.LeaseAnswer{}, status.Error(codes.Unimplemented, "the test broker does not lease")
}

// testBroker is a gRPC server on a loopback port over a memstore, without TLS.
type testBroker struct {
	addr  string
	mem   *memstore.Store
	clock *fakeClock
}

// startBroker serves the queue API over a memstore on 127.0.0.1, without TLS
// unless opts bring credentials. A nil auth makes every caller an admin, like
// jobqueue-server --insecure.
func startBroker(t *testing.T, auth *broker.Authenticator, opts ...grpc.ServerOption) *testBroker {
	t.Helper()
	clock := &fakeClock{now: epoch}
	mem := memstore.New(memstore.Options{Clock: clock.Now})
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := broker.NewServer(mem, noLease{}, broker.NewMetrics(prometheus.NewRegistry()), log, 1)

	const maxMsg = 20 << 20
	gs := grpc.NewServer(append([]grpc.ServerOption{
		grpc.ChainUnaryInterceptor(broker.UnaryAuth(auth)),
		grpc.MaxRecvMsgSize(maxMsg), grpc.MaxSendMsgSize(maxMsg),
	}, opts...)...)
	jobqueuev1.RegisterJobQueueServiceServer(gs, srv)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return &testBroker{addr: lis.Addr().String(), mem: mem, clock: clock}
}

// result is what one run of the command wrote and returned.
type result struct {
	code           int
	stdout, stderr string
}

// runCtl runs the command in-process with the given environment.
func runCtl(env map[string]string, args ...string) result {
	var out, errb bytes.Buffer
	code := run(args, &out, &errb, func(k string) string { return env[k] })
	return result{code: code, stdout: out.String(), stderr: errb.String()}
}

// ctl runs the command against the broker, without TLS.
func (b *testBroker) ctl(args ...string) result {
	return runCtl(nil, append([]string{"--addr", b.addr, "--insecure"}, args...)...)
}

// ok runs the command and fails the test unless it succeeded; it returns what
// the command printed.
func (b *testBroker) ok(t *testing.T, args ...string) string {
	t.Helper()
	r := b.ctl(args...)
	if r.code != 0 || r.stderr != "" {
		t.Fatalf("jobqueuectl %v = exit %d, stderr %q; want exit 0 and no stderr", args, r.code, r.stderr)
	}
	return r.stdout
}

var enqueued = regexp.MustCompile(`^id=(\d+) deduplicated=(true|false)\n$`)

// enqueue runs the enqueue command and returns the new id.
func (b *testBroker) enqueue(t *testing.T, args ...string) int64 {
	t.Helper()
	out := b.ok(t, append([]string{"enqueue"}, args...)...)
	m := enqueued.FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("enqueue printed %q; want id=<n> deduplicated=<bool>", out)
	}
	id, _ := strconv.ParseInt(m[1], 10, 64)
	return id
}

// get runs the get command and returns its key: value lines, checking that the
// values are aligned in one column.
func (b *testBroker) get(t *testing.T, id int64) map[string]string {
	t.Helper()
	return parseKV(t, b.ok(t, "get", strconv.FormatInt(id, 10)))
}

// parseKV reads "key: value" lines and fails unless every value starts in the
// same column, which is the width of the longest key plus its colon and one
// space.
func parseKV(t *testing.T, out string) map[string]string {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	width := 0
	for _, l := range lines {
		key, _, _ := strings.Cut(l, ":")
		width = max(width, len(key)+2)
	}
	kv := map[string]string{}
	for _, l := range lines {
		key, _, ok := strings.Cut(l, ":")
		if !ok || len(l) <= width || l[width-1] != ' ' || l[width] == ' ' || strings.TrimRight(l[:width], " ") != key+":" {
			t.Fatalf("line %q of\n%s\nis not a key: value line aligned at column %d", l, out, width)
		}
		kv[key] = l[width:]
	}
	return kv
}

// markDead leases n fresh jobs of a queue straight from the store and fails
// them permanently, and returns their ids in ascending order.
func (b *testBroker) markDead(t *testing.T, queue, tenant, message string, n int) []int64 {
	t.Helper()
	specs := make([]job.Spec, n)
	for i := range specs {
		specs[i] = job.Spec{Queue: queue, Tenant: tenant, Payload: []byte("p"), MaxAttempts: 1}
	}
	ctx := t.Context()
	if _, err := b.mem.Enqueue(ctx, specs); err != nil {
		t.Fatal(err)
	}
	grants, err := b.mem.Lease(ctx, store.LeaseRequest{
		Queue: queue, Takes: []store.TenantTake{{Tenant: tenant, Limit: n}}, TTL: time.Minute, Worker: "w",
	})
	if err != nil || len(grants) != n {
		t.Fatalf("Lease = %d grants, %v; want %d", len(grants), err, n)
	}
	reqs := make([]store.FailRequest, n)
	ids := make([]int64, n)
	for i, g := range grants {
		ids[i] = g.Job.ID
		reqs[i] = store.FailRequest{
			Ref:       store.Ref{ID: g.Job.ID, Queue: queue, Token: g.Job.Epoch},
			Permanent: true, Error: message,
		}
	}
	if _, err := b.mem.Fail(ctx, reqs); err != nil {
		t.Fatal(err)
	}
	return ids
}

func TestCtlEnqueueGetCancel(t *testing.T) {
	b := startBroker(t, nil)

	out := b.ok(t, "enqueue", "--queue", "emails", "--tenant", "acme", "--payload-string", "hello", "--key", "k1")
	m := enqueued.FindStringSubmatch(out)
	if m == nil || m[2] != "false" {
		t.Fatalf("first enqueue printed %q; want id=<n> deduplicated=false", out)
	}
	id, _ := strconv.ParseInt(m[1], 10, 64)

	again := b.ok(t, "enqueue", "--queue", "emails", "--tenant", "acme", "--payload-string", "other", "--key", "k1")
	if want := "id=" + m[1] + " deduplicated=true\n"; again != want {
		t.Fatalf("enqueue with the same key printed %q, want %q", again, want)
	}

	got := b.get(t, id)
	want := map[string]string{
		"id": m[1], "queue": "emails", "tenant": "acme", "state": "ready",
		"payload_bytes": "5", "run_at": "2026-01-01T00:00:00Z", "attempts": "0", "max_attempts": "25",
		"epoch": "0", "lease_expires_at": "-", "last_error": "-", "result_bytes": "0",
		"created_at": "2026-01-01T00:00:00Z", "finished_at": "-", "idempotency_key": "k1",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("get printed %v, want %v", got, want)
	}

	b.clock.Advance(time.Minute)
	if out := b.ok(t, "cancel", m[1]); out != "state=cancelled\n" {
		t.Fatalf("cancel printed %q, want state=cancelled", out)
	}
	got = b.get(t, id)
	if got["state"] != "cancelled" || got["finished_at"] != "2026-01-01T00:01:00Z" {
		t.Fatalf("after cancel: state %q, finished_at %q; want cancelled at 2026-01-01T00:01:00Z", got["state"], got["finished_at"])
	}
	if out := b.ok(t, "cancel", m[1]); out != "state=cancelled\n" {
		t.Fatalf("second cancel printed %q, want state=cancelled", out)
	}
}

func TestCtlEnqueueOptions(t *testing.T) {
	b := startBroker(t, nil)
	dir := t.TempDir()

	t.Run("payload from a file", func(t *testing.T) {
		path := filepath.Join(dir, "p.bin")
		if err := os.WriteFile(path, bytes.Repeat([]byte{0, 1, 2}, 10), 0o600); err != nil {
			t.Fatal(err)
		}
		id := b.enqueue(t, "--queue", "q", "--payload", "@"+path)
		if got := b.get(t, id); got["payload_bytes"] != "30" || got["tenant"] != "default" {
			t.Fatalf("payload_bytes %q, tenant %q; want 30 bytes for tenant default", got["payload_bytes"], got["tenant"])
		}
	})
	t.Run("an empty payload", func(t *testing.T) {
		id := b.enqueue(t, "--queue", "q", "--payload-string", "")
		if got := b.get(t, id); got["payload_bytes"] != "0" {
			t.Fatalf("payload_bytes %q, want 0", got["payload_bytes"])
		}
	})
	t.Run("a payload at the limit and one past it", func(t *testing.T) {
		path := filepath.Join(dir, "big.bin")
		if err := os.WriteFile(path, make([]byte, job.MaxPayloadBytes), 0o600); err != nil {
			t.Fatal(err)
		}
		id := b.enqueue(t, "--queue", "q", "--payload", "@"+path)
		if got := b.get(t, id); got["payload_bytes"] != strconv.Itoa(job.MaxPayloadBytes) {
			t.Fatalf("payload_bytes %q, want %d", got["payload_bytes"], job.MaxPayloadBytes)
		}

		if err := os.WriteFile(path, make([]byte, job.MaxPayloadBytes+1), 0o600); err != nil {
			t.Fatal(err)
		}
		before := b.ok(t, "stats", "--queue", "q")
		r := b.ctl("enqueue", "--queue", "q", "--payload", "@"+path)
		// The command refuses it while reading the file, before it sends a byte.
		if r.code != 1 || r.stdout != "" || !strings.Contains(r.stderr, "--payload") || !strings.Contains(r.stderr, strconv.Itoa(job.MaxPayloadBytes)) {
			t.Fatalf("enqueue of an oversized payload = exit %d, stdout %q, stderr %q; want exit 1 from --payload, naming the limit", r.code, r.stdout, r.stderr)
		}
		if after := b.ok(t, "stats", "--queue", "q"); after != before {
			t.Fatalf("a rejected payload changed the queue:\nbefore:\n%s\nafter:\n%s", before, after)
		}
	})
	t.Run("a missing payload file", func(t *testing.T) {
		r := b.ctl("enqueue", "--queue", "q", "--payload", "@"+filepath.Join(dir, "absent"))
		if r.code != 1 || r.stdout != "" || r.stderr == "" {
			t.Fatalf("enqueue of a missing file = exit %d, stdout %q, stderr %q; want exit 1 and a message", r.code, r.stdout, r.stderr)
		}
	})
	t.Run("delay, run-at and max-attempts", func(t *testing.T) {
		id := b.enqueue(t, "--queue", "q", "--payload-string", "x", "--delay", "1h30m", "--max-attempts", "3")
		got := b.get(t, id)
		if got["run_at"] != "2026-01-01T01:30:00Z" || got["max_attempts"] != "3" {
			t.Fatalf("run_at %q, max_attempts %q; want 2026-01-01T01:30:00Z and 3", got["run_at"], got["max_attempts"])
		}
		id = b.enqueue(t, "--queue", "q", "--payload-string", "x", "--run-at", "2026-01-01T05:00:00+02:00")
		if got := b.get(t, id); got["run_at"] != "2026-01-01T03:00:00Z" {
			t.Fatalf("run_at %q, want 2026-01-01T03:00:00Z", got["run_at"])
		}
	})
	t.Run("the server's refusal is exit 1", func(t *testing.T) {
		r := b.ctl("enqueue", "--queue", "bad queue", "--payload-string", "x")
		if r.code != 1 || !strings.Contains(r.stderr, "InvalidArgument") {
			t.Fatalf("enqueue into a bad queue name = exit %d, stderr %q; want exit 1 and InvalidArgument", r.code, r.stderr)
		}
	})
}

func TestCtlGlobalFlagsBeforeAndAfterSubcommand(t *testing.T) {
	b := startBroker(t, nil)
	b.enqueue(t, "--queue", "q", "--payload-string", "x")
	// The default address is dns:///localhost:7070, so a command that ignores
	// the flag cannot succeed unless a broker happens to run there.
	const wantTenant = "default"

	for name, args := range map[string][]string{
		"before":              {"--addr", b.addr, "--insecure", "stats", "--queue", "q"},
		"after":               {"stats", "--queue", "q", "--addr", b.addr, "--insecure"},
		"split":               {"--insecure", "stats", "--addr", b.addr, "--queue", "q"},
		"equals sign":         {"--addr=" + b.addr, "--insecure=true", "stats", "--queue=q"},
		"flags between args":  {"--insecure", "stats", "--addr", b.addr, "--timeout", "5s", "--queue", "q", "--json=false"},
		"override beats lead": {"--addr", "127.0.0.1:1", "--insecure", "stats", "--queue", "q", "--addr", b.addr},
	} {
		t.Run(name, func(t *testing.T) {
			r := runCtl(nil, args...)
			if r.code != 0 || !strings.Contains(r.stdout, wantTenant) || r.stderr != "" {
				t.Fatalf("jobqueuectl %v = exit %d, stdout %q, stderr %q; want the stats of queue q", args, r.code, r.stdout, r.stderr)
			}
		})
	}

	t.Run("the environment supplies the address", func(t *testing.T) {
		r := runCtl(map[string]string{"JOBQUEUE_ADDR": b.addr}, "--insecure", "stats", "--queue", "q")
		if r.code != 0 || !strings.Contains(r.stdout, wantTenant) {
			t.Fatalf("exit %d, stdout %q, stderr %q; want the stats of queue q", r.code, r.stdout, r.stderr)
		}
	})
	t.Run("a flag beats the environment", func(t *testing.T) {
		r := runCtl(map[string]string{"JOBQUEUE_ADDR": "127.0.0.1:1"}, "--insecure", "stats", "--queue", "q", "--addr", b.addr)
		if r.code != 0 || !strings.Contains(r.stdout, wantTenant) {
			t.Fatalf("exit %d, stdout %q, stderr %q; want the stats of queue q", r.code, r.stdout, r.stderr)
		}
	})
	t.Run("a positional argument may come before the flags", func(t *testing.T) {
		r := runCtl(nil, "get", "1", "--addr", b.addr, "--insecure", "--json")
		if r.code != 0 || !strings.HasPrefix(r.stdout, `{"id":1,`) {
			t.Fatalf("exit %d, stdout %q, stderr %q; want the job as JSON", r.code, r.stdout, r.stderr)
		}
	})
}

func TestCtlTerminatorEndsTheFlags(t *testing.T) {
	b := startBroker(t, nil)
	b.enqueue(t, "--queue", "q", "--payload-string", "x")

	// After "--" the rest are arguments, flag-like or not.
	r := b.ctl("get", "--", "1")
	if r.code != 0 || r.stderr != "" || !strings.Contains(r.stdout, "id:") {
		t.Fatalf("get -- 1 = exit %d, stdout %q, stderr %q; want the job", r.code, r.stdout, r.stderr)
	}
	r = b.ctl("get", "1", "--", "--json")
	if r.code != 2 || r.stdout != "" || !strings.Contains(r.stderr, "got 2 arguments") {
		t.Fatalf("get 1 -- --json = exit %d, stdout %q, stderr %q; want exit 2: --json is an argument after --", r.code, r.stdout, r.stderr)
	}
}

func TestCtlParseReadsATerminatorOnlyWhereItIsOne(t *testing.T) {
	type outcome struct {
		positional []string
		value      string
		json       bool
	}
	for name, tc := range map[string]struct {
		args []string
		want outcome
	}{
		"a leading terminator":              {[]string{"--", "a", "--json"}, outcome{[]string{"a", "--json"}, "", false}},
		"a terminator after an argument":    {[]string{"a", "--", "b", "--json"}, outcome{[]string{"a", "b", "--json"}, "", false}},
		"a flag before the terminator":      {[]string{"--json", "--", "-v"}, outcome{[]string{"-v"}, "", true}},
		"a terminator as a flag's value":    {[]string{"--value", "--", "a", "--json"}, outcome{[]string{"a"}, "--", true}},
		"a value with an equals sign":       {[]string{"--value=--", "a", "--json"}, outcome{[]string{"a"}, "--", true}},
		"an equals sign, then a terminator": {[]string{"--value=--", "--", "a", "--json"}, outcome{[]string{"a", "--json"}, "--", false}},
		"a value flag that is a value":      {[]string{"--value", "--value", "--", "a", "--json"}, outcome{[]string{"a", "--json"}, "--value", false}},
		"a bool flag does not take a --":    {[]string{"--json", "--", "a", "--json"}, outcome{[]string{"a", "--json"}, "", true}},
		"a terminator at the very end":      {[]string{"a", "--"}, outcome{[]string{"a"}, "", false}},
		"a value that is the last element":  {[]string{"a", "--value", "--"}, outcome{[]string{"a"}, "--", false}},
	} {
		t.Run(name, func(t *testing.T) {
			e := &env{g: newGlobals(func(string) string { return "" })}
			fs := newFlagSet("t")
			var got outcome
			fs.StringVar(&got.value, "value", "", "")
			fs.BoolVar(&got.json, "json", false, "")
			var err error
			if got.positional, err = e.parse(fs, tc.args); err != nil {
				t.Fatalf("parse(%v): %v", tc.args, err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("parse(%v) = %+v, want %+v", tc.args, got, tc.want)
			}
		})
	}
}

func TestCtlStatsTable(t *testing.T) {
	b := startBroker(t, nil)
	b.enqueue(t, "--queue", "q", "--tenant", "a", "--payload-string", "x")
	b.enqueue(t, "--queue", "q", "--tenant", "a", "--payload-string", "x")
	b.enqueue(t, "--queue", "q", "--tenant", "b", "--payload-string", "x", "--delay", "1h")
	b.clock.Advance(90 * time.Second)

	out := b.ok(t, "stats", "--queue", "q")
	var rows [][]string
	for l := range strings.SplitSeq(strings.TrimSuffix(out, "\n"), "\n") {
		rows = append(rows, strings.Fields(l))
	}
	want := [][]string{
		{"TENANT", "READY", "SCHEDULED", "LEASED", "DEAD", "OLDEST"},
		{"a", "2", "0", "0", "0", "1m30s"},
		{"b", "0", "1", "0", "0", "-"},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("stats printed\n%s\nparsed as %v, want %v", out, rows, want)
	}
	// The columns line up: every line has the TENANT column as wide as the
	// header, and READY starts at the same offset on every line.
	col := strings.Index(strings.SplitN(out, "\n", 2)[0], "READY")
	for l := range strings.SplitSeq(strings.TrimSuffix(out, "\n"), "\n") {
		if len(l) <= col || l[col-1] != ' ' || l[col] == ' ' {
			t.Fatalf("line %q does not have a column starting at offset %d", l, col)
		}
	}
}

func TestCtlStatsOldestBelowASecondShowsMilliseconds(t *testing.T) {
	b := startBroker(t, nil)
	b.enqueue(t, "--queue", "q", "--payload-string", "x")
	b.clock.Advance(250 * time.Millisecond)
	out := b.ok(t, "stats", "--queue", "q")
	if got := strings.Fields(strings.Split(out, "\n")[1]); got[len(got)-1] != "250ms" {
		t.Fatalf("stats printed\n%s\nwant the oldest job's age as 250ms", out)
	}
	b.clock.Advance(1150 * time.Millisecond) // 1.4s in all
	out = b.ok(t, "stats", "--queue", "q")
	if got := strings.Fields(strings.Split(out, "\n")[1]); got[len(got)-1] != "1s" {
		t.Fatalf("stats printed\n%s\nwant the oldest job's age rounded to 1s", out)
	}
}

func TestCtlStatsJSON(t *testing.T) {
	b := startBroker(t, nil)
	b.enqueue(t, "--queue", "q", "--tenant", "a", "--payload-string", "x")
	b.enqueue(t, "--queue", "q", "--tenant", "a", "--payload-string", "x")
	b.enqueue(t, "--queue", "q", "--tenant", "b", "--payload-string", "x", "--delay", "1h")
	b.markDead(t, "q", "b", "boom", 1)
	b.clock.Advance(90 * time.Second)

	out := b.ok(t, "stats", "--queue", "q", "--json")
	if strings.Count(out, "\n") != 1 || !strings.HasSuffix(out, "\n") {
		t.Fatalf("--json printed %q; want exactly one line", out)
	}
	var got struct {
		Queue   string `json:"queue"`
		Tenants []struct {
			Tenant           string `json:"tenant"`
			Ready            int64  `json:"ready"`
			Scheduled        int64  `json:"scheduled"`
			Leased           int64  `json:"leased"`
			Dead             int64  `json:"dead"`
			OldestReadyAgeMS *int64 `json:"oldest_ready_age_ms"`
		} `json:"tenants"`
	}
	dec := json.NewDecoder(strings.NewReader(out))
	dec.DisallowUnknownFields() // pins the set of keys
	if err := dec.Decode(&got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	if got.Queue != "q" || len(got.Tenants) != 2 {
		t.Fatalf("stats = %+v, want queue q with two tenants", got)
	}
	a, bb := got.Tenants[0], got.Tenants[1]
	if a.Tenant != "a" || a.Ready != 2 || a.Scheduled != 0 || a.Dead != 0 || a.OldestReadyAgeMS == nil || *a.OldestReadyAgeMS != 90_000 {
		t.Errorf("tenant a = %+v, want 2 ready, the oldest 90s old", a)
	}
	if bb.Tenant != "b" || bb.Ready != 0 || bb.Scheduled != 1 || bb.Dead != 1 || bb.OldestReadyAgeMS != nil {
		t.Errorf("tenant b = %+v, want 1 scheduled, 1 dead and no oldest age", bb)
	}
	// A tenant with nothing ready has no oldest job: null, like the times of get.
	if !strings.Contains(out, `"oldest_ready_age_ms":null`) {
		t.Errorf("--json printed %q; want oldest_ready_age_ms null for tenant b", out)
	}
}

func TestCtlJSONOutputs(t *testing.T) {
	b := startBroker(t, nil)

	out := b.ok(t, "enqueue", "--queue", "q", "--payload-string", "hi", "--json")
	if out != `{"id":1,"deduplicated":false}`+"\n" {
		t.Errorf("enqueue --json printed %q", out)
	}
	out = b.ok(t, "get", "1", "--json")
	var j map[string]any
	if err := json.Unmarshal([]byte(out), &j); err != nil {
		t.Fatalf("get --json printed %q: %v", out, err)
	}
	for _, key := range []string{
		"id", "queue", "tenant", "state", "payload_bytes", "run_at", "attempts", "max_attempts", "epoch",
		"lease_expires_at", "last_error", "result_bytes", "created_at", "finished_at", "idempotency_key",
	} {
		if _, ok := j[key]; !ok {
			t.Errorf("get --json has no key %q: %s", key, out)
		}
	}
	if len(j) != 15 || j["state"] != "ready" || j["payload_bytes"] != float64(2) || j["finished_at"] != nil || j["run_at"] != "2026-01-01T00:00:00Z" {
		t.Errorf("get --json = %s; want 15 keys, state ready, 2 payload bytes, no finished_at, run_at set", out)
	}
	// Text that HTML would escape stays as it is: this is not a web page.
	b.markDead(t, "html", "t", "x<y>&z", 1)
	if out := b.ok(t, "dead", "list", "--queue", "html", "--json"); !strings.Contains(out, `"last_error":"x<y>&z"`) {
		t.Errorf("dead list --json printed %q; want the error message unescaped", out)
	}
	if out := b.ok(t, "cancel", "1", "--json"); out != `{"state":"cancelled"}`+"\n" {
		t.Errorf("cancel --json printed %q", out)
	}
	if out := b.ok(t, "purge", "--queue", "q", "--state", "cancelled", "--before", "2027-01-01T00:00:00Z", "--json"); out != `{"deleted":1}`+"\n" {
		t.Errorf("purge --json printed %q", out)
	}
}

func TestCtlDeadListPaginatesAndRequeue(t *testing.T) {
	b := startBroker(t, nil)
	ids := b.markDead(t, "q", "acme", "boom\nsecond line\x1b[31m red", 3)
	b.markDead(t, "other", "acme", "elsewhere", 1)

	idOf := func(l string) string { return strings.Fields(l)[0] }
	lines := func(out string) []string { return strings.Split(strings.TrimSuffix(out, "\n"), "\n") }

	// Page 1: full, so it names where to continue.
	out := b.ok(t, "dead", "list", "--queue", "q", "--limit", "2")
	ls := lines(out)
	if len(ls) != 4 {
		t.Fatalf("page 1 printed %d lines, want header, 2 rows and the next line:\n%s", len(ls), out)
	}
	if got := strings.Fields(ls[0]); !reflect.DeepEqual(got, []string{"ID", "TENANT", "ATTEMPTS", "FINISHED", "LAST_ERROR"}) {
		t.Errorf("header = %v", got)
	}
	if idOf(ls[1]) != strconv.FormatInt(ids[0], 10) || idOf(ls[2]) != strconv.FormatInt(ids[1], 10) {
		t.Errorf("page 1 ids = %s, %s; want %d, %d", idOf(ls[1]), idOf(ls[2]), ids[0], ids[1])
	}
	if want := "next: --after " + strconv.FormatInt(ids[1], 10); ls[3] != want {
		t.Errorf("last line = %q, want %q", ls[3], want)
	}
	// A hostile error message must neither break the row nor reach the
	// terminal as an escape sequence.
	if strings.ContainsAny(out, "\x1b\r") || !strings.Contains(ls[1], "boom second line") {
		t.Errorf("row %q must show the error on one line without control characters", ls[1])
	}
	if !strings.Contains(ls[1], "1/1") || !strings.Contains(ls[1], "acme") {
		t.Errorf("row %q must show the tenant and attempts 1/1", ls[1])
	}

	// Page 2 continues after the id the first page named, and is short.
	out = b.ok(t, "dead", "list", "--queue", "q", "--limit", "2", "--after", strconv.FormatInt(ids[1], 10))
	ls = lines(out)
	if len(ls) != 2 || idOf(ls[1]) != strconv.FormatInt(ids[2], 10) || strings.Contains(out, "next:") {
		t.Fatalf("page 2 printed\n%s\nwant the header and job %d only, with no next line", out, ids[2])
	}

	// The JSON page carries the whole jobs and the continuation.
	out = b.ok(t, "dead", "list", "--queue", "q", "--limit", "2", "--json")
	var page struct {
		Jobs []struct {
			ID        int64  `json:"id"`
			State     string `json:"state"`
			LastError string `json:"last_error"`
		} `json:"jobs"`
		NextAfterID int64 `json:"next_after_id"`
	}
	if err := json.Unmarshal([]byte(out), &page); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	if len(page.Jobs) != 2 || page.Jobs[0].ID != ids[0] || page.Jobs[0].State != "dead" || page.NextAfterID != ids[1] ||
		page.Jobs[0].LastError != "boom\nsecond line\x1b[31m red" {
		t.Fatalf("json page = %+v, want the first two dead jobs with the raw error and next_after_id %d", page, ids[1])
	}

	// Requeue revives one; the list no longer shows it.
	idArg := strconv.FormatInt(ids[0], 10)
	if out := b.ok(t, "requeue", idArg); out != "id="+idArg+" requeued=true\n" {
		t.Errorf("requeue printed %q", out)
	}
	if got := b.get(t, ids[0])["state"]; got != "ready" {
		t.Errorf("after requeue the job is %q, want ready", got)
	}
	out = b.ok(t, "dead", "list", "--queue", "q")
	ls = lines(out)
	if len(ls) != 3 || idOf(ls[1]) != strconv.FormatInt(ids[1], 10) || idOf(ls[2]) != strconv.FormatInt(ids[2], 10) || strings.Contains(out, "next:") {
		t.Errorf("after requeue the list printed\n%s\nwant jobs %d and %d and no next line", out, ids[1], ids[2])
	}
	// A job that is not dead cannot be requeued.
	r := b.ctl("requeue", idArg)
	if r.code != 1 || !strings.Contains(r.stderr, "FailedPrecondition") {
		t.Errorf("requeue of a ready job = exit %d, stderr %q; want exit 1 and FailedPrecondition", r.code, r.stderr)
	}
}

func TestCtlDeadListCutsALongErrorOnACharacterBoundary(t *testing.T) {
	b := startBroker(t, nil)
	long := strings.Repeat("\u00e9", 100)
	b.markDead(t, "q", "acme", long, 1)

	out := b.ok(t, "dead", "list", "--queue", "q")
	row := strings.Split(out, "\n")[1]
	if want := strings.Repeat("\u00e9", 57) + "..."; !strings.HasSuffix(row, "  "+want) {
		t.Errorf("row %q must end with the error cut to 60 characters, %q", row, want)
	}
	if out := b.ok(t, "dead", "list", "--queue", "q", "--json"); !strings.Contains(out, long) {
		t.Errorf("--json must carry the whole error message, got %q", out)
	}
}

func TestCtlErrorMessagesAreOneCleanLine(t *testing.T) {
	var out, errb bytes.Buffer
	e := &env{stdout: &out, stderr: &errb}
	if code := e.finish(errors.New("first\nsecond \x1b[31mred")); code != 1 {
		t.Fatalf("finish = %d, want 1", code)
	}
	if want := "jobqueuectl: first second ?[31mred\n"; errb.String() != want || out.Len() != 0 {
		t.Fatalf("stderr %q, stdout %q; want %q and nothing", errb.String(), out.String(), want)
	}
}

func TestCtlPurge(t *testing.T) {
	b := startBroker(t, nil)
	b.markDead(t, "q", "acme", "old", 2)
	b.clock.Advance(time.Hour)
	b.markDead(t, "q", "acme", "new", 1)

	// Only the jobs that finished before the cut-off go.
	if out := b.ok(t, "purge", "--queue", "q", "--state", "dead", "--before", "2026-01-01T00:30:00Z"); out != "deleted=2\n" {
		t.Fatalf("purge printed %q, want deleted=2", out)
	}
	if out := b.ok(t, "purge", "--queue", "q", "--state", "dead", "--before", "2026-01-01T00:30:00Z"); out != "deleted=0\n" {
		t.Fatalf("second purge printed %q, want deleted=0", out)
	}
	if out := b.ok(t, "dead", "list", "--queue", "q"); strings.Count(out, "\n") != 2 {
		t.Fatalf("dead list printed\n%s\nwant the header and the one job that is left", out)
	}
	// An unparseable cut-off is the caller's mistake, found before any call.
	r := b.ctl("purge", "--queue", "q", "--state", "dead", "--before", "bogus")
	if r.code != 2 {
		t.Fatalf("purge with a bad time = exit %d, want 2", r.code)
	}
}

func TestCtlUsageErrorsExit2(t *testing.T) {
	const payload = "--payload-string=x"
	cases := map[string][]string{
		"no arguments":                  nil,
		"only global flags":             {"--json"},
		"unknown subcommand":            {"frobnicate"},
		"unknown flag":                  {"stats", "--queue", "q", "--nope"},
		"unknown global flag":           {"--nope", "stats", "--queue", "q"},
		"dead without a subcommand":     {"dead"},
		"dead with another subcommand":  {"dead", "purge", "--queue", "q"},
		"token without a subcommand":    {"token"},
		"enqueue without a queue":       {"enqueue", payload},
		"enqueue with delay and run-at": {"enqueue", "--queue", "q", payload, "--delay", "1m", "--run-at", "2026-01-01T00:00:00Z"},
		"enqueue without a payload":     {"enqueue", "--queue", "q"},
		"enqueue with two payloads":     {"enqueue", "--queue", "q", payload, "--payload", "@/nonexistent"},
		"enqueue payload without @":     {"enqueue", "--queue", "q", "--payload", "file.bin"},
		"enqueue with a bad delay":      {"enqueue", "--queue", "q", payload, "--delay", "soon"},
		"enqueue with a bad run-at":     {"enqueue", "--queue", "q", payload, "--run-at", "tomorrow"},
		"enqueue with a bad attempts":   {"enqueue", "--queue", "q", payload, "--max-attempts", "many"},
		"enqueue with attempts too big": {"enqueue", "--queue", "q", payload, "--max-attempts", "4294967296"},
		"enqueue with a stray argument": {"enqueue", "--queue", "q", payload, "extra"},
		"stats without a queue":         {"stats"},
		"stats with a stray argument":   {"stats", "--queue", "q", "extra"},
		"dead list without a queue":     {"dead", "list"},
		"dead list with a bad limit":    {"dead", "list", "--queue", "q", "--limit", "x"},
		"dead list with a bad after":    {"dead", "list", "--queue", "q", "--after", "-1"},
		"purge without a queue":         {"purge", "--state", "dead", "--before", "2026-01-01T00:00:00Z"},
		"purge without a state":         {"purge", "--queue", "q", "--before", "2026-01-01T00:00:00Z"},
		"purge without a time":          {"purge", "--queue", "q", "--state", "dead"},
		"purge of a live state":         {"purge", "--queue", "q", "--state", "ready", "--before", "2026-01-01T00:00:00Z"},
		"purge with a bad time":         {"purge", "--queue", "q", "--state", "dead", "--before", "yesterday"},
		"get without an id":             {"get"},
		"get with two ids":              {"get", "1", "2"},
		"get with a bad id":             {"get", "abc"},
		"get with id zero":              {"get", "0"},
		"cancel without an id":          {"cancel"},
		"cancel with a bad id":          {"cancel", "-3"},
		"requeue without an id":         {"requeue"},
		"requeue with a bad id":         {"requeue", "1.5"},
		"migrate without a url":         {"migrate"},
		"token new without a role":      {"token", "new"},
		"token new with a bad role":     {"token", "new", "--role", "root"},
		"producer without a tenant":     {"token", "new", "--role", "producer"},
		"producer with queues":          {"token", "new", "--role", "producer", "--tenant", "t", "--queues", "q"},
		"producer with a bad tenant":    {"token", "new", "--role", "producer", "--tenant", "a b"},
		"worker without queues":         {"token", "new", "--role", "worker"},
		"worker with a tenant":          {"token", "new", "--role", "worker", "--queues", "q", "--tenant", "t"},
		"worker with a bad queue":       {"token", "new", "--role", "worker", "--queues", "a,,b"},
		"admin with a tenant":           {"token", "new", "--role", "admin", "--tenant", "t"},
		"admin with queues":             {"token", "new", "--role", "admin", "--queues", "q"},
		"insecure with a CA file":       {"--insecure", "--tls-ca", "ca.pem", "stats", "--queue", "q"},
		"a zero timeout":                {"--timeout", "0s", "stats", "--queue", "q"},
		"a bad timeout":                 {"--timeout", "fast", "stats", "--queue", "q"},
		"sleep without a duration":      {"sleep"},
		"sleep with a bad duration":     {"sleep", "soon"},
		"sleep with a negative one":     {"sleep", "-1s"},
		"sleep with two durations":      {"sleep", "1s", "2s"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			// No broker listens at the default address and the commands are
			// not given one: a usage error must be found before any call.
			r := runCtl(nil, args...)
			if r.code != 2 || r.stdout != "" || !strings.HasPrefix(r.stderr, "jobqueuectl: ") {
				t.Fatalf("jobqueuectl %v = exit %d, stdout %q, stderr %q; want exit 2, no stdout and a message", args, r.code, r.stdout, r.stderr)
			}
		})
	}
}

func TestCtlHelpExits0AndHidesSleep(t *testing.T) {
	for _, args := range [][]string{{"help"}, {"--help"}, {"-h"}} {
		r := runCtl(nil, args...)
		if r.code != 0 || r.stderr != "" {
			t.Fatalf("jobqueuectl %v = exit %d, stderr %q; want exit 0 and no stderr", args, r.code, r.stderr)
		}
		for _, want := range []string{"enqueue", "get", "cancel", "stats", "dead list", "requeue", "purge", "migrate", "token new", "--addr", "JOBQUEUE_ADDR", "JOBQUEUE_TOKEN"} {
			if !strings.Contains(r.stdout, want) {
				t.Errorf("jobqueuectl %v: usage does not mention %q:\n%s", args, want, r.stdout)
			}
		}
		if strings.Contains(r.stdout, "sleep") {
			t.Errorf("jobqueuectl %v: usage lists the hidden sleep command:\n%s", args, r.stdout)
		}
	}
	r := runCtl(nil, "enqueue", "-h")
	if r.code != 0 || !strings.Contains(r.stdout, "--queue") || !strings.Contains(r.stdout, "--addr") {
		t.Errorf("enqueue -h = exit %d, stdout %q; want exit 0 and the flags of enqueue, global ones included", r.code, r.stdout)
	}
}

func TestCtlHelpNeverPrintsASecret(t *testing.T) {
	env := map[string]string{
		"JOBQUEUE_TOKEN":        "env-token-value",
		"JOBQUEUE_DATABASE_URL": "postgres://u:env-db-password@h/db",
	}
	for _, args := range [][]string{
		{"stats", "-h"},
		{"migrate", "-h"},
		{"--token", "flag-token-value", "stats", "-h"},
		{"migrate", "--database-url", "postgres://u:flag-db-password@h/db", "-h"},
		{"--token", "flag-token-value", "help"},
	} {
		r := runCtl(env, args...)
		all := r.stdout + r.stderr
		for _, secret := range []string{"env-token-value", "env-db-password", "flag-token-value", "flag-db-password"} {
			if strings.Contains(all, secret) {
				t.Errorf("jobqueuectl %v printed the secret %q:\n%s", args, secret, all)
			}
		}
		if r.code != 0 {
			t.Errorf("jobqueuectl %v = exit %d, want 0", args, r.code)
		}
	}
}

func TestCtlServerErrorsExit1(t *testing.T) {
	b := startBroker(t, nil)
	for name, tc := range map[string]struct {
		args []string
		want string
	}{
		"get of an unknown id":      {[]string{"get", "999"}, "NotFound"},
		"cancel of an unknown id":   {[]string{"cancel", "999"}, "NotFound"},
		"requeue of an unknown id":  {[]string{"requeue", "999"}, "NotFound"},
		"stats of a bad queue name": {[]string{"stats", "--queue", "a b"}, "InvalidArgument"},
	} {
		t.Run(name, func(t *testing.T) {
			r := b.ctl(tc.args...)
			if r.code != 1 || r.stdout != "" || !strings.HasPrefix(r.stderr, "jobqueuectl: ") || !strings.Contains(r.stderr, tc.want) {
				t.Fatalf("jobqueuectl %v = exit %d, stdout %q, stderr %q; want exit 1 and %s", tc.args, r.code, r.stdout, r.stderr, tc.want)
			}
			if strings.Count(r.stderr, "\n") != 1 {
				t.Errorf("stderr %q is not one line", r.stderr)
			}
		})
	}
}

// field returns the value that follows "name: " on a line of out.
func field(t *testing.T, out, name string) string {
	t.Helper()
	for l := range strings.SplitSeq(out, "\n") {
		if v, ok := strings.CutPrefix(l, name+": "); ok {
			return v
		}
	}
	t.Fatalf("no %q line in %q", name, out)
	return ""
}

func TestCtlTokenNewLineLoads(t *testing.T) {
	for name, tc := range map[string]struct {
		args []string
		want broker.Principal
	}{
		"producer": {[]string{"token", "new", "--role", "producer", "--tenant", "acme"}, broker.Principal{Role: broker.RoleProducer, Tenant: "acme"}},
		"worker":   {[]string{"token", "new", "--role", "worker", "--queues", "b,a,b"}, broker.Principal{Role: broker.RoleWorker, Queues: []string{"a", "b"}}},
		"admin":    {[]string{"token", "new", "--role", "admin"}, broker.Principal{Role: broker.RoleAdmin}},
	} {
		t.Run(name, func(t *testing.T) {
			r := runCtl(nil, tc.args...)
			if r.code != 0 || r.stderr != "" || strings.Count(r.stdout, "\n") != 2 {
				t.Fatalf("exit %d, stdout %q, stderr %q; want exit 0 and the token and line lines", r.code, r.stdout, r.stderr)
			}
			token, line := field(t, r.stdout, "token"), field(t, r.stdout, "line")
			if !strings.HasPrefix(r.stdout, "token: ") || strings.Index(r.stdout, "line: ") < strings.Index(r.stdout, "token: ") {
				t.Errorf("output %q must give the token first and the line second", r.stdout)
			}

			raw, err := base64.RawURLEncoding.DecodeString(token)
			if err != nil || len(raw) != 32 {
				t.Fatalf("token %q: decoded to %d bytes, %v; want 32 bytes of unpadded base64url", token, len(raw), err)
			}
			if strings.Contains(line, token) {
				t.Fatalf("the tokens-file line %q contains the token itself, not only its hash", line)
			}
			auth, err := broker.LoadTokens(strings.NewReader(line + "\n"))
			if err != nil {
				t.Fatalf("LoadTokens(%q): %v", line, err)
			}
			got, ok := auth.Authenticate(token)
			if !ok || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("Authenticate(token) = %+v, %v; want %+v", got, ok, tc.want)
			}
			if _, ok := auth.Authenticate(token + "x"); ok {
				t.Error("a different token authenticated")
			}

			again := runCtl(nil, tc.args...)
			if field(t, again.stdout, "token") == token {
				t.Error("two runs printed the same token")
			}
			var j struct{ Token, Line string }
			js := runCtl(nil, append(tc.args, "--json")...)
			if err := json.Unmarshal([]byte(js.stdout), &j); err != nil || j.Token == "" || j.Line == "" {
				t.Errorf("--json printed %q (%v); want an object with token and line", js.stdout, err)
			}
		})
	}
}

func TestCtlTokenAuthenticatesOverTheWire(t *testing.T) {
	mint := func(args ...string) (token, line string) {
		r := runCtl(nil, append([]string{"token", "new"}, args...)...)
		if r.code != 0 {
			t.Fatalf("token new %v = exit %d, %q", args, r.code, r.stderr)
		}
		return field(t, r.stdout, "token"), field(t, r.stdout, "line")
	}
	adminTok, adminLine := mint("--role", "admin")
	prodTok, prodLine := mint("--role", "producer", "--tenant", "acme")
	auth, err := broker.LoadTokens(strings.NewReader(adminLine + "\n" + prodLine + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	b := startBroker(t, auth)

	check := func(name string, r result, code int, want string) {
		t.Helper()
		if r.code != code || !strings.Contains(r.stdout+r.stderr, want) {
			t.Errorf("%s = exit %d, stdout %q, stderr %q; want exit %d with %q", name, r.code, r.stdout, r.stderr, code, want)
		}
	}
	check("admin by flag", b.ctl("--token", adminTok, "stats", "--queue", "q"), 0, "TENANT")
	check("admin by environment", runCtl(map[string]string{"JOBQUEUE_TOKEN": adminTok}, "--addr", b.addr, "--insecure", "stats", "--queue", "q"), 0, "TENANT")
	check("a flag beats the environment", runCtl(map[string]string{"JOBQUEUE_TOKEN": "wrong"}, "--addr", b.addr, "--insecure", "stats", "--queue", "q", "--token", adminTok), 0, "TENANT")
	check("no token", b.ctl("stats", "--queue", "q"), 1, "Unauthenticated")
	check("a wrong token", b.ctl("--token", "wrong", "stats", "--queue", "q"), 1, "Unauthenticated")

	check("producer enqueues for its tenant", b.ctl("--token", prodTok, "enqueue", "--queue", "q", "--payload-string", "x"), 0, "deduplicated=false")
	got := b.ctl("--token", adminTok, "get", "1")
	if got.code != 0 {
		t.Fatalf("get = exit %d, %q", got.code, got.stderr)
	}
	if tenant := parseKV(t, got.stdout)["tenant"]; tenant != "acme" {
		t.Errorf("the producer's job has tenant %q, want acme", tenant)
	}
	check("producer enqueues for another tenant", b.ctl("--token", prodTok, "enqueue", "--queue", "q", "--tenant", "other", "--payload-string", "x"), 1, "PermissionDenied")
	check("producer reads stats", b.ctl("--token", prodTok, "stats", "--queue", "q"), 1, "PermissionDenied")
}

func TestCtlSleep(t *testing.T) {
	start := time.Now()
	r := runCtl(nil, "sleep", "40ms")
	if elapsed := time.Since(start); r.code != 0 || r.stdout != "" || r.stderr != "" || elapsed < 40*time.Millisecond {
		t.Fatalf("sleep 40ms = exit %d, stdout %q, stderr %q after %v; want exit 0, silent, after at least 40ms", r.code, r.stdout, r.stderr, elapsed)
	}
	start = time.Now()
	if r := runCtl(nil, "sleep", "0s"); r.code != 0 || time.Since(start) > time.Second {
		t.Fatalf("sleep 0s = exit %d after %v; want exit 0 at once", r.code, time.Since(start))
	}
}

func TestCtlVersionPrintsTheVersionOfTheBuild(t *testing.T) {
	// The release build sets version with -ldflags -X main.version=..., so a
	// binary says which release it is. No broker is involved: the command works
	// with nothing running. --version and -version do the same as the server's,
	// and the version command does the same as both.
	want := version + "\n"
	for _, args := range [][]string{{"version"}, {"--version"}, {"-version"}, {"--insecure", "--version"}} {
		if r := runCtl(nil, args...); r.code != 0 || r.stdout != want || r.stderr != "" {
			t.Errorf("jobqueuectl %v = exit %d, stdout %q, stderr %q; want exit 0, %q and no stderr", args, r.code, r.stdout, r.stderr, want)
		}
	}
	wantJSON := `{"version":"` + version + "\"}\n"
	for _, args := range [][]string{{"version", "--json"}, {"--json", "--version"}} {
		if r := runCtl(nil, args...); r.code != 0 || r.stdout != wantJSON || r.stderr != "" {
			t.Errorf("jobqueuectl %v = exit %d, stdout %q, stderr %q; want exit 0 and %q", args, r.code, r.stdout, r.stderr, wantJSON)
		}
	}
	// The default is "dev", but a test run with -ldflags -X main.version=...
	// has another value, so only what a line can hold is required of it.
	if version == "" || strings.ContainsAny(version, " \t\r\n") {
		t.Errorf("version is %q, want a word: a build sets it with -ldflags -X main.version=...", version)
	}
	if r := runCtl(nil, "version", "extra"); r.code != 2 || r.stdout != "" || !strings.Contains(r.stderr, `"extra"`) {
		t.Errorf("version extra = exit %d, stdout %q, stderr %q; want exit 2 naming the argument", r.code, r.stdout, r.stderr)
	}
	// --version is a flag of the command line before the command's name, like
	// the other global flags: a command does not take it.
	if r := runCtl(nil, "get", "5", "--version"); r.code != 2 || r.stdout != "" {
		t.Errorf("get 5 --version = exit %d, stdout %q; want exit 2 and nothing on stdout", r.code, r.stdout)
	}
	r := runCtl(nil, "help")
	if !strings.Contains(r.stdout, "\n  version\n") {
		t.Errorf("help does not list the version command:\n%s", r.stdout)
	}
	// --version is accepted in front of the command's name only, so the help
	// lists it under a heading of its own and not with the global flags, which
	// every command takes before and after its name.
	global := strings.Index(r.stdout, "global flags, accepted before and after the command name:")
	only := strings.Index(r.stdout, "accepted before the command name only:")
	switch {
	case global < 0 || only < global:
		t.Errorf("help has no heading for a flag of the command line in front of the command, after the global flags:\n%s", r.stdout)
	case strings.Contains(r.stdout[global:only], "--version"):
		t.Errorf("help lists --version with the global flags, which every command takes:\n%s", r.stdout)
	case !strings.Contains(r.stdout[only:], "--version"):
		t.Errorf("help does not list the --version flag under its own heading:\n%s", r.stdout)
	}
}

func TestCtlMigrateErrorsDoNotLeakThePassword(t *testing.T) {
	const password = "hunter2-s3cret"
	for name, tc := range map[string]struct {
		env  map[string]string
		args []string
	}{
		"nothing listens": {nil, []string{"migrate", "--database-url", "postgres://u:" + password + "@127.0.0.1:1/db?sslmode=disable"}},
		"a malformed url": {map[string]string{"JOBQUEUE_DATABASE_URL": "postgres://u:" + password + "@127.0.0.1:notaport/db"}, []string{"migrate"}},
	} {
		t.Run(name, func(t *testing.T) {
			r := runCtl(tc.env, tc.args...)
			if r.code != 1 || r.stdout != "" || r.stderr == "" {
				t.Fatalf("exit %d, stdout %q, stderr %q; want exit 1 and a message", r.code, r.stdout, r.stderr)
			}
			if strings.Contains(r.stderr, password) {
				t.Fatalf("the error shows the password: %q", r.stderr)
			}
		})
	}
}

// TestCtlMigrateStopsAtTheTimeout runs migrate against a server that accepts
// connections and never answers, as a database behind a dead network does.
func TestCtlMigrateStopsAtTheTimeout(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var held []net.Conn
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, c) // never read from or written to
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = lis.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range held {
			_ = c.Close()
		}
	})

	url := "postgres://u:p@" + lis.Addr().String() + "/db?sslmode=disable"
	done := make(chan result, 1)
	start := time.Now()
	go func() { done <- runCtl(nil, "migrate", "--database-url", url, "--timeout", "300ms") }()
	select {
	case r := <-done:
		elapsed := time.Since(start)
		if r.code != 1 || r.stdout != "" || r.stderr == "" {
			t.Fatalf("migrate = exit %d, stdout %q, stderr %q; want exit 1 and a message", r.code, r.stdout, r.stderr)
		}
		if elapsed < 250*time.Millisecond || elapsed > 5*time.Second {
			t.Fatalf("migrate took %v with --timeout 300ms; want it to stop at the timeout", elapsed)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("migrate was still running 10s after --timeout 300ms")
	}
}
