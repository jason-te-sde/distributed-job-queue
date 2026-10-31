package exp

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/jason-te-sde/distributed-job-queue/internal/broker"
	jobqueuev1 "github.com/jason-te-sde/distributed-job-queue/internal/gen/jobqueue/v1"
)

func discard(Result) {}

// refuses runs the experiment and checks that it refuses the options before it
// touches a database: the URL is one that nothing listens on, so an experiment
// that got as far as connecting would fail with a different message.
func refuses(t *testing.T, name, wantInMessage string, run func() error) {
	t.Helper()
	err := run()
	if err == nil {
		t.Errorf("%s: no error", name)
		return
	}
	if !strings.Contains(err.Error(), wantInMessage) {
		t.Errorf("%s: error = %q, want it to mention %q", name, err, wantInMessage)
	}
}

func TestThroughputRefusesBadOptions(t *testing.T) {
	ok := ThroughputOptions{Setup: Setup{DatabaseURL: "postgres://127.0.0.1:1/none"}, Rate: 100, Duration: time.Second}
	for name, tc := range map[string]struct {
		mutate func(*ThroughputOptions)
		want   string
	}{
		"no database":          {func(o *ThroughputOptions) { o.DatabaseURL = "" }, "database URL"},
		"neither rate nor max": {func(o *ThroughputOptions) { o.Rate = 0 }, "--rate"},
		"both rate and max":    {func(o *ThroughputOptions) { o.FindMax = true }, "alternatives"},
		"payload under stamp":  {func(o *ThroughputOptions) { o.PayloadBytes = 4 }, "payload"},
		"payload over limit":   {func(o *ThroughputOptions) { o.PayloadBytes = 262145 }, "payload"},
		"negative brokers":     {func(o *ThroughputOptions) { o.Brokers = -1 }, "brokers"},
		"too many brokers":     {func(o *ThroughputOptions) { o.Brokers = 9 }, "brokers"},
		"negative workers":     {func(o *ThroughputOptions) { o.Workers = -1 }, "workers"},
		"too much concurrency": {func(o *ThroughputOptions) { o.Concurrency = 10001 }, "concurrency"},
		"negative duration":    {func(o *ThroughputOptions) { o.Duration = -time.Second }, "duration"},
	} {
		o := ok
		tc.mutate(&o)
		refuses(t, name, tc.want, func() error { return Throughput(context.Background(), o, discard) })
	}
}

func TestAblateRefusesBadOptions(t *testing.T) {
	base := ThroughputOptions{Setup: Setup{DatabaseURL: "postgres://127.0.0.1:1/none"}, Rate: 100, Duration: time.Second}
	refuses(t, "unknown ablation", "--what", func() error {
		return Ablate(context.Background(), AblateOptions{ThroughputOptions: base, What: "fsync"}, discard)
	})
	refuses(t, "notify with find-max", "fixed rate", func() error {
		o := base
		o.Rate, o.FindMax = 0, true
		return Ablate(context.Background(), AblateOptions{ThroughputOptions: o, What: "notify"}, discard)
	})
}

func TestNoisyRefusesBadOptions(t *testing.T) {
	ok := NoisyOptions{Setup: Setup{DatabaseURL: "postgres://127.0.0.1:1/none"}, Scheduler: "drr"}
	for name, tc := range map[string]struct {
		mutate func(*NoisyOptions)
		want   string
	}{
		"no database":       {func(o *NoisyOptions) { o.DatabaseURL = "" }, "database URL"},
		"unknown scheduler": {func(o *NoisyOptions) { o.Scheduler = "lifo" }, "--scheduler"},
		"negative flood":    {func(o *NoisyOptions) { o.Flood = -1 }, "flood"},
		"negative b rate":   {func(o *NoisyOptions) { o.BRate = -1 }, "b-rate"},
		"negative service":  {func(o *NoisyOptions) { o.ServiceTime = -time.Millisecond }, "service-time"},
	} {
		o := ok
		tc.mutate(&o)
		refuses(t, name, tc.want, func() error { return Noisy(context.Background(), o, discard) })
	}
}

func TestRecoveryRefusesBadOptions(t *testing.T) {
	ok := RecoveryOptions{Setup: Setup{DatabaseURL: "postgres://127.0.0.1:1/none"}, Kill: "worker"}
	for name, tc := range map[string]struct {
		mutate func(*RecoveryOptions)
		want   string
	}{
		"no database":      {func(o *RecoveryOptions) { o.DatabaseURL = "" }, "database URL"},
		"unknown target":   {func(o *RecoveryOptions) { o.Kill = "database" }, "--kill"},
		"ttl under second": {func(o *RecoveryOptions) { o.TTL = 500 * time.Millisecond }, "ttl"},
		"margin too large": {func(o *RecoveryOptions) { o.TTL, o.Margin = 4*time.Second, time.Second }, "margin"},
		"negative k":       {func(o *RecoveryOptions) { o.K = -1 }, "-k"},
	} {
		o := ok
		tc.mutate(&o)
		refuses(t, name, tc.want, func() error { return Recovery(context.Background(), o, discard) })
	}
}

func TestSoakRefusesBadOptions(t *testing.T) {
	ok := SoakOptions{Setup: Setup{DatabaseURL: "postgres://127.0.0.1:1/none"}, Rate: 100}
	for name, tc := range map[string]struct {
		mutate func(*SoakOptions)
		want   string
	}{
		"no database":        {func(o *SoakOptions) { o.DatabaseURL = "" }, "database URL"},
		"no rate":            {func(o *SoakOptions) { o.Rate = 0 }, "--rate"},
		"negative duration":  {func(o *SoakOptions) { o.Duration = -time.Minute }, "duration"},
		"negative retention": {func(o *SoakOptions) { o.Retention = -time.Minute }, "retention"},
	} {
		o := ok
		tc.mutate(&o)
		refuses(t, name, tc.want, func() error { return Soak(context.Background(), o, discard) })
	}
}

func TestCapLeaseJobsLowersOnlyTheLeaseRequestsThatAskForMore(t *testing.T) {
	var seen []int32
	invoker := func(_ context.Context, _ string, req, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		if r, ok := req.(*jobqueuev1.LeaseRequest); ok {
			seen = append(seen, r.GetMaxJobs())
		}
		return nil
	}
	intercept := capLeaseJobs(1)
	for _, ask := range []int32{64, 1, 17} {
		if err := intercept(context.Background(), "/jobqueue.v1.JobQueueService/Lease", &jobqueuev1.LeaseRequest{Queue: "q", MaxJobs: ask}, nil, nil, invoker); err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != 3 || seen[0] != 1 || seen[1] != 1 || seen[2] != 1 {
		t.Errorf("the broker saw max_jobs %v for requests of 64, 1 and 17, want 1 each", seen)
	}

	// A call that is not a Lease passes untouched.
	ack := &jobqueuev1.AckRequest{Items: []*jobqueuev1.AckItem{{}, {}, {}}}
	if err := intercept(context.Background(), "/jobqueue.v1.JobQueueService/Ack", ack, nil, nil, invoker); err != nil {
		t.Fatal(err)
	}
	if len(ack.GetItems()) != 3 {
		t.Errorf("an Ack request was changed: %d items left of 3", len(ack.GetItems()))
	}
	// And the cap is a cap, not a floor: 64 is the most the SDK ever asks.
	seen = nil
	if err := capLeaseJobs(64)(context.Background(), "Lease", &jobqueuev1.LeaseRequest{MaxJobs: 5}, nil, nil, invoker); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 || seen[0] != 5 {
		t.Errorf("a request for 5 under a cap of 64 reached the broker as %v, want 5", seen)
	}
}

func TestVariantsSwitchTheFeatureOff(t *testing.T) {
	batch, err := variants("batch")
	if err != nil {
		t.Fatal(err)
	}
	if batch[0].Spec.LeaseMaxJobs != 0 || batch[1].Spec.LeaseMaxJobs != 1 || batch[0].Name != "max_jobs_64" || batch[1].Name != "max_jobs_1" {
		t.Errorf("batch variants = %+v, want the SDK's own max_jobs first and a cap of 1 second", batch)
	}
	coalesce, err := variants("coalesce")
	if err != nil {
		t.Fatal(err)
	}
	if coalesce[0].Spec.AckBatch != 0 || coalesce[1].Spec.AckBatch != 1 || coalesce[1].Name != "ack_batch_1" {
		t.Errorf("coalesce variants = %+v, want the default AckBatch first and 1 second", coalesce)
	}
	notify, err := variants("notify")
	if err != nil {
		t.Fatal(err)
	}
	if notify[0].Cluster.DisableListen || !notify[1].Cluster.DisableListen || notify[1].Params["disable_listen"] != "true" {
		t.Errorf("notify variants = %+v, want the listener on first and --disable-listen second", notify)
	}
}

func TestSDKProbeKeepsTheWorkerSpecOfTheVariant(t *testing.T) {
	o := ThroughputOptions{Workers: 3, Concurrency: 7, PayloadBytes: 100}
	p := (&rig{}).sdkProbeFor(o, workerSpec{LeaseMaxJobs: 1, AckBatch: 1})
	if p.spec.Count != 3 || p.spec.Concurrency != 7 || p.spec.LeaseMaxJobs != 1 || p.spec.AckBatch != 1 || p.payloadBytes != 100 {
		t.Errorf("spec = %+v, payload %d; want the options' count and concurrency with the variant's cap and batch kept", p.spec, p.payloadBytes)
	}
}

// postgresDefaultMaxConnections is max_connections of a PostgreSQL server that
// nobody has tuned.
const postgresDefaultMaxConnections = 100

func TestMaxBrokersFitThePostgresDefaultConnectionLimit(t *testing.T) {
	cfg, err := broker.ParseFlags(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Each broker holds a pool of MaxDBConns and one connection for its NOTIFY
	// listener, and the harness holds a pool of its own.
	perBroker := int(cfg.MaxDBConns) + 1
	if got := maxBrokers*perBroker + benchPoolConns; got > postgresDefaultMaxConnections {
		t.Errorf("%d brokers of %d connections and the harness's %d need %d connections, over the %d of a default PostgreSQL",
			maxBrokers, perBroker, benchPoolConns, got, postgresDefaultMaxConnections)
	}
}

func TestBrokerKillResultStatesWhatWasKilled(t *testing.T) {
	p := RecoveryOptions{Kill: "broker", Rate: 100, Duration: 6 * time.Second}.withDefaults().brokerParams()
	if got, want := p["mode"], "connections reset through a proxy; broker process stays up"; got != want {
		t.Errorf("params[mode] = %q, want %q: the JSON must say that a proxy was cut and the broker was not stopped", got, want)
	}
	if p["kill"] != "broker" || p["brokers"] != "3" || p["rate"] != "100" || p["duration"] != "6s" {
		t.Errorf("params = %v, want the kill, the three brokers and the load", p)
	}
}
