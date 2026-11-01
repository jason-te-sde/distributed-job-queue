//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	jobqueue "github.com/jason-te-sde/distributed-job-queue"
)

const (
	imageEnv = "JOBQUEUE_E2E_IMAGE"

	// The chart names its objects after the release when the release name
	// already contains the chart's name, so the release "jobqueue" gives these.
	release      = "jobqueue"
	brokerDeploy = "jobqueue"
	grpcService  = "jobqueue-grpc"
	workerDeploy = "jobqueue-example-worker"
	scaledObject = "jobqueue-example-worker"
	grpcPort     = 7070
	// brokerSelector selects the broker pods of the release.
	brokerSelector = "app.kubernetes.io/instance=" + release + ",app.kubernetes.io/component=broker"

	// The Secrets the chart reads; the chart creates none.
	databaseSecret    = "jobqueue-database"
	tokensSecret      = "jobqueue-tokens"
	workerTokenSecret = "jobqueue-worker-token"
	databaseURL       = "postgres://postgres:postgres@postgres:5432/postgres?sslmode=disable"

	// queue is the queue of the example worker, and tenant the tenant that the
	// producer token is bound to.
	queue  = "thumbnails"
	tenant = "e2e"

	// moduleCtl is the package of the command that mints the tokens.
	moduleCtl = "github.com/jason-te-sde/distributed-job-queue/cmd/jobqueuectl"
)

// Source edges, in pixels, of the pictures that the jobs ask the worker to
// draw. The cost of a job grows with the square of the edge. Measured on an
// Apple M2 with a benchmark of the thumbnail function alone, one job took 2.2 ms
// at 256, 5 ms at 512, 14 ms at 1024 and 52 ms at 2048. Nothing was measured on
// a CI runner.
const (
	// sizeLight is for the subtest about the brokers. At 2.2 ms a job, one
	// worker thread does about 450 jobs a second on an M2, more than ten times
	// the 40 a second that the subtest enqueues, so a late job is unlikely to be
	// the worker's doing. A runner that is five times slower still has more than
	// twice the rate it needs.
	sizeLight = 256
	// sizeDefault is the edge that `thumbnail produce` uses.
	sizeDefault = 1024
	// sizeHeavy is the largest edge the worker accepts. The scaling subtest
	// uses it so that a backlog of 500 jobs lasts longer than the autoscaler
	// needs to act on it.
	sizeHeavy = 2048
)

// scalingConcurrency is how many jobs a worker pod runs at once in the scaling
// subtest, which sets it with helm upgrade before it starts; the other
// subtests run with the chart's default of 4. With 4, one pod would drain 500
// jobs of the heavy size in about 7 seconds on an idle M2 (500 jobs of 52 ms,
// four at a time). That is less than the 15 seconds between two looks of the
// HPA, so the backlog could be gone before the autoscaler sees it, and whether
// the subtest passed would depend on the speed of the machine. With 1 the pod
// needs 29 seconds, as measured with the replicas capped at one, and the
// autoscaler must scale out for the backlog to clear sooner.
const scalingConcurrency = 1

// fixture is what the subtests hand to each other.
type fixture struct {
	// producerToken is the bearer token of the producer for tenant e2e.
	producerToken string
}

// TestE2E installs the stack once and runs the subtests in order: each builds
// on the state of the one before, so a failure stops the run.
func TestE2E(t *testing.T) {
	image := os.Getenv(imageEnv)
	if image == "" {
		t.Fatalf("%s is not set: name an image that is already loaded into the kind cluster, for example jq:e2e", imageEnv)
	}
	repository, tag, err := splitImage(image)
	if err != nil {
		t.Fatalf("%s=%q: %v", imageEnv, image, err)
	}
	pinKindCluster(t)
	requireKEDA(t)
	t.Cleanup(func() {
		if t.Failed() {
			dumpCluster(t)
		}
	})

	f := &fixture{}
	steps := []struct {
		name string
		run  func(*testing.T)
	}{
		{"Install", func(t *testing.T) { f.install(t, repository, tag) }},
		{"ProcessesJobs", f.processesJobs},
		{"BrokerPodDeletionAndRollingRestartUnderLoad", f.brokerPodDeletionAndRollingRestartUnderLoad},
		{"KEDAScalesToZeroAndBack", f.kedaScalesToZeroAndBack},
	}
	for _, s := range steps {
		if !t.Run(s.name, s.run) {
			t.Fatalf("%s failed, and the subtests after it need what it set up", s.name)
		}
	}
}

// splitImage splits an image reference of the form [registry/]name:tag into
// the repository and the tag, the two values the chart takes.
func splitImage(image string) (repository, tag string, err error) {
	i := strings.LastIndex(image, ":")
	if i < 0 || strings.Contains(image[i:], "/") || i == len(image)-1 {
		return "", "", errors.New("want name:tag, as the chart takes a repository and a tag; digests are not supported")
	}
	return image[:i], image[i+1:], nil
}

// install puts PostgreSQL, the Secrets and the chart in a new namespace and
// waits for the brokers.
func (f *fixture) install(t *testing.T, repository, tag string) {
	kubectl(t, "create", "namespace", namespace())
	kubectl(t, "apply", "--filename", postgresManifest)
	waitRollout(t, "postgres", 3*time.Minute)

	// The tokens come from the command that mints them. Only their hashes go
	// into the broker's tokens file; each Secret is made from a file, so that no
	// token appears on a command line.
	dir := t.TempDir()
	ctl := buildCtl(t, dir)
	worker := mintToken(t, ctl, "--role", "worker", "--queues", queue)
	producer := mintToken(t, ctl, "--role", "producer", "--tenant", tenant)
	f.producerToken = producer.Token

	tokensFile := filepath.Join(dir, "tokens")
	writeFile(t, tokensFile, worker.Line+"\n"+producer.Line+"\n")
	workerTokenFile := filepath.Join(dir, "worker-token")
	writeFile(t, workerTokenFile, worker.Token)

	kubectl(t, "create", "secret", "generic", databaseSecret, "--from-literal=url="+databaseURL)
	kubectl(t, "create", "secret", "generic", tokensSecret, "--from-file=tokens="+tokensFile)
	kubectl(t, "create", "secret", "generic", workerTokenSecret, "--from-file=token="+workerTokenFile)

	// insecure: the cluster has no cert-manager, so the brokers serve plaintext.
	// The tokens stay on: a broker with a tokens file requires one from every
	// caller, and the worker presents the one of its Secret.
	helmInstall(t, release,
		"image.repository="+repository,
		"image.tag="+tag,
		"insecure=true",
		"tokens.secretName="+tokensSecret,
		"scaler.enabled=true",
		"example.enabled=true",
		"example.tokenSecret="+workerTokenSecret,
		"example.tokenKey=token",
		"example.keda.enabled=true",
	)
	waitRollout(t, brokerDeploy, 3*time.Minute)
}

// token is what `jobqueuectl token new --json` prints.
type token struct {
	// Token is the credential; Line is the line of the tokens file that lists
	// its hash.
	Token string `json:"token"`
	Line  string `json:"line"`
}

// buildCtl builds jobqueuectl into dir and returns its path.
func buildCtl(t *testing.T, dir string) string {
	t.Helper()
	bin := filepath.Join(dir, "jobqueuectl")
	if _, err := execute(5*time.Minute, "go", "build", "-o", bin, moduleCtl); err != nil {
		t.Fatalf("%v", err)
	}
	return bin
}

// mintToken runs `jobqueuectl token new` with args.
func mintToken(t *testing.T, ctl string, args ...string) token {
	t.Helper()
	out, err := execute(commandTimeout, ctl, append([]string{"token", "new", "--json"}, args...)...)
	if err != nil {
		t.Fatalf("%v", err)
	}
	var tok token
	if err := json.Unmarshal([]byte(out), &tok); err != nil || tok.Token == "" || tok.Line == "" {
		t.Fatalf("jobqueuectl token new printed something else than a token and a line (decode error %v)", err)
	}
	return tok
}

// writeFile writes content to path, readable by the owner only.
func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// connect forwards the brokers' gRPC port and returns a producer for tenant e2e
// that goes through it. The forward is bound to one broker pod, so when that
// pod goes away the producer meets a refused or dropped connection until the
// forward is back. Its retry policy is generous enough to ride that out, and
// each call keeps one idempotency key across its attempts, so a retry never
// enqueues a job twice.
func (f *fixture) connect(t *testing.T) *jobqueue.Producer {
	t.Helper()
	addr := portForward(t, grpcService, grpcPort)
	conn, err := jobqueue.Dial(addr, jobqueue.WithInsecure(), jobqueue.WithToken(f.producerToken))
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return jobqueue.NewProducer(conn, jobqueue.ProducerOptions{
		Tenant: tenant,
		Retry:  jobqueue.RetryPolicy{Attempts: 20, Base: 100 * time.Millisecond, Cap: time.Second},
	})
}

// enqueueBatch enqueues n jobs in one call, n at most 64, with the seeds
// from, from+1 and so on, and returns the error of the call.
func enqueueBatch(ctx context.Context, p *jobqueue.Producer, from, n, size int) error {
	items := make([]jobqueue.EnqueueParams, n)
	for i := range items {
		items[i] = jobqueue.EnqueueParams{
			Queue:   queue,
			Payload: []byte(fmt.Sprintf(`{"seed":%d,"size":%d}`, from+i, size)),
		}
	}
	_, err := p.EnqueueBatch(ctx, items)
	return err
}

// enqueueAll enqueues n jobs, in batches of 64, and fails the test if a call
// fails.
func enqueueAll(t *testing.T, p *jobqueue.Producer, n, size int) {
	t.Helper()
	for from := 0; from < n; from += 64 {
		if err := enqueueBatch(t.Context(), p, from, min(64, n-from), size); err != nil {
			t.Fatalf("enqueue jobs %d..%d: %v", from, min(from+64, n)-1, err)
		}
	}
}

// maxJobID is the highest job id so far, or 0. A subtest takes it before it
// enqueues, and then looks only at the jobs above it: its own.
func maxJobID(t *testing.T) int64 {
	t.Helper()
	return mustInt(t, psql(t, "SELECT coalesce(max(id), 0) FROM jobqueue.jobs"))
}

// mustInt parses the output of a query that returns one integer.
func mustInt(t *testing.T, s string) int64 {
	t.Helper()
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		t.Fatalf("want an integer from the database, got %q", s)
	}
	return n
}

// jobCounts counts the jobs above base by state.
func jobCounts(base int64) (map[string]int, error) {
	out, err := psqlErr(fmt.Sprintf("SELECT state, count(*) FROM jobqueue.jobs WHERE id > %d GROUP BY state", base))
	if err != nil {
		return nil, err
	}
	counts := map[string]int{}
	for line := range strings.SplitSeq(out, "\n") {
		if line == "" {
			continue
		}
		state, n, ok := strings.Cut(line, "|")
		count, err := strconv.Atoi(n)
		if !ok || err != nil {
			return nil, fmt.Errorf("unexpected row %q", line)
		}
		counts[state] = count
	}
	return counts, nil
}

// formatCounts renders counts as "dead=0 ready=3 succeeded=7".
func formatCounts(counts map[string]int) string {
	states := make([]string, 0, len(counts))
	for s := range counts {
		states = append(states, s)
	}
	sort.Strings(states)
	parts := make([]string, len(states))
	for i, s := range states {
		parts[i] = fmt.Sprintf("%s=%d", s, counts[s])
	}
	return strings.Join(parts, " ")
}

// waitSucceeded waits until exactly want jobs above base exist and every one
// has succeeded. It fails at once if a job is dead, which no run of these
// tests may produce, and when timeout passes.
func waitSucceeded(t *testing.T, base int64, want int, timeout time.Duration) {
	t.Helper()
	eventually(t, timeout, time.Second, fmt.Sprintf("%d jobs succeeded", want), func() (bool, string) {
		counts, err := jobCounts(base)
		if err != nil {
			t.Fatalf("count the jobs: %v", err)
		}
		if counts["dead"] > 0 {
			t.Fatalf("%d job(s) are dead:\n%s", counts["dead"], psql(t,
				fmt.Sprintf("SELECT id, attempts, max_attempts, last_error FROM jobqueue.jobs WHERE id > %d AND state = 'dead' ORDER BY id LIMIT 10", base)))
		}
		total := 0
		for _, n := range counts {
			total += n
		}
		if total > want {
			t.Fatalf("%d jobs exist above id %d, but only %d were enqueued: %s", total, base, want, formatCounts(counts))
		}
		return counts["succeeded"] == want, formatCounts(counts)
	})
}

// requireThumbnails checks that the worker wrote the result of every one of
// the want jobs above base to the sink: one row each, under the job's id.
func requireThumbnails(t *testing.T, base int64, want int) {
	t.Helper()
	got := mustInt(t, psql(t, fmt.Sprintf(
		"SELECT count(*) FROM thumbnails t JOIN jobqueue.jobs j ON j.id = t.job_id WHERE j.id > %d AND j.state = 'succeeded'", base)))
	if got != int64(want) {
		t.Fatalf("the sink holds %d thumbnails for the %d succeeded jobs", got, want)
	}
}

// requireNoDeadJobs checks that no job anywhere in the database is dead.
func requireNoDeadJobs(t *testing.T) {
	t.Helper()
	if n := mustInt(t, psql(t, "SELECT count(*) FROM jobqueue.jobs WHERE state = 'dead'")); n != 0 {
		t.Fatalf("%d dead job(s):\n%s", n, psql(t, "SELECT id, attempts, max_attempts, last_error FROM jobqueue.jobs WHERE state = 'dead' ORDER BY id LIMIT 10"))
	}
}

// processesJobs waits for the idle example worker to be at zero replicas, then
// enqueues 300 jobs. That every job succeeds proves that KEDA raised the worker
// from zero on the strength of the broker's backlog.
func (f *fixture) processesJobs(t *testing.T) {
	const (
		jobs = 300
		// within is the whole budget, from the enqueue to the last job. KEDA's
		// first activation of a new ScaledObject is slow: its scaler cannot answer
		// until the brokers are up, and KEDA retries with a backoff, so the first
		// replica can take twenty seconds. That wait is part of the budget, not a
		// bound of its own.
		within = 180 * time.Second
	)
	// The chart leaves spec.replicas of the worker to KEDA, so the Deployment
	// is created with one replica, the Kubernetes default, and stays there until
	// KEDA first looks at the idle queue. That look can be late (see within), and
	// the scale-up below proves something only if the jobs arrive at zero, so
	// wait for it. The bound is that of the scale-down in kedaScalesToZeroAndBack.
	const toZeroWithin = 30*time.Second + 90*time.Second
	eventually(t, toZeroWithin, 2*time.Second, "KEDA takes the idle example worker to zero", func() (bool, string) {
		desired, ready, err := replicaCounts(workerDeploy)
		if err != nil {
			t.Fatalf("%v", err)
		}
		return desired == 0 && ready == 0, fmt.Sprintf("%d replicas asked for, %d ready", desired, ready)
	})
	base := maxJobID(t)
	p := f.connect(t)
	enqueueAll(t, p, jobs, sizeDefault)
	enqueued := time.Now()

	// The jobs take longer than a poll to run, so a ready replica is visible
	// before the last of them finishes.
	eventually(t, within, 500*time.Millisecond, "KEDA scales the worker up from zero", func() (bool, string) {
		n, err := readyReplicas(workerDeploy)
		if err != nil {
			t.Fatalf("%v", err)
		}
		return n >= 1, fmt.Sprintf("%d ready replicas", n)
	})
	t.Logf("the worker had a ready replica %s after the jobs were enqueued", time.Since(enqueued).Round(time.Second))

	waitSucceeded(t, base, jobs, time.Until(enqueued.Add(within)))
	requireThumbnails(t, base, jobs)
	t.Logf("all %d jobs succeeded %s after they were enqueued", jobs, time.Since(enqueued).Round(time.Second))
}

// brokerPods lists the broker pods that are not being deleted.
func brokerPods(t *testing.T) []string {
	t.Helper()
	out := kubectl(t, "get", "pods", "--selector", brokerSelector, "--output",
		`go-template={{range .items}}{{if not .metadata.deletionTimestamp}}{{.metadata.name}}{{"\n"}}{{end}}{{end}}`)
	var pods []string
	for line := range strings.SplitSeq(out, "\n") {
		if line != "" {
			pods = append(pods, line)
		}
	}
	sort.Strings(pods)
	return pods
}

// sleepUntil waits until at, and reports whether it did: false means that ctx
// ended first.
func sleepUntil(ctx context.Context, at time.Time) bool {
	timer := time.NewTimer(time.Until(at))
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// brokerPodDeletionAndRollingRestartUnderLoad enqueues 40 jobs a second for a
// minute. At 20s it deletes one broker pod, and at 40s it restarts the broker
// Deployment. What it asserts is the outcome: every one of the 2400 jobs
// succeeded, none is dead, none took 30s or more from enqueue to finish, and
// the faults happened. It does not prove that brokers drain cleanly: killing a
// broker pod with no grace period did not break these assertions in an
// experiment, because a broker holds few leases in memory and workers
// acknowledge through any broker.
func (f *fixture) brokerPodDeletionAndRollingRestartUnderLoad(t *testing.T) {
	const (
		rate      = 40 // jobs per second
		loadFor   = 60 * time.Second
		tick      = 100 * time.Millisecond
		perTick   = int(rate * tick / time.Second)
		batches   = int(loadFor / tick)
		jobs      = batches * perTick
		deleteAt  = 20 * time.Second
		restartAt = 40 * time.Second
		// drainFor is how long after the load ends the jobs may take to finish.
		drainFor = 120 * time.Second
		// maxLatency bounds finished_at - created_at of every job, as the brief
		// does. It is also the lease TTL, so a lease left to expire would show
		// as a job at or above the bound.
		maxLatency = 30 * time.Second
	)
	before := brokerPods(t)
	if len(before) != 2 {
		t.Fatalf("want 2 broker pods before the faults, have %v", before)
	}
	victim := before[0]
	base := maxJobID(t)
	p := f.connect(t)

	ctx, cancel := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	var faultErr error // written by the goroutine below, read after wg.Wait
	defer func() {
		cancel()
		wg.Wait()
	}()

	start := time.Now()
	wg.Add(1)
	go func() {
		defer wg.Done()
		if !sleepUntil(ctx, start.Add(deleteAt)) {
			return
		}
		if _, err := kubectlErr("delete", "pod", victim, "--wait=false"); err != nil {
			faultErr = err
			return
		}
		t.Logf("%s: deleted broker pod %s", time.Since(start).Round(time.Second), victim)
		if !sleepUntil(ctx, start.Add(restartAt)) {
			return
		}
		if _, err := kubectlErr("rollout", "restart", "deployment/"+brokerDeploy); err != nil {
			faultErr = err
			return
		}
		t.Logf("%s: restarted the broker Deployment", time.Since(start).Round(time.Second))
	}()

	// The schedule is open: a batch that is late does not push the next one
	// back, so the next ones follow at once and the rate is kept.
	for b := range batches {
		sleepUntil(ctx, start.Add(time.Duration(b)*tick))
		if err := enqueueBatch(ctx, p, b*perTick, perTick, sizeLight); err != nil {
			t.Fatalf("enqueue the batch at %s: %v", time.Since(start).Round(time.Millisecond), err)
		}
	}
	loadEnd := time.Now()
	t.Logf("enqueued %d jobs in %s", jobs, loadEnd.Sub(start).Round(100*time.Millisecond))

	wg.Wait()
	if faultErr != nil {
		t.Fatalf("inject a fault: %v", faultErr)
	}

	waitSucceeded(t, base, jobs, drainFor)
	t.Logf("all %d jobs succeeded %s after the load ended", jobs, time.Since(loadEnd).Round(time.Second))
	requireNoDeadJobs(t)
	requireThumbnails(t, base, jobs)

	late := mustFloat(t, psql(t, fmt.Sprintf(
		"SELECT coalesce(max(extract(epoch FROM finished_at - created_at)), 0) FROM jobqueue.jobs WHERE id > %d", base)))
	t.Logf("the slowest of the %d jobs took %.2fs from enqueue to finish", jobs, late)
	if late >= maxLatency.Seconds() {
		t.Fatalf("a job took %.2fs from enqueue to finish; every job must take less than %s", late, maxLatency)
	}

	// The faults really happened: the rolling restart replaced every pod that
	// the run started with, the deleted one included, and the Deployment is
	// whole again.
	waitRollout(t, brokerDeploy, 2*time.Minute)
	after := brokerPods(t)
	if len(after) != 2 {
		t.Fatalf("want 2 broker pods after the restart, have %v", after)
	}
	for _, name := range after {
		for _, old := range before {
			if name == old {
				t.Fatalf("broker pod %s is older than the faults: before %v, after %v", name, before, after)
			}
		}
	}
	if n := replicas(t, brokerDeploy); n != 2 {
		t.Fatalf("%d broker replicas are ready after the restart, want 2", n)
	}
}

// mustFloat parses the output of a query that returns one number.
func mustFloat(t *testing.T, s string) float64 {
	t.Helper()
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		t.Fatalf("want a number from the database, got %q", s)
	}
	return f
}

// cooldownPeriod reads the cooldown of the installed ScaledObject, so that the
// bounds follow the value the chart installed and not a copy of it here.
func cooldownPeriod(t *testing.T) time.Duration {
	t.Helper()
	seconds := mustInt(t, kubectl(t, "get", "scaledobject", scaledObject, "-o", "jsonpath={.spec.cooldownPeriod}"))
	return time.Duration(seconds) * time.Second
}

// requireWorkerConcurrency checks that the example worker Deployment runs n
// jobs at once, from the --concurrency argument of its container.
func requireWorkerConcurrency(t *testing.T, n int) {
	t.Helper()
	args := kubectl(t, "get", "deployment", workerDeploy, "-o", "jsonpath={.spec.template.spec.containers[0].args}")
	if want := fmt.Sprintf(`"--concurrency","%d"`, n); !strings.Contains(args, want) {
		t.Fatalf("the example worker runs with %s, want %s", args, want)
	}
}

// kedaScalesToZeroAndBack checks the whole cycle of the example worker: with
// nothing to do it is at zero replicas; a backlog of 500 jobs brings it up,
// and to more than one replica; and once the backlog is gone it returns to
// zero.
func (f *fixture) kedaScalesToZeroAndBack(t *testing.T) {
	const (
		jobs = 500
		// upWithin is how long the first replica may take to become ready.
		upWithin = 60 * time.Second
		// downSlack is what the scale-down may take beyond the cooldown: KEDA's
		// polling, the termination of the pods, and the controller's own delay.
		downSlack = 90 * time.Second
		// processWithin is how long the 500 jobs may take, from the enqueue to
		// the last one. The brief gives no bound for it; this one is the 180s of
		// ProcessesJobs, and the jobs took between 12s and 20s in the local runs.
		processWithin = 180 * time.Second
	)
	settle := cooldownPeriod(t) + downSlack

	toZero := func(what string) {
		t.Helper()
		start := time.Now()
		eventually(t, settle, 2*time.Second, what, func() (bool, string) {
			n, err := readyReplicas(workerDeploy)
			if err != nil {
				t.Fatalf("%v", err)
			}
			return n == 0, fmt.Sprintf("%d ready replicas", n)
		})
		t.Logf("%s: zero ready replicas after %s (bound %s)", what, time.Since(start).Round(time.Second), settle)
	}
	toZero("the worker has no backlog and scales to zero")

	// Only this subtest needs the worker to be slow (see scalingConcurrency).
	// The worker is at zero replicas, so the upgrade has no pod to restart; the
	// check below shows that it took effect.
	helmUpgrade(t, release, "example.concurrency="+strconv.Itoa(scalingConcurrency))
	requireWorkerConcurrency(t, scalingConcurrency)

	base := maxJobID(t)
	p := f.connect(t)
	enqueueAll(t, p, jobs, sizeHeavy)
	enqueued := time.Now()

	// The sampler looks every 250ms, much more often than the subtest does
	// anything else, so that a scale-out is seen however short it is. It records
	// the replicas that the Deployment was asked for as well as the ready ones:
	// if the ready count stays at one, the first tells an autoscaler that never
	// scaled from pods that started too late.
	stopSampling := sampleReplicas(t.Context(), workerDeploy, 250*time.Millisecond)
	eventually(t, upWithin, 500*time.Millisecond, "the backlog scales the worker up", func() (bool, string) {
		n, err := readyReplicas(workerDeploy)
		if err != nil {
			t.Fatalf("%v", err)
		}
		return n >= 1, fmt.Sprintf("%d ready replicas", n)
	})
	t.Logf("the worker had a ready replica %s after the jobs were enqueued", time.Since(enqueued).Round(time.Second))

	waitSucceeded(t, base, jobs, processWithin)
	peaks := stopSampling()
	t.Logf("all %d jobs succeeded %s after they were enqueued; the Deployment was asked for at most %d replicas and at most %d were ready",
		jobs, time.Since(enqueued).Round(time.Second), peaks.Desired, peaks.Ready)
	if peaks.Ready <= 1 {
		why := "the autoscaler never asked for a second replica"
		if peaks.Desired > 1 {
			why = fmt.Sprintf("the autoscaler asked for %d replicas, but the pods were not ready before the jobs were done", peaks.Desired)
		}
		t.Fatalf("the worker never had more than %d ready replica(s) while %d jobs were processed, and a backlog of %d with a target of 10 per replica must scale it out: %s",
			peaks.Ready, jobs, jobs, why)
	}
	requireThumbnails(t, base, jobs)

	toZero("the worker has no backlog again and scales to zero")
}

// dumpCluster logs what the cluster looked like when a test failed: the
// workloads, the autoscaling objects, the recent events and the tail of every
// broker and worker log. The CI job collects more, as artifacts.
func dumpCluster(t *testing.T) {
	t.Helper()
	for _, args := range [][]string{
		{"get", "pods", "--output", "wide"},
		{"get", "scaledobject,hpa"},
		{"get", "events", "--sort-by", ".lastTimestamp"},
		{"logs", "--selector", brokerSelector, "--tail", "40", "--prefix", "--ignore-errors"},
		{"logs", "--selector", "app.kubernetes.io/component=example-worker", "--tail", "40", "--prefix", "--ignore-errors"},
	} {
		out, err := kubectlErr(args...)
		if err != nil {
			out = err.Error()
		}
		t.Logf("kubectl %s:\n%s", strings.Join(args, " "), out)
	}
}
