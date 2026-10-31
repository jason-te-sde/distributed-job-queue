//go:build e2e

// Package e2e runs the job queue on a Kubernetes cluster: the image, the Helm
// chart, the example worker and the KEDA scaler, together, under the faults
// that a cluster brings (a pod that is deleted, a rolling restart) and under
// the autoscaler's own scaling.
//
// The tests need a kind cluster with KEDA installed in the namespace keda, a
// kubectl whose current context is that cluster, helm, and the image already
// loaded into the cluster. They do not create any of these. CI sets them up
// in the k8s-e2e job of .github/workflows/ci.yml; a local run is
//
//	kind create cluster --name jq-e2e --config deploy/kind/kind-config.yaml
//	helm repo add kedacore https://kedacore.github.io/charts
//	helm install keda kedacore/keda --version 2.21.0 --namespace keda --create-namespace --wait
//	docker build -t jq:e2e . && kind load docker-image jq:e2e --name jq-e2e
//	JOBQUEUE_E2E_IMAGE=jq:e2e go test -tags e2e -count=1 -timeout 40m ./test/e2e/
//
// A missing piece fails the run: nothing here skips. The run leaves its
// namespace behind, so that the objects can be inspected after a failure, and
// refuses to start where the namespace already exists.
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
// draw. The cost of a job grows with the square of the edge: on an Apple M2 one
// job takes about 2 ms at 256, 14 ms at 1024 and 52 ms at 2048.
const (
	// sizeLight is for the subtest that is about the brokers: the worker must
	// keep far ahead of 40 jobs a second, so that a late job is the brokers'
	// doing and not the worker's.
	sizeLight = 256
	// sizeDefault is the edge that `thumbnail produce` uses.
	sizeDefault = 1024
	// sizeHeavy is the largest edge the worker accepts. The scaling subtest
	// uses it so that a backlog of 500 jobs lasts longer than the autoscaler
	// needs to act on it.
	sizeHeavy = 2048
)

// workerConcurrency is how many jobs a worker pod runs at once. The chart's
// default is 4, and with it one pod would drain 500 jobs of the heavy size in
// about 7 seconds on an idle M2 (500 jobs of 52 ms, four at a time). That is
// less than the 15 seconds between two looks of the HPA, so the backlog could be
// gone before the autoscaler sees it, and whether the scaling subtest passed
// would depend on the speed of the machine. With 1 the pod needs 29 seconds, as
// measured with the replicas capped at one, and the autoscaler must scale out
// for the backlog to clear sooner.
const workerConcurrency = 1

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
	requireKindCluster(t)
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

// requireKindCluster fails unless the current kubectl context is a kind
// cluster. The run installs a release and creates a namespace, and that must
// never happen on a cluster that somebody cares about.
func requireKindCluster(t *testing.T) {
	t.Helper()
	ctx, err := execute(commandTimeout, "kubectl", "config", "current-context")
	if err != nil {
		t.Fatalf("no current kubectl context: %v", err)
	}
	if !strings.HasPrefix(ctx, "kind-") {
		t.Fatalf("the current kubectl context is %q, not a kind cluster (kind-...): the run installs and deletes things, so it refuses to run elsewhere", ctx)
	}
}

// requireKEDA fails unless the KEDA custom resources exist: the chart's
// ScaledObject cannot be installed without them.
func requireKEDA(t *testing.T) {
	t.Helper()
	if _, err := execute(commandTimeout, "kubectl", "get", "crd", "scaledobjects.keda.sh"); err != nil {
		t.Fatalf("KEDA is not installed in the cluster (install the kedacore/keda chart into the namespace keda first): %v", err)
	}
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
		"example.concurrency="+strconv.Itoa(workerConcurrency),
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

// specReplicas is the number of replicas that a Deployment is asked to have.
func specReplicas(t *testing.T, deploy string) int {
	t.Helper()
	return int(mustInt(t, kubectl(t, "get", "deployment", deploy, "-o", "jsonpath={.spec.replicas}")))
}

// processesJobs enqueues 300 jobs while the example worker is at zero
// replicas. That every job succeeds proves that KEDA raised the worker from
// zero on the strength of the broker's backlog.
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
	if n := specReplicas(t, workerDeploy); n != 0 {
		t.Fatalf("the example worker should start at 0 replicas under KEDA, but is asked for %d", n)
	}
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
// Deployment. Brokers drain on SIGTERM, so no lease may be left to expire: no
// job dies, none is late by the lease TTL of 30s, and none is lost.
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
		// maxLatency bounds finished_at - created_at of every job. The lease TTL
		// is 30s: a job whose broker vanished without releasing its lease would
		// wait that long for the reaper, and would break the bound.
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
		t.Fatalf("a job took %.2fs from enqueue to finish; a drained broker leaves no lease to expire, so every job must take less than %s", late, maxLatency)
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
	)
	settle := cooldownPeriod(t) + downSlack

	toZero := func(what string) {
		t.Helper()
		eventually(t, settle, 2*time.Second, what, func() (bool, string) {
			n, err := readyReplicas(workerDeploy)
			if err != nil {
				t.Fatalf("%v", err)
			}
			return n == 0, fmt.Sprintf("%d ready replicas", n)
		})
	}
	toZero("the worker has no backlog and scales to zero")

	base := maxJobID(t)
	p := f.connect(t)
	enqueueAll(t, p, jobs, sizeHeavy)
	enqueued := time.Now()

	// The sampler looks every 250ms, much more often than the subtest does
	// anything else, so that a scale-out is seen however short it is.
	stopSampling := sampleReplicas(t.Context(), workerDeploy, 250*time.Millisecond)
	eventually(t, upWithin, 500*time.Millisecond, "the backlog scales the worker up", func() (bool, string) {
		n, err := readyReplicas(workerDeploy)
		if err != nil {
			t.Fatalf("%v", err)
		}
		return n >= 1, fmt.Sprintf("%d ready replicas", n)
	})
	t.Logf("the worker had a ready replica %s after the jobs were enqueued", time.Since(enqueued).Round(time.Second))

	waitSucceeded(t, base, jobs, 180*time.Second)
	peak := stopSampling()
	t.Logf("all %d jobs succeeded %s after they were enqueued; at most %d replicas were ready", jobs, time.Since(enqueued).Round(time.Second), peak)
	if peak <= 1 {
		t.Fatalf("the worker never had more than %d ready replica(s) while %d jobs were processed; a backlog of %d with a target of 10 per replica must scale it out", peak, jobs, jobs)
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
