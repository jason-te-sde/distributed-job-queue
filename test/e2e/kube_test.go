package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This file is the layer between the tests and the cluster: every kubectl and
// helm command of the run goes through it. It has no build tag, so that the
// tests of pin_test.go, which need no cluster, run with every go test.

// The environment of a run, and where the files it applies are, relative to
// this package, which is where go test runs the binary.
const (
	namespaceEnv     = "JOBQUEUE_E2E_NAMESPACE"
	defaultNamespace = "jobqueue-e2e"

	chartDir         = "../../deploy/helm/jobqueue"
	postgresManifest = "../../deploy/kind/postgres.yaml"

	// commandTimeout bounds one kubectl, helm or psql call. Commands that
	// wait for the cluster pass their own, longer bound.
	commandTimeout = 2 * time.Minute

	// The two programs that talk to the cluster. Nothing outside this file
	// names them: pin_test.go reads the sources to make sure of that.
	kubectlBin = "kubectl"
	helmBin    = "helm"
)

// namespace is the Kubernetes namespace of the run.
func namespace() string {
	if ns := os.Getenv(namespaceEnv); ns != "" {
		return ns
	}
	return defaultNamespace
}

// pinnedContext is the kubectl context that every command of the run names. It
// is set once, by pinKindCluster, before anything else starts.
var pinnedContext string

// kubeContext returns the pinned context. Asking before the pin is a mistake
// of the test code and it panics: a command that went out without a context
// would use whichever one is current.
func kubeContext() string {
	if pinnedContext == "" {
		panic("e2e: a kubectl or helm command was built before pinKindCluster pinned the context")
	}
	return pinnedContext
}

// kubectlArgs returns the arguments of a kubectl command that names the pinned
// context and the namespace of the run. Every kubectl command line is built
// here.
func kubectlArgs(args ...string) []string {
	return append([]string{"--context", kubeContext(), "--namespace", namespace()}, args...)
}

// helmArgs returns the arguments of a helm command that names the pinned
// context and the namespace of the run. Every helm command line is built here.
func helmArgs(args ...string) []string {
	return append([]string{"--kube-context", kubeContext(), "--namespace", namespace()}, args...)
}

// runProcess runs name with args, gives up after timeout, and returns the
// trimmed standard output. A failure carries the command line and the standard
// error. It is a variable so that the unit tests can record the command lines
// instead of running them. It never calls into testing, so goroutines other
// than the test's may use it.
var runProcess = func(timeout time.Duration, name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%s %s: %w\n%s", name, strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(stdout.String()), nil
}

// execute runs a program that does not talk to the cluster. It refuses kubectl
// and helm, which have kubectlRun and helmRun: those pin the context.
func execute(timeout time.Duration, name string, args ...string) (string, error) {
	if name == kubectlBin || name == helmBin {
		return "", fmt.Errorf("execute %s: use kubectlRun or helmRun, which pin the kind context", name)
	}
	return runProcess(timeout, name, args...)
}

// kubectlRun runs kubectl against the pinned cluster, in the namespace of the
// run, with its own time limit.
func kubectlRun(timeout time.Duration, args ...string) (string, error) {
	return runProcess(timeout, kubectlBin, kubectlArgs(args...)...)
}

// helmRun runs helm against the pinned cluster, in the namespace of the run,
// with its own time limit.
func helmRun(timeout time.Duration, args ...string) (string, error) {
	return runProcess(timeout, helmBin, helmArgs(args...)...)
}

// kubectlErr runs kubectl in the namespace of the run and returns its output.
func kubectlErr(args ...string) (string, error) {
	return kubectlRun(commandTimeout, args...)
}

// kubectl runs kubectl in the namespace of the run, and fails the test with
// the standard error if it fails.
func kubectl(t *testing.T, args ...string) string {
	t.Helper()
	out, err := kubectlErr(args...)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return out
}

// checkKindCluster says whether a cluster is one that the run may touch: the
// name of its context starts with kind-, and its API server address is on the
// loopback interface. kind publishes the API server of a cluster on
// 127.0.0.1, so a name that says kind with an address that is not local is a
// context that someone else made.
func checkKindCluster(contextName, server string) error {
	if !strings.HasPrefix(contextName, "kind-") {
		return fmt.Errorf("the current kubectl context is %q, not a kind cluster (kind-...)", contextName)
	}
	u, err := url.Parse(server)
	if err != nil || u.Hostname() == "" {
		return fmt.Errorf("context %q: cannot read the API server address %q", contextName, server)
	}
	host := u.Hostname()
	if ip := net.ParseIP(host); host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return fmt.Errorf("context %q: the API server is at %s, which is not a loopback address", contextName, host)
	}
	return nil
}

// pinKindCluster reads the current kubectl context, fails the test unless it
// belongs to a kind cluster on this machine, and pins the run to it: from here
// on every kubectl command names the context, and every helm command names it
// too. The run installs a release and deletes pods, and that must never reach a
// cluster that somebody cares about, not even after the current context
// changes underneath the run.
func pinKindCluster(t *testing.T) {
	t.Helper()
	name, err := runProcess(commandTimeout, kubectlBin, "config", "current-context")
	if err != nil {
		t.Fatalf("no current kubectl context: %v", err)
	}
	server, err := runProcess(commandTimeout, kubectlBin, "config", "view", "--minify", "--context", name,
		"--output", "jsonpath={.clusters[0].cluster.server}")
	if err != nil {
		t.Fatalf("read the API server of context %q: %v", name, err)
	}
	if err := checkKindCluster(name, server); err != nil {
		t.Fatalf("%v: the run installs and deletes things, so it refuses to run elsewhere", err)
	}
	pinnedContext = name
	t.Logf("pinned to kubectl context %s (API server %s)", name, server)
}

// requireKEDA fails unless the KEDA custom resources exist: the chart's
// ScaledObject cannot be installed without them.
func requireKEDA(t *testing.T) {
	t.Helper()
	if _, err := kubectlRun(commandTimeout, "get", "crd", "scaledobjects.keda.sh"); err != nil {
		t.Fatalf("KEDA is not installed in the cluster (install the kedacore/keda chart into the namespace keda first): %v", err)
	}
}

// helmRelease runs helm install or upgrade of the chart as release in the
// namespace of the run, with one --set for each of sets, which have the form
// key=value. It returns once Helm has applied the objects and the hooks have
// completed; it does not wait for the pods. The tokens never go through here:
// only Secret names and settings do.
func helmRelease(t *testing.T, verb, release string, flags, sets []string) {
	t.Helper()
	args := append([]string{verb, release, chartDir, "--timeout", "5m"}, flags...)
	for _, s := range sets {
		args = append(args, "--set", s)
	}
	if _, err := helmRun(6*time.Minute, args...); err != nil {
		t.Fatalf("%v", err)
	}
}

// helmInstall installs the chart as release in the namespace of the run, with
// one --set for each of sets.
func helmInstall(t *testing.T, release string, sets ...string) {
	t.Helper()
	helmRelease(t, "install", release, nil, sets)
}

// helmUpgrade changes the settings of an installed release: it upgrades it to
// the same chart, keeps every value it was installed with, and sets those of
// sets.
func helmUpgrade(t *testing.T, release string, sets ...string) {
	t.Helper()
	helmRelease(t, "upgrade", release, []string{"--reuse-values"}, sets)
}

// waitRollout waits until the rollout of a Deployment is complete.
func waitRollout(t *testing.T, deploy string, timeout time.Duration) {
	t.Helper()
	// kubectl enforces timeout itself; the command's own bound only has to be
	// longer, so that the failure is kubectl's message.
	_, err := kubectlRun(timeout+30*time.Second, "rollout", "status", "deployment/"+deploy, "--timeout="+timeout.String())
	if err != nil {
		t.Fatalf("rollout of %s: %v", deploy, err)
	}
}

// forwardLog keeps what the supervised kubectl of a port-forward last said, so
// that a forward that never comes up can say why. The supervisor goroutine
// writes it and the test goroutine reads it, hence the mutex.
type forwardLog struct {
	mu     sync.Mutex
	stderr string // the last non-empty line kubectl wrote to standard error
	exit   string // how the last run of kubectl ended
}

// Write implements io.Writer for the standard error of kubectl.
func (l *forwardLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for line := range strings.SplitSeq(string(p), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			l.stderr = line
		}
	}
	return len(p), nil
}

// setExit records how kubectl ended.
func (l *forwardLog) setExit(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err == nil {
		l.exit = "exited without an error"
		return
	}
	l.exit = err.Error()
}

// String describes what kubectl last said and how it ended.
func (l *forwardLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stderr == "" && l.exit == "" {
		return "kubectl has not said anything yet"
	}
	return fmt.Sprintf("kubectl last said %q and %s", l.stderr, l.exit)
}

// portForwardArgs returns the arguments of the kubectl that forwards the local
// port to port of the Service svc.
func portForwardArgs(svc string, local, port int) []string {
	return kubectlArgs("port-forward", "service/"+svc, fmt.Sprintf("%d:%d", local, port))
}

// portForward forwards a free local port to port of a Service in the namespace
// of the run, and returns the local address. It is a kubectl process in the
// background, which is stopped when the test ends. kubectl binds the forward
// to one pod and exits when that pod goes away, so a supervisor starts it
// again, on the same local port, for as long as the test runs: a client that
// keeps one address keeps working across a broker restart, apart from the
// moment the forward is down.
func portForward(t *testing.T, svc string, port int) (localAddr string) {
	t.Helper()
	local := freePort(t)
	localAddr = net.JoinHostPort("127.0.0.1", strconv.Itoa(local))
	args := portForwardArgs(svc, local, port)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var starts atomic.Int64
	var said forwardLog
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			starts.Add(1)
			cmd := exec.CommandContext(ctx, kubectlBin, args...)
			cmd.Stderr = &said
			said.setExit(cmd.Run()) // returns when the pod is gone, or when ctx ends
			select {
			case <-ctx.Done():
			case <-time.After(200 * time.Millisecond):
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		t.Logf("port-forward to %s: kubectl started %d time(s)", svc, starts.Load())
	})

	eventually(t, 30*time.Second, 100*time.Millisecond, "port-forward to "+svc, func() (bool, string) {
		conn, err := net.DialTimeout("tcp", localAddr, time.Second)
		if err != nil {
			return false, fmt.Sprintf("%v; %s", err, &said)
		}
		_ = conn.Close()
		return true, ""
	})
	return localAddr
}

// freePort returns a TCP port on the loopback address that nothing listens on
// right now.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find a free port: %v", err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

// replicaCounts returns the number of replicas that a Deployment is asked to
// have (.spec.replicas, which the autoscaler sets) and the number that are
// ready (.status.readyReplicas, which Kubernetes leaves out while it is zero).
func replicaCounts(deploy string) (desired, ready int, err error) {
	out, err := kubectlErr("get", "deployment", deploy, "-o", "jsonpath={.spec.replicas}/{.status.readyReplicas}")
	if err != nil {
		return 0, 0, err
	}
	d, r, ok := strings.Cut(out, "/")
	if !ok {
		return 0, 0, fmt.Errorf("replicas of %s: %q is not desired/ready", deploy, out)
	}
	if desired, err = atoiOrZero(d); err != nil {
		return 0, 0, fmt.Errorf("desired replicas of %s: %w", deploy, err)
	}
	if ready, err = atoiOrZero(r); err != nil {
		return 0, 0, fmt.Errorf("ready replicas of %s: %w", deploy, err)
	}
	return desired, ready, nil
}

// atoiOrZero parses a count, where the empty string means zero.
func atoiOrZero(s string) (int, error) {
	if s == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, errors.New(strconv.Quote(s) + " is not a number")
	}
	return n, nil
}

// replicas is the number of ready replicas of a Deployment.
func replicas(t *testing.T, deploy string) int {
	t.Helper()
	n, err := readyReplicas(deploy)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return n
}

// readyReplicas is replicas for callers that cannot fail the test.
func readyReplicas(deploy string) (int, error) {
	_, ready, err := replicaCounts(deploy)
	return ready, err
}

// replicaPeaks are the largest counts that sampleReplicas saw.
type replicaPeaks struct {
	// Desired is the most replicas the Deployment was asked for, and Ready the
	// most that were ready at a look.
	Desired, Ready int
}

// sampleReplicas polls the replicas of a Deployment every interval, in the
// background, so that a peak between two looks of the test is not missed. The
// returned function stops the sampling and returns the largest counts it saw.
// Sampling also stops when ctx ends.
func sampleReplicas(ctx context.Context, deploy string, interval time.Duration) (stop func() replicaPeaks) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	var desiredPeak, readyPeak atomic.Int64
	go func() {
		defer close(done)
		for {
			// A failed look is skipped: the next one may succeed, and a sampler
			// that saw nothing makes the caller's check fail.
			if desired, ready, err := replicaCounts(deploy); err == nil {
				desiredPeak.Store(max(desiredPeak.Load(), int64(desired)))
				readyPeak.Store(max(readyPeak.Load(), int64(ready)))
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(interval):
			}
		}
	}()
	return func() replicaPeaks {
		cancel()
		<-done
		return replicaPeaks{Desired: int(desiredPeak.Load()), Ready: int(readyPeak.Load())}
	}
}

// psqlErr runs sql in the PostgreSQL pod and returns the result without
// headers or alignment: one line per row, columns separated by "|".
func psqlErr(sql string) (string, error) {
	return kubectlErr("exec", "deploy/postgres", "--", "psql",
		"--username=postgres", "--dbname=postgres", "--no-psqlrc",
		"--tuples-only", "--no-align", "--set=ON_ERROR_STOP=1", "--command", sql)
}

// psql runs sql in the PostgreSQL pod, like psqlErr, and fails the test if it
// fails.
func psql(t *testing.T, sql string) string {
	t.Helper()
	out, err := psqlErr(sql)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return out
}

// eventually calls check every interval until it reports done, and fails the
// test when timeout has passed without that. check returns what it saw, which
// the failure message quotes.
func eventually(t *testing.T, timeout, interval time.Duration, what string, check func() (done bool, observed string)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		done, observed := check()
		if done {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within %s; last observation: %s", what, timeout, observed)
		}
		time.Sleep(interval)
	}
}
