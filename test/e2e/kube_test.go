//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

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
)

// namespace is the Kubernetes namespace of the run.
func namespace() string {
	if ns := os.Getenv(namespaceEnv); ns != "" {
		return ns
	}
	return defaultNamespace
}

// execute runs name with args, gives up after timeout, and returns the
// trimmed standard output. A failure carries the command line and the standard
// error. It is the only place where the helpers start a process, and it never
// calls into testing, so goroutines other than the test's may use it.
func execute(timeout time.Duration, name string, args ...string) (string, error) {
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

// kubectlErr runs kubectl in the namespace of the run and returns its output.
func kubectlErr(args ...string) (string, error) {
	return execute(commandTimeout, "kubectl", append([]string{"--namespace", namespace()}, args...)...)
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

// helmInstall installs the chart as release in the namespace of the run, with
// one --set for each of sets, which have the form key=value. It returns once
// Helm has created the objects and the pre-install hook has completed; it does
// not wait for the pods. The tokens never go through here: only Secret names
// and settings do.
func helmInstall(t *testing.T, release string, sets ...string) {
	t.Helper()
	args := []string{"install", release, chartDir, "--namespace", namespace(), "--timeout", "5m"}
	for _, s := range sets {
		args = append(args, "--set", s)
	}
	if _, err := execute(6*time.Minute, "helm", args...); err != nil {
		t.Fatalf("%v", err)
	}
}

// waitRollout waits until the rollout of a Deployment is complete.
func waitRollout(t *testing.T, deploy string, timeout time.Duration) {
	t.Helper()
	// kubectl enforces timeout itself; the command's own bound only has to be
	// longer, so that the failure is kubectl's message.
	_, err := execute(timeout+30*time.Second, "kubectl", "--namespace", namespace(),
		"rollout", "status", "deployment/"+deploy, "--timeout="+timeout.String())
	if err != nil {
		t.Fatalf("rollout of %s: %v", deploy, err)
	}
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

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var starts atomic.Int64
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			starts.Add(1)
			cmd := exec.CommandContext(ctx, "kubectl", "--namespace", namespace(),
				"port-forward", "service/"+svc, fmt.Sprintf("%d:%d", local, port))
			_ = cmd.Run() // returns when the pod is gone, or when ctx ends
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
			return false, err.Error()
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

// replicas is the number of ready replicas of a Deployment, from
// .status.readyReplicas, which Kubernetes leaves out while it is zero.
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
	out, err := kubectlErr("get", "deployment", deploy, "-o", "jsonpath={.status.readyReplicas}")
	if err != nil {
		return 0, err
	}
	if out == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(out)
	if err != nil {
		return 0, fmt.Errorf("ready replicas of %s: %q is not a number", deploy, out)
	}
	return n, nil
}

// sampleReplicas polls the ready replicas of a Deployment every interval, in
// the background, so that a peak between two looks of the test is not missed.
// The returned function stops the sampling and returns the largest number it
// saw. Sampling also stops when ctx ends.
func sampleReplicas(ctx context.Context, deploy string, interval time.Duration) (stop func() int) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	var peak atomic.Int64
	go func() {
		defer close(done)
		for {
			// A failed look is skipped: the next one may succeed, and a sampler
			// that saw nothing makes the caller's check fail.
			if n, err := readyReplicas(deploy); err == nil && int64(n) > peak.Load() {
				peak.Store(int64(n))
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(interval):
			}
		}
	}()
	return func() int {
		cancel()
		<-done
		return int(peak.Load())
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
