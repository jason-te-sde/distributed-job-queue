package e2e

import (
	"go/ast"
	"go/parser"
	gotoken "go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// The tests of this file need no cluster and no build tag. They check the rule
// that the layer in kube_test.go exists to keep: every kubectl and helm command
// of the run names the one context that the run is pinned to.

// recorder stands in for runProcess and keeps the command lines it was given.
type recorder struct {
	mu   sync.Mutex
	cmds [][]string // each is the program followed by its arguments
}

// run is a runProcess that records and answers with something every helper can
// parse.
func (r *recorder) run(_ time.Duration, name string, args ...string) (string, error) {
	r.mu.Lock()
	r.cmds = append(r.cmds, append([]string{name}, args...))
	r.mu.Unlock()
	line := strings.Join(args, " ")
	switch {
	case strings.Contains(line, "config current-context"):
		return "kind-unit", nil
	case strings.Contains(line, "jsonpath={.clusters[0].cluster.server}"):
		return "https://127.0.0.1:41234", nil
	case strings.Contains(line, "jsonpath={.spec.replicas}/{.status.readyReplicas}"):
		return "3/2", nil
	}
	return "1", nil
}

// commands returns a copy of the recorded command lines.
func (r *recorder) commands() [][]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.cmds)
}

// withFakeCluster replaces runProcess with a recorder and pins the context to
// kind-unit, until the test ends. The tests that use it must not run in
// parallel: both are package variables.
func withFakeCluster(t *testing.T) *recorder {
	t.Helper()
	r := &recorder{}
	oldRun, oldPin := runProcess, pinnedContext
	runProcess, pinnedContext = r.run, "kind-unit"
	t.Cleanup(func() { runProcess, pinnedContext = oldRun, oldPin })
	return r
}

// flagValue returns the value that follows flag in args, and whether flag is
// there.
func flagValue(args []string, flag string) (string, bool) {
	i := slices.Index(args, flag)
	if i < 0 || i+1 >= len(args) {
		return "", false
	}
	return args[i+1], true
}

func TestCheckKindCluster(t *testing.T) {
	tests := []struct {
		name    string
		context string
		server  string
		wantErr string // empty: accepted; otherwise a part of the message
	}{
		{"kind on 127.0.0.1", "kind-jq-e2e", "https://127.0.0.1:41234", ""},
		{"kind on localhost", "kind-jq-e2e", "https://localhost:6443", ""},
		{"kind on IPv6 loopback", "kind-jq-e2e", "https://[::1]:6443", ""},
		{"another loopback address", "kind-jq-e2e", "https://127.0.0.2:6443", ""},
		{"not a kind name", "minikube", "https://127.0.0.1:41234", "not a kind cluster"},
		{"name that only contains kind", "my-kind-cluster", "https://127.0.0.1:41234", "not a kind cluster"},
		{"a kind name on a remote address", "kind-prod", "https://10.0.0.5:6443", "not a loopback address"},
		{"a kind name on a host name", "kind-prod", "https://api.example.com", "not a loopback address"},
		{"a host name that starts like localhost", "kind-prod", "https://localhost.example.com:6443", "not a loopback address"},
		{"a host name that starts like an address", "kind-prod", "https://127.0.0.1.example.com:6443", "not a loopback address"},
		{"no address", "kind-prod", "", "cannot read the API server address"},
		{"an address that is not a URL", "kind-prod", "%zz", "cannot read the API server address"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := checkKindCluster(tc.context, tc.server)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("checkKindCluster(%q, %q) = %v, want it accepted", tc.context, tc.server, err)
			case tc.wantErr != "" && err == nil:
				t.Fatalf("checkKindCluster(%q, %q) accepted, want an error containing %q", tc.context, tc.server, tc.wantErr)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Fatalf("checkKindCluster(%q, %q) = %q, want it to contain %q", tc.context, tc.server, err, tc.wantErr)
			}
		})
	}
}

// TestPinKindClusterNamesTheContext checks the first commands of a run: the
// current context is read, and then the API server of that context is read by
// name, not of whatever the current context has become in between.
func TestPinKindClusterNamesTheContext(t *testing.T) {
	r := withFakeCluster(t)
	pinnedContext = ""
	pinKindCluster(t)
	if pinnedContext != "kind-unit" {
		t.Fatalf("pinned context = %q, want kind-unit", pinnedContext)
	}
	cmds := r.commands()
	if len(cmds) != 2 {
		t.Fatalf("pinKindCluster ran %d commands, want 2: %v", len(cmds), cmds)
	}
	if got, ok := flagValue(cmds[1], "--context"); !ok || got != "kind-unit" {
		t.Fatalf("the command that reads the API server does not name the context: %v", cmds[1])
	}
}

// TestHelpersNameThePinnedContext runs every helper that talks to the cluster
// and requires each kubectl command line to carry --context and each helm
// command line --kube-context, with the pinned name, in front of the
// subcommand.
func TestHelpersNameThePinnedContext(t *testing.T) {
	r := withFakeCluster(t)

	if _, err := kubectlErr("get", "pods"); err != nil {
		t.Fatal(err)
	}
	kubectl(t, "get", "pods")
	if _, err := kubectlRun(time.Minute, "get", "crd", "scaledobjects.keda.sh"); err != nil {
		t.Fatal(err)
	}
	if _, err := helmRun(time.Minute, "list"); err != nil {
		t.Fatal(err)
	}
	helmInstall(t, "rel", "a=b")
	helmUpgrade(t, "rel", "a=c")
	waitRollout(t, "deploy", time.Minute)
	requireKEDA(t)
	if n := replicas(t, "deploy"); n != 2 {
		t.Fatalf("replicas = %d, want 2", n)
	}
	if _, err := readyReplicas("deploy"); err != nil {
		t.Fatal(err)
	}
	if _, err := psqlErr("SELECT 1"); err != nil {
		t.Fatal(err)
	}
	psql(t, "SELECT 1")

	stop := sampleReplicas(t.Context(), "deploy", time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	if peaks := stop(); peaks != (replicaPeaks{Desired: 3, Ready: 2}) {
		t.Fatalf("sampleReplicas peaks = %+v, want desired 3 and ready 2", peaks)
	}

	// The forward is started by a supervisor that is not run here: its command
	// line is built by portForwardArgs, which is what the supervisor runs.
	r.mu.Lock()
	r.cmds = append(r.cmds, append([]string{kubectlBin}, portForwardArgs("svc", 12345, 7070)...))
	r.mu.Unlock()

	var kubectls, helms int
	for _, cmd := range r.commands() {
		var flag string
		switch cmd[0] {
		case kubectlBin:
			flag, kubectls = "--context", kubectls+1
		case helmBin:
			flag, helms = "--kube-context", helms+1
		default:
			t.Fatalf("a helper started %q, which is neither kubectl nor helm", cmd[0])
		}
		if got, ok := flagValue(cmd, flag); !ok || got != "kind-unit" {
			t.Errorf("%s command line does not carry %s kind-unit: %v", cmd[0], flag, cmd)
		}
		if got, ok := flagValue(cmd, "--namespace"); !ok || got != namespace() {
			t.Errorf("%s command line does not name the namespace %q: %v", cmd[0], namespace(), cmd)
		}
		// The flags are global ones, so they must come before the subcommand.
		if i := slices.Index(cmd, flag); i != 1 {
			t.Errorf("%s: %s is at position %d, want it right after the program name: %v", cmd[0], flag, i, cmd)
		}
	}
	if kubectls < 10 || helms != 3 {
		t.Fatalf("the test saw %d kubectl and %d helm commands; it should have seen at least 10 and exactly 3 (the helper calls and the explicit helm run)", kubectls, helms)
	}
}

// TestExecuteRefusesClusterPrograms checks that a call site cannot reach the
// cluster through the generic runner, which would leave the context out.
func TestExecuteRefusesClusterPrograms(t *testing.T) {
	r := withFakeCluster(t)
	for _, name := range []string{kubectlBin, helmBin} {
		if _, err := execute(time.Second, name, "version"); err == nil {
			t.Errorf("execute(%q) ran the command, want an error", name)
		}
	}
	if n := len(r.commands()); n != 0 {
		t.Errorf("execute started %d commands for kubectl and helm, want none", n)
	}
	if _, err := execute(time.Second, "go", "version"); err != nil {
		t.Errorf("execute refused a program that does not talk to the cluster: %v", err)
	}
}

// TestCommandsBeforeThePinPanic checks that a command line cannot be built
// before the context is pinned.
func TestCommandsBeforeThePinPanic(t *testing.T) {
	withFakeCluster(t)
	pinnedContext = ""
	for name, build := range map[string]func(){
		"kubectlArgs": func() { kubectlArgs("get", "pods") },
		"helmArgs":    func() { helmArgs("list") },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s built a command line without a pinned context", name)
				}
			}()
			build()
		}()
	}
}

// TestOnlyTheLayerStartsClusterCommands reads the sources of the package and
// checks that kubectl and helm are named, and processes are started, only in
// the functions of kube_test.go that pin the context. A new call site that goes
// around them fails here.
func TestOnlyTheLayerStartsClusterCommands(t *testing.T) {
	// The functions that may use the program names, and those that may start a
	// process.
	mayNamePrograms := []string{"kubectlRun", "helmRun", "portForward", "pinKindCluster", "execute"}
	mayStartProcesses := []string{"runProcess", "portForward"}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, e := range entries {
		// This file names the programs to test the rule, so it is not held to it.
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || e.Name() == "pin_test.go" {
			continue
		}
		fset := gotoken.NewFileSet()
		file, err := parser.ParseFile(fset, filepath.Join(".", e.Name()), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		checked++
		for _, decl := range file.Decls {
			if gen, ok := decl.(*ast.GenDecl); ok && gen.Tok == gotoken.CONST {
				continue // the constants that name the programs are declared here
			}
			name := declName(decl)
			ast.Inspect(decl, func(n ast.Node) bool {
				pos := func() string { return fset.Position(n.Pos()).String() }
				switch x := n.(type) {
				case *ast.BasicLit:
					if x.Kind == gotoken.STRING && (x.Value == `"kubectl"` || x.Value == `"helm"`) {
						t.Errorf("%s: the program name %s is written out in %q; use kubectlRun or helmRun", pos(), x.Value, name)
					}
				case *ast.Ident:
					if (x.Name == "kubectlBin" || x.Name == "helmBin") && !slices.Contains(mayNamePrograms, name) {
						t.Errorf("%s: %s is used in %q, which is not one of %v", pos(), x.Name, name, mayNamePrograms)
					}
				case *ast.CallExpr:
					if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
						if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "exec" && strings.HasPrefix(sel.Sel.Name, "Command") &&
							!slices.Contains(mayStartProcesses, name) {
							t.Errorf("%s: exec.%s is called in %q, which is not one of %v", pos(), sel.Sel.Name, name, mayStartProcesses)
						}
					}
				}
				return true
			})
		}
	}
	if checked < 3 {
		t.Fatalf("read %d source files, want at least kube_test.go, e2e_test.go and doc.go", checked)
	}
}

// declName names a top-level declaration: the function, or the first variable.
func declName(decl ast.Decl) string {
	switch d := decl.(type) {
	case *ast.FuncDecl:
		return d.Name.Name
	case *ast.GenDecl:
		for _, spec := range d.Specs {
			if v, ok := spec.(*ast.ValueSpec); ok && len(v.Names) > 0 {
				return v.Names[0].Name
			}
		}
	}
	return ""
}

// TestForwardLogKeepsTheLastLine checks what a failed port-forward reports.
func TestForwardLogKeepsTheLastLine(t *testing.T) {
	var l forwardLog
	if got := l.String(); !strings.Contains(got, "not said anything") {
		t.Errorf("an empty log says %q", got)
	}
	_, _ = l.Write([]byte("Forwarding from 127.0.0.1:1 -> 7070\n\nerror: unable to forward port: pod is gone\n\n"))
	l.setExit(os.ErrProcessDone)
	got := l.String()
	for _, want := range []string{"unable to forward port: pod is gone", os.ErrProcessDone.Error()} {
		if !strings.Contains(got, want) {
			t.Errorf("log %q does not contain %q", got, want)
		}
	}

	// The supervisor writes while the test goroutine reads; -race checks it.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 100 {
			_, _ = l.Write([]byte("line\n"))
			l.setExit(nil)
		}
	}()
	go func() {
		defer wg.Done()
		for range 100 {
			_ = l.String()
		}
	}()
	wg.Wait()
}
