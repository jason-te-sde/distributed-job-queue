package archtest

import (
	"go/ast"
	"go/types"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

const mod = "github.com/jason-te-sde/distributed-job-queue"

// mustExist lists the module-relative paths of packages that the repository
// test must find. It guards the test against silently looking at nothing: a
// package is added here in the task that creates it.
var mustExist = []string{
	"internal/sim",
	"internal/core/job",
	"internal/core/fair",
	"internal/store/memstore",
	"internal/core/lease",
	"internal/core/dispatch",
}

// wantRule fails unless exactly one violation has the given rule.
func wantRule(t *testing.T, vs []Violation, rule string) {
	t.Helper()
	n := 0
	for _, v := range vs {
		if v.Rule == rule {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("want exactly one %q violation, got %d in %v", rule, n, vs)
	}
}

func TestCheckFlagsCoreImportOfOS(t *testing.T) {
	v := Check(mod, []Package{{Path: mod + "/internal/core/job", Imports: []string{"os"}}})
	wantRule(t, v, "core-imports")
}

func TestCheckAllowsCoreImportOfFlawAndTime(t *testing.T) {
	v := Check(mod, []Package{{Path: mod + "/internal/core/fair", Imports: []string{"time", "sort", mod + "/internal/flaw"}}})
	if len(v) != 0 {
		t.Fatalf("unexpected: %v", v)
	}
}

func TestCheckFlagsEveryBannedCoreImport(t *testing.T) {
	banned := []string{
		"os", "net", "net/http", "net/netip", "sync", "sync/atomic", "syscall", "unsafe",
		"math/rand", "crypto/rand", "log", "log/slog", "runtime", "reflect", "io/ioutil",
	}
	for _, imp := range banned {
		t.Run(imp, func(t *testing.T) {
			v := Check(mod, []Package{{Path: mod + "/internal/core/job", Imports: []string{imp}}})
			wantRule(t, v, "core-imports")
		})
	}
}

func TestCheckAllowsOrdinaryCoreImports(t *testing.T) {
	imports := []string{
		"errors", "fmt", "math", "math/rand/v2", "slices", "strings", "time", "unicode/utf8",
		mod + "/internal/core/job", mod + "/internal/flaw",
	}
	v := Check(mod, []Package{{Path: mod + "/internal/core/fair", Imports: imports}})
	if len(v) != 0 {
		t.Fatalf("unexpected: %v", v)
	}
}

func TestCheckFlagsCoreImportOutsideStdlibCoreAndFlaw(t *testing.T) {
	for _, imp := range []string{"github.com/jackc/pgx/v5", mod + "/internal/store", mod} {
		t.Run(imp, func(t *testing.T) {
			v := Check(mod, []Package{{Path: mod + "/internal/core/job", Imports: []string{imp}}})
			wantRule(t, v, "core-imports")
		})
	}
}

func TestCheckCoreRulesApplyToWholePathElements(t *testing.T) {
	// internal/core itself is core; internal/corefoo is not.
	v := Check(mod, []Package{{Path: mod + "/internal/core", Imports: []string{"os"}}})
	wantRule(t, v, "core-imports")
	v = Check(mod, []Package{{Path: mod + "/internal/corefoo", Imports: []string{"os"}}})
	if len(v) != 0 {
		t.Fatalf("unexpected: %v", v)
	}
}

func TestCheckFlagsGoStatementSelectAndChanInCore(t *testing.T) {
	v := Check(mod, []Package{{Path: mod + "/internal/core/dispatch", Files: []File{
		{Name: "a.go", HasGo: true},
		{Name: "b.go", HasSelect: true},
		{Name: "c.go", HasChan: true},
	}}})
	if len(v) != 3 {
		t.Fatalf("want 3 violations, got %v", v)
	}
	for i, file := range []string{"a.go", "b.go", "c.go"} {
		if v[i].Rule != "core-concurrency" || v[i].File != file {
			t.Errorf("violation %d = %v, want core-concurrency in %s", i, v[i], file)
		}
	}
}

func TestCheckAllowsConcurrencyOutsideCore(t *testing.T) {
	v := Check(mod, []Package{{Path: mod + "/internal/broker", Files: []File{
		{Name: "a.go", HasGo: true, HasSelect: true, HasChan: true},
	}}})
	if len(v) != 0 {
		t.Fatalf("unexpected: %v", v)
	}
}

func TestCheckFlagsTimeNowAndGlobalRandInCore(t *testing.T) {
	v := Check(mod, []Package{{Path: mod + "/internal/core/fair", Files: []File{
		{Name: "a.go", Calls: []string{"time.Now", "math/rand/v2.IntN"}},
		{Name: "b.go", Calls: []string{"math/rand/v2.New", "math/rand/v2.NewPCG", "math/rand/v2.NewChaCha8", "time.Date", "time.Unix", "fmt.Sprintf"}},
	}}})
	if len(v) != 2 {
		t.Fatalf("want 2 violations, got %v", v)
	}
	for _, x := range v {
		if x.Rule != "core-calls" || x.File != "a.go" {
			t.Errorf("unexpected violation %v", x)
		}
	}
}

func TestCheckFlagsEveryBannedTimeFunctionInCore(t *testing.T) {
	for _, fn := range []string{"Now", "Since", "Until", "After", "AfterFunc", "Sleep", "Tick", "NewTimer", "NewTicker"} {
		t.Run(fn, func(t *testing.T) {
			v := Check(mod, []Package{{Path: mod + "/internal/core/lease", Files: []File{{Name: "a.go", Calls: []string{"time." + fn}}}}})
			wantRule(t, v, "core-calls")
		})
	}
}

func TestCheckAllowsClockAndRandCallsOutsideCore(t *testing.T) {
	v := Check(mod, []Package{{Path: mod + "/internal/broker", Files: []File{
		{Name: "a.go", Calls: []string{"time.Now", "time.Sleep", "math/rand/v2.IntN"}},
	}}})
	if len(v) != 0 {
		t.Fatalf("unexpected: %v", v)
	}
}

func TestCheckFlagsGoStatementInSim(t *testing.T) {
	v := Check(mod, []Package{{Path: mod + "/internal/sim", Files: []File{
		{Name: "a.go"},
		{Name: "b.go", HasGo: true},
	}}})
	wantRule(t, v, "sim-determinism")
	if v[0].Package != mod+"/internal/sim" || v[0].File != "b.go" {
		t.Errorf("violation %v does not name internal/sim and b.go", v[0])
	}
}

func TestCheckFlagsEveryBannedTimeFunctionInSim(t *testing.T) {
	for _, fn := range []string{"Now", "Since", "Until", "After", "AfterFunc", "Sleep", "Tick", "NewTimer", "NewTicker"} {
		t.Run(fn, func(t *testing.T) {
			v := Check(mod, []Package{{Path: mod + "/internal/sim", Files: []File{{Name: "a.go", Calls: []string{"time." + fn}}}}})
			wantRule(t, v, "sim-determinism")
			if len(v) != 1 {
				t.Fatalf("want only the sim-determinism violation, got %v", v)
			}
		})
	}
}

func TestCheckFlagsGlobalRandFunctionsInSim(t *testing.T) {
	for _, fn := range []string{"IntN", "N", "Shuffle", "Perm", "Float64"} {
		t.Run(fn, func(t *testing.T) {
			call := "math/rand/v2." + fn
			v := Check(mod, []Package{{Path: mod + "/internal/sim", Files: []File{{Name: "a.go", Calls: []string{call}}}}})
			wantRule(t, v, "sim-determinism")
			if len(v) != 1 {
				t.Fatalf("want only the sim-determinism violation, got %v", v)
			}
			if !strings.Contains(v[0].Detail, call) || !strings.Contains(v[0].Detail, "use the World's Rand") {
				t.Errorf("detail %q should name %s and point at the World's Rand", v[0].Detail, call)
			}
		})
	}
}

func TestCheckAllowsOrdinaryCodeInSim(t *testing.T) {
	// The simulator is not core: it may use closures, channels, select and
	// the time package's pure functions. The constructors of an explicitly
	// seeded math/rand/v2 generator are how it gets its randomness.
	v := Check(mod, []Package{{Path: mod + "/internal/sim", Files: []File{
		{Name: "a.go", HasSelect: true, HasChan: true, Calls: []string{
			"time.Date", "time.Unix", "fmt.Sprintf",
			"math/rand/v2.New", "math/rand/v2.NewPCG", "math/rand/v2.NewChaCha8",
		}},
	}}})
	if len(v) != 0 {
		t.Fatalf("unexpected: %v", v)
	}
}

func TestCheckSimRuleAppliesToSubpackagesAndWholePathElements(t *testing.T) {
	v := Check(mod, []Package{{Path: mod + "/internal/sim/checker", Files: []File{{Name: "a.go", HasGo: true}}}})
	wantRule(t, v, "sim-determinism")
	// internal/simulator is not internal/sim.
	v = Check(mod, []Package{{Path: mod + "/internal/simulator", Files: []File{{Name: "a.go", HasGo: true, Calls: []string{"time.Now"}}}}})
	if len(v) != 0 {
		t.Fatalf("unexpected: %v", v)
	}
}

func TestCheckFlagsLayering(t *testing.T) {
	cases := []struct{ name, importer, imported string }{
		{"store imports broker", mod + "/internal/store/pgstore", mod + "/internal/broker"},
		{"store imports sim", mod + "/internal/store/memstore", mod + "/internal/sim"},
		{"store imports the root package", mod + "/internal/store", mod},
		{"store imports cmd", mod + "/internal/store/pgstore", mod + "/cmd/jobqueue-server"},
		{"broker imports the root package", mod + "/internal/broker", mod},
		{"broker imports sim", mod + "/internal/broker", mod + "/internal/sim"},
		{"root imports broker", mod, mod + "/internal/broker"},
		{"root imports a store package", mod, mod + "/internal/store/pgstore"},
		{"root imports store", mod, mod + "/internal/store"},
		{"root imports sim", mod, mod + "/internal/sim"},
		{"root imports pgx", mod, "github.com/jackc/pgx/v5"},
		{"sim imports pgstore", mod + "/internal/sim", mod + "/internal/store/pgstore"},
		{"sim imports broker", mod + "/internal/sim", mod + "/internal/broker"},
		{"non-test code imports storetest", mod + "/internal/store/memstore", mod + "/internal/store/storetest"},
		{"non-test code imports pgtest", mod + "/internal/broker", mod + "/internal/pgtest"},
		{"non-test code imports brokertest", mod + "/internal/sim", mod + "/internal/brokertest"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v := Check(mod, []Package{{Path: c.importer, Imports: []string{c.imported}}})
			wantRule(t, v, "layering")
			if len(v) != 1 {
				t.Fatalf("want only the layering violation, got %v", v)
			}
			if v[0].Package != c.importer || !strings.Contains(v[0].Detail, c.imported) {
				t.Errorf("violation %v does not name both %s and %s", v[0], c.importer, c.imported)
			}
		})
	}
}

func TestCheckAllowsPermittedLayering(t *testing.T) {
	cases := []struct{ name, importer, imported string }{
		{"root imports core and generated code", mod, mod + "/internal/core/job"},
		{"root imports grpc", mod, "google.golang.org/grpc"},
		{"store imports core", mod + "/internal/store/pgstore", mod + "/internal/core/job"},
		{"pgstore imports pgx", mod + "/internal/store/pgstore", "github.com/jackc/pgx/v5"},
		{"storetest imports store", mod + "/internal/store/storetest", mod + "/internal/store"},
		{"broker imports store and pgstore", mod + "/internal/broker", mod + "/internal/store/pgstore"},
		{"sim imports memstore", mod + "/internal/sim", mod + "/internal/store/memstore"},
		{"pgtest imports storetest", mod + "/internal/pgtest", mod + "/internal/store/storetest"},
		{"brokertest imports broker and pgtest", mod + "/internal/brokertest", mod + "/internal/pgtest"},
		{"cmd imports everything", mod + "/cmd/jobqueue-server", mod + "/internal/broker"},
		{"cmd imports the root package", mod + "/cmd/jobqueuectl", mod},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v := Check(mod, []Package{{Path: c.importer, Imports: []string{c.imported}}})
			if len(v) != 0 {
				t.Fatalf("unexpected: %v", v)
			}
		})
	}
}

func TestCheckLayeringMatchesWholePathElements(t *testing.T) {
	// internal/brokertest is not internal/broker, and internal/storefront is not internal/store.
	v := Check(mod, []Package{
		{Path: mod + "/internal/brokertest", Imports: []string{mod}},
		{Path: mod + "/internal/storefront", Imports: []string{mod + "/internal/broker"}},
		{Path: mod + "/internal/simulator", Imports: []string{mod + "/internal/store/pgstore"}},
	})
	if len(v) != 0 {
		t.Fatalf("unexpected: %v", v)
	}
}

func TestCheckFlagsUnlistedDependency(t *testing.T) {
	v := Check(mod, []Package{{Path: mod, Imports: []string{"github.com/google/uuid"}}})
	wantRule(t, v, "dependencies")
}

func TestCheckAllowsListedDependencies(t *testing.T) {
	imports := []string{
		"fmt", "net/http", "google.golang.org/grpc", "google.golang.org/grpc/codes",
		"google.golang.org/protobuf/proto", "github.com/jackc/pgx/v5/pgxpool",
		"github.com/prometheus/client_golang/prometheus", "github.com/prometheus/client_model/go",
		mod + "/internal/core/job",
	}
	v := Check(mod, []Package{{Path: mod + "/internal/gen/jobqueue/v1", Imports: imports}})
	if len(v) != 0 {
		t.Fatalf("unexpected: %v", v)
	}
}

func TestCheckDependenciesMatchWholePathElements(t *testing.T) {
	for _, imp := range []string{"google.golang.org/grpcfoo", "github.com/jackc/pgx/v4", "github.com/prometheus/client_golang2"} {
		t.Run(imp, func(t *testing.T) {
			v := Check(mod, []Package{{Path: mod + "/internal/broker", Imports: []string{imp}}})
			wantRule(t, v, "dependencies")
		})
	}
}

func TestCheckIgnoresBenchModule(t *testing.T) {
	v := Check(mod, []Package{{Path: mod + "/bench/internal/river", Imports: []string{"github.com/riverqueue/river"}}})
	if len(v) != 0 {
		t.Fatalf("unexpected: %v", v)
	}
	// bench is a path element, not a prefix: bench2 is checked.
	v = Check(mod, []Package{{Path: mod + "/bench2", Imports: []string{"github.com/riverqueue/river"}}})
	wantRule(t, v, "dependencies")
}

func TestViolationString(t *testing.T) {
	v := Violation{Package: "p", File: "f.go", Rule: "r", Detail: "d"}
	if got, want := v.String(), "p f.go: r: d"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
	v.File = ""
	if got, want := v.String(), "p: r: d"; got != want {
		t.Errorf("String() without a file = %q, want %q", got, want)
	}
}

// TestLoadSeesConstructsInFixture pins the loader: a rule can only fire on
// what load reports, so a loader that misses a construct would let a
// violation through unseen. The fixture is a separate module that go
// ./... never builds.
func TestLoadSeesConstructsInFixture(t *testing.T) {
	pkgs := loadDir(t, filepath.Join(moduleRoot(t), "internal", "archtest", "testdata", "fixture"))
	var bad *Package
	for i := range pkgs {
		if pkgs[i].Path == mod+"/internal/core/bad" {
			bad = &pkgs[i]
		}
	}
	if bad == nil || len(bad.Files) != 1 {
		t.Fatalf("fixture package not loaded as one file: %+v", pkgs)
	}
	f := bad.Files[0]
	if !f.HasGo || !f.HasSelect || !f.HasChan {
		t.Errorf("fixture file flags = go:%v select:%v chan:%v, want all true", f.HasGo, f.HasSelect, f.HasChan)
	}
	for _, want := range []string{"time.Now", "math/rand/v2.IntN", "math/rand/v2.N", "math/rand/v2.New", "math/rand/v2.NewPCG"} {
		if !slices.Contains(f.Calls, want) {
			t.Errorf("Calls %v is missing %s", f.Calls, want)
		}
	}
	// A method (t.Sub) and a conversion (time.Duration(n)) are not
	// package-level functions.
	for _, unwanted := range []string{"time.Sub", "time.Duration"} {
		if slices.Contains(f.Calls, unwanted) {
			t.Errorf("Calls %v contains %s, which is not a package-level function", f.Calls, unwanted)
		}
	}
	if !slices.Contains(bad.Imports, "os") {
		t.Errorf("Imports %v is missing os", bad.Imports)
	}

	rules := map[string]int{}
	for _, v := range Check(mod, pkgs) {
		rules[v.Rule]++
	}
	want := map[string]int{"core-imports": 1, "core-concurrency": 3, "core-calls": 3}
	if !maps.Equal(rules, want) {
		t.Errorf("fixture violations by rule = %v, want %v", rules, want)
	}
}

func TestRepositoryHasNoViolations(t *testing.T) {
	pkgs := load(t)
	if len(pkgs) == 0 {
		t.Fatal("loaded no packages: the test is not looking at the repository")
	}
	for _, p := range pkgs {
		if p.Path != mod && !strings.HasPrefix(p.Path, mod+"/") {
			t.Errorf("loaded package %s is outside module %s: the mod constant does not match go.mod", p.Path, mod)
		}
	}
	for _, v := range Check(mod, pkgs) {
		t.Error(v)
	}
	for _, p := range mustExist {
		if !loaded(pkgs, p) {
			t.Errorf("expected package %s to exist and be checked", p)
		}
	}
}

// loaded reports whether the package at the module-relative path rel was loaded.
func loaded(pkgs []Package, rel string) bool {
	return slices.ContainsFunc(pkgs, func(p Package) bool { return p.Path == mod+"/"+rel })
}

// moduleRoot returns the directory holding the repository's go.mod.
func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the test's working directory")
		}
		dir = parent
	}
}

// load describes every non-test package of the repository.
func load(t *testing.T) []Package {
	t.Helper()
	return loadDir(t, moduleRoot(t))
}

// loadDir describes every non-test package of the module rooted at dir.
func loadDir(t *testing.T, dir string) []Package {
	t.Helper()
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedImports | packages.NeedFiles |
			packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir:   dir,
		Tests: false,
	}
	loadedPkgs, err := packages.Load(cfg, "./...")
	if err != nil {
		t.Fatalf("loading packages: %v", err)
	}
	var out []Package
	for _, lp := range loadedPkgs {
		for _, e := range lp.Errors {
			t.Errorf("package %s: %v", lp.PkgPath, e)
		}
		p := Package{Path: lp.PkgPath}
		for imp := range lp.Imports {
			p.Imports = append(p.Imports, imp)
		}
		slices.Sort(p.Imports)
		for _, syn := range lp.Syntax {
			p.Files = append(p.Files, describeFile(lp, syn))
		}
		out = append(out, p)
	}
	return out
}

// describeFile reports the concurrency constructs in a file and the
// package-level functions it calls.
func describeFile(lp *packages.Package, file *ast.File) File {
	f := File{Name: filepath.Base(lp.Fset.Position(file.Package).Filename)}
	calls := map[string]bool{}
	ast.Inspect(file, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.GoStmt:
			f.HasGo = true
		case *ast.SelectStmt:
			f.HasSelect = true
		case *ast.ChanType:
			f.HasChan = true
		case *ast.CallExpr:
			if name, ok := calleeName(lp.TypesInfo, n.Fun); ok {
				calls[name] = true
			}
		}
		return true
	})
	for name := range calls {
		f.Calls = append(f.Calls, name)
	}
	slices.Sort(f.Calls)
	return f
}

// calleeName returns "<package path>.<name>" when fun denotes a
// package-level function, and false for methods, conversions and builtins.
func calleeName(info *types.Info, fun ast.Expr) (string, bool) {
	fun = ast.Unparen(fun)
	switch x := fun.(type) {
	case *ast.IndexExpr: // explicit instantiation: f[T](...)
		fun = ast.Unparen(x.X)
	case *ast.IndexListExpr:
		fun = ast.Unparen(x.X)
	}
	var id *ast.Ident
	switch x := fun.(type) {
	case *ast.Ident:
		id = x
	case *ast.SelectorExpr:
		id = x.Sel
	default:
		return "", false
	}
	fn, ok := info.Uses[id].(*types.Func)
	if !ok || fn.Pkg() == nil {
		return "", false
	}
	if fn.Type().(*types.Signature).Recv() != nil {
		return "", false
	}
	return fn.Pkg().Path() + "." + fn.Name(), true
}
