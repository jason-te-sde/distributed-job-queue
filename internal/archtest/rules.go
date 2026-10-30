package archtest

import (
	"fmt"
	"strings"
)

// File describes one non-test source file.
type File struct {
	// Name is the file's base name.
	Name string
	// HasGo, HasSelect and HasChan report any go statement, any select
	// statement and any channel type expression in the file.
	HasGo, HasSelect, HasChan bool
	// Calls lists the package-level functions the file calls, fully
	// qualified by package path, for example "time.Now" or
	// "math/rand/v2.IntN". Methods are not listed.
	Calls []string
}

// Package describes one package, from its non-test files only.
type Package struct {
	// Path is the import path.
	Path string
	// Imports lists the direct imports of the non-test files.
	Imports []string
	// Files describes the non-test files.
	Files []File
}

// Violation is one broken rule.
type Violation struct {
	// Package is the import path of the offending package.
	Package string
	// File is the offending file's base name, or empty for a rule that
	// concerns the package as a whole, such as an import.
	File string
	// Rule is the rule's name: core-imports, core-concurrency, core-calls,
	// layering or dependencies.
	Rule string
	// Detail says what was found.
	Detail string
}

// String formats the violation as "<pkg> <file>: <rule>: <detail>", or
// "<pkg>: <rule>: <detail>" when no file is involved.
func (v Violation) String() string {
	if v.File == "" {
		return fmt.Sprintf("%s: %s: %s", v.Package, v.Rule, v.Detail)
	}
	return fmt.Sprintf("%s %s: %s: %s", v.Package, v.File, v.Rule, v.Detail)
}

// Check applies every rule to pkgs, which belong to the module with the
// given path, and returns the violations rule by rule, package by package,
// in the order given.
func Check(module string, pkgs []Package) []Violation {
	var out []Violation
	out = append(out, coreImports(module, pkgs)...)
	out = append(out, coreConcurrency(module, pkgs)...)
	out = append(out, coreCalls(module, pkgs)...)
	out = append(out, layering(module, pkgs)...)
	out = append(out, dependencies(module, pkgs)...)
	return out
}

// under reports whether path is base or a package below base. It compares
// whole path elements, so "a/bc" is not under "a/b".
func under(path, base string) bool {
	return path == base || strings.HasPrefix(path, base+"/")
}

// isStdlib reports whether an import path names a standard library package,
// which is the case when its first element has no dot.
func isStdlib(path string) bool {
	first, _, _ := strings.Cut(path, "/")
	return !strings.Contains(first, ".")
}

func isCore(module, path string) bool { return under(path, module+"/internal/core") }

// bannedInCore reports whether a standard library package performs effects
// that internal/core must leave to its callers.
func bannedInCore(path string) bool {
	switch path {
	case "os", "sync", "sync/atomic", "syscall", "unsafe", "math/rand", "crypto/rand",
		"log", "log/slog", "runtime", "reflect", "io/ioutil":
		return true
	}
	return under(path, "net")
}

// bannedCoreCall reports whether a fully qualified function must not be
// called from internal/core: the clock, and the generator that math/rand/v2
// keeps in global state. New, NewPCG and NewChaCha8 build explicitly seeded
// generators and stay allowed.
func bannedCoreCall(call string) bool {
	switch call {
	case "time.Now", "time.Since", "time.Until", "time.After", "time.AfterFunc",
		"time.Sleep", "time.Tick", "time.NewTimer", "time.NewTicker":
		return true
	case "math/rand/v2.New", "math/rand/v2.NewPCG", "math/rand/v2.NewChaCha8":
		return false
	}
	return strings.HasPrefix(call, "math/rand/v2.")
}

func coreImports(module string, pkgs []Package) []Violation {
	var out []Violation
	for _, p := range pkgs {
		if !isCore(module, p.Path) {
			continue
		}
		for _, imp := range p.Imports {
			switch {
			case isStdlib(imp) && bannedInCore(imp):
				out = append(out, Violation{Package: p.Path, Rule: "core-imports",
					Detail: fmt.Sprintf("imports %s, which performs effects that core must leave to its callers", imp)})
			case isStdlib(imp), under(imp, module+"/internal/core"), imp == module+"/internal/flaw":
			default:
				out = append(out, Violation{Package: p.Path, Rule: "core-imports",
					Detail: fmt.Sprintf("imports %s, which is neither the standard library, another core package nor internal/flaw", imp)})
			}
		}
	}
	return out
}

func coreConcurrency(module string, pkgs []Package) []Violation {
	var out []Violation
	for _, p := range pkgs {
		if !isCore(module, p.Path) {
			continue
		}
		for _, f := range p.Files {
			for _, found := range []struct {
				has  bool
				what string
			}{{f.HasGo, "go statement"}, {f.HasSelect, "select statement"}, {f.HasChan, "channel type"}} {
				if found.has {
					out = append(out, Violation{Package: p.Path, File: f.Name, Rule: "core-concurrency",
						Detail: "contains a " + found.what + ", which core must not use"})
				}
			}
		}
	}
	return out
}

func coreCalls(module string, pkgs []Package) []Violation {
	var out []Violation
	for _, p := range pkgs {
		if !isCore(module, p.Path) {
			continue
		}
		for _, f := range p.Files {
			for _, call := range f.Calls {
				if bannedCoreCall(call) {
					out = append(out, Violation{Package: p.Path, File: f.Name, Rule: "core-calls",
						Detail: "calls " + call + ", which core must not call"})
				}
			}
		}
	}
	return out
}

// layerReasons returns why importer must not import imported, one entry per
// layering rule that forbids it.
func layerReasons(module, importer, imported string) []string {
	var (
		store    = module + "/internal/store"
		broker   = module + "/internal/broker"
		sim      = module + "/internal/sim"
		cmd      = module + "/cmd"
		testOnly = []string{module + "/internal/store/storetest", module + "/internal/pgtest", module + "/internal/brokertest"}
	)
	var reasons []string
	switch {
	case under(importer, store):
		if under(imported, broker) || under(imported, sim) || imported == module || under(imported, cmd) {
			reasons = append(reasons, "store packages must not import the broker, the simulator, the root package or cmd")
		}
	case under(importer, broker):
		if imported == module || under(imported, sim) {
			reasons = append(reasons, "the broker must not import the root package or the simulator")
		}
	case importer == module:
		if under(imported, broker) || under(imported, store) || under(imported, sim) || under(imported, "github.com/jackc/pgx") {
			reasons = append(reasons, "the root package must not import the broker, a store package, the simulator or pgx")
		}
	case under(importer, sim):
		if under(imported, store+"/pgstore") || under(imported, broker) {
			reasons = append(reasons, "the simulator must not import pgstore or the broker")
		}
	}
	for _, helper := range testOnly {
		if !under(imported, helper) {
			continue
		}
		exempt := false
		for _, other := range testOnly {
			exempt = exempt || under(importer, other)
		}
		if !exempt {
			reasons = append(reasons, "only test files may import "+strings.TrimPrefix(helper, module+"/"))
		}
	}
	return reasons
}

func layering(module string, pkgs []Package) []Violation {
	var out []Violation
	for _, p := range pkgs {
		for _, imp := range p.Imports {
			if reasons := layerReasons(module, p.Path, imp); len(reasons) > 0 {
				out = append(out, Violation{Package: p.Path, Rule: "layering",
					Detail: fmt.Sprintf("imports %s: %s", imp, strings.Join(reasons, "; "))})
			}
		}
	}
	return out
}

func dependencies(module string, pkgs []Package) []Violation {
	allowed := []string{
		module,
		"google.golang.org/grpc",
		"google.golang.org/protobuf",
		"github.com/jackc/pgx/v5",
		"github.com/prometheus/client_golang",
		"github.com/prometheus/client_model",
	}
	var out []Violation
	for _, p := range pkgs {
		if under(p.Path, module+"/bench") {
			continue
		}
		for _, imp := range p.Imports {
			if isStdlib(imp) {
				continue
			}
			ok := false
			for _, a := range allowed {
				ok = ok || under(imp, a)
			}
			if !ok {
				out = append(out, Violation{Package: p.Path, Rule: "dependencies",
					Detail: fmt.Sprintf("imports %s, which is not on the dependency list", imp)})
			}
		}
	}
	return out
}
