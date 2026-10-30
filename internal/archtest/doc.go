// Package archtest holds the architecture rules of the repository and the
// test that enforces them.
//
// rules.go is pure: it checks a description of the packages (their imports,
// and per file the concurrency constructs and the functions called) against
// the rules, and uses only the standard library. archtest_test.go loads the
// real module with go/packages, builds that description, and fails when any
// rule is broken. go/packages is imported nowhere else, so the dependency
// stays test-only.
//
// The rules are:
//
//   - core-imports: internal/core/... may import only the standard library
//     (minus a list of packages that perform effects), other internal/core
//     packages and internal/flaw.
//   - core-concurrency: internal/core/... has no go statement, no select
//     statement and no channel type.
//   - core-calls: internal/core/... never reads the clock and never uses the
//     global math/rand/v2 functions. This rule sees direct call expressions
//     only: a function value such as `var now = time.Now` is not a call of
//     time.Now, so archtest misses it. forbidigo matches every use of the
//     identifier and does catch it (see .golangci.yml).
//   - sim-determinism: internal/sim/... has no go statement, never reads or
//     waits on the wall clock and never uses the global math/rand/v2
//     functions, so a simulated run depends on its seed alone. Like
//     core-calls it sees direct call expressions only, and forbidigo covers
//     the rest (see .golangci.yml).
//   - layering: which internal packages may import which, and which helper
//     packages only test files may import.
//   - dependencies: the non-test code of the module imports only the
//     standard library, the module itself and an explicit list of modules.
package archtest
