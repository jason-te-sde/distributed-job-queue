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
//     global math/rand/v2 functions.
//   - layering: which internal packages may import which, and which helper
//     packages only test files may import.
//   - dependencies: the non-test code of the module imports only the
//     standard library, the module itself and an explicit list of modules.
package archtest
