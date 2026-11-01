package jobqueue_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// The tests in this file hold the documentation to the repository: a table
// that names a test must name one that exists, a link must lead somewhere, and
// a design record must have the sections a reader looks for. They read files
// relative to the package directory, which is the repository root.

// minGuarantees is the fewest rows the guarantees table may have. A table
// that the parser fails to find would otherwise pass for want of rows.
const minGuarantees = 12

// minLinks is the fewest relative links the documents must hold in all, for
// the same reason.
const minLinks = 20

var (
	// testFuncRE finds the test functions of a file.
	testFuncRE = regexp.MustCompile(`(?m)^func (Test[A-Za-z0-9_]*)\(`)
	// testRefRE is a citation in the guarantees table: `TestName` in `path`.
	testRefRE = regexp.MustCompile("`(Test[A-Za-z0-9_]*)` in `([^`]+)`")
	// testTokenRE is any code span that starts with Test, cited or not.
	testTokenRE = regexp.MustCompile("`(Test[A-Za-z0-9_]*)[^`]*`")
	// inlineLinkRE and refLinkRE are the two forms of a markdown link.
	inlineLinkRE = regexp.MustCompile(`\]\(([^)\s]+)(?:\s+"[^"]*")?\)`)
	refLinkRE    = regexp.MustCompile(`(?m)^\s{0,3}\[[^\]]+\]:\s*(\S+)`)
)

// docsRead returns the text of a file of the repository.
func docsRead(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// docsWalk calls fn for every regular file under root, skipping the version
// control directory and directories that are not part of the repository.
func docsWalk(t *testing.T, root string, fn func(path string)) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "bin", "dist":
				return filepath.SkipDir
			}
			return nil
		}
		fn(path)
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
}

// docsSection returns the lines of the section of a markdown document that
// starts at the heading "## <title>" and ends before the next "## " heading.
func docsSection(t *testing.T, doc, title string) []string {
	t.Helper()
	var lines []string
	in := false
	for _, line := range strings.Split(doc, "\n") {
		switch {
		case line == "## "+title:
			in = true
		case in && strings.HasPrefix(line, "## "):
			return lines
		case in:
			lines = append(lines, line)
		}
	}
	if !in {
		t.Fatalf("the document has no section %q", "## "+title)
	}
	return lines
}

// TestREADMEGuaranteesNameRealTests requires every test that the guarantees
// table of the README cites to exist, in the file that the row cites. A row
// reads: guarantee | mechanism | `TestName` in `path`, and may cite several.
func TestREADMEGuaranteesNameRealTests(t *testing.T) {
	// Where each test function is defined, over the whole repository.
	defined := map[string][]string{}
	docsWalk(t, ".", func(path string) {
		if !strings.HasSuffix(path, "_test.go") {
			return
		}
		for _, m := range testFuncRE.FindAllStringSubmatch(docsRead(t, path), -1) {
			defined[m[1]] = append(defined[m[1]], filepath.ToSlash(path))
		}
	})

	rows := 0
	for _, line := range docsSection(t, docsRead(t, "README.md"), "Guarantees") {
		if !strings.HasPrefix(line, "|") || strings.HasPrefix(line, "| ---") || strings.HasPrefix(line, "| Guarantee") {
			continue
		}
		rows++
		refs := testRefRE.FindAllStringSubmatch(line, -1)
		if len(refs) == 0 {
			t.Errorf("a guarantee cites no test: %.80s...", line)
		}
		// Every code span that looks like a test must be cited in full, with
		// its path: a bare name could be anywhere.
		if tokens := testTokenRE.FindAllString(line, -1); len(tokens) != len(refs) {
			t.Errorf("a row has %d test names but %d citations of the form `TestName` in `path`: %.80s...", len(tokens), len(refs), line)
		}
		for _, ref := range refs {
			name, path := ref[1], ref[2]
			paths, ok := defined[name]
			switch {
			case !ok:
				t.Errorf("the README cites %s, which no *_test.go file defines", name)
			case !slices.Contains(paths, path):
				t.Errorf("the README cites %s in %s, but it is defined in %v", name, path, paths)
			}
		}
	}
	if rows < minGuarantees {
		t.Errorf("the guarantees table has %d rows, want at least %d", rows, minGuarantees)
	}
}

// TestDocsLinksResolve requires every relative link of the README, of the
// other documents in the repository root and of everything under docs/ to
// lead to a file or directory that exists.
func TestDocsLinksResolve(t *testing.T) {
	var docs []string
	roots, err := filepath.Glob("*.md")
	if err != nil {
		t.Fatal(err)
	}
	docs = append(docs, roots...)
	docsWalk(t, "docs", func(path string) {
		if strings.HasSuffix(path, ".md") {
			docs = append(docs, path)
		}
	})
	if !slices.Contains(docs, "README.md") {
		t.Fatal("there is no README.md")
	}

	checked := 0
	for _, doc := range docs {
		for _, target := range docsLinks(docsRead(t, doc)) {
			if strings.Contains(target, "://") || strings.HasPrefix(target, "mailto:") || strings.HasPrefix(target, "#") {
				continue
			}
			target, _, _ = strings.Cut(target, "#")
			target, _, _ = strings.Cut(target, "?")
			if target == "" {
				continue
			}
			resolved := filepath.Join(filepath.Dir(doc), target)
			if strings.HasPrefix(target, "/") {
				resolved = strings.TrimPrefix(target, "/")
			}
			checked++
			if _, err := os.Stat(resolved); err != nil {
				t.Errorf("%s links to %s, which does not exist", doc, target)
			}
		}
	}
	if checked < minLinks {
		t.Errorf("the documents hold %d relative links, want at least %d", checked, minLinks)
	}
}

// docsLinks returns the targets of the markdown links of a document, outside
// fenced code blocks.
func docsLinks(doc string) []string {
	var kept []string
	fenced := false
	for _, line := range strings.Split(doc, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			fenced = !fenced
			continue
		}
		if !fenced {
			kept = append(kept, line)
		}
	}
	text := strings.Join(kept, "\n")
	var targets []string
	for _, m := range inlineLinkRE.FindAllStringSubmatch(text, -1) {
		targets = append(targets, m[1])
	}
	for _, m := range refLinkRE.FindAllStringSubmatch(text, -1) {
		targets = append(targets, m[1])
	}
	return targets
}

// adrFiles are the design records of the repository.
var adrFiles = []string{
	"0001-postgres-as-source-of-truth.md",
	"0002-leases-and-fencing.md",
	"0003-pure-cores-and-simulator.md",
	"0004-time-authority.md",
	"0005-drr-fair-scheduling.md",
	"0006-stateless-brokers-and-load-balancing.md",
	"0007-scope.md",
}

// TestADRsHaveRequiredSections requires the seven design records to exist and
// each file under docs/design to have the four sections of a record.
func TestADRsHaveRequiredSections(t *testing.T) {
	found, err := filepath.Glob("docs/design/*.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range adrFiles {
		if !slices.Contains(found, filepath.Join("docs/design", name)) {
			t.Errorf("docs/design/%s is missing", name)
		}
	}
	for _, path := range found {
		lines := strings.Split(docsRead(t, path), "\n")
		for _, heading := range []string{"## Context", "## Decision", "## Consequences", "## Alternatives rejected"} {
			if !slices.Contains(lines, heading) {
				t.Errorf("%s has no %q section", path, heading)
			}
		}
	}
}

// TestREADMESnippetsMatchExamples requires the Go snippets of the README to
// be the bodies of the two examples of example_test.go, line for line, so that
// the code a reader copies is the code that compiles.
func TestREADMESnippetsMatchExamples(t *testing.T) {
	readme := docsRead(t, "README.md")
	examples := docsRead(t, "example_test.go")
	for _, name := range []string{"ExampleProducer_Enqueue", "ExampleNewWorker"} {
		_, rest, ok := strings.Cut(examples, "func "+name+"() {\n")
		if !ok {
			t.Fatalf("example_test.go has no %s", name)
		}
		body, _, ok := strings.Cut(rest, "\n}\n")
		if !ok {
			t.Fatalf("example_test.go: %s has no closing brace at the start of a line", name)
		}
		var dedented []string
		for _, line := range strings.Split(body, "\n") {
			dedented = append(dedented, strings.TrimPrefix(line, "\t"))
		}
		snippet := strings.Join(dedented, "\n")
		if !strings.Contains(readme, snippet) {
			t.Errorf("the README does not hold the body of %s, dedented by one tab:\n%s", name, snippet)
		}
	}
}
