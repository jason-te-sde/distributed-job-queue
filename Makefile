# Targets used locally and by CI. `make preflight` runs everything CI can run
# on a developer machine.

GO ?= go

# Pinned tool versions. Keep them in step with .github/workflows/ci.yml.
GOLANGCI_LINT_VERSION := v2.15.0
GOLANGCI_LINT_SERIES := 2.15
GOVULNCHECK_VERSION := v1.9.0

.PHONY: test test-pg lint hygiene test-scripts preflight

# Unit and simulator tests.
test:
	$(GO) test -race ./...

# Tests that need a live PostgreSQL, selected by the pg build tag.
test-pg:
	@if [ -z "$$JOBQUEUE_TEST_DSN" ]; then \
		echo "test-pg: JOBQUEUE_TEST_DSN is empty; point it at a PostgreSQL database, for example" >&2; \
		echo "  JOBQUEUE_TEST_DSN='postgres://localhost:5432/postgres?sslmode=disable' make test-pg" >&2; \
		exit 1; \
	fi
	$(GO) test -race -tags pg ./...

lint:
	@golangci-lint version 2>/dev/null | grep -qF '$(GOLANGCI_LINT_SERIES)' || { \
		echo "lint: golangci-lint $(GOLANGCI_LINT_SERIES).x is required; install it with" >&2; \
		echo "  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)" >&2; \
		exit 1; \
	}
	golangci-lint run ./...
	$(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

# Tracked files are English only and nothing names an AI tool as an author.
hygiene:
	scripts/check-hygiene.sh .

# Tests for the scripts themselves.
test-scripts:
	bash scripts/tests/hygiene_test.sh

preflight:
	scripts/preflight.sh
