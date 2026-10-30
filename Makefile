# Targets used locally and by CI. `make preflight` runs everything CI can run
# on a developer machine.

GO ?= go

# Pinned tool versions. Keep them in step with .github/workflows/ci.yml.
GOLANGCI_LINT_VERSION := v2.15.0
GOLANGCI_LINT_SERIES := 2.15
GOVULNCHECK_VERSION := v1.9.0
BUF_VERSION := 1.75.0
PROTOC_GEN_GO_VERSION := v1.36.12
PROTOC_GEN_GO_GRPC_VERSION := v1.6.2

# Code generators are installed here, so a generation run never depends on
# whatever versions happen to be on the developer's PATH.
BIN := $(CURDIR)/bin

.PHONY: test test-pg lint check-buf gen hygiene test-scripts preflight

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

lint: check-buf
	@golangci-lint version 2>/dev/null | grep -qF '$(GOLANGCI_LINT_SERIES)' || { \
		echo "lint: golangci-lint $(GOLANGCI_LINT_SERIES).x is required; install it with" >&2; \
		echo "  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)" >&2; \
		exit 1; \
	}
	golangci-lint run ./...
	buf lint
	@if ! git rev-parse --verify -q origin/main >/dev/null; then \
		echo "lint: origin/main is not available, so buf breaking cannot run; fetch it with" >&2; \
		echo "  git fetch origin main" >&2; \
		exit 1; \
	elif git cat-file -e origin/main:proto 2>/dev/null; then \
		buf breaking --against '.git#branch=origin/main'; \
	else \
		echo "lint: skipping buf breaking; origin/main has no proto/ directory yet"; \
	fi
	$(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

# Fails unless the buf on PATH is the pinned version, so generated code and
# lint results do not depend on whichever buf a machine happens to have.
check-buf:
	@buf --version 2>/dev/null | grep -qxF '$(BUF_VERSION)' || { \
		echo "buf $(BUF_VERSION) is required; install it with" >&2; \
		echo "  go install github.com/bufbuild/buf/cmd/buf@v$(BUF_VERSION)" >&2; \
		exit 1; \
	}

# Regenerates internal/gen from proto/. The generated code is committed, and
# CI and preflight fail when `make gen` changes anything. The generated files
# are deleted first so output for a removed or renamed proto does not linger;
# doc.go and the tests next to them are not touched.
gen: check-buf
	GOBIN="$(BIN)" $(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	GOBIN="$(BIN)" $(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
	find internal/gen -name '*.pb.go' -delete
	PATH="$(BIN):$$PATH" buf generate

# Tracked files are English only and nothing names an AI tool as an author.
hygiene:
	scripts/check-hygiene.sh .

# Tests for the scripts themselves.
test-scripts:
	bash scripts/tests/hygiene_test.sh

preflight:
	scripts/preflight.sh
