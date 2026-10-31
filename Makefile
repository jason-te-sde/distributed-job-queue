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

.PHONY: test sim test-pg flaws lint check-buf gen hygiene helm-check test-scripts preflight

# Unit and simulator tests.
test:
	$(GO) test -race ./...

# The simulator's seeded fault-injection sweep and its regression seeds, on
# their own and with a longer timeout than the default. It runs 500 seeds from
# seed 1; JOBQUEUE_SIM_SEEDS and JOBQUEUE_SIM_SEED_START change that, and the
# nightly run sets 20000. A failing seed leaves internal/sim/testdata/failures/
# seed-<n>.txt with the command that replays it. The timeout is 30m unless
# SIM_TIMEOUT says otherwise, which the nightly run does.
SIM_TIMEOUT ?= 30m
sim:
	$(GO) test -race -count=1 -timeout $(SIM_TIMEOUT) -run 'TestSim' ./internal/sim/

# Tests that need a live PostgreSQL, selected by the pg build tag. The run is
# recorded as JSON in pg.json and then counted: a test that silently stops
# running leaves the build green, so the count must not fall below
# scripts/pg-min-tests.txt. -count=1 keeps go test from replaying a cached
# result, which would count passes without touching PostgreSQL. The count
# check runs even when go test failed, so that it can print what the failing
# tests wrote, which the JSON hides.
test-pg:
	@if [ -z "$$JOBQUEUE_TEST_DSN" ]; then \
		echo "test-pg: JOBQUEUE_TEST_DSN is empty; point it at a PostgreSQL database, for example" >&2; \
		echo "  JOBQUEUE_TEST_DSN='postgres://localhost:5432/postgres?sslmode=disable' make test-pg" >&2; \
		exit 1; \
	fi
	@status=0; \
	$(GO) test -race -count=1 -tags pg -json ./... >pg.json || status=$$?; \
	scripts/check-pg-count.sh pg.json || status=$$?; \
	exit $$status

# Asserts that every planted flaw turns its named tests red and that the build
# that carries them is inert otherwise (see scripts/check-flaws.sh and
# scripts/flaws.tsv). The pg rows need a database; without JOBQUEUE_TEST_DSN
# they are skipped and the output says how many. CI always has one and never
# skips them.
flaws:
	@if [ -z "$$JOBQUEUE_TEST_DSN" ]; then \
		scripts/check-flaws.sh --no-pg; \
	else \
		scripts/check-flaws.sh; \
	fi

# The second golangci-lint pass compiles the flaw sites in, which no other
# build does: the jobqueue_flaws files are checked only by it.
lint: check-buf
	@golangci-lint version 2>/dev/null | grep -qF '$(GOLANGCI_LINT_SERIES)' || { \
		echo "lint: golangci-lint $(GOLANGCI_LINT_SERIES).x is required; install it with" >&2; \
		echo "  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)" >&2; \
		exit 1; \
	}
	golangci-lint run ./...
	golangci-lint run --build-tags jobqueue_flaws,pg ./...
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

# Runs a command that needs Helm: with_helm <command> <name of the check>.
# Without Helm a developer machine prints a SKIPPED line and moves on. CI always
# installs Helm, so there (the CI variable is set) a missing Helm is an error.
define with_helm
	@if command -v helm >/dev/null 2>&1; then \
		$(1); \
	elif [ -n "$$CI" ]; then \
		echo "$(2): helm is required in CI but is not installed" >&2; \
		exit 1; \
	else \
		echo "SKIPPED $(2): helm not installed"; \
	fi
endef

# Lints and renders the Helm chart and asserts on the rendering.
helm-check:
	$(call with_helm,scripts/check-helm.sh,helm check)

# Tests for the scripts themselves.
test-scripts:
	bash scripts/tests/hygiene_test.sh
	bash scripts/tests/pg_count_test.sh
	bash scripts/tests/flaws_test.sh
	$(call with_helm,bash scripts/tests/helm_test.sh,helm test)

preflight:
	scripts/preflight.sh
