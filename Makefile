GO ?= go

# The workspace joins the framework module (the root: facade, adapters,
# thread, otel, mcp, mw, wefttest), the core module (the loop alone;
# its only dependency is the OTel API) and the example modules (ADR
# 0027), so build/test/vet/lint loop over the modules `go list -m`
# reports from go.work. The platform (Studio, obsdb, the runtime link,
# the weftbase CLI) is github.com/weftbase/weftbase, its own
# repository; make layering keeps this one from importing it.
MODULES = $(shell $(GO) list -m -f '{{.Dir}}')

# Every package of every workspace module, as patterns one go command
# takes (go.work resolves them from any directory): build, vet and the
# test targets run the modules in ONE invocation, so their packages
# build and run side by side instead of a module at a time — the
# slowest package bounds the wall clock, not the sum of the modules.
# The module-at-a-time loop stays where the tool is per module (lint,
# tidy, fmt).
PKGS = $(addsuffix /...,$(shell $(GO) list -m))

.PHONY: check build test vet fmt lint layering tidy generate live tools apidiff apidiff-core apidiff-all apidiff-selftest offline fuzz fuzz-thread soak-thread stress

# The local gate in one command: what to run before claiming done
# (AGENTS.md, "Working in this repo"). The cheap gates first.
check: vet lint layering test

build:
	$(GO) build $(PKGS)

# The per-package timeout `make test` passes: the one CI's test job
# passes (.github/workflows/ci.yml); thread/pool's soak-style suites
# are the slowest under -race. Override per invocation:
#   make test TEST_TIMEOUT=30m
TEST_TIMEOUT ?= 20m

test:
	$(GO) test -race -timeout $(TEST_TIMEOUT) $(PKGS)

vet:
	$(GO) vet $(PKGS)

fmt:
	for m in $(MODULES); do (cd $$m && $(GO) fmt ./...) || exit 1; done

# Needs: go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0
# (the version ci.yml's lint job pins; a release built with a Go older
# than the toolchain's export data fails every import — v2.13.2 under
# Go 1.27.2). PATH gains GOPATH/bin, last, so the target works from a
# bare shell (as apidiff does) and a golangci-lint already on PATH wins.
lint:
	for m in $(MODULES); do (cd $$m && PATH="$$PATH:$$(go env GOPATH)/bin" golangci-lint run) || exit 1; done

# The layering guard (scripts/check-layering.sh): no package of this
# workspace — its tests included — imports the platform
# (github.com/weftbase/weftbase). The platform imports the framework,
# never the reverse. CI runs it.
layering:
	GO=$(GO) scripts/check-layering.sh

tidy:
	for m in $(MODULES); do (cd $$m && $(GO) mod tidy) || exit 1; done

# The facades (the root package, mw, wefttest, wefttest/conformance)
# are generated from core (internal/cmd/genfacade, ADR 0027); run this
# after changing core's exported API. TestFacadesAreComplete fails
# when a committed facade is stale.
generate:
	$(GO) generate ./...

# Pinned tooling — the exact versions CI installs, so a local gate and
# the remote gate can never disagree (TODO §1.2a, the §1 pre-flight of
# docs/phase2b-store-plan.md). go install is idempotent; a warm module
# cache makes this a no-op.
tools:
	$(GO) install golang.org/x/exp/cmd/apidiff@v0.0.0-20260908205506-85c1c2202aba

# The apidiff gate (TODO §1.7): the framework module vs the last v*
# tag (reported: the pre-freeze layers live in it). The tools target
# installs the pinned apidiff so the gate runs on a machine without a
# pre-existing binary; PATH gains GOPATH/bin for it.
apidiff: tools
	PATH="$$(go env GOPATH)/bin:$$PATH" scripts/apidiff.sh

# The core module vs the last core/v* tag (enforced). Deliberate
# pre-1.0 widenings are listed in core/.apidiff-allow.
apidiff-core: tools
	PATH="$$(go env GOPATH)/bin:$$PATH" scripts/apidiff.sh "" core

# Every module go.work lists, each vs its own last tag and under its
# own policy (scripts/apidiff.sh's header: core is enforced, the root
# is reported, examples skip). A module added to go.work without a
# policy fails here. CI runs this.
#   APIDIFF_STRICT=1 make apidiff-all   # release check: every module
#                                       # builds from published tags
apidiff-all: tools
	PATH="$$(go env GOPATH)/bin:$$PATH" scripts/apidiff.sh "" all

# Exercises the gates' own failure modes — a broken tree must fail
# them, never read green.
apidiff-selftest: tools
	PATH="$$(go env GOPATH)/bin:$$PATH" scripts/apidiff-selftest.sh

# The offline gate (ADR 0013's kill-switch clause): every suite in the
# workspace stays green under WEFT_MODEL_REQUESTS=deny. Adapter suites
# drive their fixtures through injected SDK clients — test doubles by
# construction — so deny still means "no self-built client egress".
offline:
	WEFT_MODEL_REQUESTS=deny $(GO) test $(PKGS)

# Fuzz every Fuzz* target of the core module, one invocation each (Go
# fuzzes one target at a time), FUZZTIME apiece. A crasher is written
# to core/testdata/fuzz/<Target>/ — commit it as a regression seed.
FUZZTIME ?= 10s
fuzz:
	for f in $$(cd core && $(GO) test -list 'Fuzz.*' . | grep '^Fuzz'); do \
	  (cd core && $(GO) test -run '^$$' -fuzz "^$$f\$$" -fuzztime $(FUZZTIME) .) || exit 1; \
	done

# The thread package's decoders join the fuzz gate:
# entries, headers, signed decisions and grant predicates — one
# invocation each, crashers committed as seeds the same way.
fuzz-thread:
	for p in . ./jsonl; do \
	  for f in $$(cd thread && $(GO) test -list 'Fuzz.*' $$p | grep '^Fuzz'); do \
	    (cd thread && $(GO) test -run '^$$' -fuzz "^$$f\$$" -fuzztime $(FUZZTIME) $$p) || exit 1; \
	  done; \
	done

# The race soak CI runs nightly (.github/workflows/soak.yml): the
# thread package's suite ten times under the race detector, and
# thread/sqlite's three times.
soak-thread:
	cd thread && $(GO) test -race -count=10 -timeout 45m ./...
	cd thread/sqlite && $(GO) test -race -count=3 -timeout 30m ./...

# The loaded stress (the pre-push gate for anything concurrent): CI's
# runner is slow and shared, so timing windows that never open on a
# quiet workstation open there once in a few hundred runs. Two busy
# loops beside the race-detected suites, GOMAXPROCS=2 and -cpu 1,2
# approximate it. STRESS_PKGS and STRESS_COUNT are overridable:
#   make stress STRESS_PKGS=./thread/pool STRESS_COUNT=100
STRESS_PKGS ?= ./thread/...
STRESS_COUNT ?= 10
stress:
	@sh -c '(while :; do :; done) & h1=$$!; (while :; do :; done) & h2=$$!; \
	  trap "kill $$h1 $$h2 2>/dev/null" EXIT INT TERM; \
	  GOMAXPROCS=2 $(GO) test -race -count=$(STRESS_COUNT) -cpu 1,2 -timeout 60m $(STRESS_PKGS)'

# Live adapter tests behind the `live` build tag; never in CI (no keys).
live:
	$(GO) test -tags live $(PKGS)

