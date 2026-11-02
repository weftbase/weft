# Testing weft

Every kind of test weft has, in one place: what each one proves, the
command, when it runs, how long it takes, its knobs, and what to do when
it fails. The sources are the `Makefile`, `.github/workflows/ci.yml`,
`.github/workflows/soak.yml`, `scripts/` and the test helpers. When this
page and one of them disagree, the source wins; fix this page in the same
change.

Durations were measured on a 16-thread Linux workstation with a warm
build and module cache. Other work was running on the machine at the
time, so read them as approximate. A cold cache adds a few minutes to
the first Go run, and CI's shared runners are slower.

## At a glance

| Layer | Command | Every push (CI) | Nightly | Release gate | Minutes (approx.) |
|---|---|---|---|---|---|
| Format | `make fmt` | no (lint covers gofmt) | — | yes | < 1 |
| Vet | `make vet` | yes (`test` job) | — | yes | < 1 |
| Lint | `make lint` | yes (`lint` job) | — | yes | < 1 (cached) |
| Unit + contract + examples, `-race` | `make test` | yes (`test` job, Go 1.26.x and stable) | — | yes | ~10 |
| Offline kill switch | `make offline` | yes (`offline` job) | — | yes | ~1 |
| Fuzz, core | `make fuzz` | yes (`fuzz` job) | — | yes | ~1.2 |
| Fuzz, thread | `make fuzz-thread` | yes (`fuzz` job) | — | yes | ~1.4 |
| API compatibility | `make apidiff-all` | yes (`apidiff` job, with `apidiff-selftest`) | — | yes | < 1 |
| Generated facades | `make generate` + `git status` | via `TestFacadesAreComplete` in `make test` | — | yes | < 1 |
| Loaded stress | `make stress` | no | — | yes (and before pushing concurrent code) | not timed |
| Thread race soak | `make soak-thread` | no | yes (`soak.yml`, 03:17 UTC) | no | not timed (est. 20–40) |
| Go floor | `GOTOOLCHAIN=go1.26.0 make build vet` | partly (the matrix runs the newest 1.26.x) | — | yes | < 1 |
| Live adapter conformance | `make live` | **never** (needs keys, costs money) | — | by hand, at release time | depends on the provider |
| The arena | `make check-dev` in `../weft-arena` | no (separate repo) | — | by hand | not timed |

CI (`ci.yml`) runs on every push to `main` and on every pull request.
The platform (Studio, the store, the CLI) and its gates — ClickHouse,
the web app and panel, `make mcp-check`, the phase gates — are
weftbase's (github.com/weftbase/weftbase, `docs/testing.md`). The soak (`soak.yml`) runs nightly and on manual
dispatch.

---

## 1. The Go suite: `make test`, `make vet`, `make lint`, `make fmt`

**What it proves.** All of weft's Go tests pass under the race
detector, in every module the workspace lists:

| Module (`go list -m`) | Directory | Holds |
|---|---|---|
| `github.com/weftbase/weft` | `.` | the facades, adapters, `mcp`, `mw`, `wefttest`, `thread`, `otel`, `version` |
| `github.com/weftbase/weft/core` | `core` | the loop, `core/mw`, `core/wefttest`, `core/wefttest/conformance` |
| `github.com/weftbase/weft/examples/otel` | `examples/otel` | the OTel example |

Each target loops over these modules (`MODULES = go list -m -f '{{.Dir}}'`)
and runs the Go tool in each one:

```sh
make test   # go test -race -timeout 20m ./...   (TEST_TIMEOUT, CI's value)
make vet    # go vet ./...
make lint   # golangci-lint run, using .golangci.yml at the root
make fmt    # go fmt ./...  (rewrites files; commit the result)
make build  # go build ./...
```

`make lint` needs `golangci-lint` v2.14.0, the version `ci.yml` pins:
`go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0`.
The target puts `$(go env GOPATH)/bin` at the end of `PATH`, so a
`golangci-lint` already on `PATH` takes precedence. An older release can
fail every import with "could not load export data" when the Go
toolchain is newer than the linter.

**When it runs.** On every push. CI's `test` job runs vet, then
`go test -race -timeout 20m ./...`, then build, per module, under Go
1.26.x and stable. The `lint` job runs golangci-lint per module.

**Measured.** `make vet` 7 s, `make lint` 5 s (cached), `make test`
a few minutes since the platform moved out (it was about 10 minutes,
601 s, with `studio` alone at about 520 s). `make test` passes
`-timeout 20m`, the value CI passes (`TEST_TIMEOUT ?= 20m` in the
Makefile; raise it per invocation with `make test TEST_TIMEOUT=30m`).
The slowest are `thread/sqlite` (~95 s), and `core`, `thread` and
`otel` (40–50 s each).

**Knobs.** These are standard `go test` flags. To narrow a run, go to
the module and pass them yourself:

```sh
go test -race -run '^TestToolEventOrderRule$' -count=1 -v ./core/
cd core && go test -race -run 'Contract|Order' ./...
go test -short ./thread/...   # a few long tests shrink under -short
```

**What lives in it.**

- **`core/contract_test.go`** pins the promises of the loop, about 180
  tests. Each one names the rule it holds: AGENTS.md's rules 1–13 (for
  example `TestToolEventOrderRule` for rule 3,
  `TestMaxTokensWithToolCallsFailsThemWithoutExecuting` for rule 11, and
  `TestModelStreamContractViolations` for rule 7), the record contract,
  and every model-visible text. The convention comes from AGENTS.md:
  **add a test here for any promise you add or change.** A change to a
  model-visible string fails one of these tests by design (rule 5 needs
  an ADR).
- **Godoc examples are tests.** `go test` runs every `Example…` function
  that has an `// Output:` comment and compares what it prints, for
  example the 50 examples in `core/example_test.go` and the ones in each
  package's `example_test.go`. A public feature gets an example, and the
  example is how its documented behaviour stays true.
- **Golden files.** `wefttest.Golden(t, path, got)` compares bytes. Run
  `go test ./… -update` to rewrite the files, then review the diff and
  commit it. The record-contract fixtures have their own flag,
  described in §8.
- **`TestFacadesAreComplete`** (root, `facade_test.go`) fails when a
  generated facade is stale (§9).
- **`thread`'s rules T1–T5** are pinned by its own tests, including the
  crash matrix and the one-writer tables.
- **The store's backend conformance and Studio's auth matrix** moved
  with the platform: weftbase's `docs/testing.md`.

**Reading a failure.** A failing test prints its name and file:line.
Re-run that one test with `-run '^Name$' -count=1 -v` in its package.
`-count=1` defeats the test cache. For a race report, both stacks are
in the output. For a flake that only appears under load, go to §6.

## 2. The offline gate: `make offline` and `WEFT_MODEL_REQUESTS=deny`

**What it proves.** Every suite in the workspace stays green while
model requests are denied, so no test quietly depends on reaching a
provider.

```sh
make offline   # per module: WEFT_MODEL_REQUESTS=deny go test ./...  (no -race)
```

**How the switch works.** `weft.ModelRequestsAllowed()` reads
`WEFT_MODEL_REQUESTS` on every call, so a suite can toggle it per test
with `t.Setenv`. When it is `deny`, every first-party adapter refuses to
call its provider and yields `weft.ErrModelRequestsDenied`. The switch
guards only clients the adapters build themselves from credentials. A
client injected through an adapter's `Client(c)` option is a test double
by construction and stays reachable. That is why the adapters'
fixture-driven suites pass under deny. `wefttest` models ignore the
switch.

**When.** On every push (the `offline` job, Go stable).
**Measured:** 63 s (no `-race`).

Export `WEFT_MODEL_REQUESTS=deny` in any shell where you run tests that
must not spend money. It is also the first rule of every agent brief in
this repo.

## 3. `wefttest`: scripted models, record/replay, conformance, `live`

These are tools for writing tests, not separate commands. They run inside
`make test`.

- **`wefttest.Script(wefttest.ToolCalls(…), wefttest.Say(…))`** is a
  deterministic model that plays a fixed dialogue, with no network and
  no key. Use it first: almost every test in the repo uses it.
  `wefttest.NewSteers()` is its counterpart for steering.
- **`wefttest.Record(t, dir, inner)` and `wefttest.Replay(t, dir)`**
  (ADR 0017) record real provider answers once and replay them in every
  later run. Record writes pretty-printed JSON under `dir/<TestName>/`,
  one file per request, keyed by a hash of the canonical request. Replay
  answers from those files and fails loudly on a request it has no file
  for. The switch that turns recording on belongs to your suite, by
  convention `WEFT_RECORD=1`; `wefttest` never reads it. Record spends
  money and never checks `WEFT_MODEL_REQUESTS` (the inner model does).
  Re-recording is the review: read the fixture diff. weftbase's Studio
  (`runs/{id}/export?format=wefttest`, the run page's "save as test")
  and `weftbase export <run> --wefttest ./pkg` write the same tree from a
  recorded run — `<agent>_replay_test.go` beside
  `testdata/TestReplay_<run>/` — and `--fixtures-only --test Name` the
  fixtures alone under `./testdata/Name/`.
- **`wefttest/conformance`** (ADR 0013) is the executable contract every
  provider adapter passes. Each adapter (`openai/`, `anthropic/`,
  `google/`) runs `conformance.Run` offline against recorded wire
  fixtures in its `testdata/*.sse`, served by
  `conformance.FixtureServer`. A file can hold several responses
  separated by `=== response N ===`, and a request past the last
  response fails with a 500 that names the file. Proving that the
  adapters parse the wire is the fixtures' job, not replay's.
- **The `live` build tag.** `openai/live_test.go`,
  `anthropic/live_test.go` and `google/live_test.go` run the same
  conformance suite against the real API:

  ```sh
  OPENAI_API_KEY=… OPENAI_MODEL=gpt-4o-mini go test -tags live ./openai/
  make live   # every module, -tags live
  ```

  Each test skips when its key is missing. The live suite never runs in
  CI, because CI has no keys and the calls cost money. It is a manual
  step at release time. Never run it from an agent session.

## 4. Fuzz: `make fuzz`, `make fuzz-thread`

**What it proves.** The decoders and the repair paths never panic, and
they keep their invariants on arbitrary input.

| Target | Package | Exercises |
|---|---|---|
| `FuzzToolInvokeArgs` | `core` | a tool's argument decoding |
| `FuzzRepair` | `core` | `weft.Repair` on arbitrary transcripts |
| `FuzzUnmarshalEvent` | `core` | `weft.UnmarshalEvent` (unknown types are errors, never panics) |
| `FuzzMessageUnmarshal` | `core` | message and part JSON |
| `FuzzPartialJSON` | `core` | the streaming partial-JSON closer (partials only grow) |
| `FuzzRawRedaction` | `core` | credential stripping in raw bodies |
| `FuzzDecodeEntry` | `thread` | session entries |
| `FuzzDecodeHeader` | `thread` | session headers |
| `FuzzDecideSigned` | `thread` | signed decisions |
| `FuzzGrantMatches` | `thread` | grant predicates |
| `FuzzLoad` | `thread/jsonl` | the JSONL store's loader |

```sh
make fuzz                  # the six core targets, FUZZTIME (default 10s) each
make fuzz-thread           # the five thread targets (. and ./jsonl)
FUZZTIME=1m make fuzz      # longer
cd core && go test -run '^$' -fuzz '^FuzzRepair$' -fuzztime 5m .   # one target
```

Go fuzzes one target per invocation, so the targets list themselves
with `go test -list 'Fuzz.*'` and run one at a time. The seed corpora
(`f.Add` plus `testdata/fuzz/<Target>/`) also run as plain tests under
`-race` in `make test`.

**When.** On every push (the `fuzz` job, stable Go, without `-race`).
**Measured:** `make fuzz` 70 s, `make fuzz-thread` 81 s, at the
default 10 s per target.

**A crasher.** The fuzzer writes the failing input to
`<package>/testdata/fuzz/<Target>/<hash>`, for example
`core/testdata/fuzz/FuzzRepair/…`. CI uploads `core/testdata/fuzz` and
`thread/testdata/fuzz` as the `fuzz-crashers` artifact. To reproduce it,
run `go test -run '^FuzzRepair/<hash>$' ./core/`. Fix the bug, then
**commit the file as a regression seed**. It runs in every later
`make test`. The committed corpora today are under
`core/testdata/fuzz/` (`FuzzRepair`, `FuzzPartialJSON`) and
`thread/testdata/fuzz/` (`FuzzDecodeEntry`, `FuzzDecodeHeader`). The
`FuzzRepair` seed was added this way.

## 5. ClickHouse: `WEFT_CLICKHOUSE_DSN`

Moved to weftbase (github.com/weftbase/weftbase, `docs/testing.md`) with the platform.

---


## 6. Loaded stress: `make stress`, and `TestNestedRandomChains`

**What it proves.** Concurrent code holds up when the scheduler is
hostile. CI's runners are slow and shared, so timing windows that never
open on a quiet workstation open there once in a few hundred runs.
`make stress` imitates that environment. It starts two CPU-burning
`while :; do :; done` loops, sets `GOMAXPROCS=2`, and runs the
race-detected suites at `-cpu 1,2` (each test once with each value),
`STRESS_COUNT` times, under `-timeout 60m`. A trap stops the burn loops
when it exits.

```sh
make stress                                            # STRESS_PKGS default, STRESS_COUNT=10
make stress STRESS_PKGS=./thread/pool STRESS_COUNT=100 # one package, harder
make stress STRESS_PKGS='./thread/sqlite ./thread/jsonl' STRESS_COUNT=50
```

`STRESS_PKGS` defaults to `./thread/...` (the platform's packages
are stressed in weftbase). The target has no `-run` variable. To stress a single test,
repeat its recipe by hand:

```sh
sh -c '(while :; do :; done) & a=$!; (while :; do :; done) & b=$!; trap "kill $a $b" EXIT; \
  GOMAXPROCS=2 go test -race -count=200 -cpu 1,2 -run "^TestCancelFromTheSiblingHandler$" ./thread/pool'
```

**When.** It is not in CI. Run it **before pushing anything
concurrent** (the briefs use `STRESS_PKGS=<your packages>
STRESS_COUNT=50`) and as a release gate. The default run executes
every package ten times inside the 60-minute `-timeout`, which applies
to each package's whole run. It was not timed end to end since the
platform moved out.

### `thread/pool`'s `TestNestedRandomChains`

This test builds random delegation chains: depth 2–4, fan-out 1–3, one
or two chains on a pool of one or two, `Submit` children beside them,
an occasional request expiry, and `Forward`s to running receipts. On
one iteration in three it also sends `Forward`s into a run's start
window (the running mark down, the child's turn not yet sent, on the
second pool after a restart too), and each must be refused with a
`*StateError` reading the state the run is leaving: `accepted` before a
first run, `parked` before a resume. It drives them to the end by deciding whatever the leads offer, a quarter
of them denied. Some iterations are interrupted halfway, by a `Close`
racing a `Decide`, a `Cancel` racing a `Decide` on the same lead, or a
restart followed by `Recover` on a second pool. Every iteration must
keep the pool's promises: every `Wait` returns, the bound holds, every
receipt ends terminal, no call is delegated twice, and no goroutine
leaks. **Measured:** the default 30 iterations under `-race` took about 5 s
(the log's last line read `modes: close 2, cancel 5, restart 1, decide
22; expiry 4, async 25, forward 25 (96 at a start); 30 of 30 iterations
passed` — `forward` counts the iterations that sent a `Forward`, the
figure in brackets the start-window `Forward`s). To
search for a flake, raise `POOL_CHAINS_ITERS` and add `-count`.

| Knob | Effect |
|---|---|
| `POOL_CHAINS_ITERS` | iteration count (default 30; 10 under `-short` unless set) |
| `POOL_CHAINS_SEED` | the random seed (default: the clock). The first log line prints `seed N, M iterations` |
| `POOL_CHAINS_MODE` | force one mode for every iteration: `close`, `cancel`, `restart` or `decide` (anything else fails the test) |
| `POOL_CHAINS_EXPIRY` | `1` adds an expiry to every iteration, `0` to none (default: one in six) |
| `POOL_CHAINS_NOFORWARD` | non-empty: no `Forward` |
| `POOL_CHAINS_NOASYNC` | non-empty: no `Submit` children |
| `POOL_CHAINS_NOCANCEL` | non-empty: cancel iterations become plain `decide` ones |

**Replaying a failure.** The failing run's log names the seed. The last
line counts the iterations run in each mode and how many passed, and a
failing iteration dumps its sessions' entries. Re-run with the same seed
and narrow it down:

```sh
cd thread/pool && POOL_CHAINS_SEED=1729 POOL_CHAINS_ITERS=30 \
  go test -race -run '^TestNestedRandomChains$' -count=1 -v .
POOL_CHAINS_SEED=1729 POOL_CHAINS_MODE=cancel POOL_CHAINS_NOFORWARD=1 …   # isolate the feature
```

The seed fixes the *shape* of the chains and the decisions. The timing
of the racing goroutines still varies from run to run, so a race may
need several runs with the same seed, or the stress recipe above, to
show up again.

## 7. The nightly race soak: `make soak-thread`

**What it proves.** The thread layer's crash matrix, its one-writer
tables and the pool's scheduling tests are where a single pass can miss
a flake. The soak runs them many times over.

```sh
make soak-thread
# = cd thread && go test -race -count=10 -timeout 45m ./...
#   cd thread/sqlite && go test -race -count=3 -timeout 30m ./...
```

**When.** Nightly. `.github/workflows/soak.yml` runs on cron `17 3 * * *`
(03:17 UTC) and on `workflow_dispatch`, with a 90-minute job limit and
the same two steps. It is too slow to run on every push. Locally, expect
it to take roughly 20–40 minutes, since one `-race` pass of
`thread/...` takes about 2 minutes here (`thread/sqlite` is ~95 s of
that). This was not timed end to end. To reproduce a soak failure, run the named test with
`-count=50` and then under `make stress` (§6).

## 8. Record-contract fixtures

`docs/record-contract.md` describes what the pipeline emits and what
a store reads. The page stays here (weft emits the records); the tests
that pin it live with the store, in weftbase (github.com/weftbase/weftbase):

- `TestRecordContractDocNames` (weftbase `obsdb/contract_test.go`,
  reading this page and weft's `core`, `otel` and `thread` sources
  through `go list -m`) fails when the page names a `weft.*` or
  `gen_ai.*` key that no emitter or reader uses, or when the store's
  model uses a key the page does not name.
- `TestContract` checks every case in weftbase's
  `obsdb/testdata/contract/<case>/`: a real OTLP/JSON export of weft's
  pipeline and `expect.json`, the run rows a conforming store derives
  from it. Regenerate them there with
  `go test ./obsdb -run 'TestContract$' -update-contract`, against the
  weft checkout the workspace resolves.

A change to the records here is a change to that page in the same
commit, and a fixture regeneration in weftbase.

## 9. API stability and generated code

```sh
make apidiff-all     # every go.work module vs its own last tag (CI)
make apidiff-core    # core vs the newest core/v* tag — enforced
make apidiff         # the root vs the newest v* tag — reported, never fatal
make apidiff-selftest  # the gate's own failure modes: a broken tree must fail it
APIDIFF_STRICT=1 make apidiff-all   # release check: every module builds from published tags
```

`make tools` installs the pinned `apidiff`. Policies live in
`scripts/apidiff.sh`. `core` is **enforced**: an incompatible change
fails unless its exact apidiff line is listed in `core/.apidiff-allow`.
Only a deliberate, source-compatible widening may be listed there, and
only before 1.0. The file is reset at each tag, and renames always fail.
The root module is **reported**: its pre-freeze layers may break, but a
break makes the next tag a minor bump with a breaking CHANGELOG entry.
`examples/*` are skipped. A module added to `go.work` without a policy
fails the gate. CI runs the self-test first, then `scripts/apidiff.sh ""
all`. **Measured:** `make apidiff-all` took 12 s, with apidiff already
installed. Its last line reads `apidiff: every workspace module passed
its gate`.

**Facades.** The root package, `mw`, `wefttest` and
`wefttest/conformance` are generated from `core`
(`internal/cmd/genfacade`, ADR 0027). After changing core's exported
API, run `make generate` (`go generate ./...`) and commit the
result. `TestFacadesAreComplete` fails in `make test` while a facade is
stale. Before a release, `make generate && git status --short` must
print nothing. **Measured:** under a second.

**The Go floor.** `go.work` and `go.mod` say `go 1.26.0`. CI's matrix
runs the newest 1.26.x and stable, but not 1.26.0 itself, so the
release checks the floor by hand:

```sh
GOTOOLCHAIN=go1.26.0 make build vet   # downloads go1.26.0 once into the module cache
GOTOOLCHAIN=go1.26.0 make test        # the stricter form
```

**Measured:** `GOTOOLCHAIN=go1.26.0 make build vet` took 12 s, with
the go1.26.0 toolchain already in the module cache.
Also run `make tidy` and then `git status --short`; no `go.mod` or
`go.sum` should change.

## 10. Studio and the devtools panel

Moved to weftbase (github.com/weftbase/weftbase, `docs/testing.md`) with the platform.

---


## 11. `make mcp-check`

Moved to weftbase (github.com/weftbase/weftbase, `docs/testing.md`) with the platform.

---


## 12. The phase gate scripts (`scripts/ux-gate/`)

Moved to weftbase (github.com/weftbase/weftbase, `docs/testing.md`) with the platform.

---


## 13. The arena (`../weft-arena`)

weft's own tests prove each promise in isolation. The arena is a
separate repository and harness that checks the framework the way a
user meets it. It has about 150 catalogued scenarios over every module,
mapped to around 170 tracked features. `weft-arena coverage` fails on a
feature that no scenario covers. The scenarios run offline (scripted
`wefttest` models, deterministic) or live through provider profiles.
They can be driven from a CLI, a browser UI, or MCP, and they can seed a
running Studio with realistic runs. It pins the published weft tags
(`make check`, `GOWORK=off`). `make check-dev` runs the same gate
against a sibling `../weft` checkout through `go.work`. The phase gates
use its offline seed as their data.

```sh
cd ../weft-arena
make check-dev                                   # vet, go test, every offline scenario, coverage vs ../weft
go run ./cmd/weft-arena run --suite pool18 --offline
go run ./cmd/weft-arena run --offline --studio --no-save --jobs 8   # seed a running Studio
```

For profiles, the live suite and the UI, see the arena's own README.
Live runs call real providers; keep them out of weft's gates.

---

## Before you push

- [ ] `make fmt vet lint test` all exit 0.
- [ ] `export WEFT_MODEL_REQUESTS=deny` in that shell. No test you added
      may need the network: `make offline`, or at least the packages you
      touched under deny.
- [ ] You changed core's exported API: `make generate`, commit it, and
      `make apidiff-core` passes (or the line is justified in
      `core/.apidiff-allow`).
- [ ] You touched concurrent code: `make stress STRESS_PKGS=<your
      packages> STRESS_COUNT=50`.
- [ ] You touched a `thread` decoder, or added a message part or event
      type: `make fuzz` / `make fuzz-thread` with a short `FUZZTIME`.
- [ ] The doc that describes the behaviour is updated in the same
      change (AGENTS.md's docs list).

## Before a release

The release recipe bumps the version, tags `core/vX.Y.Z` and then
`vX.Y.Z`, publishes the panel asset and pins the examples. Run these
first, from a clean tree, each exiting 0:

- [ ] `make fmt vet lint test`
- [ ] `make offline`
- [ ] `make apidiff-all` (core enforced; read the root's report: an
      incompatible line means a minor bump and a breaking CHANGELOG
      entry). After the tags exist, `APIDIFF_STRICT=1 make apidiff-all`.
- [ ] `make generate && git status --short`: no diff.
- [ ] `make tidy && git status --short`: no diff.
- [ ] `GOTOOLCHAIN=go1.26.0 make build vet`: the floor.
- [ ] `make stress`, at the default packages and count.
- [ ] `make fuzz` and `make fuzz-thread`.
- [ ] By hand, with keys, outside agent sessions: `make live`.

## When you add a feature

| You add… | Add this test |
|---|---|
| A promise of the loop (an ordering, an error shape, a budget, a model-visible text) | a test in `core/contract_test.go` naming the rule; an ADR if model-visible behaviour changes (rule 5) |
| A public function, option or type | a godoc `Example…` with `// Output:` |
| A message part or event type | a JSON round-trip test, a seed for `FuzzMessageUnmarshal` / `FuzzUnmarshalEvent` (`f.Add` or `core/testdata/fuzz/<Target>/`), and `make generate` |
| A record attribute or record kind | the row in `docs/record-contract.md` (`TestRecordContractDocNames` fails otherwise), and, in weftbase, a case or a regenerated fixture under `obsdb/testdata/contract/` (`-update-contract`) on both backends |
| An adapter behaviour | a recorded `.sse` fixture and a conformance case (ADR 0013) |
| A `thread` entry kind or decoder | a seed for the thread fuzz targets, and the format reference in ADR 0011 |
| Concurrent code | a race test, and a `make stress` run on its package |
| A provider-facing path | a `WEFT_MODEL_REQUESTS=deny` case: the adapter yields `ErrModelRequestsDenied` |
