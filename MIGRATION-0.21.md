# Migrating to weft 0.21: the module path and the weftbase split

weft 0.21 does two breaking things at once, so importers move once:

1. The framework's module path changes: `github.com/weftgo/weft` →
   `github.com/weftbase/weft`, and `github.com/weftgo/weft/core` →
   `github.com/weftbase/weft/core`.
2. The platform — Studio, the store, the runtime link, the local
   exporter and the CLI — leaves weft for its own repository and module,
   **weftbase**: `github.com/weftbase/weftbase` (its own version line,
   starting at v0.1.0; it reports the weft version it embeds). weftbase
   imports weft; weft never imports weftbase.

An app that only builds agents (the loop, the adapters, `thread`, `otel`,
`mcp`, `mw`, `wefttest`) needs step 1 only.

## 1. The framework's import path

```sh
# every .go file and go.mod in your module
grep -rl 'github.com/weftgo/weft' --include='*.go' . | xargs sed -i 's#github.com/weftgo/weft#github.com/weftbase/weft#g'
go mod edit -droprequire github.com/weftgo/weft -droprequire github.com/weftgo/weft/core
go get github.com/weftbase/weft@v0.21.0
go mod tidy
```

| Before | After |
|---|---|
| `github.com/weftgo/weft` | `github.com/weftbase/weft` |
| `github.com/weftgo/weft/core` (and `core/mw`, `core/wefttest`, …) | `github.com/weftbase/weft/core` (…) |
| `github.com/weftgo/weft/{openai,anthropic,google,mcp,mw,wefttest,thread,otel,version}` | `github.com/weftbase/weft/{…}` |

The instrumentation scope names follow the path (`github.com/weftbase/weft/core`,
`github.com/weftbase/weft/otel/slog`, `github.com/weftbase/weft/thread`): a
backend query or dashboard filtering on the old scope name needs the new one.

## 2. The platform packages

| Before (weft 0.20, or 0.21 pre-split) | After |
|---|---|
| `github.com/weftgo/weft/studio` (+ `studio/ingest`, `studio/runtime`) | `github.com/weftbase/weftbase/studio` (…) |
| `github.com/weftgo/weft/obsdb` (+ `obsdb/sqlite`, `obsdb/clickhouse`, `obsdb/obsdbtest`) | `github.com/weftbase/weftbase/obsdb` (…) |
| `github.com/weftgo/weft/runtime` | `github.com/weftbase/weftbase/runtime` |
| `github.com/weftgo/weft/scope` | `github.com/weftbase/weftbase/scope` |
| `github.com/weftgo/weft/exporter` | `github.com/weftbase/weftbase/exporter` |
| `github.com/weftgo/weft/cmd/weft` | `github.com/weftbase/weftbase/cmd/weftbase` |
| `github.com/weftgo/weft/examples/studio-local`, `examples/devtools-vite` | weftbase's `examples/studio-local`, `examples/devtools-vite` |
| `github.com/weftgo/weft/thread/examples/approvals` | weftbase's `examples/approvals` |

```sh
go get github.com/weftbase/weftbase@v0.1.0
```

### `otel.Local` and the other local destinations

Phase 1 (the Unreleased otel entry before this one) already moved them
out of `otel`; they now live in weftbase:

| weft 0.20 | weft 0.21 + weftbase |
|---|---|
| `otel.Local(path)` | `exporter.Local(path)` |
| `otel.Studio(url, tok)` | `exporter.Studio(url, tok)` |
| `otel.Install(…)` relying on the zero-config local sink | `exporter.Install(…)` |
| `otel.LocalDB()`, `p.LocalDB()` | `exporter.LocalDB()`, `exporter.LocalDBOf(p)` |
| `otel.StudioEndpoint()` | `exporter.StudioEndpoint()` |
| `otel.NoDiscovery()` | `exporter.NoDiscovery()` |

with `import "github.com/weftbase/weftbase/exporter"`. `otel.Install`
keeps every other destination (`otel.OTLP`, `otel.Datadog`,
`otel.Langfuse`, `otel.Exporters`, `otel.Sink`) and reads the standard
`OTEL_*` variables only.

## 3. The command line

The installed binary is `weftbase`; weft ships none.

```sh
go install github.com/weftbase/weftbase/cmd/weftbase@latest
```

| weft 0.20 | weftbase |
|---|---|
| `weft studio [flags]` | `weftbase dev [flags]` |
| `weft dev [flags] -- go run ./cmd/app` | `weftbase dev [flags] -- go run ./cmd/app` |
| `weft dev` (no command: ran `go run .`) | `weftbase dev -- go run .` (the command is required for the dev loop; `weftbase dev` alone serves Studio) |
| `weft runs`, `weft open`, `weft export`, `weft doctor`, `weft mcp`, `weft vacuum` | `weftbase runs`, `weftbase open`, … (same flags) |
| `weft version` | `weftbase version` → `weftbase v0.1.0 (weft v0.21.0)` |
| `claude mcp add weft -- weft mcp` | `claude mcp add weft -- weftbase mcp` |

## 4. Environment variables and files

No fallback reads the old names.

| weft 0.20 | weftbase |
|---|---|
| `WEFT_STUDIO_URL` | `WEFTBASE_URL` |
| `WEFT_STUDIO_TOKEN` | `WEFTBASE_TOKEN` |
| `WEFT_STUDIO_ADDR` | `WEFTBASE_ADDR` |
| `WEFT_DB` | `WEFTBASE_DB` |
| `WEFT_DISCOVERY` | `WEFTBASE_DISCOVERY` |
| `./.weft/` (database, `<db>.token`, `studio.json`) | `./.weftbase/` |
| `./.weft/weft.db` (the default database) | `./.weftbase/weftbase.db` |
| `$XDG_RUNTIME_DIR/weft/`, `<user cache dir>/weft/` | `$XDG_RUNTIME_DIR/weftbase/`, `<user cache dir>/weftbase/` |
| panel tokens `weft_pt.…`, live grants `weft_lg.…` | `wb_pt.…`, `wb_lg.…` |

Unchanged: `WEFT_ENV` (weft's development gate), `WEFT_MODEL_REQUESTS`,
`WEFT_MANIFEST`, `WEFT_RETAIN`, `WEFT_PRICING`, `WEFT_LOG_LEVEL`,
`WEFT_LOG_FORMAT`, `WEFT_CLICKHOUSE_DSN`, the `OTEL_*` variables and the
`@weftgo/devtools` npm package name.

To keep an existing local database, move it:
`mkdir -p .weftbase && mv .weft/weft.db .weftbase/weftbase.db && mv .weft/weft.db.token .weftbase/weftbase.db.token`
(or pass `--db sqlite://.weft/weft.db`). Tokens minted by a 0.20 Studio
(`weft_pt.…`) are not accepted: mint new ones.

## 5. Docs that moved

`studio/README.md`, `docs/adr/0018-studio-ui-stack.md`,
`docs/adr/0024-observability-data.md`, the `weft mcp` recipe
(`docs/mcp-recipe.md`) and the platform's testing sections are in
weftbase. `docs/record-contract.md` stays here (weft emits the
records); weftbase's tests read it from the weft module.
