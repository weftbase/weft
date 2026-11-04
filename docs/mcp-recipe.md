# MCP recipes

Reading runs from Claude Code or Cursor (the stdio MCP server over a
running Studio) is the platform's: `weftbase mcp`, documented in
weftbase's `docs/mcp-recipe.md` (github.com/weftbase/weftbase). This
page is the framework's part.

## Your own agents over MCP: the caller link (`weft/mcp`)

The `weft/mcp` package, when one of your agents
calls another you serve with `mcp.Serve`. To see the served run linked
to the call that made it, opt in on the client:

    tools, err := mcp.Tools(ctx, sess, mcp.Server("helpers"), mcp.CallerMeta())

Every call those tools make then carries, in `params._meta`, the W3C
`traceparent` (and `tracestate`) of its `execute_tool` span and
`dev.weftgo/caller` `{run_id, call_id}`. The served run records
`weft.caller.run.id` / `weft.caller.call.id` with `weft.caller.remote`,
and its `invoke_agent` span links (an OpenTelemetry link) to the caller's
span — its own trace, never the caller's child, so the client's
sampling never decides whether the server records it. Studio keeps a
remote link off the run's place: the served run is top-level and its
session's own turn, whatever ids a client states (record contract §2).

`CallerMeta` is **off by default**: the run and call ids and the trace
state name your runs to the server. Turn it on for servers you run, not
for a third party's.

**The idempotency key always rides along.** Every `tools/call` carries
the call's `Call.IdempotencyKey` in `params._meta` under
`dev.weftgo/idempotency_key` (`mcp.MetaIdempotencyKey`), with or without
`CallerMeta`: a server that deduplicates on it applies a retried call
once. A transport failure — the connection dropped after the request
may have reached the server — is an unknown outcome
(`core.ErrOutcomeUnknown`), never a plain failure: for an imported tool
(`ReplayNever` unless you vouch otherwise) the model reads
`OUTCOME_UNKNOWN: …`, and a `weft/thread` session parks the call for its
recovery ladder (ADR 0032).
