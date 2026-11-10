# ADR 0031 — the dispatch seam: idempotency keys and the dispatch gate

- Status: decided (2026-11-04; weftbase work #1 and #2, after the
  architecture review of the `weftbase-gaps` branch)
- Depends on: ADR 0003 (the tool contract and its replay classes), ADR
  0006 (the two seams; "a phase turned into a hook needs an ADR"), ADR
  0007 (the approval boundary), ADR 0014 (subagents as tools)
- Amends: ADR 0006 — two per-call run options at the dispatch phase,
  neither of them a third seam

## Context

A tool with side effects — a payment, a refund, an email — must take
effect once even when it is executed twice: retried after a crash,
re-run by a resume, retried by the platform. The standard answer is an
idempotency key the receiving API deduplicates on (Stripe's
`Idempotency-Key`). The handler has to *have* a key, and the key has to
be the same on every execution of the same call — which only the layer
that owns the call's durable record can name: a plain run knows its run
id, a `weft/thread` session knows the journal entry that holds the call.

The same layer also has to be able to say *no* at the last moment. A
session must not start a side effect it could not account for after a
crash: if the step entry that holds the call is not durable, a crash
leaves a payment that the file does not show was ever asked for. The
loop cannot know this; the session can.

Both needs sit at one phase of the life of a call (docs/life-of-a-call.md):
after the tool chain and the approval check, right before the handler.
ADR 0006 says a phase turned into a hook needs an ADR. This is that ADR.

## Decision

1. **`Call.IdempotencyKey`.** Every call the loop executes carries a
   key, read like the rest of the call with `CallFromContext` — ADR
   0006's context-decoration convention, so middleware reads it too
   (keys are set before the tool chain runs). Without an option the key
   is run-scoped: 32 lowercase hex digits, the first 128 bits of the
   SHA-256 of the run id and the call id, NUL-separated; stable for one
   run. The format is documented on the field; the function that
   computes it is not exported — nobody outside the loop needs to
   compute a run-scoped key.
2. **`IdempotencyKeys(fn func(Call) string) RunOption`.** Names each
   call's key. The loop calls `fn` once per call it is about to execute
   — an ordinary step's call and an approved call a resume runs — on
   the dispatching goroutine, in call order, with the `Call` as the
   handler will see it; a non-empty answer is the key, an empty one
   falls back to the run-scoped key. `weft/thread` passes it on every
   turn (`thread.IdempotencyKey`: session id, the id of the assistant
   entry that holds the call, the call id — plus the edited arguments'
   hash for an edited call), so a session's key is recovery-stable: the
   same call keeps it across a crash, a restart, a resume and a
   recovery retry, on any worker, with no store and no coordination.
3. **`BeforeDispatch(fn DispatchFunc) RunOption` is a gate.**
   `DispatchFunc func(ctx, Call, *ToolDef) error` is called for every
   call about to execute, after the agent's and the tool's middleware
   and after the approval check — so a parked call never reaches it —
   immediately before the arguments are decoded and the handler runs,
   on the call's goroutine. It answers one question: may this call start
   now? A non-nil error stops the call: the handler never runs and the
   model reads an error result wrapping `ErrNotDispatched` and the
   gate's error (data, rule 1 — siblings keep running). The gate is
   for preconditions, not work: it is not where a session writes
   anything — except `weft/thread`'s, which records the dispatch it is
   about to allow (ADR 0032's dispatch records). `weft/thread`'s gate
   is Rule A of crash recovery (ADR 0032): a call that is not
   `ReplaySafe` starts only when the step entry holding it, and its own
   dispatch record, are durable. Its counterpart, `AfterDispatch(fn
   AfterDispatchFunc) RunOption` (amended 2026-11-04, review 3 P2-2), is
   called on the same goroutine once the handler has returned or
   panicked — also a handler the loop abandoned at its `Timeout` or the
   run's cancellation, when it finally returns — so a caller knows
   whether a call it may retry is still running (`weft/thread` never
   retries a keyed call over its own running handler). Same
   inheritance as the gate.
4. **A child run inherits neither; a `CallTool` inherits both, derived
   (amended 2026-11-04, review P1-5).** A `Subagent`'s child run gets
   neither the key option nor the gate: the gate is cleared from the
   handler's context. A child run's calls carry run-scoped keys.
   Nothing can make them recovery-stable: a child re-run after a crash
   asks its model again and issues new calls. So `ReplayKeyed` holds for
   a session's own calls; inside a `Subagent`, the `Subagent` tool's own
   replay class decides recovery — and `Subagent` panics when marked
   `ReplayKeyed`, since nothing would deduplicate a retried delegation.
   A `CallTool` from inside a handler is a call with effects like the
   loop's own: its key is derived from the dispatching call's key and
   its 1-based position among that handler's `CallTool` calls (the
   first 128 bits of the SHA-256 of the two, NUL-separated) — the same
   on a retry of the outer call that dispatches in the same order, so
   an inner MCP call carries a key too — and it passes the run's gate,
   `Call.Outer` naming the dispatching call. `weft/thread`'s gate lets
   an inner call start only under an outer call it let start — the
   step's recorded call, or an inner call it let start before, at any
   depth (a nested call's key is derived from its outer inner call's,
   recursively) — never under a `ReplaySafe` tool's call (its handler
   starts no effect), and while the writer holds the session. An inner handler cut off by the
   run's cancellation before it answered returns an error wrapping
   `ErrOutcomeUnknown`.
5. **The last option wins**, for both; a nil function is ignored. A
   middleware that calls `next` more than once passes the gate once per
   attempt (harmless for a gate) and keeps the call's key.

## Alternatives considered

- **Middleware.** A `WrapTools` layer cannot derive the key: it needs
  the id of the journal entry holding the call, which only the session
  knows, at dispatch time; and agent-level `WrapTools` is fixed at `New`
  with no per-run form. The *use* of the key — deduplication — stays in
  the tool or in middleware, where it belongs. weft keeps no dedupe
  store.
- **A per-call "started" marker written by the gate.** Built on the
  branch first, then dropped: it cost one synced append per call that is
  not `ReplaySafe` — the default class — forever, for precision only
  after a crash, and it hid safety state in an opaque custom entry an
  older reader could not see (ADR 0032 has the whole argument). The
  gate's signature stays able to refuse; it is documented as a gate.
  Amended (2026-11-04, review P0-1): `weft/thread`'s gate does write
  one record per dispatched call that is not `ReplaySafe` — a typed
  format-6 `dispatch` audit entry, appended and flushed before the
  handler runs — because a step entry says which calls were issued,
  not which started past a park rule or a middleware (ADR 0032,
  "Dispatch records"). The write is the session's, not the core's:
  the gate stays the core's only word on dispatch.
- **A key on the tool result or the transcript.** The key is not model
  input and changes nothing the model sees; it rides the call's context
  only.

## Consequences

- A tool that deduplicates declares `Replay(ReplayKeyed)` and passes
  `Call.IdempotencyKey` to its API (ADR 0032 retries such a call on its
  own after a crash).
- The refusal text (`weft: tool call not dispatched: <cause>`) is
  model-visible, like every error result; its words are pinned by the
  gate's tests.
- Costs: one hash per executed call; no writes, no reads.

## Amendment (2026-11-05 — a call's scope is its run's alone)

Review of 0.23 (core findings C1, C3, C5, C6, C8, C11, C12) found the
per-call context leaking past the run that owns it, and two promises
the code did not keep.

1. **A run starts from none of its caller's call scope.** Every run
   clears, at entry, what a tool call's context carries for that call's
   handler: the gate, the `AfterDispatch` hook, `ParkUnknown`, the park
   rule a `CallTool` reads, the `OnlyTools` narrowing and the inner
   dispatch scope. §4's "a child run inherits neither" now holds for any
   run started on a call's context — a `Subagent`'s child, a `Generate`
   in a handler or in a tool middleware — not only for one started past
   the base, where the gate was already cleared. `ParkAllExcept` and
   `Metadata` are inherited as before.
2. **An inner call matches its outer call whole.** A handler's
   `CallTool` is keyed and gated from the scope of the call whose `Call`
   (run, step, id) it carries, never by call id alone: a child run's call
   may reuse its parent's call id (Gemini's `call_0`).
3. **A `CallTool` from a `WrapTools` middleware is no inner call.** It
   runs before the outer call passed the gate: nothing has let an effect
   start under it, and `weft/thread`'s gate already refused it (its
   outer call was never recorded as started). Decision: under a gate the
   core refuses it, before any layer runs, with `weft: tool call not
   dispatched: tool "X" was dispatched from a tool middleware of call
   "Y", before that call's handler started; under a dispatch gate an
   inner call is dispatched only from inside the handler` (pinned). With
   no gate it runs keyed: the first 128 bits of the SHA-256 of the outer
   key, `wrap` and its 1-based position among that middleware's
   `CallTool` calls, NUL-separated — never equal to a handler's inner
   key, never empty. Keying it from the handler's counter was rejected:
   the counter is per handler attempt, and a middleware-level call and
   the handler's first call would share a key.
4. **`CallTool` honours the run's `OnlyTools`.** A handler's inner call
   resolves against the run's narrowed tools: a narrowed-away tool is
   unknown (`ErrNoSuchTool`), as it is to the loop.
5. **The default key is run-scoped, and says so.** §1 stands: without
   `IdempotencyKeys` a key is stable for one run. A resume is another
   run, so a call it re-runs (an approval, a `ParkUnknown` park) gets
   another key. The field's godoc no longer promises "re-run by a resume
   takes effect once"; a key that holds across a resume needs
   `IdempotencyKeys` (`weft/thread` passes it). The derivation is
   unchanged — except that an outer call with an empty key gives its
   inner calls (a handler's and a middleware's) an empty key too, never
   one derived from `""`: that would be one constant shared by every
   keyless call, and a deduplicating downstream would drop the second of
   two distinct requests. The loop's own calls always carry a key; an
   `Agent.ServeCall` whose client sent none is the case.
6. **A panicking `IdempotencyKeys` fails the call closed.** It was
   called uncontained on the dispatching goroutine (under `Stream`, a
   process crash). The panic is contained: that call is not dispatched
   — `weft: tool call not dispatched: the run's IdempotencyKeys function
   panicked naming the call's key: <value>` (pinned), its handler never
   started — the run and its siblings go on (rule 1), and one Warn line
   names the call.

Tests: `TestParkUnknownIsNotInheritedByAChildRun`,
`TestChildMiddlewareCallToolNeverReachesTheParentsGate`,
`TestParkOnIsNotInheritedByAChildRunsCallTool`,
`TestCallToolFromAMiddleware`, `TestCallToolHonoursOnlyTools`,
`TestIdempotencyKeysPanicFailsTheCallClosed`,
`TestServeCallKeylessInnerCallsCarryNoKey` (core).

## Amendment (2026-11-06 — no inner call behind a settled answer)

A handler the loop abandoned at its `Timeout` (or at the run's
cancellation) keeps its goroutine, and on plain core nothing stopped it
from dispatching inner calls afterwards: an email could be sent after
the model had read `OUTCOME_UNKNOWN` for the call that sent it. Under a
session the gate refused them; without one nothing did. §4 now holds
without a gate too: an inner call starts only while its outer call is
open. Once the loop has settled the outer call — answered it at its
deadline or cancellation, or read its handler's return — `CallTool`
from that handler (or from a goroutine it left behind) fails with an
error wrapping `ErrNotDispatched`, and the inner handler never runs.
The rule follows 0.22.1's and 0.24.0's abandon lock (a handler not
entered by the deadline never starts). No text the model reads changes:
the refusal is the abandoned handler's to read. Test:
`TestCallToolAfterTheOuterCallSettledIsRefused`.

## Amendment (2026-11-10 — a gate may park)

Decision 3 said a gate's error stops the call with an `ErrNotDispatched`
result. A gate that wraps `ErrApprovalRequired` in its error instead
**parks** the call: the handler never runs, the call is pending
(`RunResult.Pending`), the run ends at the approval boundary as for any
park (ADR 0007), and its siblings run. A resume that approves the call
passes it through the gate again — the gate is the last word — so a gate
still holding it parks it again. A call dispatched with `CallTool` that
the gate parks fails its outer call as any dispatched park does (ADR
0032's final-review amendment, P2-3). The gate still writes nothing for a
parked call; `AfterDispatch` is not called (no handler started).

Why: `weft/thread`'s still-running gate (ADR 0032, 2026-11-10) must hold a
call for a person while an abandoned handler of the same tool still runs —
"not now, a person decides", which is a park, not a refusal; a refusal
would read as the tool's failure and invite the model to retry. The core
always matched `ErrApprovalRequired` through the gate's wrapped error; this
amendment makes it the contract. Test: `TestBeforeDispatchParkParksTheCall`.
