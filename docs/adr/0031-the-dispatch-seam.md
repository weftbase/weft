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
   dispatch record, are durable.
4. **Neither is inherited.** A `Subagent`'s child run, and a `CallTool`
   from inside a handler, get neither the key option nor the gate: the
   gate is cleared from the handler's context. A child run's calls carry
   run-scoped keys. Nothing can make them recovery-stable: a child
   re-run after a crash asks its model again and issues new calls. So
   `ReplayKeyed` holds for a session's own calls; inside a `Subagent`,
   the `Subagent` tool's own replay class decides recovery.
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
