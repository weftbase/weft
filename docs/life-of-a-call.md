# Life of a step, life of a tool call

Where each thing sits, in order. **Phases are documentation; seams are
code.** A named phase here never becomes a hook — behaviour attaches at
the two middleware seams (ADR 0006), observation at the tap (ADR 0004)
and in the loop's own spans and log lines (ADR 0016).

## A step

```
loop builds ModelRequest
  system  = Instructions
  tools   = ToolSource() if set, else the static list
  thinking, SequentialTools
        │
        ▼
PrepareStep (the one loop knob): rewrite the request per step —
  messages, tools, system; what it returns is what the step both
  advertises and dispatches against (nils/duplicates fail the run);
  PromptSnippets compose after it, from the tools it returned
        │
        ▼
system = Instructions + PromptSnippets of the (prepared) tools
        │
        ▼
┌─ `chat` span begins (ADR 0016): it wraps the whole chain, so one
│  span per step measures the chain's outcome; spans an adapter starts
│  for its own HTTP calls parent under it
│  — and the chain's context carries the reporter (weft.ReportFromContext):
│  mw.Retry/mw.Fallback (and any adapter that opts in) report each attempt,
│  an `attempt` span under `chat`; reporting, not a seam (ADR 0016)
model middleware chain (WrapModel; first listed = outermost)
  mw.Log ─── observation: request summary, finish, duration
  mw.Retry ─ alternative dispatch: retry a call that failed before any event
  mw.Fallback ─ alternative dispatch: another model on failure
  mw.RepairJSON ─ shaping: fix truncated tool-call arguments once
        │
        ▼
adapter (weft/openai, weft/anthropic, weft/google) → vendor SDK → HTTP
        │  ModelTextDelta / ModelReasoningDelta / ModelToolCallDelta /
        │  ModelToolCall … ModelFinish
        ▼
contract enforcement (ErrModelContract: exactly one finish, nothing after it)
        │
        ▼
└─ `chat` span ends: usage, finish reason, tool-call count — or the
   error status when the stream failed
        │
        ▼
transcript: one assistant message (reasoning parts, text, tool calls)
        │
        ├─ max_tokens with calls → every call fails without executing
        │                          (ADR 0002 "fail truncated calls")
        └─ calls → life of a tool call (below), then one tool message
        │
        ▼
StepFinish → pending approvals? (parked) → StopWhen? (its condition's
        │  name: StepCountIs(1) on a text step reports step_count_is:1)
        │  → steering drain (a delivery redirects into one more step)
        │  → no calls? (final_answer) → RunFinish
        │  (a run-ending step succeeds even if it overshot a budget)
        ▼
the continuation point: budgets (MaxModelRetries, UsageLimit, DetectLoops;
  first breach wins) → the next step (MaxSteps bounds it)
```

## A tool call

```
slot acquisition (Parallelism(n); Sequential() = one at a time;
                  a Sequential tool = barrier: wait, run alone, resume)
        │
        ▼
ToolStart (Seq assigned under the ordering lock)
        │
        ▼
Call.IdempotencyKey set (IdempotencyKeys' name, else the run-scoped key)
        │
        ▼
── containment boundary: panics and Timeout are handled here, outside ──
        │
        ▼
┌─ `execute_tool` span begins on the call's ctx (ADR 0016): a span the
│  handler starts — an HTTP client's, a child run's invoke_agent — is
│  its child
agent WrapTools chain (first listed = outermost)
  mw.Audit ──── observation: run, step, call, duration, error + cause
  AuthUser ──── decoration: verified data onto ctx (ExampleWrapTools_context)
  mw.Allow ──── decision: DENIED result, the model sees it
  mw.MapErrors ─ shaping: plain errors → coded ToolErrors (after next)
        │
        ▼
per-tool WrapTools chain (Tool(..., weft.WrapTools(...)))
        │
        ▼
base
  resolve name ─────────── unknown → NO_SUCH_TOOL
  RequireApproval? ──────── not Call.Approved → ErrApprovalRequired → pending
  BeforeDispatch gate ───── refused → ErrNotDispatched; the handler never runs
  handler middleware ────── WrapHandler (first listed = outermost): the
                            handler's side — reached only by a call
                            allowed to start; may answer instead of
                            the handler (thread/pool's delegation)
  decode (StrictInput) ──── failure → INVALID_INPUT naming the field
  handler ───────────────── string verbatim / JSON; error or *ToolError
  AfterDispatch hook ────── the handler returned (abandoned or not)
        │
        ▼
── containment boundary ── panic → "tool X panicked"; Timeout → "timed out"
                           (not ReplaySafe and the handler started: a
                           panic, a Timeout or a cancel → OUTCOME_UNKNOWN;
                           so does a middleware's or the AfterDispatch
                           hook's failure, panic or park after a handler
                           that answered with no error or still runs —
                           ADR 0032's 2026-11-05 amendment)
        │
        ▼
render: *ToolError → "CODE: message"; other error → its text
cap: MaxResultBytes (per tool, else per agent) + visible marker —
     over an edited call's EDITED line too (ApproveEdited)
        │
        ├─ pending → no ToolFinish; RunFinish.Pending after the step
        ▼
└─ `execute_tool` span ends before ToolFinish (result bytes, pending,
   or Error status + error.type — never the result text)
        │
        ▼
ToolFinish (Seq) → ToolResultPart, in call order, on the step's tool message
```

**The idempotency key and the gate** (ADR 0031, the dispatch seam).
Every call the loop executes carries `Call.IdempotencyKey` (read it with
`weft.CallFromContext`): pass it to an API that deduplicates (a payment
provider's `Idempotency-Key`) so a call executed twice takes effect
once. Without a session it is run-scoped — 32 hex digits of the SHA-256
of the run id and the call id, stable for one run. Under `weft/thread`
it is the session's (`thread.IdempotencyKey`): the same call keeps it
across a crash, a restart, an approval resume and a recovery retry. The
`BeforeDispatch` gate sits after every middleware and the approval
check, right before the handler: a parked call never reaches it, and a
refusal is an error result the model reads. `weft/thread`'s gate
refuses a call that is not `ReplaySafe` unless the step entry holding
it is durable, and records the dispatch (a format-6 `dispatch` audit
entry, flushed) before letting it start — after a crash a call with no
such record never started. It refuses every call once an Interrupt, a
Rollback or `Close` has felled the turn; any other refusal (a step or
record that cannot be made durable, a lost lease) also ends the run,
cause `thread.not_dispatched`. A `CallTool` from inside a handler is
keyed (from the dispatching call's key and its position) and passes
the same gate, `Call.Outer` naming the dispatching call — at any depth:
a call nested under an inner call the gate let start starts too, its
key derived from that one's. A park of such a call (its tool's
`RequireApproval`, the run's park rule) is the handler's to read
(`ErrApprovalRequired` from `CallTool`): returned by the handler it
fails the dispatching call — the model reads that the inner tool needs
approval and did not run — never parks it, for an approval would not
carry to the inner call. A `CallTool` from a `WrapTools` middleware —
before the call's handler started — is no handler's inner call: under a
gate it is refused (`ErrNotDispatched`), with none it is keyed apart
from the handler's (ADR 0031's 2026-11-05 amendment). The handler's
`CallTool` resolves against the run's `OnlyTools` narrowing. A run
started on a call's context — a `Subagent`'s child — inherits none of
the call's scope: not the key option, the gate, the hook, `ParkUnknown`,
the park rule (`ParkAllExcept` excepted: default-deny only tightens on
the way down) or the narrowing.

Where a **subagent's child run** sits: inside the handler line above.
A `thread/pool` delegation sits there too, as the handler middleware
`pool.Wrap` installs (`weft.WrapHandler`): under a session the child
session is created only after the approval boundary (a wrapped tool's
`RequireApproval`, `ParkOn`, `ParkAllExcept`, `ParkSource`, a
middleware's park) and after the gate has recorded, flushed and fenced
the delegating call — a parked, denied, felled or fenced-off call never
starts a child (ADR 0022's and ADR 0032's 2026-11-04 amendments). A
delegation reached through a handler's `CallTool` is refused under a
session (`pool.ErrNestedDelegation`): its receipt would name a call
the parent's transcript does not hold.
The `Subagent` handler calls the child agent's `execute` on the call's
ctx, so the child occupies the slot like any tool; its events are
wrapped (`Nested`, Seq from this run's counter, under this step's
ordering lock) between the delegating call's `ToolStart` and
`ToolFinish`, and its usage is recorded per call before the finish —
nothing of the child crosses the finish (the late-event rule, ADR 0014).

## The two conventions

- **Context decoration.** Middleware that verifies something (a user, a
  tenant, a quota) puts it on ctx before `next`; the handler reads it
  through a typed accessor, exactly like `weft.CallFromContext`.
  Decorate only with what the middleware verified; derive business
  values in the handler after decode.
- **Error shaping.** Handlers return `*weft.ToolError` when the model
  should branch on a code; `mw.MapErrors` codes everything else
  centrally. The cause (`ToolError.Err`) is for `Audit` and
  `errors.As`, never for the model.

## Resuming an approval

```
Generate(Messages(res.Messages...), Approve(id), Deny(id, reason))
        │
        ▼
repair, exempting the last assistant message's unresolved calls
        │
        ▼
approved → the tool-call chain above, Call.Approved = true
denied   → DENIED: <reason>;  undecided → DENIED: no decision
           (weft/thread: a skipped recovery request reads OUTCOME_UNKNOWN,
            and Resume refuses while one is undecided)
        │
        ▼
results complete the earlier tool message → first model call of this run
```

See ADR 0006 (seams), ADR 0007 (approval), ADR 0002 (errors), ADR 0004
(events).
