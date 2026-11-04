# ADR 0032 — thread recovery: cut-off turns and calls of unknown outcome

- Status: decided (2026-11-04; weftbase work #2 and #5, review findings
  1, 2, 3 and 7, approval gap G5, after the architecture review of the
  `weftbase-gaps` branch)
- Depends on: ADR 0003 (replay classes), ADR 0007 (the approval
  boundary), ADR 0011 (sessions, the format, T1–T5), ADR 0021 (thread
  approvals), ADR 0022 (the pool), ADR 0031 (the dispatch seam)
- Amends: ADR 0011 (format 6: `turn.cut_off`, `approval_request.recovery`)
  and ADR 0021 (the recovery request, Via `recovery`, step `recover`)

## Context

A process that dies mid-turn — a crash, a kill, a deploy that stops a
worker — leaves a session whose file shows a run that started and never
ended. Before this record the next turn's input repair answered any
call left without a result with an error ("interrupted"), and the model
was free to ask for it again. For a read that is harmless. For a
payment it is a double charge: the handler may have run to completion
an instant before the process died.

A first design wrote a durable per-call "started" marker (a custom
entry) before every call that is not `ReplaySafe`. It was dropped:

- it duplicated the step record — the assistant entry that holds the
  calls is appended before the loop dispatches them, and already says
  "these calls were issued";
- it cost one synced append per call of the default class, forever, on
  every backend (an HTTP round trip on a remote store) for precision
  only after a crash;
- it hid safety state in an opaque `custom` entry a 0.21 reader could
  not see — that reader would resume the session and run the call
  again — and that application code could forge.

The first argument proved wrong (review P0-1): "issued" is not
"started" when a park rule or a middleware holds one call of a step
and lets its sibling run. The record came back as a typed, format-6
`dispatch` audit entry — see "Dispatch records" below — which answers
the other two objections: it is a kind a 0.21 reader refuses, not an
opaque custom entry, and the cost buys a guarantee.

## Decision

**Rule A — no side effect without its step on file.** Every turn carries
a `BeforeDispatch` gate (ADR 0031). A call that is not `ReplaySafe`
starts only when the step entry holding it is durably appended (the step
observer's append, retried once at the gate) and only under the
session's key for that entry. If the append fails — a storage error, or
`ErrLocked` / `ErrStale` because another process took the session over
— the call fails closed: the handler never runs and the model reads an
error wrapping `core.ErrNotDispatched`. "Durable" is the backend's
append guarantee (`FsyncOnFlush` for jsonl's power-loss case). A
resume's calls sit in the boundary's entry, already on file.

**Rule B — outcome unknown.** After a crash, a call that is not
`ReplaySafe` and has no recorded result has an unknown outcome when it
may have run: its dispatch is recorded (below). That is a run's own
last step's calls the gate let start, and a resume run's boundary
calls approved to run and dispatched. A call delegating to a
`thread/pool` child is the pool's to settle (its child session holds
the outcome).

**Dispatch records (amended 2026-11-04, review P0-1).** A step entry
says which calls were *issued*, not which *started*: in one step a
call can run while a sibling is held by the agent's `ParkSource`, a
`ParkOn`/`ParkAllExcept` rule or a middleware's `ErrApprovalRequired`
— none of which survives a crash (a per-Send rule is lost, a
middleware's decision was never written). Recovery that read every
issued call as "may have run" retried a parked `ReplayKeyed` call on
the session's own approval: it ran with nobody's. So the gate records
before acting: after the step is on file and the fence holds, it
appends an `approval_audit` entry with step `dispatch` (CallID, RunID)
and flushes, then lets the handler run; a failed append or flush fails
the call closed. The step entry carries `dispatch_recorded`. A call of
such a step with no dispatch record under the cut-off run never
started: it parks as an **ordinary** request (`cut off before it
started: approve to run it, deny to skip it`, or the tool's own
`tool requires approval`), for a person. A step written before
dispatch records (no `dispatch_recorded`) is not knowable: nothing
automatic runs its calls — every one that may have run is a recovery
request for a person, a `ReplayKeyed` one included, and a Reconcile
"missing" is recorded but approves nothing. This costs one append per
call that is not `ReplaySafe` (the first design's cost, now bought
for a guarantee rather than for precision); `ReplaySafe` calls are not
recorded.

**The cut-off run.** The last run started on the leaf's path — a
prompt's run id, a resume's `started` audit, a recovery's continuation,
an overflow attempt's re-run — whose turn entry is missing, or marked
`cut_off` (a shutdown, below), and that no `recover` audit entry has handled. It is a fact of the file: `Open` is read-only and takes no
lock, so a turn another worker is running looks the same. A worker that
acts on it Opens, **Claims**, then recovers: the lease decides which of
two workers recovers, and the loser gets `ErrLocked`.

**Refusal (G5).** While a cut-off run has a call of unknown outcome,
`Send`, `Continue`, `Resume`, `Decide`/`DecideSigned` (when they would
arm the resume), the pool's `ReplayDecisions`, and the resume runner
itself (every armed resume passes the check before its run starts)
refuse with `ErrRecoveryRequired`. A cut-off run whose unanswered calls
are all `ReplaySafe` is not refused: input repair answers them, as
before.

**`Session.RecoverTurn(ctx)`** handles the cut-off run once, in one
atomic append ending with a `recover` audit entry:

| The run was cut off… | RecoverTurn |
|---|---|
| in a model call, between steps, or after a resume's results landed | writes its turn entry (`cut_off`) and starts a continuation from the last finished step, no new prompt, a fresh run id |
| with its own step's calls unanswered, all `ReplaySafe` | drops that step (the leaf moves back before it) and continues: the model is asked again |
| with its own step's calls unanswered, some not `ReplaySafe` | parks each call (rule C) |
| in a resume, before its results landed | the approved calls of unknown outcome park again (rule C); the rest keep their decisions |
| after its final answer, before its end | writes the end; runs nothing |

**Rule C — keyed retries, never parks.** A cut-off `ReplayKeyed` call
whose dispatch is recorded is retried by the session on its own: a
recovery request and the session's own approval (Via `recovery`, Who
`thread`), so it runs again with its same idempotency key — a
deduplicating receiver applies it once. A `ReplaySafe` call beside it
is not run again — a park rule may have held it, and that is not
recorded — the session resolves it with the pinned result `tool call X
was cut off before it answered and was not run again; call it again if
you still need its result` (Via `recovery`), and the model may ask
again under the agent's own gates. A `ReplayNever` call parks as a
**recovery request** (`ApprovalRequestEntry.Recovery`,
`Request.Recovery`) for a person. A call that never started parks as
an ordinary request. The session's own recovery approval resolves
alone, under any quorum: the call was already approved, or deduplicates
on its key.

**Rule D — retry or skip.** A recovery request is approved — retry the
call as it ran, same key — or denied or resolved — skip it, or supply
its result after checking by hand. An edit is refused (`ErrInvalidDecision`):
the call may have run with the original arguments, and different
arguments under the same key would be rejected or deduplicated by the
receiver. `ApproveAlways` is refused: a standing approval would let the
call run again unasked.

**Rule E — audited like any decision.** The session's own retries are
decision entries with a `recover` audit step (Outcome `retry`, Detail
the replay class) before each; a person's decisions on recovery requests
are ordinary decisions. All appear in `Audit()` and as
`weft.thread.approval.*` records; an approved call's span names
`weft.tool.approved_by = recovery` for the session's own retry.

**Model-visible text (pinned).** The request reasons
`cut off: the call may have run; approve to retry it with the same
idempotency key, deny to skip it`, `cut off: the call deduplicates on
its idempotency key and is retried`, `cut off: the call is ReplaySafe
and runs again`; the turn's `err`
`thread: turn cut off: the session's writer stopped before the turn ended`.
A dropped step changes the transcript the model sees: its assistant
message leaves the path (it stays in the file on its own line).

**Format 6.** A cut-off turn entry (`cut_off`), a recovery request
(`recovery`), a step that records its dispatches (`dispatch_recorded`)
and a dispatch record (step `dispatch`) are written with `"v":6`.
Every session this version writes a tool step to is therefore refused
by a 0.21 reader (checked against v0.21.0): a rolling deploy cannot
hand a crash file to an older worker that would answer a call that may
have run "interrupted". A reader from before recovery
fails loudly (`ErrNewerFormat`) on such a session instead of answering
its cut-off calls with a synthetic result. A session that never
recovered keeps its old versions.

**Shutdown.** A turn felled with the cause `thread.ErrShutdown` — the
cause of the context a `Close` gave up on, of a context already
canceled when given to `pool.Close`, or of the turn's own context — is
recorded `cut_off`, not `canceled`: its calls that are
not `ReplaySafe` and whose handler the cancellation felled (an error
result, effect unknown) or that never got a result stay unanswered on
`pending`, without synthetic text, and RecoverTurn handles the turn as
any cut-off run.

## Consequences

- One append (and flush) per dispatched call that is not `ReplaySafe`
  — the dispatch record; recovery then costs one atomic append per
  recovered session.
- A `Send`'s own run options do not survive a crash: a continuation runs
  under the session's run metadata and the boundary's recorded
  settings. A call a per-Send park rule or a middleware held never
  started, the dispatch records say so, and it parks as an ordinary
  request for a person; a call the model asks for in a continuation is
  held only by the agent's own gates (`RequireApproval`, `ParkSource`,
  middleware).
- A turn whose end could not be written in the same process (a panic in
  the writer) reads as cut off too: its call's result is not on file.
- Across hosts the lease is the arbiter: recovery on another host needs
  the TTL leases of the platform plan; until then a sticky lock needs
  `BreakLock`.
- `ReplayKeyed` holds for a session's own calls; inside a `Subagent`,
  the `Subagent` tool's class decides (ADR 0031).

## Amendment (2026-11-04 — never guess at dispatch: WR-4, WR-6)

**Flushed, not only appended (WR-1).** Under a backend that defers
its sync (jsonl's `FsyncOnFlush`), the gate flushes (`Flusher`) before a
risky call starts; a failed flush fails the call closed.

**The self-fence (WR-4).** Rule A's gate also asks the storage, with
no write, whether this writer still holds the session's lock — before
every call that is not `ReplaySafe`, not once per step: the optional
`Fencer` capability (`Fence(ctx, session) error`). sqlite re-reads the
lock row (one read); jsonl checks the locked file is still the
session's file; the in-process lease is checked from memory. A writer
whose lock was broken (`sqlite.BreakLock`), released or taken over
since its step landed starts no side effect: the call fails closed with
`ErrNotDispatched`.

**Outcome unknown in a live process (WR-6).** A crash is not the only
way to lose a call's answer. A call of a tool that is not `ReplaySafe`
whose handler started and gave no answer of its own — its `Timeout`
passed and the abandoned handler may still complete, or the run was
canceled under it (an Interrupt or Rollback send, the turn's own
context, a Close) — has an unknown outcome too. The core never states
otherwise: without a caller's ladder its result is the pinned error
`OUTCOME_UNKNOWN: tool "X" did not answer within D; it may still
complete; outcome unknown` (or `… was cut off before it answered; it
may have run; outcome unknown`), never "timed out", "context canceled"
or "interrupted". Under `core.ParkUnknown()` — every session turn
carries it — such a call parks instead (no result; span
`weft.tool.pending.source = outcome_unknown`) and the run ends at that
boundary after its step, the step's other results kept. The session
sends it down the ladder at the turn's end, in the turn's own append: a
`ReplayKeyed` call is retried by the session under its same key, a
`ReplayNever` call parks as a recovery request — an Interrupt's
follow-up waits behind it. A turn a Close fells is recorded cut off
(for `RecoverTurn` in whichever process opens the session next), as a
shutdown's is. A `ReplaySafe` call keeps the old texts: it can be run
again.

**No re-run over effects (WR-5).** ADR 0020 §5's overflow re-run
starts the turn again from its start on a compacted path that leaves the
failed attempt's steps behind. When that attempt started a call with
effects (one the gate let through), the re-run is skipped: the turn ends
with `ErrContextOverflow`, the attempt's steps and results kept on the
line, so the effect is never repeated and never hidden from the model.

**A rollback never hides an effect.** The `Rollback` busy policy
(ADR 0019) moves the leaf back before the felled turn, off the line
the model reads. When that turn has started a call that is not
`ReplaySafe`, the move would let the model ask for the effect again
as if it had not happened: the send is accepted as an `Interrupt`
instead (recorded as one), and `Send` returns its `Turn` beside an error
wrapping `ErrRollbackRefused`. The check and the gate serialize on the
session's lock (review P0-2): a call still in middleware when the
Rollback is accepted reaches the gate after it and is refused — the
gate refuses every call once an Interrupt, a Rollback or `Close` has
felled the turn, or once the run's context has ended (a shutdown). So
either the gate passes first and the Rollback is downgraded, or the
Rollback lands first and the call never starts.

**A refusal ends the run (review P2).** A gate refusal other than a
felled turn — the step or the dispatch record cannot be made durable,
or the fence says the lock is gone — also cancels the turn's run with
the cause `thread.not_dispatched` (the turn's error wraps the refusal:
a lost lease reads `ErrLocked`). A writer that cannot record starts
nothing more, instead of handing the model refusals until `MaxSteps`.
The fence is read last, after the dispatch record is flushed; the
residual window is the time from that read to the handler's effect,
and the handler's own run — a receiver that must refuse a zombie
writer needs the idempotency key, not the fence.

## Amendment (2026-11-04 — the ladder's second rung: Reconcile)

The ladder is: keyed retry, then reconcile with the system of record,
then a person (R4). A tool declares the second rung with
`core.Reconcile(fn)`, `fn(ctx, key) (found bool, result string, err
error)` — weft holds no store; the function is the tool's, and the same
name serves the flow tier. It is asked for a `ReplayNever` call of
unknown outcome only (a `ReplayKeyed` call retries first; a
`ReplaySafe` one needs nothing), with the call's own idempotency key,
while the writer still holds the session's lock (`Fencer`), with the
session's lock released, under a 30 s bound:

- found → a recovery request, a `recover` audit step
  `reconcile_found`, and the session's resolve (Via `reconcile`) with
  the recorded result: the tool does not run, the model reads the real
  result;
- missing → `reconcile_missing` and the session's own approval (Via
  `recovery`): the call runs again under the same key;
- an error, a panic or the bound → `reconcile_error` (Detail the error)
  and a recovery request for a person. Never "assume missing".

The answers are recorded in the same append as the rest of the
recovery, before anything acts on them; a crash while asking writes
nothing, so the next recovery asks again and the tool never runs on
its account. Pinned reasons: `cut off: the system of record reports
the call took effect; its result is recorded`, `cut off: the system of
record has no such call; it runs again with the same idempotency key`.

**A lost answer (WR-8).** `core.ErrOutcomeUnknown` lets a handler say it
cannot tell whether its effect happened (a request sent and its answer
lost); for a tool that is not `ReplaySafe` the loop then treats the call
as of unknown outcome, as above. weft/mcp wraps every tools/call
transport failure in it and always sends the call's idempotency key in
`_meta` (`dev.weftgo/idempotency_key`), so a server that deduplicates
applies a retry once.
