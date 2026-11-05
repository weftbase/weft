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
appends an `approval_audit` entry with step `dispatch` (CallID, RunID,
and Detail the tool's replay class as it starts — review 3 P2-5)
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
call run again unasked. A skip never reads "DENIED" to the model — that
invites the model to ask again, and the call may have run (review
P1-3): a denied recovery request resolves the call with the pinned
error result `OUTCOME_UNKNOWN: tool "X" may have run; it was skipped,
not retried: do not repeat it (skipped: <reason>)` (the parenthesis
only when the decider gave a reason; an expiry's or an interrupt's
denial reads the same way).

**Bounded keyed retries (review P1-2).** The session's own retries of
one call occurrence are capped at 3 — counted from the `recover`/`retry`
audit steps after the step entry holding it, so the cap survives a
restart — and a resume that carries the n-th (n ≥ 1) waits 1 s ·
2^(n-1) first, at most 30 s — 1 s, 2 s, 4 s (review 3 P2-2: the first
retry backs off too; an interrupt cuts the wait short). Past the cap the call
parks as a recovery request for a person, reason `outcome unknown: the
session's automatic retries are spent; approve to retry it with the
same idempotency key, deny to skip it`; a person's retry that ends
unknown again comes straight back to a person. Without the cap a
`Timeout` shorter than the provider looped (timeout → park → retry →
timeout).

**No retry over a running handler (review 3 P2-2).** In a live
process the handler a keyed retry would repeat may still be running —
the loop abandoned it at its `Timeout`, or it ignored the run's
cancellation. The session sees each handler the gate let start return
(`core.AfterDispatch`, called on the call's goroutine when the handler
returns or panics, abandoned or not) and the end of the turn waits for
it, at most 30 s, before it approves the retry: two handlers of one call
never run at once. Still running past the bound, the call parks as a
recovery request for a person instead, reason `outcome unknown: the
call's handler is still running past its bound; approve to retry it
with the same idempotency key once it has returned, deny to skip it` —
no retry is counted (the final-review amendment extends the wait to
every class and to a person's approval).
The wait holds the turn's end; a `Close` waits for the turn's end
within its own deadline. A `Close` that gives up first cuts the turn
off and the end of the turn does not wait: the handler still running is
filed in the process's registry (release-gate amendment below), so a
`RecoverTurn` in the same process parks its call for a person — no
keyed retry, no `Reconcile` — and an approval waits for it. After a
crash no handler is left running, and nothing waits.

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
and is not run again; the model may ask for it again`; the turn's `err`
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

**A cut-off pool child is recovered, not failed (review P1-4).**
`thread/pool`'s `Recover` used to settle a child whose run a crash or a
shutdown cut off as `failed` — and a parent told "failed" may delegate
the work again over the child's effect. Such a child (a run started and
never ended, or a turn recorded `cut_off` that no recovery closed) is
now reattached and recovered on its own ladder (`RecoverTurn`, on a
pool goroutine): its calls of unknown outcome park and are mirrored
onto the parent as recovery requests, or it continues to its answer,
which settles the receipt; the receipt stays open meanwhile. A run is
started from its start record — a prompt, a resume's `started` audit,
a recovery's continuation (its `recover` audit's Detail) — until its own
turn entry lands, the session's own rule: a continuation or a keyed
retry cut off by a second crash is cut off too (review 3 P1-a, P1-b),
never read as the first cut-off turn's failure or as a park with
nothing to decide.

**A child cut off while delegating asks a person (review 3 P1-c).**
Recovery is not built at depth. A cut-off child that was itself
delegating (an open receipt or mirror of its own) has its own
delegations ended depth first, as before — a parked one canceled at its
boundary, a cut-off one settled `canceled` (never `failed`: what it did
is not known), a finished one from its file — and is then recovered like
any cut-off child, never settled `failed`. Its delegating call has no
result and its delegation is over; its dispatch record says it may have
run (the delegation passes the gate like any call: amendment below —
before it, the acceptance receipt stood in for that record).
`RecoverTurn` parks it for a person whatever its tool's class (no keyed
retry, no `Reconcile`), as a recovery request with the pinned reason
`cut off while it delegated: the delegated work may have run; approve to
delegate it again, deny to skip it`, mirrored onto the parent. Approve
runs the call again (a new delegation); deny skips it, and the child's
model reads `OUTCOME_UNKNOWN: tool "X" may have run; it was skipped, not
retried: do not repeat it`.

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
- **The replay class is the one at dispatch (review 3 P2-5).** A call
  with a dispatch record is recovered under the class its record names
  — a tool relabeled `ReplaySafe` or `ReplayKeyed` across a deploy does
  not make a call that started `ReplayNever` "not run again" or retried
  on the session's say. A `ReplaySafe` call is not recorded (that is the
  point of the class), so the other direction is a **deploy rule**: do
  not relabel a tool from `ReplaySafe` to another class, or drop a
  `ReplaySafe` tool (or the `ToolSource` that serves it — recovery reads
  a tool the agent no longer holds as `ReplayNever`), while sessions
  are cut off or parked on it: its cut-off call, which ran unrecorded,
  reads "cut off before it started" and parks as an ordinary request.
  It was a read, so approving it is harmless, but the reason text is
  wrong. Recording safe calls too would cost a write per read on the
  hot path (R13), which buys no safety.
- **The gate holds the session's lock across its append, flush and
  fence (review 3 P2-6).** The dispatch record, its sync and the fence
  read run under `s.mu`, so a call's gate serializes with Interrupt,
  Rollback and Close (that is what makes the felled-turn rule exact)
  and with the other calls' gates of the step: parallel non-safe calls
  of one step pay their syncs one after another, and a `Decide` or a
  `Pending` waits out a slow disk. A known cost, kept: releasing the
  lock across the sync would reopen the window the felled-turn rule
  closes.

## Amendment (2026-11-04 — never guess at dispatch: WR-4, WR-6)

**Flushed, not only appended (WR-1).** Under a backend that defers
its sync (jsonl's `FsyncOnFlush`), the gate flushes (`Flusher`) before a
risky call starts; a failed flush fails the call closed. sqlite defers
nothing (review 3 P2-4): every commit runs under `synchronous=FULL` in
WAL mode, `FsyncOnFlush` included — its `Flush` used to be a passive
checkpoint, which syncs nothing while a reader holds an old snapshot, so
the dispatch record was not provably durable before the handler. A
failed sync fails the commit, and the gate with it.

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
`ReplayKeyed` call is retried by the session under its same key (up to
the bound above), a `ReplayNever` call parks as a recovery request —
never reconciled here: the abandoned handler may still be running, and
a system of record asked before it lands answers "missing" for an
effect about to happen (review P1-1) — an Interrupt's
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
name serves the flow tier. It is asked by `RecoverTurn` — after a
crash or a shutdown, never for a handler this process abandoned and
that may still be running — for a `ReplayNever` call of unknown outcome
only (a `ReplayKeyed` call retries first; a `ReplaySafe` one needs
nothing), with the call's own idempotency key,
while the writer still holds the session's lock (`Fencer`), with the
session's lock released, under a 30 s bound:

- found → a recovery request, a `recover` audit step
  `reconcile_found`, and the session's resolve (Via `reconcile`) with
  the recorded result: the tool does not run, the model reads the real
  result;
- missing → `reconcile_missing` and the session's own approval (Via
  `recovery`): the call runs again under the same key — one of the
  session's automatic retries, with a `retry` audit step beside it, so
  the cap of 3 holds (review 3 P2-3: a handler that takes the process
  down every time looped across restarts); past the cap,
  `reconcile_missing` and a recovery request for a person (the retries
  spent);
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

## Amendment (2026-11-04 — a delegation is a call)

`thread/pool`'s `Wrap` used to delegate in tool middleware, which runs
before the approval boundary and before the dispatch gate. Under a
session a pool-wrapped tool's own `RequireApproval`, the run's `ParkOn`
and `ParkAllExcept`, an agent's `ParkSource` and a middleware's
`ErrApprovalRequired` were therefore ignored — the child ran unapproved
— and a delegation wrote no dispatch record and met no felled check and
no fence (a fail-open of WR-1, WR-4 and WR-7 for delegations).

**Decision: a delegation is an effect like any other call.** It
happens only where a handler would run: after the park decision and
after the gate passes. The core gains `core.WrapHandler(mw...)`, tool
middleware on the handler's side of the chain (agent middleware → tool
middleware → approval boundary → `BeforeDispatch` → handler middleware
→ handler); `Wrap` installs its delegation there, so the delegation is
the call's effective handler. So, under a session:

- a call the run parks never creates a child — approve and it
  delegates once, deny and no child ever starts;
- the gate writes the delegation's dispatch record (its replay class
  in `Detail`, `ReplayNever` unless the tool says otherwise) and
  flushes it before the child session or its acceptance receipt is
  created, refuses a felled turn, and fences: a writer that lost its
  lease starts no child (the run ends `thread.not_dispatched`);
- it is a handler for the rest of the rules too: inside
  `AfterDispatch`, a panic in it is the handler's, and a delegation of
  a tool that is not `ReplaySafe` cut off before it answered has an
  unknown outcome.

A delegating call whose child parked passed the gate, yet it is not of
unknown outcome: its child's requests are mirrored onto the parent, and
the turn's end parks it for the child's answer as before (ADR 0022 §7),
never down the ladder.

Recovery reads the delegation's own dispatch record. The acceptance
receipt no longer stands in for it (review 3 P1-c's stopgap is
dropped): a call whose delegation was ended unanswered is "may have
run" because its record says so, and still goes to a person whatever
its class (`cut off while it delegated: …`) — recovery is not built at
depth. A crash after the dispatch record and before the acceptance
receipt leaves a recorded call with no receipt: it may have run, so it
parks as an ordinary recovery request (`cut off: the call may have run;
…`; `Reconcile` asked first when the tool has one) — never "cut off
before it started", never delegated again on the session's own say.

Why a core hook and not the pool alone: the pool cannot move its
delegation behind the base without one. A tool of its own whose
handler delegates (and calls a `Subagent`'s `Invoke` outside a session)
loses what only `core.Subagent` records — the manifest's delegation
edge and the child's hash — and a context value that swaps the handler
would have to be cleared before every nested call to keep it from
leaking. `WrapHandler` is one tool option with no state.

Tests: `TestDelegationParksBeforeTheChild`, `TestDelegationPassesTheGate`,
`TestDelegationCutOffBeforeAcceptance` (thread/pool, jsonl and sqlite),
`TestWrapHandlerRunsPastApprovalAndGate`,
`TestWrapHandlerOrderAndManifest` (core).

## Amendment (2026-11-04 — final review: no second effect, no blind wait)

**A live delegation is the pool's (P1-1).** After a crash the boot
sequence `Open → pool.Recover → RecoverTurn` parked a delegating call
whose child was alive and mirrored as a recovery request too; the
request cleared the wrapper mapping, and approving both delegated
twice. `RecoverTurn` now leaves a call whose child request is mirrored
here, and whose delegation is not over, to the pool — as the "unknown
outcome" check already did. A delegation ended unanswered still parks
for a person (`cut off while it delegated: …`).

**No delegation from a nested `CallTool` (P1-2).** A delegation's
receipt, lineage and mirrors name the delegating call; one reached
through `Agent.CallTool` names an inner call the parent's transcript
does not hold, so its outer call was offered as an unknown outcome
beside the parked child. Decision: fail closed. Under a session
`pool.Wrap` refuses a call with `Call.Outer` set, before any child
exists, with `pool.ErrNestedDelegation`; keying a delegation on the
outermost call is not built.

**An undecided recovery request is a person's (P1-3).** `Resume`
denies undecided calls `no decision` — a plain `DENIED` the model
answers by asking again. While a recovery request is undecided `Resume`
fails with `ErrRecoveryRequired` and starts nothing. Every other path
that settles one is a deny of some channel (expiry, an interrupt, a
pool cancel), which reads as the `OUTCOME_UNKNOWN … do not repeat it`
skip, or arms only a settled boundary (`Continue`, `AutoResume`, the
pool's replay); `Close` records nothing.

**No retry beside a running handler, any class (P2-1, P2-4).** The
rule "no retry over a running handler" above covered only the session's
own keyed retry; a `ReplayNever` call abandoned at its `Timeout` was
offered at once, and a fast `Approve` started a second handler beside
the first. The end of the turn now waits for every abandoned handler
whose call may be retried (keyed or `ReplayNever`; a `ReplaySafe` one
is not run again and waits for nothing) before it retries or offers the
call — in parallel, under one shared 30 s deadline, so N calls hold the
turn 30 s, not N × 30 s. Past it the request's reason reads `outcome
unknown: the call's handler is still running past its bound; approve
to retry it with the same idempotency key once it has returned, deny
to skip it` (pinned), and an approval is refused with
`ErrStillRunning` until the handler returns (in-process: a restart
ends the handler with the process). A deny or a resolve is taken.

**An interrupt over an effect waits for a person, visibly (P2-2).** An
`Interrupt` (or a cancel) that fells a turn while a handler with
effects runs parks that call as a recovery request; the Interrupt's own
turn waits behind it until a person decides — unbounded, by design.
The felled turn's `Wait` error matches `ErrRecoveryRequired` beside the
cancellation, so a caller learns it must decide instead of waiting
blind.

**A handler's park of a dispatched call fails the call (P2-3).** A
handler that returned the `ErrApprovalRequired` of a call it dispatched
with `CallTool` (a `RequireApproval` or park-rule tool) was offered as
an unknown outcome; approving reran it, and it parked again for ever.
Parking the outer call as an ordinary request would loop the same way:
an approval names one call and never carries to what its handler
dispatches. So the core reads that park as the handler's failure — the
handler still sees `ErrApprovalRequired` from `CallTool`, and the model
reads `tool "X", dispatched by this call's handler, requires approval
and did not run: a dispatched call is never parked for one` (pinned).
A handler's own `ErrApprovalRequired` still parks its call.

Tests: `TestRecoverTurnLeavesALiveDelegationToThePool`,
`TestNestedCallToolDelegationIsRefused` (thread/pool),
`TestResumeRefusesAnUndecidedRecoveryRequest`,
`TestNeverRetryWaitsForTheAbandonedHandler`,
`TestApprovalWaitsForAStillRunningHandler`,
`TestAbandonedHandlersShareOneDeadline`,
`TestInterruptedEffectParksAndHoldsTheInterrupt`,
`TestCallToolParkReturnedByTheHandlerDoesNotPark` (thread),
`TestCallToolParkReturnedByTheHandlerFailsTheOuterCall` (core).

## Amendment (2026-11-04 — release gate: one choke point)

Review rounds kept finding one more path that settled an undecided
recovery request as a plain `DENIED: no decision`, or reported an
unknown outcome as a failure. Each was closed where it was found; this
amendment closes the class.

**One choke point for every resume (P1-A).** A resume is minted in one
function (`mintResumeLocked`); it refuses — `ErrRecoveryRequired`,
nothing minted, the boundary parked — while a cut-off run's call of
unknown outcome is on file or a recovery request (the session's own,
or a pool child's mirrored here) is undecided. Every arming path mints
there: `Decide`, `DecideSigned`, `Resume`, `Continue`, a `Send`'s
settled-boundary pickup, the runner's chain-decided hand-off and
settled pickup, the re-arm after an `Interrupt` felled a resume,
`RecoverTurn`, `ReplayDecisions` and the pool's replay. The runner
asks the same function again when it starts a resume (`runResume`), for
what may have changed since the arming. The path the review found: an
`Interrupt` felled a resume while its approved call with effects ran;
the felled call parked as a recovery request, and the runner's re-arm
for the Interrupt's follow-up resumed over it. It now stays parked, the
follow-up behind it, until a person decides — as P2-2 above already
said of a felled turn.

The deny channels (expiry, an Interrupt over a parked boundary, a pool
`Cancel`) still settle a recovery request: each is a decision of its
own, read by the model as the `OUTCOME_UNKNOWN … do not repeat it`
skip, never as `DENIED`.

**One place a child's refusal maps to "parked" (P1-B).** A pool child
refuses a replay with `ErrStillRunning` (an approval of a call whose
abandoned handler still runs) or `ErrRecoveryRequired`. Neither is the
delegation's failure — the call's outcome is unknown, not failed — so
the pool's run maps both, in one place (`refusedReplay`), to a park on
the same mirrors, never to a `failed` settlement (which read
`SUBAGENT_FAILED` to the parent's model). And before that,
`Pool.Decide` and `Pool.DecideSigned` refuse such an approval on the
parent with `ErrStillRunning`, nothing recorded; one recorded through
the parent session's own `Decide` is replayed by the pool when the
handler returns.

**The abandoned handler is the process's (P2-1, P2-2).** "In-process"
above meant one `Session`: a `Close` then `Open` in the same process —
a server evicting an idle session — forgot the handler, and an
approval ran a second one beside it. The handlers a turn's end leaves
running are now held in a process-wide registry keyed by the call's
occurrence: its run id and call id. Run ids embed the session they were
minted in (`<session>-t<n>`), so the key names the origin session too,
and every session holding the request — the one that ran it, a reopen
of it, a `Fork` that copied it, a pool parent's mirror — asks under the
same key, whatever its own id. Two stores holding one pinned session id
collide on it; a collision can only refuse an approval, never let one
through beside a running handler. An entry is added by the turn's end
that leaves the handler running and removed by `core.AfterDispatch`
when the handler returns, whatever became of the request — the
registry holds only handlers that run. A restart still ends them with
the process.

**A `Close` that gives up files the handler too (release gate, last
round, P1-1).** A `Close` whose deadline passes while a handler ignores
the cancellation cuts the turn off (`TurnEntry.CutOff`, as a shutdown):
the decision chain does not run, so the end of the turn neither waited
for the handler nor filed it, and a `Close`, `Open`, `RecoverTurn` in
the same process retried a keyed call at once beside the first
handler, asked a `ReplayNever` call's `Reconcile` while it ran, and
took an approval that started a second one. The cut-off branch now
files every unknown-outcome call's live handler under the cut-off run,
before the cut-off is durable. `RecoverTurn` (and its rungs) treat a
filed, still-running call as still running: no keyed retry, no
`Reconcile`, the call parked for a person with the still-running
reason, an approval refused with `ErrStillRunning` until the handler
returns, then taken as any. The same holds under a pool: `Pool.Close`
under a shutdown cut its running children off; the closed pool now
finishes each child's `Close` once its cut-off landed, releasing its
hold, so a pool rebuilt in the same process can `Recover` it — and the
recovered call waits for the handler. A `Fork` of a session carries a
still-running request with its original run id; the fork's approval is
refused the same way (P2-1).

**A live delegation is read from the ledger, not the mirror (release
gate, last round).** `RecoverTurn` left a delegating call to the pool
only when a mirrored child request named it. After a `Close` that gave
up on a sync delegation, `RecoverTurn` run before the pool's `Recover`
found no mirror yet and parked the call as a plain "approve to retry"
recovery request; the pool then mirrored the child's still-running
call beside it, and approving the first delegated twice. The durable
record of a live delegation is the pool's acceptance receipt with no
settlement: `RecoverTurn` (and `unknownLocked`) now leave a call so
accepted to the pool as they leave a mirrored one, so both orders end
the same. Until the delegation settles the call stays offered as its
boundary's own call, its reason saying the delegation is open, and an
approval of it is refused with `ErrStillRunning` — the pool resolves
it, or, when the pool still holds an ending delegate and takes nothing
up, a later `Recover` does. A delegation ended unanswered still parks
for a person (`reasonDelegated`).

Tests: `TestInterruptedResumeLeavesItsRecoveryRequestForAPerson`,
`TestNoArmingPathSettlesAnUndecidedRecoveryRequest`,
`TestEveryArmingFunctionRefusesAnUndecidedRecoveryRequest`,
`TestStillRunningSurvivesAnInProcessReopen`,
`TestStillRunningRunIDCollisionOnlyRefuses`,
`TestAbandonedRegistryReleasesOnReturn`,
`TestCloseGiveUpThenRecoverTurnWaitsForTheStillRunningHandler`,
`TestForkRefusesApprovingAStillRunningCall` (thread);
`TestPoolCloseGiveUpThenRecoverWaitsForTheStillRunningHandler`,
`TestPumpWhenReturnedKeepsOneWaiterPerHandler`,
`TestPoolDecideRefusesApprovingAStillRunningChildCall`,
`TestRefusedChildReplayKeepsTheDelegationParked`,
`TestPoolReplayNeverSettlesAnUndecidedRecoveryRequest`,
`TestCloseGiveUpSyncDelegationIsNotOfferedAsARetry` (thread/pool).

## Amendment (2026-11-05 — the _meta keys are renamed)

The idempotency key's `_meta` name quoted above, `dev.weftgo/idempotency_key`,
is `ai.weftbase/idempotency_key` (`mcp.MetaIdempotencyKey`) from 0.23.0,
and the caller link's `dev.weftgo/caller` is `ai.weftbase/caller`. weft
sends only the new names; for one release `mcp.Serve` reads a caller
under the old name when the new one is absent, and the old names stay
exported as `mcp.LegacyMetaCaller` / `mcp.LegacyMetaIdempotencyKey` for
servers that deduplicate during the transition. The rest of the
decision is unchanged.

## Amendment (2026-11-05 — the runner reads the file; torn batches; shutdown's partial step)

**A step's partial results do not hide its cut-off call (R1).** A
shutdown keeps the results of a step's calls that answered and leaves
the others unanswered ("Shutdown" above), so a step cut off with one
call answered and a sibling still running ends in a partial tool
message. The cut-off check read only the run's last message — that
tool message — and saw no call of the run's own: `Send` was accepted
and the unanswered call was offered as an ordinary request, its skip
reading `DENIED`. The check now looks past tool messages to the step's
assistant message: the unanswered call is the cut-off run's own, and
RecoverTurn decides it as any. The turn entry's `pending` lists it
once.

**Only an unknown outcome is taken out (R5).** The shutdown's rule
"whose handler the cancellation felled (an error result, effect
unknown)" is read literally: a result is taken out of the cut-off step
only when the core marked it `OUTCOME_UNKNOWN` (without `ParkUnknown`;
a session's run parks such a call with no result instead). A handler's
own error — a declined card — is its answer and stays, and so does a
refusal before the handler ran (the gate, a middleware): recovery never
offers a call whose outcome is known as one that "may have run".

**The runner asks the check on its own transitions (R2).** The check
answered "not cut off" while the session's runner was alive, and the
runner itself never asked it: a turn whose end could not be written
(`ErrNotPersisted`) or that panicked (`ErrTurnPanicked`) after it
dispatched a call of unknown outcome handed the queue on, the next
send's input repaired the call into "interrupted", and recovery lost it
for good. "A turn whose end could not be written in the same process
reads as cut off" (Consequences) now holds in the process that wrote
it: the check skips only a turn in flight, not a runner between items;
the runner holds the queue behind such a turn (a steer's follow-up
included); a queued send is put back before its prompt; a resume reads
the file before it starts; the failed turn's error carries
`ErrRecoveryRequired`; and RecoverTurn — its run, the boundary it
parks, or, when it starts nothing, the queue's head at once — takes
the held sends on.

**A torn batch reads as not done (R4).** jsonl can keep a prefix of a
batch's complete lines after a crash (a kill inside a large write, a
power cut); only a torn last line is dropped. "Atomic against the
writer's death" was true of sqlite and Memory, not of jsonl. The turn
entry was first in a turn's end batch, so a prefix could hold the turn
without the recovery request the in-process ladder wrote for its call
of unknown outcome: the call read as an ordinary request. The turn
entry now goes last — after the chain's entries, the delivered steers'
receipts and a resume's end audit — so any prefix reads as the turn
cut off. RecoverTurn's own batch already ends with its `recover` audit,
so a prefix of it reads as not recovered (a torn request is superseded
by the next recovery's; a torn `retry` audit counts toward the cap: the
safe side). No format change.

**An interrupted ask is not an answer (R6).** "A crash while asking
writes nothing" now covers a caller who walks away: when RecoverTurn's
context ends while `Reconcile` is asked, it returns the context's error
and writes nothing — it used to record `reconcile_error` and send the
call to a person for good. The calls of one recovery are asked in
parallel under one shared 30 s bound, and a `Reconcile` that ignores
its context is abandoned at the bound (an error, a person decides).

Tests: `TestShutdownWithAnAnsweredSiblingIsRecovered`,
`TestShutdownKeepsAHandlersOwnFailure`,
`TestQueuedSendWaitsForRecoveryOfAnUnrecordedTurnEnd`,
`TestSteerFollowUpWaitsForRecoveryOfAnUnrecordedTurnEnd`,
`TestQueuedSendWaitsForRecoveryOfAPanickedTurn`,
`TestQueuedSendWaitsForRecoveryOfAnUnrecordedResumeEnd`,
`TestTornTurnEndBatchReadsCutOff`, `TestTornRecoverBatchStaysCutOff`,
`TestReconcileInterruptedByTheCallerWritesNothing`,
`TestReconcileAsksCallsInParallel` (thread); the crash matrix's
`tool_running`, `tool_ran_keyed`, `tool_dispatched` and `recover_batch`
rows (jsonl and sqlite).
