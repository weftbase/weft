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
with the same idempotency key, deny to skip it` — no retry is counted.
The wait holds the turn's end (a `Close` waits for it, within its own
deadline). After a crash no handler is left running, and nothing
waits.

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
result and its delegation is over: the acceptance receipt naming the
call is the delegation's dispatch record, so the call may have run.
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
