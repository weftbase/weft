# Running `weft/thread` — the operator's page

What a person who deploys, backs up, restarts and debugs sessions needs
to know. The godoc is the API reference
(`go doc github.com/weftbase/weft/thread`), [ADR 0011](adr/0011-thread-sessions.md)
is the design record and holds the format reference; this page is the
operating rules, each one checked against the code.

`weft/thread` is pre-1.0 and **not frozen**: the API and the stored
format may change between minor versions, and the CHANGELOG says how.
Nothing here is a compatibility promise. What you can rely on today is
how a reader behaves when it meets something it does not understand:
it fails loudly (§7), it never guesses.

## 1. What is on disk

| | `thread/jsonl` | `thread/sqlite` (the one package that imports the SQLite driver) |
|---|---|---|
| Layout | one directory; one `<id>.jsonl` file per session | one SQLite database file holding every session |
| A session | the header line, then one JSON line per entry, append-only | the same line bytes, one per row (`sessions.header`, `entries.line`) |
| Permissions | files `0600`; the directory `0700` when `Open` creates it (an existing directory keeps its mode) | the file `0600` and its directory `0700` when `Open` creates them |
| Side files | none | `<file>-wal` and `<file>-shm` beside the main file (WAL mode, set once on the file) |
| Lock | an advisory lock on the session file | a row per session in `session_locks` |
| Schema | the format itself | embedded migrations, tracked in `thread_migrations`; a database from a newer weft fails `Open` with `sqlite.ErrNewerSchema` |

`thread.Memory()` keeps sessions in a map and loses them with the
process; it is for tests.

A session id is 1–128 letters, digits, `_` or `-` (`thread.ValidID`),
so it is always exactly one path component. Read a jsonl session with
`jq -c . <dir>/<id>.jsonl`; the entry kinds and their fields are in
ADR 0011's format reference.

Both backends store the same lines, so they differ in where the bytes
live, never in what they say.

## 2. Durability

- **jsonl** fsyncs every `Append` before it returns (and the file and
  directory at `Create`). With `thread.FsyncOnFlush()` the fsync moves
  to the turn's end — the session flushes the prompt before the run
  and again when the turn lands — which is cheaper and can lose the
  unsynced tail of a turn in a crash, never a synced one.
- **sqlite** commits every `Append` as one transaction, in WAL mode,
  under `synchronous=FULL` whatever the fsync policy: the log is
  fsynced at every commit, so an entry survives a power cut as well as
  the process dying, and a failed sync fails the `Append`.
  `thread.FsyncOnFlush()` is accepted and changes nothing: SQLite has
  no sync point a `Flush` could force without a checkpoint, and a
  passive checkpoint a reader holds back syncs nothing — a dispatch
  record must be durable before its handler starts (ADR 0032).
  (Before, `FsyncOnFlush` was `synchronous=NORMAL` and `Flush` a
  passive checkpoint.)
- An `Append` batch is atomic against the writer's death: after a
  crash a load returns all of it or none of it.

## 3. Backups

**jsonl.** `cp` (or rsync, or a filesystem snapshot) of the directory
is a backup, at any time, with writers running. A file copied while
its writer is mid-append can end in a half-written line; the copy
still loads — the torn line is dropped and reported (§6) — and it is
missing at most that one batch. Restore by putting files back. The
advisory lock is not in the file, so a restored copy is unlocked.

**sqlite.** The main file alone is **not** a backup while the
database is in use: committed data can still be in `-wal`. Use
SQLite's own online backup (`sqlite3 sessions.db ".backup out.db"`, or
`VACUUM INTO 'out.db'`), or stop every writer and copy the main file
together with `-wal` and `-shm`. One more thing travels with a sqlite
backup: the `session_locks` rows of whichever writers held sessions
when it was taken. Restored on the same host they belong to a dead
process and are taken over on first write (one `Warn` line each).
Restored on a host with a different hostname they are never taken
over (§4) — clear the table after such a restore.

Neither backend has export, import or encryption at rest; encrypt the
volume.

## 4. Locks and leases — one writer per session

There are two layers, and they answer different questions.

Neither, nor the backend or its durability, is recorded: Studio's
session page shows a `not_recorded` storage badge unless the app names
them itself on every turn —
`thread.WithRunMetadata(map[string]string{"storage": "jsonl · per-step durable"})`
— which the page then shows as its storage line, a first clue when a
write fails with `ErrLocked`.

**The backend's lock: which `Storage` value, in which process, may
write.** It is taken on a session's first write (`Create`, `Append`,
or a Session's first write) and held until the session is released,
deleted, or the process dies. A second writer gets `thread.ErrLocked`,
naming the session. Readers never lock: `Load`, `List`, `Watch`,
`thread.Open` and every read of a `Session` always work.

- jsonl: `flock` on unix, `LockFileEx` on Windows, on the open session
  file. The kernel drops it when the holder exits, however it exits —
  a crashed writer never strands a session. The price is one open file
  descriptor per session a process is writing, until that session is
  closed; size your fd limit for the sessions you hold open at once.
- sqlite: the lock row names its holder by `Storage` instance, host,
  a random per-process token, pid and process start time, and a later
  writer decides from those whether the holder is dead:
  - the same process (another `Storage` value in it): alive,
    `ErrLocked`;
  - this process's own pid under another process token — a restarted
    container that is PID 1 again: an earlier incarnation, taken over;
  - a pid that is not running, or one that is running but started at a
    different time than the row records (the pid was reused): dead,
    taken over;
  - a holder with a **different hostname**: never judged dead from
    here, `ErrLocked`. The start time is read from `/proc` on Linux
    and from the kernel on macOS and Windows; on other unixes it is
    unknown, and an unrelated live process wearing a dead holder's pid
    keeps the session locked until it exits.
  A takeover is logged as one `Warn` (through `thread.OpenLogger`,
  `slog.Default()` otherwise).

**The lease: which `Session` value may write.** Two `Session` values
on one `Storage` value look identical to the backend, so a Session
names itself: it takes the session's lease with its first write
(`Create` and `Fork` are first writes) and keeps it until
`Session.Close`. Meanwhile another Session on the same `Storage` value
opens and reads normally and every write it makes fails with
`ErrLocked`, changing nothing. A Session that was opened before
another writer appended — and so would attach to a leaf the session
has moved past — fails its write with `thread.ErrStale` instead.

Consequences for a deployment:

- **Upgrading from 0.21: drain the 0.21 workers first.** This version
  writes every assistant step that calls a tool as format 6
  (`dispatch_recorded`, and a `dispatch` record per call that is not
  `ReplaySafe`). A 0.21 reader refuses format 6 (`ErrNewerFormat`), so
  once a session has run a tool under this version, a 0.21 worker can
  no longer open it — by design: it would answer a cut-off call
  "interrupted". In a rolling deploy, stop routing sessions to 0.21
  workers (drain them: let their turns end, `Close` their sessions)
  before the first worker of this version takes traffic; never run the
  two side by side on one store. A rollback to 0.21 strands every
  session this version touched with a tool call.
- **Do not relabel a tool's replay class while sessions are cut off
  or parked on it.** Recovery reads a recorded call's class from its
  dispatch record (the class it started under), but a `ReplaySafe`
  call is not recorded: relabeling a `ReplaySafe` tool, dropping it,
  or removing the `ToolSource` (an MCP server) that served it makes
  its cut-off call read "cut off before it started" (ADR 0032). Change
  replay classes once the sessions using the tool are settled.
- **Containers.** A restarted container that keeps its hostname
  reclaims its sqlite sessions on its own (same pid, new process
  token). A *replacement* container with a new hostname — the default
  for a new pod or `docker run` — does not: the old rows read as
  another host's. Give session writers a stable hostname, or close
  sessions on shutdown (§5) so no rows are left. Once the old host is
  known to be gone, `sqlite.BreakLock(ctx, st, session)` removes a
  session's lock row; the same applies to a database restored from a
  backup that carried its lock rows. You vouch that the holder is
  gone: nothing can check it. A holder that was alive after all is
  stopped at its next write, which finds the row is no longer its own
  and fails with `ErrLocked`.
- **Several hosts.** jsonl's lock is only as good as the filesystem's
  `flock` (network filesystems vary; some lie). A sqlite database on a
  shared filesystem is outside SQLite's own supported envelope. Run
  one writer host per directory or database; readers elsewhere are
  fine on jsonl.
- **`thread.NoLock()`** opens jsonl without the advisory lock — for a
  platform that has none (there `jsonl.Open` fails without it) or a
  filesystem whose locks cannot be trusted. One-writer-per-session
  then becomes your promise: two processes writing one session will
  interleave lines. sqlite and Memory accept the option and keep
  locking.
- **Stale detection is a count plus the header's creation time.** A
  Session compares the number of complete entry lines the storage
  holds with the number it has seen, and the stored header's `Created`
  with the one it loaded. It catches a writer that appended behind it
  and a session deleted and re-created under the same id; it is not a
  content comparison of the entries.
- `s.Claim(ctx)` asks the lease question ahead of a write: it takes the lease for this Session (nil for the holder), or fails `ErrLocked` / `ErrStale` / `ErrClosed` and changes nothing — on `ErrStale` the lease the check took is given back, so the next Session opened on the session can claim it — weftbase/runtime's `Sessions` asks it before acking a debugger's decision. A Claim with no later write still holds the lease (jsonl's `flock`, sqlite's lock row) until `Close`, as one that wrote does: claim only with a Close to come.
- `s.Steer(ctx, runID, msgs...)` is the steer for a caller that does not own the turn (the devtools debugger): delivered into the turn running now when it is `runID` (`""`: any running turn), exactly as `Send(…, As(Steer))` on a busy session; `ErrNotRunning` and nothing written when none is — idle, between turns, parked, or a session reopened from a crash mid-turn (its prompt is durable, but nothing runs in the reopened value) — or when the running turn is another run (the error names both). A delivered steer that misses its drain point (StopWhen, a park, the run ending first) becomes a follow-up turn under ADR 0019's rule. A multi-message Steer whose later append fails has accepted the earlier messages.
- **A Session someone forgot to close** holds its lease for the life
  of the process. `st.(thread.Releaser).Release(ctx, id)` ends the
  hold whoever has it — the escape hatch, to be used only when that
  Session is truly abandoned (its next write is refused once another
  writer has written).
- The Windows paths (the `LockFileEx` lock, process liveness) compile
  and pass `GOOS=windows go vet`, run by hand — CI has no Windows job
  — and have never been executed by a test. Treat Windows as untested.

## 5. Close and shutdown

`Session.Close(ctx)`, in order: `Send` and `Continue` fail with
`ErrClosed` at once; Close waits for the running turn and the queue
behind it to drain; the Session is sealed (every later write is
`ErrClosed`, reads keep answering); the lease and the backend's hold
are released, and with them jsonl's file descriptor and sqlite's lock
row.

If `ctx` ends first, Close cancels the running turn — it is recorded
as canceled — ends the queued turns with `ErrClosed`, and returns
`ctx.Err()`. The canceled turn may still be writing its end: call
`Close` again to finish sealing and releasing.

**A worker that is stopping** gives Close a context whose cause is
`thread.ErrShutdown` (ADR 0032):

```go
ctx, cancel := context.WithTimeoutCause(ctx, d, thread.ErrShutdown)
defer cancel()
_ = s.Close(ctx)      // drain; at the deadline the turn is felled with ErrShutdown
_ = s.Close(graceCtx) // let its end land, seal, release
```

The same drain, but a turn still running at the deadline is recorded
**cut off** — `TurnEntry.CutOff`, not `Canceled`; `Turn.Wait`'s error
wraps `ErrShutdown` — and a call that is not `ReplaySafe` whose handler
the cancellation felled (or that never got a result) stays unanswered,
without synthetic text. The next process finds the turn
(`s.Ready()`: `ReadyRecover`) and handles it with `s.RecoverTurn(ctx)`
(§6). A turn whose own context is canceled with
`cancel(thread.ErrShutdown)` gets the same record.

What Close does not lose: sends still queued when it gives up keep
their `accepted` receipts in the file, steers their `queued` ones, and
the next `Open` restores both to the queue (§8).

A shutdown sequence that leaves nothing behind:

1. Stop accepting requests.
2. `pool.Close(ctx)` for each pool: running children are canceled and
   settle their receipts, parked ones stay parked for the next
   process — except a sync child parked under a pool child that is
   itself being canceled (running, waiting on its delegations), which
   is canceled at its boundary with it: nothing could resume it under
   a parent that settles. Close also waits, at most 30 s, for a parent
   session's turn still running a sibling call to park the delegating
   call of a child it canceled, then resolves it. Session.Close does
   not wait for pool children. A ctx already canceled with the cause
   `thread.ErrShutdown` cuts running children off instead: their
   turns are recorded cut off and their receipts stay unsettled for
   `pool.Recover` after the restart, which recovers each such child
   on its own ladder (`RecoverTurn`) rather than settling it failed:
   its calls of unknown outcome reach the parent as mirrored recovery
   requests, or it runs on to its answer.
3. `s.Close(ctx)` for each open session, with a deadline you can
   afford and the cause `thread.ErrShutdown` (above).
4. Exit.

Skipping it is survivable — that is what the crash rules are for —
but it costs the in-flight turn, and on sqlite it leaves lock rows for
the next process to take over (§4).

Never call `Close` from inside the session's own run (a tool, a hook):
it would wait for the turn that is calling it.

## 6. After a crash

**The torn tail.** A writer killed inside an append can leave a final
line without its newline. Nothing needs to be done by hand:

- a load drops it and says so — `LoadReport.Torn` from
  `Storage.Load`, `Session.LoadReport()` on an opened session, and one
  warning through the agent's logger;
- the next writer removes it before appending (jsonl truncates to the
  last newline and fsyncs; sqlite deletes the torn row) and logs one
  `Warn`;
- a live tail (`Watch`) treats it as a write in flight and waits.

So a crash costs the half-written batch and nothing after it.

**A damaged line elsewhere** fails the load with `thread.ErrCorrupt`
(a `*thread.CorruptError` naming the session, the line and, when the
tree is the problem, the entry). Opening the backend with
`thread.Salvage()` skips such lines instead and reports them
(`LoadReport.Skipped`); entries whose parent was on a skipped line are
kept and listed (`OpenReport.Orphaned`), and the model's context for a
leaf below one starts at that entry — everything before the skipped
line is off the path. Salvage is a recovery tool: open with it, read
`s.LoadReport()`, decide whether the session is still usable. It never
skips data from a newer weft.

**A file with no complete header line** (a crash inside `Create`) is
`ErrCorrupt` on line 1 and is not repaired; `thread.Delete` it and
create the session again.

**The cut-off turn** (ADR 0032). Its prompt and every message its steps
emitted are in the file; its turn entry is missing. The run id is not
reused (it is on the prompt entry). No call that is not
`weft.ReplaySafe` ever starts before the step entry holding it is in
the file (rule A: otherwise it fails closed with `weft.ErrNotDispatched`
— a storage error, or `ErrLocked`/`ErrStale` because another process
took the session over), and none starts before its own **dispatch
record** is in the file too (an `approval_audit` entry, step
`dispatch`, appended and flushed by the gate). So the file names every
side effect that may have run — and every call that never started: a
call a park rule (`ParkSource`, `ParkOn`, `ParkAllExcept`) or a
middleware held in the same step as a call that ran has no dispatch
record. A call with a dispatch record and no result has an **unknown
outcome**, and until the turn is recovered `Send`, `Continue`,
`Resume` and every path that would arm a resume (`Decide`,
`DecideSigned`, the pool's replay) refuse with
`thread.ErrRecoveryRequired`. A cut-off turn whose unanswered calls are
all `ReplaySafe` is not refused: the next turn's input repair answers
them.

Boot recovery is **Open → Claim → RecoverTurn**: `Open` takes no lock,
so a turn another worker is running reads exactly like a cut-off one;
`s.Claim(ctx)` takes the lease (`ErrLocked`: it is alive elsewhere —
leave it). `s.RecoverTurn(ctx)` then handles the turn once:

| The turn was cut off… | `RecoverTurn` |
|---|---|
| in a model call, between steps, or after a resume's results landed | closes it (`TurnEntry.CutOff`) and runs a continuation from the last finished step, no new prompt, a fresh run id |
| with its last step's calls unanswered, all `ReplaySafe` | drops that step (the leaf moves back before it) and continues — the model is asked again |
| with its last step's calls unanswered, some not | parks them: a call with no dispatch record never started and parks as an **ordinary** request (`cut off before it started: …`, or `tool requires approval`) — only a person runs it; a dispatched `weft.ReplayKeyed` call is approved by the session itself (Via `recovery`) and retried with its same `IdempotencyKey` (at most 3 times across restarts, then a person); a dispatched `ReplayNever` call asks its tool's `Reconcile` (found: its result is recorded; missing: it runs again under its key, one of the session's 3 automatic retries — past them a person decides), else parks as a **recovery request** (`Request.Recovery`) — `Approve` retries it with the same key, `Deny` or `Resolve` skips it, an edit or `ApproveAlways` is refused; a `ReplaySafe` call is not run again (its result says so; the model may ask again). In a step written before dispatch records (0.21) every call that may have run waits for a person |
| during a resume, before its results landed | its approved calls of unknown outcome park again the same way; the rest keep their decisions |
| after its final answer, before its end | writes the end; runs nothing |

The same never-guess rule holds while the process lives (ADR 0032,
WR-4 and WR-6): before each call that is not `ReplaySafe` the writer
checks it still holds the session's lock (`thread.Fencer`: sqlite reads
the lock row, jsonl checks its file) and starts nothing after a
`BreakLock` or a takeover — the run ends with the cause
`thread.not_dispatched`, its error wrapping `ErrLocked`; a call that
reaches the gate after an Interrupt, a Rollback or `Close` felled its
turn never starts; and a call whose handler started but gave
no answer — its `Timeout` passed, or an Interrupt, Rollback or cancel
felled it — is never recorded "timed out" or "interrupted": it parks
for the same ladder at the turn's end (keyed retry, or a recovery
request — never a `Reconcile`, since the abandoned handler may still be
running), the step's other results kept. The session's own keyed
retries of one call are capped at 3 (on file), each after a backoff
of 1 s, 2 s, 4 s; then a person decides. No retry — the session's own
or a person's — ever starts while the abandoned handler of the same
call is still running: the end of the turn waits for every such
handler to return, under one shared 30 s deadline, before it retries
or offers the call; still running past that, the call goes to a person
(`… still running past its bound; approve to retry it … once it has
returned …`) and an `Approve` is refused with `thread.ErrStillRunning`
until the handler returns (retry it then; `Deny` is taken at once).
Skipping a recovery request (`Deny`) shows the model
`OUTCOME_UNKNOWN: … it was skipped, not retried: do not repeat it`,
never `DENIED`; and `Resume` refuses with `ErrRecoveryRequired` while a
recovery request is undecided — its default would deny it `no
decision`, a `DENIED` the model would answer by asking again.

**An Interrupt over a call with effects waits for a person.** An
`Interrupt` (or a cancel) that fells a turn while a handler with
effects runs leaves that call's outcome unknown: the felled turn parks
it as a recovery request, its `Wait` error matches
`ErrRecoveryRequired` (beside `context.Canceled`), and the Interrupt's
own turn waits in the queue behind the request until someone decides
it in `s.Pending()` — unbounded, by design: the session never guesses
whether the effect landed. Watch for `ErrRecoveryRequired` on the
felled turn (or `OnRequest`), and bound your own wait with
`turn.WaitContext`.

A `Send`'s own run options are not on file: a continuation or a
recovery resume runs under the session's run metadata and the
boundary's recorded settings. A call a per-Send `ParkOn` held before
the crash never started and parks again as an ordinary request; a call
the model asks for in the continuation is held only by the tool
(`weft.RequireApproval`), the agent (`weft.ParkSource`) and middleware. Recovery is audited
(`Audit()`: a `recover` step per run, a `recover`/`retry` step beside
each of the session's own approvals) and the session file is written in
format 6 from then on (a reader before 0.22 refuses it).

**Accepted input that never ran** is back in `s.Queue()` after `Open`
(§8). `Open` never runs anything on its own.

**Pool delegations** a dead process left unsettled: §10.

## 7. Errors: what to do with each

Match with `errors.Is`.

| Error | Class | What it means | What to do |
|---|---|---|---|
| `ErrBusy` | retry | a turn is running (a `Reject`-policy `Send`, `Branch`, `Compact`, `ApplyCompaction`, `Uncompact`) | wait for the turn (`turn.Wait`, `s.WaitIdle`) and call again, or send under `Queue` |
| `ErrLocked` | retry | another writer holds the session — another process, another `Storage` value, or another `Session` that has not closed | retry after that writer closes; if it is dead on sqlite see §4 |
| `ErrStale` | reopen | the stored session moved on behind this Session | `thread.Open` it again and redo the write |
| `ErrClosed` | reopen | this Session's `Close` has run | `thread.Open` it again |
| `ErrNotFound` | terminal for the call | no such session (or it was deleted under an open Session) | — |
| `ErrExists` | terminal for the call | `Create` over an existing id | pick another id |
| `ErrCorrupt` | terminal for the data | a line this build cannot decode, or a tree that does not hold together | inspect the line named; reopen the backend with `thread.Salvage()` to read around it |
| `ErrNewerFormat` | terminal for this build | written by a newer weft: an unknown entry kind, a newer `"v"`, a newer header envelope | upgrade the reader; Salvage does not skip it. `List` cannot see a session whose *header* is newer |
| `ErrAwaitingApproval` | wait | compaction while approval requests are pending | decide them first |
| `sqlite.ErrNewerSchema` | terminal for this build | the database was migrated by a newer weft | upgrade |

How a turn ended is on `turn.Outcome()`, and the error `turn.Wait()`
returns says why: a `*weft.RunError` (the run started and failed — the
partial transcript is in `.Result`), or an error wrapping `ErrNotRun`
(no model was called), `ErrDropped` (`ClearQueue` removed it),
`ErrClosed`, `ErrTurnPanicked`, or `ErrNotPersisted` — the run ended
but its end could not be written; treat that as a storage incident,
the turn's usage is in no ledger. A turn entry with `late_steps > 0`
means step writes failed and landed late: nothing was lost, and the
storage was unhealthy for that long.

The decision errors (`ErrNotPending`, `ErrExpired`, `ErrDelegated`,
`ErrInvalidDecision`, `ErrSignatureRequired`, `ErrBadSignature`,
`ErrReplay`, `ErrArgsChanged`) each refuse one decision and record
nothing; godoc says which call returns which.

## 8. The queue

A `Send` that meets a busy session is, by default, queued — and
durable: an `accepted` receipt entry is written and flushed before
`Send` returns. A steer writes a `queued` receipt the same way. After
a crash or a `Close` that gave up, `thread.Open` restores both to
`s.Queue()`; nothing runs until the application says so:

- the next `Send` runs behind the restored sends, in order, and a
  restored steer is delivered into the first turn that runs;
- `s.Continue(ctx)` runs them now, with no new message;
- `s.ClearQueue(ctx)` drops them, on the record.

**A failed `Send` of a message that will not run leaves the line as
it found it.** When the prompt entry or the `accepted` receipt is
written but its flush fails — or an `Interrupt`'s denial of the parked
calls cannot be recorded — `Send` returns the error, the receipt is
settled `dropped` (a reopen does not run it) and a `leaf` entry moves
the leaf back to where it stood, the two in one append, flushed. That
is best effort: if the append is refused too, the in-memory leaf still
moves and a reopen restores the send from its undropped receipt (the
log says so). The idle path and the queued one read the same after the
failure, so it does not matter which one a `Send` took — and the one
right after a turn's `Wait` can take either: `Wait` returns once the
turn's entries have landed, while the runner is still between items
(the automatic compaction runs there), and a `Send` in that window is
accepted and queued behind it (after a turn that parked, the approval
boundary holds the session instead and the policy's boundary rules
apply). Call `s.WaitIdle(ctx)` first when the next `Send` must find
the session idle.

A steer is the exception: a `Steer` whose `queued` receipt was written
but not flushed returns the error, yet the receipt is on disk and a
reopen would restore it, so the live session matches — the steer
defers to a follow-up turn and runs. A steer whose deferral at its
turn's end was written but not flushed keeps it the same way: its
`Send` succeeded, the follow-up runs under the turn its `accepted`
receipt names, and a reopen before it ran restores it, never after. A
deferral refused outright — nothing written — runs from memory too,
and the steer's `queued` receipt is what a reopen sees: the follow-up's
prompt is appended together with the `deferred` receipt that settles
it, so a reopen before the prompt landed restores the steer once, and
one after restores nothing. A steer the running turn drained
is in that run's transcript at once, its `delivered` receipt at the
turn's end: after a crash between the two, `Open` reads the message on
the line as the delivery and does not restore the steer. The live
session reads it the same way when the storage refuses the turn's end
batch: a drained steer whose message is already on the line ends
delivered, its `delivered` receipt written on its own, not deferred
into a second copy.

**Steer follow-ups inherit.** A steer that cannot join the running
turn becomes a follow-up turn under that turn's run options (and the
values on its context), the steer's own options after them — a turn
sent with `weft.ParkAllExcept` keeps its follow-ups parked. A steer to
an idle session runs under its own options. Restored messages run
under what their receipts recorded as data (`ReceiptEntry.Run`, a
`core.RunSettings`: the park rule, `OnlyTools`, lowered budgets,
overrides, metadata); `UseModel` and `OnMessages` are code and do not
come back.

**One resumer: `OnReady`.** A platform that must decide when work runs
— admission, slots, budgets in its own queue; a desktop app's job
runner — opens every session with `thread.OnReady(fn)`. The session then
starts nothing nobody asked for: a decision that settles an approval
boundary does not resume it (`AutoResume` is off), and the sends queued
behind a turn are not drained when it ends. Instead the session calls
`fn(thread.Ready{Session, Kind, RunID})` — after the append that made
the work ready, on a goroutine of its own, never under its lock, in
order — and the owner starts the work when it admits it:
`s.Resume(ctx)` for `thread.ReadyResume`, `s.Continue(ctx)` for
`thread.ReadyQueued` (one queued send per call; the next is handed over
when it ends). A `Send` on an idle session still runs at once. A hint
is in-process and best-effort, not a durable event: it may come twice,
the work may be gone when the owner acts (`Resume` then fails with
`ErrNotPending`), and it dies with its process — whether the work it
names survives a power loss is the backend's append guarantee
(`FsyncOnFlush`). Work that was ready before the Session existed (a
boundary decided before a crash, sends restored by `Open`, a cut-off
turn) is never handed over: boot recovery is **Open → Claim → Ready →
act**. `s.Ready()` reads the journal — `ReadyRecover` alone when a turn
was cut off (`RecoverTurn` first, §6), `ReadyResume` for a boundary
whose requests are all decided or lapsed, `ReadyQueued` for waiting
sends — and the `Claim` (`ErrLocked`: alive in another worker, leave it)
keeps two workers from recovering the same session. Without `OnReady`,
`AutoResume(false)` keeps its contract and `s.Ready()` still lists a
decided boundary. Discovering which sessions have work without opening
each one needs a store-level index — not in weft yet.

**The queue is unbounded.** The session does not cap how many steers
or sends may wait; each costs one entry. If users can pile messages
onto a running turn, check `len(s.Queue())` before sending.

## 9. Costs and limits

Numbers are from the budget tests, measured on the maintainer's
machine on the dates given; CI enforces generous bounds around them,
not the numbers themselves.

- **Opening a session reads all of it** into memory, and a `Session`
  holds its whole tree. A 100,000-entry session opens in about 3 s on
  both backends (2026-09-30).
- **`List` on jsonl reads every header in the directory on every
  call** — the directory is the index. A page over 10,000 sessions
  takes about 70 ms (2026-10-01) and allocates a header's worth per
  file. Cost grows with the fleet, not the page. A header line longer
  than 1 MiB is not listed.
- **`List` on sqlite pages in SQL**: the page is an index range read
  and `Total` a count over the same index; walking 10,000 sessions a
  hundred at a time takes about 0.17 s in all (2026-10-01).
- **`Query.TitleSearch`** costs more than the rest: jsonl reads each
  candidate session's whole file to find its title; sqlite scans a
  title column.
- `Query.Meta` matches the header's create-time metadata only — a key
  added later with `SetInfo` never matches — and a key must be present
  to match.
- `List` skips what it cannot decode (a torn or foreign header, a
  newer envelope) without an error; `Load` on that id says why.
- **Paging**: newest first, 50 per page by default, 500 at most. Pass
  the last session's `Created` and `ID` as `Before` and `BeforeID`;
  `Before` alone skips sessions sharing that creation time.
- **A Session's lock is held across its storage writes.** With the
  default fsync policy every method of that Session — reads included
  — waits out an fsync in progress.
- **`Watch` polls** every 200 ms and reads only what is new.
- **`Delete` does not look for open Sessions.** Close first; a turn
  running at that moment loses the entries it had yet to write.

## 10. Pool operations

- **A delegation is the model's call.** Under a session a wrapped tool
  reached through a handler's `CallTool` is refused before any child
  exists (`pool.ErrNestedDelegation`); give the model the wrapped tool
  itself.

- **One pool per storage's delegations.** `Recover` treats an
  unsettled receipt that no child of *this* pool is running as left by
  a dead process. With two live pools over the same sessions, a
  `Recover` or `Cancel` through one settles the other's running work
  as failed. The exception is several pools of one process delegating
  from one parent session (each its own bound): every pool watching
  the parent is pumped by each decision recorded there, and a pool
  skips the receipts whose child another pool watching that parent
  holds — a live delegation, a `Register`, or the wrap it was made
  under — with no error and no log, in its pumps and its `Recover`.
  A closed pool still holds the children its `Close` is ending (their
  settlements are its to record), never one it left parked; a receipt
  that settled while a pump read it is skipped too. After a restart a pool watches the parent from its first `Recover`
  or `Decide` on it: call every pool's `Recover`; the first one
  reports a parked child of a pool not yet attached with `ErrNoAgent`
  and leaves it for its own pool's. Cancel and Forward a receipt
  through the pool that made it.
- **After a restart**, for each parent session the new process takes
  over: wrap or register the agents first, then
  `p.Recover(ctx, parent)`. It reattaches children parked at an
  approval (mirroring any request the crash left unmirrored), settles
  children that finished from their own files, and settles `failed`
  those that never started their first run. A child whose run a crash
  or a shutdown cut off — its first run, a resume after an approval,
  the session's own keyed retry, or a recovery's continuation that a
  second crash cut off — is never settled `failed`: it is recovered on
  its own ladder (`RecoverTurn`, §6) and its receipt stays open. A
  call of unknown outcome reaches the parent as a mirrored recovery
  request (approve retries it with its same key, deny skips it); a
  `weft.ReplayKeyed` one is retried under its key, at most 3 times
  across restarts, then a person decides; otherwise the child runs on
  to its answer. A child cut off while it was itself delegating is not
  recovered at depth: its own delegations are ended first (a cut-off
  grandchild settles `canceled`, never `failed`), and its delegating
  call — its dispatch record says it may have run — reaches the parent
  as a recovery request (`cut off while it delegated: …`): approve
  delegates again, deny skips it and the child's model reads
  `OUTCOME_UNKNOWN … do not repeat it`. A delegation is a call like any
  other: it passes the approval boundary and the dispatch gate before
  its child session exists, so its dispatch record lands before its
  acceptance receipt. A crash between the two leaves a call that may
  have run and no receipt: `RecoverTurn` parks it as a recovery request
  for a person (never "cut off before it started", never delegated
  again on its own). A child
  whose agent the pool does not hold is reported (`pool.ErrNoAgent`)
  and left for a later call. Limits: a finished child's answer is read
  back as its last assistant text (a structured `Output` answer is not
  recovered as such), and the cycle guard above a rebuilt child
  restarts at that child's own agent — the depth limit still holds.
- **Wrap names are resume keys.** A child made by `p.Wrap`/`MustWrap`
  records the wrap's name; a restarted process that wraps the same
  agents under the same names can resume them. Renamed a wrap, or used
  `Submit`? `p.Register(childID, agent)` before `Recover`.
- **Receipt states** (`pool.Receipts(parent)`): `accepted` (queued for
  a slot) → `running` ⇄ `parked` (at an approval, holding no slot) →
  exactly one of `done`, `failed`, `canceled`, `capped`. A receipt
  that stays `accepted` or `running` across a restart needs `Recover`.
- **Decisions.** `p.Decide(ctx, parent, ds...)` records in the parent
  and queues the children that are now decided; it does not wait.
  `p.Wait(ctx, parent, receiptID)` blocks until that delegation is
  settled or parked again. Decisions taken directly on the parent
  session, expiries, and an interrupting `Send` that denies nested
  requests reach the children when the pump next runs:
  `p.Decide(ctx, parent)` with no decisions is the pump alone and is
  always safe to call. A grandchild's park (lead → child → grandchild,
  each a sync wrap) surfaces on the lead and one decision there
  resumes the chain: the grandchild runs, its answer resumes the
  middle child, whose answer is the lead's tool result — in one
  process and after a restart that wraps all three under their names
  again (before 0.18.1 the middle delegation settled failed with "the
  child's resume did not survive a restart"). That failure is now only
  a restart's whose grandchild has no agent to resume it. A grandchild
  that asks a second approval parks the chain again — the middle child
  parks and the new request is offered on the lead (same namespaced
  call id, a new run id; the earlier decision does not carry over) —
  and each decision there takes the chain one park further — once the
  middle child's other grandchildren are parked too: while a sibling
  still runs its approved call the middle waits on, and parks when it
  ends. `Cancel` of a middle child — waiting on its grandchildren, or
  parked on one — and `Close` of a waiting one end the grandchildren
  first (a parked one is canceled at its boundary and never resumed —
  under `Close` too, since 0.18.1: the middle is ending, not parking)
  and wait for them, so both ledgers end settled. Under a
  `RequestExpiry` a request can lapse in a middle child before the
  middle parks (its step still running another call): the middle then
  waits for the lapsed grandchild's answer instead of parking with
  nothing to offer, so the lead never sees the delegating call itself
  as a decidable request (before 0.18.1 it did, and approving it ran
  the delegation twice). `Cancel` of a parked child returns at once;
  when the delegating call's turn has not parked it yet, the
  resolution follows when it does — `Wait` waits for it. The wait is bounded at 30 s: a grandchild whose
  run has not ended by then — a tool handler that ignores its context
  and has no `Timeout` — is settled `canceled` by the pool in the
  middle's ledger ("its run did not end within 30s of its parent's
  end"), one WARN names it, and its run is abandoned; `Close` still
  waits for that run, and its eventual end writes nothing. Give such
  tools a `Timeout`. A middle child's async children (`Submit`, an
  `Async` wrap) are not ended with it. A `Cancel` of a running child
  that reaches an approval first settles it `canceled` at that
  boundary instead of parking it; one made while the child is
  parking (its request already on the parent's `Pending`, its receipt
  reading `parked`) waits for the park, so the receipt reads
  `canceled` when `Cancel` returns, as for a parked child (before
  0.18.2 it returned at once, the receipt still `parked`).
- **Do not let the parent's decision chain approve a delegating
  call.** A grant or a live `Approver` that matches a pool wrap's tool
  re-runs the delegation when it parks (see `Pool.Decide`'s godoc for
  whether your version still has this gap). Scope grants and the
  Approver away from wrap names.
- **Deleting a parent deletes no child.** Cascade yourself:
  `pool.Descendants(ctx, parent)` returns the whole subtree deepest
  first — delete in that order, the parent last.
  `pool.Children(ctx, parent)` is the direct ones.
- **Cost.** A child's usage is on its receipt and in the parent
  session's `Usage().Delegated` — not in the parent run's
  `RunResult.Usage`. A budget that must cover delegated work reads
  `Delegated`.
- **What the recorder sees.** A child session's turn is a session of
  its own in the records: its own `weft.session.id`, its lineage in
  `weft.session.parent` / `weft.session.parent_call`, and — under a
  `Wrap`ped call — `weft.caller.run.id` / `weft.caller.call.id` naming
  the delegating turn and call, never `weft.parent.*` (that names a
  `core.Subagent` child alone; record contract §2). Studio lists the
  child session with its lineage, opens it from the child's run page,
  and lists it under the parent's session page with the delegated
  usage and cost beside the parent's own (`GET /api/sessions/{id}`'s
  `delegated`, `delegated_usage`).
- **Depth.** `pool.MaxDepth(n)` (default 8) bounds a delegation chain;
  the model reads `SUBAGENT_DEPTH` past it.
- **Default-deny reaches every child.** A parent turn sent with
  `thread.RunOptions(weft.ParkAllExcept(...))` binds its children,
  sync and async, on every turn they run — the first and each resume —
  so a child's side-effect tools park unless named. Name a wrap's tool
  in the list for the delegation itself to run.

## 11. Approvals operations

- **Expiry is the caller's.** weft sets no default request expiry: a
  request waits until it is decided unless the session was opened with
  `thread.RequestExpiry(d)` (lazy: a lapsed request is denied — never
  run — on the next touch). Choose one per deployment; a platform with
  its own timers (Weftbase) runs without it.
- **Deciding is bound and idempotent.** Every `Decide` decision names
  the request it answers — `thread.Approve(r.CallID).Bind(r)` sets
  `RequestID` and `ArgsSHA256` from the `Request` shown; an unbound
  decision is `ErrInvalidDecision`. The same decision delivered again
  (a reconciler re-forwarding what it recorded, a retried request) is a
  no-op: nil, nothing recorded — and the same signed bytes again
  (`DecideSigned`, same nonce) are applied, not `ErrReplay`, and not
  audited. A different decision on a decided request is `ErrNotPending`;
  another one under a spent nonce `ErrReplay`.
- **Edits.** `thread.Edit(callID, args).Bind(r)` corrects a parked
  call: the request closes and a revision opens on the edited arguments
  (`Request.RevisionOf`), approved once by the editor — under
  `Quorum(n)` it still needs n distinct approvers, and approvals of the
  old arguments do not count. The arguments are canonical JSON; the
  resume runs them only while they hash to what was recorded, under a
  key that covers them, and the model's result starts with
  `EDITED: the call ran with the arguments …`. A recovery request takes
  no edit. Re-checking policy on the edited arguments is the decider's.
  A session that holds an edit is format 6: a 0.21 reader refuses it.
- **Cosigners.** An approval service outside the session that counts
  its own quorum forwards one decision naming every decider it counted
  in `Decision.Cosigners` (MAC-covered
  when signed); the session records them and never counts them toward
  its own `Quorum`.
- **Idempotency keys.** Every call a session runs carries
  `weft.Call.IdempotencyKey` = `thread.IdempotencyKey(session id,
  assistant entry id, call id)` (ADR 0031): a tool with side effects
  passes it to an API that deduplicates. The same call keeps it across
  a crash, a restart, a resume on another worker and a recovery retry;
  `Request.IdempotencyKey` names it on a parked call. A call the model
  re-issues under the same id in a later turn sits in another entry
  and gets another key; a fork gets new keys. A `weft.Subagent`'s child
  run does not inherit them: its calls carry run-scoped keys, new on
  every re-run of the child.
- **Keys.** A `thread.Keyring` holds HMAC keys by id; the one active
  key mints new challenges, every key verifies. Keys are never
  written to a session — only key ids are — and a ring is
  configuration: pass it with `thread.WithKeyring` at every `Create`
  and `Open`.
- **Rotation.** Build a new ring with the new key active and the old
  keys still in it, and hand it to sessions as they open. Requests
  already challenged under an old key keep verifying. Drop an old key
  only when nothing minted under it can still be pending; after that a
  decision signed with it is `ErrBadSignature` (the same answer as a
  key that never existed).
- **`RequireSigned`** is stored in the session's header at `Create`
  (`weft.require_signed`) and enforced by every later `Open`, whatever
  options it passes; nothing turns it off. Such a session cannot be
  opened without a keyring that has an active key. Under it the
  unsigned `Decide` is refused; grants, the live Approver, expiry and
  the session's own denials still record. `s.Grant` stays an
  in-process, unsigned call: whoever can call it can pre-approve.
- **Signed decisions are bound** to the request entry and the run that
  parked the call, and are single-use (`ErrReplay`, across restarts).
  A challenge minted before upgrading to this version does not verify
  (the challenge domain moved to v2): request a fresh one.
- **Expiry is lazy.** `thread.RequestExpiry(d)` gives each request a
  lifetime, read against the session's clock. Nothing fires on a
  timer: an expired request is denied, with the stated reason, the
  next time the session is touched — `Decide`, `Resume`, `Send`, the
  runner, the pool's pump, or `s.Request` for the lapsed call (it
  still answers `ErrExpired`, and under `RequireSigned` it is how a
  signer's client sweeps it). An idle session holds its lapsed request
  until then.
- **Quorum.** A signed approval counts as its key; an unsigned one as
  whatever `Who` the caller wrote. A quorum that must hold against the
  deciding process itself needs `RequireSigned` and one key per
  approver.
- **The audit trail is an index, not evidence.** `s.Audit()` returns
  every request, chain step, decision, grant and revocation the
  session recorded. The entries are plain appended lines, unsigned and
  unchained: whoever can write the storage can change them. If you
  need tamper-evidence, put it in the storage (an append-only volume,
  a signed export). Refused signed decisions are recorded, at most 16
  per request.
- A session file written before this version counted grant uses in
  prose; those uses are not counted any more, so a `MaxUses` grant in
  such a file starts counting again.

## 12. Retention

There is one retention operation: `thread.Delete(ctx, st, id)`. It
removes the session and all its entries; nothing else in the module
removes data — compaction, branching, `Uncompact` and `ClearQueue` all
append. There is no TTL, no archive tier and no per-entry redaction;
build them on `List` + `Delete` (and `pool.Descendants` for children).
A session grows for as long as it is used, and so does the cost of
opening it (§9).

## 13. Observability

Every run a session starts carries these as run metadata — on every
span and every log record of the run (ADR 0024):

| Key | Value |
|---|---|
| `weft.session.id` | the session id |
| `weft.turn` | the turn counter the run id `<session>-t<n>` was minted from |
| `weft.public_id` | the session's public id, when created with `thread.PublicID` |
| `weft.session.forked_from` | `<session>#<entry>`, on a fork |
| `weft.session.parent`, `weft.session.parent_call` | the parent session and delegating call, on a pool child |
| `weft.turn.parent` | the turn whose line this turn continues, on the first turn after a navigation (`Branch`, `Uncompact`, a rollback) took the leaf off the newest line; `0` when that line holds no turn |
| `weft.session.branch` | the branch the turn's line descends from: the label on the entry the navigation went to, else the navigation's entry id |

The session's keys win over a caller's colliding
`thread.RunOptions(weft.Metadata(...))`; other keys pass through. A
key the session has no value for is set to `""`, which the core reads
as none: a session's turn sent from inside another session's tool
handler (or a pool child's turn) never carries the enclosing run's
public id, fork origin or pool lineage as its own
(`docs/record-contract.md` §2).

A session's own tags ride every turn with `thread.WithRunMetadata(kv)`:
written into the header at `Create` and `Fork` (`Header.RunMeta`; a fork
takes its origin's under its own), held over the stored pairs for an
`Open`'s life, stamped under the identity keys above (which win) and
under a `Send`'s own `thread.RunOptions(weft.Metadata(...))` (which wins
per key) — a tenant or a batch tag on every turn without threading a
context through each `Send`. The pairs a run starts under are recorded
with what waits on disk: a parked request and a queued receipt keep
the session's run metadata merged under the send's own (`.run.metadata`),
so a reopen by a process with other run metadata — another worker —
reads them back (`Request.Metadata`) and the resume or the restored
send runs under them, the recorded pairs winning. A `thread/pool`
child runs under its parent's run metadata. A caller puts its own
labels here under its own keys; weft never reads them.

The compaction marker: every compaction that lands (threshold,
`Compact`, `ApplyCompaction`, a trim, the overflow re-run) emits a
record of kind `compaction` (event name `weft.compaction`, ADR 0028 §8)
through the agent's `LoggerProvider`, at once, under the id of the last
run that produced the compacted context — on that run's span, with its
metadata, when this Session drove it — so a compaction followed by
`Close` is reported. It carries `weft.compaction.scope = session`, the
compaction entry's hash, and a body with the reason, the messages
replaced → summary entries and the estimated tokens before → after; no
messages. A compaction of a context no run produced (entries appended
directly to the store) is emitted under the next run the Session
drives, or at `Close` as the state log's records held for no run are:
on a context carrying no span, its body `held: true`, filed under no
run (no `weft.run.id`), keyed by the session's metadata alone. `obsdb`'s
`DB.Compactions` reads it back for Studio. Its body also names the
summary's cost (`summarizer_model`, `summarizer_usage`), and the
summarizer's model call itself — every compaction's and every
`SummarizeLeft` branch summary's — is a `chat` span with
`weft.chat.role = summarizer` and its usage, filed under the turn in
flight or the last run (a compaction the pre-run trigger makes waits for
that turn's run to start and opens under it; one whose run ends, or
the Session closes, before that run reports is dropped with a Debug
line, never opened as a trace of its own), never folded into that
run's own usage: `obsdb`'s session row counts it apart, as
`SummarizerUsage`.

The state log, as records. Beside the marker, the session reports every
state fact it writes as a record of kind `thread` (event name
`weft.thread.<kind>`, `docs/record-contract.md` §4): approval requests,
decisions (with `via`, the key id, the grant that decided) and refused
signatures; grants minted, used (with the use count), revoked and lapsed
(`MaxUses` reached, or an `Expiry` the chain noticed passed); the busy
policies' receipts (a steer queued, delivered, deferred or dropped; a
send accepted, with the turn it was minted as — and a queued steer's
turn number too, the one no run will carry); interrupts (the turn
that felled the run) and rewinds (a rollback, an overflow re-run's
branch-back); branches (with a summary's cost), uncompacts, labels,
pins, forks; and `thread/pool` delegations and their settlements. Each
is filed under the run it concerns — the run the entry names, else the
turn in flight, else the last run on the leaf's line; with none yet
(grants minted before the first turn), or while the turn in flight has
not reported, held for that run; what is still held at `Close` goes out
then, marked `held` — with no run, filed under none and keyed by the
session id. The session decides each record under its lock and one
emitter goroutine per Session hands it to the provider, in order, so a
slow or blocking destination never holds the session (`Pending`,
`Entries`, `Decide` stay prompt); `Close` waits for the emitter, bounded
by its context. Ids, enums and counts always; who decided,
reasons, a steer's text and a summary only where content is on (the
agent's `weft.Content`, else the destination's policy — a content-off
destination receives the record without them); a label's name is
metadata and always rides. A resumed call's `execute_tool` span names
who approved it — `weft.tool.approved_by` `user`, `grant` (with
`weft.grant.id`), `approver`, `quorum` or `policy` (`weft.ApproveBy`).
The file stays the record: the exported copy
is an index, not evidence (ADR 0021, amendment 2026-10-19), and an Observability
destination that drops one changes nothing the session does.

What the module logs on its own. Through the agent's logger
(`weft.Logger`): `Warn` for a repaired load at `Open`, a step write
that failed and was held, a compaction that failed or fell back;
`Error` for what could not be recorded (a turn end, a rollback, a
receipt) and for a contained panic in a hook; `Info` for each
compaction that lands. Through the backend's logger
(`thread.OpenLogger`, `slog.Default()` otherwise): `Warn` for a torn
tail removed and a lock taken over. No `Warn` or `Error` line from
`thread` means the storage took every write the session made.
