package thread

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/weftbase/weft/core"
)

// Crash recovery (ADR 0032). There is no per-call "started" marker: the
// step entry that holds a call is its dispatch record. The rules:
//
//   - A. A call that is not core.ReplaySafe never starts unless the
//     step entry holding it is durably appended (dispatchGate).
//   - B. After a crash, such a call with no recorded result has an
//     unknown outcome: it may have run.
//   - C. A cut-off core.ReplayKeyed call is retried on its own, with
//     its same idempotency key; a core.ReplayNever call parks as a
//     recovery request.
//   - D. A recovery request is approved (retry, same key) or denied or
//     resolved (skip); an edit and a standing approval are refused.
//   - E. Every recovery decision is recorded and audited like any.

// The model-visible request reasons RecoverTurn parks cut-off calls
// with, and the texts it records (pinned bytes, ADR 0032).
const (
	reasonOutcomeUnknown = "cut off: the call may have run; approve to retry it with the same idempotency key, deny to skip it"
	reasonRetryKeyed     = "cut off: the call deduplicates on its idempotency key and is retried"
	reasonSafeNotRerun   = "cut off: the call is ReplaySafe and is not run again; the model may ask for it again"
	reasonNotStarted     = "cut off before it started: approve to run it, deny to skip it"
	reasonDelegated      = "cut off while it delegated: the delegated work may have run; approve to delegate it again, deny to skip it"
	reasonRetriesSpent   = "outcome unknown: the session's automatic retries are spent; approve to retry it with the same idempotency key, deny to skip it"
	reasonStillRunning   = "outcome unknown: the call's handler is still running past its bound; approve to retry it with the same idempotency key, deny to skip it"
	reasonReconcileFound = "cut off: the system of record reports the call took effect; its result is recorded"
	reasonReconcileMiss  = "cut off: the system of record has no such call; it runs again with the same idempotency key"
	cutOffTurn           = "thread: turn cut off: the session's writer stopped before the turn ended"
	recoverContinued     = "continued"
	recoverParked        = "parked"
	recoverResumed       = "resumed"
	recoverClosed        = "closed"
	recoverRetry         = "retry"
	reconcileFound       = "reconcile_found"
	reconcileMissing     = "reconcile_missing"
	reconcileError       = "reconcile_error"
	viaReconcile         = "reconcile"
	reconcileTimeout     = 30 * time.Second
)

// safeNotRerunResult is the model-visible result of a cut-off
// ReplaySafe call RecoverTurn does not run again (pinned bytes, ADR
// 0032).
func safeNotRerunResult(tool string) string {
	return fmt.Sprintf("tool call %s was cut off before it answered and was not run again; call it again if you still need its result", tool)
}

// skippedResult is the model-visible result of a recovery request a
// person skipped (Deny): the call may have run, so it reads as an
// unknown outcome not to repeat, with the decider's reason (pinned
// bytes, ADR 0032).
func skippedResult(tool, reason string) string {
	out := fmt.Sprintf("%s: tool %q may have run; it was skipped, not retried: do not repeat it", core.CodeOutcomeUnknown, tool)
	if reason != "" {
		out += " (skipped: " + reason + ")"
	}
	return out
}

// maxAutoRetries bounds the session's own keyed retries of one call
// occurrence (R12); past it the call waits for a person. The count is
// the "retry" audit steps on file, so it survives a restart.
const maxAutoRetries = 3

// autoRetriesLocked counts the session's own retries of c's current
// occurrence: the StepRecover "retry" audit entries for it after the
// assistant entry holding it. Callers hold s.mu.
func (s *Session) autoRetriesLocked(callID string) int {
	i, ok := s.byID[s.callEntryLocked(callID)]
	if !ok {
		return 0
	}
	n := 0
	for _, e := range s.order[i+1:] {
		if a, ok := e.(ApprovalAuditEntry); ok && a.Step == StepRecover && a.Outcome == recoverRetry && a.CallID == callID {
			n++
		}
	}
	return n
}

// retryBackoffLocked is how long a resume waits before it runs: the
// session's n-th automatic retry of a call (n ≥ 1) waits
// retryBackoffBase·2^(n-1), at most retryBackoffCap — 1 s, 2 s, 4 s
// for the three the bound allows. Callers hold s.mu.
func (s *Session) retryBackoffLocked() time.Duration {
	var wait time.Duration
	walk := s.approvalWalkLocked()
	for _, c := range s.danglingCallsLocked() {
		_, ds := walk.occurrence(c.ID)
		d, ok := effectiveDecision(ds, s.quorumOf(walk.requests[c.ID]))
		if !ok || d.Outcome != OutcomeApprove || d.Via != viaRecovery {
			continue
		}
		if n := s.autoRetriesLocked(c.ID); n >= 1 {
			wait = max(wait, min(retryBackoffBase<<min(n-1, 16), retryBackoffCap))
		}
	}
	return wait
}

// retryBackoffBase is the backoff before the session's first keyed
// retry of one call, doubling for each next one up to retryBackoffCap
// (R12). abandonedWait bounds how long the end of a turn waits for a
// handler the loop abandoned (its Timeout passed, a cancel it ignored)
// to return before the session retries its call: still running past
// it, the call goes to a person instead. Variables for the tests only.
var (
	retryBackoffBase = defaultRetryBackoffBase
	retryBackoffCap  = 30 * time.Second
	abandonedWait    = defaultAbandonedWait
)

const (
	defaultRetryBackoffBase = time.Second
	defaultAbandonedWait    = 30 * time.Second
)

// reconciled is what a tool's Reconcile said of one call (ADR 0032,
// the ladder's second rung): found with its result, missing, or err.
type reconciled struct {
	found  bool
	result string
	err    error
}

// reconcileLocked asks each call's tool Reconcile, for the calls of
// unknown outcome that are ReplayNever and have one — the keyed ones
// retry first, the safe ones need nothing — and returns its answers by
// call id. It runs with s.mu released (fn is the tool's code, bounded
// by reconcileTimeout) and only while the writer still holds the
// session's lock (Fencer): a writer that lost it asks nothing, and the
// calls go to a person. A crash during it writes nothing, so the next
// recovery asks again; the tool never runs on its account. Callers
// hold s.mu; it is released and re-taken.
func (s *Session) reconcileLocked(ctx context.Context, calls []core.ToolCallPart) map[string]reconciled {
	type ask struct {
		call core.ToolCallPart
		fn   core.ReconcileFunc
		key  string
	}
	var asks []ask
	for _, c := range calls {
		def := s.toolLocked(c.Name)
		if def == nil || def.ReplayPolicy() != core.ReplayNever || def.Reconciler() == nil {
			continue
		}
		entry := s.callEntryLocked(c.ID)
		if entry == "" {
			continue
		}
		asks = append(asks, ask{call: c, fn: def.Reconciler(), key: s.callKeyLocked(entry, c.ID)})
	}
	if len(asks) == 0 {
		return nil
	}
	out := make(map[string]reconciled, len(asks))
	if f, ok := s.st.(Fencer); ok {
		if err := f.Fence(ctx, s.header.ID); err != nil {
			for _, a := range asks {
				out[a.call.ID] = reconciled{err: fmt.Errorf("not asked: the session's lock is not held: %w", err)}
			}
			return out
		}
	}
	s.mu.Unlock()
	defer s.mu.Lock()
	for _, a := range asks {
		out[a.call.ID] = runReconcile(ctx, a.fn, a.key)
	}
	return out
}

// runReconcile calls fn under the rung's bound, a panic contained as
// its error.
func runReconcile(ctx context.Context, fn core.ReconcileFunc, key string) (r reconciled) {
	rctx, cancel := context.WithTimeout(ctx, reconcileTimeout)
	defer cancel()
	defer func() {
		if p := recover(); p != nil {
			r = reconciled{err: fmt.Errorf("reconcile panicked: %v", p)}
		}
	}()
	found, result, err := fn(rctx, key)
	if err == nil && rctx.Err() != nil {
		err = rctx.Err()
	}
	if err != nil {
		return reconciled{err: err}
	}
	return reconciled{found: found, result: result}
}

// dispatchGate is rule A, the core.BeforeDispatch gate every turn
// carries: a call that is not ReplaySafe starts only when the step
// entry holding it is in the file — appended by the step observer
// before the loop dispatched the step, or the resume boundary's — under
// the session's key for that entry, with its own dispatch record
// appended and flushed after it, and while this writer still holds the
// session. A side effect the file does not show was asked for is never
// started.
//
// It decides under s.mu, the lock an Interrupt, a Rollback and Close
// take to fell the turn: a call reaching the gate after one of them, or
// after the run's context ended (a shutdown, a caller walking away), is
// refused — the felled turn starts nothing, and a Rollback accepted
// because no effect had started stays true. Either the gate passes
// first (and the turn has effects: a Rollback is downgraded) or the
// felling does (and the call never starts).
//
// Any other refusal — the step or the record cannot be made durable (a
// storage error, ErrLocked / ErrStale for a writer whose session
// another process has taken over), or the fence says the lock is gone —
// fails the call closed (an error wrapping core.ErrNotDispatched) and
// ends the turn's run with that cause: a writer that cannot record
// starts nothing more, rather than handing the model refusals until
// MaxSteps.
//
// The fence is read last, after the record is flushed, as close to the
// handler as the gate can be. The residual window is the time from
// that read to the handler's first effect, plus the handler's own run:
// a lock broken inside it is not seen — a receiver that must refuse a
// zombie needs the idempotency key, not the fence. ctx is the turn's
// persistence window. How durable "appended" is, is the backend's
// (FsyncOnFlush for jsonl).
func (s *Session) dispatchGate(ctx context.Context, sp *stepPersist, t *Turn) core.RunOption {
	return core.BeforeDispatch(func(cctx context.Context, c core.Call, def *core.ToolDef) error {
		if def == nil || def.ReplayPolicy() == core.ReplaySafe {
			return nil
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if err := felled(cctx, t); err != nil {
			return err
		}
		refuse := func(err error) error {
			s.agent.Logger().Warn("thread: a call is not dispatched; the run ends",
				"session", s.header.ID, "run", c.RunID, "call", c.CallID, "err", err)
			t.mu.Lock()
			cancel := t.cancel
			t.mu.Unlock()
			if cancel != nil {
				cancel(&notDispatchedCause{err: err})
			}
			return err
		}
		if c.Outer != "" {
			// A call a handler dispatched with CallTool: the outer call
			// is what the file and recovery know — its step, its
			// dispatch record, its key, from which the inner key is
			// derived. The inner call starts only under an outer call
			// this gate let start — the step's recorded call, or an
			// inner call it let start before, at any depth (its key
			// derived from that one's, recursively); a ReplaySafe tool's
			// handler starts no effect — and while the writer still
			// holds the session.
			if !sp.started[sp.calls[c.Outer]+"\x00"+c.Outer] && !sp.inner[c.Outer] {
				return fmt.Errorf("call %q is dispatched from call %q, which this session did not record as dispatched (a ReplaySafe tool starts no effect)", c.CallID, c.Outer)
			}
			if f, ok := s.st.(Fencer); ok {
				if err := f.Fence(ctx, s.header.ID); err != nil {
					return refuse(fmt.Errorf("the session's lock is not held: %w", err))
				}
			}
			sp.inner[c.CallID] = true
			return nil
		}
		entry := sp.calls[c.CallID]
		if entry == "" {
			if err := s.flushStepsLocked(ctx, sp); err != nil {
				return refuse(fmt.Errorf("the step holding call %q is not durable: %w", c.CallID, err))
			}
			entry = sp.calls[c.CallID]
		}
		if entry == "" {
			return refuse(fmt.Errorf("call %q is not in this run's recorded steps", c.CallID))
		}
		want := IdempotencyKey(s.header.ID, entry, c.CallID, "")
		if sp.resume {
			want = s.callKeyLocked(entry, c.CallID)
		}
		if c.IdempotencyKey != want {
			// The loop named the call before its step was durable: its
			// key is run-scoped, not the session's.
			return refuse(fmt.Errorf("call %q carries no recovery-stable key", c.CallID))
		}
		// Record before acting: the dispatch is on file and, with the
		// step before it, flushed (WR-1: under a backend that defers its
		// sync, jsonl's FsyncOnFlush) before the handler runs. After a
		// crash a call of a step that records its dispatches and has no
		// record never started — a call a park rule or a middleware
		// parked is never mistaken for one that may have run (ADR 0032).
		if err := s.appendLocked(ctx, func(id, parent string, created time.Time) Entry {
			return ApprovalAuditEntry{ID: id, ParentID: parent, Created: created,
				CallID: c.CallID, Step: StepDispatch, RunID: c.RunID}
		}); err != nil {
			return refuse(fmt.Errorf("the dispatch of call %q is not recorded: %w", c.CallID, err))
		}
		if f, ok := s.st.(Flusher); ok {
			if err := f.Flush(ctx, s.header.ID); err != nil {
				return refuse(fmt.Errorf("the dispatch of call %q is not durable: %w", c.CallID, err))
			}
		}
		// The self-fence (WR-4), last: the writer still holds the
		// session's lock, read now — a writer whose lock was broken or
		// taken over since its step landed starts nothing. A record
		// written just before a failed fence reads "may have run" after
		// a crash: the safe side.
		if f, ok := s.st.(Fencer); ok {
			if err := f.Fence(ctx, s.header.ID); err != nil {
				return refuse(fmt.Errorf("the session's lock is not held: %w", err))
			}
		}
		sp.started[entry+"\x00"+c.CallID] = true
		sp.handlerStartedLocked(entry + "\x00" + c.CallID)
		t.mu.Lock()
		t.effects = true
		t.mu.Unlock()
		return nil
	})
}

// felled is the gate's refusal for a turn already felled: an Interrupt,
// a Rollback or Close marked it (under s.mu, which the caller holds),
// or the call's context ended — a shutdown, the caller walking away.
// The run is ending: nothing more starts in it.
func felled(ctx context.Context, t *Turn) error {
	if t.wasInterrupted() {
		return fmt.Errorf("the turn was felled before the call started: %w", t.interruptCause())
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("the run ended before the call started: %w", context.Cause(ctx))
	}
	return nil
}

// notDispatchedCause is the cause a refusing gate fells its turn's run
// with (weft.cancel.cause thread.not_dispatched): a cancellation that
// carries the refusal — a lost lease reads errors.Is(err, ErrLocked).
type notDispatchedCause struct{ err error }

func (c *notDispatchedCause) Error() string {
	return "thread: the run stopped: a call could not be dispatched: " + c.err.Error()
}
func (c *notDispatchedCause) Unwrap() []error      { return []error{c.err, core.ErrNotDispatched} }
func (c *notDispatchedCause) Is(target error) bool { return target == context.Canceled }

// Kind names the cause on the run's span (core.CancelCause's rule).
func (c *notDispatchedCause) Kind() string { return "thread.not_dispatched" }

// cutOff is a run the session's file shows started and never ended
// on its own: no turn entry, or one marked CutOff (a shutdown), and no
// StepRecover audit entry has handled it.
type cutOff struct {
	run     string
	resume  bool // a resume run over an approval boundary
	start   int  // where the run started on the path
	hasTurn bool // the file holds its turn entry (a shutdown's)
	// own is set when the unanswered calls are the run's own last
	// step's, ownEntry the assistant entry holding them; otherwise
	// they are the boundary's a resume was running.
	own      bool
	ownEntry string
	// lastAnswer: the run's last message is an assistant message
	// without calls — it answered, and only its end was lost.
	lastAnswer bool
	calls      []core.ToolCallPart // without a result, in call order
}

// cutOffLocked finds the cut-off run, if any: the last run started on
// the leaf's path — a prompt entry's run id, a resume's "started"
// audit, a recovery's continuation, an overflow attempt's re-run —
// whose turn entry is missing or marked CutOff, and which no
// StepRecover audit has handled. ok is false while this Session runs
// a turn: that turn is running, not cut off. A file-level fact: a turn
// another process is running looks the same (Open takes no lock), so
// a caller acting on it Claims the session first. Callers hold s.mu.
func (s *Session) cutOffLocked() (cutOff, bool) {
	if s.running {
		return cutOff{}, false
	}
	path, err := s.walkLocked(s.leaf)
	if err != nil {
		return cutOff{}, false
	}
	x := cutOff{start: -1}
	recovered := map[string]bool{}
	for i, e := range path {
		switch e := e.(type) {
		case MessageEntry:
			if e.RunID != "" {
				x.run, x.start, x.resume = e.RunID, i, false
			}
		case ApprovalAuditEntry:
			switch {
			case e.Step == StepResume && e.Outcome == "started":
				x.run, x.start, x.resume = e.RunID, i, true
			case e.Step == StepRecover && e.CallID == "":
				recovered[e.RunID] = true
				if e.Outcome == recoverContinued && e.Detail != "" {
					x.run, x.start, x.resume = e.Detail, i, false
				}
			}
		}
	}
	if x.run == "" {
		return cutOff{}, false
	}
	// An overflow attempt's turn entry names its re-run, which has no
	// start record of its own: follow the chain to the run that ran
	// last.
	turns := map[string]TurnEntry{}
	for _, e := range s.order {
		if te, ok := e.(TurnEntry); ok {
			turns[te.RunID] = te
		}
	}
	for {
		te, ok := turns[x.run]
		if !ok || te.ReRun == "" || te.CutOff {
			break
		}
		x.run = te.ReRun
	}
	if recovered[x.run] {
		return cutOff{}, false
	}
	te, hasTurn := turns[x.run]
	if hasTurn && !te.CutOff {
		return cutOff{}, false
	}
	x.hasTurn = hasTurn

	// The run's last message on the path, and the calls it left.
	lastCalls := -1
	for j := len(path) - 1; j > x.start; j-- {
		me, ok := path[j].(MessageEntry)
		if !ok {
			continue
		}
		if me.Message.Role == core.RoleAssistant {
			if hasToolCall(me.Message) {
				lastCalls = j
			} else {
				x.lastAnswer = true
			}
		}
		break
	}
	x.calls = s.unansweredCallsLocked()
	if lastCalls >= 0 && len(x.calls) > 0 {
		x.own, x.ownEntry = true, idOf(path[lastCalls])
	}
	if !x.own && !x.resume {
		x.calls = nil // the dangling calls are an older boundary's, not this run's
	}
	return x, true
}

func hasToolCall(m core.Message) bool {
	for _, p := range m.Content {
		if _, ok := p.(core.ToolCallPart); ok {
			return true
		}
	}
	return false
}

// toolLocked returns the agent's tool by name, nil when it has none
// (a tool since removed, or one only a ToolSource supplies): such a
// call is treated as core.ReplayNever. Callers hold s.mu.
func (s *Session) toolLocked(name string) *core.ToolDef {
	for _, def := range s.agent.Tools() {
		if def.Name == name {
			return def
		}
	}
	return nil
}

// replayOfLocked is a call's replay class, ReplayNever for a tool the
// agent does not hold. Callers hold s.mu.
func (s *Session) replayOfLocked(name string) core.ReplayPolicy {
	if def := s.toolLocked(name); def != nil {
		return def.ReplayPolicy()
	}
	return core.ReplayNever
}

// callStart is what the file says of whether a cut-off call started.
type callStart int

const (
	// startUnknowable: the step holding the call predates dispatch
	// records (DispatchRecorded unset). It may have run, and whether a
	// park rule or a middleware held it is not knowable: nothing
	// automatic may run it.
	startUnknowable callStart = iota
	// startNever: the step records its dispatches and holds none for
	// the call under the cut-off run — it never started (a park rule,
	// a middleware or the approval boundary held it, or the crash came
	// first).
	startNever
	// startMaybe: its dispatch is recorded — it may have run.
	startMaybe
)

// callStartLocked reads what the file says of c's start in the cut-off
// run x: the step entry holding it, and the dispatch records after it
// under x's run. Callers hold s.mu.
func (s *Session) callStartLocked(x cutOff, c core.ToolCallPart) callStart {
	entry := x.ownEntry
	if !x.own {
		entry = s.callEntryLocked(c.ID)
	}
	i, ok := s.byID[entry]
	if !ok {
		return startUnknowable
	}
	if s.delegationEndedLocked(entry, c.ID) {
		return startMaybe // it delegated: the child may have run
	}
	if me, ok := s.order[i].(MessageEntry); !ok || !me.DispatchRecorded {
		return startUnknowable
	}
	for _, e := range s.order[i+1:] {
		if a, ok := e.(ApprovalAuditEntry); ok && a.Step == StepDispatch && a.CallID == c.ID && a.RunID == x.run {
			return startMaybe
		}
	}
	return startNever
}

// delegationEndedLocked reports whether callID, a call of the step
// entry entry, delegated to a thread/pool child whose delegation is
// over: an acceptance receipt naming it follows the step — the
// delegation's dispatch record, appended before the child runs — and a
// settlement of that receipt follows too. Its call has no result: what
// the child did is the delegation's, and the pool resolves it no more.
// A delegation still open is the pool's to settle (ADR 0022 §7).
// Callers hold s.mu.
func (s *Session) delegationEndedLocked(entry, callID string) bool {
	i, ok := s.byID[entry]
	if !ok {
		return false
	}
	receipt := ""
	for _, e := range s.order[i+1:] {
		pr, ok := e.(PoolReceiptEntry)
		switch {
		case !ok:
		case pr.Receipt == "" && pr.Call == callID:
			receipt = pr.ID
		case receipt != "" && pr.Receipt == receipt && pr.Status != PoolAccepted && pr.Status != PoolRunning && pr.Status != PoolParked:
			return true
		}
	}
	return false
}

// unknownLocked returns the cut-off run's calls whose outcome is
// unknown (rule B): calls that are not ReplaySafe, without a result,
// that the run may have started — its own step's calls whose dispatch
// is recorded (or, in a step from before dispatch records, every one
// its tool's RequireApproval does not park); for a resume, the
// boundary's calls approved to run, except those whose step records
// that they never started. A call delegating to a pool child is the
// pool's to settle. Callers hold s.mu.
func (s *Session) unknownLocked(x cutOff) []core.ToolCallPart {
	if len(x.calls) == 0 {
		return nil
	}
	walk := s.approvalWalkLocked()
	var out []core.ToolCallPart
	for _, c := range x.calls {
		def := s.toolLocked(c.Name)
		if def != nil && def.ReplayPolicy() == core.ReplaySafe {
			continue
		}
		if walk.wrappers[c.ID] != "" {
			// A call delegating to a thread/pool child: the child
			// session holds its outcome, and the pool's Recover
			// settles it (ADR 0022 §7).
			continue
		}
		start := s.callStartLocked(x, c)
		if start == startNever {
			continue // never started: its handler never ran
		}
		if x.own {
			if start == startUnknowable && def != nil && def.RequiresApproval() {
				continue // parked before it could start
			}
		} else {
			_, ds := walk.occurrence(c.ID)
			d, ok := effectiveDecision(ds, s.quorumOf(walk.requests[c.ID]))
			if !ok || d.Outcome != OutcomeApprove {
				continue // denied, resolved or undecided: its handler never ran
			}
		}
		out = append(out, c)
	}
	return out
}

// recoveryRequiredLocked reports whether a cut-off run left a call
// whose outcome is unknown — the case in which a new turn or a resume
// could repeat it. Every path that starts a turn or arms a resume asks
// it (G5). Callers hold s.mu.
func (s *Session) recoveryRequiredLocked() bool {
	x, ok := s.cutOffLocked()
	return ok && len(s.unknownLocked(x)) > 0
}

// errRecoveryRequiredLocked is ErrRecoveryRequired naming the session,
// nil when nothing waits for recovery. Callers hold s.mu.
func (s *Session) errRecoveryRequiredLocked() error {
	if s.recoveryRequiredLocked() {
		return fmt.Errorf("%w: session %s", ErrRecoveryRequired, s.header.ID)
	}
	return nil
}

// RecoverTurn resumes, re-asks or parks the turn a crash or a shutdown
// cut off (ADR 0032), and records that it did so — a StepRecover audit
// entry, in the same atomic append as everything it decides — so the
// turn is recovered once:
//
//   - The run left no call without a result — it died in a model call,
//     between steps, or after a resume's results landed: its turn entry
//     is written (CutOff) and a continuation run starts from the last
//     finished step, with no new prompt, under a fresh run id (the
//     audit entry's Detail). The returned Turn is that run.
//   - Its last step's calls are unanswered and every one is
//     core.ReplaySafe: the step is dropped — the leaf moves back to
//     before its assistant message, which stays on its own line — and a
//     continuation run asks the model again.
//   - Otherwise each unanswered call parks under the cut-off run, its
//     outcome stated in the request's Reason. What the file says of
//     its start decides how. The step entry records its dispatches
//     (MessageEntry.DispatchRecorded, a StepDispatch audit entry per
//     call the gate let start, written before the handler ran), so a
//     call with no record never started — a park rule (ParkSource,
//     ParkOn, ParkAllExcept), a middleware or its tool's own approval
//     held it, or the crash came first: it parks as an ordinary
//     request for a person, never run on the session's own say. A
//     call whose dispatch is recorded may have run: it parks as a
//     recovery request (Request.Recovery) — a core.ReplayKeyed one
//     approved at once by the session (Via "recovery", a "retry" audit
//     step), to run again with its same IdempotencyKey; a
//     core.ReplayNever one asks its tool's
//     Reconcile, else waits for a person to approve (retry, same key)
//     or deny (skip) it. A ReplaySafe call beside them is not run
//     again: its result says so, and the model may ask again. In a
//     step written before dispatch records, where whether a call was
//     parked is not knowable, nothing runs on the session's own say:
//     every call that may have run waits for a person. When that
//     decides every call, the resume starts and is returned; else
//     RecoverTurn returns nil and the boundary waits.
//   - A resume run cut off before its results landed: the boundary's
//     approved calls whose dispatch is recorded (or, before dispatch
//     records, every approved one) and are not ReplaySafe park again
//     as recovery requests, as above; the rest keep their decisions;
//     the resume starts again once all are decided.
//   - The run had answered and only its end was lost: the turn entry
//     is written and nothing runs (nil).
//
// A Send's own run options do not survive a crash: a continuation runs
// under the session's run metadata and the boundary's recorded
// settings, not under a per-Send park rule. A call that rule parked
// never started and parks again as an ordinary request; a call the
// model asks for in a continuation is held only by the agent's own
// gates (RequireApproval, ParkSource, middleware).
//
// RecoverTurn is an explicit start, like Resume: it starts its run
// under OnReady too. It fails with ErrNotPending when no turn was cut
// off, ErrBusy while a turn runs, ErrClosed on a closed session, and
// ErrLocked when another writer holds the session — the turn was not
// cut off, it runs elsewhere. Open, Claim, then RecoverTurn: the claim
// decides which of two workers recovers.
func (s *Session) RecoverTurn(ctx context.Context) (*Turn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.admitLocked(); err != nil {
		return nil, err
	}
	if s.running {
		return nil, fmt.Errorf("%w: session %s", ErrBusy, s.header.ID)
	}
	x, ok := s.cutOffLocked()
	if !ok {
		return nil, fmt.Errorf("%w: no cut-off turn in session %s", ErrNotPending, s.header.ID)
	}
	persist := context.WithoutCancel(ctx)
	// The ladder's second rung, before anything is written: the
	// unknown calls whose tool can ask the system of record do.
	if unknown := s.unknownLocked(x); len(unknown) > 0 {
		run := x.run
		rc := s.reconcileLocked(ctx, unknown)
		// The lock was released: the session must still be as found.
		if s.running {
			return nil, fmt.Errorf("%w: session %s", ErrBusy, s.header.ID)
		}
		if y, ok := s.cutOffLocked(); !ok || y.run != run {
			return nil, fmt.Errorf("%w: the cut-off turn of session %s changed while it was reconciled", ErrNotPending, s.header.ID)
		}
		return s.recoverWithLocked(ctx, persist, x, rc)
	}
	return s.recoverWithLocked(ctx, persist, x, nil)
}

// recoverWithLocked is RecoverTurn's crash matrix over the cut-off run,
// with the Reconcile answers rc. Callers hold s.mu.
func (s *Session) recoverWithLocked(ctx, persist context.Context, x cutOff, rc map[string]reconciled) (*Turn, error) {
	switch {
	case len(x.calls) > 0 && !x.own:
		return s.recoverResumeLocked(ctx, persist, x, rc)
	case len(x.calls) > 0:
		allSafe := true
		for _, c := range x.calls {
			allSafe = allSafe && s.replayOfLocked(c.Name) == core.ReplaySafe
		}
		if !allSafe {
			return s.recoverParkLocked(ctx, persist, x, rc)
		}
		// Only ReplaySafe calls were cut off: drop the step and ask
		// the model again.
		parent := ""
		if i, ok := s.byID[x.ownEntry]; ok {
			parent = parentOf(s.order[i])
		}
		if parent != "" {
			if err := s.branchBackLocked(persist, parent); err != nil {
				return nil, err
			}
		}
		return s.recoverContinueLocked(ctx, persist, x)
	case x.lastAnswer:
		return nil, s.recordRecoverLocked(persist, x, recoverClosed, "", nil)
	default:
		return s.recoverContinueLocked(ctx, persist, x)
	}
}

// recordRecoverLocked appends the cut-off run's turn entry (when the
// file has none) and the StepRecover audit entry, with extra entries
// between them, in one atomic append. Callers hold s.mu.
func (s *Session) recordRecoverLocked(ctx context.Context, x cutOff, outcome, detail string, pending []core.ToolCallPart, extra ...Entry) error {
	var entries []Entry
	parent := s.leaf
	now := s.now()
	add := func(e Entry) error {
		id, err := s.mintCheckedLocked(entries)
		if err != nil {
			return err
		}
		e = withTree(e, id, parent, now)
		entries = append(entries, e)
		parent = id
		return nil
	}
	if !x.hasTurn {
		if err := add(TurnEntry{RunID: x.run, Err: cutOffTurn, CutOff: true, Pending: pending}); err != nil {
			return err
		}
	}
	lastRequest := map[string]string{} // call id → the request just added for it
	for _, e := range extra {
		if d, ok := e.(ApprovalDecisionEntry); ok && d.RequestID == "" {
			d.RequestID = lastRequest[d.CallID] // a recovery decision names its request
			e = d
		}
		if err := add(e); err != nil {
			return err
		}
		if re, ok := entries[len(entries)-1].(ApprovalRequestEntry); ok {
			lastRequest[re.CallID] = re.ID
		}
	}
	if err := add(ApprovalAuditEntry{Step: StepRecover, Outcome: outcome, Detail: detail, RunID: x.run}); err != nil {
		return err
	}
	return s.appendEntriesLocked(ctx, entries...)
}

// withTree stamps an entry's id, parent and time.
func withTree(e Entry, id, parent string, at time.Time) Entry {
	switch e := e.(type) {
	case TurnEntry:
		e.ID, e.ParentID, e.Created = id, parent, at
		return e
	case ApprovalRequestEntry:
		e.ID, e.ParentID, e.Created = id, parent, at
		return e
	case ApprovalDecisionEntry:
		e.ID, e.ParentID, e.Created = id, parent, at
		return e
	case ApprovalAuditEntry:
		e.ID, e.ParentID, e.Created = id, parent, at
		return e
	}
	return withParent(e, parent)
}

// recoverContinueLocked closes the cut-off run and starts the
// continuation: a turn with no prompt over the session's context.
// Callers hold s.mu.
func (s *Session) recoverContinueLocked(ctx, persist context.Context, x cutOff) (*Turn, error) {
	t := s.newTurnLocked()
	if err := s.recordRecoverLocked(persist, x, recoverContinued, t.runID, nil); err != nil {
		return nil, err
	}
	t.setPromptDone()
	s.startRunnerLocked(workItem{ps: pendingSend{ctx: ctx, turn: t}})
	return t, nil
}

// recoveryEntries parks one cut-off call (rules B–E). start is what
// the file says of its start:
//
//   - startNever: an ordinary request — it never started, and a park
//     rule, a middleware or its tool's own gate may have held it, so
//     only a person runs it;
//   - a ReplaySafe call: not run again — the session resolves it with
//     a result saying so, and the model may ask for it again (a park
//     rule that held it holds it again then);
//   - startUnknowable (a step from before dispatch records): a
//     recovery request for a person, whatever the replay class —
//     nothing automatic runs a call that may have been parked; only a
//     system of record's "found" resolves it;
//   - a call whose thread/pool delegation was ended unanswered (a
//     cut-off child's, its delegations ended by the pool's Recover):
//     a recovery request for a person, whatever its class — recovery
//     is not built at depth;
//   - startMaybe: the ladder — a ReplayKeyed call is retried by the
//     session under its unchanged key ("retry" audit step, Via
//     "recovery"), a ReplayNever one asks its Reconcile answer (rc)
//     and else waits for a person.
//
// args are the arguments the call runs on; quorum the approvals a
// person's retry needs. Callers hold s.mu.
func (s *Session) recoveryEntries(c core.ToolCallPart, run string, args []byte, settings *core.RunSettings, quorum int, start callStart, rc map[string]reconciled) []Entry {
	def := s.toolLocked(c.Name)
	policy := core.ReplayNever
	if def != nil {
		policy = def.ReplayPolicy()
	}
	req := ApprovalRequestEntry{
		CallID: c.ID, Tool: c.Name, Args: slices.Clone(args), ArgsSHA256: hashArgs(args),
		RunID: run, Run: settings.Clone(), SchemaSHA256: toolSchemaHash(s.agent, c.Name),
		Quorum: quorum, Recovery: true,
	}
	switch {
	case policy == core.ReplaySafe:
		req.Reason = reasonSafeNotRerun
		return []Entry{req,
			ApprovalDecisionEntry{CallID: c.ID, Outcome: OutcomeResolveError, Content: safeNotRerunResult(c.Name),
				Who: "thread", Via: viaRecovery, RunID: run, ArgsSHA256: hashArgs(args)},
		}
	case start == startNever:
		// It never started: whatever held it — its tool's own gate, a
		// park rule, a middleware — a person decides it, as any call.
		req.Recovery, req.Reason = false, reasonNotStarted
		if def != nil && def.RequiresApproval() {
			req.Reason = parkingReason(s, c.Name)
		}
		return []Entry{req}
	case start == startUnknowable && def != nil && def.RequiresApproval():
		// Its tool parks it before it starts: it never ran.
		req.Recovery, req.Reason = false, parkingReason(s, c.Name)
		return []Entry{req}
	case s.delegationEndedLocked(s.callEntryLocked(c.ID), c.ID):
		// It delegated to a thread/pool child, and the delegation was
		// ended unanswered (thread/pool's Recover ends a cut-off
		// child's delegations): what the child did is not known, and
		// recovery is not built at depth. Nothing automatic runs it
		// again — no keyed retry, no Reconcile, whatever its tool's
		// class: a person approves (it delegates again) or denies (it
		// reads as an unknown outcome not to repeat).
		req.Reason = reasonDelegated
		return []Entry{req}
	case policy == core.ReplayKeyed && start == startMaybe && s.autoRetriesLocked(c.ID) >= maxAutoRetries:
		// Bounded (R12): the session's own retries are spent — a
		// timeout shorter than the provider would loop for ever.
		req.Reason = reasonRetriesSpent
		return []Entry{req}
	case policy == core.ReplayKeyed && start == startMaybe:
		req.Reason = reasonRetryKeyed
		return []Entry{req,
			ApprovalAuditEntry{CallID: c.ID, Step: StepRecover, Outcome: recoverRetry, Detail: string(policy), RunID: run},
			ApprovalDecisionEntry{CallID: c.ID, Outcome: OutcomeApprove, Who: "thread", Via: viaRecovery,
				RunID: run, ArgsSHA256: hashArgs(args)},
		}
	}
	req.Reason = reasonOutcomeUnknown
	r, asked := rc[c.ID]
	switch {
	case asked && r.err == nil && r.found:
		// The system of record knows: its result is the call's.
		req.Reason = reasonReconcileFound
		return []Entry{req,
			ApprovalAuditEntry{CallID: c.ID, Step: StepRecover, Outcome: reconcileFound, RunID: run},
			ApprovalDecisionEntry{CallID: c.ID, Outcome: OutcomeResolve, Content: r.result, Who: "thread", Via: viaReconcile,
				RunID: run, ArgsSHA256: hashArgs(args)},
		}
	case asked && r.err == nil && start == startMaybe && policy == core.ReplayNever:
		req.Reason = reasonReconcileMiss
		return []Entry{req,
			ApprovalAuditEntry{CallID: c.ID, Step: StepRecover, Outcome: reconcileMissing, RunID: run},
			ApprovalDecisionEntry{CallID: c.ID, Outcome: OutcomeApprove, Who: "thread", Via: viaRecovery,
				RunID: run, ArgsSHA256: hashArgs(args)},
		}
	case asked && r.err == nil:
		// Missing, but whether a park rule held it is not knowable: a
		// person decides.
		return []Entry{req,
			ApprovalAuditEntry{CallID: c.ID, Step: StepRecover, Outcome: reconcileMissing, RunID: run},
		}
	case asked:
		return []Entry{req,
			ApprovalAuditEntry{CallID: c.ID, Step: StepRecover, Outcome: reconcileError, Detail: r.err.Error(), RunID: run},
		}
	}
	return []Entry{req}
}

// recoverParkLocked parks the cut-off run's own unanswered calls
// (recoveryEntries) and starts the resume when that decided every
// call. Callers hold s.mu.
func (s *Session) recoverParkLocked(ctx, persist context.Context, x cutOff, rc map[string]reconciled) (*Turn, error) {
	var extra []Entry
	for _, c := range x.calls {
		extra = append(extra, s.recoveryEntries(c, x.run, c.Args, nil, s.parkQuorum(), s.callStartLocked(x, c), rc)...)
	}
	if err := s.recordRecoverLocked(persist, x, recoverParked, "", x.calls, extra...); err != nil {
		return nil, err
	}
	return s.armRecoveredLocked(ctx)
}

// recoverResumeLocked handles a resume run cut off before its results
// landed: each approved call whose outcome is unknown parks again as a
// recovery request under the cut-off resume run — on the arguments it
// ran on, under the quorum its approval needed — and every other call
// keeps its decision. The resume starts again when all are decided.
// Callers hold s.mu.
func (s *Session) recoverResumeLocked(ctx, persist context.Context, x cutOff, rc map[string]reconciled) (*Turn, error) {
	unknown := s.unknownLocked(x)
	outcome := recoverResumed
	var extra []Entry
	if len(unknown) > 0 {
		outcome = recoverParked
		walk := s.approvalWalkLocked()
		settings := s.persistedRunLocked()
		for _, c := range unknown {
			re := walk.requests[c.ID]
			args := c.Args
			if len(re.Args) > 0 {
				args = re.Args
			}
			// An approved boundary call: its park status is settled, so a
			// step from before dispatch records reads as "may have run".
			extra = append(extra, s.recoveryEntries(c, x.run, args, settings, max(s.parkQuorum(), re.Quorum), startMaybe, rc)...)
		}
	}
	if err := s.recordRecoverLocked(persist, x, outcome, "", unknown, extra...); err != nil {
		return nil, err
	}
	return s.armRecoveredLocked(ctx)
}

// armRecoveredLocked starts the boundary's resume when every call is
// decided — RecoverTurn's own explicit start, under OnReady too — and
// otherwise leaves it for the decisions. Callers hold s.mu.
func (s *Session) armRecoveredLocked(ctx context.Context) (*Turn, error) {
	if _, err := s.resolveExpiredLocked(ctx); err != nil {
		return nil, err
	}
	if !s.boundaryLocked() || len(s.pendingLocked()) > 0 {
		return nil, nil
	}
	return s.armResumeLocked(ctx)
}
