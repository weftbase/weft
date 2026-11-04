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
	reasonRetrySafe      = "cut off: the call is ReplaySafe and runs again"
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
// entry holding it is in the file — appended and flushed by the step
// observer before the loop dispatched the step, or the resume
// boundary's — and only under the session's key for that entry. When
// the entry is not durable (its step's append failed, and a retry here
// fails too — a storage error, or ErrLocked / ErrStale for a writer
// whose session another process has taken over), the handler never
// runs and the model reads an error wrapping core.ErrNotDispatched. A
// side effect the file does not show was asked for is never started.
// ctx is the turn's persistence window. How durable "appended" is,
// is the backend's (FsyncOnFlush for jsonl).
func (s *Session) dispatchGate(ctx context.Context, sp *stepPersist, t *Turn) core.RunOption {
	return core.BeforeDispatch(func(_ context.Context, c core.Call, def *core.ToolDef) error {
		if def == nil || def.ReplayPolicy() == core.ReplaySafe {
			return nil
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		entry := sp.calls[c.CallID]
		if entry == "" {
			if err := s.flushStepsLocked(ctx, sp); err != nil {
				s.agent.Logger().Warn("thread: a call's step is not durable; the call is not run",
					"session", s.header.ID, "run", c.RunID, "call", c.CallID, "err", err)
				return fmt.Errorf("the step holding call %q is not durable: %w", c.CallID, err)
			}
			entry = sp.calls[c.CallID]
		}
		if entry == "" {
			return fmt.Errorf("call %q is not in this run's recorded steps", c.CallID)
		}
		// The self-fence (WR-4): the writer still holds the session's
		// lock, read now — a writer whose lock was broken or taken over
		// since its step landed starts nothing.
		if f, ok := s.st.(Fencer); ok {
			if err := f.Fence(ctx, s.header.ID); err != nil {
				s.agent.Logger().Warn("thread: this writer no longer holds the session; the call is not run",
					"session", s.header.ID, "run", c.RunID, "call", c.CallID, "err", err)
				return fmt.Errorf("the session's lock is not held: %w", err)
			}
		}
		want := IdempotencyKey(s.header.ID, entry, c.CallID, "")
		if sp.resume {
			want = s.callKeyLocked(entry, c.CallID)
		}
		if c.IdempotencyKey != want {
			// The loop named the call before its step was durable: its
			// key is run-scoped, not the session's.
			return fmt.Errorf("call %q carries no recovery-stable key", c.CallID)
		}
		sp.started[entry+"\x00"+c.CallID] = true
		t.mu.Lock()
		t.effects = true
		t.mu.Unlock()
		return nil
	})
}

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

// unknownLocked returns the cut-off run's calls whose outcome is
// unknown (rule B): calls that are not ReplaySafe, without a result,
// that the run may have started — its own step's calls, except those
// its tool's RequireApproval parks before they start; for a resume,
// the boundary's calls approved to run. A call delegating to a pool
// child is the pool's to settle. Callers hold s.mu.
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
		if x.own {
			if def != nil && def.RequiresApproval() {
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
//     outcome stated in the request's Reason: a call that is not
//     ReplaySafe parks as a recovery request (Request.Recovery) — a
//     core.ReplayKeyed one approved at once by the session (Via
//     "recovery", a "retry" audit step), to run again with its same
//     IdempotencyKey; a core.ReplayNever one waiting for a person to
//     approve (retry, same key) or deny (skip) it. A ReplaySafe call
//     beside them is approved at once; a call whose tool requires
//     approval parks as an ordinary request — it never started. When
//     that decides every call, the resume starts and is returned; else
//     RecoverTurn returns nil and the boundary waits.
//   - A resume run cut off before its results landed: the boundary's
//     calls that were approved and are not ReplaySafe park again as
//     recovery requests, as above; the rest keep their decisions; the
//     resume starts again once all are decided.
//   - The run had answered and only its end was lost: the turn entry
//     is written and nothing runs (nil).
//
// A Send's own run options do not survive a crash: a continuation runs
// under the session's run metadata and the boundary's recorded
// settings, not under a per-Send park rule. A gate that must hold after
// a crash belongs on the tool (RequireApproval) or the agent
// (ParkSource).
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

// recoveryEntries parks one cut-off call (rules B–E): its request —
// a recovery request unless the tool's own approval gate parks it,
// which never started — and, for a call safe to run again (ReplaySafe,
// or ReplayKeyed: it deduplicates on its unchanged key), the session's
// own approval with its "retry" audit step. args are the arguments the
// call runs on; quorum the approvals a person's retry needs; ownStep
// says the call is the cut-off run's own step's, not an approved
// boundary call a resume was running. Callers hold s.mu.
func (s *Session) recoveryEntries(c core.ToolCallPart, run string, args []byte, settings *core.RunSettings, quorum int, ownStep bool, rc map[string]reconciled) []Entry {
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
	case ownStep && def != nil && def.RequiresApproval():
		// Its tool parks it before it starts: it never ran.
		req.Recovery, req.Reason = false, parkingReason(s, c.Name)
		return []Entry{req}
	case policy == core.ReplaySafe:
		req.Reason = reasonRetrySafe
	case policy == core.ReplayKeyed:
		req.Reason = reasonRetryKeyed
	default:
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
		case asked && r.err == nil:
			req.Reason = reasonReconcileMiss
			return []Entry{req,
				ApprovalAuditEntry{CallID: c.ID, Step: StepRecover, Outcome: reconcileMissing, RunID: run},
				ApprovalDecisionEntry{CallID: c.ID, Outcome: OutcomeApprove, Who: "thread", Via: viaRecovery,
					RunID: run, ArgsSHA256: hashArgs(args)},
			}
		case asked:
			req.Reason = reasonOutcomeUnknown
			return []Entry{req,
				ApprovalAuditEntry{CallID: c.ID, Step: StepRecover, Outcome: reconcileError, Detail: r.err.Error(), RunID: run},
			}
		}
		req.Reason = reasonOutcomeUnknown
		return []Entry{req}
	}
	return []Entry{req,
		ApprovalAuditEntry{CallID: c.ID, Step: StepRecover, Outcome: recoverRetry, Detail: string(policy), RunID: run},
		ApprovalDecisionEntry{CallID: c.ID, Outcome: OutcomeApprove, Who: "thread", Via: viaRecovery,
			RunID: run, ArgsSHA256: hashArgs(args)},
	}
}

// recoverParkLocked parks the cut-off run's own unanswered calls
// (recoveryEntries) and starts the resume when that decided every
// call. Callers hold s.mu.
func (s *Session) recoverParkLocked(ctx, persist context.Context, x cutOff, rc map[string]reconciled) (*Turn, error) {
	var extra []Entry
	for _, c := range x.calls {
		extra = append(extra, s.recoveryEntries(c, x.run, c.Args, nil, s.parkQuorum(), true, rc)...)
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
			extra = append(extra, s.recoveryEntries(c, x.run, args, settings, max(s.parkQuorum(), re.Quorum), false, rc)...)
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
