package pool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
)

// Recover brings parent's delegations back under this pool after a
// restart (ADR 0022 §7): the ledger is entries, the pool's memory is
// not, and a process can die between any two of them. For every
// receipt of parent's that is unsettled and that no child of this
// pool is working, Recover reads the child session and makes the
// ledger say what is true:
//
//   - the child is parked at an approval: the bridge is rebuilt (the
//     child reopened under its registered or wrap-named agent), any
//     pending request the parent holds no mirror for is mirrored —
//     the crash window between a child's park and its mirror — and
//     the receipt records parked. The child resumes when its requests
//     are decided, as if nothing had happened. A sync delegation whose
//     delegating run died before its turn ended — the child parked and
//     mirrored, the parent's park never recorded — has no delegating
//     call to resolve: nothing parked it (no turn entry, no boundary;
//     the parent's next run repairs it as interrupted). The child is
//     reattached as a delegation by no call: its requests stay
//     offered, a decision resumes it, and its answer settles the
//     receipt only.
//   - a crash or a shutdown cut the child's run off (ADR 0032: the last
//     run started — a prompt, a resume, a recovery's continuation —
//     has no turn entry, or one recorded cut off, and no recovery
//     handled it; a second crash inside a recovery is cut off the same
//     way): it is not settled failed — what it did is not known, and a
//     parent told "failed" may delegate the work again over an effect
//     the child had. The child is reattached and recovered on its own
//     ladder (thread.Session.RecoverTurn), the receipt left open: a
//     call of unknown outcome that is not core.ReplaySafe parks as a
//     recovery request mirrored onto the parent, for a person to retry
//     (same idempotency key) or skip; a core.ReplayKeyed call is
//     retried under its key (up to the session's bound, then a
//     person); otherwise it continues to its answer, which settles the
//     receipt. That includes a resume a person's approval or the
//     session's own keyed retry started: the approved call is never
//     re-run blind and never left parked with nothing to decide. The
//     same holds at any depth: a grandchild whose approved call was
//     running parks through its parent's recovered delegation.
//     Recovery is not built at depth: a cut-off child that was itself
//     delegating (an open receipt or mirror of its own) has its own
//     delegations ended first, depth first as below, and its
//     delegating call — whose outcome is the ended delegation's, not
//     known — parks for a person whatever its tool's replay class: the
//     parent offers it as a mirrored recovery request, approve runs the
//     call again (a new delegation), deny skips it and the child's
//     model reads OUTCOME_UNKNOWN ("may have run … do not repeat it").
//   - the child finished its run: the receipt settles from the child's
//     own ledger — done with its final text, failed, capped or
//     canceled with its recorded cause — with the child's usage.
//   - the child never started its run (queued when the process died):
//     the receipt settles failed, saying so, its own delegations ended
//     first.
//   - the child session is gone: the receipt settles failed — saying
//     its run never started and nothing ran when the receipt reads
//     accepted (the acceptance is written before the child is created,
//     and a crash between the two leaves a receipt with no child).
//
// A child settled as over that is itself a parent ends its own
// unsettled delegations first, depth first, as Cancel and Close end
// them: nothing will resolve their calls in it. A parked sync
// grandchild is canceled at its boundary — its pending calls denied in
// its session, its requests in the child denied — and settles canceled
// in the child's ledger; one whose run is over settles from its file;
// a parked async one is left parked (D4). Without the agent to reopen
// one of them, the child's receipt is left unsettled and reported with
// ErrNoAgent, for a later Recover. A kill inside this cascade is
// finished by the next Recover: a child is descended into while it
// holds requests no decision answers too, and a settled delegation's
// requests still offered in it are denied.
//
// A sync delegation's call still parked in the parent is resolved
// with the settlement, including for receipts that settled but whose
// resolution a crash lost — and a delegating call a cut-off turn left
// dangling with no mirror (its child recovered without parking) is
// resolved the same way, never left offered for an approval that would
// delegate again. Last, the pump runs (Decide with no
// decisions): children whose requests were already decided resume.
//
// A cut-off child's recovery runs on a pool goroutine: Recover returns
// once it is started, not once it has parked or answered. Its receipt
// then reads unsettled (running, or parked once the recovery parks)
// and Wait on it blocks until the delegation is at rest — parked at a
// request for a person, or settled.
//
// Call it once per parent session a new process takes over, after the
// agents are wrapped or registered. The same holds in a process whose
// pool outlived a parent session closed under thread.ErrShutdown: the
// child turns that close cut off are let go by the pool (their
// receipts unsettled, no delegate claiming them), and a Recover of the
// reopened parent recovers them — a handler the cut-off left running
// is waited for by the child's own recovery. Until then Wait returns
// such a receipt as it stands, and Cancel recovers it first.
//
// A child whose agent the pool does not hold is reported with
// ErrNoAgent and left for a later call; the rest are recovered. The error is every failure joined. Recover on a
// session with nothing to recover does nothing; it is safe to call on
// a live session too — receipts whose children this pool is working
// are left alone.
func (p *Pool) Recover(ctx context.Context, parent *thread.Session) error {
	if parent == nil {
		return fmt.Errorf("thread/pool: Recover with no parent session")
	}
	if err := p.open(ctx); err != nil {
		return err
	}
	p.attach(parent)
	var errs []error
	open := openWrappers(parent)
	offered := map[string]bool{} // children with a request still pending
	for _, r := range parent.Pending() {
		offered[r.Child] = true
	}
	for _, rc := range Receipts(parent) {
		if p.working(rc) {
			continue
		}
		if !rc.Settled() {
			if err := p.recoverOne(ctx, parent, rc); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		p.forgetSettled(rc.Child)
		// A settlement whose aftermath a crash lost: requests still
		// offered for a delegation that is over, a delegating call
		// still parked beside it.
		if offered[rc.Child] {
			if err := parent.DenyMirrored(ctx, rc.Child, endedReason(rc.State)); err != nil {
				errs = append(errs, fmt.Errorf("thread/pool: requests of ended child %s: %w", rc.Child, err))
			}
		}
		// The call may also dangle with no mirror: a turn cut off while
		// the delegation ran, its child recovered without parking
		// (ResolveDelegation checks the receipt names the child). Not
		// one settled canceled: what that child did is not known, and
		// the call is a person's to decide (RecoverTurn's recovery
		// request), never resolved with the canceled text.
		if rc.Call != "" && (open[rc.Child] == rc.Call || rc.State != Canceled && delegatingCallPending(parent, rc)) {
			if err := p.reResolve(ctx, parent, rc); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if err := p.pump(ctx, parent); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// working reports whether this pool answers for rc's delegation now: a
// registered delegate, or a child it is still creating — counted until
// its registration, or, when the pool closed meanwhile, until its
// settlement is written.
func (p *Pool) working(rc Receipt) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, live := p.delegates[rc.ID]
	return live || p.creating[rc.Child] > 0
}

// recoverOne recovers one unsettled receipt no delegate of this pool
// answers for.
func (p *Pool) recoverOne(ctx context.Context, parent *thread.Session, rc Receipt) error {
	p.mu.Lock()
	creating := p.creating[rc.Child] > 0
	p.mu.Unlock()
	if creating {
		return nil // a live delegation whose child is being made: not left behind
	}
	h, entries, _, err := parent.Storage().Load(ctx, rc.Child)
	if rc.State == Accepted && noHeader(err) {
		err = neverCreated(ctx, parent.Storage(), rc.Child)
	}
	if errors.Is(err, thread.ErrNotFound) {
		stop := fmt.Sprintf("child session %s is gone", rc.Child)
		if rc.State == Accepted {
			// Its run never started (the running receipt precedes it):
			// accepted, and the process stopped before the child was
			// created — or it was deleted before it ran. Nothing ran.
			stop = fmt.Sprintf(neverCreatedStop, rc.Child)
		}
		return p.settleFromLedger(ctx, parent, rc, "", Failed, stop, core.Usage{})
	}
	if err != nil {
		return fmt.Errorf("thread/pool: child session %s: %w", rc.Child, err)
	}
	name := h.Meta[metaAgent]
	if p.foreign(parent, rc.Child, name) {
		// Another pool watching the parent holds this child — it may be
		// working it now: its Recover recovers it, never this pool's.
		return nil
	}
	if cutOff(entries) {
		// A crash or a shutdown cut the child's run off: what it did is
		// not known, so it is never settled failed — the parent could
		// delegate the work again over an effect the child had. The
		// child recovers on its own ladder (RecoverTurn) and the
		// delegation stays open: it parks (its calls of unknown outcome
		// mirrored for a person) or runs on to its answer. Recovery is
		// not built at depth: a cut-off child that was itself
		// delegating ends its own delegations first, depth first (as
		// below), and its delegating call — its outcome the ended
		// delegation's, not known — parks for a person (RecoverTurn's
		// rule for a call that delegated to a pool child).
		if hasLoose(entries) {
			if err := p.endSubtree(ctx, parent, rc.Child); err != nil {
				return err
			}
		}
		return p.recoverCutOff(ctx, parent, rc)
	}
	state, stop, usage, parked := classify(entries)
	if parked {
		return p.reattach(ctx, parent, rc)
	}
	if hasLoose(entries) {
		// A child that is itself a parent ends its own sync
		// delegations before it settles, as under Cancel and Close
		// (ADR 0022 §F): its run is over, nothing will resolve their
		// calls in it, and a parked one's requests would reach nobody.
		// When that cannot be done the receipt is left for a later
		// Recover, which ends them first.
		if err := p.endSubtree(ctx, parent, rc.Child); err != nil {
			return err
		}
		// The settlements just recorded in the child bill its subtree.
		usage = reread(ctx, parent.Storage(), rc.Child, usage)
	}
	return p.settleFromLedger(ctx, parent, rc, name, state, stop, usage)
}

// reread is a child's usage as classify reads it after its ledger
// changed under Recover (its own delegations ended), or was when the
// child cannot be read again.
func reread(ctx context.Context, st thread.Storage, childID string, was core.Usage) core.Usage {
	_, entries, _, err := st.Load(ctx, childID)
	if err != nil {
		return was
	}
	_, _, usage, _ := classify(entries)
	return usage
}

// hasLoose reports whether a session's entries hold a delegation that
// is not finished with: a pool receipt with no settlement, or a
// mirrored child request no later decision answers (a crash between a
// settlement and the denial of its requests).
func hasLoose(entries []thread.Entry) bool {
	open := map[string]bool{}
	mirrors := map[[2]string]bool{} // call id, run id
	for _, e := range entries {
		switch e := e.(type) {
		case thread.PoolReceiptEntry:
			switch {
			case e.Receipt == "":
				open[e.ID] = true
			case State(e.Status).Settled():
				delete(open, e.Receipt)
			}
		case thread.ApprovalRequestEntry:
			if e.Child != "" {
				mirrors[[2]string{e.CallID, e.RunID}] = true
			}
		case thread.ApprovalDecisionEntry:
			delete(mirrors, [2]string{e.CallID, e.RunID})
		}
	}
	return len(open) > 0 || len(mirrors) > 0
}

// endSubtree ends the delegations of a child of above whose run is
// over (recoverOne): the child session is opened under its agent to
// record their settlements in its ledger, and closed again.
func (p *Pool) endSubtree(ctx context.Context, above *thread.Session, childID string) error {
	child, err := p.openEnded(ctx, above, childID)
	if err != nil {
		return err
	}
	defer func() { _ = child.Close(context.WithoutCancel(ctx)) }()
	return p.endDelegations(ctx, child)
}

// openEnded opens a child of above whose delegation Recover is ending,
// under its registered or wrap-named agent, to write its ledger.
func (p *Pool) openEnded(ctx context.Context, above *thread.Session, childID string) (*thread.Session, error) {
	agent, _, err := p.agentForSession(ctx, above, childID)
	if err != nil {
		return nil, err
	}
	child, err := thread.Open(ctx, above.Storage(), childID, agent, thread.InheritApprovals(above),
		thread.WithRunMetadata(above.RunMeta()))
	if err != nil {
		return nil, fmt.Errorf("thread/pool: child session %s not reopened: %w", childID, err)
	}
	return child, nil
}

// endDelegations ends every unsettled delegation of s, a session whose
// run is over, that no child of this pool is working (endOne). A
// settled one's requests still offered in s — a crash between its
// settlement and their denial — are denied.
func (p *Pool) endDelegations(ctx context.Context, s *thread.Session) error {
	var errs []error
	for _, rc := range Receipts(s) {
		p.mu.Lock()
		_, live := p.delegates[rc.ID]
		p.mu.Unlock()
		if live {
			continue
		}
		if rc.Settled() {
			if err := s.DenyMirrored(ctx, rc.Child, endedReason(rc.State)); err != nil {
				errs = append(errs, fmt.Errorf("thread/pool: requests of ended child %s: %w", rc.Child, err))
			}
			continue
		}
		if err := p.endOne(ctx, s, rc); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// endOne ends one unsettled delegation of a session whose run is over,
// from the delegated child's file, depth first: a parked sync child is
// canceled at its boundary — its own delegations ended first, then its
// pending calls denied in its own session, never to run on a later
// approval — and settles canceled; a cut-off child settles canceled,
// never failed (what it did is not known), its own delegations ended
// first; a child whose run is over settles as classify reads it, its
// own delegations ended first; a gone one settles failed. Its requests still offered in s are denied. A parked
// async child is left parked: async delegations are independent of
// the run that made them (D4), and a Recover of s reattaches it.
// Nothing is resolved: s's delegating calls were never parked, and s
// does not run again.
func (p *Pool) endOne(ctx context.Context, s *thread.Session, rc Receipt) error {
	_, entries, _, err := s.Storage().Load(ctx, rc.Child)
	if rc.State == Accepted && noHeader(err) {
		err = neverCreated(ctx, s.Storage(), rc.Child)
	}
	if errors.Is(err, thread.ErrNotFound) {
		stop := fmt.Sprintf("child session %s is gone", rc.Child)
		if rc.State == Accepted {
			stop = fmt.Sprintf(neverCreatedStop, rc.Child)
		}
		return p.recordEnded(ctx, s, rc, Failed, stop, core.Usage{})
	}
	if err != nil {
		return fmt.Errorf("thread/pool: child session %s: %w", rc.Child, err)
	}
	state, stop, usage, parked := classify(entries)
	if cutOff(entries) {
		// Cut off too: what it did is not known — never "failed". It is
		// ended (nothing will resume it under a run that is over) and
		// its own delegations first; the call that delegated to it goes
		// to a person in s (RecoverTurn), its work possibly done.
		if hasLoose(entries) {
			if err := p.endSubtree(ctx, s, rc.Child); err != nil {
				return err
			}
			usage = reread(ctx, s.Storage(), rc.Child, usage)
		}
		return p.recordEnded(ctx, s, rc, Canceled, cutOffCanceledStop, usage)
	}
	switch {
	case parked:
		if !syncDelegation(s, rc) {
			return nil
		}
		child, err := p.openEnded(ctx, s, rc.Child)
		if err != nil {
			return err
		}
		defer func() { _ = child.Close(context.WithoutCancel(ctx)) }()
		if err := p.endDelegations(ctx, child); err != nil {
			return err
		}
		if err := child.CancelDelegated(ctx, cancelDenyReason); err != nil {
			return fmt.Errorf("thread/pool: child session %s: %w", rc.Child, err)
		}
		state, stop = Canceled, orphanCanceledStop
		u := child.Usage()
		usage = u.Turns.Add(u.Summaries).Add(u.Delegated)
	case hasLoose(entries):
		if err := p.endSubtree(ctx, s, rc.Child); err != nil {
			return err
		}
		usage = reread(ctx, s.Storage(), rc.Child, usage)
	}
	return p.recordEnded(ctx, s, rc, state, stop, usage)
}

// neverCreatedStop is the stop of a receipt Recover settles whose child
// session does not exist and whose run never started: the acceptance is
// written before the child is created, and a crash between the two
// leaves it so. Model-visible inside SUBAGENT_FAILED when the
// delegating call is still parked (ADR 0022, amendment 2026-11-05).
const neverCreatedStop = "child session %s does not exist and its run never started: nothing ran"

// cutOffCanceledStop is the stop of a cut-off child Recover ends
// because the run that delegated to it was cut off too: it is not known
// what it did.
const cutOffCanceledStop = "canceled after a restart cut it off while the run that delegated to it was cut off too: what it did is not known"

// orphanCanceledStop is the stop of a parked sync child Recover ends
// because the run that delegated to it did not survive a restart.
const orphanCanceledStop = "canceled while parked at an approval: the run that delegated to it did not survive a restart"

// syncDelegation reports whether rc, an unsettled receipt of s whose
// child is parked, is a sync delegation — its child's requests
// mirrored under a delegating call — rather than an async one. A child
// with no mirror (the crash window between its park and the mirror)
// counts as sync unless s holds a result for its delegating call, which
// an async delegation's receipt line is.
func syncDelegation(s *thread.Session, rc Receipt) bool {
	mirrored, answered, accepted := false, false, false
	for _, e := range s.Entries() {
		switch e := e.(type) {
		case thread.ApprovalRequestEntry:
			if e.Child == rc.Child {
				if e.Wrapper != "" {
					return true
				}
				mirrored = true
			}
		case thread.PoolReceiptEntry:
			if e.ID == rc.ID {
				accepted = true
			}
		case thread.MessageEntry:
			if !accepted || e.Message.Role != core.RoleTool {
				continue
			}
			for _, part := range e.Message.Content {
				if r, ok := part.(core.ToolResultPart); ok && r.CallID == rc.Call {
					answered = true
				}
			}
		}
	}
	return !mirrored && rc.Call != "" && !answered
}

// classify reads what a child session's file says became of its
// delegation: parked at an approval (its last turn left calls
// pending), or over — done with its final text, canceled, capped or
// failed with the recorded cause, or failed because no turn was ever
// recorded (the process died with the run in flight or still queued).
// usage is the session's whole cost, turns and summaries, and what its
// settled delegations cost (its own Delegated, as Session.Usage sums
// it): a settlement bills the child's subtree.
func classify(entries []thread.Entry) (state State, stop string, usage core.Usage, parked bool) {
	var last *thread.TurnEntry
	closed := false
	for _, e := range entries {
		switch e := e.(type) {
		case thread.TurnEntry:
			last = &e
			closed = false
			usage = usage.Add(e.Usage)
		case thread.CompactionEntry:
			usage = usage.Add(e.SummarizerUsage)
		case thread.PoolReceiptEntry:
			if e.Receipt != "" && State(e.Status).Settled() {
				usage = usage.Add(e.Usage)
			}
		case thread.ApprovalAuditEntry:
			// RecoverTurn found the cut-off run had answered and only
			// its end was lost: it wrote the end, the answer stands.
			closed = closed || (last != nil && e.Step == thread.StepRecover && e.CallID == "" && e.Outcome == "closed" && e.RunID == last.RunID)
		}
	}
	switch {
	case last != nil && last.CutOff && closed:
		return Done, finalText(entries), usage, false
	case last == nil:
		return Failed, "the child's run did not survive a restart: no turn was recorded", usage, false
	case len(last.Pending) > 0:
		return "", "", usage, true
	case last.Canceled:
		return Canceled, last.Err, usage, false
	case last.Err != "":
		if strings.Contains(last.Err, core.ErrMaxSteps.Error()) || strings.Contains(last.Err, core.ErrUsageLimit.Error()) {
			return Capped, last.Err, usage, false
		}
		return Failed, last.Err, usage, false
	}
	return Done, finalText(entries), usage, false
}

// cutOff reports whether a child's file shows a run that started and
// never ended on its own (ADR 0032), the rule the session's own
// recovery reads (thread.Session.RecoverTurn): the last run started —
// a prompt entry's run, a resume's "started" audit, a recovery's
// continuation (its "continued" audit's Detail), an overflow attempt's
// re-run — has no turn entry, or one marked CutOff, and no StepRecover
// audit has handled it. Its outcome is not known. A run is started
// from its start record until its own turn entry lands: a continuation
// or a resume cut off by a second crash is cut off too, never read as
// the first cut-off turn's failure.
func cutOff(entries []thread.Entry) bool {
	run := ""
	recovered := map[string]bool{}
	turns := map[string]thread.TurnEntry{}
	for _, e := range entries {
		switch e := e.(type) {
		case thread.MessageEntry:
			if e.RunID != "" {
				run = e.RunID
			}
		case thread.TurnEntry:
			turns[e.RunID] = e
		case thread.ApprovalAuditEntry:
			switch {
			case e.Step == thread.StepResume && e.Outcome == "started":
				run = e.RunID
			case e.Step == thread.StepRecover && e.CallID == "":
				recovered[e.RunID] = true
				if e.Outcome == "continued" && e.Detail != "" {
					run = e.Detail
				}
			}
		}
	}
	if run == "" {
		return false
	}
	for {
		te, ok := turns[run]
		if !ok || te.ReRun == "" || te.CutOff {
			break
		}
		run = te.ReRun
	}
	if recovered[run] {
		return false
	}
	te, ok := turns[run]
	return !ok || te.CutOff
}

// recoverCutOff reattaches a cut-off child and starts its recovery on
// a pool goroutine (arm, replayOrRecover → RecoverTurn): the receipt
// stays unsettled, and the delegation ends as the recovered child does.
func (p *Pool) recoverCutOff(ctx context.Context, parent *thread.Session, rc Receipt) error {
	d, err := p.delegateFor(ctx, parent, rc, wrapperOf(parent, rc))
	if errors.Is(err, errSettled) || errors.Is(err, errForeign) {
		return nil
	}
	if err != nil {
		return err
	}
	p.mu.Lock()
	gen := d.parks
	p.mu.Unlock()
	p.arm(d, gen, nil)
	return nil
}

// reattach rebuilds the bridge to a child parked at an approval:
// missing mirrors are written, and the receipt records parked.
func (p *Pool) reattach(ctx context.Context, parent *thread.Session, rc Receipt) error {
	d, err := p.delegateFor(ctx, parent, rc, wrapperOf(parent, rc))
	if errors.Is(err, errSettled) || errors.Is(err, errForeign) {
		return nil // settled since the caller read the ledger, or another pool's: nothing to reattach
	}
	if err != nil {
		return err
	}
	if err := p.mirror(ctx, d); err != nil {
		return fmt.Errorf("thread/pool: child session %s: %w", rc.Child, &unmirroredError{err})
	}
	if rc.State != Parked {
		if _, err := parent.AppendPoolReceipt(ctx, thread.PoolReceiptEntry{
			Receipt: rc.ID, Status: thread.PoolParked, Child: rc.Child,
		}); err != nil {
			return fmt.Errorf("thread/pool: parked receipt for %s: %w", rc.ID, err)
		}
	}
	return nil
}

// wrapperOf is the delegating call a reattached child's answer
// resolves, "" when none can: the call still open in the parent, or the
// one its mirrors name — unless the run that delegated died before its
// turn ended.
func wrapperOf(parent *thread.Session, rc Receipt) string {
	wrapper := ""
	if rc.Call != "" && (callPending(parent, rc.Call) || delegatingCallPending(parent, rc)) {
		// The delegating call is still open in the parent: the child's
		// requests park under it again, and its answer resolves it.
		wrapper = rc.Call
	}
	for _, e := range parent.Entries() {
		if re, ok := e.(thread.ApprovalRequestEntry); ok && re.Child == rc.Child && re.Wrapper != "" {
			wrapper = re.Wrapper
		}
	}
	if wrapper != "" && !turnLandedSince(parent, rc.ID) {
		// The run that delegated died before its turn ended: the
		// delegating call was never parked (the session's rule — no
		// turn entry, no boundary; the parent's next run repairs it)
		// and never will be. The child is parked in its own right and
		// its requests stay decidable; it resumes as a delegation by no
		// call would, and its answer settles the receipt. Reattached
		// under the call, its resume would wait for a park that never
		// comes.
		wrapper = ""
	}
	return wrapper
}

// turnLandedSince reports whether a turn of parent ended after
// receipt's acceptance: the run that delegated it — one runner per
// session — wrote its turn entry, parked or not.
func turnLandedSince(parent *thread.Session, receipt string) bool {
	accepted := false
	for _, e := range parent.Entries() {
		switch e := e.(type) {
		case thread.PoolReceiptEntry:
			if e.ID == receipt {
				accepted = true
			}
		case thread.TurnEntry:
			if accepted {
				return true
			}
		}
	}
	return false
}

// callPending reports whether callID is a delegating call a crash
// left open in parent: offered as pending, and with no request entry
// of its own — a tail the decision chain never saw, which is what a
// turn that died mid-delegation leaves. An ordinary parked call that
// merely reuses the id has a request entry, and is not it.
func callPending(parent *thread.Session, callID string) bool {
	for _, r := range parent.Pending() {
		if r.CallID == callID && r.Child == "" && !requestEntry(parent, r.ID) {
			return true
		}
	}
	return false
}

// delegatingCallPending reports whether rc's delegating call is pending
// in parent as a call of its own boundary (no Child) and rc is that
// call's latest delegation: a delegating call left unanswered — a
// cut-off turn's, or one parked as of unknown outcome when the pool cut
// its child off and left the receipt open. A later delegation under a
// reused call id has a later acceptance, and is not rc.
func delegatingCallPending(parent *thread.Session, rc Receipt) bool {
	latest := ""
	for _, e := range parent.Entries() {
		if pr, ok := e.(thread.PoolReceiptEntry); ok && pr.Receipt == "" && pr.Call == rc.Call {
			latest = pr.ID
		}
	}
	if latest != rc.ID {
		return false
	}
	for _, r := range parent.Pending() {
		if r.CallID == rc.Call && r.Child == "" {
			return true
		}
	}
	return false
}

// requestEntry reports whether id names a request entry of s — not the
// id Pending gives a dangling call that has none.
func requestEntry(s *thread.Session, id string) bool {
	for _, e := range s.Entries() {
		if re, ok := e.(thread.ApprovalRequestEntry); ok && re.ID == id {
			return true
		}
	}
	return false
}

// settleFromLedger settles a receipt whose child no delegate of this
// pool holds, from what the child's file says, and resolves the
// delegating call if it is still parked in the parent.
func (p *Pool) settleFromLedger(ctx context.Context, parent *thread.Session, rc Receipt, name string, state State, stop string, usage core.Usage) error {
	if err := p.recordEnded(ctx, parent, rc, state, stop, usage); err != nil {
		return err
	}
	rc.State, rc.Stop = state, stop
	return p.resolveFromLedger(ctx, parent, rc, name)
}

// recordEnded records a settlement read from a child's file in
// parent's ledger, forgets the child, and denies its requests still
// offered in parent.
func (p *Pool) recordEnded(ctx context.Context, parent *thread.Session, rc Receipt, state State, stop string, usage core.Usage) error {
	if _, err := parent.AppendPoolReceipt(ctx, thread.PoolReceiptEntry{
		Receipt: rc.ID, Status: string(state), Child: rc.Child, Stop: stop, Usage: usage,
	}); err != nil {
		return fmt.Errorf("thread/pool: settlement receipt for %s: %w", rc.ID, err)
	}
	p.mu.Lock()
	delete(p.sessionAgents, rc.Child)
	p.markRetired(rc.Child)
	p.mu.Unlock()
	if err := parent.DenyMirrored(ctx, rc.Child, endedReason(state)); err != nil {
		return fmt.Errorf("thread/pool: requests of ended child %s: %w", rc.Child, err)
	}
	return nil
}

// reResolve resolves the delegating call of a receipt that settled
// while the call stayed parked — the crash window between a
// settlement and its resolution.
func (p *Pool) reResolve(ctx context.Context, parent *thread.Session, rc Receipt) error {
	name := ""
	if h, _, _, err := parent.Storage().Load(ctx, rc.Child); err == nil {
		name = h.Meta[metaAgent]
	}
	return p.resolveFromLedger(ctx, parent, rc, name)
}

// openWrappers maps each child that has a mirror naming a delegating
// call to that call, when no decision recorded after the mirror has
// answered it — the cheap test, one pass over the parent's entries,
// before a resolution is attempted.
func openWrappers(parent *thread.Session) map[string]string {
	open := map[string]string{}  // child → wrapper
	owner := map[string]string{} // wrapper → child
	for _, e := range parent.Entries() {
		switch e := e.(type) {
		case thread.ApprovalRequestEntry:
			if e.Child != "" && e.Wrapper != "" {
				if prev, ok := owner[e.Wrapper]; ok && prev != e.Child {
					delete(open, prev) // the call id moved on to a later delegation
				}
				open[e.Child], owner[e.Wrapper] = e.Wrapper, e.Child
			}
		case thread.ApprovalDecisionEntry:
			if child, ok := owner[e.CallID]; ok {
				delete(open, child)
				delete(owner, e.CallID)
			}
		}
	}
	return open
}

// resolveFromLedger resolves a settled receipt's delegating call with
// the outcome the ledger records. A call that delegates to this child
// no longer — resolved already, or never parked — is not an error.
func (p *Pool) resolveFromLedger(ctx context.Context, parent *thread.Session, rc Receipt, name string) error {
	if rc.Call == "" {
		return nil
	}
	d := &delegate{name: name}
	content, isErr := rc.Stop, false
	switch rc.State {
	case Done:
	case Canceled:
		content, isErr = d.canceledText(), true
	default:
		content, isErr = d.failureText(rc.Stop), true
	}
	_, err := parent.ResolveDelegation(ctx, rc.Call, rc.Child, content, isErr)
	if err != nil && !errors.Is(err, thread.ErrNotPending) {
		return fmt.Errorf("thread/pool: delegating call %s of receipt %s: %w", rc.Call, rc.ID, err)
	}
	return nil
}

// finalText is a finished child's answer read back from its entries:
// the text of its last assistant message. A child built with Output
// answers through its submit call, which a live delegation reads off
// the run's result; recovered from the file, the answer is the text.
func finalText(entries []thread.Entry) string {
	for i := len(entries) - 1; i >= 0; i-- {
		me, ok := entries[i].(thread.MessageEntry)
		if !ok || me.Message.Role != core.RoleAssistant {
			continue
		}
		var b strings.Builder
		for _, part := range me.Message.Content {
			if tp, ok := part.(core.TextPart); ok {
				b.WriteString(tp.Text)
			}
		}
		return b.String()
	}
	return ""
}

// noHeader reports whether err is a child session's load failing on a
// file with no complete header line — line 1 corrupt, the file ending
// before its header did (jsonl wraps io.ErrUnexpectedEOF): what a crash
// inside the child's Create leaves, the exclusive open done and the
// header not yet durable. Any other corruption is not this, and stays
// an error.
func noHeader(err error) bool {
	var ce *thread.CorruptError
	return errors.As(err, &ce) && ce.Line == 1 && errors.Is(err, io.ErrUnexpectedEOF)
}

// neverCreated settles what a header-less stub under an accepted
// receipt is: a child whose creation never completed, so it never ran.
// The stub is removed first (a crash after it reads as absent, and the
// next Recover settles it the same way); a removal that fails is
// returned, nothing settled, and the next Recover tries again. It
// returns ErrNotFound for the caller's never-created path. The caller
// has checked no delegation of this pool is creating the child.
func neverCreated(ctx context.Context, st thread.Storage, child string) error {
	if err := st.Delete(ctx, child); err != nil && !errors.Is(err, thread.ErrNotFound) {
		return fmt.Errorf("thread/pool: child session %s holds no header (its creation never completed) and could not be removed: %w", child, err)
	}
	return fmt.Errorf("%w: child session %s never completed its creation", thread.ErrNotFound, child)
}
