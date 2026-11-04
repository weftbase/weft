package thread

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/weftbase/weft/core"
)

// Outcome is a decision's kind (ADR 0021 §1): approve (run the call
// through the ordinary chain), deny with a reason, or resolve with
// content computed outside the process, resolve_error marking it an
// error. The wire values are the decision entry's "outcome" field:
// stored bytes, so a value this build writes is one it keeps reading
// (the format goldens pin them); a decision entry carrying any other
// value never resolves a call.
type Outcome string

const (
	// OutcomeApprove runs the call: the resume executes it through the
	// ordinary tool chain with Call.Approved set (ADR 0007).
	OutcomeApprove Outcome = "approve"
	// OutcomeDeny resolves the call without running it: the model sees
	// "DENIED: " followed by the decision's Reason.
	OutcomeDeny Outcome = "deny"
	// OutcomeResolve resolves the call with the decision's Content as
	// its result, verbatim; the handler never runs.
	OutcomeResolve Outcome = "resolve"
	// OutcomeResolveError is OutcomeResolve with the result marked as
	// an error.
	OutcomeResolveError Outcome = "resolve_error"
	// OutcomeEdit corrects the call's arguments (Edit): the request is
	// closed and a revision opened — a new request for the same call on
	// the edited arguments (Request.RevisionOf), which the editor's
	// decision approves once. It resolves like any request: alone under
	// no quorum, with n distinct approvers in all, the editor counted,
	// under Quorum(n); approvals of the old arguments never count. The
	// resume runs the call on the revision's arguments
	// (core.ApproveEdited). A recovery request takes no edit.
	OutcomeEdit Outcome = "edit"
)

// The audit entry's steps (ADR 0021 §2): every step the session takes
// over a parked call leaves an approval_audit entry naming its step.
const (
	StepGrant    = "grant"    // a matching live grant decided; GrantID names it
	StepApprover = "approver" // the live chain step, consulted and bounded
	StepPark     = "park"     // the request persisted, the turn ended pending
	StepExpiry   = "expiry"   // an expired request denied
	StepResume   = "resume"   // the boundary's resume run: one entry "started" before the run, one "completed" or "failed" with its end
	StepSigned   = "signed"   // a signed decision refused; Detail names why, no decision recorded
	StepRecover  = "recover"  // RecoverTurn: per run, Outcome continued (Detail: the new run), parked, resumed or closed; per call, "retry" (Detail: the replay class) beside the session's own approval
	StepDispatch = "dispatch" // the dispatch gate let a call that is not ReplaySafe start: appended and flushed before its handler runs (ADR 0032)
)

// The channels a decision entry's Via names. Decide records "user",
// DecideSigned "signed"; the rest are the session's own paths.
const (
	viaUser      = "user"
	viaSigned    = "signed"
	viaApprover  = "approver"
	viaGrant     = "grant"
	viaExpiry    = "expiry"
	viaInterrupt = "interrupt"
	viaChild     = "child"
	viaParent    = "parent" // a pool child's record of a decision its parent session took (ADR 0022 §7)
	viaQuorum    = "quorum"
	viaRecovery  = "recovery" // RecoverTurn's own retry of a cut-off ReplaySafe or ReplayKeyed call (ADR 0032)
)

// ErrInvalidDecision is returned by Decide for a batch that cannot
// be recorded as written: no decisions at all, a decision without an
// outcome, or two decisions naming the same call — one call takes one
// verdict per Decide, and a batch that both approves and denies it
// says nothing the session could apply. Nothing is recorded.
var ErrInvalidDecision = errors.New("thread: invalid decision")

// ErrDelegated is returned by Decide, DecideSigned and Request for a
// call that delegates to a thread/pool child session (ADR 0022 §7): a
// call some mirrored child request names as its Wrapper. Such a call
// is parked because its child is, and it completes with the child's
// answer — never by a decision of its own: approving it would run the
// delegation a second time, in a second child session. The session's
// own decision chain keeps the same rule without an error: a grant or
// an Approver is never consulted for such a call. Decide the
// child's requests (Pending lists them, Child naming the session);
// the pool resolves the call when the child ends.
var ErrDelegated = errors.New("thread: call is delegated to a child session")

// Request is a parked call awaiting a decision (ADR 0021 §1): what the
// model asked for, hashed and named so a decision can state exactly
// what it decided. Session.Pending returns the undecided ones; they
// survive restarts because they are entries.
type Request struct {
	// ID is the request entry's id: the name of this one occurrence of
	// the call. Call ids repeat across turns; this never does, and a
	// signed decision is bound to it. Empty on the Request an Approver
	// is handed (the chain asks before it persists). A dangling call
	// with no request entry is named by a deterministic id ("r_" and 26
	// hex digits, no entry's), so a bound decision can decide it.
	ID string
	// Session is the session the request lives in.
	Session string
	// CallID is the pending call's id — the key decisions address and
	// the core's Approve/Deny/Resolve key on.
	CallID string
	// Tool is the call's tool name.
	Tool string
	// Args is the call's arguments, verbatim as the model wrote them.
	Args json.RawMessage
	// ArgsSHA256 is the hex SHA-256 of Args — a decision names what it
	// decided by this hash, and a signed decision carries it in the
	// challenge.
	ArgsSHA256 string
	// RunID is the run that parked the call.
	RunID string
	// Reason is why the call parked: "tool requires approval" for a
	// RequireApproval tool, "middleware required approval" when the
	// tool chain parked it.
	Reason string
	// Expiry is when the request lapses — zero means never. A request
	// is expired strictly after this time, never at it: from then on
	// it takes no decision and is denied with the stated reason (ADR
	// 0021 §5).
	Expiry time.Time
	// Created is when the request was persisted.
	Created time.Time
	// Nonce is the challenge a signed decision answers (ADR 0021 §3):
	// minted by Session.Request, single-use, bound to this request
	// entry — and empty on the Pending view, which mints no challenge.
	Nonce string
	// KeyID names the keyring key the challenge was minted under — the
	// ring's active key, and the key Keyring.Sign and SignDecision sign
	// with. An approver holding a key of their own signs with Key.Sign
	// instead, which names that key. Set by Session.Request beside
	// Nonce; empty on the Pending view.
	KeyID string
	// Child names the delegated session this call parks in, when the
	// call is a pool child's mirrored onto this parent (ADR 0022 §7):
	// the lineage a decision routes by — decide it through the pool,
	// which resumes the child and then completes the delegation. Empty
	// on an ordinary request.
	Child string
	// Quorum is how many approvals from distinct identities the call
	// needs before it resolves — the request's own, recorded when it
	// parked, or the session's Quorum if higher; 0 when one decision
	// resolves it.
	Quorum int
	// Metadata is the run metadata the parked run ran under, as its
	// request recorded it (ApprovalRequestEntry.Run): the session's
	// run metadata (WithRunMetadata) merged under the parking Send's
	// own context and core.Metadata pairs — the caller's labels, under
	// its own keys, readable by whichever process reopens the session.
	// weft never reads it. A copy; nil when the run carried none.
	Metadata map[string]string
	// RevisionOf names the request this one revises (OutcomeEdit): its
	// Args are an editor's, not the model's, and the resume runs them.
	// Empty on every other request.
	RevisionOf string
	// Recovery marks a recovery request (ADR 0032): a call a crash or
	// a shutdown left without a result, parked by Session.RecoverTurn
	// under the run that was cut off. Approve retries it with the same
	// IdempotencyKey; Deny or Resolve skips it; ApproveAlways is
	// refused (ErrInvalidDecision). Reason says what is known of its
	// outcome.
	Recovery bool
	// IdempotencyKey is the key the call runs under when it is
	// approved (IdempotencyKey: from the session, the assistant entry
	// holding the call and the call id) — the handler reads the same
	// value as core.Call.IdempotencyKey, on whichever worker resumes it,
	// and a recovery retry keeps it. Empty on a mirrored child request
	// (the child's own Pending names it).
	IdempotencyKey string
}

// Decision is one call's outcome, the value Decide records (ADR 0021
// §1). Build one with Approve, Deny, Resolve or ResolveError; Who is
// the caller's to fill.
type Decision struct {
	// CallID is the pending call the decision addresses.
	CallID string
	// Kind is the outcome: approve, deny, resolve or resolve_error.
	Kind Outcome
	// Reason is the deny reason — the text the model sees after
	// "DENIED: ".
	Reason string
	// Content is the resolve payload, verbatim; resolve_error marks it
	// an error.
	Content string
	// Who decided, for the audit trail. Through Decide it is a
	// declaration — whatever the caller wrote, verified by nobody —
	// and it is the identity Quorum counts for unsigned decisions.
	// Through DecideSigned it is a label only: the signing key is the
	// identity.
	//
	// The run that applies the decision carries it (a signed one's key
	// id instead) as weft.approval.actor run metadata, which survives
	// content-off like every metadata pair: it rides every span and
	// record verbatim, to every destination. Put an opaque id in Who
	// (and in a keyring KeyID), never an email or a name.
	Who string
	// Via names the channel the decision arrived by. The session fills
	// it on every path it records — Decide always records "user",
	// DecideSigned "signed", whatever this field holds; only an
	// Approver's answer keeps a Via of its own ("approver" when
	// empty).
	Via string
	// Always approves and grants the same thing for the future: the
	// tool plus the call's exact arguments become a session grant
	// ("approve and always allow this", ADR 0021 §4), minted when the
	// call's effective verdict is approve — never beside a denial.
	// Build it with ApproveAlways.
	Always bool
	// RequestID and ArgsSHA256 bind the decision to what its decider
	// was shown — the Request's ID (the occurrence) and ArgsSHA256 (the
	// arguments). Both are optional; when set, the session checks them
	// against the pending request before anything is recorded: another
	// occurrence of the call id fails with ErrNotPending, other
	// arguments with ErrArgsChanged. A decider that shows a request
	// in one place and decides it in another (an inbox, a web page
	// left open for days) copies both from the Request it showed, so a
	// stale page can never decide a call it did not show. ArgsSHA256
	// is recorded on the decision entry as the arguments the decider
	// saw; DecideSigned fills both from the verified challenge.
	RequestID  string
	ArgsSHA256 string
	// EditedArgs are an OutcomeEdit's arguments: a JSON object, recorded
	// and hashed in canonical form (Edit). Empty on every other outcome.
	EditedArgs json.RawMessage
	// Cosigners names everyone a remote authority counted for this
	// decision — the deciders behind one forwarded decision, when an
	// authority outside the session counts its own quorum and forwards
	// once. Recorded on the decision entry and covered by a signature's
	// MAC when present; weft never counts it toward its own Quorum.
	// Opaque ids, never addresses.
	Cosigners []string
}

// Edit returns a Decision that corrects the call's arguments (ADR
// 0021's 2026-11-04 amendment): the request is closed and a revision
// opened on args, in canonical JSON (keys sorted, no insignificant
// space, no HTML escaping), and the decision counts as the editor's
// approval of the revision. Under Quorum(n) the revision still needs
// n distinct approvers, the editor one of them; approvals of the old
// arguments never count. The resume runs the call on the edited
// arguments and tells the model so. Re-checking policy against the
// edited arguments is the decider's (an edit from 40 to 900); the
// tool validates them as any call's. Bind it to the request shown, as
// every Decide decision.
func Edit(callID string, args json.RawMessage) Decision {
	d := Decision{CallID: callID, Kind: OutcomeEdit, EditedArgs: slices.Clone(args)}
	if c, err := canonicalArgs(args); err == nil {
		d.EditedArgs = c
	}
	return d
}

// Approve returns a Decision that resumes the call: the run executes
// it through the ordinary tool chain with Call.Approved set (ADR
// 0007).
func Approve(callID string) Decision {
	return Decision{CallID: callID, Kind: OutcomeApprove}
}

// ApproveAlways returns a Decision that approves the call and grants
// the same thing for the future (ADR 0021 §4's "approve and always
// allow this command"): the tool plus the call's exact arguments
// become a session grant — so the next such call never parks. The
// grant is minted only when the call's effective verdict is approve:
// in the same append as the decision when one approval resolves the
// call, and with the approval that completes the quorum under Quorum
// — a call that ends denied leaves no grant behind. A richer grant (a
// command glob, a path prefix) is s.Grant's to make.
func ApproveAlways(callID string) Decision {
	return Decision{CallID: callID, Kind: OutcomeApprove, Always: true}
}

// Deny returns a Decision that resolves the call without running it:
// the model sees "DENIED: <reason>" and the loop continues. On a
// recovery request (Request.Recovery) it skips a call that may have
// run: the model reads an OUTCOME_UNKNOWN result saying not to repeat
// it, the reason appended (ADR 0032).
func Deny(callID, reason string) Decision {
	return Decision{CallID: callID, Kind: OutcomeDeny, Reason: reason}
}

// Resolve returns a Decision that resumes the call with a result
// computed outside the process — the human-as-tool-executor shape. The
// handler never runs; the content becomes the call's result verbatim.
func Resolve(callID, content string) Decision {
	return Decision{CallID: callID, Kind: OutcomeResolve, Content: content}
}

// ResolveError is Resolve with the result marked as an error.
func ResolveError(callID, content string) Decision {
	return Decision{CallID: callID, Kind: OutcomeResolveError, Content: content}
}

// Approver is the decision chain's live step (ADR 0021 §2): the "ask
// now" path for a terminal or an already-connected UI. It returns the
// Decision and true when it decided, or false to decline (a pipe, no
// TTY). It is never given unbounded time: the session consults it
// under WithApprover's timeout, and a timeout reads as a decline. It
// must not call back into the same Session — the chain consults it
// outside the session lock, but the turn is still running. The Request
// it is handed has no ID, Nonce or Created: the chain asks before the
// request is persisted.
type Approver func(ctx context.Context, r Request) (Decision, bool)

// Request builder options below (SessionOptions).

type approverOption struct {
	a       Approver
	timeout time.Duration
}

func (o approverOption) applySession(c *sessionConfig) {
	if o.a != nil {
		c.approver, c.approverTimeout = o.a, o.timeout
	}
}

// WithApprover sets the decision chain's live step (ADR 0021 §2) and
// the time it is given. The Approver is consulted for a call about to
// park, after grants and before parking, under a context with the
// timeout as its deadline; a timeout or a panic reads as a decline,
// audited, and the call parks. The timeout is part of the option
// because an Approver without one would either never be consulted or
// block the turn forever: it must be positive — Create and Open fail
// on a non-positive timeout rather than leave the Approver silently
// unconsulted. A session with no interactive channel sets no Approver
// at all. A nil approver is ignored.
func WithApprover(a Approver, timeout time.Duration) SessionOption {
	return approverOption{a: a, timeout: timeout}
}

type autoResumeOption bool

func (o autoResumeOption) applySession(c *sessionConfig) { c.autoResume = bool(o) }

// AutoResume sets whether the session resumes on its own once every
// pending call of the parked boundary has a decision (ADR 0021 §1).
// The default is on; AutoResume(false) parks the decided boundary
// until the caller calls Resume — Session.Ready lists it, and under
// OnReady (which implies AutoResume(false)) the session hands it to
// the callback. Either way a plain Send never runs
// while a boundary is open — only the resume run resolves it.
func AutoResume(on bool) SessionOption { return autoResumeOption(on) }

type onRequestOption func(Request)

func (o onRequestOption) applySession(c *sessionConfig) {
	if o != nil {
		c.onRequest = o
	}
}

// OnRequest sets the notification fired when a request parks (ADR 0021
// §5): called after the request entry is durable, once per parked
// call, in call order, so an application can push it anywhere. fn
// runs synchronously on the session's runner goroutine, before the
// parking turn's Wait returns and before the next turn can start: a
// slow fn holds the session, so hand the request to a queue or a
// goroutine and return. It holds no session lock — fn may call
// Pending or Request — but it must not wait for the turn that is
// calling it. The session does no I/O of its own for this; a panic in
// fn is contained and logged, never failing the turn. A nil fn is
// ignored.
func OnRequest(fn func(Request)) SessionOption { return onRequestOption(fn) }

type requestExpiryOption time.Duration

func (o requestExpiryOption) applySession(c *sessionConfig) { c.requestExpiry = time.Duration(o) }

type quorumOption int

func (o quorumOption) applySession(c *sessionConfig) { c.quorum = int(o) }

// Quorum sets how many approvals from distinct approver identities a
// call needs before it resolves (ADR 0021 §5). Values below 2 read as
// the default — one decision resolves. A deny resolves alone, and
// conflicting decisions resolve to deny with the pinned reason.
//
// What an identity is depends on the door the approval came through.
// A signed approval (DecideSigned) is its key: one keyring key is one
// approver, whatever Who the signature carries — give every approver
// their own key. An unsigned approval (Decide, an Approver's answer)
// is its Who, which is only what the caller declared: one caller can
// write two names. A grant's approval is one identity of its own. So
// a quorum that must hold against the deciding process itself needs
// RequireSigned; without it Quorum is a workflow rule among callers
// that are trusted to say who they are.
func Quorum(n int) SessionOption { return quorumOption(n) }

// RequestExpiry gives every request the session parks a lifetime: a
// request strictly past Created plus d takes no decision any more —
// Decide and DecideSigned refuse one with ErrExpired — and is denied
// with the stated reason the next time the session looks at the
// boundary: a Decide, a Resume, a Send, or the runner's own pickup
// (ADR 0021 §5). Nothing fires on a timer: an idle session denies an
// expired request when it is next touched. ErrExpired answers the
// path that finds the lapse; once one has denied it, the call is no
// longer pending, and a later Decide, DecideSigned or Request for it
// fails with ErrNotPending, as for any decided call. The runner's
// pickup runs right after a parked turn's Wait returns, so a request
// that lapses in that moment can be denied before the caller's next
// call. Values <= 0 (the default) mean requests never expire.
func RequestExpiry(d time.Duration) SessionOption { return requestExpiryOption(d) }

// approvalNow is the one clock every approval path reads — request
// and grant expiry, the challenge's lapse, the entries' timestamps —
// so the session's clock, when it has one, is a one-line redirect.
func (s *Session) approvalNow() time.Time { return s.now().UTC() }

// Pending returns the session's undecided requests, in call order:
// the calls the leaf's path leaves dangling that have an approval
// request and no decision yet. It works after a restart — requests and
// decisions are entries — and it never shows a call whose boundary a
// resume already resolved.
func (s *Session) Pending() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.offeredPendingLocked()
}

// offeredPendingLocked is Pending's view of the boundary (ADR 0022 §7):
// the raw pending, minus a delegating wrapper call a mirrored child
// request parks under (the wrapper completes through its child, never
// by a direct decision — Decide refuses it with ErrDelegated), plus
// the mirrored child requests themselves — with their lineage, Child
// naming the session a decision resumes. A wrapper is hidden by
// occurrence, not by id: call ids repeat across turns (ADR 0007), and
// a later, ordinary call that reuses a wrapper's id is offered like
// any other. pendingLocked stays the raw truth the resume machinery
// reads: the wrapper is pending there until the pool resolves it,
// which is what holds a plain Send while a nested approval is open.
// Callers hold s.mu.
func (s *Session) offeredPendingLocked() []Request {
	pending := s.pendingLocked()
	if len(pending) == 0 {
		return nil // nothing to hide a wrapper from: no walk owed
	}
	wrappers := s.approvalWalkLocked().wrappers
	var out []Request
	for _, r := range pending {
		if r.Child == "" && wrappers[r.CallID] != "" {
			continue
		}
		out = append(out, r)
	}
	return out
}

// Decide records decisions over the session's pending calls, durably,
// and resumes the boundary when they complete it (ADR 0021 §1). It is
// the unsigned, in-process door: every decision is recorded with Via
// "user" — whatever its Via field holds — and Who exactly as the
// caller declared it; a session under RequireSigned rejects the call
// with ErrSignatureRequired.
//
// The batch is validated whole before anything is recorded, and
// recorded in one atomic append: no decisions, a decision without an
// outcome, or two decisions for one call fail with ErrInvalidDecision;
// a decision for a call that is not pending — or naming a RequestID
// that is not the pending occurrence's — fails with ErrNotPending; one
// naming an ArgsSHA256 that is not the request's with ErrArgsChanged;
// one for a delegating call with ErrDelegated; a decision for a
// request past its expiry fails with ErrExpired. On
// any of them none of the batch is recorded and no run starts on its
// account.
//
// A call that delegates to a thread/pool child (ADR 0022 §7) takes no
// decision: a batch naming one fails with ErrDelegated. The rule is
// the same on every session — a pool child included, whose parent's
// decisions reach it through the pool's own replay, not this door.
//
// Expiry is resolved first, on every call: each pending request
// strictly past its expiry is denied on the spot with the stated
// reason (an expiry audit step and a decision with Via "expiry"),
// before the batch is looked at — which is why a decision for one is
// ErrExpired, never an approval of a lapsed request. Those denials
// stand even when Decide then returns an error, and when they
// complete the boundary under AutoResume its resume starts: reach it
// through the parked turn's Next, or Resume.
//
// When, after recording, every pending call of the boundary has a
// decision and AutoResume is on (the default), the resume run starts
// and Decide returns its Turn; otherwise the return is (nil, nil) and
// the caller drives Resume. The undecided calls a Resume-driven run
// carries are denied with the core's "no decision" text.
func (s *Session) Decide(ctx context.Context, ds ...Decision) (*Turn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cfg.requireSigned {
		return nil, fmt.Errorf("%w: session %s", ErrSignatureRequired, s.header.ID)
	}
	wrappers := s.approvalWalkLocked().wrappers
	batch := make([]recordedDecision, len(ds))
	for i, d := range ds {
		if d.RequestID == "" || d.ArgsSHA256 == "" {
			// A decision states what it decided (R5, WR-12): the request
			// occurrence and the arguments shown. Bind it to the Request
			// it answers (Decision.Bind).
			return nil, fmt.Errorf("%w: the decision for call %q names no request and no arguments; bind it to the Request it answers (Decision.Bind)",
				ErrInvalidDecision, d.CallID)
		}
		if child := wrappers[d.CallID]; child != "" {
			return nil, fmt.Errorf("%w: call %q completes with child session %s", ErrDelegated, d.CallID, child)
		}
		d.Via = viaUser // the door names the channel, never the caller
		batch[i] = recordedDecision{Decision: d}
	}
	return s.decideLocked(ctx, batch, true)
}

// recordedDecision is a decision on its way into the file: what was
// decided, plus the signed path's record — the nonce it answered and
// the key that vouched for it, both empty on every unsigned path.
type recordedDecision struct {
	Decision
	nonce string
	keyID string
}

// decideLocked is Decide's body after its door check: expiry is
// resolved, the batch is refused when it addresses a request that
// lapsed, then it is recorded — one decision per call when
// onePerCall — and the boundary resumes when that completes it.
// Callers hold s.mu.
func (s *Session) decideLocked(ctx context.Context, batch []recordedDecision, onePerCall bool) (*Turn, error) {
	expired, err := s.resolveExpiredLocked(ctx)
	if err != nil {
		return nil, err
	}
	// fail returns err — after arming the resume the expiry denials
	// may have completed: they are durable whatever became of the
	// batch, and AutoResume's contract does not wait for a caller who
	// got an error.
	fail := func(err error) (*Turn, error) {
		if len(expired) > 0 {
			if _, aerr := s.armSettledLocked(context.WithoutCancel(ctx)); aerr != nil {
				s.agent.Logger().Error("thread: auto-resume arm failed",
					"session", s.header.ID, "err", aerr)
			}
		}
		return nil, err
	}
	for _, d := range batch {
		for _, r := range expired {
			if r.CallID == d.CallID {
				return fail(fmt.Errorf("%w: call %q: %s", ErrExpired, d.CallID, expiryDetail(r.Expiry)))
			}
		}
	}
	if err := s.recordDecisionsLocked(ctx, batch, onePerCall); err != nil {
		return fail(err)
	}
	// The live context, by design: a decider whose context dies after
	// the call reads "canceled before it started" and retries with a
	// live one — the pinned TestResumeRetryAfterCanceledArm contract.
	// The entries are already durable; nothing is lost.
	return s.armSettledLocked(ctx)
}

// recordDecisionsLocked validates a batch against the pending calls
// and records it in one atomic append — the recorder every path that
// decides a parked call ends in: Decide and DecideSigned through
// decideLocked, the interrupting Send, the pool's delegation
// resolution. It holds no signature rule (RequireSigned is the
// exported Decide's own) and resolves no expiry (its callers do).
// With onePerCall a batch naming one call twice is refused; without
// it — ReplayDecisions, the pool's replay into a child session — the
// decisions for one call are recorded in the order given, like so
// many batches. The
// grant an Always decision asks for joins the same append — but only
// with the decision that makes the call's effective verdict, over
// every decision the occurrence then holds, an approve. Callers hold
// s.mu.
func (s *Session) recordDecisionsLocked(ctx context.Context, batch []recordedDecision, onePerCall bool) error {
	if len(batch) == 0 {
		return fmt.Errorf("%w: Decide with no decisions", ErrInvalidDecision)
	}
	pending := s.pendingLocked()
	byCall := make(map[string]Request, len(pending))
	for _, r := range pending {
		byCall[r.CallID] = r
	}
	// Deciding is idempotent (G3, G4): the same decision again — a
	// platform re-forwarding what it recorded, a retried request — is a
	// no-op, not an error, when it names the request it decided and
	// matches what was recorded for it. Without a request id a retry
	// is ambiguous and never takes this path.
	kept := batch[:0:0]
	var inForce approvalWalk
	walked := false
	for _, d := range batch {
		req, ok := byCall[d.CallID]
		if (!ok || d.RequestID != req.ID) && s.alreadyDecidedLocked(d) {
			continue
		}
		if ok && d.RequestID == req.ID && d.RequestID != "" {
			// Still pending — under Quorum, one approval short: the same
			// decision again counts nothing and records nothing. Only a
			// decision in force repeats (a spent one, on another line of
			// the tree, does not: the call reads pending again).
			if !walked {
				inForce, walked = s.approvalWalkLocked(), true
			}
			ds := scopedDecisions(inForce.decisions[req.CallID], req.RunID)
			if req.Child == "" {
				_, ds = inForce.occurrence(req.CallID)
			}
			if slices.ContainsFunc(ds, func(de ApprovalDecisionEntry) bool { return de.RequestID == d.RequestID && sameDecision(de, d) }) {
				continue
			}
		}
		kept = append(kept, d)
	}
	if len(kept) == 0 {
		return nil // every decision was already recorded
	}
	batch = kept
	seen := make(map[string]bool, len(batch))
	for _, d := range batch {
		if !validOutcome(d.Kind) {
			return fmt.Errorf("%w: decision for call %q has no outcome", ErrInvalidDecision, d.CallID)
		}
		if seen[d.CallID] && onePerCall {
			return fmt.Errorf("%w: call %q decided twice in one batch", ErrInvalidDecision, d.CallID)
		}
		seen[d.CallID] = true
		req, ok := byCall[d.CallID]
		if !ok {
			return fmt.Errorf("%w: call %q", ErrNotPending, d.CallID)
		}
		if d.RequestID != "" && d.RequestID != req.ID {
			return fmt.Errorf("%w: call %q: the decision answers request %q, the pending one is %q",
				ErrNotPending, d.CallID, d.RequestID, req.ID)
		}
		if d.ArgsSHA256 != "" && d.ArgsSHA256 != req.ArgsSHA256 {
			return fmt.Errorf("%w: call %q: decided over arguments %s, the request holds %s",
				ErrArgsChanged, d.CallID, d.ArgsSHA256, req.ArgsSHA256)
		}
		if err := recoveryDecisionErr(req, d.Decision); err != nil {
			return err
		}
		if d.Kind == OutcomeApprove && req.Recovery && s.handlerRunningLocked(req) {
			return fmt.Errorf("%w: call %q (request %s): approving retries it; approve again once its handler has returned",
				ErrStillRunning, d.CallID, req.ID)
		}
		if d.Kind == OutcomeEdit {
			if _, err := canonicalArgs(d.EditedArgs); err != nil || !jsonObject(d.EditedArgs) {
				return fmt.Errorf("%w: the edit of call %q carries no JSON object of arguments", ErrInvalidDecision, d.CallID)
			}
			if d.Always {
				return fmt.Errorf("%w: the edit of call %q asks for a standing grant; grant the edited arguments with Grant", ErrInvalidDecision, d.CallID)
			}
		}
	}
	// One atomic Append carries every decision — and every grant an
	// Always decision makes: a partial recording of a batch cannot
	// happen (the storage's all-or-nothing rule).
	walk := s.approvalWalkLocked()
	now := s.approvalNow()
	var entries []Entry
	parent := s.leaf
	held := map[string][]ApprovalDecisionEntry{} // per call: the occurrence's decisions, this batch's included
	for _, d := range batch {
		req := byCall[d.CallID]
		before, known := held[d.CallID]
		if !known {
			if req.Child != "" {
				before = scopedDecisions(walk.decisions[req.CallID], req.RunID)
			} else {
				_, before = walk.occurrence(req.CallID)
			}
			before = slices.Clone(before)
		}
		e := ApprovalDecisionEntry{
			CallID:     d.CallID,
			Outcome:    d.Kind,
			Reason:     d.Reason,
			Content:    d.Content,
			Who:        d.Who,
			Via:        d.Via,
			RunID:      req.RunID,
			Nonce:      d.nonce,
			KeyID:      d.keyID,
			RequestID:  req.ID,
			ArgsSHA256: d.ArgsSHA256,
			// Only an approval grants: a Deny or Resolve built with
			// Always set records no wish for a standing approval.
			Always: d.Always && d.Kind == OutcomeApprove,
		}
		e.Cosigners = slices.Clone(d.Cosigners)
		if d.Kind == OutcomeEdit {
			// An edit closes the request and opens a revision on the
			// edited arguments, which the editor approves once (D1): the
			// edit entry, the revision, the editor's approval — one
			// append, so no reader ever sees the one without the others.
			edited, _ := canonicalArgs(d.EditedArgs)
			e.EditedArgs, e.EditedArgsSHA256 = edited, hashArgs(edited)
			if e.ArgsSHA256 == "" {
				e.ArgsSHA256 = req.ArgsSHA256
			}
			e.ID, e.ParentID, e.Created = s.mintIDLocked(), parent, now
			entries = append(entries, e)
			old := walk.requests[req.CallID]
			if old.ID != req.ID {
				old = ApprovalRequestEntry{CallID: req.CallID, Tool: req.Tool, RunID: req.RunID, Reason: req.Reason,
					Expiry: req.Expiry, Quorum: req.Quorum}
			}
			rev := old
			rev.Args, rev.ArgsSHA256, rev.RevisionOf, rev.Recovery = edited, hashArgs(edited), req.ID, false
			rev.Run = old.Run.Clone()
			rev.ID, rev.ParentID, rev.Created = s.mintIDLocked(), e.ID, now
			entries = append(entries, rev)
			ap := ApprovalDecisionEntry{CallID: d.CallID, Outcome: OutcomeApprove, Reason: d.Reason, Who: d.Who, Via: d.Via,
				RunID: rev.RunID, KeyID: d.keyID, RequestID: rev.ID, ArgsSHA256: rev.ArgsSHA256, Cosigners: slices.Clone(d.Cosigners)}
			ap.ID, ap.ParentID, ap.Created = s.mintIDLocked(), rev.ID, now
			entries = append(entries, ap)
			parent = ap.ID
			held[d.CallID] = []ApprovalDecisionEntry{ap}
			continue
		}
		e.ID, e.ParentID, e.Created = s.mintIDLocked(), parent, now
		entries = append(entries, e)
		parent = e.ID
		// The grant is the verdict's, not the decision's (ADR 0021
		// §4): minted with the decision that makes the call's
		// effective verdict an approve, when some approval of the
		// occurrence asked for it — never while a quorum is still
		// open, never beside a denial, and once.
		quorum := max(s.cfg.quorum, req.Quorum)
		_, wasDecided := effectiveDecision(before, quorum)
		all := append(before, e)
		held[d.CallID] = all
		if eff, ok := effectiveDecision(all, quorum); !wasDecided && ok && eff.Outcome == OutcomeApprove && anyAlways(all) {
			g := alwaysGrant(req.Tool, req.Args, e)
			g.ID, g.ParentID, g.Created = s.mintIDLocked(), parent, now
			entries = append(entries, g)
			parent = g.ID
		}
	}
	if err := s.st.Append(ctx, s.header.ID, entries...); err != nil {
		return err
	}
	for _, e := range entries {
		s.adoptLocked(e)
	}
	if err := s.flushLocked(ctx); err != nil {
		return fmt.Errorf("thread: decision flush: %w", err)
	}
	for _, d := range batch {
		if byCall[d.CallID].Child != "" {
			// A mirrored child request was decided: the pool that
			// mirrored it carries the decision to the child.
			s.mirrorsDecidedLocked()
			break
		}
	}
	return nil
}

// handlerRunningLocked reports whether req's call has a handler a
// turn's end left running past its bound that has not returned yet
// (stillRunning); one that has is forgotten. Callers hold s.mu.
func (s *Session) handlerRunningLocked(req Request) bool {
	key := req.RunID + "\x00" + req.CallID
	done, ok := s.stillRunning[key]
	if !ok {
		return false
	}
	select {
	case <-done:
		delete(s.stillRunning, key)
		return false
	default:
		return true
	}
}

// anyAlways reports whether some approval among ds asked for the
// standing grant.
func anyAlways(ds []ApprovalDecisionEntry) bool {
	for _, d := range ds {
		if d.Always && d.Outcome == OutcomeApprove {
			return true
		}
	}
	return false
}

// denyPendingLocked denies every pending call with reason, recorded
// under via — the interrupting Send's path (Via "interrupt"). Expiry
// is resolved first, so a lapsed request keeps its own reason; the
// rest are denied in one atomic append, and the boundary resumes when
// that completes it. A session-internal path: no signature rule
// applies. Callers hold s.mu.
func (s *Session) denyPendingLocked(ctx context.Context, reason, via string) error {
	if _, err := s.resolveExpiredLocked(ctx); err != nil {
		return err
	}
	pending := s.pendingLocked()
	if len(pending) > 0 {
		batch := make([]recordedDecision, 0, len(pending))
		for _, r := range pending {
			d := Deny(r.CallID, reason)
			d.Via = via
			batch = append(batch, recordedDecision{Decision: d})
		}
		if err := s.recordDecisionsLocked(ctx, batch, true); err != nil {
			return err
		}
	}
	_, err := s.armSettledLocked(ctx)
	return err
}

// resolveExpiredLocked denies every pending request strictly past its
// expiry, on the spot (ADR 0021 §5): an expiry audit step and a deny
// decision with Via "expiry" and the stated reason, in one atomic
// append. It returns the requests it denied. Every path that records
// a decision or arms a resume runs it first, so an expired request is
// never approved and never left for whichever path happens to look.
// Callers hold s.mu.
func (s *Session) resolveExpiredLocked(ctx context.Context) ([]Request, error) {
	now := s.approvalNow()
	expired := s.expiredPendingLocked(now)
	if len(expired) == 0 {
		return nil, nil
	}
	var entries []Entry
	parent := s.leaf
	for _, r := range expired {
		audit := ApprovalAuditEntry{
			CallID: r.CallID, Step: StepExpiry, Outcome: "denied",
			Detail: expiryDetail(r.Expiry), RunID: r.RunID,
		}
		audit.ID, audit.ParentID, audit.Created = s.mintIDLocked(), parent, now
		entries = append(entries, audit)
		parent = audit.ID
		d := ApprovalDecisionEntry{
			CallID: r.CallID, Outcome: OutcomeDeny,
			Reason: expiryReason(r.Expiry), Via: viaExpiry, RunID: r.RunID, RequestID: r.ID,
		}
		d.ID, d.ParentID, d.Created = s.mintIDLocked(), parent, now
		entries = append(entries, d)
		parent = d.ID
	}
	if err := s.st.Append(ctx, s.header.ID, entries...); err != nil {
		return nil, err
	}
	for _, e := range entries {
		s.adoptLocked(e)
	}
	if err := s.flushLocked(ctx); err != nil {
		return nil, fmt.Errorf("thread: expiry flush: %w", err)
	}
	for _, r := range expired {
		if r.Child != "" {
			s.mirrorsDecidedLocked() // the lapse reaches the child through its pool
			break
		}
	}
	return expired, nil
}

// sweepExpiredLocked is resolveExpiredLocked for the runner's own
// arming paths, which have no caller to hand an error to: it runs
// under the boundary's persistence window and logs a failure — the
// requests stay pending and the next path that looks tries again.
// Callers hold s.mu.
func (s *Session) sweepExpiredLocked() {
	ctx := s.await.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if _, err := s.resolveExpiredLocked(context.WithoutCancel(ctx)); err != nil {
		s.agent.Logger().Warn("thread: expired requests not denied",
			"session", s.header.ID, "err", err)
	}
}

// settledBoundaryLocked reports whether an open boundary is ready for
// its resume: expiry resolved first, then every pending call decided.
// The runner's arming paths ask it instead of counting pending calls
// themselves, so none of them can arm around the expiry sweep.
// Callers hold s.mu.
func (s *Session) settledBoundaryLocked() bool {
	if !s.boundaryLocked() {
		return false
	}
	s.sweepExpiredLocked()
	return len(s.pendingLocked()) == 0
}

// armSettledLocked arms the resume when AutoResume is on and the open
// boundary has every call decided; (nil, nil) otherwise. Callers hold
// s.mu and have resolved expiry.
func (s *Session) armSettledLocked(ctx context.Context) (*Turn, error) {
	if s.boundaryLocked() && len(s.pendingLocked()) == 0 {
		if s.cfg.autoResume {
			return s.armResumeLocked(ctx)
		}
		if s.await.resumed == nil {
			s.announceResumeLocked() // OnReady's to start
		}
	}
	return nil, nil
}

// Resume starts the boundary's resume run now (ADR 0021 §1): the
// decided calls resolve under their decisions — approved calls run
// with Call.Approved set, denied ones show their reason, resolved ones
// their content — and every undecided call is denied with the core's
// "no decision" text — except a recovery request (Request.Recovery):
// while one is undecided Resume fails with ErrRecoveryRequired and
// starts nothing, for the call may have run and only a person retries
// or skips it. Requests strictly past their expiry are denied
// first, on the spot, with the stated reason — the same sweep Decide
// and every other arming path runs. The parked boundary must still be
// open: Resume with nothing to resume fails with ErrNotPending. The
// returned Turn is the resume run's receipt and handle, like any
// Send's; a Resume while the boundary's resume is armed — queued or
// already running — returns that turn, until it lands. A Resume after
// it landed finds the boundary resolved and fails with ErrNotPending
// (or, when the resume failed without resolving it, arms a fresh one):
// a caller that must share the first resume's Turn calls Resume before
// that turn can end, or follows the parked turn's Next.
//
// A decision is spent by the resume that applies it: it resolves its
// call on that resume's line of the tree only. After a Branch back to
// the decided boundary the calls are pending again and need new
// decisions — the approved call never runs twice on one approval.
func (s *Session) Resume(ctx context.Context) (*Turn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// The arming registry first: per-step durability (ADR 0011 §7)
	// appends the resume's step messages as they join, so the boundary's
	// dangling tail can already read resolved while the resume is still
	// in flight — the armed resume, not the tail, is the source of truth
	// for "one boundary resumes once", and a Resume joining it gets the
	// in-flight turn (which itself answers ErrNotPending if the boundary
	// it was armed for is gone).
	if armed := s.await.resumed; armed != nil {
		return armed, nil
	}
	if !s.boundaryLocked() {
		return nil, fmt.Errorf("%w: no parked approvals to resume", ErrNotPending)
	}
	if _, err := s.resolveExpiredLocked(ctx); err != nil {
		return nil, err
	}
	for _, r := range s.pendingLocked() {
		if r.Recovery {
			// The resume would deny it "no decision": a plain DENIED
			// the model answers by asking again, over an effect that
			// may have landed. A person decides it (ADR 0032 rule D).
			return nil, fmt.Errorf("%w: session %s: recovery request %s for call %q is undecided; approve (retry) or deny (skip) it",
				ErrRecoveryRequired, s.header.ID, r.ID, r.CallID)
		}
	}
	return s.armResumeLocked(ctx)
}

// armResumeLocked hands the resume run to the runner: the Turn is
// minted here so Decide and Resume can return it, and the runner —
// started if no runner is alive — picks the work up. Arming is
// idempotent while the armed resume has not landed: a boundary resumes
// once, so a second Resume before the first lands returns the turn
// already armed — overwriting it would orphan the first caller's Turn
// on a wait that never ends. Callers hold s.mu; the boundary is
// decided (or Resume is forcing it).
func (s *Session) armResumeLocked(ctx context.Context) (*Turn, error) {
	if s.await.resumed != nil {
		return s.await.resumed, nil // one boundary resumes once
	}
	if err := s.errRecoveryRequiredLocked(); err != nil {
		// A resume a crash cut short after it started an approved
		// call: running the boundary again could repeat the call.
		return nil, err
	}
	t := s.mintResumeLocked()
	if s.running {
		s.resumeWork = &pendingResume{ctx: ctx, turn: t}
		return t, nil
	}
	item := workItem{ps: pendingSend{ctx: ctx, turn: t}, resume: true}
	s.running = true
	s.inFlight = t
	s.bindLocked(item)
	go s.execute(item)
	return t, nil
}

// mintResumeLocked mints a resume turn for the open boundary and
// registers it as the boundary's one armed resume — the idempotency
// key Decide, Resume and the runner's own arming paths (the
// chain-decided hand-off, the settled-boundary pickup) all share, so
// two paths can never arm two resumes for one boundary. A resume
// already armed is returned as-is, never displaced. Callers hold s.mu.
func (s *Session) mintResumeLocked() *Turn {
	if s.await.resumed != nil {
		return s.await.resumed
	}
	t := s.newTurnLocked()
	t.resume = true
	s.await.resumed = t
	if s.await.turn != nil { // nil after a restart: the parked Turn object is gone
		s.await.turn.setNext(t)
	}
	return t
}

// settleResumeLocked retires a resume turn's arming and drops the
// boundary's captured settings when the resume resolved it. Called
// under s.mu on every path that decides a resume turn — the run
// completed, the context died first, the persistence failed, the turn
// panicked — always before the turn is decided, so a caller whose Wait
// returns faces a settled boundary: a retry after a failure arms
// fresh, a resolution reads ErrNotPending. Never a stale dead Turn on
// a wait that cannot end.
func (s *Session) settleResumeLocked(t *Turn) {
	if s.await.resumed == t {
		s.await.resumed = nil
	}
	if !s.boundaryLocked() {
		s.await = awaitState{}
	}
}

// validOutcome reports whether o is one of the four decision outcomes.
func validOutcome(o Outcome) bool {
	switch o {
	case OutcomeApprove, OutcomeDeny, OutcomeResolve, OutcomeResolveError, OutcomeEdit:
		return true
	}
	return false
}

// expiryReason is the model-visible denial text for an expired request
// (ADR 0021 §5) — pinned bytes, asserted inline by the approval tests:
// the reason names the deadline so the model can tell a lapsed request
// from a refused one.
func expiryReason(expiry time.Time) string {
	return "expired: no decision before " + expiry.UTC().Format(time.RFC3339)
}

// expiryDetail is the audit entry's Detail for the same event.
func expiryDetail(expiry time.Time) string {
	return "request expired at " + expiry.UTC().Format(time.RFC3339)
}

// hashArgs returns the hex SHA-256 of a call's argument bytes — nil
// args hash as the empty string's hash, the bytes being the bytes.
func hashArgs(args []byte) string {
	sum := sha256.Sum256(args)
	return hex.EncodeToString(sum[:])
}

// chainResult is what the decision chain produced for one parked turn:
// the entries to append with it (callers fill their ids at append
// time), the calls that parked (for OnRequest), and the turn settings
// the resume run inherits.
type chainResult struct {
	entries     []Entry
	parkedCalls []core.ToolCallPart
	opts        []core.RunOption // the parked send's captured run options
	awaitCtx    context.Context  // the persistence window the resume inherits
	runID       string           // the run that parked
}

// recoveryRequests counts the recovery requests the chain left for a
// person: a request entry with Recovery set and no decision after it.
func (cr *chainResult) recoveryRequests() int {
	n := 0
	for i, e := range cr.entries {
		re, ok := e.(ApprovalRequestEntry)
		if !ok || !re.Recovery {
			continue
		}
		decided := false
		for _, f := range cr.entries[i+1:] {
			if d, ok := f.(ApprovalDecisionEntry); ok && d.CallID == re.CallID {
				decided = true
			}
		}
		if !decided {
			n++
		}
	}
	return n
}

// runChain is the decision chain (ADR 0021 §2): for each call a turn
// left pending, grants (ADR 0021 §4), then a bounded Approver, then
// the park — except a call that delegates to a thread/pool child,
// which skips both and parks. It runs at the turn's end, before the
// request is persisted — "before a request parks" is before the
// durable parked state exists, because the core's run boundary has
// already ended the run — and every step leaves an audit entry,
// including automatic approvals. It holds no lock: the Approver is a
// caller's function and may take its own time, bounded by
// WithApprover's timeout.
//
// A call the chain leaves open always parks with a request entry —
// also when a chain step did decide and the decision alone does not
// resolve the call (one approval under Quorum): the request is what
// carries the expiry, the notification and the challenge the
// remaining decisions answer, so the first approval is recorded
// beside it, not instead of it.
func (s *Session) runChain(ctx context.Context, t *Turn, opts []core.RunOption, calls []core.ToolCallPart, sp *stepPersist) *chainResult {
	cr := &chainResult{opts: opts, awaitCtx: ctx, runID: t.runID}
	// What the parked run ran under, as data: a resume after a restart
	// has lost opts and ctx, and puts this back (runResume).
	settings := s.settingsOf(ctx, opts)
	var expiry time.Time
	if s.cfg.requestExpiry > 0 {
		expiry = s.approvalNow().Add(s.cfg.requestExpiry)
	}
	// The uses this chain has already spent of each session grant: the
	// audit entries that count them land with the turn, after the
	// chain, so MaxUses is tallied here for the calls of one step.
	uses := map[string]int{}
	// The delegating calls among them (ADR 0022 §7): a call a mirrored
	// child request names as its Wrapper parked because its child did.
	// The chain has nothing to say about it — no grant and no Approver
	// is consulted, it always parks, and only the child's answer
	// resolves it (ResolveDelegation): an approval here would run the
	// delegation again in a second child session, a denial would
	// abandon a child that is still parked.
	var wrappers map[string]string
	s.locked(func() { wrappers = s.approvalWalkLocked().wrappers })
	// The calls whose handler started and gave no answer
	// (core.ParkUnknown): the outcome is unknown, and the recovery
	// ladder decides it (ADR 0032) — a keyed retry (bounded), or a
	// person. (A delegating call passed the gate too — the delegation
	// is its handler — but its child parked, mirrored: it parks below,
	// for the child's answer.) Not the tool's Reconcile: the abandoned
	// handler may still be running (its Timeout passed, a cancel it
	// ignores), and a system of record asked now could answer
	// "missing" for an effect about to land — the rerun would be a
	// second one.
	unknown := map[string][]Entry{}
	var waitFor []string
	for _, c := range calls {
		if wrappers[c.ID] != "" || !sp.wasStarted(s, c.ID) {
			continue
		}
		var es []Entry
		s.locked(func() {
			es = s.recoveryEntries(c, t.runID, c.Args, settings, s.parkQuorum(), startMaybe, nil)
		})
		unknown[c.ID] = es
		if d, ok := es[len(es)-1].(ApprovalDecisionEntry); !ok || d.Outcome == OutcomeApprove {
			// A retry — the session's own, or a person's — never starts
			// while the abandoned handler may still be running: the
			// request is not offered until it returns, bounded (review 3
			// P2-2, final review P2-1). A call resolved without a run
			// (ReplaySafe) waits for nothing.
			waitFor = append(waitFor, c.ID)
		}
	}
	running := sp.stillRunning(s, waitFor, abandonedWait)
	for _, c := range calls {
		if es, ok := unknown[c.ID]; ok {
			if done, still := running[c.ID]; still {
				// Still running past the bound: a person decides, and
				// an approval waits for the handler (ErrStillRunning).
				req := es[0].(ApprovalRequestEntry)
				req.Reason = reasonStillRunning
				es = []Entry{req}
				s.locked(func() {
					if s.stillRunning == nil {
						s.stillRunning = map[string]<-chan struct{}{}
					}
					s.stillRunning[t.runID+"\x00"+c.ID] = done
				})
			}
			cr.entries = append(cr.entries, es...)
			if _, approved := es[len(es)-1].(ApprovalDecisionEntry); !approved {
				cr.parkedCalls = append(cr.parkedCalls, c)
			}
			continue
		}
		// steps are this call's chain entries in order; resolved says
		// they hold the call's effective verdict.
		var steps []Entry
		resolved := false
		if wrappers[c.ID] != "" {
			// Delegated: straight to the park.
		} else if d, ref, ok := s.matchGrant(ctx, c, uses); ok {
			// The chain's first step decides at once, audited — the
			// audit names the grant (GrantShared marking a store's, so
			// the session's use counting cannot cross-count an id
			// collision), and the session counts its own grants' uses
			// from exactly these entries (ADR 0021 §2, §4).
			if !ref.shared {
				uses[ref.id]++
			}
			d.CallID = c.ID
			outcome := "approved"
			if d.Kind == OutcomeDeny {
				outcome = "denied"
			}
			detail := "grant " + ref.id
			if ref.shared {
				detail = "shared grant " + ref.id
			}
			de := decisionFrom(c.ID, d, t.runID)
			steps = append(steps,
				ApprovalAuditEntry{
					CallID: c.ID, Step: StepGrant, Outcome: outcome,
					Detail: detail, RunID: t.runID,
					GrantID: ref.id, GrantShared: ref.shared,
				},
				de,
			)
			_, resolved = effectiveDecision([]ApprovalDecisionEntry{de}, s.cfg.quorum)
		} else if s.cfg.approver != nil && s.cfg.approverTimeout > 0 {
			req := Request{
				Session: s.header.ID, CallID: c.ID, Tool: c.Name,
				Args: slices.Clone(c.Args), ArgsSHA256: hashArgs(c.Args),
				RunID: t.runID, Reason: parkingReason(s, c.Name), Expiry: expiry,
			}
			s.locked(func() { s.requestKeyLocked(&req) })
			d, decided, outcome := s.consultApprover(ctx, req)
			if decided && d.Kind == OutcomeEdit {
				// An edit opens a revision a person decides through
				// Decide; the chain's Approver approves, denies or
				// resolves. The call parks.
				decided, outcome = false, "declined"
			}
			steps = append(steps, ApprovalAuditEntry{
				CallID: c.ID, Step: StepApprover, Outcome: outcome, RunID: t.runID,
			})
			if decided {
				if d.Via == "" {
					d.Via = viaApprover
				}
				de := decisionFrom(c.ID, d, t.runID)
				steps = append(steps, de)
				var eff ApprovalDecisionEntry
				eff, resolved = effectiveDecision([]ApprovalDecisionEntry{de}, s.cfg.quorum)
				if resolved && eff.Outcome == OutcomeApprove && de.Always {
					// "Approve and always allow" from the live step
					// grants like the same decision through Decide or
					// DecideSigned (ADR 0021 §4): the tool plus the
					// call's exact arguments, recorded with the
					// decision in the same append — when the approval
					// is the call's verdict. Under a quorum it is one
					// approval; the decision keeps the wish (Always)
					// and the approval that completes the quorum mints
					// the grant.
					steps = append(steps, alwaysGrant(c.Name, c.Args, de))
				}
			}
		}
		if resolved {
			cr.entries = append(cr.entries, steps...)
			continue
		}
		args := slices.Clone(c.Args)
		cr.entries = append(cr.entries, ApprovalRequestEntry{
			CallID: c.ID, Tool: c.Name, Args: args, ArgsSHA256: hashArgs(args),
			RunID: t.runID, Reason: parkingReason(s, c.Name), Expiry: expiry,
			Run: settings.Clone(), SchemaSHA256: toolSchemaHash(s.agent, c.Name),
			Quorum: s.parkQuorum(),
		})
		cr.entries = append(cr.entries, steps...)
		cr.entries = append(cr.entries,
			ApprovalAuditEntry{CallID: c.ID, Step: StepPark, Outcome: "parked", RunID: t.runID})
		cr.parkedCalls = append(cr.parkedCalls, c)
	}
	return cr
}

// decisionFrom builds the decision entry a chain step records.
func decisionFrom(callID string, d Decision, runID string) ApprovalDecisionEntry {
	return ApprovalDecisionEntry{
		CallID: callID, Outcome: d.Kind, Reason: d.Reason, Content: d.Content,
		Who: d.Who, Via: d.Via, RunID: runID, ArgsSHA256: d.ArgsSHA256,
		Always: d.Always && d.Kind == OutcomeApprove,
	}
}

// consultApprover runs the chain's live step under its timeout: the
// answer, whether it decided, and the audit outcome ("approved",
// "denied", "resolved", "declined", "timeout", or "panic"). The
// consultation never blocks the turn beyond its timeout — an
// approver that ignores its context is abandoned where it stands, the
// buffered channel catching its late answer, and the call parks.
func (s *Session) consultApprover(ctx context.Context, r Request) (Decision, bool, string) {
	actx, cancel := context.WithTimeout(ctx, s.cfg.approverTimeout)
	defer cancel()
	type answer struct {
		d        Decision
		ok       bool
		panicked bool
	}
	ch := make(chan answer, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				s.agent.Logger().Error("thread: approver panicked",
					"session", s.header.ID, "call", r.CallID, "panic", p)
				ch <- answer{panicked: true}
			}
		}()
		d, ok := s.cfg.approver(actx, r)
		d.CallID = r.CallID // the request names the call, whatever the approver copied
		d.RequestID = ""    // no entry exists yet (the Request it was handed has no ID)
		if d.ArgsSHA256 != "" && d.ArgsSHA256 != r.ArgsSHA256 {
			ok = false // decided over other arguments: no decision for this call
		}
		ch <- answer{d: d, ok: ok}
	}()
	select {
	case a := <-ch:
		switch {
		case a.panicked:
			return Decision{}, false, "panic"
		case !a.ok:
			return Decision{}, false, "declined"
		}
		switch a.d.Kind {
		case OutcomeApprove:
			return a.d, true, "approved"
		case OutcomeDeny:
			return a.d, true, "denied"
		case OutcomeResolve, OutcomeResolveError:
			return a.d, true, "resolved"
		}
		return Decision{}, false, "declined" // an outcome-less decision is no decision
	case <-actx.Done():
		return Decision{}, false, "timeout"
	}
}

// parkingReason is why a call parked, for the request entry: the
// tool's own RequireApproval mark when it has one, the tool chain's
// deferral otherwise, and the unknown-tool text a tool source that
// changed shape leaves.
func parkingReason(s *Session, tool string) string {
	for _, def := range s.agent.Tools() {
		if def.Name == tool {
			if def.RequiresApproval() {
				return "tool requires approval"
			}
			return "middleware required approval"
		}
	}
	return "unknown tool"
}

// fireOnRequest delivers the chain's parked requests to the OnRequest
// callback, after the append that made them durable (ADR 0021 §5).
// Panics are contained — a push notification must not fail a turn
// that already landed.
func (s *Session) fireOnRequest(cr *chainResult) {
	if s.cfg.onRequest == nil || len(cr.parkedCalls) == 0 {
		return
	}
	want := make(map[string]bool, len(cr.parkedCalls))
	for _, c := range cr.parkedCalls {
		want[c.ID] = true
	}
	s.mu.Lock()
	var reqs []Request
	for i := len(s.order) - 1; i >= 0 && len(reqs) < len(cr.parkedCalls); i-- {
		if re, ok := s.order[i].(ApprovalRequestEntry); ok && want[re.CallID] {
			r := requestFromEntry(s.header.ID, re)
			s.requestKeyLocked(&r)
			reqs = append(reqs, r)
		}
	}
	fn := s.cfg.onRequest
	s.mu.Unlock()
	slices.Reverse(reqs) // call order, like everything the chain reports
	for _, r := range reqs {
		func() {
			defer func() {
				if p := recover(); p != nil {
					s.agent.Logger().Error("thread: OnRequest panicked",
						"session", s.header.ID, "call", r.CallID, "panic", p)
				}
			}()
			fn(r)
		}()
	}
}

// Audit returns the session's approval trail (ADR 0021 §5): every
// request, chain step, decision, grant and revocation entry, in
// append order, each carrying its entry id and time — the whole file,
// not only the leaf's path, because the trail is what happened, not
// what the leaf remembers. An expiry denial reads as its audit entry
// plus the decision with Via "expiry"; a grant match as the audit
// entry naming the grant in GrantID; a refused signed decision as a
// StepSigned audit entry with no decision beside it.
//
// A resume reads as two StepResume audit entries: "started", written
// before the run starts and listing the decisions it applies, and
// "completed" or "failed" (Detail carrying the error), written in the
// same atomic append as the resume's turn entry. Each carries the run
// id it was written under; they agree unless the resume re-ran after a
// context overflow, where the second names the re-run. A started entry
// with no second entry after it is a resume that never finished: a
// crash, or a run still in flight. Turn entries are not part of the
// trail — the turn's ledger is Entries'.
//
// The trail is an index of the session's log, not evidence that
// stands on its own: entries are plain appended lines, unsigned and
// unchained, so whoever can write the session's storage can add,
// change or remove them, and Who on an unsigned decision is whatever
// the caller declared. It answers "what did this session record";
// tamper-evidence, where it is needed, belongs to the storage (an
// append-only store, a signed export).
func (s *Session) Audit() []Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Entry
	for _, e := range s.order {
		switch e := e.(type) {
		case ApprovalAuditEntry, ApprovalRequestEntry, ApprovalDecisionEntry, GrantEntry, GrantRevokedEntry:
			out = append(out, cloneEntry(e))
		}
	}
	return out
}

// auditResumeStartLocked writes the resume's "started" audit entry
// before its run starts: a crash between the two leaves the boundary
// resumable and the audit honest about the attempt. The entry lists
// the decisions the resume applies — which is what spends them
// (approvalWalkLocked). Callers hold s.mu.
func (s *Session) auditResumeStartLocked(ctx context.Context, t *Turn) error {
	dangling := s.danglingCallsLocked()
	walk := s.approvalWalkLocked()
	var applied []string
	var drifted []ApprovalAuditEntry
	for _, c := range dangling {
		_, ds := walk.occurrence(c.ID)
		for _, d := range ds {
			applied = append(applied, d.ID)
		}
		if d, ok := effectiveDecision(ds, s.quorumOf(walk.requests[c.ID])); ok && d.Outcome == OutcomeApprove {
			if drift := s.driftLocked(walk.requests[c.ID]); drift != "" {
				drifted = append(drifted, ApprovalAuditEntry{
					CallID: c.ID, Step: StepResume, Outcome: "drift", Detail: drift, RunID: t.runID,
				})
			}
		}
	}
	if err := s.appendLocked(ctx, func(id, parent string, created time.Time) Entry {
		return ApprovalAuditEntry{
			ID: id, ParentID: parent, Created: created,
			Step: StepResume, Outcome: "started",
			Detail: fmt.Sprintf("%d call(s) to resolve", len(dangling)), RunID: t.runID,
			Decisions: applied,
		}
	}); err != nil {
		return err
	}
	// An approved call the resume will deny because its tool changed
	// since it parked is named in the trail before the run starts.
	for _, a := range drifted {
		if err := s.appendLocked(ctx, func(id, parent string, created time.Time) Entry {
			a.ID, a.ParentID, a.Created = id, parent, created
			return a
		}); err != nil {
			return err
		}
	}
	return nil
}

// driftLocked says why a parked call's approval no longer covers the
// call, "" when it does: the request recorded its tool's input schema
// (SchemaSHA256), and the session's agent no longer has a tool of that
// name, or has one with another schema — code changed between the park
// and the resume (a deploy, a restart on another build). The reason is
// model-visible ("DENIED: …"): the model may call the tool again, and
// that call parks or runs under the tool as it is now. Callers hold
// s.mu.
func (s *Session) driftLocked(r ApprovalRequestEntry) string {
	if r.SchemaSHA256 == "" {
		return ""
	}
	switch now := toolSchemaHash(s.agent, r.Tool); now {
	case r.SchemaSHA256:
		return ""
	case "":
		return fmt.Sprintf("tool %q is no longer available; the approved call was not run", r.Tool)
	default:
		return fmt.Sprintf("tool %q changed its input since this call was approved; the call was not run — call it again if it is still wanted", r.Tool)
	}
}

// toolSchemaHash is the hex SHA-256 of the input schema of a's tool
// named name, as JSON — "" when a's static tools hold none of that
// name (a ToolSource's tool is not checked).
func toolSchemaHash(a *core.Agent, name string) string {
	for _, def := range a.Tools() {
		if def.Name != name {
			continue
		}
		b, err := json.Marshal(def.InputSchema)
		if err != nil {
			return ""
		}
		return hashArgs(b)
	}
	return ""
}

// requestFromEntry lifts a stored request entry to its Request value.
// Quorum is the entry's; the session's own, when higher, is the
// caller's to fold in (pendingLocked).
func requestFromEntry(session string, e ApprovalRequestEntry) Request {
	r := Request{
		ID: e.ID, Session: session, CallID: e.CallID, Tool: e.Tool,
		Args: slices.Clone(e.Args), ArgsSHA256: e.ArgsSHA256,
		RunID: e.RunID, Reason: e.Reason, Expiry: e.Expiry, Created: e.Created,
		Child: e.Child, Quorum: e.Quorum, Recovery: e.Recovery, RevisionOf: e.RevisionOf,
	}
	if e.Run != nil && len(e.Run.Metadata) > 0 {
		r.Metadata = maps.Clone(e.Run.Metadata)
	}
	return r
}

// parkQuorum is the quorum a request parked now records: the
// session's when above 1, else 0.
func (s *Session) parkQuorum() int {
	if s.cfg.quorum > 1 {
		return s.cfg.quorum
	}
	return 0
}

// quorumOf is the quorum a call's occurrence is decided under: the
// stricter of the one its request recorded when it parked and the
// session's own — so a session reopened without Quorum (another
// process, another build) never resolves on fewer approvals than
// the call parked asking for. Callers hold s.mu.
func (s *Session) quorumOf(r ApprovalRequestEntry) int {
	return max(s.cfg.quorum, r.Quorum)
}

// fillApprovalEntry stamps a chain entry's tree fields — id, parent,
// created — at append time, the same stamping recordTurnEnd gives the
// messages and the turn entry.
func fillApprovalEntry(e Entry, id, parent string, created time.Time) Entry {
	switch e := e.(type) {
	case ApprovalRequestEntry:
		e.ID, e.ParentID, e.Created = id, parent, created
		return e
	case ApprovalDecisionEntry:
		e.ID, e.ParentID, e.Created = id, parent, created
		return e
	case ApprovalAuditEntry:
		e.ID, e.ParentID, e.Created = id, parent, created
		return e
	case GrantEntry:
		// The approver's "approve and always allow" lands its grant
		// through the chain's batch, like Decide lands its.
		e.ID, e.ParentID, e.Created = id, parent, created
		return e
	}
	return e
}

// danglingCallsLocked returns the calls parked on the leaf's path: the
// tool calls of the last assistant message with calls that have no
// result in the tool messages directly after it (ADR 0007's unresolved
// set, read from the tree) and that a turn recorded as pending — a
// request entry names the call, or the turn entry that ended the step
// lists it. That second half is what tells an approval boundary from a
// crash: per-step durability (ADR 0011 §7) writes an assistant message
// the moment it joins, so a writer that died before the step's tool
// message leaves a call with no result and no turn entry. Nothing
// parked it and no decision can address it; it is not a boundary, the
// next run's input repair answers it, and holding the session for it
// would hold it forever.
//
// A parked step's tool message is partial when some of its calls ran,
// and the resume's completed one takes its place on the path (ADR 0011
// §7: the join attaches to the assistant entry), so the path holds one
// tool message after the assistant. A session file written before
// that rule holds both, the partial and then the complete one: every
// tool message directly following the assistant is read here, so such
// a boundary reads closed — as it was resolved — instead of holding
// the session forever. Callers hold s.mu.
func (s *Session) danglingCallsLocked() []core.ToolCallPart {
	unanswered := s.unansweredCallsLocked()
	if len(unanswered) == 0 {
		return nil
	}
	walk := s.approvalWalkLocked()
	var out []core.ToolCallPart
	for _, c := range unanswered {
		_, requested := walk.requests[c.ID]
		_, recorded := walk.runs[c.ID]
		if requested || recorded {
			out = append(out, c)
		}
	}
	return out
}

// unansweredCallsLocked returns the calls of the path's last assistant
// message with calls that no tool message directly after it answers —
// parked or not (danglingCallsLocked tells which). Callers hold s.mu.
func (s *Session) unansweredCallsLocked() []core.ToolCallPart {
	path, err := s.walkLocked(s.leaf) // read-only: the calls returned are copied below
	if err != nil {
		return nil
	}
	// Only message entries are transcript here: the scan reads them
	// straight off the path, from the leaf back, and builds nothing —
	// it runs on every turn, over the whole path.
	last := -1
	for j := len(path) - 1; j >= 0 && last < 0; j-- {
		me, ok := path[j].(MessageEntry)
		if !ok || me.Message.Role != core.RoleAssistant {
			continue
		}
		for _, p := range me.Message.Content {
			if _, ok := p.(core.ToolCallPart); ok {
				last = j
				break
			}
		}
	}
	if last < 0 {
		return nil
	}
	served := map[string]bool{}
	for j := last + 1; j < len(path); j++ {
		me, ok := path[j].(MessageEntry)
		if !ok {
			continue
		}
		if me.Message.Role != core.RoleTool {
			break
		}
		for _, p := range me.Message.Content {
			if r, ok := p.(core.ToolResultPart); ok {
				served[r.CallID] = true
			}
		}
	}
	var out []core.ToolCallPart
	for _, p := range path[last].(MessageEntry).Message.Content {
		if c, ok := p.(core.ToolCallPart); ok && !served[c.ID] {
			c.Args = slices.Clone(c.Args) // the caller's bytes, not the tree's
			out = append(out, c)
		}
	}
	return out
}

// scopedDecisions keeps the decisions that belong to one occurrence
// of a call: the ones recorded for the run that parked it (ADR 0007
// lets call ids repeat across turns, so a call id alone names nothing
// — the parked run does). The match is exact, an empty run id
// included: a decision is never lent to an occurrence it was not
// recorded for.
func scopedDecisions(ds []ApprovalDecisionEntry, runID string) []ApprovalDecisionEntry {
	var out []ApprovalDecisionEntry
	for _, d := range ds {
		if d.RunID == runID {
			out = append(out, d)
		}
	}
	return out
}

// approvalWalk is what the leaf's path says about its approvals: the
// latest request entry per call id, the run that left each call
// pending (from the turn entry — the name of an occurrence that
// parked without a request entry), and the decisions in force per
// call id, in append order.
type approvalWalk struct {
	requests  map[string]ApprovalRequestEntry
	runs      map[string]string
	decisions map[string][]ApprovalDecisionEntry
	// wrappers holds the calls that delegate to a pool child, each
	// mapped to that child's session id: the current occurrence of
	// the call is named as Wrapper by a mirrored child request (ADR
	// 0022 §7).
	wrappers map[string]string
}

// occurrence returns the run that names a call's current occurrence —
// its request entry's, else the turn entry's that recorded it pending
// — and the decisions in force for exactly that occurrence.
func (w approvalWalk) occurrence(callID string) (string, []ApprovalDecisionEntry) {
	run := w.runs[callID]
	if re, ok := w.requests[callID]; ok {
		run = re.RunID
	}
	return run, scopedDecisions(w.decisions[callID], run)
}

// approvalWalkLocked collects the approval entries on the leaf's
// path: the requests by call id, and every decision in force by call
// id in append order — the quorum's raw material, folded by
// effectiveDecision over the occurrence's slice. A call id the model
// re-issues (ADR 0007 lets call ids repeat across turns) starts a
// fresh occurrence: the walk resets the call's request and decisions
// at the message that carries it, so a later occurrence never
// inherits an earlier one's request or verdicts.
//
// Two kinds of decision on the path are not in force. A spent one: a
// resume that applied it lives on another line of the tree (its
// "started" audit entry lists the decision and is not on this path)
// — the call was resolved there, and a Branch back to the decided
// boundary must not run it again on the old approval. And an
// inherited one: a decision a Fork copied from its origin was made
// for the origin session, which may resume on it too. Either way the
// call reads pending again and takes a new decision. Callers hold
// s.mu.
func (s *Session) approvalWalkLocked() approvalWalk {
	w := approvalWalk{
		requests:  map[string]ApprovalRequestEntry{},
		runs:      map[string]string{},
		decisions: map[string][]ApprovalDecisionEntry{},
		wrappers:  map[string]string{},
	}
	// Read-only: the walk keeps entry values, and the one reference a
	// request entry carries — its argument bytes — is copied wherever
	// a request leaves the session (requestFromEntry).
	path, err := s.walkLocked(s.leaf)
	if err != nil {
		return w
	}
	onPath := make(map[string]bool, len(path))
	for _, e := range path {
		onPath[idOf(e)] = true
	}
	spent := map[string]bool{}
	for _, e := range s.order {
		a, ok := e.(ApprovalAuditEntry)
		if !ok || a.Step != StepResume || onPath[a.ID] {
			continue
		}
		for _, id := range a.Decisions {
			spent[id] = true
		}
	}
	// The entries a Fork copied sit at and before the fork point in
	// append order; everything after it is this session's own.
	inherited := -1
	if p := s.header.Parent; p != nil && p.Entry != "" {
		if i, ok := s.byID[p.Entry]; ok {
			inherited = i
		}
	}
	for _, e := range path {
		switch e := e.(type) {
		case MessageEntry:
			for _, p := range e.Message.Content {
				if c, ok := p.(core.ToolCallPart); ok {
					delete(w.requests, c.ID)
					delete(w.runs, c.ID)
					delete(w.decisions, c.ID)
					delete(w.wrappers, c.ID)
				}
			}
		case TurnEntry:
			for _, c := range e.Pending {
				w.runs[c.ID] = e.RunID
			}
		case ApprovalRequestEntry:
			w.requests[e.CallID] = e
			w.decisions[e.CallID] = nil
			if e.Child != "" && e.Wrapper != "" {
				w.wrappers[e.Wrapper] = e.Child
			}
			if e.Child == "" && e.Recovery {
				// A recovery request of its own: the call's delegation
				// is over (RecoverTurn parks a delegating call only
				// then) and a person decides it, not the pool.
				delete(w.wrappers, e.CallID)
			}
		case ApprovalDecisionEntry:
			if spent[e.ID] {
				continue
			}
			if i, ok := s.byID[e.ID]; ok && i <= inherited {
				continue
			}
			w.decisions[e.CallID] = append(w.decisions[e.CallID], e)
		}
	}
	// A mirrored child request (ADR 0022 §7) parks in another session:
	// on this tree it is ledger, not part of any line's transcript, and
	// Pending reads it from the whole file. Its decisions are read the
	// same way — a decision recorded for a mirror stays in force
	// wherever the leaf has moved since (a Branch, a resume's join),
	// instead of the request reading undecided again from every line
	// that does not hold the decision. They are matched by call id and
	// run id, the child's own, so a mirror's decision is never lent to
	// a call of this session that happens to share its id.
	mirrors := map[string]map[string]bool{} // call id → the child runs that parked it
	for _, e := range s.order {
		if re, ok := e.(ApprovalRequestEntry); ok && re.Child != "" {
			if mirrors[re.CallID] == nil {
				mirrors[re.CallID] = map[string]bool{}
			}
			mirrors[re.CallID][re.RunID] = true
		}
	}
	if len(mirrors) > 0 {
		for i, e := range s.order {
			d, ok := e.(ApprovalDecisionEntry)
			if !ok || !mirrors[d.CallID][d.RunID] || onPath[d.ID] || spent[d.ID] || i <= inherited {
				continue
			}
			w.decisions[d.CallID] = append(w.decisions[d.CallID], d)
		}
	}
	return w
}

// conflictReason is the model-visible denial text for conflicting
// decisions under quorum (ADR 0021 §5: pinned bytes).
const conflictReason = "conflicting decisions"

// approverIdentity is who an approval counts as under Quorum (ADR
// 0021 §5). A signed approval is its key — one key, one approver,
// whatever label the signature carries in Who; a grant's approval is
// the one identity "a grant"; every other approval is its declared
// Who (the empty string one identity). The three are namespaced, so a
// Who can never pose as a key or as the grant.
func approverIdentity(d ApprovalDecisionEntry) string {
	switch {
	case d.KeyID != "":
		return "key:" + d.KeyID
	case d.Via == viaGrant:
		return "grant"
	}
	return "who:" + d.Who
}

// sessionDenial reports whether a deny is the session's own — an
// expired request, an interrupting Send — rather than an approver's
// refusal: it resolves the call with its own stated reason even
// beside an approval, because it is not one side of a split verdict.
func sessionDenial(d ApprovalDecisionEntry) bool {
	return d.Outcome == OutcomeDeny && (d.Via == viaExpiry || d.Via == viaInterrupt)
}

// effectiveDecision folds a call's recorded decisions into the one
// that resolves it, under the session's quorum (ADR 0021 §5): a deny
// alone resolves; a deny beside an approve is a conflict, and a
// conflict resolves to deny with the pinned reason, so nothing
// outranks a refusal and a split verdict names itself — except the
// session's own denials (expiry, interrupt), which resolve with their
// stated reason whatever stands beside them; one resolve resolves, a
// resolve beside an approve or a second resolve conflicts; and
// approvals resolve once n distinct approver identities
// (approverIdentity) have approved. ok is false while the call is
// still pending.
func effectiveDecision(decisions []ApprovalDecisionEntry, quorum int) (ApprovalDecisionEntry, bool) {
	if quorum < 1 {
		quorum = 1
	}
	conflict := func(d ApprovalDecisionEntry) (ApprovalDecisionEntry, bool) {
		return ApprovalDecisionEntry{
			CallID: d.CallID, Outcome: OutcomeDeny,
			Reason: conflictReason, Via: viaQuorum, RunID: d.RunID,
		}, true
	}
	var resolve *ApprovalDecisionEntry
	sawApprove := false
	approvers := map[string]bool{}
	for i := range decisions {
		d := decisions[i]
		switch d.Outcome {
		case OutcomeDeny:
			if sawApprove && !sessionDenial(d) {
				return conflict(d)
			}
			return d, true
		case OutcomeResolve, OutcomeResolveError:
			if resolve != nil || sawApprove {
				return conflict(d)
			}
			resolve = &decisions[i]
		case OutcomeEdit:
			// An edit closed its request: the revision after it holds
			// the call's decisions. Alone it decides nothing.
			continue
		case OutcomeApprove:
			if d.Via == viaRecovery && resolve == nil {
				// The session's own retry of a cut-off call that is
				// safe to run again (ADR 0032): one decision, no quorum.
				return d, true
			}
			if resolve != nil {
				// An approve beside a resolve is the same split verdict
				// the other order is — the fold's words, both ways.
				return conflict(d)
			}
			sawApprove = true
			approvers[approverIdentity(d)] = true
		}
	}
	if resolve != nil {
		return *resolve, true
	}
	if sawApprove && len(approvers) >= quorum {
		// Any recorded approval serves as the effective one; they agree.
		return decisions[len(decisions)-1], true
	}
	return ApprovalDecisionEntry{}, false
}

// danglingRequestID names the request Pending offers for a dangling
// call that has no request entry: deterministic — from the session,
// the assistant entry holding the call and the call id — so a decision
// bound to it (Decision.Bind) names this occurrence on every read, in
// every process. It is no entry's id: "r_" and 26 hex digits of the
// SHA-256 of the three, NUL-separated.
func danglingRequestID(session, entry, callID string) string {
	h := sha256.Sum256([]byte(session + "\x00" + entry + "\x00" + callID))
	return "r_" + hex.EncodeToString(h[:13])
}

// pendingLocked returns the undecided requests of the open boundary,
// in call order: the dangling calls that carry a request and no
// effective decision. A dangling call without a request entry — the
// chain writes one for every call it leaves open, so this is a tail
// the chain never saw — is still reported, defensively, named by the
// run its turn entry recorded: Pending never hides a dangling call.
// Callers hold s.mu.
func (s *Session) pendingLocked() []Request {
	dangling := s.danglingCallsLocked()
	mirrors := s.liveMirrorsLocked()
	if len(dangling) == 0 && len(mirrors) == 0 {
		// No boundary and no nested request: nothing can be pending,
		// and the approval walk — a pass over the whole path — is not
		// owed. This is every turn of a session that never parks.
		return nil
	}
	walk := s.approvalWalkLocked()
	var out []Request
	for _, c := range dangling {
		run, ds := walk.occurrence(c.ID)
		re, requested := walk.requests[c.ID]
		if _, ok := effectiveDecision(ds, s.quorumOf(re)); ok {
			continue
		}
		if requested {
			r := requestFromEntry(s.header.ID, re)
			r.Quorum = s.quorumOf(re)
			if r.Quorum < 2 {
				r.Quorum = 0
			}
			s.requestKeyLocked(&r)
			out = append(out, r)
			continue
		}
		r := Request{
			ID:      danglingRequestID(s.header.ID, s.callEntryLocked(c.ID), c.ID),
			Session: s.header.ID, CallID: c.ID, Tool: c.Name,
			Args: slices.Clone(c.Args), ArgsSHA256: hashArgs(c.Args),
			RunID: run, Quorum: s.parkQuorum(),
		}
		s.requestKeyLocked(&r)
		out = append(out, r)
	}
	// Mirrored child requests (ADR 0022 §7): a pool child's parked
	// call is requested in its own session and mirrored here, so the
	// parent's pending — raw and offered alike — carries it with its
	// lineage, and a decision addressed to it records like any other.
	// An async child's mirror is the only shape that surfaces: the
	// parent's own transcript never dangles for it.
	for _, re := range mirrors {
		if _, ok := effectiveDecision(scopedDecisions(walk.decisions[re.CallID], re.RunID), s.quorumOf(re)); ok {
			continue
		}
		r := requestFromEntry(s.header.ID, re)
		if r.Quorum = s.quorumOf(re); r.Quorum < 2 {
			r.Quorum = 0
		}
		out = append(out, r)
	}
	return out
}

// liveMirrorsLocked returns the mirrored child requests in force, in
// append order (ADR 0022 §7): every mirror entry but the superseded
// ones. A child that parks again on a call id it parked on before is
// mirrored again under the same namespaced id, and the later entry is
// the request — the earlier one would otherwise read undecided for
// ever, its decisions reset by the entry that replaced it. Callers
// hold s.mu.
func (s *Session) liveMirrorsLocked() []ApprovalRequestEntry {
	last := map[string]int{}
	for i, e := range s.order {
		if re, ok := e.(ApprovalRequestEntry); ok && re.Child != "" {
			last[re.CallID] = i
		}
	}
	if len(last) == 0 {
		return nil
	}
	var out []ApprovalRequestEntry
	for i, e := range s.order {
		re, ok := e.(ApprovalRequestEntry)
		if !ok || re.Child == "" || last[re.CallID] != i {
			continue
		}
		out = append(out, re)
	}
	return out
}

// boundaryLocked reports whether the leaf's path ends on an approval
// boundary: calls left dangling that only a resume run resolves. While
// it is open no plain Send runs — the queue holds — and no compaction
// touches the tail: the dangling calls must stay raw for the decisions
// to resolve. Callers hold s.mu.
// persistedRunLocked returns the run settings the open boundary's
// requests recorded (ApprovalRequestEntry.Run), nil when none did.
// Callers hold s.mu.
func (s *Session) persistedRunLocked() *core.RunSettings {
	requests := s.approvalWalkLocked().requests
	for _, c := range s.danglingCallsLocked() {
		if r, ok := requests[c.ID]; ok && r.Run != nil {
			return r.Run
		}
	}
	return nil
}

func (s *Session) boundaryLocked() bool {
	return len(s.danglingCallsLocked()) > 0
}

// expiredPendingLocked filters the pending requests strictly past
// their expiry. A delegating wrapper call never expires on its own
// (ADR 0022 §7): its fate is its child's — the child's mirrored
// requests carry the expiry, their denial resumes the child, and the
// child's answer resolves the wrapper. Callers hold s.mu.
func (s *Session) expiredPendingLocked(now time.Time) []Request {
	wrappers := s.approvalWalkLocked().wrappers
	var out []Request
	for _, r := range s.pendingLocked() {
		if r.Child == "" && wrappers[r.CallID] != "" {
			continue
		}
		if !r.Expiry.IsZero() && now.After(r.Expiry) {
			out = append(out, r)
		}
	}
	return out
}

// approvalActorKey is the run metadata naming who decided the calls a
// resume run resolves (record contract §2, UX-E-17): a signed
// decision's key id, else the decision's Who; distinct actors in call
// order — a quorum's approvers every one, sorted within their call —
// comma-joined. A session owns it: every run of the session
// sets it, "" (none) on a run that applies no decision or whose
// decisions name nobody, so no enclosing run's actor is inherited.
// Metadata survives content-off (Decision.Who): an opaque id, never a
// person's address.
const approvalActorKey = "weft.approval.actor"

// resumeLinkLocked is what a run applying decisions reports about the
// boundary it resolves (UX-E-17) — a resume turn. Defensive: today
// only resume turns carry decisions (ADR 0021 §1); should another turn
// ever carry them, the link comes with them. It names the run that
// parked its calls — the request entries' run id, the run core.ResumeOf names — and the actors of the
// decisions in force (approvalActorKey's value). Both "" with nothing
// dangling. Callers hold s.mu.
func (s *Session) resumeLinkLocked() (parkedRun, actor string) {
	dangling := s.danglingCallsLocked()
	if len(dangling) == 0 {
		return "", ""
	}
	walk := s.approvalWalkLocked()
	var actors []string
	for _, c := range dangling {
		run, ds := walk.occurrence(c.ID)
		if parkedRun == "" {
			parkedRun = run
		}
		quorum := s.quorumOf(walk.requests[c.ID])
		d, ok := effectiveDecision(ds, quorum)
		if !ok {
			continue
		}
		for _, who := range decisionActors(d, ds, quorum) {
			if !slices.Contains(actors, who) {
				actors = append(actors, who)
			}
		}
	}
	return parkedRun, strings.Join(actors, ",")
}

// decisionActors names who decided one call, as approvalActorKey reads
// it: each decision's signing key id, else its Who. An approval is the
// approvals that counted toward the quorum — in arrival order up to the
// n-th distinct approver (effectiveDecision's approverIdentity), so a
// late extra approval recorded after the quorum was met names nobody —
// named sorted and deduped (arrival order says nothing about who
// decided); any other effective decision its own actor. "" names nobody
// and is left out.
func decisionActors(d ApprovalDecisionEntry, ds []ApprovalDecisionEntry, quorum int) []string {
	actorOf := func(d ApprovalDecisionEntry) string {
		if d.KeyID != "" {
			return d.KeyID
		}
		return d.Who
	}
	if d.Outcome != OutcomeApprove {
		if who := actorOf(d); who != "" {
			return []string{who}
		}
		return nil
	}
	quorum = max(quorum, 1)
	var out []string
	counted := map[string]bool{}
	for _, a := range ds {
		if a.Outcome != OutcomeApprove {
			continue
		}
		id := approverIdentity(a)
		if !counted[id] {
			if len(counted) == quorum {
				continue // past the quorum: this approval decided nothing
			}
			counted[id] = true
		}
		if who := actorOf(a); who != "" && !slices.Contains(out, who) {
			out = append(out, who)
		}
	}
	slices.Sort(out)
	return out
}

// danglingDecisionsLocked maps the boundary's calls to the core
// options the resume run carries (ADR 0007's mapping, exact):
// approved calls run with Call.Approved set, denied ones show their
// reason, resolved ones their content. Undecided calls get no option
// of their own: the core denies a pending call it was given no
// decision for with rule 12's "DENIED: no decision", and records it as
// such — weft.resume.calls and the call's execute_tool span say
// no_decision, never denied (a synthesized core.Deny would have said
// denied, and Studio's decided block with it). The core resolves the
// pending calls only when the run carries some decision, though: a
// resume carrying none would repair the dangling calls as interrupted,
// losing the boundary instead of denying it. So with denyUndecided
// (the resume run's own mode) and no decided call at all, the options
// carry undecidedMarker alone — a Deny for a call id no call carries,
// which the core ignores (ADR 0007: unknown ids are) beyond turning
// the resume on.
//
// onBehalf, when not empty, is the approval class the layer that
// decided through this session's own door named for its approvals
// (WithApprovedBy: weftbase/runtime's "runtime"): an approval the
// door would report as user's reports as onBehalf instead — reporting
// only. Callers hold s.mu.
func (s *Session) danglingDecisionsLocked(denyUndecided bool, onBehalf string) []core.RunOption {
	dangling := s.danglingCallsLocked()
	if len(dangling) == 0 {
		return nil
	}
	walk := s.approvalWalkLocked()
	var opts []core.RunOption
	undecided := false
	for _, c := range dangling {
		_, ds := walk.occurrence(c.ID)
		d, ok := effectiveDecision(ds, s.quorumOf(walk.requests[c.ID]))
		if !ok {
			undecided = true
			continue
		}
		switch d.Outcome {
		case OutcomeApprove:
			// An approval is for the call as its tool was when it
			// parked: a tool since removed, renamed or given another
			// input schema does not run on it (driftLocked).
			if drift := s.driftLocked(walk.requests[c.ID]); drift != "" {
				opts = append(opts, core.Deny(c.ID, drift))
				continue
			}
			// The resumed call's execute_tool span names who or what
			// approved it (UX-B-14): reporting only, the resume is Approve's.
			by, grant := s.approvalSourceLocked(d)
			if by == approvedByUser && onBehalf != "" {
				by = onBehalf
			}
			if args, _, edited, ok := editedArgsOf(walk.requests[c.ID], c); edited {
				// A revision (or a recovery retry of one): the call runs on
				// the recorded edited arguments, and only while they still
				// hash to what was recorded.
				if !ok {
					opts = append(opts, core.Deny(c.ID, "the edited arguments do not match their recorded hash"))
					continue
				}
				opts = append(opts, core.ApproveEdited(c.ID, args, by))
				continue
			}
			opts = append(opts, core.ApproveBy(c.ID, by, grant))
		case OutcomeDeny:
			if walk.requests[c.ID].Recovery {
				// A skipped recovery request: the call may have run, so
				// the model never reads "DENIED" — it would ask again.
				opts = append(opts, core.ResolveError(c.ID, skippedResult(c.Name, d.Reason)))
				continue
			}
			// A deny grant's refusal names its grant on the call's
			// denied span (weft.tool.denied_by grant, weft.grant.id):
			// reporting only, the denial is Deny's.
			if by, grant := s.approvalSourceLocked(d); by == approvedByGrant {
				opts = append(opts, core.DenyBy(c.ID, d.Reason, by, grant))
			} else {
				opts = append(opts, core.Deny(c.ID, d.Reason))
			}
		case OutcomeResolve:
			opts = append(opts, core.Resolve(c.ID, d.Content))
		case OutcomeResolveError:
			opts = append(opts, core.ResolveError(c.ID, d.Content))
		}
	}
	if denyUndecided && undecided && len(opts) == 0 {
		opts = append(opts, core.Deny(undecidedMarker, "no decision"))
	}
	return opts
}

// undecidedMarker is the call id of the decision a resume with no
// decided call carries to turn the core's resume on
// (danglingDecisionsLocked): empty, which no tool call carries — a
// model's call with an empty id fails its run (ErrModelContract) — so
// the core's Deny of an unknown id ignores it, and every pending call
// is the core's no_decision.
const undecidedMarker = ""

// The classes core.ApproveBy names on an approved call's execute_tool
// span (weft.tool.approved_by; record contract §3).
const (
	approvedByUser     = "user"     // Decide, DecideSigned: a person, one approval
	approvedByGrant    = "grant"    // a standing grant matched (weft.grant.id names it)
	approvedByApprover = "approver" // the WithApprover policy step
	approvedByQuorum   = "quorum"   // a Quorum(n > 1) of approvals
	approvedByPolicy   = "policy"   // any other session path (a pool parent's or child's record)
	approvedByRecovery = "recovery" // RecoverTurn's retry of a cut-off ReplaySafe or ReplayKeyed call
)

// approvalSourceLocked is an effective approval's provenance: the class
// that approved it and, for a grant, the grant's id — read from the
// chain's audit entry written just before the decision. Callers hold
// s.mu.
func (s *Session) approvalSourceLocked(d ApprovalDecisionEntry) (by, grant string) {
	if a, ok := s.precedingLocked(d.ID).(ApprovalAuditEntry); ok && d.ID != "" && a.Step == StepGrant && a.CallID == d.CallID {
		return approvedByGrant, a.GrantID
	}
	switch d.Via {
	case viaGrant:
		return approvedByGrant, ""
	case viaApprover:
		return approvedByApprover, ""
	case viaQuorum:
		return approvedByQuorum, ""
	case viaRecovery:
		return approvedByRecovery, ""
	case viaUser, viaSigned, "":
		quorum := s.cfg.quorum
		if i, ok := s.byID[d.RequestID]; ok && d.RequestID != "" {
			if re, ok := s.order[i].(ApprovalRequestEntry); ok {
				quorum = s.quorumOf(re)
			}
		}
		if quorum > 1 {
			return approvedByQuorum, ""
		}
		return approvedByUser, ""
	}
	return approvedByPolicy, ""
}

// recoveryDecisionErr refuses a decision a recovery request does not
// take (ADR 0032 rule D): it is approved — retry the call as it ran,
// with the same idempotency key — or denied or resolved — skip it. A
// standing approval (Always) would let the call run again unasked on a
// later turn; it is refused. Nil for every other request.
func recoveryDecisionErr(req Request, d Decision) error {
	if d.Kind == OutcomeEdit && req.Child != "" {
		return fmt.Errorf("%w: call %q is a delegated child's request: an edit is not forwarded to a pool child; approve or deny it",
			ErrInvalidDecision, d.CallID)
	}
	if !req.Recovery {
		return nil
	}
	if d.Kind == OutcomeEdit {
		return fmt.Errorf("%w: call %q is a recovery request: the call may have run with its arguments, so it is retried as it ran (approve) or skipped (deny); an edit is refused",
			ErrInvalidDecision, d.CallID)
	}
	if d.Always {
		return fmt.Errorf("%w: call %q is a recovery request: approve retries it, deny or resolve skips it; ApproveAlways is refused",
			ErrInvalidDecision, d.CallID)
	}
	return nil
}

// alreadyDecidedLocked reports whether d repeats a decision already
// recorded for the request it names: same request, outcome, reason,
// content, decider, shown arguments, standing wish and signing key. A
// decision naming no request never repeats one (G4). Callers hold s.mu.
func (s *Session) alreadyDecidedLocked(d recordedDecision) bool {
	if d.RequestID == "" {
		return false
	}
	for _, e := range s.order {
		de, ok := e.(ApprovalDecisionEntry)
		if !ok || de.RequestID != d.RequestID || de.CallID != d.CallID {
			continue
		}
		if sameDecision(de, d) {
			return true
		}
	}
	return false
}

// sameDecision reports whether a recorded decision entry is d again:
// same outcome, reason, content, decider, shown arguments, standing
// wish, signing key, edit and cosigners.
func sameDecision(de ApprovalDecisionEntry, d recordedDecision) bool {
	return de.CallID == d.CallID && de.Outcome == d.Kind && de.Reason == d.Reason && de.Content == d.Content && de.Who == d.Who &&
		de.ArgsSHA256 == d.ArgsSHA256 && de.Always == (d.Always && d.Kind == OutcomeApprove) && de.KeyID == d.keyID &&
		de.EditedArgsSHA256 == editedHash(d.Decision) && slices.Equal(de.Cosigners, d.Cosigners)
}

// Bind returns d bound to the request it answers: the request entry's
// id and the hash of the arguments it shows (Request.ID,
// Request.ArgsSHA256), and its call. Decide refuses a decision that
// names neither (ErrInvalidDecision): a decision states exactly what
// it decided, so a retry is recognised (a no-op) and a decision never
// lands on another occurrence of the call.
func (d Decision) Bind(r Request) Decision {
	d.CallID, d.RequestID, d.ArgsSHA256 = r.CallID, r.ID, r.ArgsSHA256
	return d
}
