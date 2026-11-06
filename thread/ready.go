package thread

import (
	"sync"
)

// ReadyKind names the work a Ready describes, and so the call that
// starts it.
type ReadyKind string

const (
	// ReadyResume: an approval boundary has every call decided — or
	// every undecided request lapsed, which the resume denies first —
	// and its resume run has not started. Start it with Session.Resume.
	ReadyResume ReadyKind = "resume"
	// ReadyQueued: sends wait in the session's queue — accepted while a
	// turn ran, restored by Open, or follow-ups of steers — and nothing
	// runs them. Start the head with Session.Continue.
	ReadyQueued ReadyKind = "queued"
	// ReadyRecover: the session's file shows a turn a crash or a
	// shutdown cut off (ADR 0032). Handle it with Session.RecoverTurn,
	// after Claim: without the lease it is only a candidate — the turn
	// may be running in another process. Listed by Session.Ready,
	// never handed to OnReady: it can only exist when the Session was
	// opened.
	ReadyRecover ReadyKind = "recover"
)

// Ready is one piece of work a session holds and is not running: what
// it is and which run it belongs to. A platform maps the run to
// whatever it routes by (its own record of the run, the request's
// Metadata).
type Ready struct {
	// Session is the session holding the work.
	Session string
	// Kind says what to call: Resume, Continue or RecoverTurn.
	Kind ReadyKind
	// RunID names the run the work belongs to: the parked run a resume
	// resolves (ReadyResume), the run id minted for the queue's head
	// (ReadyQueued; empty when only restored steers wait), the run
	// that was cut off (ReadyRecover).
	RunID string
}

type onReadyOption func(Ready)

func (o onReadyOption) applySession(c *sessionConfig) {
	if o != nil {
		c.onReady = o
	}
}

// OnReady hands the session's pending work to fn instead of starting
// it: one resumer — a platform's queue, a desktop app's job runner —
// decides when work runs. Under it a session never starts a run nobody
// asked for: a boundary whose last decision lands is not resumed (it
// implies AutoResume(false), whatever AutoResume says), and sends
// queued behind a turn are not drained when it ends. Each time such
// work becomes ready the session calls fn, and fn's owner starts it
// when it admits it — Session.Resume for ReadyResume, Session.Continue
// for ReadyQueued (one queued send per call; the next is handed over
// when it ends). A Send on an idle session still runs at once — its
// caller is the one admitting it — and a thread/pool child's runs stay
// its pool's.
//
// fn is called after the append that made the work ready, on a
// goroutine of the session's own, never under its lock and never on
// the caller's — it may call back into the Session — one call at a
// time, in order. It is an in-process, best-effort hint, not a lease
// and not a durable event: the same work may be handed over more than
// once, work handed over may be gone when its owner acts (Resume then
// fails with ErrNotPending, Continue returns a nil turn), and a hint is
// lost with its process. Whether the work it names survives a power
// loss is the backend's append guarantee (FsyncOnFlush for jsonl).
// Work that was ready before the Session existed — a boundary decided
// before a crash, sends restored by Open, a cut-off turn — is not
// handed over: Session.Ready lists it, for the owner's boot recovery
// (Open → Claim → Ready → act). RecoverTurn, like Resume and Continue,
// is the owner's explicit start: under OnReady it still starts the run
// it decides — a continuation, or the resume when its own decisions
// (a keyed retry, a Reconcile) complete the boundary — and returns that
// Turn already running; fn is not called for it. fn must not block for long: a slow fn
// delays the next hint, nothing else; a panic in it is contained. A nil
// fn is ignored.
func OnReady(fn func(Ready)) SessionOption { return onReadyOption(fn) }

// notifier delivers a session's OnReady hints in order on one
// goroutine, started on demand and gone when the backlog is empty.
type notifier struct {
	mu      sync.Mutex
	backlog []Ready
	running bool
}

func (n *notifier) post(fn func(Ready), r Ready) {
	n.mu.Lock()
	n.backlog = append(n.backlog, r)
	if n.running {
		n.mu.Unlock()
		return
	}
	n.running = true
	n.mu.Unlock()
	go n.drain(fn)
}

func (n *notifier) drain(fn func(Ready)) {
	for {
		n.mu.Lock()
		if len(n.backlog) == 0 {
			n.running = false
			n.mu.Unlock()
			return
		}
		r := n.backlog[0]
		n.backlog = n.backlog[1:]
		n.mu.Unlock()
		func() {
			defer func() { _ = recover() }() // a callback's panic never takes the session down
			fn(r)
		}()
	}
}

// readyHintsLocked reports whether OnReady decides when work runs.
// Callers hold s.mu.
func (s *Session) readyHintsLocked() bool { return s.cfg.onReady != nil }

// announceResumeLocked hands a settled boundary's resume to OnReady.
// Callers hold s.mu and have checked the boundary is settled.
func (s *Session) announceResumeLocked() {
	if s.cfg.onReady == nil {
		return
	}
	s.notify.post(s.cfg.onReady, s.readyResumeLocked())
}

// readyResumeLocked describes the open boundary's resume. Callers hold
// s.mu.
func (s *Session) readyResumeLocked() Ready {
	run, _ := s.resumeLinkLocked()
	return Ready{Session: s.header.ID, Kind: ReadyResume, RunID: run}
}

// announceQueuedLocked hands the queue's head to OnReady. Callers hold
// s.mu.
func (s *Session) announceQueuedLocked() {
	if s.cfg.onReady == nil || len(s.queue) == 0 {
		return
	}
	s.notify.post(s.cfg.onReady, s.readyQueuedLocked())
}

func (s *Session) readyQueuedLocked() Ready {
	r := Ready{Session: s.header.ID, Kind: ReadyQueued}
	if len(s.queue) > 0 {
		r.RunID = s.queue[0].turn.RunID()
	}
	return r
}

// Ready lists the work the session holds and is not running, computed
// from the journal: a cut-off turn (ReadyRecover, listed alone —
// RecoverTurn it first), a boundary whose every call is decided or
// whose undecided requests have all lapsed (ReadyResume: Resume denies
// the lapsed ones, then runs), sends or restored steers waiting in the
// queue (ReadyQueued, the head send's run id). Nil when nothing waits,
// or while a turn runs (its runner reaches the rest). It reads only:
// no lapsed request is denied by asking.
//
// It is what a resumer's boot recovery asks after Open, and what an
// AutoResume(false) session without OnReady reads. Open takes no lock,
// so a session another worker is running can read ReadyRecover here:
// Claim before acting, and leave a session whose Claim fails with
// ErrLocked.
func (s *Session) Ready() []Ready {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return nil
	}
	if x, ok := s.cutOffLocked(); ok {
		// RecoverTurn first: until it has, a boundary that looks
		// settled may be a resume the crash cut short.
		return []Ready{{Session: s.header.ID, Kind: ReadyRecover, RunID: x.run}}
	}
	var out []Ready
	if s.boundaryLocked() {
		if s.await.resumed == nil && s.settledOrLapsedLocked() {
			out = append(out, s.readyResumeLocked())
		}
		return out // queued sends wait behind the boundary
	}
	if len(s.queue) > 0 || len(s.steerQueue) > 0 {
		out = append(out, s.readyQueuedLocked())
	}
	return out
}

// settledOrLapsedLocked reports, reading only, whether the open
// boundary needs no more decisions: every pending request is strictly
// past its expiry (a resume denies those first). Callers hold s.mu.
func (s *Session) settledOrLapsedLocked() bool {
	now := s.approvalNow()
	for _, r := range s.pendingLocked() {
		if r.Expiry.IsZero() || !now.After(r.Expiry) {
			return false
		}
	}
	return true
}
