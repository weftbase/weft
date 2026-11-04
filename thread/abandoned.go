package thread

import (
	"sync"
	"time"
)

// The process-wide registry of abandoned handlers (ADR 0032, the
// 2026-11 amendment): a handler the loop abandoned — its Timeout
// passed, a cancellation it ignored — that the end of its turn left
// running past ladder.AbandonedWait. Its call parks as a recovery request
// for a person, and an approval of it (a retry) is refused with
// ErrStillRunning while the handler runs: a second handler beside the
// first could repeat its effect.
//
// The registry lives beside the process, not the Session: a Close and
// an Open of the same session in one process — a server evicting an
// idle session and reopening it for the decision — must still see the
// handler the first Session abandoned. An entry is added by the turn's
// end that parks the call and removed by core.AfterDispatch when the
// handler returns, whatever became of the request in between (approved
// later, denied, resolved, still pending), so the registry holds
// exactly the abandoned handlers still running. A restart ends them
// with the process, and the registry with them.
//
// The key is the call's occurrence: session id, run id, call id. Run
// ids embed the session id (<session>-t<n>), and session ids are
// minted random unless IDs pins them, so two stores holding sessions
// of one id — the same id pinned in two test stores, say — are told
// apart by the session's header Created as well, the identity the
// lease already uses for "the same stored session" (leased.created).
// A lookup that cannot name Created (AbandonedHandler, from a pool
// parent that knows only its child's id) matches any: a collision there can
// only refuse an approval, never let one through beside a handler.
var abandoned = &abandonedRegistry{m: map[abandonKey][]abandonedHandler{}}

// abandonKey is one call occurrence of one session.
type abandonKey struct {
	session, run, call string
}

// abandonedHandler is one registered handler: the session's header
// time it ran under, and the channel AfterDispatch closes when it
// returns.
type abandonedHandler struct {
	created time.Time
	done    <-chan struct{}
}

type abandonedRegistry struct {
	mu sync.Mutex
	m  map[abandonKey][]abandonedHandler
}

// add registers a still-running handler under k. A handler that has
// returned already is not registered.
func (r *abandonedRegistry) add(k abandonKey, created time.Time, done <-chan struct{}) {
	select {
	case <-done:
		return
	default:
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.m[k] = append(r.m[k], abandonedHandler{created: created, done: done})
}

// release removes the handler whose done channel is done from k: it
// has returned.
func (r *abandonedRegistry) release(k abandonKey, done <-chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	hs := r.m[k]
	for i, h := range hs {
		if h.done == done {
			hs = append(hs[:i:i], hs[i+1:]...)
			break
		}
	}
	if len(hs) == 0 {
		delete(r.m, k)
	} else {
		r.m[k] = hs
	}
}

// running reports whether a handler registered under k for the
// session header time created is still running. Entries whose handler
// has returned are dropped on the way.
func (r *abandonedRegistry) running(k abandonKey, created time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	hs := r.m[k]
	live := hs[:0]
	found := false
	for _, h := range hs {
		select {
		case <-h.done:
			continue
		default:
		}
		live = append(live, h)
		if h.created.Equal(created) {
			found = true
		}
	}
	if len(live) == 0 {
		delete(r.m, k)
	} else {
		r.m[k] = live
	}
	return found
}

// AbandonedHandler reports a handler its turn's end left running past
// its bound, in this process, for call callID of run runID of session:
// a channel closed when the handler returns, nil when none is running
// — none was abandoned, it has returned, or this is a new process (a
// restart ended every such handler). While one runs, an approval of
// the call's recovery request is refused with ErrStillRunning. The
// call is named in the session's own terms: a pool parent's mirror
// names it by the child session, the child's run and the child's own
// call id. thread/pool asks it before a parent records an approval the
// child could only refuse, and waits on it to replay one the child did
// refuse.
func AbandonedHandler(session, runID, callID string) <-chan struct{} {
	return abandoned.live(abandonKey{session: session, run: runID, call: callID})
}

// live returns the done channel of a handler registered under k that
// is still running, of any session header time; nil when there is
// none.
func (r *abandonedRegistry) live(k abandonKey) <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, h := range r.m[k] {
		select {
		case <-h.done:
		default:
			return h.done
		}
	}
	return nil
}
