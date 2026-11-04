package thread

import "sync"

// The process-wide registry of abandoned handlers (ADR 0032, the
// 2026-11 amendment): a handler the loop abandoned — its Timeout
// passed, a cancellation it ignored — that the end of its turn left
// running: past ladder.AbandonedWait, or under a Close that gave up on
// it (the turn is cut off, recorded for RecoverTurn). Its call parks
// as a recovery request for a person; nothing automatic runs it again
// (no keyed retry, no Reconcile) and an approval of it (a retry) is
// refused with ErrStillRunning while the handler runs: a second
// handler beside the first could repeat its effect.
//
// The registry lives beside the process, not the Session: a Close and
// an Open of the same session in one process — a server evicting an
// idle session and reopening it for the decision — must still see the
// handler the first Session abandoned, and so must a Fork that copied
// the request. An entry is added by the turn's end that leaves the
// handler running and removed by core.AfterDispatch when the handler
// returns, whatever became of the request in between (approved later,
// denied, resolved, still pending), so the registry holds exactly the
// abandoned handlers still running. A restart ends them with the
// process, and the registry with them.
//
// The key is the call's occurrence: run id and call id, whatever
// session asks. Run ids embed the session they were minted in
// (<session>-t<n>), so the key names the origin session too, and a
// fork — or a reopen — that carries the request asks under the same
// key. Two sessions of one pinned id in two stores collide; a collision
// can only refuse an approval, never let one through beside a handler.
var abandoned = &abandonedRegistry{m: map[abandonKey][]<-chan struct{}{}}

// abandonKey is one call occurrence: its run and its call id.
type abandonKey struct {
	run, call string
}

type abandonedRegistry struct {
	mu sync.Mutex
	m  map[abandonKey][]<-chan struct{} // the done channels AfterDispatch closes
}

// add registers a still-running handler under k. A handler that has
// returned already is not registered.
func (r *abandonedRegistry) add(k abandonKey, done <-chan struct{}) {
	select {
	case <-done:
		return
	default:
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.m[k] = append(r.m[k], done)
}

// release removes the handler whose done channel is done from k: it
// has returned.
func (r *abandonedRegistry) release(k abandonKey, done <-chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	hs := r.m[k]
	for i, h := range hs {
		if h == done {
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

// live returns the done channel of a handler registered under k that
// is still running; nil when there is none.
func (r *abandonedRegistry) live(k abandonKey) <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, done := range r.m[k] {
		select {
		case <-done:
		default:
			return done
		}
	}
	return nil
}

// AbandonedHandler reports a handler its turn's end left running, in
// this process, for call callID of run runID: a channel closed when
// the handler returns, nil when none is running — none was abandoned,
// it has returned, or this is a new process (a restart ended every
// such handler). While one runs, an approval of the call's recovery
// request is refused with ErrStillRunning, from any session that
// holds the request (the one that ran it, a reopen of it, a fork of
// it). The call is named in the terms of the session that ran it: a
// pool parent's mirror names it by the child's run and the child's own
// call id. thread/pool asks it before a parent records an approval the
// child could only refuse, and waits on it to replay one the child did
// refuse.
func AbandonedHandler(runID, callID string) <-chan struct{} {
	return abandoned.live(abandonKey{run: runID, call: callID})
}

// handlerRunning reports whether call callID of run has a handler a
// turn's end left running that has not returned yet (AbandonedHandler).
func handlerRunning(run, callID string) bool {
	return AbandonedHandler(run, callID) != nil
}
