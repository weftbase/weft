package thread

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/weftbase/weft/core"
)

// IdempotencyKey is the recovery-stable key of one tool call in a
// session (ADR 0031): 32 lowercase hex digits, the first 128 bits of
// the SHA-256 of the session id, the id of the assistant message entry
// that holds the call, and the call id, NUL-separated. Every run of a
// Session hands it to the loop (core.IdempotencyKeys), so a tool reads
// it as core.Call.IdempotencyKey: the same call keeps it across a
// crash, a restart, a resume after an approval and a recovery retry,
// because all of them run the call from the same recorded assistant
// message — on any worker, with no store. A call the model issues anew
// — even with the same call id, in a later turn — sits in another
// message entry and gets another key. A Fork copies entries under new
// ids, so a forked call has a key of its own.
//
// Request.IdempotencyKey carries it for a parked call; a platform
// computes it itself only to name a key outside the session.
func IdempotencyKey(sessionID, entryID, callID string) string {
	h := sha256.Sum256([]byte(sessionID + "\x00" + entryID + "\x00" + callID))
	return hex.EncodeToString(h[:16])
}

// idempotencyKeys is the run option naming each executed call's key
// from the tree: the assistant message entry holding the call, as the
// attempt knows it (stepPersist.calls) — its own step's, appended by
// the step observer before the loop dispatches the step's calls, or,
// for a resume, the boundary's. Never an older message that reused the
// call id. A call the attempt cannot place (its step's append failed
// and is held) gets "", the loop's run-scoped key.
func (s *Session) idempotencyKeys(sp *stepPersist) core.RunOption {
	return core.IdempotencyKeys(func(c core.Call) string {
		s.mu.Lock()
		defer s.mu.Unlock()
		entry := sp.calls[c.CallID]
		if entry == "" {
			return ""
		}
		return IdempotencyKey(s.header.ID, entry, c.CallID)
	})
}

// callEntryLocked returns the id of the nearest assistant message entry
// on the leaf's path holding a tool call with callID, "" when none
// does. Callers hold s.mu.
func (s *Session) callEntryLocked(callID string) string {
	path, err := s.walkLocked(s.leaf)
	if err != nil {
		return ""
	}
	for j := len(path) - 1; j >= 0; j-- {
		me, ok := path[j].(MessageEntry)
		if !ok || me.Message.Role != core.RoleAssistant {
			continue
		}
		for _, p := range me.Message.Content {
			if c, ok := p.(core.ToolCallPart); ok && c.ID == callID {
				return me.ID
			}
		}
	}
	return ""
}

// requestKeyLocked sets r.IdempotencyKey: the key the parked call runs
// under, from the assistant entry on the leaf's path that holds it.
// A mirrored child request's key is the child session's, named there.
// Callers hold s.mu.
func (s *Session) requestKeyLocked(r *Request) {
	if r.Child != "" {
		return
	}
	if entry := s.callEntryLocked(r.CallID); entry != "" {
		r.IdempotencyKey = IdempotencyKey(s.header.ID, entry, r.CallID)
	}
}
