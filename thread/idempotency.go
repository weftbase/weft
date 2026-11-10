package thread

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/weftbase/weft/core"
)

// IdempotencyKey is the recovery-stable key of one tool call in a
// session (ADR 0031): 32 lowercase hex digits, the first 128 bits of
// the SHA-256 of the session id, the id of the assistant message entry
// that holds the call, and the call id, NUL-separated — and, for a call
// that runs on edited arguments (an edit's revision), the edited
// arguments' hash after them, so a receiver that deduplicates on the
// key neither rejects nor replays the edited request as the original
// one; editedArgsSHA256 is "" for every other call. A recovery retry
// of an edited call keeps its edited key. Every run of a
// Session hands it to the loop (core.IdempotencyKeys), so a tool reads
// it as core.Call.IdempotencyKey: the same call keeps it across a
// crash, a restart, a resume after an approval and a recovery retry,
// because all of them run the call from the same recorded assistant
// message — on any worker, with no store. A call the model issues anew
// — even with the same call id, in a later turn — sits in another
// message entry and gets another key: a second identical order is a
// second effect. A forked call has a key of its own: the fork is
// another session.
//
// The one exception is an effect whose outcome is unknown (ADR 0032,
// the 2026-11-10 amendment). Once a call's outcome is unknown in a
// session — its abandoned handler may still run, a person skipped its
// recovery request, the model read OUTCOME_UNKNOWN — a later call in
// the session to the same tool (not ReplaySafe) with the same
// canonical arguments runs under that call's key, not this one, until
// a call under the key gets an answer of its own; a Fork carries the
// open ones with the origin's keys. The session records each such key
// (an ApprovalAuditEntry with step StepKey) before the call runs, and
// names it in core.Call.IdempotencyKey and Request.IdempotencyKey.
//
// Request.IdempotencyKey carries it for a parked call; a platform
// computes it itself only to name a key outside the session.
func IdempotencyKey(sessionID, entryID, callID, editedArgsSHA256 string) string {
	in := sessionID + "\x00" + entryID + "\x00" + callID
	if editedArgsSHA256 != "" {
		in += "\x00" + editedArgsSHA256
	}
	h := sha256.Sum256([]byte(in))
	return hex.EncodeToString(h[:16])
}

// callKeyLocked is the key a call runs under: an inherited key the
// file names for it (effectIndexLocked), else its derived one.
// Callers hold s.mu.
func (s *Session) callKeyLocked(entry, callID string) string {
	return s.stepKeyLocked(entry, callID, true)
}

// derivedKeyLocked is the call's occurrence key: from the entry
// holding it and, when its request runs it on edited arguments, their
// hash. Callers hold s.mu.
func (s *Session) derivedKeyLocked(entry, callID string) string {
	edited := ""
	if c, ok := s.callInEntryLocked(entry, callID); ok {
		if _, sha, isEdited, _ := editedArgsOf(s.approvalWalkLocked().requests[callID], c); isEdited {
			edited = sha
		}
	}
	return IdempotencyKey(s.header.ID, entry, callID, edited)
}

// callInEntryLocked returns the call part callID of the message entry
// with id entry. Callers hold s.mu.
func (s *Session) callInEntryLocked(entry, callID string) (core.ToolCallPart, bool) {
	i, ok := s.byID[entry]
	if !ok {
		return core.ToolCallPart{}, false
	}
	me, ok := s.order[i].(MessageEntry)
	if !ok {
		return core.ToolCallPart{}, false
	}
	for _, p := range me.Message.Content {
		if c, ok := p.(core.ToolCallPart); ok && c.ID == callID {
			return c, true
		}
	}
	return core.ToolCallPart{}, false
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
		// The step's own call is never edited; either may inherit.
		return s.stepKeyLocked(entry, c.CallID, sp.resume)
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
		return // requestFromEntry carried the child's own
	}
	if entry := s.callEntryLocked(r.CallID); entry != "" {
		r.IdempotencyKey = s.callKeyLocked(entry, r.CallID)
	}
}
