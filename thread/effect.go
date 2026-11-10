package thread

import (
	"bytes"
	"encoding/json"
	"maps"
	"math/big"
	"slices"
	"strings"

	"github.com/weftbase/weft/core"
)

// Effect identity (ADR 0032, the 2026-11-10 amendment, rule D).
//
// thread.IdempotencyKey names a call occurrence: the assistant entry
// that holds it and its call id. That is the right key for every call
// whose outcome is known — a later identical call is a new effect, a
// second order on purpose. It is the wrong key for a call that repeats
// an effect whose outcome is unknown: the model, told OUTCOME_UNKNOWN
// (a person skipped the recovery request), asks for the same tool with
// the same arguments again, the repeat sits in a new entry, and under a
// new key a receiver that deduplicates cannot tell it is the same
// effect.
//
// So once a call's outcome is unknown in a session, a later call in the
// session to the same tool with the same canonical arguments runs under
// the earlier call's key — it inherits it — until a call under that key
// gets an answer of its own. The rule is read from the journal, never
// from memory, by one walk in file order (effectIndexLocked):
//
//   - an unknown outcome opens: a recovery request (the session's own,
//     or a pool child's mirrored with its key), a tool result reading
//     OUTCOME_UNKNOWN (a skipped recovery request), a key record
//     (StepKey: the gate's, or a Fork's carrying the origin's key);
//   - a call to a tool that is not ReplaySafe, whose effect (tool +
//     canonical arguments — an edit's revision: its edited arguments)
//     is open where the call sits in the file, inherits the open key;
//   - an answer that is not OUTCOME_UNKNOWN, for a call running under
//     the open key, closes it: the next identical call is a new effect.
//
// The walk is in file order, not path order: a Branch navigates the
// conversation, not the world, and an effect that may have happened on
// one line may have happened for every line.

// effectIndex is the walk's answer: the key every call occurrence on
// file runs under when it is not its derived one, and the effects
// still open at the end of the file.
type effectIndex struct {
	// keys maps an occurrence (entry id, NUL, call id) to the key it
	// runs under when that is not thread.IdempotencyKey's: inherited,
	// or named by a key record.
	keys map[string]string
	// open maps an effect (effectID) to the key of its open unknown
	// outcome.
	open map[string]openEffect
}

// openEffect is one effect of unknown outcome: the key it ran under,
// and the occurrence that opened it ("" entry for a mirror's).
type openEffect struct {
	key, entry, callID, tool, hash string
}

// effectID names an effect: the tool and the hash of its canonical
// arguments (effectHash).
func effectID(tool, hash string) string { return tool + "\x00" + hash }

func occurrence(entry, callID string) string { return entry + "\x00" + callID }

// effectHash is the hex SHA-256 of args in effect-canonical form: JSON
// decoded and encoded again with object keys sorted, insignificant
// space dropped, strings in one escaping, and every number by its exact
// value — 1, 1.0, 1e0 and 10e-1 are one number, 0.50 and 5e-1 another;
// "1" (a string) is not.
// Bytes that are not one JSON value hash as they are. null and an
// absent field stay distinct: the tool may read them differently.
func effectHash(args []byte) string {
	c, ok := effectCanonical(args)
	if !ok {
		return hashArgs(args)
	}
	return hashArgs(c)
}

// effectCanonical is effectHash's form; ok is false for bytes that are
// not one JSON value.
func effectCanonical(args []byte) ([]byte, bool) {
	if len(bytes.TrimSpace(args)) == 0 {
		return []byte("null"), true // no arguments: the tool reads none
	}
	dec := json.NewDecoder(bytes.NewReader(args))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil || dec.More() {
		return nil, false
	}
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(normalizeNumbers(v)); err != nil {
		return nil, false
	}
	return bytes.TrimRight(out.Bytes(), "\n"), true
}

// normalizeNumbers rewrites every json.Number in v to its exact value's
// one spelling: an integer in decimal, any other in the shortest exact
// decimal. A number whose exponent is too large to expand cheaply keeps its
// spelling: two spellings of it are two effects — the safe side is a
// new key, never a work bomb.
func normalizeNumbers(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			x[k] = normalizeNumbers(e)
		}
		return x
	case []any:
		for i, e := range x {
			x[i] = normalizeNumbers(e)
		}
		return x
	case json.Number:
		return normalizeNumber(string(x))
	}
	return v
}

// maxExponentDigits bounds the exponent normalizeNumber expands: a
// literal like 1e999999999 is kept as written.
const maxExponentDigits = 3

func normalizeNumber(s string) any {
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		exp := strings.TrimLeft(s[i+1:], "+-")
		if len(strings.TrimLeft(exp, "0")) > maxExponentDigits {
			return json.Number(s)
		}
	}
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return json.Number(s)
	}
	if r.IsInt() {
		return json.Number(r.Num().String())
	}
	// A JSON literal is a decimal fraction: its denominator divides a
	// power of ten, so it has an exact finite decimal spelling — the
	// fewest fraction digits that hold it.
	ten, pow, d := big.NewInt(10), big.NewInt(10), r.Denom()
	n := 1
	for new(big.Int).Mod(pow, d).Sign() != 0 {
		if n > len(s)+1000 { // not a decimal fraction after all: keep it
			return json.Number(s)
		}
		pow.Mul(pow, ten)
		n++
	}
	return json.Number(r.FloatString(n))
}

// isOutcomeUnknown reports whether a tool result is an unknown outcome
// the model read: an error result with the OUTCOME_UNKNOWN code.
func isOutcomeUnknown(r core.ToolResultPart) bool {
	return r.IsError && strings.HasPrefix(r.Content, core.CodeOutcomeUnknown+":")
}

// inheritsLocked reports whether a call to tool may inherit a key: a
// tool that is not ReplaySafe (a tool the agent no longer holds counts
// as not ReplaySafe). A ReplaySafe call repeats nothing. Callers hold
// s.mu.
func (s *Session) inheritsLocked(tool string) bool {
	def := s.toolLocked(tool)
	return def == nil || def.ReplayPolicy() != core.ReplaySafe
}

// effectIndexLocked walks the file once, in order (see the comment at
// the top of this file). Callers hold s.mu.
func (s *Session) effectIndexLocked() effectIndex {
	// Incremental: the file only grows (entries are appended, never
	// rewritten — T4), so the walk resumes where it stopped, and a
	// lookup costs the entries appended since — not the whole file.
	if s.effWalk == nil {
		s.effWalk = newEffectWalker()
	}
	for s.effWalk.n < len(s.order) {
		s.effWalk.feed(s, s.order[s.effWalk.n])
		s.effWalk.n++
	}
	return s.effWalk.idx
}

// effectIndexOfLocked is the walk over order, a prefix of the file (a
// Fork reads the file up to its fork point), from the start. Callers
// hold s.mu.
func (s *Session) effectIndexOfLocked(order []Entry) effectIndex {
	w := newEffectWalker()
	for _, e := range order {
		w.feed(s, e)
	}
	return w.idx
}

// effectWalker is the walk's state: the index so far, what it knows of
// each occurrence, and how many entries of the file it has read.
type effectWalker struct {
	idx      effectIndex
	occEff   map[string]string // occurrence → the effect it runs (edits move it)
	edited   map[string]string // occurrence → its edited arguments' canonical hash
	explicit map[string]bool   // occurrence → its key is a key record's
	n        int               // entries of s.order read (effectIndexLocked)
}

func newEffectWalker() *effectWalker {
	return &effectWalker{
		idx:    effectIndex{keys: map[string]string{}, open: map[string]openEffect{}},
		occEff: map[string]string{}, edited: map[string]string{}, explicit: map[string]bool{},
	}
}

// keyOf is the key occurrence occ runs under so far: inherited or a
// key record's, else derived.
func (w *effectWalker) keyOf(s *Session, occ, entry, callID string) string {
	if k, ok := w.idx.keys[occ]; ok {
		return k
	}
	return IdempotencyKey(s.header.ID, entry, callID, w.edited[occ])
}

// feed reads one entry of the file, in order. Callers hold s.mu.
func (w *effectWalker) feed(s *Session, e Entry) {
	switch e := e.(type) {
	case MessageEntry:
		switch e.Message.Role {
		case core.RoleAssistant:
			for _, p := range e.Message.Content {
				c, ok := p.(core.ToolCallPart)
				if !ok {
					continue
				}
				occ := occurrence(e.ID, c.ID)
				eff := effectID(c.Name, effectHash(c.Args))
				w.occEff[occ] = eff
				if o, ok := w.idx.open[eff]; ok && s.inheritsLocked(c.Name) && !w.explicit[occ] {
					w.idx.keys[occ] = o.key
				}
			}
		case core.RoleTool:
			for _, p := range e.Message.Content {
				r, ok := p.(core.ToolResultPart)
				if !ok {
					continue
				}
				entry := s.callAncestorLocked(e.ParentID, r.CallID)
				if entry == "" {
					continue
				}
				occ := occurrence(entry, r.CallID)
				eff, ok := w.occEff[occ]
				if !ok {
					continue
				}
				key := w.keyOf(s, occ, entry, r.CallID)
				if isOutcomeUnknown(r) {
					tool, hash, _ := strings.Cut(eff, "\x00")
					w.idx.open[eff] = openEffect{key: key, entry: entry, callID: r.CallID, tool: tool, hash: hash}
				} else if o, ok := w.idx.open[eff]; ok && o.key == key {
					delete(w.idx.open, eff)
				}
			}
		}
	case ApprovalRequestEntry:
		switch {
		case e.Child != "":
			// A pool child's request mirrored here: a recovery
			// request opens its effect under the child's key, so the
			// parent's own identical call inherits it (the pool's
			// mirror gives the same answer as the child).
			if e.Recovery && e.IdempotencyKey != "" {
				hash := effectHash(e.Args)
				w.idx.open[effectID(e.Tool, hash)] = openEffect{key: e.IdempotencyKey, callID: e.CallID, tool: e.Tool, hash: hash}
			}
		case e.RevisionOf != "":
			// An edit's revision: the call runs on the edited
			// arguments — a different effect, under a key of its own
			// unless that effect is open.
			entry := s.callAncestorLocked(e.ParentID, e.CallID)
			if entry == "" {
				return
			}
			occ := occurrence(entry, e.CallID)
			hash := effectHash(e.Args)
			eff := effectID(e.Tool, hash)
			w.occEff[occ] = eff
			w.edited[occ] = canonicalHash(e.Args)
			if w.explicit[occ] {
				return
			}
			if o, ok := w.idx.open[eff]; ok && s.inheritsLocked(e.Tool) {
				w.idx.keys[occ] = o.key
			} else {
				delete(w.idx.keys, occ)
			}
		case e.Recovery:
			entry := s.callAncestorLocked(e.ParentID, e.CallID)
			if entry == "" {
				return
			}
			occ := occurrence(entry, e.CallID)
			hash := effectHash(e.Args)
			eff := effectID(e.Tool, hash)
			w.occEff[occ] = eff
			w.idx.open[eff] = openEffect{key: w.keyOf(s, occ, entry, e.CallID), entry: entry, callID: e.CallID, tool: e.Tool, hash: hash}
		}
	case ApprovalAuditEntry:
		if e.Step != StepKey || e.Key == "" {
			return
		}
		occ := occurrence(e.Entry, e.CallID)
		w.idx.keys[occ] = e.Key
		w.explicit[occ] = true
		if _, held := s.byID[e.Entry]; held {
			w.occEff[occ] = effectID(e.Tool, e.ArgsSHA256)
		}
		w.idx.open[effectID(e.Tool, e.ArgsSHA256)] = openEffect{key: e.Key, entry: e.Entry, callID: e.CallID, tool: e.Tool, hash: e.ArgsSHA256}
	}
}

// callAncestorLocked returns the id of the nearest assistant message
// entry holding a call callID on the chain of parents from id up — the
// occurrence an entry about that call (its result, its request) is
// about; "" when there is none. Callers hold s.mu.
func (s *Session) callAncestorLocked(id, callID string) string {
	for n := 0; id != "" && n <= len(s.order); n++ {
		i, ok := s.byID[id]
		if !ok {
			return ""
		}
		if me, ok := s.order[i].(MessageEntry); ok && me.Message.Role == core.RoleAssistant {
			for _, p := range me.Message.Content {
				if c, ok := p.(core.ToolCallPart); ok && c.ID == callID {
					return me.ID
				}
			}
		}
		id = parentOf(s.order[i])
	}
	return ""
}

// stepKeyLocked is the key the call callID of the assistant entry entry
// runs under in a run attempt: a key record's or an inherited one when
// the file names one, else thread.IdempotencyKey — for a step's own
// call never edited, for a resume's the edited arguments' when its
// request is an edit's revision. Callers hold s.mu.
func (s *Session) stepKeyLocked(entry, callID string, resume bool) string {
	if k, ok := s.effectIndexLocked().keys[occurrence(entry, callID)]; ok {
		return k
	}
	if !resume {
		return IdempotencyKey(s.header.ID, entry, callID, "")
	}
	return s.derivedKeyLocked(entry, callID)
}

// toolRunningLocked reports whether an abandoned handler of tool still
// runs in this process for a call this session's file holds a recovery
// request of — its own, or a pool child's mirrored here (named in the
// child's terms, childCallRunning). A restart ends every such handler.
// Callers hold s.mu.
func (s *Session) toolRunningLocked(tool string) bool {
	if abandoned.empty() {
		return false
	}
	for _, e := range s.order {
		re, ok := e.(ApprovalRequestEntry)
		if !ok || !re.Recovery || re.Tool != tool {
			continue
		}
		if requestHandlerRunning(re.RunID, re.CallID, re.Child != "") {
			return true
		}
	}
	return false
}

// requestHandlerRunning reports whether the call a request names has
// an abandoned handler still running. A mirror's call id is the child's
// own namespaced by the child session, once per level of a chain, the
// run id the innermost child's: each suffix past a session prefix names
// a call in some child's terms.
func requestHandlerRunning(runID, callID string, mirror bool) bool {
	if !mirror {
		return handlerRunning(runID, callID)
	}
	parts := strings.Split(callID, "/")
	for i := 0; i+1 < len(parts); i++ {
		if handlerRunning(runID, strings.Join(parts[i+1:], "/")) {
			return true
		}
	}
	return false
}

// derivedRunKeyLocked is stepKeyLocked without inheritance: the
// occurrence key the call would run under were no effect open.
// Callers hold s.mu.
func (s *Session) derivedRunKeyLocked(entry, callID string, resume bool) string {
	if !resume {
		return IdempotencyKey(s.header.ID, entry, callID, "")
	}
	return s.derivedKeyLocked(entry, callID)
}

// forkKeyRecordsLocked is what a Fork at entryID carries of the
// origin's effect identity (ADR 0032, 2026-11-10): a key record, with
// the origin's key, for every effect of unknown outcome open at the
// fork point — the fork's history holds the call, so a repeat of it in
// the fork is the same effect — and for every call on the copied path
// that may run again under its key: it has no result there and was
// dispatched, parked as a recovery request, or runs under an inherited
// key. Without one the fork would derive each from its own session id,
// and a keyed retry, a Reconcile or a person's retry there would run
// the effect under a key the receiver never saw. A call that never
// started and inherits nothing — an ordinary pending request — keeps
// a key of its own in the fork, as before. The records carry no ids
// yet. Callers hold s.mu.
func (s *Session) forkKeyRecordsLocked(entryID string, path []Entry) []ApprovalAuditEntry {
	i, ok := s.byID[entryID]
	if !ok {
		return nil
	}
	idx := s.effectIndexOfLocked(s.order[:i+1])
	var out []ApprovalAuditEntry
	seen := map[string]bool{}
	add := func(entry, callID, tool, hash, key string) {
		occ := occurrence(entry, callID)
		if seen[occ] || key == "" {
			return
		}
		seen[occ] = true
		out = append(out, ApprovalAuditEntry{CallID: callID, Step: StepKey, Entry: entry, Tool: tool, ArgsSHA256: hash, Key: key})
	}
	answered := map[string]bool{} // occurrence → a result on the copied path
	mayRun := map[string]bool{}   // call id → dispatched or parked for recovery on the path
	for _, e := range path {
		switch e := e.(type) {
		case MessageEntry:
			if e.Message.Role != core.RoleTool {
				continue
			}
			for _, p := range e.Message.Content {
				if r, ok := p.(core.ToolResultPart); ok {
					if entry := s.callAncestorLocked(e.ParentID, r.CallID); entry != "" {
						answered[occurrence(entry, r.CallID)] = true
					}
				}
			}
		case ApprovalAuditEntry:
			if e.Step == StepDispatch {
				mayRun[e.CallID] = true
			}
		case ApprovalRequestEntry:
			if e.Recovery && e.Child == "" {
				mayRun[e.CallID] = true
			}
		}
	}
	for _, e := range path {
		me, ok := e.(MessageEntry)
		if !ok || me.Message.Role != core.RoleAssistant {
			continue
		}
		for _, p := range me.Message.Content {
			c, ok := p.(core.ToolCallPart)
			if !ok {
				continue
			}
			occ := occurrence(me.ID, c.ID)
			inherited, isInherited := idx.keys[occ]
			if answered[occ] || (!mayRun[c.ID] && !isInherited) {
				continue
			}
			key := inherited
			if !isInherited {
				key = s.derivedKeyLocked(me.ID, c.ID)
			}
			add(me.ID, c.ID, c.Name, effectHash(c.Args), key)
		}
	}
	// The open effects last, each whatever came before — the fork's walk
	// reads the last record of an effect as its open key, as the
	// origin's did — in a fixed order.
	effs := slices.Sorted(maps.Keys(idx.open))
	for _, eff := range effs {
		o := idx.open[eff]
		out = append(out, ApprovalAuditEntry{CallID: o.callID, Step: StepKey, Entry: o.entry, Tool: o.tool, ArgsSHA256: o.hash, Key: o.key})
	}
	return out
}
