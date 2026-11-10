package thread

import (
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
)

// effectHash settles canonical arguments (ADR 0032, 2026-11-10): key
// order, space and escaping do not matter, numbers compare by value,
// and a string, a null and an absent field stay distinct.
func TestEffectHashCanonical(t *testing.T) {
	same := [][]string{
		{`{"a":1,"b":[1,2]}`, `{ "b" : [1, 2.0], "a" : 1e0 }`, `{"b":[10e-1,2],"a":1.00}`},
		{`{"x":0.5}`, `{"x":5e-1}`, `{"x":0.50}`, `{"x":50E-2}`},
		{`{"n":-0}`, `{"n":0}`, `{"n":0.0}`},
		{`{"s":"A<"}`, `{"s":"A<"}`},
		{`{"big":12345678901234567890123}`, `{"big":1.2345678901234567890123e22}`},
		{``, `null`, ` `},
	}
	for _, group := range same {
		want := effectHash([]byte(group[0]))
		for _, in := range group[1:] {
			if got := effectHash([]byte(in)); got != want {
				t.Errorf("effectHash(%s) != effectHash(%s)", in, group[0])
			}
		}
	}
	distinct := []string{`{"a":1}`, `{"a":"1"}`, `{"a":null}`, `{}`, `{"a":1.5}`, `{"a":12345678901234567891}`, `{"a":12345678901234567890}`, `{"a":1e999999}`, `{"a":10e999998}`, `not json`}
	seen := map[string]string{}
	for _, in := range distinct {
		h := effectHash([]byte(in))
		if prev, ok := seen[h]; ok {
			t.Errorf("effectHash(%s) == effectHash(%s)", in, prev)
		}
		seen[h] = in
	}
}

// The effect index is incremental (the file only grows): a lookup
// reads only the entries appended since the last one, and its answer
// equals a walk of the whole file.
func TestEffectIndexIsIncremental(t *testing.T) {
	s := &Session{header: Header{ID: "s_x"}, byID: map[string]int{}, agent: core.New(wefttest.Script())}
	add := func(e Entry) {
		s.byID[idOf(e)] = len(s.order)
		s.order = append(s.order, e)
	}
	add(MessageEntry{ID: "e_1", Message: core.Message{Role: core.RoleAssistant, Content: []core.Part{
		core.ToolCallPart{ID: "c1", Name: "ship", Args: []byte(`{"o":1}`)}}}})
	add(ApprovalRequestEntry{ID: "e_2", ParentID: "e_1", CallID: "c1", Tool: "ship", Args: []byte(`{"o":1}`), Recovery: true})
	_ = s.effectIndexLocked()
	if s.effWalk.n != 2 {
		t.Fatalf("read %d entries, want 2", s.effWalk.n)
	}
	add(MessageEntry{ID: "e_3", ParentID: "e_2", Message: core.Message{Role: core.RoleAssistant, Content: []core.Part{
		core.ToolCallPart{ID: "c2", Name: "ship", Args: []byte(`{"o":1.0}`)}}}})
	got := s.effectIndexLocked()
	if s.effWalk.n != 3 {
		t.Fatalf("read %d entries, want 3", s.effWalk.n)
	}
	want := s.effectIndexOfLocked(s.order)
	if got.keys[occurrence("e_3", "c2")] == "" || got.keys[occurrence("e_3", "c2")] != want.keys[occurrence("e_3", "c2")] ||
		got.keys[occurrence("e_3", "c2")] != IdempotencyKey("s_x", "e_1", "c1", "") {
		t.Fatalf("incremental %v, full %v: want c2 under c1's key", got.keys, want.keys)
	}
}
