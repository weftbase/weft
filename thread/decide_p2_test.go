package thread_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// Review P2: under Quorum, the same bound decision again before the
// quorum is met is a no-op — nothing recorded twice — as it is once the
// request is decided (G3, G4).
func TestQuorumRepeatIsANoOp(t *testing.T) {
	gated := core.Tool("wire", "Wire.", func(_ context.Context, _ struct{}) (string, error) { return "wired", nil }, core.RequireApproval())
	s, err := thread.Create(bg, thread.Memory(), core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "wire", ID: "w1", Args: `{}`}),
		wefttest.Say("done"),
	), gated), thread.Quorum(2))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(bg) }()
	turn, _ := s.Send(bg, core.User("wire"))
	if _, err := turn.Wait(); err != nil {
		t.Fatal(err)
	}
	d := thread.Approve("w1")
	d.Who = "alice"
	d = bound(s, d)
	for range 2 {
		if rt, err := s.Decide(bg, d); err != nil || rt != nil {
			t.Fatalf("Decide = %v, %v; want recorded, quorum open", rt, err)
		}
	}
	n := 0
	for _, e := range s.Entries() {
		if de, ok := e.(thread.ApprovalDecisionEntry); ok && de.CallID == "w1" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d decision entries, want 1: the repeat is a no-op", n)
	}
}

// Review P2: a dangling call with no request entry (a turn recorded
// pending without the chain's request) is still offered by Pending —
// with a deterministic ID, so a bound decision can decide it.
func TestDanglingRequestIsDecidable(t *testing.T) {
	st := thread.Memory()
	const id = "s_dangling"
	if err := st.Create(bg, thread.Header{ID: id, Created: at(0)}); err != nil {
		t.Fatal(err)
	}
	call := core.ToolCallPart{ID: "w1", Name: "wire", Args: json.RawMessage(`{}`)}
	entries := []thread.Entry{
		thread.MessageEntry{ID: "e_1", Created: at(1), Message: core.User("wire"), RunID: id + "-t1"},
		thread.MessageEntry{ID: "e_2", ParentID: "e_1", Created: at(2), Message: core.Message{Role: core.RoleAssistant, Content: []core.Part{call}}},
		thread.TurnEntry{ID: "e_3", ParentID: "e_2", Created: at(3), RunID: id + "-t1", Pending: []core.ToolCallPart{call}},
	}
	if err := st.Append(bg, id, entries...); err != nil {
		t.Fatal(err)
	}
	wired := 0
	s, err := thread.Open(bg, st, id, core.New(wefttest.Script(wefttest.Say("done")),
		core.Tool("wire", "Wire.", func(_ context.Context, _ struct{}) (string, error) { wired++; return "wired", nil }, core.RequireApproval())))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(bg) }()
	p := s.Pending()
	if len(p) != 1 || p[0].ID == "" {
		t.Fatalf("Pending = %+v, want one request with an ID", p)
	}
	if again := s.Pending(); again[0].ID != p[0].ID {
		t.Fatalf("the ID is not deterministic: %q then %q", p[0].ID, again[0].ID)
	}
	rt, err := s.Decide(bg, thread.Approve("w1").Bind(p[0]))
	if err != nil || rt == nil {
		t.Fatalf("Decide = %v, %v", rt, err)
	}
	if _, err := rt.Wait(); err != nil {
		t.Fatal(err)
	}
	if wired != 1 {
		t.Fatalf("wired %d times", wired)
	}
}
