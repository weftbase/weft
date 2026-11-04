package thread_test

import (
	"context"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// A grant records who made it and through which door, and every call
// it decides is attributable to that grantor: the automatic decision's
// Who is the grant's. Both ways in — Session.Grant and an
// ApproveAlways decision — keep the grantor.
func TestGrantRecordsItsGrantor(t *testing.T) {
	ctx := context.Background()
	refund := core.Tool("refund", "Refund an order.",
		func(ctx context.Context, in refundInput) (string, error) { return "refunded " + in.OrderID, nil },
		core.RequireApproval())
	call := func(id, order string) wefttest.Turn {
		return wefttest.ToolCalls(wefttest.Call{Name: "refund", ID: id, Args: `{"order_id":"` + order + `"}`})
	}
	agent := core.New(wefttest.Script(
		call("call_1", "1"), wefttest.Say("ok"), // granted by Session.Grant
		call("call_2", "2"), wefttest.Say("ok"), // parks; approved always
		call("call_3", "2"), wefttest.Say("ok"), // granted by the minted grant
	), core.Name("grantor"), refund)
	s, err := thread.Create(ctx, thread.Memory(), agent)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(ctx) }()
	if err := s.Grant(ctx, thread.Grant{Tool: "refund", Args: []thread.Arg{thread.ArgEquals("/order_id", []byte(`"1"`))}, Who: "merchant:42"}); err != nil {
		t.Fatal(err)
	}
	send := func(prompt string) *core.RunResult {
		t.Helper()
		turn, err := s.Send(ctx, core.User(prompt))
		if err != nil {
			t.Fatal(err)
		}
		res, err := turn.Wait()
		if err != nil {
			t.Fatal(err)
		}
		if res.Pending == nil && turn.Next() != nil {
			if res, err = turn.Next().Wait(); err != nil {
				t.Fatal(err)
			}
		}
		return res
	}
	send("refund 1")
	if res := send("refund 2"); len(res.Pending) != 1 {
		t.Fatalf("call_2 should park: %+v", res.Pending)
	}
	d := thread.ApproveAlways("call_2")
	d.Who = "merchant:7"
	rt, err := s.Decide(ctx, bound(s, d))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Wait(); err != nil {
		t.Fatal(err)
	}
	send("refund 2 again")

	var grants []thread.GrantEntry
	byCall := map[string]thread.ApprovalDecisionEntry{}
	for _, e := range s.Audit() {
		switch e := e.(type) {
		case thread.GrantEntry:
			grants = append(grants, e)
		case thread.ApprovalDecisionEntry:
			byCall[e.CallID] = e
		}
	}
	if len(grants) != 2 {
		t.Fatalf("grants: %+v", grants)
	}
	if g := grants[0]; g.Who != "merchant:42" || g.Via != "user" {
		t.Fatalf("Session.Grant's entry: who %q via %q, want merchant:42 via user", g.Who, g.Via)
	}
	if g := grants[1]; g.Who != "merchant:7" || g.Via != "user" {
		t.Fatalf("ApproveAlways's grant: who %q via %q, want merchant:7 via user", g.Who, g.Via)
	}
	for call, who := range map[string]string{"call_1": "merchant:42", "call_3": "merchant:7"} {
		if d := byCall[call]; d.Via != "grant" || d.Who != who {
			t.Fatalf("%s decided via %q by %q, want via grant by %s", call, d.Via, d.Who, who)
		}
	}
}
