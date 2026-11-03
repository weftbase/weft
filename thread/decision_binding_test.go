package thread_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// A decision can be bound to what its decider was shown: the request
// occurrence (Decision.RequestID) and the arguments (ArgsSHA256). A
// page left open on an earlier occurrence of a reused call id — the
// refund of order 1, denied since — must not approve the later one
// that reuses the id for order 2. Both bindings are checked before
// anything is recorded; the hash the decider saw is on the entry.
func TestDecisionBindsWhatWasShown(t *testing.T) {
	ctx := context.Background()
	refund := core.Tool("refund", "Refund an order.",
		func(ctx context.Context, in refundInput) (string, error) { return "refunded " + in.OrderID, nil },
		core.RequireApproval())
	call := func(order string) wefttest.Turn {
		return wefttest.ToolCalls(wefttest.Call{Name: "refund", ID: "call_1", Args: `{"order_id":"` + order + `"}`})
	}
	agent := core.New(wefttest.Script(call("1"), wefttest.Say("ok"), call("2"), wefttest.Say("ok")),
		core.Name("binding"), refund)
	s, err := thread.Create(ctx, thread.Memory(), agent)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(ctx) }()

	park := func(prompt string) thread.Request {
		t.Helper()
		turn, err := s.Send(ctx, core.User(prompt))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := turn.Wait(); err != nil {
			t.Fatal(err)
		}
		p := s.Pending()
		if len(p) != 1 {
			t.Fatalf("pending: %+v", p)
		}
		return p[0]
	}
	first := park("refund 1")
	shown := first // the page an approver left open
	d := thread.Deny("call_1", "no")
	d.RequestID, d.ArgsSHA256 = first.ID, first.ArgsSHA256
	rt, err := s.Decide(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Wait(); err != nil {
		t.Fatal(err)
	}
	second := park("refund 2")
	if second.CallID != shown.CallID || second.ID == shown.ID {
		t.Fatalf("want a second occurrence of call_1: %+v vs %+v", second, shown)
	}

	stale := thread.Approve("call_1")
	stale.RequestID = shown.ID
	if _, err := s.Decide(ctx, stale); !errors.Is(err, thread.ErrNotPending) {
		t.Fatalf("approving the occurrence the page showed: err = %v, want ErrNotPending", err)
	}
	stale = thread.Approve("call_1")
	stale.ArgsSHA256 = shown.ArgsSHA256
	if _, err := s.Decide(ctx, stale); !errors.Is(err, thread.ErrArgsChanged) {
		t.Fatalf("approving the arguments the page showed: err = %v, want ErrArgsChanged", err)
	}
	if p := s.Pending(); len(p) != 1 {
		t.Fatalf("a refused decision recorded something: pending %+v", p)
	}

	sum := sha256.Sum256([]byte(`{"order_id":"2"}`))
	if hex.EncodeToString(sum[:]) != second.ArgsSHA256 {
		t.Fatalf("ArgsSHA256 is not the hex SHA-256 of the args")
	}
	ok := thread.Approve("call_1")
	ok.RequestID, ok.ArgsSHA256, ok.Who = second.ID, second.ArgsSHA256, "merchant:42"
	rt, err = s.Decide(ctx, ok)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Wait(); err != nil {
		t.Fatal(err)
	}
	var recorded *thread.ApprovalDecisionEntry
	for _, e := range s.Audit() {
		if d, ok := e.(thread.ApprovalDecisionEntry); ok && d.RequestID == second.ID {
			recorded = &d
		}
	}
	if recorded == nil || recorded.ArgsSHA256 != second.ArgsSHA256 || recorded.Who != "merchant:42" {
		t.Fatalf("decision entry = %+v, want the args hash the decider saw", recorded)
	}
}
