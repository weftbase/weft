package thread_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/thread/jsonl"
	"github.com/weftbase/weft/wefttest"
)

// A call parked under Quorum(2) needs two approvals however the session
// is reopened: a process that opens it without the option (a worker
// whose wiring forgot it, another build) must not resolve it on one.
// The quorum rides the request entry; the stricter of it and the
// session's own holds.
func TestQuorumSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	st, err := jsonl.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var ran atomic.Int32
	agent := func(turns ...wefttest.Turn) *core.Agent {
		return core.New(wefttest.Script(turns...), core.Name("quorum"),
			core.Tool("refund", "Refund.", func(ctx context.Context, in refundInput) (string, error) {
				ran.Add(1)
				return "refunded", nil
			}, core.RequireApproval()))
	}
	s, err := thread.Create(ctx, st, agent(wefttest.ToolCalls(wefttest.Call{Name: "refund", ID: "call_1", Args: `{"order_id":"1"}`})),
		thread.Quorum(2))
	if err != nil {
		t.Fatal(err)
	}
	turn, err := s.Send(ctx, core.User("refund"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := turn.Wait(); err != nil {
		t.Fatal(err)
	}
	abandon(t, st, s.ID())

	s2, err := thread.Open(ctx, st, s.ID(), agent(wefttest.Say("done"))) // no Quorum
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close(ctx) }()
	if p := s2.Pending(); len(p) != 1 || p[0].Quorum != 2 {
		t.Fatalf("pending after reopen = %+v, want one request needing 2", p)
	}
	first := thread.Approve("call_1")
	first.Who = "alice"
	rt, err := s2.Decide(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	if rt != nil || len(s2.Pending()) != 1 {
		t.Fatalf("one approval resolved a call parked under Quorum(2): turn %v, pending %+v", rt, s2.Pending())
	}
	second := thread.Approve("call_1")
	second.Who = "bob"
	if rt, err = s2.Decide(ctx, second); err != nil || rt == nil {
		t.Fatalf("the second approval: turn %v, err %v", rt, err)
	}
	if _, err := rt.Wait(); err != nil {
		t.Fatal(err)
	}
	if n := ran.Load(); n != 1 {
		t.Fatalf("refund ran %d times, want once after two approvals", n)
	}
}
