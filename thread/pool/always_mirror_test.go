package pool_test

import (
	"context"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/thread/pool"
)

// ApproveAlways on a pool child's mirrored request mints no grant in
// the parent (review finding A1): the grant there bound the parent's
// own tool of the same name — another system — whose next call then
// ran unasked. The standing approval is the child's: the decision's
// Always wish replays into the child, which mints its own grant.
func TestApproveAlwaysOnAMirrorMintsNoParentGrant(t *testing.T) {
	ctx := context.Background()
	p := pool.New(2)
	defer func() { c, cc := context.WithTimeout(ctx, time.Second); defer cc(); _ = p.Close(c) }()
	child, _ := gatedChild(
		wefttest.ToolCalls(wefttest.Call{Name: "refund", ID: "c1", Args: `{"order_id":"1"}`}),
		wefttest.Say("child refunded"),
	)
	parentRan := 0
	parentRefund := core.Tool("refund", "The parent's own refund, another system.", func(_ context.Context, in struct {
		OrderID string `json:"order_id"`
	}) (string, error) {
		parentRan++
		return "parent refunded " + in.OrderID, nil
	}, core.RequireApproval())
	parent := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "research", ID: "c-wrapper", Args: `{"prompt":"refund"}`}),
		wefttest.Say("delegated"),
		wefttest.ToolCalls(wefttest.Call{Name: "refund", ID: "p1", Args: `{"order_id":"1"}`}),
		wefttest.Say("parent done"),
	), p.MustWrap("research", "Delegates.", child), parentRefund)
	s, err := thread.Create(ctx, thread.Memory(), parent)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(ctx) }()
	t1, rc := park(t, s)
	pend := s.Pending()
	if len(pend) != 1 || pend[0].Child != rc.Child {
		t.Fatalf("Pending = %+v, want the child's request mirrored", pend)
	}
	if err := p.Decide(ctx, s, bound(s, thread.ApproveAlways(pend[0].CallID))); err != nil {
		t.Fatal(err)
	}
	waitFor(t, p, s, rc.ID, pool.Done)
	if n := continuation(t, t1); n != nil {
		if _, err := n.Wait(); err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range s.Entries() {
		if g, ok := e.(thread.GrantEntry); ok {
			t.Fatalf("the parent holds a grant from the child's ApproveAlways: %+v", g)
		}
	}
	t2, err := s.Send(ctx, core.User("now refund in our system"))
	if err != nil {
		t.Fatal(err)
	}
	res, err := t2.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if parentRan != 0 || len(res.Pending) != 1 || res.Pending[0].ID != "p1" {
		t.Fatalf("the parent's own refund ran %d times, pending %+v: want it parked for a person", parentRan, res.Pending)
	}
	cs, err := thread.Open(ctx, s.Storage(), rc.Child, child)
	if err != nil {
		t.Fatal(err)
	}
	granted := false
	for _, e := range cs.Entries() {
		if g, ok := e.(thread.GrantEntry); ok && g.Tool == "refund" {
			granted = true
		}
	}
	if !granted {
		t.Fatal("the child session holds no grant: the Always wish did not reach it")
	}
}
