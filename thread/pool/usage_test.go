package pool_test

import (
	"context"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/thread/pool"
)

// Usage.Delegated at the top of a two-level chain covers the
// grandchild too (AGENTS.md rule 13: a budget that must cover delegated
// work reads Delegated): the child's settlement bills its turns and its
// own Delegated, each level counted once.
func TestDelegatedCoversGrandchildren(t *testing.T) {
	ctx := context.Background()
	st := thread.Memory()
	p := pool.New(2)
	defer func() { _ = p.Close(ctx) }()
	grand := core.New(wefttest.Script(wefttest.Say("g done")))
	child := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "g", ID: "g1", Args: wefttest.Args(struct{ Prompt string }{"go"})}),
		wefttest.Say("c done"),
	), p.MustWrap("g", "g", grand))
	lead := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "c", ID: "c1", Args: wefttest.Args(struct{ Prompt string }{"go"})}),
		wefttest.Say("lead done"),
	), p.MustWrap("c", "c", child))
	s, err := thread.Create(ctx, st, lead)
	if err != nil {
		t.Fatal(err)
	}
	turn, err := s.Send(ctx, core.User("go"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := turn.Wait(); err != nil {
		t.Fatal(err)
	}
	rs := pool.Receipts(s)
	cs, err := thread.Open(ctx, st, rs[0].Child, child)
	if err != nil {
		t.Fatal(err)
	}
	cu := cs.Usage()
	if cu.Delegated == (core.Usage{}) {
		t.Fatal("the child's Delegated is empty: no grandchild was billed")
	}
	if want := cu.Turns.Add(cu.Summaries).Add(cu.Delegated); s.Usage().Delegated != want {
		t.Errorf("top Delegated = %+v, want the child's turns plus the grandchild's %+v = %+v",
			s.Usage().Delegated, cu.Delegated, want)
	}
}
