package pool_test

import (
	"context"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/thread/pool"
)

// The turn-run rule that refuses a Subagent child's delegation
// (ErrNestedDelegation) lets every run the session itself starts
// delegate: the resume run of a decided boundary, and a forked
// session's turn.
func TestSessionRunsStillDelegate(t *testing.T) {
	ctx := context.Background()
	t.Run("resume run", func(t *testing.T) {
		p := pool.New(2)
		defer func() { _ = p.Close(ctx) }()
		confirm := core.Tool("confirm", "", func(context.Context, struct{}) (string, error) { return "ok", nil },
			core.RequireApproval())
		lead := core.New(wefttest.Script(
			wefttest.ToolCalls(wefttest.Call{Name: "confirm", ID: "c1", Args: `{}`}),
			wefttest.ToolCalls(wefttest.Call{Name: "research", ID: "r1", Args: wefttest.Args(struct{ Prompt string }{"go"})}),
			wefttest.Say("lead done"),
		), confirm, p.MustWrap("research", "", core.New(wefttest.Script(wefttest.Say("leaf done")))))
		s, err := thread.Create(ctx, thread.Memory(), lead)
		if err != nil {
			t.Fatal(err)
		}
		t1, err := s.Send(ctx, core.User("go"))
		if err != nil {
			t.Fatal(err)
		}
		if res, err := t1.Wait(); err != nil || len(res.Pending) != 1 {
			t.Fatalf("first turn: %v, %+v", err, res)
		}
		if _, err := s.Decide(ctx, bound(s, thread.Approve("c1"))); err != nil {
			t.Fatal(err)
		}
		n := t1.Next()
		if n == nil {
			t.Fatal("no resume armed")
		}
		if res, err := n.Wait(); err != nil || res.Text() != "lead done" {
			t.Fatalf("resume: %q, %v", res.Text(), err)
		}
		if got := toolResultsOf(s.Context()); len(got) != 2 || got[1] != "leaf done" {
			t.Errorf("results = %q, want the resume run's delegation answered", got)
		}
	})
	t.Run("forked session", func(t *testing.T) {
		p := pool.New(2)
		defer func() { _ = p.Close(ctx) }()
		lead := core.New(wefttest.Script(
			wefttest.Say("hello"),
			wefttest.ToolCalls(wefttest.Call{Name: "research", ID: "r1", Args: wefttest.Args(struct{ Prompt string }{"go"})}),
			wefttest.Say("lead done"),
		), p.MustWrap("research", "", core.New(wefttest.Script(wefttest.Say("leaf done")))))
		s, err := thread.Create(ctx, thread.Memory(), lead)
		if err != nil {
			t.Fatal(err)
		}
		t1, err := s.Send(ctx, core.User("hi"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := t1.Wait(); err != nil {
			t.Fatal(err)
		}
		f, err := s.Fork(ctx, s.Leaf())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = f.Close(ctx) }()
		t2, err := f.Send(ctx, core.User("research"))
		if err != nil {
			t.Fatal(err)
		}
		if res, err := t2.Wait(); err != nil || res.Text() != "lead done" {
			t.Fatalf("fork turn: %q, %v", res.Text(), err)
		}
		if rs := pool.Receipts(f); len(rs) != 1 || rs[0].State != pool.Done {
			t.Errorf("fork receipts = %+v, want one delegation done", rs)
		}
	})
}
