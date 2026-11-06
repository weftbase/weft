package thread_test

import (
	"context"
	"errors"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
	"github.com/weftbase/weft/thread"
)

// ResolveDelegation resolves a delegating call only: an ordinary parked
// call, which delegates to nobody, fails with ErrNotPending whatever
// child it is handed — the empty one included — and nothing is
// recorded. Resolving it would bypass the approval (and RequireSigned
// and the request expiry) with a result recorded Via "child".
func TestResolveDelegationRefusesAnOrdinaryCall(t *testing.T) {
	ctx := context.Background()
	ran := 0
	send := core.Tool("send_email", "", func(context.Context, struct{}) (string, error) { ran++; return "sent", nil },
		core.RequireApproval())
	agt := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "send_email", ID: "c1", Args: `{}`}),
		wefttest.Say("done"),
	), send)
	s, err := thread.Create(ctx, thread.Memory(), agt)
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
	before := len(s.Entries())
	for _, child := range []string{"", "s_nobody"} {
		turn, err := s.ResolveDelegation(ctx, "c1", child, "forged result", false)
		if !errors.Is(err, thread.ErrNotPending) || turn != nil {
			t.Errorf("ResolveDelegation(c1, %q) = turn %v, %v; want ErrNotPending", child, turn != nil, err)
		}
	}
	if n := len(s.Entries()); n != before {
		t.Errorf("%d entries written by refused resolutions", n-before)
	}
	if p := s.Pending(); len(p) != 1 || p[0].CallID != "c1" {
		t.Errorf("pending = %+v, want the call still awaiting its approval", p)
	}
	if ran != 0 {
		t.Errorf("send_email ran %d times", ran)
	}
}
