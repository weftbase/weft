package pool_test

import (
	"context"
	"errors"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/thread/jsonl"
	"github.com/weftbase/weft/thread/pool"
	"github.com/weftbase/weft/wefttest"
)

// A pool child resumes after a restart by its wrap name alone (the
// child header's pool_agent). Whatever agent the restarted process
// wraps under that name, the parked call never runs without its
// decision: with no agent the delegation cannot resume and says so
// (ErrNoAgent) while the call stays parked; with an agent whose tool
// changed its input since the park, the approved call is denied, not
// run; with an agent whose tool merely dropped its approval mark, the
// approved call runs once and a denied one never.
func TestPoolChildRestartUnderAnotherAgent(t *testing.T) {
	type amountInput struct {
		OrderID string
		Amount  int `json:",omitempty"`
	}
	for _, tc := range []struct {
		name    string
		child   func(ran *flags) *core.Agent // nil: nothing wrapped under the name
		approve bool
		want    []bool // the approved flags of the runs the refund made
	}{
		{name: "missing", approve: true},
		{name: "schema changed", approve: true, child: func(ran *flags) *core.Agent {
			return core.New(wefttest.Script(wefttest.Say("done")), core.Tool("refund", "",
				func(ctx context.Context, in amountInput) (string, error) {
					c, _ := core.CallFromContext(ctx)
					ran.add(c.Approved)
					return "refunded", nil
				}, core.RequireApproval()))
		}},
		{name: "mark dropped, approved", approve: true, want: []bool{true}, child: func(ran *flags) *core.Agent {
			return core.New(wefttest.Script(wefttest.Say("done")), core.Tool("refund", "",
				func(ctx context.Context, in struct{ OrderID string }) (string, error) {
					c, _ := core.CallFromContext(ctx)
					ran.add(c.Approved)
					return "refunded", nil
				}))
		}},
		{name: "mark dropped, denied", child: func(ran *flags) *core.Agent {
			return core.New(wefttest.Script(wefttest.Say("done")), core.Tool("refund", "",
				func(ctx context.Context, in struct{ OrderID string }) (string, error) {
					c, _ := core.CallFromContext(ctx)
					ran.add(c.Approved)
					return "refunded", nil
				}))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, err := jsonl.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			p := pool.New(2)
			child, ran := gatedChild(wefttest.ToolCalls(wefttest.Call{Name: "refund", Args: `{"OrderID":"9"}`}))
			s, err := thread.Create(ctx, st, nestedParent(p, child))
			if err != nil {
				t.Fatal(err)
			}
			t1, err := s.Send(ctx, core.User("refund it"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := t1.Wait(); err != nil {
				t.Fatal(err)
			}
			if err := p.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(ctx); err != nil {
				t.Fatal(err)
			}
			restart(t, st)

			p2 := pool.New(2)
			ran2 := &flags{}
			var tools []core.Option
			if tc.child != nil {
				tools = append(tools, p2.MustWrap("research", "delegates the refund flow", tc.child(ran2)))
			}
			open, err := thread.Open(ctx, st, s.ID(), core.New(wefttest.Script(wefttest.Say("all done")), tools...))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = p2.Close(ctx); _ = open.Close(ctx) }()
			pend := open.Pending()
			if len(pend) != 1 || pend[0].Tool != "refund" {
				t.Fatalf("Pending after restart = %+v", pend)
			}
			d := thread.Deny(pend[0].CallID, "no")
			if tc.approve {
				d = thread.Approve(pend[0].CallID)
			}
			err = p2.Decide(ctx, open, bound(open, d))
			if tc.child == nil {
				if !errors.Is(err, pool.ErrNoAgent) {
					t.Fatalf("Decide with no agent wrapped: err = %v, want ErrNoAgent", err)
				}
			} else {
				if err != nil {
					t.Fatalf("Decide: %v", err)
				}
				if rc, err := p2.Wait(ctx, open, pool.Receipts(open)[0].ID); err != nil || !rc.Settled() {
					t.Fatalf("child resume: %+v, %v", rc, err)
				}
			}
			if got := ran.snapshot(); len(got) != 0 {
				t.Errorf("the pre-restart agent ran: %v", got)
			}
			if got := ran2.snapshot(); len(got) != len(tc.want) || (len(got) == 1 && !got[0]) {
				t.Errorf("refund runs after the restart = %v, want %v", got, tc.want)
			}
		})
	}
}
