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

// A park rule the parked send ran under keeps parking after a restart:
// a ParkAllExcept or ParkOn run that parks a plain tool, reopened from
// disk and approved, must park the next call to it again instead of
// running it unasked. The rule rides the request entries.
func TestParkRuleSurvivesRestart(t *testing.T) {
	for name, rule := range map[string]core.RunOption{
		"park_all_except": core.ParkAllExcept(),
		"park_on":         core.ParkOn("ship"),
	} {
		t.Run(name, func(t *testing.T) { parkRuleSurvivesRestart(t, rule) })
	}
}

func parkRuleSurvivesRestart(t *testing.T, rule core.RunOption) {
	ctx := context.Background()
	st, err := jsonl.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var unasked atomic.Int32
	shipAgent := func(turns ...wefttest.Turn) *core.Agent {
		ship := core.Tool("ship", "Ship an order.",
			func(ctx context.Context, in refundInput) (string, error) {
				if c, _ := core.CallFromContext(ctx); !c.Approved {
					unasked.Add(1)
				}
				return "shipped " + in.OrderID, nil
			})
		return core.New(wefttest.Script(turns...), core.Name("park-restart"), ship)
	}

	s, err := thread.Create(ctx, st, shipAgent(wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "call_1", Args: `{"order_id":"1"}`})))
	if err != nil {
		t.Fatal(err)
	}
	turn, err := s.Send(ctx, core.User("ship it"), thread.RunOptions(rule))
	if err != nil {
		t.Fatal(err)
	}
	res, err := turn.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Pending) != 1 {
		t.Fatalf("pending before restart: %+v", res.Pending)
	}
	id := s.ID()
	if p := s.Pending(); len(p) != 1 {
		t.Fatalf("pending: %+v", p)
	}
	abandon(t, st, id)

	s2, err := thread.Open(ctx, st, id, shipAgent(
		wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "call_2", Args: `{"order_id":"2"}`}),
		wefttest.Say("done"),
	))
	if err != nil {
		t.Fatal(err)
	}
	rt, err := s2.Decide(ctx, bound(s2, thread.Approve("call_1")))
	if err != nil {
		t.Fatal(err)
	}
	rres, err := rt.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if n := unasked.Load(); n != 0 {
		t.Fatalf("%d call(s) ran unasked after a restart under the park rule", n)
	}
	if len(rres.Pending) != 1 || rres.Pending[0].ID != "call_2" {
		t.Fatalf("call_2 should park again; pending: %+v", rres.Pending)
	}
}
