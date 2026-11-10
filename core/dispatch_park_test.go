package core_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
)

// A BeforeDispatch gate that answers with an error wrapping
// ErrApprovalRequired parks the call (ADR 0031, the 2026-11-10
// amendment) instead of failing it with ErrNotDispatched: the handler
// never runs, the run ends at the approval boundary with the call
// pending, its siblings run, and a resume that approves it passes the
// gate again — which may park it again. weft/thread's still-running
// gate is the user.
func TestBeforeDispatchParkParksTheCall(t *testing.T) {
	var ran, sibling atomic.Int32
	var hold atomic.Bool
	hold.Store(true)
	ship := core.Tool("ship", "Ship.", func(context.Context, struct{}) (string, error) {
		ran.Add(1)
		return "shipped", nil
	})
	look := core.Tool("look", "Look.", func(context.Context, struct{}) (string, error) {
		sibling.Add(1)
		return "seen", nil
	}, core.Replay(core.ReplaySafe))
	gate := core.BeforeDispatch(func(_ context.Context, c core.Call, def *core.ToolDef) error {
		if def.Name == "ship" && hold.Load() {
			return fmt.Errorf("%w: an earlier call is still running", core.ErrApprovalRequired)
		}
		return nil
	})
	agt := core.New(wefttest.Script(
		wefttest.ToolCalls(
			wefttest.Call{Name: "ship", ID: "c1", Args: `{}`},
			wefttest.Call{Name: "look", ID: "c2", Args: `{}`},
		),
		wefttest.Say("done"),
	), ship, look)
	res, err := agt.Generate(context.Background(), core.Prompt("go"), gate)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Pending) != 1 || res.Pending[0].ID != "c1" || ran.Load() != 0 || sibling.Load() != 1 {
		t.Fatalf("pending %+v, ship ran %d, look ran %d: want c1 parked unrun, its sibling run", res.Pending, ran.Load(), sibling.Load())
	}
	// Approved while the gate still holds it: parked again, not run.
	res, err = agt.Generate(context.Background(), core.Messages(res.Messages...), core.Approve("c1"), gate)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Pending) != 1 || ran.Load() != 0 {
		t.Fatalf("approved under a holding gate: pending %+v, ship ran %d", res.Pending, ran.Load())
	}
	hold.Store(false)
	res, err = agt.Generate(context.Background(), core.Messages(res.Messages...), core.Approve("c1"), gate)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Pending) != 0 || ran.Load() != 1 || res.Text() != "done" {
		t.Fatalf("approved once the gate lifted: pending %+v, ship ran %d, text %q", res.Pending, ran.Load(), res.Text())
	}
	if errors.Is(err, core.ErrNotDispatched) {
		t.Fatal("unreachable")
	}
}
