package core_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
)

// P1-5: a call a handler dispatches with CallTool is a call with
// effects like the loop's own: it carries a deterministic key derived
// from the outer call's key and its position — the same on a retry of
// the outer call — and passes through the run's dispatch gate, which
// sees the outer call's id. A refusal stops it.
func TestCallToolInnerCallsAreKeyedAndGated(t *testing.T) {
	var mu sync.Mutex
	keys := map[string][]string{}
	gated := map[string]string{}
	refund := core.Tool("refund", "Refund.", func(ctx context.Context, _ struct{}) (string, error) {
		c, _ := core.CallFromContext(ctx)
		mu.Lock()
		keys[c.CallID] = append(keys[c.CallID], c.IdempotencyKey)
		mu.Unlock()
		return "refunded", nil
	})
	var agent *core.Agent
	var refused error
	outer := core.Tool("outer", "Dispatch by hand.", func(ctx context.Context, _ struct{}) (string, error) {
		_, _ = agent.CallTool(ctx, core.ToolCallPart{ID: "in1", Name: "refund", Args: []byte(`{}`)})
		_, _ = agent.CallTool(ctx, core.ToolCallPart{ID: "in2", Name: "refund", Args: []byte(`{}`)})
		_, refused = agent.CallTool(ctx, core.ToolCallPart{ID: "in3", Name: "refund", Args: []byte(`{}`)})
		return "done", nil
	})
	run := func() {
		agent = core.New(wefttest.Script(
			wefttest.ToolCalls(wefttest.Call{Name: "outer", ID: "c1", Args: `{}`}),
			wefttest.Say("ok"),
		), refund, outer)
		if _, err := agent.Generate(context.Background(), core.Prompt("go"),
			core.IdempotencyKeys(func(c core.Call) string { return "outer-key" }),
			core.BeforeDispatch(func(_ context.Context, c core.Call, _ *core.ToolDef) error {
				mu.Lock()
				defer mu.Unlock()
				gated[c.CallID] = c.Outer
				if c.CallID == "in3" {
					return errors.New("no")
				}
				return nil
			})); err != nil {
			t.Fatal(err)
		}
	}
	run()
	run() // the outer call again, under the same key
	mu.Lock()
	defer mu.Unlock()
	k1, k2 := keys["in1"], keys["in2"]
	if len(k1) != 2 || len(k2) != 2 || k1[0] == "" || k1[0] != k1[1] || k2[0] != k2[1] || k1[0] == k2[0] || k1[0] == "outer-key" {
		t.Fatalf("inner keys = %v, %v: want deterministic, distinct, derived", k1, k2)
	}
	if gated["in1"] != "c1" || gated["c1"] != "" {
		t.Fatalf("gate saw %v: want the inner calls with their outer call's id", gated)
	}
	if len(keys["in3"]) != 0 || !errors.Is(refused, core.ErrNotDispatched) {
		t.Fatalf("the refused inner call ran %d times, err %v", len(keys["in3"]), refused)
	}
}

// An inner call whose handler is cut off before it answers has an
// unknown outcome: CallTool's error wraps ErrOutcomeUnknown, so an
// outer handler that returns it makes the outer call unknown too.
func TestCallToolCutOffIsUnknown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	charge := core.Tool("charge", "Charge.", func(ctx context.Context, _ struct{}) (string, error) {
		cancel()
		<-ctx.Done()
		return "", ctx.Err()
	})
	var agent *core.Agent
	var inner error
	outer := core.Tool("outer", "Dispatch by hand.", func(ctx context.Context, _ struct{}) (string, error) {
		_, inner = agent.CallTool(ctx, core.ToolCallPart{ID: "in1", Name: "charge", Args: []byte(`{}`)})
		return "", inner
	})
	agent = core.New(wefttest.Script(wefttest.ToolCalls(wefttest.Call{Name: "outer", ID: "c1", Args: `{}`})), charge, outer)
	_, _ = agent.Generate(ctx, core.Prompt("go"))
	if !errors.Is(inner, core.ErrOutcomeUnknown) {
		t.Fatalf("inner err = %v, want ErrOutcomeUnknown", inner)
	}
}

// A Subagent cannot be ReplayKeyed: its child run's calls carry
// run-scoped keys, so a retry of the delegation is not deduplicated.
func TestSubagentRefusesReplayKeyed(t *testing.T) {
	defer func() {
		p := recover()
		if p == nil || !strings.Contains(p.(string), "ReplayKeyed") {
			t.Fatalf("Subagent with ReplayKeyed did not panic naming it: %v", p)
		}
	}()
	core.Subagent("d", "Delegate.", core.New(wefttest.Script()), core.Replay(core.ReplayKeyed))
}

// Review P2: a handler that panics may have taken effect before it
// did: for a tool that is not ReplaySafe the call's outcome is unknown,
// never a plain failure (and parks under ParkUnknown). A ReplaySafe
// tool keeps the panic text.
func TestHandlerPanicIsAnUnknownOutcome(t *testing.T) {
	boom := func(name string, opts ...core.ToolOption) *core.ToolDef {
		return core.Tool(name, "Boom.", func(context.Context, struct{}) (string, error) {
			panic("half way")
		}, opts...)
	}
	agent := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "c1", Args: `{}`}, wefttest.Call{Name: "read", ID: "c2", Args: `{}`}),
		wefttest.Say("ok"),
	), boom("pay"), boom("read", core.Replay(core.ReplaySafe)))
	res, err := agent.Generate(context.Background(), core.Prompt("go"))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, r := range res.Steps[0].Results {
		got[r.CallID] = r.Content
	}
	if want := core.CodeOutcomeUnknown + `: tool "pay" panicked before it answered (half way); it may have run; outcome unknown`; got["c1"] != want {
		t.Errorf("pay = %q, want %q", got["c1"], want)
	}
	if !strings.Contains(got["c2"], "panicked") || strings.Contains(got["c2"], core.CodeOutcomeUnknown) {
		t.Errorf("read = %q, want the plain panic text", got["c2"])
	}
}
