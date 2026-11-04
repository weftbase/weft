package core_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
)

// Every executed call carries an IdempotencyKey: the run's own by
// default, the IdempotencyKeys option's when it names one, and a
// Subagent child run never inherits the option.
func TestIdempotencyKey(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]string{}
	probe := core.Tool("probe", "Probe.", func(ctx context.Context, _ struct{}) (string, error) {
		c, _ := core.CallFromContext(ctx)
		mu.Lock()
		seen[c.CallID] = c.IdempotencyKey
		mu.Unlock()
		return "ok", nil
	})
	script := func() core.Model {
		return wefttest.Script(
			wefttest.ToolCalls(wefttest.Call{Name: "probe", ID: "c1", Args: `{}`}, wefttest.Call{Name: "probe", ID: "c2", Args: `{}`}),
			wefttest.Say("done"),
		)
	}
	agent := core.New(script(), probe)
	if _, err := agent.Generate(context.Background(), core.Messages(core.User("go")), core.RunID("run-1")); err != nil {
		t.Fatal(err)
	}
	if seen["c1"] != core.DefaultIdempotencyKeyForTest("run-1", "c1") || seen["c1"] == seen["c2"] || len(seen["c1"]) != 32 {
		t.Fatalf("default keys = %v", seen)
	}

	agent = core.New(script(), probe)
	if _, err := agent.Generate(context.Background(), core.Messages(core.User("go")),
		core.IdempotencyKeys(func(c core.Call) string {
			if c.IdempotencyKey != "" {
				t.Errorf("fn saw a key already set: %q", c.IdempotencyKey)
			}
			if c.CallID == "c1" {
				return "mine-" + c.CallID
			}
			return ""
		})); err != nil {
		t.Fatal(err)
	}
	if seen["c1"] != "mine-c1" {
		t.Fatalf("named key = %q", seen["c1"])
	}
	if seen["c2"] == "" || seen["c2"] == "mine-c2" {
		t.Fatalf("an empty name must fall back to the default, got %q", seen["c2"])
	}
}

// BeforeDispatch is a gate: it sees each call about to run, with its
// key, and a refusal stops the call before its handler — the model
// reads an error result wrapping ErrNotDispatched — while its siblings
// run. A call parked at the approval boundary never reaches the gate,
// and a Subagent's child run inherits neither the gate nor the keys.
func TestBeforeDispatchGate(t *testing.T) {
	var mu sync.Mutex
	ran := map[string]bool{}
	gated := map[string]string{}
	tool := func(name string, opts ...core.ToolOption) *core.ToolDef {
		return core.Tool(name, name+".", func(ctx context.Context, _ struct{}) (string, error) {
			mu.Lock()
			ran[name] = true
			mu.Unlock()
			return "ok", nil
		}, opts...)
	}
	child := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "inner", ID: "i1", Args: `{}`}),
		wefttest.Say("child done"),
	), tool("inner"))
	agent := core.New(wefttest.Script(
		wefttest.ToolCalls(
			wefttest.Call{Name: "pay", ID: "c1", Args: `{}`},
			wefttest.Call{Name: "read", ID: "c2", Args: `{}`},
			wefttest.Call{Name: "gated", ID: "c3", Args: `{}`},
			wefttest.Call{Name: "delegate", ID: "c4", Args: `{"prompt":"go"}`},
		),
		wefttest.Say("done"),
	), tool("pay"), tool("read"), tool("gated", core.RequireApproval()), core.Subagent("delegate", "Delegate.", child))
	res, err := agent.Generate(context.Background(), core.Prompt("go"), core.RunID("run-g"),
		core.IdempotencyKeys(func(c core.Call) string { return "k-" + c.CallID }),
		core.BeforeDispatch(func(ctx context.Context, c core.Call, def *core.ToolDef) error {
			mu.Lock()
			gated[c.CallID] = c.IdempotencyKey
			mu.Unlock()
			if def.Name == "pay" {
				return errors.New("step not durable")
			}
			return nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	if ran["pay"] || !ran["read"] || !ran["inner"] {
		t.Fatalf("ran = %v: the refused call must not run, its siblings and the child's must", ran)
	}
	if gated["c1"] != "k-c1" || gated["c2"] != "k-c2" {
		t.Fatalf("gate saw keys %v", gated)
	}
	if _, ok := gated["c3"]; ok {
		t.Fatal("a parked call reached the gate")
	}
	if _, ok := gated["i1"]; ok {
		t.Fatal("a Subagent's child run inherited the gate")
	}
	if len(res.Pending) != 1 || res.Pending[0].ID != "c3" {
		t.Fatalf("pending = %+v", res.Pending)
	}
	var refused *core.ToolResultPart
	for _, m := range res.Messages {
		for _, p := range m.Content {
			if r, ok := p.(core.ToolResultPart); ok && r.CallID == "c1" {
				refused = &r
			}
		}
	}
	if refused == nil || !refused.IsError || !strings.Contains(refused.Content, "step not durable") {
		t.Fatalf("refused call's result = %+v", refused)
	}
}

// WR-7: a handler's CallTool dispatches under the run's park rule. A
// tool the run's ParkOn names returns ErrApprovalRequired to the
// handler and never runs unasked; a tool the rule does not name runs.
func TestCallToolHonoursTheParkRule(t *testing.T) {
	var refunded, looked int
	var inner error
	refund := core.Tool("refund", "Refund.", func(ctx context.Context, _ struct{}) (string, error) {
		refunded++
		return "refunded", nil
	})
	look := core.Tool("look", "Look.", func(ctx context.Context, _ struct{}) (string, error) {
		looked++
		return "seen", nil
	})
	var agent *core.Agent
	outer := core.Tool("outer", "Dispatch by hand.", func(ctx context.Context, _ struct{}) (string, error) {
		_, inner = agent.CallTool(ctx, core.ToolCallPart{ID: "in1", Name: "refund", Args: []byte(`{}`)})
		_, _ = agent.CallTool(ctx, core.ToolCallPart{ID: "in2", Name: "look", Args: []byte(`{}`)})
		return "done", nil
	})
	agent = core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "outer", ID: "c1", Args: `{}`}),
		wefttest.Say("ok"),
	), refund, look, outer)
	if _, err := agent.Generate(context.Background(), core.Prompt("go"), core.ParkOn("refund")); err != nil {
		t.Fatal(err)
	}
	if refunded != 0 || !errors.Is(inner, core.ErrApprovalRequired) {
		t.Fatalf("refunded %d times, inner err %v: want 0 and ErrApprovalRequired", refunded, inner)
	}
	if looked != 1 {
		t.Fatalf("looked %d times, want 1", looked)
	}
}

// WR-6: a call with an effect whose handler outlives its Timeout has
// an unknown outcome: the result says so (OUTCOME_UNKNOWN), never
// "timed out" as if it had not happened — the abandoned handler may
// still complete. A ReplaySafe tool keeps the timeout text. Under
// ParkUnknown the risky call parks instead, the others' results kept.
func TestTimeoutIsAnUnknownOutcome(t *testing.T) {
	var done atomic.Int32
	slow := func(name string, opts ...core.ToolOption) *core.ToolDef {
		return core.Tool(name, "Slow.", func(ctx context.Context, _ struct{}) (string, error) {
			time.Sleep(60 * time.Millisecond) // ignores ctx: the effect lands after the deadline
			done.Add(1)
			return "ok", nil
		}, append(opts, core.Timeout(10*time.Millisecond))...)
	}
	script := func() core.Model {
		return wefttest.Script(
			wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "c1", Args: `{}`}, wefttest.Call{Name: "read", ID: "c2", Args: `{}`}),
			wefttest.Say("next"),
		)
	}
	agent := core.New(script(), slow("pay"), slow("read", core.Replay(core.ReplaySafe)))
	res, err := agent.Generate(context.Background(), core.Prompt("go"))
	if err != nil {
		t.Fatal(err)
	}
	if r := res.Steps[0].Results[0]; !r.IsError || !strings.HasPrefix(r.Content, core.CodeOutcomeUnknown+`: tool "pay" did not answer within 10ms`) {
		t.Fatalf("risky result = %+v", r)
	}
	if r := res.Steps[0].Results[1]; !strings.Contains(r.Content, "timed out after 10ms") {
		t.Fatalf("safe result = %+v", r)
	}
	res, err = core.New(script(), slow("pay"), slow("read", core.Replay(core.ReplaySafe))).Generate(context.Background(), core.Prompt("go"), core.ParkUnknown())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Pending) != 1 || res.Pending[0].ID != "c1" {
		t.Fatalf("under ParkUnknown: pending %+v", res.Pending)
	}
	time.Sleep(100 * time.Millisecond)
	if done.Load() != 4 {
		t.Fatalf("handlers completed %d times, want 4 (each one, late)", done.Load())
	}
}
