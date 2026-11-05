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

// What a tool call's context carries for its own handler is that run's
// alone: a run started on it — here a Subagent's child — starts from
// none of it. These pin each piece (C1, C3, C8, ADR 0031 §4).

// C1: ParkUnknown is the parent run's; the child's unknown-outcome call
// answers OUTCOME_UNKNOWN inside the child, it does not park the child.
func TestParkUnknownIsNotInheritedByAChildRun(t *testing.T) {
	pay := core.Tool("pay", "Pay.", func(ctx context.Context, _ struct{}) (string, error) {
		<-ctx.Done()
		time.Sleep(20 * time.Millisecond) // answers after its Timeout
		return "paid", nil
	}, core.Timeout(5*time.Millisecond))
	child := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "k1", Args: `{}`}),
		wefttest.Say("child done"),
	), core.Name("child"), pay)
	parent := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "delegate", ID: "c1", Args: `{"prompt":"pay"}`}),
		wefttest.Say("parent done"),
	), core.Subagent("delegate", "Delegate.", child))
	res, err := parent.Generate(context.Background(), core.Prompt("go"), core.ParkUnknown())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Pending) != 0 {
		t.Fatalf("parent pending = %+v, want none", res.Pending)
	}
	if r := res.Steps[0].Results[0].Content; strings.Contains(r, core.CodeSubagentPending) || r != "child done" {
		t.Errorf("delegate result = %q, want the child's answer (its unknown call answered in the child, not parked)", r)
	}
}

// C3: a child's WrapTools middleware that dispatches with CallTool never
// reaches the parent's gate — not even when the child's call reuses the
// parent's call id (Gemini's call_0).
func TestChildMiddlewareCallToolNeverReachesTheParentsGate(t *testing.T) {
	var mu sync.Mutex
	var gated []string
	audit := core.Tool("audit", "Audit.", func(context.Context, struct{}) (string, error) { return "ok", nil })
	pay := core.Tool("pay", "Pay.", func(context.Context, struct{}) (string, error) { return "paid", nil })
	var child *core.Agent
	pre := func(next core.ToolCaller) core.ToolCaller {
		return func(ctx context.Context, call core.ToolCallPart) (string, error) {
			if call.Name == "pay" {
				if _, err := child.CallTool(ctx, core.ToolCallPart{ID: "a1", Name: "audit", Args: []byte(`{}`)}); err != nil {
					return "", err
				}
			}
			return next(ctx, call)
		}
	}
	child = core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "call_0", Args: `{}`}),
		wefttest.Say("child done"),
	), core.Name("child"), pay, audit, core.WrapTools(pre))
	parent := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "delegate", ID: "call_0", Args: `{"prompt":"pay"}`}),
		wefttest.Say("parent done"),
	), core.Subagent("delegate", "Delegate.", child))
	res, err := parent.Generate(context.Background(), core.Prompt("go"),
		core.BeforeDispatch(func(_ context.Context, c core.Call, _ *core.ToolDef) error {
			mu.Lock()
			gated = append(gated, c.RunID+"|"+c.CallID)
			mu.Unlock()
			return nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	if len(gated) != 1 || !strings.HasSuffix(gated[0], "|call_0") {
		t.Errorf("parent gate saw %v, want its own delegating call alone", gated)
	}
	if r := res.Steps[0].Results[0].Content; r != "child done" {
		t.Errorf("delegate result = %q, want the child's answer", r)
	}
}

// C8: the parent's ParkOn is the parent's: a child handler's CallTool
// of a tool the parent named runs, as the child loop's own call does.
func TestParkOnIsNotInheritedByAChildRunsCallTool(t *testing.T) {
	var auditRan atomic.Int32
	audit := core.Tool("audit", "Audit.", func(context.Context, struct{}) (string, error) { auditRan.Add(1); return "ok", nil })
	var child *core.Agent
	var callErr error
	pay := core.Tool("pay", "Pay.", func(ctx context.Context, _ struct{}) (string, error) {
		_, callErr = child.CallTool(ctx, core.ToolCallPart{ID: "a1", Name: "audit", Args: []byte(`{}`)})
		return "paid", nil
	})
	child = core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "k1", Args: `{}`}),
		wefttest.Say("child done"),
	), core.Name("child"), pay, audit)
	parent := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "delegate", ID: "c1", Args: `{"prompt":"pay"}`}),
		wefttest.Say("parent done"),
	), core.Subagent("delegate", "Delegate.", child),
		core.Tool("audit", "Audit.", func(context.Context, struct{}) (string, error) { return "", nil }))
	if _, err := parent.Generate(context.Background(), core.Prompt("go"), core.ParkOn("audit")); err != nil {
		t.Fatal(err)
	}
	if callErr != nil || auditRan.Load() != 1 {
		t.Errorf("child handler's CallTool(audit) = %v, ran %d; want it run under the child's own (empty) park rule", callErr, auditRan.Load())
	}
}

// C3: a CallTool from a WrapTools middleware, before the call reaches
// its handler: under a dispatch gate it is refused (ErrNotDispatched,
// nothing runs) — inner calls start only under a started outer call;
// with no gate it runs keyed, never with an empty key, and its key is
// not the key of the handler's first CallTool.
func TestCallToolFromAMiddleware(t *testing.T) {
	setup := func(innerKeys *[]string, mu *sync.Mutex, mwErr *error) *core.Agent {
		audit := core.Tool("audit", "Audit.", func(ctx context.Context, _ struct{}) (string, error) {
			c, _ := core.CallFromContext(ctx)
			mu.Lock()
			*innerKeys = append(*innerKeys, c.IdempotencyKey)
			mu.Unlock()
			return "ok", nil
		}, core.Replay(core.ReplayKeyed))
		var agent *core.Agent
		pre := func(next core.ToolCaller) core.ToolCaller {
			return func(ctx context.Context, call core.ToolCallPart) (string, error) {
				if call.Name == "pay" {
					if _, err := agent.CallTool(ctx, core.ToolCallPart{ID: "a1", Name: "audit", Args: []byte(`{}`)}); err != nil {
						*mwErr = err
						return "", err
					}
				}
				return next(ctx, call)
			}
		}
		pay := core.Tool("pay", "Pay.", func(ctx context.Context, _ struct{}) (string, error) {
			_, err := agent.CallTool(ctx, core.ToolCallPart{ID: "a2", Name: "audit", Args: []byte(`{}`)})
			return "paid", err
		})
		agent = core.New(wefttest.Script(
			wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "c1", Args: `{}`}),
			wefttest.Say("done"),
		), pay, audit, core.WrapTools(pre))
		return agent
	}

	t.Run("gated", func(t *testing.T) {
		var mu sync.Mutex
		var keys []string
		var mwErr error
		agent := setup(&keys, &mu, &mwErr)
		var gated []string
		res, err := agent.Generate(context.Background(), core.Prompt("go"),
			core.BeforeDispatch(func(_ context.Context, c core.Call, _ *core.ToolDef) error {
				mu.Lock()
				gated = append(gated, c.CallID)
				mu.Unlock()
				return nil
			}))
		if err != nil {
			t.Fatal(err)
		}
		if !errors.Is(mwErr, core.ErrNotDispatched) || len(keys) != 0 || len(gated) != 0 {
			t.Fatalf("middleware CallTool = %v, inner ran %d, gate saw %v; want ErrNotDispatched, nothing run or gated", mwErr, len(keys), gated)
		}
		want := `weft: tool call not dispatched: tool "audit" was dispatched from a tool middleware of call "c1", before that call's handler started; under a dispatch gate an inner call is dispatched only from inside the handler`
		if got := res.Steps[0].Results[0].Content; got != want {
			t.Errorf("result = %q\nwant %q", got, want)
		}
	})

	t.Run("ungated", func(t *testing.T) {
		var mu sync.Mutex
		var keys []string
		var mwErr error
		agent := setup(&keys, &mu, &mwErr)
		if _, err := agent.Generate(context.Background(), core.Prompt("go")); err != nil {
			t.Fatal(err)
		}
		if mwErr != nil || len(keys) != 2 {
			t.Fatalf("middleware CallTool = %v, inner keys %v; want both inner calls run", mwErr, keys)
		}
		if keys[0] == "" || keys[1] == "" || keys[0] == keys[1] {
			t.Errorf("inner keys = %q; want two distinct non-empty keys (middleware's and handler's first CallTool)", keys)
		}
	})
}

// C11: a handler's CallTool resolves against the run's OnlyTools
// narrowing, as the loop's own calls do: a narrowed-away tool is
// unknown (ErrNoSuchTool), and a tool the run kept runs.
func TestCallToolHonoursOnlyTools(t *testing.T) {
	var refunds atomic.Int32
	refund := core.Tool("refund", "Refund.", func(context.Context, struct{}) (string, error) { refunds.Add(1); return "refunded", nil })
	lookup := core.Tool("lookup", "Look up.", func(context.Context, struct{}) (string, error) { return "found", nil })
	var agent *core.Agent
	var errs []error
	pay := core.Tool("pay", "Pay.", func(ctx context.Context, _ struct{}) (string, error) {
		_, e1 := agent.CallTool(ctx, core.ToolCallPart{ID: "i1", Name: "refund", Args: []byte(`{}`)})
		_, e2 := agent.CallTool(ctx, core.ToolCallPart{ID: "i2", Name: "lookup", Args: []byte(`{}`)})
		errs = []error{e1, e2}
		return "paid", nil
	})
	agent = core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "c1", Args: `{}`}),
		wefttest.Say("done"),
	), pay, refund, lookup)
	if _, err := agent.Generate(context.Background(), core.Prompt("go"), core.OnlyTools("pay", "lookup")); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(errs[0], core.ErrNoSuchTool) || refunds.Load() != 0 {
		t.Errorf("CallTool(refund) narrowed away = %v, ran %d; want ErrNoSuchTool, never run", errs[0], refunds.Load())
	}
	if errs[1] != nil {
		t.Errorf("CallTool(lookup) kept by OnlyTools = %v, want it run", errs[1])
	}
	// Outside a run nothing narrows it.
	if _, err := agent.CallTool(context.Background(), core.ToolCallPart{ID: "x", Name: "refund", Args: []byte(`{}`)}); err != nil || refunds.Load() != 1 {
		t.Errorf("CallTool(refund) outside a run = %v (ran %d), want it run", err, refunds.Load())
	}
}

// C12: a panicking IdempotencyKeys function is contained: the call it
// could not key fails closed — ErrNotDispatched, its handler never
// started — its siblings and the run go on (rule 1).
func TestIdempotencyKeysPanicFailsTheCallClosed(t *testing.T) {
	var paid, looked atomic.Int32
	pay := core.Tool("pay", "Pay.", func(context.Context, struct{}) (string, error) { paid.Add(1); return "paid", nil })
	lookup := core.Tool("lookup", "Look up.", func(context.Context, struct{}) (string, error) { looked.Add(1); return "found", nil })
	agent := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "c1", Args: `{}`}, wefttest.Call{Name: "lookup", ID: "c2", Args: `{}`}),
		wefttest.Say("done"),
	), pay, lookup)
	res, err := agent.Generate(context.Background(), core.Prompt("go"), core.IdempotencyKeys(func(c core.Call) string {
		if c.Name == "pay" {
			panic("key store down")
		}
		return "k-" + c.CallID
	}))
	if err != nil {
		t.Fatalf("run = %v, want it to go on", err)
	}
	if paid.Load() != 0 || looked.Load() != 1 {
		t.Errorf("pay ran %d, lookup ran %d; want 0 and 1", paid.Load(), looked.Load())
	}
	r := res.Steps[0].Results
	want := `weft: tool call not dispatched: the run's IdempotencyKeys function panicked naming the call's key: key store down`
	if !r[0].IsError || r[0].Content != want {
		t.Errorf("pay result = %+v\nwant the error %q", r[0], want)
	}
	if r[1].IsError || r[1].Content != "found" {
		t.Errorf("lookup result = %+v", r[1])
	}
}
