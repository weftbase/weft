package thread_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// P1-5 under a session: a call a handler dispatches with CallTool
// passes the session's gate — it starts only under an outer call the
// gate let start (dispatched, recorded) — and carries a key derived
// from the outer call's recovery-stable key. A ReplaySafe tool's
// handler starts no effect through CallTool: the inner call is refused.
// Both backends.
func TestCallToolInnerCallsUnderTheSessionGate(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			var mu sync.Mutex
			var keys []string
			charge := core.Tool("charge", "Charge.", func(ctx context.Context, _ struct{}) (string, error) {
				c, _ := core.CallFromContext(ctx)
				mu.Lock()
				keys = append(keys, c.IdempotencyKey)
				mu.Unlock()
				return "charged", nil
			})
			var agent *core.Agent
			var outerKey string
			errs := map[string]error{}
			hand := func(name string, opts ...core.ToolOption) *core.ToolDef {
				return core.Tool(name, "By hand.", func(ctx context.Context, _ struct{}) (string, error) {
					c, _ := core.CallFromContext(ctx)
					_, err := agent.CallTool(ctx, core.ToolCallPart{ID: "in-" + c.CallID, Name: "charge", Args: []byte(`{}`)})
					mu.Lock()
					errs[name] = err
					if name == "checkout" {
						outerKey = c.IdempotencyKey
					}
					mu.Unlock()
					return "done", nil
				}, opts...)
			}
			agent = core.New(wefttest.Script(
				wefttest.ToolCalls(
					wefttest.Call{Name: "checkout", ID: "c1", Args: `{}`},
					wefttest.Call{Name: "peek", ID: "c2", Args: `{}`},
				),
				wefttest.Say("ok"),
			), charge, hand("checkout"), hand("peek", core.Replay(core.ReplaySafe)))
			s, err := thread.Create(bg, b.open(t), agent)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close(bg) }()
			turn, err := s.Send(bg, core.User("go"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := turn.Wait(); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if errs["checkout"] != nil || len(keys) != 1 || keys[0] == "" || keys[0] == outerKey {
				t.Fatalf("checkout's inner call: err %v, keys %v (outer %q)", errs["checkout"], keys, outerKey)
			}
			if !errors.Is(errs["peek"], core.ErrNotDispatched) {
				t.Fatalf("a ReplaySafe handler's inner effect = %v, want ErrNotDispatched", errs["peek"])
			}
		})
	}
}

// Review 3 P2-1: CallTool at depth under a session. A non-safe tool's
// handler dispatches a non-safe tool whose handler dispatches the
// charge: each inner call starts under an outer call the gate let start
// — the recorded call or a recorded inner one — and carries a key
// derived from its outer's key and position, recursively. The charge
// runs once; a nested call under a ReplaySafe tool's inner call is
// still refused. Both backends.
func TestCallToolNestedUnderTheSessionGate(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			var mu sync.Mutex
			var keys []string
			var agent *core.Agent
			var innerErr, nestedErr, safeNestedErr error
			charge := core.Tool("charge", "Charge.", func(ctx context.Context, _ struct{}) (string, error) {
				c, _ := core.CallFromContext(ctx)
				mu.Lock()
				keys = append(keys, c.IdempotencyKey)
				mu.Unlock()
				return "charged", nil
			})
			wrap := core.Tool("wrap", "Wrap.", func(ctx context.Context, _ struct{}) (string, error) {
				_, err := agent.CallTool(ctx, core.ToolCallPart{ID: "n1", Name: "charge", Args: []byte(`{}`)})
				nestedErr = err
				return "wrapped", err
			})
			look := core.Tool("look", "Look.", func(ctx context.Context, _ struct{}) (string, error) {
				_, err := agent.CallTool(ctx, core.ToolCallPart{ID: "n2", Name: "charge", Args: []byte(`{}`)})
				safeNestedErr = err
				return "looked", nil
			}, core.Replay(core.ReplaySafe))
			checkout := core.Tool("checkout", "Checkout.", func(ctx context.Context, _ struct{}) (string, error) {
				_, innerErr = agent.CallTool(ctx, core.ToolCallPart{ID: "i1", Name: "wrap", Args: []byte(`{}`)})
				_, _ = agent.CallTool(ctx, core.ToolCallPart{ID: "i2", Name: "look", Args: []byte(`{}`)})
				return "done", nil
			})
			agent = core.New(wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "checkout", ID: "c1", Args: `{}`}),
				wefttest.Say("ok"),
			), charge, wrap, look, checkout)
			s, err := thread.Create(bg, b.open(t), agent)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close(bg) }()
			turn, err := s.Send(bg, core.User("go"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := turn.Wait(); err != nil {
				t.Fatal(err)
			}
			if innerErr != nil || nestedErr != nil {
				t.Fatalf("inner %v, nested %v: a call nested under a recorded inner call runs", innerErr, nestedErr)
			}
			if !errors.Is(safeNestedErr, core.ErrNotDispatched) {
				t.Fatalf("nested under a ReplaySafe tool's call = %v, want ErrNotDispatched", safeNestedErr)
			}
			if len(keys) != 1 || keys[0] == "" {
				t.Fatalf("charge keys %v, want one run with a key", keys)
			}
		})
	}
}
