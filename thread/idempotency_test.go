package thread_test

import (
	"context"
	"sync"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// keyProbe is a tool that records the IdempotencyKey each execution saw.
type keyProbe struct {
	mu   sync.Mutex
	keys map[string][]string
}

func (k *keyProbe) tool(opts ...core.ToolOption) *core.ToolDef {
	return core.Tool("charge", "Charge a card.", func(ctx context.Context, in refundInput) (string, error) {
		c, _ := core.CallFromContext(ctx)
		k.mu.Lock()
		defer k.mu.Unlock()
		if k.keys == nil {
			k.keys = map[string][]string{}
		}
		k.keys[c.CallID] = append(k.keys[c.CallID], c.IdempotencyKey)
		return "charged " + in.OrderID, nil
	}, opts...)
}

// callEntry is the id of the assistant entry holding callID, the
// nearest one on the session's path.
func callEntry(t *testing.T, s *thread.Session, callID string) string {
	t.Helper()
	path, err := s.Path(s.Leaf())
	if err != nil {
		t.Fatal(err)
	}
	for i := len(path) - 1; i >= 0; i-- {
		me, ok := path[i].(thread.MessageEntry)
		if !ok {
			continue
		}
		for _, p := range me.Message.Content {
			if c, ok := p.(core.ToolCallPart); ok && c.ID == callID {
				return me.ID
			}
		}
	}
	t.Fatalf("no assistant entry holds %s", callID)
	return ""
}

// A call's IdempotencyKey is the session's, derived from the assistant
// entry that holds it: a platform computes it from the parked request
// before the resume, and the handler that runs after a restart — in
// another process, under another run id — reads exactly that key. A
// call the model issues in a later turn under the same call id gets
// another. Both file backends, a fresh store for the reopen.
func TestIdempotencyKeySurvivesRestart(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			probe := &keyProbe{}
			agent := func(turns ...wefttest.Turn) *core.Agent {
				return core.New(wefttest.Script(turns...), probe.tool(core.RequireApproval()))
			}
			st := b.open(t)
			s, err := thread.Create(bg, st, agent(wefttest.ToolCalls(wefttest.Call{Name: "charge", ID: "call_1", Args: `{"order_id":"1"}`})))
			if err != nil {
				t.Fatal(err)
			}
			turn, err := s.Send(bg, core.User("charge it"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := turn.Wait(); err != nil {
				t.Fatal(err)
			}
			id := s.ID()
			want := thread.IdempotencyKey(id, callEntry(t, s, "call_1"), "call_1")
			if p := s.Pending(); len(p) != 1 || p[0].IdempotencyKey != want {
				t.Fatalf("pending = %+v, want the key %s on the request", p, want)
			}
			st = restart(t, b, st, id)

			s2, err := thread.Open(bg, st, id, agent(
				wefttest.Say("charged"),
				wefttest.ToolCalls(wefttest.Call{Name: "charge", ID: "call_1", Args: `{"order_id":"2"}`}),
			))
			if err != nil {
				t.Fatal(err)
			}
			if p := s2.Pending(); len(p) != 1 || p[0].IdempotencyKey != want {
				t.Fatalf("pending after reopen = %+v, want the key %s", p, want)
			}
			rt, err := s2.Decide(bg, thread.Approve("call_1"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := rt.Wait(); err != nil {
				t.Fatal(err)
			}
			if got := probe.keys["call_1"]; len(got) != 1 || got[0] != want {
				t.Fatalf("handler keys = %v, want [%s]", got, want)
			}
			// The same call id issued anew: another entry, another key.
			t2, err := s2.Send(bg, core.User("again"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := t2.Wait(); err != nil {
				t.Fatal(err)
			}
			rt, err = s2.Decide(bg, thread.Approve("call_1"))
			if err != nil {
				t.Fatal(err)
			}
			if rt != nil {
				_, _ = rt.Wait()
			}
			if got := probe.keys["call_1"]; len(got) != 2 || got[1] == want || got[1] == "" {
				t.Fatalf("a re-issued call id kept the old key: %v", got)
			}
			if err := s2.Close(bg); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// A call that runs inside its own step gets the session's key too.
func TestIdempotencyKeyInStep(t *testing.T) {
	probe := &keyProbe{}
	s, err := thread.Create(bg, thread.Memory(), core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "charge", ID: "c", Args: `{"order_id":"1"}`}),
		wefttest.Say("done"),
	), probe.tool()))
	if err != nil {
		t.Fatal(err)
	}
	turn, err := s.Send(bg, core.User("go"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := turn.Wait(); err != nil {
		t.Fatal(err)
	}
	if want := thread.IdempotencyKey(s.ID(), callEntry(t, s, "c"), "c"); len(probe.keys["c"]) != 1 || probe.keys["c"][0] != want {
		t.Fatalf("keys = %v, want %s", probe.keys["c"], want)
	}
}

// A Subagent's child run does not inherit the session's keys (ADR
// 0031): its calls carry run-scoped keys, so a re-run of the child
// issues its calls under new ones. The parent's own call — the
// Subagent tool's — carries the session's key.
func TestSubagentChildKeysAreRunScoped(t *testing.T) {
	probe := &keyProbe{}
	var outer []string
	child := func() *core.Agent {
		return core.New(wefttest.Script(
			wefttest.ToolCalls(wefttest.Call{Name: "charge", ID: "inner", Args: `{"order_id":"1"}`}),
			wefttest.Say("child done"),
		), probe.tool())
	}
	sub := core.Subagent("delegate", "Delegate.", child())
	wrap := func(next core.ToolCaller) core.ToolCaller {
		return func(ctx context.Context, call core.ToolCallPart) (string, error) {
			c, _ := core.CallFromContext(ctx)
			if c.Name == "delegate" {
				outer = append(outer, c.IdempotencyKey)
			}
			return next(ctx, call)
		}
	}
	s, err := thread.Create(bg, thread.Memory(), core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "delegate", ID: "d1", Args: `{"prompt":"go"}`}),
		wefttest.Say("done"),
	), sub, core.WrapTools(wrap)))
	if err != nil {
		t.Fatal(err)
	}
	turn, err := s.Send(bg, core.User("go"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := turn.Wait(); err != nil {
		t.Fatal(err)
	}
	if want := thread.IdempotencyKey(s.ID(), callEntry(t, s, "d1"), "d1"); len(outer) != 1 || outer[0] != want {
		t.Fatalf("the Subagent call's key = %v, want the session's %s", outer, want)
	}
	inner := probe.keys["inner"]
	if len(inner) != 1 || inner[0] == "" || inner[0] == outer[0] {
		t.Fatalf("child call keys = %v: want one run-scoped key", inner)
	}
}
