package thread_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// recoverAudits returns the outcomes of the StepRecover audit entries
// for call.
func recoverAudits(s *thread.Session, call string) []string {
	var out []string
	for _, e := range s.Audit() {
		if a, ok := e.(thread.ApprovalAuditEntry); ok && a.Step == thread.StepRecover && a.CallID == call {
			out = append(out, a.Outcome)
		}
	}
	return out
}

// A caller that walks away while RecoverTurn asks the system of record
// is not an answer (review R6): RecoverTurn returns the context's error
// and writes nothing — no reconcile_error, no request for a person —
// and the next RecoverTurn asks again. Both backends, across a restart.
func TestReconcileInterruptedByTheCallerWritesNothing(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			pay := &sideEffect{name: "pay"}
			var asked atomic.Int32
			inAsk := make(chan struct{}, 1)
			reconcile := core.Reconcile(func(ctx context.Context, key string) (bool, string, error) {
				if asked.Add(1) == 1 {
					inAsk <- struct{}{}
					<-ctx.Done()
					return false, "", ctx.Err()
				}
				return true, "paid: receipt r-9", nil
			})
			agent := func(turns ...wefttest.Turn) *core.Agent {
				return core.New(wefttest.Script(turns...), pay.def(reconcile))
			}
			id, st := crashTurn(t, b, func(cs *crashStore) *core.Agent {
				pay.crash = cs
				return agent(charged("pay"))
			}, nil)
			pay.crash = nil
			st = restart(t, b, st, id)
			s, err := thread.Open(bg, st, id, agent(wefttest.Say("settled")))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close(bg) }()
			ctx, cancel := context.WithCancel(bg)
			go func() { <-inAsk; cancel() }()
			if _, err := s.RecoverTurn(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("RecoverTurn under a canceled ask = %v, want context.Canceled", err)
			}
			if got := recoverAudits(s, "call_1"); len(got) != 0 {
				t.Fatalf("an interrupted ask wrote %v", got)
			}
			if p := s.Pending(); len(p) != 0 && p[0].Recovery {
				t.Fatalf("an interrupted ask parked the call for a person: %+v", p)
			}
			rt, err := s.RecoverTurn(bg)
			if err != nil || rt == nil {
				t.Fatalf("the second RecoverTurn = %v, %v", rt, err)
			}
			res, err := rt.Wait()
			if err != nil || toolResult(res, "call_1") != "paid: receipt r-9" {
				t.Fatalf("resume = %v, %v", res, err)
			}
			if got := recoverAudits(s, "call_1"); len(got) != 1 || got[0] != "reconcile_found" {
				t.Fatalf("audit = %v, want one reconcile_found", got)
			}
			if n := len(pay.ran()); n != 1 {
				t.Fatalf("pay ran %d times, want only the cut-off run", n)
			}
		})
	}
}

// Several cut-off calls ask their Reconcile in parallel, under one
// shared bound: each of two here answers only once the other has been
// asked, which a sequential ask would never let happen.
func TestReconcileAsksCallsInParallel(t *testing.T) {
	var started sync.WaitGroup
	started.Add(2)
	var ran sync.WaitGroup
	ran.Add(2)
	var crash atomic.Pointer[crashStore]
	tool := func(name string) *core.ToolDef {
		return core.Tool(name, "A side effect.", func(ctx context.Context, in orderArg) (string, error) {
			ran.Done()
			ran.Wait()
			crash.Load().down.Store(true) // both started: the process dies before either result lands
			return name + " done", nil
		}, core.Reconcile(func(ctx context.Context, key string) (bool, string, error) {
			started.Done()
			done := make(chan struct{})
			go func() { started.Wait(); close(done) }()
			select {
			case <-done:
				return true, name + ": done", nil
			case <-time.After(5 * time.Second):
				return false, "", errors.New("the other call was never asked beside this one")
			}
		}))
	}
	pay, ship := tool("pay"), tool("ship")
	b := durableBackends(t)[0]
	id, st := crashTurn(t, b, func(cs *crashStore) *core.Agent {
		crash.Store(cs)
		return core.New(wefttest.Script(wefttest.ToolCalls(
			wefttest.Call{Name: "pay", ID: "call_pay", Args: `{"order_id":"1"}`},
			wefttest.Call{Name: "ship", ID: "call_ship", Args: `{"order_id":"1"}`},
		)), pay, ship, core.Parallelism(2))
	}, nil)
	st = restart(t, b, st, id)
	s, err := thread.Open(bg, st, id, core.New(wefttest.Script(wefttest.Say("ok")), pay, ship))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(bg) }()
	rt, err := s.RecoverTurn(bg)
	if err != nil || rt == nil {
		t.Fatalf("RecoverTurn = %v, %v", rt, err)
	}
	if _, err := rt.Wait(); err != nil {
		t.Fatal(err)
	}
	for _, c := range []string{"call_pay", "call_ship"} {
		if got := recoverAudits(s, c); len(got) != 1 || got[0] != "reconcile_found" {
			t.Errorf("%s: audit %v, want reconcile_found", c, got)
		}
	}
}
