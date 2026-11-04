package thread_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// The ladder's second rung (ADR 0032): a ReplayNever call cut off by a
// crash, whose tool can ask the system of record (core.Reconcile), is
// settled by the answer — found: its recorded result is the call's and
// the tool does not run; missing: it runs again under the same key; an
// error: a person decides (a recovery request). A crash while it asks
// writes nothing, so the next recovery asks again and the tool never
// runs on its account. Both backends, across restarts.
func TestReconcileRung(t *testing.T) {
	for _, b := range durableBackends(t) {
		for _, mode := range []string{"found", "missing", "error", "crash"} {
			t.Run(b.name+"/"+mode, func(t *testing.T) {
				pay := &sideEffect{name: "pay"}
				var asked atomic.Int32
				var keys []string
				var down *crashStore
				reconcile := core.Reconcile(func(ctx context.Context, key string) (bool, string, error) {
					keys = append(keys, key)
					switch n := asked.Add(1); {
					case mode == "found":
						return true, "paid: receipt r-77", nil
					case mode == "missing":
						return false, "", nil
					case mode == "error":
						return false, "", errors.New("ledger unavailable")
					case mode == "crash" && n == 1:
						down.down.Store(true) // the process dies while it asks
						return false, "", nil
					default:
						return true, "paid: receipt r-77", nil
					}
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
				var s *thread.Session
				var err error
				if mode == "crash" {
					cs := &crashStore{Storage: st}
					down = cs
					s, err = thread.Open(bg, cs, id, agent(wefttest.Say("x")))
					if err != nil {
						t.Fatal(err)
					}
					if _, err := s.RecoverTurn(bg); err == nil {
						t.Fatal("RecoverTurn on a dead store succeeded")
					}
					st = restart(t, b, st, id)
				}
				s, err = thread.Open(bg, st, id, agent(wefttest.Say("settled")))
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = s.Close(bg) }()
				rt, err := s.RecoverTurn(bg)
				if err != nil {
					t.Fatal(err)
				}
				audit := ""
				for _, e := range s.Audit() {
					if a, ok := e.(thread.ApprovalAuditEntry); ok && a.Step == thread.StepRecover && a.CallID == "call_1" {
						audit = a.Outcome
					}
				}
				switch mode {
				case "found", "crash":
					if rt == nil || audit != "reconcile_found" {
						t.Fatalf("found: turn %v, audit %q", rt, audit)
					}
					res, err := rt.Wait()
					if err != nil || toolResult(res, "call_1") != "paid: receipt r-77" {
						t.Fatalf("resume = %v, %v", res, err)
					}
					if n := len(pay.ran()); n != 1 {
						t.Fatalf("pay ran %d times, want only the cut-off run", n)
					}
					if mode == "crash" && asked.Load() != 2 {
						t.Fatalf("asked %d times, want again after the crash", asked.Load())
					}
				case "missing":
					if rt == nil || audit != "reconcile_missing" {
						t.Fatalf("missing: turn %v, audit %q", rt, audit)
					}
					if _, err := rt.Wait(); err != nil {
						t.Fatal(err)
					}
					if k := pay.ran(); len(k) != 2 || k[0] != k[1] || k[0] != keys[0] {
						t.Fatalf("pay keys %v, asked with %v: want one key throughout", k, keys)
					}
				case "error":
					p := s.Pending()
					if rt != nil || audit != "reconcile_error" || len(p) != 1 || !p[0].Recovery {
						t.Fatalf("error: turn %v, audit %q, pending %+v", rt, audit, p)
					}
					if n := len(pay.ran()); n != 1 {
						t.Fatalf("pay ran %d times", n)
					}
				}
			})
		}
	}
}
