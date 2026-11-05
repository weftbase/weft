package thread_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// lockFree fails the test when fn has not returned after d — the
// symptom of a session lock held for good.
func lockFree(t *testing.T, d time.Duration, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); fn() }()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s did not return within %s: the session lock is held", what, d)
	}
}

// A panic inside Decide or DecideSigned — the caller's IDs function
// here, which the decision's entries call under the session lock —
// propagates to the caller and leaves the session unlocked: the next
// Session method returns, and the session still decides.
func TestDecidePanicReleasesTheLock(t *testing.T) {
	for _, signed := range []bool{false, true} {
		name := map[bool]string{false: "Decide", true: "DecideSigned"}[signed]
		t.Run(name, func(t *testing.T) {
			var boom atomic.Bool
			ring := matrixRing(t)
			refund := core.Tool("refund", "Refund.", func(context.Context, refundV1) (string, error) {
				return "refunded", nil
			}, core.RequireApproval())
			s, err := thread.Create(bg, thread.Memory(), core.New(wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "refund", ID: "c1", Args: `{"id":"1"}`}),
				wefttest.Say("done"),
			), refund), thread.WithKeyring(ring), thread.IDs(func() string {
				if boom.Load() {
					panic("ids exploded")
				}
				return thread.NewEntryID()
			}))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close(bg) }()
			turn, _ := s.Send(bg, core.User("refund"))
			if _, err := turn.Wait(); err != nil {
				t.Fatal(err)
			}
			d := bound(s, thread.Approve("c1"))
			decide := func() (*thread.Turn, error) {
				if !signed {
					return s.Decide(bg, d)
				}
				ch, err := s.Request("c1")
				if err != nil {
					t.Fatal(err)
				}
				sd, err := ring.Sign(ch, d)
				if err != nil {
					t.Fatal(err)
				}
				return s.DecideSigned(bg, sd)
			}
			boom.Store(true)
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				_, _ = decide()
			}()
			boom.Store(false)
			if recovered == nil {
				t.Fatal("the IDs panic did not reach the caller")
			}
			lockFree(t, 3*time.Second, "Pending after the panic", func() { _ = s.Pending() })
			if len(s.Pending()) != 1 {
				t.Fatalf("pending after the panicked decision: %+v; want the call still pending, nothing recorded", s.Pending())
			}
			rt, err := decide()
			if err != nil || rt == nil {
				t.Fatalf("decide after the panic = %v, %v", rt, err)
			}
			if _, err := rt.Wait(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// The agent's ToolSource is never called with the session lock held:
// a source that reads the session (here s.Leaf, which takes the lock)
// completes through an ApproveAlways decision (the grant binds the
// tool's schema), the resume it arms (the drift check), Grant, and a
// recovery of a cut-off turn — each once deadlocked.
func TestToolSourceMayReadTheSession(t *testing.T) {
	var cur atomic.Pointer[thread.Session]
	var calls, ran atomic.Int32
	src := core.ToolSource(func() []*core.ToolDef {
		if s := cur.Load(); s != nil {
			_ = s.Leaf() // takes the session lock
			calls.Add(1)
		}
		return []*core.ToolDef{core.Tool("refund", "Refund.", func(context.Context, refundV1) (string, error) {
			ran.Add(1)
			return "refunded", nil
		}, core.RequireApproval())}
	})
	s, err := thread.Create(bg, thread.Memory(), core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "refund", ID: "c1", Args: `{"id":"1"}`}),
		wefttest.Say("done"),
	), src))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(bg) }()
	cur.Store(s)
	turn, _ := s.Send(bg, core.User("refund"))
	if _, err := turn.Wait(); err != nil {
		t.Fatal(err)
	}
	lockFree(t, 5*time.Second, "Decide(ApproveAlways) and its resume", func() {
		rt, err := s.Decide(bg, bound(s, thread.ApproveAlways("c1")))
		if err != nil || rt == nil {
			t.Errorf("Decide = %v, %v", rt, err)
			return
		}
		if _, err := rt.Wait(); err != nil {
			t.Error(err)
		}
	})
	if ran.Load() != 1 {
		t.Fatalf("the approved call ran %d times, want once", ran.Load())
	}
	lockFree(t, 5*time.Second, "Grant", func() {
		if err := s.Grant(bg, thread.Grant{Tool: "refund"}); err != nil {
			t.Error(err)
		}
	})
	if calls.Load() == 0 {
		t.Fatal("the source was never consulted")
	}
	var schemas int
	for _, e := range s.Entries() {
		if g, ok := e.(thread.GrantEntry); ok && g.SchemaSHA256 != "" {
			schemas++
		}
	}
	if schemas != 2 {
		t.Fatalf("%d grants bound to the source tool's schema, want 2 (ApproveAlways, Grant)", schemas)
	}
}

// RecoverTurn reads the agent's tools before it locks: a ToolSource
// that reads the session completes the recovery of a cut-off call to
// one of its tools.
func TestRecoverWithASourceThatReadsTheSession(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			look := &sideEffect{name: "look"}
			var cur atomic.Pointer[thread.Session]
			src := func() core.Option {
				return core.ToolSource(func() []*core.ToolDef {
					if s := cur.Load(); s != nil {
						_ = s.Leaf()
					}
					return []*core.ToolDef{look.def(core.Replay(core.ReplaySafe))}
				})
			}
			id, st := crashTurn(t, b, func(cs *crashStore) *core.Agent {
				look.crash = cs
				return core.New(wefttest.Script(charged("look")), src())
			}, nil)
			look.crash = nil
			st = restart(t, b, st, id)
			s, err := thread.Open(bg, st, id, core.New(wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "look", ID: "call_2", Args: `{"order_id":"1"}`}),
				wefttest.Say("looked up"),
			), src()))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close(bg) }()
			cur.Store(s)
			lockFree(t, 5*time.Second, "RecoverTurn", func() {
				rt, err := s.RecoverTurn(bg)
				if err != nil || rt == nil {
					t.Errorf("RecoverTurn = %v, %v", rt, err)
					return
				}
				if _, err := rt.Wait(); err != nil {
					t.Error(err)
				}
			})
		})
	}
}
