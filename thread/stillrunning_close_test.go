package thread_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
	"github.com/weftbase/weft/thread"
)

// closeHeldTool is a ship tool whose first handler ignores its context
// and runs until released — a Close that gives up abandons it, still
// running. Its Reconcile (ReplayNever only) counts the asks made while
// a handler runs.
type closeHeldTool struct {
	mu                    sync.Mutex
	invoked, active, most int
	askedWhileRunning     int
	release               chan struct{}
	started               chan struct{}
}

func newCloseHeldTool() *closeHeldTool {
	return &closeHeldTool{release: make(chan struct{}), started: make(chan struct{}, 4)}
}

func (a *closeHeldTool) def(policy core.ReplayPolicy) *core.ToolDef {
	opts := []core.ToolOption{core.Replay(policy), core.Timeout(time.Hour)}
	if policy == core.ReplayNever {
		opts = append(opts, core.Reconcile(func(context.Context, string) (bool, string, error) {
			a.mu.Lock()
			defer a.mu.Unlock()
			if a.active > 0 {
				a.askedWhileRunning++
			}
			return false, "", nil // "missing": the ladder would run it again
		}))
	}
	return core.Tool("ship", "Ship.", func(context.Context, struct{}) (string, error) {
		a.mu.Lock()
		a.invoked++
		first := a.invoked == 1
		a.active++
		a.most = max(a.most, a.active)
		a.mu.Unlock()
		a.started <- struct{}{}
		defer func() { a.mu.Lock(); a.active--; a.mu.Unlock() }()
		if first {
			<-a.release // ignores its context
		}
		return "shipped", nil
	}, opts...)
}

func (a *closeHeldTool) counts() (invoked, most, asked int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.invoked, a.most, a.askedWhileRunning
}

// P1-1 (release-gate review): a Close that gives up cuts the turn off
// while a handler that ignores its context still runs. A reopen of the
// session in the same process, then RecoverTurn: the call parks for a
// person as still running — no keyed retry, no Reconcile — and an
// approval is refused with ErrStillRunning until the handler returned;
// then the retry runs alone. ReplayKeyed and ReplayNever (with a
// Reconcile that would say "missing"), over every store; the reopen
// uses a fresh Storage value over the same file, as a server's would.
func TestCloseGiveUpThenRecoverTurnWaitsForTheStillRunningHandler(t *testing.T) {
	for _, policy := range []core.ReplayPolicy{core.ReplayKeyed, core.ReplayNever} {
		t.Run(string(policy), func(t *testing.T) {
			stores := []struct {
				name   string
				first  func(t *testing.T) thread.Storage
				reopen func(t *testing.T, st thread.Storage) thread.Storage
			}{{
				name:   "memory",
				first:  func(*testing.T) thread.Storage { return thread.Memory() },
				reopen: func(_ *testing.T, st thread.Storage) thread.Storage { return st },
			}}
			for _, b := range durableBackends(t) {
				stores = append(stores, struct {
					name   string
					first  func(t *testing.T) thread.Storage
					reopen func(t *testing.T, st thread.Storage) thread.Storage
				}{b.name, b.open, func(t *testing.T, _ thread.Storage) thread.Storage { return b.open(t) }})
			}
			for _, sc := range stores {
				t.Run(sc.name, func(t *testing.T) {
					ship := newCloseHeldTool()
					released := false
					defer func() {
						if !released {
							close(ship.release)
						}
					}()
					st := sc.first(t)
					s, err := thread.Create(bg, st, core.New(wefttest.Script(
						wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{}`}),
					), ship.def(policy)))
					if err != nil {
						t.Fatal(err)
					}
					turn, err := s.Send(bg, core.User("ship it"))
					if err != nil {
						t.Fatal(err)
					}
					<-ship.started
					cctx, cancel := context.WithTimeout(bg, 50*time.Millisecond)
					_ = s.Close(cctx) // gives up: the handler ignores the cancellation
					cancel()
					if _, err := turn.Wait(); err == nil {
						t.Fatal("the turn a Close gave up on ended without an error")
					}
					if err := s.Close(bg); err != nil {
						t.Fatal(err)
					}
					s2, err := thread.Open(bg, sc.reopen(t, st), s.ID(),
						core.New(wefttest.Script(wefttest.Say("done")), ship.def(policy)))
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = s2.Close(bg) }()
					rt, err := s2.RecoverTurn(bg)
					if err != nil || rt != nil {
						t.Fatalf("RecoverTurn = %v, %v; want the call parked for a person (nil, nil)", rt, err)
					}
					p := s2.Pending()
					if len(p) != 1 || !p[0].Recovery || !strings.Contains(p[0].Reason, "still running") {
						t.Fatalf("Pending = %+v, want one still-running recovery request", p)
					}
					if rt, err := s2.Decide(bg, thread.Approve("s1").Bind(p[0])); !errors.Is(err, thread.ErrStillRunning) || rt != nil {
						t.Fatalf("Decide while the handler runs = %v, %v; want ErrStillRunning", rt, err)
					}
					if invoked, _, asked := ship.counts(); invoked != 1 || asked != 0 {
						t.Fatalf("while the handler runs: ship ran %d times, Reconcile asked %d times; want 1 and 0", invoked, asked)
					}
					close(ship.release)
					released = true
					deadline := time.Now().Add(5 * time.Second)
					for {
						rt, err = s2.Decide(bg, thread.Approve("s1").Bind(p[0]))
						if !errors.Is(err, thread.ErrStillRunning) || time.Now().After(deadline) {
							break
						}
						time.Sleep(5 * time.Millisecond)
					}
					if err != nil || rt == nil {
						t.Fatalf("Decide after the handler returned = %v, %v", rt, err)
					}
					if res, err := rt.Wait(); err != nil || res.Text() != "done" {
						t.Fatalf("resume = %v, %v", res, err)
					}
					if invoked, most, asked := ship.counts(); invoked != 2 || most != 1 || asked != 0 {
						t.Fatalf("ship ran %d times, at most %d at once, Reconcile asked %d times while running; want 2, 1, 0", invoked, most, asked)
					}
				})
			}
		})
	}
}

// P2-1 (release-gate review): Fork copies a still-running recovery
// request with its original run id. The fork's approval is refused
// with ErrStillRunning while the first handler runs — the registry
// matches the run id and call id whatever session asks — and taken
// once it returned: no second handler beside the first.
func TestForkRefusesApprovingAStillRunningCall(t *testing.T) {
	t.Parallel()
	outlive(t, 10*time.Millisecond, func(timeout time.Duration) bool {
		ship := &abandonedTool{release: make(chan struct{})}
		released := false
		defer func() {
			if !released {
				close(ship.release)
			}
		}()
		s, err := thread.Create(bg, thread.Memory(), core.New(wefttest.Script(
			wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{}`}),
			wefttest.Say("done"),
		), ship.def(core.ReplayNever, timeout)))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = s.Close(bg) }()
		turn, err := s.Send(bg, core.User("ship it"))
		if err != nil {
			t.Fatal(err)
		}
		_, _ = turn.Wait()
		if invoked, _ := ship.counts(); invoked == 0 {
			return false // timed out in the gate: never started
		}
		if p := s.Pending(); len(p) != 1 || !strings.Contains(p[0].Reason, "still running") {
			t.Fatalf("Pending = %+v", p)
		}
		f, err := s.Fork(bg, s.Leaf())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = f.Close(bg) }()
		fp := f.Pending()
		if len(fp) != 1 || !fp[0].Recovery {
			t.Fatalf("fork Pending = %+v, want the still-running recovery request carried", fp)
		}
		if rt, err := f.Decide(bg, thread.Approve("s1").Bind(fp[0])); !errors.Is(err, thread.ErrStillRunning) || rt != nil {
			t.Fatalf("the fork's Decide while the handler runs = %v, %v; want ErrStillRunning", rt, err)
		}
		close(ship.release)
		released = true
		var rt *thread.Turn
		deadline := time.Now().Add(5 * time.Second)
		for {
			rt, err = f.Decide(bg, thread.Approve("s1").Bind(fp[0]))
			if !errors.Is(err, thread.ErrStillRunning) || time.Now().After(deadline) {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if err != nil || rt == nil {
			t.Fatalf("the fork's Decide after the handler returned = %v, %v", rt, err)
		}
		if _, err := rt.Wait(); err != nil {
			t.Fatal(err)
		}
		if gateTimedOut(f) {
			return false // the retry timed out in the gate
		}
		if invoked, most := ship.counts(); invoked != 2 || most != 1 {
			t.Fatalf("ship ran %d times, at most %d at once", invoked, most)
		}
		return true
	})
}
