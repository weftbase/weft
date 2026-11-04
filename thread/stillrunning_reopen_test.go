package thread_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
	"github.com/weftbase/weft/thread"
)

// P2-1 (release-gate review): a handler abandoned past its bound is
// known to the process, not to the Session that abandoned it. A Close
// and an Open of the same session in one process — a server evicting
// an idle session and reopening it for the decision — must still
// refuse the approval (ErrStillRunning) while the first handler runs,
// and take it once it returned: the retry runs alone. ReplayNever and
// ReplayKeyed, over every store; the reopen uses a fresh Storage value
// over the same file, as a server's would.
func TestStillRunningSurvivesAnInProcessReopen(t *testing.T) {
	for _, policy := range []core.ReplayPolicy{core.ReplayNever, core.ReplayKeyed} {
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
					ship := &abandonedTool{release: make(chan struct{})}
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
					_, _ = turn.Wait()
					p := s.Pending()
					if len(p) != 1 || !p[0].Recovery || !strings.Contains(p[0].Reason, "still running") {
						t.Fatalf("Pending = %+v", p)
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
					p = s2.Pending()
					if len(p) != 1 {
						t.Fatalf("reopened Pending = %+v", p)
					}
					if rt, err := s2.Decide(bg, thread.Approve("s1").Bind(p[0])); !errors.Is(err, thread.ErrStillRunning) || rt != nil {
						t.Fatalf("Decide on the reopened session while the handler runs = %v, %v; want ErrStillRunning", rt, err)
					}
					if len(s2.Pending()) != 1 {
						t.Fatalf("the refused approval recorded something: %+v", s2.Pending())
					}
					close(ship.release)
					released = true
					var rt *thread.Turn
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
					if invoked, most := ship.counts(); invoked != 2 || most != 1 {
						t.Fatalf("ship ran %d times, at most %d at once: want the retry after the first returned", invoked, most)
					}
				})
			}
		})
	}
}

// The registry's key keeps two stores apart: sessions of one pinned id
// in two stores, the first's handler still running past its bound, the
// second's returned. An approval in the second is taken — the first's
// handler is another stored session's (told apart by the header's
// Created) — and runs; the first's is still refused.
func TestStillRunningDoesNotCrossStoresOfOneSessionID(t *testing.T) {
	pinned := func() thread.SessionOption {
		var n atomic.Int64
		return thread.IDs(func() string {
			if n.Add(1) == 1 {
				return "s_same"
			}
			return fmt.Sprintf("e_%d", n.Load())
		})
	}
	held := &abandonedTool{release: make(chan struct{})}
	defer close(held.release)
	a, err := thread.Create(bg, thread.Memory(), core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{}`}),
	), held.def(core.ReplayNever)), pinned())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close(bg) }()
	turn, err := a.Send(bg, core.User("ship it"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = turn.Wait()
	pa := a.Pending()
	if len(pa) != 1 || !strings.Contains(pa[0].Reason, "still running") {
		t.Fatalf("a's Pending = %+v", pa)
	}

	time.Sleep(2 * time.Millisecond) // a later header time, as any second Create has
	var ran atomic.Int32
	quick := core.Tool("ship", "Ship.", func(context.Context, struct{}) (string, error) {
		if ran.Add(1) == 1 {
			time.Sleep(30 * time.Millisecond) // past its Timeout, back inside the bound
		}
		return "shipped", nil
	}, core.Replay(core.ReplayNever), core.Timeout(5*time.Millisecond))
	b, err := thread.Create(bg, thread.Memory(), core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{}`}),
		wefttest.Say("done"),
	), quick), pinned())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close(bg) }()
	if a.ID() != b.ID() {
		t.Fatalf("ids %s and %s: the test needs one id", a.ID(), b.ID())
	}
	turn, err = b.Send(bg, core.User("ship it"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = turn.Wait()
	pb := b.Pending()
	if len(pb) != 1 || !pb[0].Recovery || pb[0].RunID != pa[0].RunID {
		t.Fatalf("b's Pending = %+v, want a recovery request of the same run id as a's", pb)
	}
	rt, err := b.Decide(bg, thread.Approve("s1").Bind(pb[0]))
	if err != nil || rt == nil {
		t.Fatalf("b's Decide = %v, %v: another store's handler refused it", rt, err)
	}
	if res, err := rt.Wait(); err != nil || res.Text() != "done" {
		t.Fatalf("b's resume = %v, %v", res, err)
	}
	if _, err := a.Decide(bg, thread.Approve("s1").Bind(pa[0])); !errors.Is(err, thread.ErrStillRunning) {
		t.Fatalf("a's Decide = %v, want ErrStillRunning", err)
	}
}
