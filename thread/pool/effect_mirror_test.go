package pool_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/thread/pool"
	"github.com/weftbase/weft/wefttest"
)

// mirrorShip is one ship tool shared by a parent agent and its pool
// child: it records every handler's key and how many ran at once; the
// first handler blocks until release, ignoring its context.
type mirrorShip struct {
	mu      sync.Mutex
	keys    []string
	active  int
	most    int
	release chan struct{}
}

func (m *mirrorShip) def(timeout time.Duration) *core.ToolDef {
	return core.Tool("ship", "Ship.", func(ctx context.Context, _ struct{}) (string, error) {
		c, _ := core.CallFromContext(ctx)
		m.mu.Lock()
		m.keys = append(m.keys, c.IdempotencyKey)
		first := len(m.keys) == 1
		m.active++
		m.most = max(m.most, m.active)
		m.mu.Unlock()
		defer func() {
			m.mu.Lock()
			m.active--
			m.mu.Unlock()
		}()
		if first {
			<-m.release
		}
		return "shipped", nil
	}, core.Replay(core.ReplayNever), core.Timeout(timeout))
}

func (m *mirrorShip) snapshot() ([]string, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.keys...), m.most
}

// The pool's mirror gives the parent the child's answers (ADR 0032,
// 2026-11-10): while a child's abandoned ship handler still runs, the
// parent's own call to ship parks with the still-running reason and an
// approval is refused; once the handler has returned the approval runs
// it — with the same arguments, under the child's key, which the
// mirror carried.
func TestPoolMirrorGatesAndKeysTheParentsRepeat(t *testing.T) {
	for _, sc := range poolStores(t) {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			outlive(t, 100*time.Millisecond, func(timeout time.Duration) bool {
				ctx := context.Background()
				ship := &mirrorShip{release: make(chan struct{})}
				released := false
				defer func() {
					if !released {
						close(ship.release)
					}
				}()
				child := core.New(wefttest.Script(
					wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{}`}),
					wefttest.Say("child done"),
				), ship.def(timeout))
				p := pool.New(4)
				defer func() { _ = p.Close(ctx) }()
				parent := core.New(wefttest.Script(
					wefttest.ToolCalls(wefttest.Call{Name: "worker", ID: "d1", Args: wefttest.Args(struct{ Prompt string }{"go"})}),
					wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "p1", Args: `{}`}),
					wefttest.Say("parent done"),
				), p.MustWrap("worker", "w", child), ship.def(time.Minute))
				s, err := thread.Create(ctx, sc.st(t), parent)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = s.Close(ctx) }()
				turn, err := s.Send(ctx, core.User("go"))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := turn.Wait(); err != nil {
					t.Fatal(err)
				}
				if keys, _ := ship.snapshot(); len(keys) == 0 {
					return false // timed out in the gate: never started
				}
				pend := s.Pending()
				if len(pend) != 1 || !pend[0].Recovery || pend[0].Child == "" || pend[0].IdempotencyKey == "" {
					t.Fatalf("parent Pending = %+v, want the child's still-running recovery request mirrored with its key", pend)
				}
				mirror := pend[0]
				// Skip the child's call while its handler runs: the child
				// answers, the delegation settles, and the parent's model
				// asks for ship itself.
				if err := p.Decide(ctx, s, bound(s, thread.Deny(mirror.CallID, ""))); err != nil {
					t.Fatalf("Pool.Decide(deny) = %v", err)
				}
				waitReceipt(t, s, pool.Done)
				var parked thread.Request
				deadline := time.Now().Add(10 * time.Second)
				for {
					if err := s.WaitIdle(ctx); err != nil {
						t.Fatal(err)
					}
					if pp := s.Pending(); len(pp) == 1 && pp[0].CallID == "p1" {
						parked = pp[0]
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("parent Pending = %+v, want p1 held by the still-running gate", s.Pending())
					}
					time.Sleep(5 * time.Millisecond)
				}
				if parked.Reason != "an earlier call of this tool is still running: its handler was abandoned and has not returned; approve once it has returned, deny to skip this call" {
					t.Fatalf("p1's reason = %q", parked.Reason)
				}
				if keys, _ := ship.snapshot(); len(keys) != 1 {
					t.Fatalf("ship started %d handlers while the child's ran, want 1", len(keys))
				}
				if _, err := s.Decide(ctx, thread.Approve("p1").Bind(parked)); !errors.Is(err, thread.ErrStillRunning) {
					t.Fatalf("Decide(approve p1) while the child's handler runs = %v, want ErrStillRunning", err)
				}
				done := thread.AbandonedHandler(mirror.RunID, "s1")
				close(ship.release)
				released = true
				if done != nil {
					<-done
				}
				rt, err := s.Decide(ctx, thread.Approve("p1").Bind(parked))
				if err != nil || rt == nil {
					t.Fatalf("Decide(approve p1) after the handler returned = %v, %v", rt, err)
				}
				if res, err := rt.Wait(); err != nil || res.Text() != "parent done" {
					t.Fatalf("parent resume = %v, %v", res, err)
				}
				keys, most := ship.snapshot()
				if len(keys) != 2 || most != 1 || keys[1] != keys[0] || keys[0] != mirror.IdempotencyKey {
					t.Fatalf("keys %v (mirror key %s), at most %d at once: want the parent's repeat under the child's key", keys, mirror.IdempotencyKey, most)
				}
				return true
			})
		})
	}
}
