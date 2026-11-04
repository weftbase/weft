package pool_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/thread/jsonl"
	"github.com/weftbase/weft/thread/pool"
	"github.com/weftbase/weft/thread/sqlite"
)

// poolStores are the stores a pool recovery test runs over.
func poolStores(t *testing.T) []struct {
	name string
	st   func(t *testing.T) thread.Storage
} {
	return []struct {
		name string
		st   func(t *testing.T) thread.Storage
	}{
		{"memory", func(*testing.T) thread.Storage { return thread.Memory() }},
		{"jsonl", func(t *testing.T) thread.Storage {
			st, err := jsonl.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			return st
		}},
		{"sqlite", func(t *testing.T) thread.Storage {
			st, err := sqlite.Open(filepath.Join(t.TempDir(), "s.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if c, ok := st.(interface{ Close() error }); ok {
					_ = c.Close()
				}
			})
			return st
		}},
	}
}

// heldShip is a child tool whose first handler ignores its Timeout and
// runs until released: abandoned, still running past the bound.
type heldShip struct {
	mu                    sync.Mutex
	invoked, active, most int
	release               chan struct{}
}

func (h *heldShip) def() *core.ToolDef {
	return core.Tool("ship", "Ship.", func(context.Context, struct{}) (string, error) {
		h.mu.Lock()
		h.invoked++
		first := h.invoked == 1
		h.active++
		h.most = max(h.most, h.active)
		h.mu.Unlock()
		defer func() { h.mu.Lock(); h.active--; h.mu.Unlock() }()
		if first {
			<-h.release
		}
		return "shipped", nil
	}, core.Replay(core.ReplayNever), core.Timeout(100*time.Millisecond))
}

func (h *heldShip) counts() (invoked, most int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.invoked, h.most
}

// stillRunningDelegation runs a parent turn that delegates to a child
// whose ship call is abandoned and still running: the child parks it
// as a recovery request, mirrored onto the parent.
func stillRunningDelegation(t *testing.T, st thread.Storage) (*pool.Pool, *thread.Session, *heldShip, thread.Request) {
	t.Helper()
	ctx := context.Background()
	ship := &heldShip{release: make(chan struct{})}
	child := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{}`}),
		wefttest.Say("child done"),
	), ship.def())
	p := pool.New(4)
	parent := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "worker", ID: "d1", Args: wefttest.Args(struct{ Prompt string }{"go"})}),
		wefttest.Say("parent done"),
	), p.MustWrap("worker", "w", child))
	s, err := thread.Create(ctx, st, parent)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-ship.release:
		default:
			close(ship.release)
		}
		_ = p.Close(ctx)
		_ = s.Close(ctx)
	})
	turn, err := s.Send(ctx, core.User("go"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := turn.Wait(); err != nil {
		t.Fatal(err)
	}
	pend := s.Pending()
	if len(pend) != 1 || !pend[0].Recovery || pend[0].Child == "" || !strings.Contains(pend[0].Reason, "still running") {
		t.Fatalf("parent Pending = %+v, want the child's still-running recovery request mirrored", pend)
	}
	return p, s, ship, pend[0]
}

// waitReceipt waits for the parent's one receipt to reach state.
func waitReceipt(t *testing.T, s *thread.Session, state pool.State) pool.Receipt {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		rs := pool.Receipts(s)
		if len(rs) == 1 && rs[0].State == state {
			return rs[0]
		}
		if time.Now().After(deadline) {
			t.Fatalf("receipts = %+v, want one %s", rs, state)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// assertParkedOnTheMirror checks the delegation was neither failed nor
// settled: its receipt reads parked and the mirror is in force.
func assertParkedOnTheMirror(t *testing.T, s *thread.Session, mirror thread.Request) {
	t.Helper()
	time.Sleep(100 * time.Millisecond) // a wrong settlement would have begun
	deadline := time.Now().Add(10 * time.Second)
	for {
		rs := pool.Receipts(s)
		if len(rs) != 1 || rs[0].Settled() {
			t.Fatalf("receipts = %+v, want the delegation parked, never settled", rs)
		}
		if rs[0].State == pool.Parked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("receipts = %+v, want the delegation parked", rs)
		}
		time.Sleep(5 * time.Millisecond)
	}
	ms, err := s.MirroredRequests(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range ms {
		if m.Request.CallID == mirror.CallID {
			found = true
		}
	}
	if !found {
		t.Fatalf("mirrors = %+v, want %s in force", ms, mirror.CallID)
	}
}

// P1-B (release-gate review): Pool.Decide approving a mirrored
// recovery request whose child handler still runs is refused with
// thread.ErrStillRunning before the parent records anything — the
// delegation stays parked, the mirror undecided — and taken once the
// handler returned: the child's retry runs alone and the delegation
// ends done.
func TestPoolDecideRefusesApprovingAStillRunningChildCall(t *testing.T) {
	for _, sc := range poolStores(t) {
		t.Run(sc.name, func(t *testing.T) {
			ctx := context.Background()
			p, s, ship, mirror := stillRunningDelegation(t, sc.st(t))
			err := p.Decide(ctx, s, bound(s, thread.Approve(mirror.CallID)))
			if !errors.Is(err, thread.ErrStillRunning) {
				t.Fatalf("Pool.Decide = %v, want ErrStillRunning", err)
			}
			if pend := s.Pending(); len(pend) != 1 || pend[0].ID != mirror.ID {
				t.Fatalf("parent Pending = %+v: the refused approval recorded something", pend)
			}
			for _, e := range s.Entries() {
				if d, ok := e.(thread.ApprovalDecisionEntry); ok && d.CallID == mirror.CallID {
					t.Fatalf("the refused approval was recorded: %+v", d)
				}
			}
			assertParkedOnTheMirror(t, s, mirror)
			close(ship.release)
			deadline := time.Now().Add(5 * time.Second)
			for {
				err = p.Decide(ctx, s, bound(s, thread.Approve(mirror.CallID)))
				if !errors.Is(err, thread.ErrStillRunning) || time.Now().After(deadline) {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			if err != nil {
				t.Fatalf("Pool.Decide after the handler returned = %v", err)
			}
			waitReceipt(t, s, pool.Done)
			if invoked, most := ship.counts(); invoked != 2 || most != 1 {
				t.Fatalf("ship ran %d times, at most %d at once", invoked, most)
			}
		})
	}
}

// P1-B backstop: an approval recorded on the parent by a door that did
// not check (Session.Decide on the parent directly) reaches a child
// that refuses it (ErrStillRunning). The refusal maps to "stay
// parked, keep the mirror" — never a FAILED settlement and
// SUBAGENT_FAILED to the parent's model — and when the handler
// returns the pool replays the recorded approval by itself.
func TestRefusedChildReplayKeepsTheDelegationParked(t *testing.T) {
	for _, sc := range poolStores(t) {
		t.Run(sc.name, func(t *testing.T) {
			ctx := context.Background()
			p, s, ship, mirror := stillRunningDelegation(t, sc.st(t))
			if _, err := s.Decide(ctx, bound(s, thread.Approve(mirror.CallID))); err != nil {
				t.Fatalf("parent Decide = %v", err)
			}
			if err := p.Decide(ctx, s); err != nil { // the pump: the child refuses the replay
				t.Fatalf("Pool.Decide (pump) = %v", err)
			}
			assertParkedOnTheMirror(t, s, mirror)
			for _, e := range s.Entries() {
				if me, ok := e.(thread.MessageEntry); ok && strings.Contains(me.Message.Text(), "SUBAGENT_FAILED") {
					t.Fatalf("the parent's model read %q", me.Message.Text())
				}
			}
			if invoked, _ := ship.counts(); invoked != 1 {
				t.Fatalf("ship ran %d times while its first handler ran", invoked)
			}
			close(ship.release) // the handler returns: the pool replays
			waitReceipt(t, s, pool.Done)
			if invoked, most := ship.counts(); invoked != 2 || most != 1 {
				t.Fatalf("ship ran %d times, at most %d at once", invoked, most)
			}
		})
	}
}

// The choke point, pool replay: a child parked on an ordinary request
// and an undecided recovery request. The parent's approval of the
// ordinary one is replayed into the child without resuming it — the
// resume would settle the recovery request "DENIED: no decision" — and
// the delegation stays parked until a person decides the recovery
// request; the skip then reads OUTCOME_UNKNOWN in the child.
func TestPoolReplayNeverSettlesAnUndecidedRecoveryRequest(t *testing.T) {
	for _, sc := range poolStores(t) {
		t.Run(sc.name, func(t *testing.T) {
			ctx := context.Background()
			var shipped atomic.Int32
			ship := core.Tool("ship", "Ship.", func(context.Context, struct{}) (string, error) {
				shipped.Add(1)
				time.Sleep(300 * time.Millisecond)
				return "shipped", nil
			}, core.Replay(core.ReplayNever), core.Timeout(100*time.Millisecond))
			ask := core.Tool("ask", "Ask.", func(context.Context, struct{}) (string, error) {
				return "asked", nil
			}, core.RequireApproval())
			child := core.New(wefttest.Script(
				wefttest.ToolCalls(
					wefttest.Call{Name: "ask", ID: "a1", Args: `{}`},
					wefttest.Call{Name: "ship", ID: "s1", Args: `{}`},
				),
				wefttest.Say("child done"),
			), ship, ask)
			p := pool.New(4)
			defer func() { _ = p.Close(ctx) }()
			parent := core.New(wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "worker", ID: "d1", Args: wefttest.Args(struct{ Prompt string }{"go"})}),
				wefttest.Say("parent done"),
			), p.MustWrap("worker", "w", child))
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
			var askM, shipM thread.Request
			for _, r := range s.Pending() {
				switch {
				case strings.HasSuffix(r.CallID, "/a1"):
					askM = r
				case strings.HasSuffix(r.CallID, "/s1") && r.Recovery:
					shipM = r
				}
			}
			if askM.ID == "" || shipM.ID == "" {
				t.Fatalf("parent Pending = %+v, want both child requests mirrored", s.Pending())
			}
			if err := p.Decide(ctx, s, bound(s, thread.Approve(askM.CallID))); err != nil {
				t.Fatalf("Pool.Decide(ask) = %v", err)
			}
			assertParkedOnTheMirror(t, s, shipM)
			if pend := s.Pending(); len(pend) != 1 || pend[0].ID != shipM.ID {
				t.Fatalf("parent Pending = %+v, want the recovery mirror alone", pend)
			}
			if err := p.Decide(ctx, s, bound(s, thread.Deny(shipM.CallID, "skip"))); err != nil {
				t.Fatalf("Pool.Decide(skip) = %v", err)
			}
			rc := waitReceipt(t, s, pool.Done)
			cs, err := thread.Open(ctx, s.Storage(), rc.Child, core.New(wefttest.Script()))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = cs.Close(ctx) }()
			var got string
			for _, m := range cs.Context() {
				for _, part := range m.Content {
					if r, ok := part.(core.ToolResultPart); ok && r.CallID == "s1" {
						got = r.Content
					}
				}
			}
			if strings.Contains(got, "DENIED") || !strings.Contains(got, core.CodeOutcomeUnknown) {
				t.Fatalf("the child's s1 result = %q, want the skipped OUTCOME_UNKNOWN text", got)
			}
			if n := shipped.Load(); n != 1 {
				t.Fatalf("ship ran %d times", n)
			}
		})
	}
}

// P1-1, the pool path: Pool.Close under a shutdown cuts a child's turn
// off while its handler ignores the cancellation and keeps running.
// In the same process the parent is reopened and a new pool recovers
// it: the child's call parks for a person as still running — no keyed
// retry beside the first handler — mirrored on the parent; Pool.Decide
// approving it is refused with thread.ErrStillRunning until the
// handler returned, then the retry runs alone and the delegation ends
// done.
func TestPoolCloseGiveUpThenRecoverWaitsForTheStillRunningHandler(t *testing.T) {
	for _, sc := range poolStores(t) {
		t.Run(sc.name, func(t *testing.T) {
			ctx := context.Background()
			var mu sync.Mutex
			invoked, active, most := 0, 0, 0
			release := make(chan struct{})
			released := false
			defer func() {
				if !released {
					close(release)
				}
			}()
			started := make(chan struct{}, 4)
			ship := func() *core.ToolDef {
				return core.Tool("ship", "Ship.", func(context.Context, struct{}) (string, error) {
					mu.Lock()
					invoked++
					first := invoked == 1
					active++
					most = max(most, active)
					mu.Unlock()
					started <- struct{}{}
					defer func() { mu.Lock(); active--; mu.Unlock() }()
					if first {
						<-release // ignores its context
					}
					return "shipped", nil
				}, core.Replay(core.ReplayKeyed), core.Timeout(time.Hour))
			}
			counts := func() (int, int) { mu.Lock(); defer mu.Unlock(); return invoked, most }
			st := sc.st(t)
			parent, err := thread.Create(ctx, st, core.New(wefttest.Script()))
			if err != nil {
				t.Fatal(err)
			}
			p := pool.New(1)
			r, err := p.Submit(ctx, parent, core.New(wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{}`}),
			), ship()), "go")
			if err != nil {
				t.Fatal(err)
			}
			<-started
			cctx, cancel := context.WithCancelCause(ctx)
			cancel(thread.ErrShutdown)
			_ = p.Close(cctx) // gives up at once: the handler ignores the cancellation
			if err := parent.Close(ctx); err != nil {
				t.Fatal(err)
			}

			// The same process: the parent reopened on the same store,
			// a new pool recovers it.
			parent, err = thread.Open(ctx, st, parent.ID(), core.New(wefttest.Script()))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = parent.Close(ctx) }()
			p2 := pool.New(1)
			defer func() { _ = p2.Close(ctx) }()
			if err := p2.Register(r.Child, core.New(wefttest.Script(wefttest.Say("child done")), ship())); err != nil {
				t.Fatal(err)
			}
			// The closed pool finishes the child's Close once its cut-off
			// landed: its hold on the storage is released.
			deadline := time.Now().Add(5 * time.Second)
			for {
				cs, err := thread.Open(ctx, st, r.Child, core.New(wefttest.Script()))
				if err != nil {
					t.Fatal(err)
				}
				err = cs.Claim(ctx)
				_ = cs.Close(ctx)
				if err == nil {
					break
				}
				// ErrStale: the old child's cut-off entry landed after Open.
				if !errors.Is(err, thread.ErrLocked) && !errors.Is(err, thread.ErrStale) || time.Now().After(deadline) {
					t.Fatalf("the cut-off child's hold was not released: %v", err)
				}
				time.Sleep(5 * time.Millisecond)
			}
			if err := p2.Recover(ctx, parent); err != nil {
				t.Fatalf("Recover = %v", err)
			}
			for len(parent.Pending()) == 0 && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			pend := parent.Pending()
			if len(pend) != 1 || !pend[0].Recovery || pend[0].Child != r.Child || !strings.Contains(pend[0].Reason, "still running") {
				t.Fatalf("parent Pending = %+v, want the child's still-running recovery request mirrored", pend)
			}
			mirror := pend[0]
			if inv, _ := counts(); inv != 1 {
				t.Fatalf("ship ran %d times while its first handler ran: the keyed retry did not wait", inv)
			}
			if err := p2.Decide(ctx, parent, bound(parent, thread.Approve(mirror.CallID))); !errors.Is(err, thread.ErrStillRunning) {
				t.Fatalf("Pool.Decide while the handler runs = %v, want ErrStillRunning", err)
			}
			close(release)
			released = true
			deadline = time.Now().Add(5 * time.Second)
			for {
				err = p2.Decide(ctx, parent, bound(parent, thread.Approve(mirror.CallID)))
				if !errors.Is(err, thread.ErrStillRunning) || time.Now().After(deadline) {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			if err != nil {
				t.Fatalf("Pool.Decide after the handler returned = %v", err)
			}
			waitReceipt(t, parent, pool.Done)
			if inv, m := counts(); inv != 2 || m != 1 {
				t.Fatalf("ship ran %d times, at most %d at once; want the retry after the first returned", inv, m)
			}
		})
	}
}
