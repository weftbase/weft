package pool_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/thread/pool"
)

// cutOffDelegation builds a sync delegation whose child's turn a
// shutdown close of the parent session cut off in-process — the pool
// stays open — its ship handler still held. It returns the pool, the
// storage, the parent reopened (s2), the receipt, and release (closes
// the held handler; idempotent).
func cutOffDelegation(t *testing.T, st thread.Storage) (*pool.Pool, *thread.Session, pool.Receipt, func()) {
	t.Helper()
	ctx := context.Background()
	hold := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(hold) }) }
	t.Cleanup(release)
	started := make(chan struct{}, 8)
	var mu sync.Mutex
	first := true
	ship := core.Tool("ship", "Ship.", func(context.Context, struct{}) (string, error) {
		mu.Lock()
		h := first
		first = false
		mu.Unlock()
		started <- struct{}{}
		if h {
			<-hold
		}
		return "shipped", nil
	}, core.Replay(core.ReplayKeyed), core.Timeout(time.Hour))
	p := pool.New(4)
	t.Cleanup(func() {
		release()
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_ = p.Close(cctx)
	})
	child := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{}`}),
		wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{}`}),
		wefttest.Say("child done"), wefttest.Say("child done"),
	), ship)
	s, err := thread.Create(ctx, st, core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "worker", ID: "d1", Args: wefttest.Args(struct{ Prompt string }{"go"})}),
		wefttest.Say("parent done"),
	), p.MustWrap("worker", "w", child)))
	if err != nil {
		t.Fatal(err)
	}
	turn, err := s.Send(ctx, core.User("go"))
	if err != nil {
		t.Fatal(err)
	}
	<-started
	c, cc := context.WithCancelCause(ctx)
	cc(thread.ErrShutdown)
	_ = s.Close(c)
	_, _ = turn.Wait()
	_ = s.Close(ctx)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if rs := pool.Receipts(s); len(rs) == 1 {
			if _, es, _, err := st.Load(ctx, rs[0].Child); err == nil && turnLanded(es) {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("the child's cut-off turn never landed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	s2, err := thread.Open(ctx, st, s.ID(), core.New(wefttest.Script(wefttest.Say("parent done"), wefttest.Say("parent done")),
		p.MustWrap("worker", "w", child)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close(ctx) })
	rs := pool.Receipts(s2)
	if len(rs) != 1 || rs[0].Settled() {
		t.Fatalf("receipts after the cut-off = %+v, want one unsettled", rs)
	}
	return p, s2, rs[0], release
}

// A child whose run a shutdown cut off while the pool stays open is let
// go by the pool: a Recover of the reopened parent recovers it (it is
// not skipped as live), Wait then waits for it to rest instead of
// returning the stale running receipt, and once its abandoned handler
// returns and its requests are decided it settles, billed in Delegated.
func TestCutOffDelegateIsRecoveredInProcess(t *testing.T) {
	for _, sc := range poolStores(t) {
		t.Run(sc.name, func(t *testing.T) {
			ctx := context.Background()
			p, s2, rc, release := cutOffDelegation(t, sc.st(t))
			if err := p.Recover(ctx, s2); err != nil {
				t.Fatalf("Recover: %v", err)
			}
			wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			got, err := p.Wait(wctx, s2, rc.ID)
			if err != nil {
				t.Fatalf("Wait: %v", err)
			}
			if got.State == pool.Running || got.State == pool.Accepted {
				t.Fatalf("Wait returned %s: the recovered delegation is not at rest", got.State)
			}
			release()
			// Decide whatever the recovery offered (the still-running
			// call's request, the parent's own recovery), until settled.
			deadline := time.Now().Add(15 * time.Second)
			for {
				r := pool.Receipts(s2)[0]
				if r.Settled() {
					if r.State != pool.Done {
						t.Fatalf("settled %s (%s), want done", r.State, r.Stop)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("receipt never settled: %+v", r)
				}
				for _, pr := range s2.Pending() {
					if pr.Child != "" {
						_ = p.Decide(ctx, s2, bound(s2, thread.Deny(pr.CallID, "skip")))
					}
				}
				_ = p.Recover(ctx, s2)
				time.Sleep(20 * time.Millisecond)
			}
			if u := s2.Usage().Delegated; u == (core.Usage{}) {
				t.Errorf("Delegated = %+v, want the recovered child's usage", u)
			}
		})
	}
}

// Cancel of a cut-off child's receipt, the pool still open, recovers it
// and cancels it — it does not return nil over a delegate whose run is
// over — and Wait reads the cancellation.
func TestCutOffDelegateCancelInProcess(t *testing.T) {
	for _, sc := range poolStores(t) {
		t.Run(sc.name, func(t *testing.T) {
			ctx := context.Background()
			p, s2, rc, release := cutOffDelegation(t, sc.st(t))
			defer release()
			if err := p.Cancel(ctx, s2, rc.ID); err != nil {
				t.Fatalf("Cancel: %v", err)
			}
			wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			got, err := p.Wait(wctx, s2, rc.ID)
			if err != nil {
				t.Fatalf("Wait: %v", err)
			}
			if got.State != pool.Canceled {
				t.Fatalf("after Cancel the receipt reads %s (%s), want canceled", got.State, got.Stop)
			}
		})
	}
}
