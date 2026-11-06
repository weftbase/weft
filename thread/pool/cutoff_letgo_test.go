package pool_test

import (
	"context"
	"errors"
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

// A cut-off sync delegation whose child recovers on its own without
// parking — its ReplayKeyed call retried under the same key — resolves
// the parent's delegating call with the child's answer when its receipt
// settles, whichever of pool.Recover and the parent's RecoverTurn runs
// first. Before 0.24.1 ResolveDelegation accepted only a call a mirror
// named: the pool logged "delegating call no longer pending", the
// parent kept offering the call as a plain pending one, a second
// Recover did not fix it, and approving it delegated a second time.
func TestCutOffDelegateRecoveredUnparkedResolvesTheParentCall(t *testing.T) {
	for _, order := range []string{"pool first", "session first"} {
		for _, sc := range poolStores(t) {
			t.Run(order+"/"+sc.name, func(t *testing.T) {
				ctx := context.Background()
				p, s2, rc, release := cutOffDelegation(t, sc.st(t))
				release() // the abandoned handler returns: the child's recovery retries by key, no park
				recoverTurn := func() {
					if _, err := s2.RecoverTurn(ctx); err != nil {
						t.Fatalf("RecoverTurn: %v", err)
					}
				}
				if order == "session first" {
					recoverTurn()
				}
				if err := p.Recover(ctx, s2); err != nil {
					t.Fatalf("Recover: %v", err)
				}
				if order == "pool first" {
					recoverTurn()
				}
				deadline := time.Now().Add(15 * time.Second)
				for {
					r := pool.Receipts(s2)
					settled := len(r) == 1 && r[0].Settled()
					if settled && len(s2.Pending()) == 0 && lastText(s2) == "parent done" {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("not resolved: receipts %+v, pending %+v, last %q", r, s2.Pending(), lastText(s2))
					}
					time.Sleep(10 * time.Millisecond)
				}
				if err := p.Recover(ctx, s2); err != nil {
					t.Fatalf("a second Recover: %v", err)
				}
				rs := pool.Receipts(s2)
				if len(rs) != 1 || rs[0].ID != rc.ID || rs[0].State != pool.Done {
					t.Errorf("receipts = %+v, want the one delegation done: never delegated again", rs)
				}
				if got := toolResultsOf(s2.Context()); len(got) != 1 || got[0] != "child done" {
					t.Errorf("the parent's model read %q, want the child's answer", got)
				}
			})
		}
	}
}

// A settlement whose resolution was lost (a crash between the two) of a
// delegating call left dangling with no mirror: Recover resolves the
// call from the receipt.
func TestRecoverResolvesADanglingCallFromItsSettledReceipt(t *testing.T) {
	for _, sc := range poolStores(t) {
		t.Run(sc.name, func(t *testing.T) {
			ctx := context.Background()
			p, s2, rc, release := cutOffDelegation(t, sc.st(t))
			defer release()
			if _, err := s2.AppendPoolReceipt(ctx, thread.PoolReceiptEntry{
				Receipt: rc.ID, Status: thread.PoolDone, Child: rc.Child, Stop: "child done",
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := s2.RecoverTurn(ctx); err != nil {
				t.Fatalf("RecoverTurn: %v", err)
			}
			// Until the pool resolves it the call is offered with the
			// settled reason, and an approval — a second delegation — is
			// refused.
			pend := s2.Pending()
			if len(pend) != 1 || pend[0].Reason != "delegated: its thread/pool delegation settled and its answer is on the receipt; the pool resolves the call with it (pool.Recover), and an approval, which would delegate again, is refused" {
				t.Fatalf("pending = %+v, want the call offered with the settled reason", pend)
			}
			if _, err := s2.Decide(ctx, thread.Approve("").Bind(pend[0])); !errors.Is(err, thread.ErrDelegated) {
				t.Fatalf("Approve of the settled delegation's call = %v, want ErrDelegated", err)
			}
			if err := p.Recover(ctx, s2); err != nil {
				t.Fatalf("Recover: %v", err)
			}
			wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			if err := s2.WaitIdle(wctx); err != nil {
				t.Fatal(err)
			}
			if len(s2.Pending()) != 0 || lastText(s2) != "parent done" {
				t.Fatalf("pending %+v, last %q: want the call resolved from the receipt", s2.Pending(), lastText(s2))
			}
			if got := toolResultsOf(s2.Context()); len(got) != 1 || got[0] != "child done" {
				t.Errorf("the parent's model read %q, want the receipt's answer", got)
			}
		})
	}
}
