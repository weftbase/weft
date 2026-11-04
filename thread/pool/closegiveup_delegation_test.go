package pool_test

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
	"github.com/weftbase/weft/thread/pool"
)

// A sync delegation whose parent's Close gives up — an eviction's
// bounded Close, or a shutdown's — while the child's handler ignores
// its context. The same pool stays up; the parent is reopened in the
// same process and recovered: RecoverTurn and the pool's Recover, in
// either order. The delegating call d1 must never be offered as a
// retry while its delegation may still be live (approving it beside
// the child's still-running request delegated twice): RecoverTurn reads
// the pool's acceptance receipt, not only a mirror, and leaves the call
// to the pool. Both orders end the same.
//
//   - evict: the pool takes the delegation up (the child's call
//     mirrored, still running); d1 is not offered.
//   - shutdown: the pool still holds the ending delegate and takes
//     nothing up; d1 stays offered as its boundary's dangling call,
//     saying its delegation is open, and an approval of it is refused
//     with ErrStillRunning.
//
// The child model's script is shared by every run of the child, so a
// second "ship" can come from it; the delegation receipts are what
// say whether the call delegated twice.
func TestCloseGiveUpSyncDelegationIsNotOfferedAsARetry(t *testing.T) {
	for _, sc := range poolStores(t) {
		for _, shutdown := range []bool{false, true} {
			for _, poolFirst := range []bool{false, true} {
				name := sc.name + "/evict"
				if shutdown {
					name = sc.name + "/shutdown"
				}
				if poolFirst {
					name += "/pool-first"
				} else {
					name += "/recoverturn-first"
				}
				t.Run(name, func(t *testing.T) {
					closeGiveUpDelegation(t, sc.st(t), shutdown, poolFirst)
				})
			}
		}
	}
}

func closeGiveUpDelegation(t *testing.T, st thread.Storage, shutdown, poolFirst bool) {
	ctx := context.Background()
	var mu sync.Mutex
	active, most := 0, 0
	release := make(chan struct{})
	started := make(chan struct{}, 8)
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	first := true
	ship := core.Tool("ship", "Ship.", func(context.Context, struct{}) (string, error) {
		mu.Lock()
		hold := first
		first = false
		active++
		most = max(most, active)
		mu.Unlock()
		started <- struct{}{}
		defer func() { mu.Lock(); active--; mu.Unlock() }()
		if hold {
			<-release // ignores its context
		}
		return "shipped", nil
	}, core.Replay(core.ReplayKeyed), core.Timeout(time.Hour))
	mostAtOnce := func() int { mu.Lock(); defer mu.Unlock(); return most }
	p := pool.New(4)
	defer func() {
		// Bounded: the evicted child, parked on its mirror, is ended by
		// a drain whose backstop is long (nestedEndWait).
		cctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		_ = p.Close(cctx)
	}()
	child := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{}`}),
		wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{}`}),
		wefttest.Say("child done"),
		wefttest.Say("child done"),
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
	cctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	if shutdown {
		c, cc := context.WithCancelCause(ctx)
		cc(thread.ErrShutdown)
		cctx, cancel = c, func() {}
	}
	_ = s.Close(cctx) // gives up: the child's handler ignores the cancellation
	cancel()
	_, _ = turn.Wait()
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	// The cut-off child's turn lands once its own Close gave up: wait
	// for the record before the parent is recovered.
	deadline := time.Now().Add(5 * time.Second)
	for {
		rs := pool.Receipts(s)
		if len(rs) == 1 {
			h, es, _, err := st.Load(ctx, rs[0].Child)
			if err == nil && h.ID != "" && turnLanded(es) {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("the cut-off child's turn never landed")
		}
		time.Sleep(5 * time.Millisecond)
	}

	s2, err := thread.Open(ctx, st, s.ID(), core.New(wefttest.Script(wefttest.Say("parent done"), wefttest.Say("parent done")),
		p.MustWrap("worker", "w", child)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close(ctx) }()
	recoverTurn := func() {
		if rt, err := s2.RecoverTurn(ctx); err != nil || rt != nil {
			t.Fatalf("RecoverTurn = %v, %v; want the boundary left waiting", rt, err)
		}
	}
	if poolFirst {
		if err := p.Recover(ctx, s2); err != nil {
			t.Fatalf("pool Recover = %v", err)
		}
		recoverTurn()
	} else {
		recoverTurn()
		if err := p.Recover(ctx, s2); err != nil {
			t.Fatalf("pool Recover = %v", err)
		}
	}

	pend := s2.Pending()
	for _, r := range pend {
		if r.CallID == "d1" && r.Recovery {
			t.Fatalf("d1 offered as a recovery request beside its live delegation: %+v", r)
		}
	}
	if !shutdown {
		if len(pend) != 1 || pend[0].Child == "" || !pend[0].Recovery || !strings.Contains(pend[0].Reason, "still running") {
			t.Fatalf("Pending = %+v, want only the child's still-running call, mirrored", pend)
		}
	} else if len(pend) != 1 || pend[0].CallID != "d1" || pend[0].Child != "" || !strings.Contains(pend[0].Reason, "delegation is still open") {
		t.Fatalf("Pending = %+v, want d1 alone, its delegation open", pend)
	}
	for _, r := range pend {
		if r.Child != "" {
			if err := p.Decide(ctx, s2, bound(s2, thread.Deny(r.CallID, "skip"))); err != nil {
				t.Fatalf("deny the mirror: %v", err)
			}
			continue
		}
		if err := p.Decide(ctx, s2, bound(s2, thread.Approve(r.CallID))); !errors.Is(err, thread.ErrStillRunning) {
			t.Fatalf("approve %s while its delegation is open = %v, want ErrStillRunning", r.CallID, err)
		}
	}
	close(release)
	released = true
	// Whatever runs once the first handler returned, nothing delegates
	// d1 again: no decision approved it.
	time.Sleep(200 * time.Millisecond)
	accepted := 0
	for _, e := range s2.Entries() {
		if pr, ok := e.(thread.PoolReceiptEntry); ok && pr.Receipt == "" && pr.Call == "d1" {
			accepted++
		}
	}
	if rs := pool.Receipts(s2); accepted != 1 || len(rs) != 1 {
		t.Fatalf("d1 delegated %d times (receipts %+v), want once", accepted, rs)
	}
	if m := mostAtOnce(); m > 1 {
		t.Fatalf("%d ship handlers ran at once", m)
	}
}

// turnLanded reports whether a session's entries hold a turn entry.
func turnLanded(es []thread.Entry) bool {
	for _, e := range es {
		if _, ok := e.(thread.TurnEntry); ok {
			return true
		}
	}
	return false
}
