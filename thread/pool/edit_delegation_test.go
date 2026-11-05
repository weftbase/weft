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

// A1: a cut-off turn's delegating call whose thread/pool delegation is
// still open takes no approval (ErrStillRunning) — and no Edit either:
// an edit approves its revision, and its edited arguments would
// delegate a second time beside the first under another key, where no
// downstream dedupe could catch it. Nothing delegates d1 again. All
// three stores.
func TestEditOfAnOpenDelegationIsRefused(t *testing.T) {
	for _, sc := range poolStores(t) {
		t.Run(sc.name, func(t *testing.T) { editOpenDelegation(t, sc.st(t)) })
	}
}

func editOpenDelegation(t *testing.T, st thread.Storage) {
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
	cctx, cc := context.WithCancelCause(ctx)
	cc(thread.ErrShutdown)
	_ = s.Close(cctx) // gives up: the child's handler ignores the cancellation
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
	recoverTurn()

	// Before the pool takes the cut-off child up, the delegation is open
	// and d1 is offered as the parent's own request: no approval, no
	// edit — through the pool or the session — and nothing written.
	pend := s2.Pending()
	for _, r := range pend {
		if r.CallID == "d1" && r.Recovery {
			t.Fatalf("d1 offered as a recovery request beside its live delegation: %+v", r)
		}
	}
	if len(pend) != 1 || pend[0].CallID != "d1" || pend[0].Child != "" || !strings.Contains(pend[0].Reason, "delegation is still open") {
		t.Fatalf("Pending = %+v, want d1 alone, its delegation open", pend)
	}
	before := len(s2.Entries())
	ap := bound(s2, thread.Approve("d1"))
	if err := p.Decide(ctx, s2, ap); !errors.Is(err, thread.ErrStillRunning) {
		t.Fatalf("approve d1 while its delegation is open = %v, want ErrStillRunning", err)
	}
	ed := bound(s2, thread.Edit("d1", []byte(`{"Prompt":"go again"}`)))
	if err := p.Decide(ctx, s2, ed); !errors.Is(err, thread.ErrStillRunning) {
		t.Fatalf("edit d1 while its delegation is open = %v, want ErrStillRunning", err)
	}
	if _, err := s2.Decide(ctx, ed); !errors.Is(err, thread.ErrStillRunning) {
		t.Fatalf("Session.Decide(Edit) of d1 while its delegation is open = %v, want ErrStillRunning", err)
	}
	if got := len(s2.Entries()); got != before {
		t.Fatalf("the refused decisions wrote %d entries", got-before)
	}

	// The pool takes the cut-off child up (asynchronously) and mirrors
	// its running call: d1 now completes through its child, and a
	// decision of its own is ErrDelegated.
	if err := p.Recover(ctx, s2); err != nil {
		t.Fatalf("pool Recover = %v", err)
	}
	waitMirror(t, s2)
	if _, err := s2.Decide(ctx, ap); !errors.Is(err, thread.ErrDelegated) {
		t.Fatalf("approve d1 once its child is mirrored = %v, want ErrDelegated", err)
	}
	if _, err := s2.Decide(ctx, ed); !errors.Is(err, thread.ErrDelegated) {
		t.Fatalf("edit d1 once its child is mirrored = %v, want ErrDelegated", err)
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
		t.Errorf("d1 delegated %d times (receipts %+v), want once", accepted, rs)
	}
	if m := mostAtOnce(); m > 1 {
		t.Errorf("%d ship handlers ran at once", m)
	}
}
