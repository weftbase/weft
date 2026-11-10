package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
	"github.com/weftbase/weft/thread"
)

// orphanReceipt writes what a dead process leaves in parent's ledger
// for child: an acceptance and a running receipt, no settlement, no
// mirror — and no delegate in any pool.
func orphanReceipt(t *testing.T, parent, child *thread.Session) string {
	t.Helper()
	ctx := context.Background()
	acc, err := parent.AppendPoolReceipt(ctx, thread.PoolReceiptEntry{Status: thread.PoolAccepted, Child: child.ID(), Prompt: "go"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parent.AppendPoolReceipt(ctx, thread.PoolReceiptEntry{Receipt: acc.ID, Status: thread.PoolRunning, Child: child.ID()}); err != nil {
		t.Fatal(err)
	}
	return acc.ID
}

// together runs the functions at once and waits for them.
func together(fns ...func()) {
	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, fn := range fns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			fn()
		}()
	}
	close(start)
	wg.Wait()
}

// A receipt left unsettled by a dead process is settled once, whoever
// recovers it and however many at once: a Recover beside a Cancel of
// the same orphan (which recovers it first), or beside another
// Recover. Session.Usage sums every settlement entry into Delegated,
// so a second one bills the child twice. Before the fix recoverOne
// read the ledger, loaded the child and appended its settlement with
// nothing between two callers: both wrote one.
func TestConcurrentRecoveriesSettleAReceiptOnce(t *testing.T) {
	ctx := context.Background()
	for try := 0; try < 100; try++ {
		st := thread.Memory()
		parent, err := thread.Create(ctx, st, core.New(wefttest.Script()))
		if err != nil {
			t.Fatal(err)
		}
		// A child whose run finished; only its settlement was lost.
		child, err := thread.Create(ctx, st, core.New(wefttest.Script(wefttest.Say("child done"))), thread.WithLineage(parent.ID(), ""))
		if err != nil {
			t.Fatal(err)
		}
		turn, err := child.Send(ctx, core.User("go"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := turn.Wait(); err != nil {
			t.Fatal(err)
		}
		if err := child.Close(ctx); err != nil {
			t.Fatal(err)
		}
		receipt := orphanReceipt(t, parent, child)
		p := New(2)
		together(
			func() { _ = p.Recover(ctx, parent) },
			func() { _ = p.Cancel(ctx, parent, receipt) },
			func() { _ = p.Recover(ctx, parent) },
		)
		settlements := 0
		for _, e := range parent.Entries() {
			if pr, ok := e.(thread.PoolReceiptEntry); ok && pr.Receipt == receipt && State(pr.Status).Settled() {
				settlements++
			}
		}
		if rc, _ := lookup(parent, receipt); rc.State != Done || rc.Stop != "child done" {
			t.Fatalf("try %d: the recovered receipt = %+v, want done with the child's answer", try, rc)
		}
		if settlements != 1 {
			t.Fatalf("try %d: %d settlement entries for one receipt, want 1 (Delegated sums each)", try, settlements)
		}
		_ = p.Close(ctx)
		_ = parent.Close(ctx)
	}
}

// The same for a child found parked, its mirror lost in the crash
// window between its park and the mirror: two recoveries at once
// reattach it once — one mirror per pending request, one parked
// receipt. Before the fix both read the parent's mirrors, found none,
// and each appended the request and the parked receipt.
func TestConcurrentRecoveriesReattachAParkedChildOnce(t *testing.T) {
	ctx := context.Background()
	refund := core.Tool("refund", "", func(context.Context, struct{}) (string, error) { return "refunded", nil }, core.RequireApproval())
	gated := func() *core.Agent {
		return core.New(wefttest.Script(
			wefttest.ToolCalls(wefttest.Call{Name: "refund", ID: "r1", Args: `{}`}),
			wefttest.Say("child done")), refund)
	}
	for try := 0; try < 100; try++ {
		st := thread.Memory()
		parent, err := thread.Create(ctx, st, core.New(wefttest.Script()))
		if err != nil {
			t.Fatal(err)
		}
		child, err := thread.Create(ctx, st, gated(), thread.WithLineage(parent.ID(), ""))
		if err != nil {
			t.Fatal(err)
		}
		turn, err := child.Send(ctx, core.User("go"))
		if err != nil {
			t.Fatal(err)
		}
		if res, err := turn.Wait(); err != nil || len(res.Pending) != 1 {
			t.Fatalf("the child did not park: %v", err)
		}
		if err := child.Close(ctx); err != nil {
			t.Fatal(err)
		}
		receipt := orphanReceipt(t, parent, child)
		p := New(2)
		if err := p.Register(child.ID(), gated()); err != nil {
			t.Fatal(err)
		}
		together(
			func() { _ = p.Recover(ctx, parent) },
			func() { _ = p.Recover(ctx, parent) },
			func() { _ = p.Recover(ctx, parent) },
		)
		mirrors, parked := 0, 0
		for _, e := range parent.Entries() {
			switch e := e.(type) {
			case thread.ApprovalRequestEntry:
				if e.Child == child.ID() {
					mirrors++
				}
			case thread.PoolReceiptEntry:
				if e.Receipt == receipt && e.Status == thread.PoolParked {
					parked++
				}
			}
		}
		if mirrors != 1 || parked != 1 {
			t.Fatalf("try %d: %d mirrors and %d parked receipts for one parked child, want 1 and 1", try, mirrors, parked)
		}
		if n := len(parent.Pending()); n != 1 {
			t.Fatalf("try %d: parent Pending = %d, want the child's one request", try, n)
		}
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if err := p.Cancel(cctx, parent, receipt); err != nil {
			t.Errorf("try %d: Cancel of the reattached child: %v", try, err)
		}
		cancel()
		_ = p.Close(ctx)
		_ = parent.Close(ctx)
	}
}

// A resume canceled while it queues for a slot closes the child's
// boundary: the child parked, its request was approved on the parent,
// and the Cancel landed before the resume replayed that approval into
// the child. Cancel's promise for a parked child — its parked calls
// can never run on a later approval — holds there too: the calls are
// denied in the child's own session. Before the fix the delegation
// settled canceled with the child's request still pending in its file,
// approvable by whoever opened the session.
func TestCancelOfAQueuedResumeClosesTheChildBoundary(t *testing.T) {
	ctx := context.Background()
	st := thread.Memory()
	ran := make(chan struct{}, 1)
	refund := core.Tool("refund", "", func(context.Context, struct{}) (string, error) {
		ran <- struct{}{}
		return "refunded", nil
	}, core.RequireApproval())
	gated := func() *core.Agent {
		return core.New(wefttest.Script(
			wefttest.ToolCalls(wefttest.Call{Name: "refund", ID: "r1", Args: `{}`}),
			wefttest.Say("child done")), refund)
	}
	holding, release := make(chan struct{}), make(chan struct{})
	hold := core.Tool("hold", "", func(ctx context.Context, _ struct{}) (string, error) {
		close(holding)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return "held", nil
	})
	blocker := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "hold", ID: "h1", Args: `{}`}),
		wefttest.Say("blocker done")), hold, core.Timeout(0))

	p := New(1) // one slot: the blocker holds it while the resume queues
	defer func() { _ = p.Close(ctx) }()
	parent, err := thread.Create(ctx, st, core.New(wefttest.Script()))
	if err != nil {
		t.Fatal(err)
	}
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	r, err := p.Submit(ctx, parent, gated(), "refund")
	if err != nil {
		t.Fatal(err)
	}
	if rc, err := p.Wait(wctx, parent, r.ID); err != nil || rc.State != Parked {
		t.Fatalf("the child did not park: %+v, %v", rc, err)
	}
	if _, err := p.Submit(ctx, parent, blocker, "hold"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-holding:
	case <-wctx.Done():
		t.Fatal("the blocker never took the slot")
	}
	pend := parent.Pending()
	if len(pend) != 1 {
		t.Fatalf("parent Pending = %d, want the child's request", len(pend))
	}
	if err := p.Decide(wctx, parent, thread.Approve("").Bind(pend[0])); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	phase := p.delegates[r.ID].phase
	p.mu.Unlock()
	if phase != phaseQueued {
		t.Fatalf("the approved child's phase = %d, want queued behind the blocker", phase)
	}
	if err := p.Cancel(wctx, parent, r.ID); err != nil {
		t.Fatal(err)
	}
	if rc, err := p.Wait(wctx, parent, r.ID); err != nil || rc.State != Canceled {
		t.Fatalf("the canceled resume's receipt = %+v, %v; want canceled", rc, err)
	}
	// The child's own file: nothing left to approve.
	child, err := thread.Open(ctx, st, r.Child, gated())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = child.Close(ctx) }()
	if left := child.Pending(); len(left) != 0 {
		t.Errorf("the canceled child still offers %d request(s) in its own session: %+v", len(left), left)
	}
	close(release)
	select {
	case <-ran:
		t.Error("the canceled child's approved call ran")
	case <-time.After(100 * time.Millisecond):
	}
}

// waitingWrapper builds a parent whose ledger holds a child's mirror
// under the delegating call "w1" and no turn entry after it — the
// parent's step still running — and the delegate a resume of that
// child waits on.
func waitingWrapper(t *testing.T) (*Pool, *thread.Session, *delegate) {
	t.Helper()
	ctx := context.Background()
	st := thread.Memory()
	parent, err := thread.Create(ctx, st, core.New(wefttest.Script(wefttest.Say("landed"))))
	if err != nil {
		t.Fatal(err)
	}
	child, err := thread.Create(ctx, st, core.New(wefttest.Script()), thread.WithLineage(parent.ID(), "w1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parent.AppendApprovalRequests(ctx, thread.ApprovalRequestEntry{
		CallID: mirrorCallID(child.ID(), "c1"), Tool: "refund", Child: child.ID(), Wrapper: "w1", RunID: child.ID() + "-t1",
	}); err != nil {
		t.Fatal(err)
	}
	p := New(1)
	t.Cleanup(func() { _ = p.Close(ctx) })
	return p, parent, &delegate{parent: parent, child: child, wrapper: "w1"}
}

// land ends a turn of the waiting parent: its mirror denied (the
// boundary cleared), a turn sent and waited for.
func land(t *testing.T, parent *thread.Session, child string) {
	t.Helper()
	ctx := context.Background()
	if err := parent.DenyMirrored(ctx, child, "test: clear the boundary"); err != nil {
		t.Fatal(err)
	}
	turn, err := parent.Send(ctx, core.User("go"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := turn.Wait(); err != nil {
		t.Fatal(err)
	}
}

// A resume waiting for its delegating call to park reads the parent's
// tree once and then waits on the session's notification of its next
// turn entry: nothing is read — nothing copied — while the parent's
// step runs on, however long. It used to poll: Session.Entries, a deep
// copy of the whole ledger under the session's lock, up to a hundred
// times a second for the length of a sibling call.
func TestAwaitWrapperReadsNothingWhileItWaits(t *testing.T) {
	p, parent, d := waitingWrapper(t)
	var reads atomic.Int32
	p.testHookWrapperRead = func(*delegate) { reads.Add(1) }
	done := make(chan bool, 1)
	go func() { done <- p.awaitWrapper(context.Background(), d) }()
	time.Sleep(time.Second) // the parent's step, still running
	if n := reads.Load(); n != 1 {
		t.Errorf("the idle wait read the parent's tree %d times in a second, want once", n)
	}
	land(t, parent, d.child.ID())
	select {
	case landed := <-done:
		if !landed {
			t.Error("awaitWrapper returned without the landing")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("awaitWrapper never saw the parent's turn land")
	}
	if n := reads.Load(); n != 2 {
		t.Errorf("the wait read the parent's tree %d times in all, want twice: at its start and at the landing", n)
	}
}

// A turn that lands late — after a sibling call that ran long — is
// seen when it lands, by the session's notification, not at the next
// tick of a timer (the backstop re-check is wrapperRecheck away); and a
// canceled wait returns at its context's end.
func TestAwaitWrapperSeesALateLandingAtOnce(t *testing.T) {
	p, parent, d := waitingWrapper(t)
	ctx := context.Background()

	cctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	start := time.Now()
	if p.awaitWrapper(cctx, d) {
		t.Fatal("awaitWrapper saw a landing no turn made")
	}
	cancel()
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("a canceled wait returned after %v, want at its context's end", took)
	}

	landed := make(chan time.Time, 1)
	go func() {
		p.awaitWrapper(ctx, d)
		landed <- time.Now()
	}()
	time.Sleep(300 * time.Millisecond) // well into the wait
	land(t, parent, d.child.ID())
	sent := time.Now()
	select {
	case at := <-landed:
		if late := at.Sub(sent); late > 2*time.Second {
			t.Errorf("the landing was seen %v after it happened, want at once (the backstop is %v)", late, wrapperRecheck)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("awaitWrapper did not see the parent's turn land; the backstop is %v away", wrapperRecheck)
	}
}

// A Wait on a receipt whose child is still being created waits for the
// creation to end — the pool's own notification — and then follows the
// delegation: it neither returns the bare acceptance nor reads the
// ledger on a timer meanwhile.
func TestWaitDuringCreationFollowsTheDelegation(t *testing.T) {
	ctx := context.Background()
	p := New(1)
	defer func() { _ = p.Close(ctx) }()
	parent, err := thread.Create(ctx, thread.Memory(), core.New(wefttest.Script()))
	if err != nil {
		t.Fatal(err)
	}
	acc, err := parent.AppendPoolReceipt(ctx, thread.PoolReceiptEntry{Status: thread.PoolAccepted, Child: "s_making", Prompt: "go"})
	if err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	p.creating["s_making"]++ // a delegation of this pool is creating it
	p.mu.Unlock()
	got := make(chan Receipt, 1)
	go func() {
		rc, _ := p.Wait(ctx, parent, acc.ID)
		got <- rc
	}()
	select {
	case rc := <-got:
		t.Fatalf("Wait returned %+v while the child was being created", rc)
	case <-time.After(200 * time.Millisecond):
	}
	// The creation ends as submit ends one that failed: settled, then
	// uncounted under the pool's lock.
	if _, err := parent.AppendPoolReceipt(ctx, thread.PoolReceiptEntry{Receipt: acc.ID, Status: thread.PoolFailed, Child: "s_making", Stop: "not created"}); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	delete(p.creating, "s_making")
	close(p.created)
	p.created = nil
	p.mu.Unlock()
	select {
	case rc := <-got:
		if rc.State != Failed {
			t.Errorf("Wait after the creation ended = %+v, want the failed settlement", rc)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait never woke when the creation ended")
	}
}
