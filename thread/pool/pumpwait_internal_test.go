package pool

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
	"github.com/weftbase/weft/thread"
)

// P2-2 (release-gate review): every refused pump asks pumpWhenReturned
// to replay once the child's abandoned handler returned. Asked again
// and again while the handler runs, it keeps one waiter per handler,
// not one goroutine per ask; the waiter is gone once the handler
// returned.
func TestPumpWhenReturnedKeepsOneWaiterPerHandler(t *testing.T) {
	ctx := context.Background()
	release := make(chan struct{})
	// ship's handler must start before its Timeout passes, and a
	// Timeout spans the child's dispatch gate: one that passes while
	// the gate is still recording the call (a loaded machine) ends a
	// call that never started — nothing abandoned. Another delegation
	// tries again then, with twice the Timeout.
	var p *Pool
	var s *thread.Session
	for timeout, attempt := 100*time.Millisecond, 1; ; timeout, attempt = 2*timeout, attempt+1 {
		var ran atomic.Int32
		ship := core.Tool("ship", "Ship.", func(context.Context, struct{}) (string, error) {
			if ran.Add(1) == 1 {
				<-release // ignores its Timeout
			}
			return "shipped", nil
		}, core.Replay(core.ReplayNever), core.Timeout(timeout))
		p = New(4)
		child := core.New(wefttest.Script(
			wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{}`}),
			wefttest.Say("child done"),
		), ship)
		parent := core.New(wefttest.Script(
			wefttest.ToolCalls(wefttest.Call{Name: "worker", ID: "d1", Args: wefttest.Args(struct{ Prompt string }{"go"})}),
			wefttest.Say("parent done"),
		), p.MustWrap("worker", "w", child))
		var err error
		s, err = thread.Create(ctx, thread.Memory(), parent)
		if err != nil {
			t.Fatal(err)
		}
		turn, err := s.Send(ctx, core.User("go"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := turn.Wait(); err != nil {
			t.Fatal(err)
		}
		if ran.Load() != 0 {
			break
		}
		_ = p.Close(ctx) // timed out in the gate: never started
		_ = s.Close(ctx)
		t.Logf("the call timed out in the dispatch gate under a Timeout of %v: again, with twice that", timeout)
		if attempt == 8 {
			t.Fatalf("the call timed out in the dispatch gate 8 times running (the last Timeout %v)", timeout)
		}
	}
	released := false
	defer func() {
		if !released {
			close(release)
		}
		_ = p.Close(ctx)
		_ = s.Close(ctx)
	}()
	pend := s.Pending()
	if len(pend) != 1 || pend[0].Child == "" || !strings.Contains(pend[0].Reason, "still running") {
		t.Fatalf("parent Pending = %+v, want the child's still-running recovery request mirrored", pend)
	}
	p.mu.Lock()
	d := p.byChild[pend[0].Child]
	p.mu.Unlock()
	if d == nil {
		t.Fatal("no delegate for the parked child")
	}
	waiters := func() int {
		p.mu.Lock()
		defer p.mu.Unlock()
		return len(p.waiting)
	}
	for range 5 {
		p.pumpWhenReturned(d)
	}
	if n := waiters(); n != 1 {
		t.Fatalf("%d waiters after five refused pumps, want 1", n)
	}
	close(release)
	released = true
	deadline := time.Now().Add(5 * time.Second)
	for waiters() != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := waiters(); n != 0 {
		t.Fatalf("%d waiters after the handler returned, want 0", n)
	}
}
