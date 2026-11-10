package thread_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// A Fork taken while the origin's turn runs a call with effects copies
// the call as cut off; the origin's handler is still running in this
// process, so the fork treats it as still running (review finding,
// effects E1-B): its RecoverTurn parks the call for a person — no keyed
// retry, no Reconcile beside the live handler — and an approval there
// is refused until the handler returns.
func TestForkDuringARunningCallParksItForAPerson(t *testing.T) {
	for _, policy := range []core.ReplayPolicy{core.ReplayKeyed, core.ReplayNever} {
		t.Run(string(policy), func(t *testing.T) {
			var mu sync.Mutex
			var keys []string
			entered := make(chan struct{}, 4)
			release := make(chan struct{})
			var once sync.Once
			let := func() { once.Do(func() { close(release) }) }
			defer let() // a failure never leaves the handler held
			reconciled := 0
			tool := core.Tool("charge", "A side effect.", func(ctx context.Context, _ refundInput) (string, error) {
				c, _ := core.CallFromContext(ctx)
				mu.Lock()
				keys = append(keys, c.IdempotencyKey)
				block := len(keys) == 1
				mu.Unlock()
				entered <- struct{}{}
				if block {
					<-release
				}
				return "charged", nil
			}, core.Replay(policy), core.Reconcile(func(context.Context, string) (bool, string, error) {
				mu.Lock()
				reconciled++
				mu.Unlock()
				return false, "", nil
			}))
			s, err := thread.Create(bg, thread.Memory(), core.New(wefttest.Script(charged("charge"), wefttest.Say("done"), wefttest.Say("done2")), tool))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { let(); _ = s.Close(bg) }()
			turn, err := s.Send(bg, core.User("charge it"))
			if err != nil {
				t.Fatal(err)
			}
			<-entered // the origin's handler is running
			f, err := s.Fork(bg, s.Leaf())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { let(); _ = f.Close(bg) }()
			rt, err := f.RecoverTurn(bg)
			if err != nil {
				t.Fatalf("fork RecoverTurn = %v, %v", rt, err)
			}
			if rt != nil {
				let()
				_, _ = rt.Wait()
				t.Fatalf("fork RecoverTurn started a resume beside the origin's running handler")
			}
			p := f.Pending()
			if len(p) != 1 || !p[0].Recovery || !strings.Contains(p[0].Reason, "still running") {
				t.Fatalf("fork Pending = %+v, want the call parked as still running", p)
			}
			if _, err := f.Decide(bg, thread.Approve(p[0].CallID).Bind(p[0])); !errors.Is(err, thread.ErrStillRunning) {
				t.Fatalf("fork Decide(approve) = %v, want ErrStillRunning", err)
			}
			mu.Lock()
			n, r := len(keys), reconciled
			mu.Unlock()
			if n != 1 || r != 0 {
				t.Fatalf("charge ran %d times, Reconcile asked %d times, beside the origin's running handler", n, r)
			}
			let()
			if _, err := turn.Wait(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
