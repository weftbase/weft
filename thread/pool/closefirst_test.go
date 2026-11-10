package pool_test

import (
	"context"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/thread/pool"
)

// pool.Close under thread.ErrShutdown while a parent session's turn is
// delegating — the pool closed before the parent session — cuts the
// child off and leaves its receipt open; the parent's delegating call
// then reads as an unknown outcome and parks (left to the pool, an
// approval refused while the receipt is open), never as a plain tool
// failure its model acts on. Before 0.24.1 the parent's model read the
// raw "thread: the process is shutting down: …" as a tool error and
// carried on, while the next worker completed the child's effect. A
// Recover by the next pool then resolves the call with the child's
// answer.
func TestPoolClosedFirstParksTheDelegatingCall(t *testing.T) {
	for _, sc := range poolStores(t) {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			st := sc.st(t)
			entered := make(chan struct{}, 4)
			first := make(chan struct{}, 1)
			first <- struct{}{}
			ship := core.Tool("ship", "Ship.", func(ctx context.Context, _ struct{}) (string, error) {
				entered <- struct{}{}
				select {
				case <-first: // the first run is held until the shutdown cuts it off
					<-ctx.Done()
					return "", ctx.Err()
				default:
					return "shipped", nil
				}
			}, core.Replay(core.ReplayKeyed))
			child := core.New(wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{}`}),
				wefttest.Say("child done"), wefttest.Say("child done"),
			), ship)
			leadOver := func(p *pool.Pool, turns ...wefttest.Turn) *core.Agent {
				return core.New(wefttest.Script(turns...), p.MustWrap("worker", "w", child))
			}
			p := pool.New(2)
			s, err := thread.Create(ctx, st, leadOver(p,
				wefttest.ToolCalls(wefttest.Call{Name: "worker", ID: "d1", Args: wefttest.Args(struct{ Prompt string }{"go"})}),
				wefttest.Say("lead: the worker failed, retrying"),
			))
			if err != nil {
				t.Fatal(err)
			}
			turn, err := s.Send(ctx, core.User("go"))
			if err != nil {
				t.Fatal(err)
			}
			<-entered
			sctx, cancel := context.WithCancelCause(ctx)
			cancel(thread.ErrShutdown)
			_ = p.Close(sctx) // the wrong order: the pool before the parent session
			res, err := turn.Wait()
			if err != nil {
				t.Fatalf("lead turn: %v", err)
			}
			if len(res.Pending) != 1 || res.Pending[0].ID != "d1" {
				t.Fatalf("lead turn ended %q with pending %+v, want the delegating call parked", res.Text(), res.Pending)
			}
			if res.Text() != "" {
				t.Fatalf("the lead's model answered %q: it read the cut-off as a result and carried on", res.Text())
			}
			rs := pool.Receipts(s)
			if len(rs) != 1 || rs[0].Settled() {
				t.Fatalf("receipts = %+v, want one open", rs)
			}
			pend := s.Pending()
			if len(pend) != 1 || pend[0].CallID != "d1" {
				t.Fatalf("pending = %+v", pend)
			}
			if _, err := s.Decide(ctx, thread.Approve("").Bind(pend[0])); err == nil {
				t.Fatal("an approval of the open delegation's call was accepted: it would delegate again")
			}
			wctx, wcancel := context.WithTimeout(ctx, 10*time.Second)
			defer wcancel()
			if err := s.Close(wctx); err != nil {
				t.Fatalf("Close: %v", err)
			}

			p2 := pool.New(2)
			defer func() { _ = p2.Close(ctx) }()
			s2, err := thread.Open(ctx, st, s.ID(), leadOver(p2, wefttest.Say("lead: shipped")))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s2.Close(ctx) }()
			if err := p2.Recover(ctx, s2); err != nil {
				t.Fatalf("Recover: %v", err)
			}
			deadline := time.Now().Add(15 * time.Second)
			for lastText(s2) != "lead: shipped" {
				if time.Now().After(deadline) {
					t.Fatalf("not resolved: receipts %+v pending %+v", pool.Receipts(s2), s2.Pending())
				}
				time.Sleep(10 * time.Millisecond)
			}
			if got := toolResultsOf(s2.Context()); len(got) != 1 || got[0] != "child done" {
				t.Errorf("the lead read %q, want the child's answer", got)
			}
			if rs := pool.Receipts(s2); len(rs) != 1 || rs[0].State != pool.Done {
				t.Errorf("receipts = %+v, want the one delegation done", rs)
			}
		})
	}
}
