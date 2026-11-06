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

const reasonDelegatedText = "cut off while it delegated: the delegated work may have run; approve to delegate it again, deny to skip it"

// A cut-off delegation whose receipt settled canceled — what the child
// did is not known — stays a person's to decide: RecoverTurn parks the
// delegating call as a recovery request, and pool.Recover leaves it
// there; the model reads nothing. Neither Recover nor ResolveDelegation
// resolves it with the canceled text (a plain failure the model could
// act on by delegating again).
func TestCanceledCutOffDelegationStaysAPersons(t *testing.T) {
	for _, sc := range poolStores(t) {
		t.Run(sc.name, func(t *testing.T) {
			ctx := context.Background()
			p, s2, rc, release := cutOffDelegation(t, sc.st(t))
			defer release()
			if _, err := s2.AppendPoolReceipt(ctx, thread.PoolReceiptEntry{
				Receipt: rc.ID, Status: thread.PoolCanceled, Child: rc.Child, Stop: "canceled",
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := s2.RecoverTurn(ctx); err != nil {
				t.Fatalf("RecoverTurn: %v", err)
			}
			if err := p.Recover(ctx, s2); err != nil {
				t.Fatalf("Recover: %v", err)
			}
			if _, err := s2.ResolveDelegation(ctx, rc.Call, rc.Child, "canceled", true); !errors.Is(err, thread.ErrNotPending) {
				t.Errorf("ResolveDelegation of the canceled delegation's call = %v, want ErrNotPending", err)
			}
			wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := s2.WaitIdle(wctx); err != nil {
				t.Fatal(err)
			}
			pend := s2.Pending()
			if len(pend) != 1 || pend[0].CallID != rc.Call || !pend[0].Recovery || pend[0].Reason != reasonDelegatedText {
				t.Fatalf("pending = %+v, want the delegating call as a recovery request for a person", pend)
			}
			if got := toolResultsOf(s2.Context()); len(got) == 1 && got[0] != "no result recorded: the call was interrupted" {
				t.Errorf("the model read %q, want no result", got)
			}
			if lastText(s2) == "parent done" {
				t.Error("the parent ran on: its call was resolved behind the person")
			}
		})
	}
}

// The same for a call parked as of unknown outcome when the pool was
// closed first: once its receipt settles canceled, pool.Recover leaves it
// pending and Pending says what is true now — the delegated work may
// have run, approve to delegate again or deny — not the stale "still
// open". The approval it then takes is a person's deliberate one.
func TestCanceledCloseFirstDelegationStaysAPersons(t *testing.T) {
	ctx := context.Background()
	st := thread.Memory()
	entered := make(chan struct{}, 4)
	ship := core.Tool("ship", "Ship.", func(ctx context.Context, _ struct{}) (string, error) {
		entered <- struct{}{}
		<-ctx.Done()
		return "", ctx.Err()
	}, core.Replay(core.ReplayKeyed))
	child := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{}`}),
		wefttest.Say("child done"),
	), ship)
	p := pool.New(2)
	var mu sync.Mutex
	var notified []thread.Request
	s, err := thread.Create(ctx, st, core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "worker", ID: "d1", Args: wefttest.Args(struct{ Prompt string }{"go"})}),
		wefttest.Say("lead done"),
	), p.MustWrap("worker", "w", child)), thread.OnRequest(func(r thread.Request) {
		mu.Lock()
		notified = append(notified, r)
		mu.Unlock()
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(ctx) }()
	turn, err := s.Send(ctx, core.User("go"))
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	sctx, cancel := context.WithCancelCause(ctx)
	cancel(thread.ErrShutdown)
	_ = p.Close(sctx)
	if res, err := turn.Wait(); err != nil || len(res.Pending) != 1 {
		t.Fatalf("lead turn: %v, %+v", err, res)
	}
	mu.Lock()
	if len(notified) != 0 {
		t.Errorf("OnRequest fired for %+v: a call nobody can approve while its delegation is open", notified)
	}
	mu.Unlock()
	rc := pool.Receipts(s)[0]
	if _, err := s.AppendPoolReceipt(ctx, thread.PoolReceiptEntry{
		Receipt: rc.ID, Status: thread.PoolCanceled, Child: rc.Child, Stop: "canceled",
	}); err != nil {
		t.Fatal(err)
	}
	p2 := pool.New(2)
	defer func() { _ = p2.Close(ctx) }()
	if err := p2.Recover(ctx, s); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	pend := s.Pending()
	if len(pend) != 1 || pend[0].CallID != "d1" || pend[0].Reason != reasonDelegatedText {
		t.Fatalf("pending = %+v, want the call left for a person with the delegated reason", pend)
	}
	if lastText(s) == "lead done" {
		t.Error("the lead ran on: its call was resolved behind the person")
	}
}
