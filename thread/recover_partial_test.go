package thread_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

type orderArg struct {
	OrderID string `json:"order_id"`
}

// turnEntryOf returns the session's turn entry for run.
func turnEntryOf(t *testing.T, s *thread.Session, run string) thread.TurnEntry {
	t.Helper()
	for _, e := range s.Entries() {
		if te, ok := e.(thread.TurnEntry); ok && te.RunID == run {
			return te
		}
	}
	t.Fatalf("no turn entry for run %s", run)
	return thread.TurnEntry{}
}

// A shutdown cuts a step off while its pay handler runs and its
// ReplaySafe sibling has already answered: the step's partial tool
// message is on the line after it. The pay call is still the cut-off
// run's own unanswered call — Send is refused (ErrRecoveryRequired),
// the turn entry lists it once, RecoverTurn parks it as a recovery
// request, and a skip reads the OUTCOME_UNKNOWN text, never "DENIED".
// Both backends, across a restart (review R1).
func TestShutdownWithAnAnsweredSiblingIsRecovered(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			var payRuns atomic.Int32
			payStarted := make(chan struct{}, 1)
			pay := core.Tool("pay", "Pay.", func(ctx context.Context, in orderArg) (string, error) {
				payRuns.Add(1)
				payStarted <- struct{}{}
				<-ctx.Done() // the request was sent; its answer is lost with the shutdown
				return "", ctx.Err()
			})
			lookup := core.Tool("lookup", "Look up.", func(ctx context.Context, in orderArg) (string, error) {
				return "order A: 10 EUR", nil
			}, core.Replay(core.ReplaySafe))
			st := b.open(t)
			s, err := thread.Create(bg, st, core.New(wefttest.Script(
				wefttest.ToolCalls(
					wefttest.Call{Name: "lookup", ID: "call_look", Args: `{"order_id":"A"}`},
					wefttest.Call{Name: "pay", ID: "call_pay", Args: `{"order_id":"A"}`},
				),
			), pay, lookup, core.Parallelism(1)))
			if err != nil {
				t.Fatal(err)
			}
			id := s.ID()
			turn, err := s.Send(bg, core.User("pay order A"))
			if err != nil {
				t.Fatal(err)
			}
			<-payStarted
			_ = shutdownClose(t, s, 0)
			_, _ = turn.Wait()

			st = restart(t, b, st, id)
			model := wefttest.Script(wefttest.Say("ok"))
			s2, err := thread.Open(bg, st, id, core.New(model, pay, lookup))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s2.Close(bg) }()
			te := turnEntryOf(t, s2, id+"-t1")
			if !te.CutOff || len(te.Pending) != 1 || te.Pending[0].ID != "call_pay" {
				t.Fatalf("turn entry: cut_off=%v pending=%v, want cut off with call_pay once", te.CutOff, te.Pending)
			}
			if _, err := s2.Send(bg, core.User("did it go through?")); !errors.Is(err, thread.ErrRecoveryRequired) {
				t.Fatalf("Send over the cut-off pay call = %v, want ErrRecoveryRequired", err)
			}
			if err := s2.Claim(bg); err != nil {
				t.Fatal(err)
			}
			if _, err := s2.RecoverTurn(bg); err != nil {
				t.Fatalf("RecoverTurn: %v", err)
			}
			p := s2.Pending()
			if len(p) != 1 || p[0].CallID != "call_pay" || !p[0].Recovery {
				t.Fatalf("Pending = %+v, want call_pay as a recovery request", p)
			}
			rt, err := s2.Decide(bg, bound(s2, thread.Deny("call_pay", "no")))
			if err != nil || rt == nil {
				t.Fatalf("Decide(deny) = %v, %v", rt, err)
			}
			if _, err := rt.Wait(); err != nil {
				t.Fatal(err)
			}
			got := map[string]string{}
			for _, m := range model.Requests()[0].Messages {
				for _, part := range m.Content {
					if r, ok := part.(core.ToolResultPart); ok {
						got[r.CallID] = r.Content
					}
				}
			}
			if got["call_look"] != "order A: 10 EUR" {
				t.Errorf("the answered sibling reads %q", got["call_look"])
			}
			if !strings.HasPrefix(got["call_pay"], core.CodeOutcomeUnknown+": ") {
				t.Errorf("the skipped pay call reads %q, want the OUTCOME_UNKNOWN skip", got["call_pay"])
			}
			if n := payRuns.Load(); n != 1 {
				t.Errorf("pay ran %d times, want 1", n)
			}
		})
	}
}

// A shutdown cuts a step off while one handler ignores its context;
// its sibling, not ReplaySafe, had already answered with its own
// failure (a declined card). That failure is the call's answer: the
// shutdown keeps it, so recovery never offers the declined call as one
// that "may have run" (review R5). Only a result the core marked of
// unknown outcome is taken out.
func TestShutdownKeepsAHandlersOwnFailure(t *testing.T) {
	var payRuns atomic.Int32
	release := make(chan struct{})
	pay := core.Tool("pay", "Pay.", func(ctx context.Context, in orderArg) (string, error) {
		payRuns.Add(1)
		return "", &core.ToolError{Code: "CARD_DECLINED", Message: "card declined"}
	})
	shipStarted := make(chan struct{}, 1)
	ship := core.Tool("ship", "Ship.", func(ctx context.Context, in orderArg) (string, error) {
		shipStarted <- struct{}{}
		<-release // ignores its context
		return "shipped", nil
	})
	st := thread.Memory()
	s, err := thread.Create(bg, st, core.New(wefttest.Script(
		wefttest.ToolCalls(
			wefttest.Call{Name: "pay", ID: "call_pay", Args: `{"order_id":"A"}`},
			wefttest.Call{Name: "ship", ID: "call_ship", Args: `{"order_id":"A"}`},
		),
	), pay, ship, core.Parallelism(1)))
	if err != nil {
		t.Fatal(err)
	}
	id := s.ID()
	turn, err := s.Send(bg, core.User("pay and ship"))
	if err != nil {
		t.Fatal(err)
	}
	<-shipStarted
	ctx, cancel := context.WithTimeoutCause(bg, 0, thread.ErrShutdown)
	defer cancel()
	_ = s.Close(ctx)
	close(release)
	if err := s.Close(bg); err != nil {
		t.Fatal(err)
	}
	_, _ = turn.Wait()
	s2, err := thread.Open(bg, st, id, core.New(wefttest.Script(wefttest.Say("ok")), pay, ship))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close(bg) }()
	te := turnEntryOf(t, s2, id+"-t1")
	for _, c := range te.Pending {
		if c.ID == "call_pay" {
			t.Fatalf("the declined pay call is left unanswered on the cut-off turn: pending %v", te.Pending)
		}
	}
	if _, err := s2.RecoverTurn(bg); err != nil {
		t.Fatalf("RecoverTurn: %v", err)
	}
	for _, r := range s2.Pending() {
		if r.CallID == "call_pay" {
			t.Errorf("recovery offers the declined pay call: %+v", r)
		}
	}
	if n := payRuns.Load(); n != 1 {
		t.Errorf("pay ran %d times, want 1", n)
	}
}
