package thread_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// holdOnce is a middleware that holds the first call it sees before
// the dispatch gate — until release — and lets every other through.
func holdOnce(in, release chan struct{}) core.ToolMiddleware {
	var once sync.Once
	return func(next core.ToolCaller) core.ToolCaller {
		return func(ctx context.Context, call core.ToolCallPart) (string, error) {
			once.Do(func() {
				close(in)
				<-release
			})
			return next(ctx, call)
		}
	}
}

// P0-2: a Rollback that lands while a call waits in middleware, before
// the dispatch gate, sees no effect and is accepted as a Rollback. The
// gate and the rollback serialize: the call reaching the gate after
// the rollback is refused, so the handler never runs and the rolled
// back line hides nothing. The same holds for an Interrupt and a
// Close. Both backends.
func TestGateRefusesAfterRollback(t *testing.T) {
	for _, b := range durableBackends(t) {
		for _, how := range []string{"rollback", "interrupt", "close"} {
			t.Run(b.name+"/"+how, func(t *testing.T) {
				pay := &sideEffect{name: "pay"}
				in, release := make(chan struct{}), make(chan struct{})
				s, err := thread.Create(bg, b.open(t), core.New(wefttest.Script(
					wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "p1", Args: `{"order_id":"1"}`}),
					wefttest.Say("the follow-up"),
				), pay.def(), core.WrapTools(holdOnce(in, release))))
				if err != nil {
					t.Fatal(err)
				}
				t1, err := s.Send(bg, core.User("pay"))
				if err != nil {
					t.Fatal(err)
				}
				<-in // the call waits before the gate
				var t2 *thread.Turn
				switch how {
				case "rollback":
					t2, err = s.Send(bg, core.User("never mind"), thread.As(thread.Rollback))
					if err != nil {
						t.Fatalf("Send under Rollback = %v; want it accepted (nothing ran yet)", err)
					}
				case "interrupt":
					t2, err = s.Send(bg, core.User("never mind"), thread.As(thread.Interrupt))
					if err != nil {
						t.Fatal(err)
					}
				case "close":
					cctx, cancel := context.WithCancel(bg)
					cancel() // a Close that gives up at once fells the turn
					_ = s.Close(cctx)
				}
				close(release)
				_, _ = t1.Wait()
				if t2 != nil {
					if _, err := t2.Wait(); err != nil && !errors.Is(err, context.Canceled) {
						t.Fatalf("the follow-up = %v", err)
					}
				}
				if n := len(pay.ran()); n != 0 {
					t.Fatalf("the handler ran %d times after the %s", n, how)
				}
				if how != "close" {
					_ = s.Close(bg)
				}
			})
		}
	}
}

// lostLease is a storage whose writer has lost the session: every
// fence fails, every other call passes through.
type lostLease struct{ thread.Storage }

func (lostLease) Fence(context.Context, string) error { return thread.ErrLocked }

// A gate refusal for a lost lease ends the run, with that cause: the
// writer starts no further call and does not feed refusals to the model
// until MaxSteps. The model is asked once; nothing runs.
func TestGateRefusalEndsTheRun(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			charge := &sideEffect{}
			model := wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "charge", ID: "c1", Args: `{"order_id":"1"}`}),
				wefttest.ToolCalls(wefttest.Call{Name: "charge", ID: "c2", Args: `{"order_id":"2"}`}),
				wefttest.Say("done"),
			)
			s, err := thread.Create(bg, lostLease{b.open(t)}, core.New(model, charge.def()))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close(bg) }()
			turn, err := s.Send(bg, core.User("charge"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := turn.Wait(); !errors.Is(err, thread.ErrLocked) {
				t.Fatalf("turn = %v, want the lease-lost cause (ErrLocked)", err)
			}
			if n := len(model.Requests()); n != 1 {
				t.Fatalf("the model was asked %d times, want once", n)
			}
			if n := len(charge.ran()); n != 0 {
				t.Fatalf("charge ran %d times", n)
			}
		})
	}
}
