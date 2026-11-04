package thread_test

import (
	"context"
	"errors"
	"iter"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// hangModel's second call blocks until its context ends: a model call
// in flight when the worker stops.
type hangModel struct {
	core.Model
	calls  int
	inCall chan struct{}
}

func (m *hangModel) Stream(ctx context.Context, req core.ModelRequest) iter.Seq2[core.ModelEvent, error] {
	m.calls++
	if m.calls < 2 {
		return m.Model.Stream(ctx, req)
	}
	return func(yield func(core.ModelEvent, error) bool) {
		close(m.inCall)
		<-ctx.Done()
		yield(nil, ctx.Err())
	}
}

// shutdownClose is a stopping worker's Close: a deadline whose cause
// is ErrShutdown, then a second Close that lets the felled turn's end
// land, seals and releases.
func shutdownClose(t *testing.T, s *thread.Session, d time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithTimeoutCause(bg, d, thread.ErrShutdown)
	defer cancel()
	err := s.Close(ctx)
	grace, cancel2 := context.WithTimeout(bg, 5*time.Second)
	defer cancel2()
	if cerr := s.Close(grace); cerr != nil {
		t.Fatalf("the second Close: %v", cerr)
	}
	return err
}

// A worker that stops — Close under a deadline whose cause is
// ErrShutdown, a turn still in its model call: the turn is felled with
// ErrShutdown and recorded cut off — TurnEntry.CutOff, not Canceled —
// and after a restart the next process lists it (Ready: ReadyRecover)
// and RecoverTurn continues it from its last finished step. Both
// backends.
func TestCloseUnderShutdownRecordsACutOffTurn(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			st := b.open(t)
			m := &hangModel{Model: wefttest.Script(charged("charge")), inCall: make(chan struct{})}
			charge := &sideEffect{}
			s, err := thread.Create(bg, st, core.New(m, charge.def()))
			if err != nil {
				t.Fatal(err)
			}
			turn, err := s.Send(bg, core.User("charge it"))
			if err != nil {
				t.Fatal(err)
			}
			<-m.inCall
			if err := shutdownClose(t, s, 50*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Close under shutdown = %v, want the deadline", err)
			}
			if _, err := turn.Wait(); !errors.Is(err, thread.ErrShutdown) || !errors.Is(err, context.Canceled) {
				t.Fatalf("turn error = %v, want ErrShutdown (a cancellation)", err)
			}
			id := s.ID()
			var te thread.TurnEntry
			for _, e := range s.Entries() {
				if x, ok := e.(thread.TurnEntry); ok {
					te = x
				}
			}
			if !te.CutOff || te.Canceled || !strings.Contains(te.Err, "shutting down") {
				t.Fatalf("turn entry = %+v, want cut off, not canceled", te)
			}
			st = restart(t, b, st, id)
			s2, err := thread.Open(bg, st, id, core.New(wefttest.Script(wefttest.Say("resumed after the deploy")), charge.def()))
			if err != nil {
				t.Fatal(err)
			}
			if r := s2.Ready(); len(r) != 1 || r[0].Kind != thread.ReadyRecover || r[0].RunID != id+"-t1" {
				t.Fatalf("Ready() = %+v", r)
			}
			rt, err := s2.RecoverTurn(bg)
			if err != nil || rt == nil {
				t.Fatalf("RecoverTurn = %v, %v", rt, err)
			}
			res, err := rt.Wait()
			if err != nil || res.Text() != "resumed after the deploy" {
				t.Fatalf("continuation = %v, %v", res, err)
			}
			if n := len(charge.ran()); n != 1 {
				t.Fatalf("charges = %d, want the one before the shutdown", n)
			}
			_ = s2.Close(bg)
		})
	}
}

// A plain Close at its deadline still records a canceled turn, and a
// caller's own cancel with cause ErrShutdown records it cut off.
func TestShutdownCauseOnTheTurnContext(t *testing.T) {
	m := &hangModel{Model: wefttest.Script(wefttest.Say("first")), inCall: make(chan struct{})}
	s, err := thread.Create(bg, thread.Memory(), core.New(m))
	if err != nil {
		t.Fatal(err)
	}
	t1, _ := s.Send(bg, core.User("one"))
	if _, err := t1.Wait(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(bg)
	t2, err := s.Send(ctx, core.User("two"))
	if err != nil {
		t.Fatal(err)
	}
	<-m.inCall
	cancel(thread.ErrShutdown)
	if _, err := t2.Wait(); !errors.Is(err, thread.ErrShutdown) {
		t.Fatalf("turn error = %v", err)
	}
	if r := s.Ready(); len(r) != 1 || r[0].Kind != thread.ReadyRecover || r[0].RunID != t2.RunID() {
		t.Fatalf("Ready() = %+v", r)
	}

	m2 := &hangModel{Model: wefttest.Script(wefttest.Say("first")), inCall: make(chan struct{})}
	s2, _ := thread.Create(bg, thread.Memory(), core.New(m2))
	t3, _ := s2.Send(bg, core.User("one"))
	_, _ = t3.Wait()
	t4, _ := s2.Send(bg, core.User("two"))
	<-m2.inCall
	cctx, ccancel := context.WithTimeout(bg, 20*time.Millisecond)
	defer ccancel()
	_ = s2.Close(cctx)
	_, _ = t4.Wait()
	_ = s2.Close(bg)
	for _, e := range s2.Entries() {
		if x, ok := e.(thread.TurnEntry); ok && x.RunID == t4.RunID() && (x.CutOff || !x.Canceled) {
			t.Fatalf("a plain Close's turn entry = %+v, want canceled", x)
		}
	}
}

// A shutdown during a risky handler — it has charged and waits on the
// network when the cancellation fells it — keeps the call unanswered,
// without synthetic text: the model never reads a cancellation it
// could answer by charging again, Send is refused until recovery, and
// RecoverTurn parks the call as a recovery request. A ReplaySafe call
// felled beside it is not kept. Both backends.
func TestShutdownDuringARiskyHandler(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			var calls atomic.Int32
			inHandler := make(chan struct{})
			charge := core.Tool("charge", "Charge a card.", func(ctx context.Context, in refundInput) (string, error) {
				calls.Add(1)
				close(inHandler)
				<-ctx.Done() // charged; waiting on the network
				return "", ctx.Err()
			})
			st := b.open(t)
			s, err := thread.Create(bg, st, core.New(wefttest.Script(charged("charge")), charge))
			if err != nil {
				t.Fatal(err)
			}
			turn, _ := s.Send(bg, core.User("charge it"))
			<-inHandler
			_ = shutdownClose(t, s, 50*time.Millisecond)
			_, _ = turn.Wait()
			path, err := s.Path(s.Leaf())
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range path { // the step's tool message, if it landed, stays off the path
				me, ok := e.(thread.MessageEntry)
				if !ok {
					continue
				}
				for _, p := range me.Message.Content {
					if r, ok := p.(core.ToolResultPart); ok && r.CallID == "call_1" {
						t.Fatalf("the felled call's result was recorded: %q", r.Content)
					}
				}
			}
			id := s.ID()
			st = restart(t, b, st, id)
			s2, err := thread.Open(bg, st, id, core.New(wefttest.Script(wefttest.Say("x")), charge))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s2.Send(bg, core.User("again?")); !errors.Is(err, thread.ErrRecoveryRequired) {
				t.Fatalf("Send after the shutdown = %v, want ErrRecoveryRequired", err)
			}
			if rt, err := s2.RecoverTurn(bg); err != nil || rt != nil {
				t.Fatalf("RecoverTurn = %v, %v; want the call parked", rt, err)
			}
			if p := s2.Pending(); len(p) != 1 || !p[0].Recovery {
				t.Fatalf("Pending = %+v", p)
			}
			if n := calls.Load(); n != 1 {
				t.Fatalf("charged %d times", n)
			}
			_ = s2.Close(bg)
		})
	}
}
