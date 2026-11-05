package thread_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// failTurnEndOnce refuses, once armed, the first batch that carries a
// turn entry: a transient storage error on a turn's end.
type failTurnEndOnce struct {
	thread.Storage
	armed  atomic.Bool
	failed atomic.Bool
}

func (f *failTurnEndOnce) Append(ctx context.Context, session string, entries ...thread.Entry) error {
	if f.armed.Load() && !f.failed.Load() {
		for _, e := range entries {
			if _, ok := e.(thread.TurnEntry); ok {
				f.failed.Store(true)
				return errors.New("transient: disk full")
			}
		}
	}
	return f.Storage.Append(ctx, session, entries...)
}

// slowPay is a pay tool whose handler ignores its context and lands
// after its Timeout: the call's outcome is unknown when the run ends.
// arm runs as the handler starts.
func slowPay(ran *[]string, mu *sync.Mutex, arm func()) *core.ToolDef {
	return core.Tool("pay", "Pay.", func(ctx context.Context, in orderArg) (string, error) {
		mu.Lock()
		*ran = append(*ran, in.OrderID)
		mu.Unlock()
		arm()
		time.Sleep(150 * time.Millisecond)
		return "paid " + in.OrderID, nil
	}, core.Timeout(30*time.Millisecond))
}

// toolResultIn returns what request reads as call's result.
func toolResultIn(req core.ModelRequest, call string) (string, bool) {
	for _, m := range req.Messages {
		for _, p := range m.Content {
			if r, ok := p.(core.ToolResultPart); ok && r.CallID == call {
				return r.Content, true
			}
		}
	}
	return "", false
}

// recoverAndSkip runs RecoverTurn on a session whose cut-off turn left
// call_1 of unknown outcome, checks it parks as a recovery request, and
// skips it; the resume it starts is returned.
func recoverAndSkip(t *testing.T, s *thread.Session) *thread.Turn {
	t.Helper()
	if r := s.Ready(); len(r) != 1 || r[0].Kind != thread.ReadyRecover {
		t.Fatalf("Ready = %+v, want one ReadyRecover", r)
	}
	if _, err := s.RecoverTurn(bg); err != nil {
		t.Fatalf("RecoverTurn: %v", err)
	}
	p := s.Pending()
	if len(p) != 1 || p[0].CallID != "call_1" || !p[0].Recovery {
		t.Fatalf("Pending = %+v, want call_1 as a recovery request", p)
	}
	rt, err := s.Decide(bg, bound(s, thread.Deny("call_1", "checked by hand")))
	if err != nil || rt == nil {
		t.Fatalf("Decide(deny) = %v, %v", rt, err)
	}
	return rt
}

// The runner asks the cut-off check on its own transitions (review R2):
// a turn whose end could not be written after it dispatched a call of
// unknown outcome reads as cut off in the same process (ADR 0032). The
// queued send behind it is held — never run over the call, which its
// input would repair into a synthetic result — the failed turn's error
// says ErrRecoveryRequired, and RecoverTurn decides the call before the
// held send runs.
func TestQueuedSendWaitsForRecoveryOfAnUnrecordedTurnEnd(t *testing.T) {
	var mu sync.Mutex
	var ran []string
	started := make(chan struct{}, 4)
	st := &failTurnEndOnce{Storage: thread.Memory()}
	pay := slowPay(&ran, &mu, func() { st.armed.Store(true); started <- struct{}{} })
	model := wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "call_1", Args: `{"order_id":"A"}`}),
		wefttest.Say("skipped, as you said"), // the resume after the skip
		wefttest.Say("nothing else"),         // the held send
	)
	s, err := thread.Create(bg, st, core.New(model, pay))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(bg) }()
	a, err := s.Send(bg, core.User("pay order A"))
	if err != nil {
		t.Fatal(err)
	}
	<-started
	b, err := s.Send(bg, core.User("anything else?")) // queued behind A
	if err != nil {
		t.Fatal(err)
	}
	_, aerr := a.WaitContext(deadline(t, 5*time.Second))
	if !errors.Is(aerr, thread.ErrNotPersisted) || !errors.Is(aerr, thread.ErrRecoveryRequired) {
		t.Fatalf("turn A: %v, want ErrNotPersisted and ErrRecoveryRequired", aerr)
	}
	if err := s.WaitIdle(deadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := b.Outcome(); got != thread.TurnRunning {
		t.Fatalf("the queued send ran over the unknown call: outcome %v", got)
	}
	if n := len(model.Requests()); n != 1 {
		t.Fatalf("%d model calls, want 1 (the queued send must wait)", n)
	}
	if _, err := s.Send(bg, core.User("more")); !errors.Is(err, thread.ErrRecoveryRequired) {
		t.Fatalf("Send = %v, want ErrRecoveryRequired", err)
	}
	rt := recoverAndSkip(t, s)
	if _, err := rt.WaitContext(deadline(t, 5*time.Second)); err != nil {
		t.Fatalf("the resume: %v", err)
	}
	if _, err := b.WaitContext(deadline(t, 5*time.Second)); err != nil {
		t.Fatalf("the held send: %v", err)
	}
	reqs := model.Requests()
	if len(reqs) != 3 {
		t.Fatalf("%d model calls, want 3", len(reqs))
	}
	for i, req := range reqs[1:] {
		got, ok := toolResultIn(req, "call_1")
		if !ok || !strings.HasPrefix(got, core.CodeOutcomeUnknown+": ") {
			t.Errorf("model call %d reads call_1 as %q, want the OUTCOME_UNKNOWN skip", i+2, got)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(ran) != 1 {
		t.Errorf("pay ran %d times, want 1", len(ran))
	}
}

// A steer that missed its drain point becomes a follow-up: queued like
// a send, it is held the same way behind an unrecorded turn end.
func TestSteerFollowUpWaitsForRecoveryOfAnUnrecordedTurnEnd(t *testing.T) {
	var mu sync.Mutex
	var ran []string
	started := make(chan struct{}, 4)
	st := &failTurnEndOnce{Storage: thread.Memory()}
	pay := slowPay(&ran, &mu, func() { st.armed.Store(true); started <- struct{}{} })
	model := wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "call_1", Args: `{"order_id":"A"}`}),
		wefttest.Say("skipped"),
		wefttest.Say("follow-up answered"),
	)
	s, err := thread.Create(bg, st, core.New(model, pay))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(bg) }()
	a, err := s.Send(bg, core.User("pay order A"))
	if err != nil {
		t.Fatal(err)
	}
	<-started
	steer, err := s.Send(bg, core.User("and use the saved card"), thread.As(thread.Steer))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.WaitContext(deadline(t, 5*time.Second)); !errors.Is(err, thread.ErrRecoveryRequired) {
		t.Fatalf("turn A: %v, want ErrRecoveryRequired", err)
	}
	if err := s.WaitIdle(deadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if n := len(model.Requests()); n != 1 {
		t.Fatalf("%d model calls, want 1 (the steer's follow-up must wait)", n)
	}
	rt := recoverAndSkip(t, s)
	if _, err := rt.WaitContext(deadline(t, 5*time.Second)); err != nil {
		t.Fatalf("the resume: %v", err)
	}
	if err := s.WaitIdle(deadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := steer.Outcome(); got != thread.TurnDeferred {
		t.Fatalf("the steer: %v, want deferred", got)
	}
	reqs := model.Requests()
	if len(reqs) != 3 {
		t.Fatalf("%d model calls, want 3 (the follow-up after the recovery)", len(reqs))
	}
	if got, _ := toolResultIn(reqs[2], "call_1"); !strings.HasPrefix(got, core.CodeOutcomeUnknown+": ") {
		t.Errorf("the follow-up reads call_1 as %q, want the OUTCOME_UNKNOWN skip", got)
	}
}

// A panic in the session's own machinery at a turn's end (here the
// caller's Clock) after a call of unknown outcome: the turn ends
// ErrTurnPanicked beside ErrRecoveryRequired, and the queued send waits
// for RecoverTurn.
func TestQueuedSendWaitsForRecoveryOfAPanickedTurn(t *testing.T) {
	var mu sync.Mutex
	var ran []string
	var armed atomic.Bool
	started := make(chan struct{}, 4)
	pay := slowPay(&ran, &mu, func() { armed.Store(true); started <- struct{}{} })
	clock := thread.Clock(func() time.Time {
		if armed.Load() {
			panic("the clock blew up")
		}
		return time.Now()
	})
	model := wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "call_1", Args: `{"order_id":"A"}`}),
		wefttest.Say("skipped"),
		wefttest.Say("nothing else"),
	)
	s, err := thread.Create(bg, thread.Memory(), core.New(model, pay), clock)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(bg) }()
	a, err := s.Send(bg, core.User("pay order A"))
	if err != nil {
		t.Fatal(err)
	}
	<-started
	// Queued before the clock is armed: the receipt's append reads it.
	armed.Store(false)
	b, err := s.Send(bg, core.User("anything else?"))
	if err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	_, aerr := a.WaitContext(deadline(t, 5*time.Second))
	if !errors.Is(aerr, thread.ErrTurnPanicked) || !errors.Is(aerr, thread.ErrRecoveryRequired) {
		t.Fatalf("turn A: %v, want ErrTurnPanicked and ErrRecoveryRequired", aerr)
	}
	if err := s.WaitIdle(deadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	armed.Store(false)
	if got := b.Outcome(); got != thread.TurnRunning {
		t.Fatalf("the queued send ran over the unknown call: outcome %v", got)
	}
	rt := recoverAndSkip(t, s)
	if _, err := rt.WaitContext(deadline(t, 5*time.Second)); err != nil {
		t.Fatalf("the resume: %v", err)
	}
	if _, err := b.WaitContext(deadline(t, 5*time.Second)); err != nil {
		t.Fatalf("the held send: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(ran) != 1 {
		t.Errorf("pay ran %d times, want 1", len(ran))
	}
}

// A resume whose end could not be written after its approved call's
// outcome went unknown: the send queued behind the boundary is held as
// behind a send's turn, and RecoverTurn parks the call again.
func TestQueuedSendWaitsForRecoveryOfAnUnrecordedResumeEnd(t *testing.T) {
	var mu sync.Mutex
	var ran []string
	st := &failTurnEndOnce{Storage: thread.Memory()}
	pay := core.Tool("pay", "Pay.", func(ctx context.Context, in orderArg) (string, error) {
		mu.Lock()
		ran = append(ran, in.OrderID)
		mu.Unlock()
		st.armed.Store(true)
		time.Sleep(150 * time.Millisecond)
		return "paid", nil
	}, core.Timeout(30*time.Millisecond), core.RequireApproval())
	model := wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "call_1", Args: `{"order_id":"A"}`}),
		wefttest.Say("skipped"),
		wefttest.Say("nothing else"),
	)
	s, err := thread.Create(bg, st, core.New(model, pay))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(bg) }()
	a, err := s.Send(bg, core.User("pay order A"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.WaitContext(deadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	b, err := s.Send(bg, core.User("anything else?")) // queued behind the boundary
	if err != nil {
		t.Fatal(err)
	}
	rt, err := s.Decide(bg, bound(s, thread.Approve("call_1")))
	if err != nil || rt == nil {
		t.Fatalf("Decide = %v, %v", rt, err)
	}
	if _, err := rt.WaitContext(deadline(t, 5*time.Second)); !errors.Is(err, thread.ErrRecoveryRequired) {
		t.Fatalf("the resume: %v, want ErrRecoveryRequired", err)
	}
	if err := s.WaitIdle(deadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := b.Outcome(); got != thread.TurnRunning {
		t.Fatalf("the queued send ran over the unknown call: outcome %v", got)
	}
	if n := len(model.Requests()); n != 1 {
		t.Fatalf("%d model calls, want 1", n)
	}
	rt2 := recoverAndSkip(t, s)
	if _, err := rt2.WaitContext(deadline(t, 5*time.Second)); err != nil {
		t.Fatalf("the second resume: %v", err)
	}
	if _, err := b.WaitContext(deadline(t, 5*time.Second)); err != nil {
		t.Fatalf("the held send: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(ran) != 1 {
		t.Errorf("pay ran %d times, want 1", len(ran))
	}
}
