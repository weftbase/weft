package thread_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// P1-1: a handler abandoned at its Timeout may still be running; the
// system of record asked now answers "missing" for an effect that is
// about to land. The live ladder never reconciles an abandoned handler
// and reruns it: the call goes to a person. The effect happens once.
// Both backends.
func TestAbandonedHandlerIsNotReconciledAndRerun(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			var mu sync.Mutex
			invoked, effects := 0, 0
			release := make(chan struct{})
			finished := make(chan struct{})
			wire := core.Tool("wire", "Wire money.", func(context.Context, struct{}) (string, error) {
				mu.Lock()
				invoked++
				n := invoked
				mu.Unlock()
				if n == 1 {
					<-release // ignores its context: still running after the timeout
					defer close(finished)
				}
				mu.Lock()
				effects++
				mu.Unlock()
				return "wired", nil
			}, core.Timeout(20*time.Millisecond), core.Reconcile(func(context.Context, string) (bool, string, error) {
				mu.Lock()
				defer mu.Unlock()
				return effects > 0, "wired", nil
			}))
			s, err := thread.Create(bg, b.open(t), core.New(wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "wire", ID: "w1", Args: `{}`}),
				wefttest.Say("done"),
			), wire))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close(bg) }()
			turn, err := s.Send(bg, core.User("wire it"))
			if err != nil {
				t.Fatal(err)
			}
			_, _ = turn.Wait()
			_ = s.WaitIdle(bg)
			close(release)
			<-finished
			mu.Lock()
			defer mu.Unlock()
			if invoked != 1 || effects != 1 {
				t.Fatalf("the handler ran %d times, %d effects; want once", invoked, effects)
			}
			if p := s.Pending(); len(p) != 1 || !p[0].Recovery {
				t.Fatalf("Pending = %+v, want the call waiting for a person", p)
			}
		})
	}
}

// P1-2: a ReplayKeyed call whose Timeout is shorter than its provider
// is not retried for ever: the session's automatic retries are bounded
// (with backoff), then the call goes to a person. The count is on file:
// after a restart and a person's retry that times out again, the call
// goes straight back to a person. Both backends.
func TestKeyedRetriesAreBounded(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			var mu sync.Mutex
			invoked := 0
			slow := func() *core.ToolDef {
				return core.Tool("ship", "Ship.", func(ctx context.Context, _ struct{}) (string, error) {
					mu.Lock()
					invoked++
					mu.Unlock()
					<-ctx.Done()
					return "", ctx.Err()
				}, core.Replay(core.ReplayKeyed), core.Timeout(10*time.Millisecond))
			}
			st := b.open(t)
			s, err := thread.Create(bg, st, core.New(wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{}`}),
			), slow()))
			if err != nil {
				t.Fatal(err)
			}
			turn, err := s.Send(bg, core.User("ship it"))
			if err != nil {
				t.Fatal(err)
			}
			_, _ = turn.Wait()
			deadline := time.Now().Add(10 * time.Second)
			for {
				_ = s.WaitIdle(bg)
				if p := s.Pending(); len(p) == 1 || time.Now().After(deadline) {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			mu.Lock()
			n := invoked
			mu.Unlock()
			if n != 4 {
				t.Fatalf("ship ran %d times, want 4: the first run and 3 automatic retries", n)
			}
			p := s.Pending()
			if len(p) != 1 || !p[0].Recovery || !strings.Contains(p[0].Reason, "retries are spent") {
				t.Fatalf("Pending = %+v, want the call waiting for a person", p)
			}
			id := s.ID()
			_ = s.Close(bg)
			st = restart(t, b, st, id)
			s2, err := thread.Open(bg, st, id, core.New(wefttest.Script(), slow()))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s2.Close(bg) }()
			rt, err := s2.Decide(bg, bound(s2, thread.Approve("s1")))
			if err != nil || rt == nil {
				t.Fatalf("Decide = %v, %v", rt, err)
			}
			_, _ = rt.Wait()
			_ = s2.WaitIdle(bg)
			mu.Lock()
			n = invoked
			mu.Unlock()
			if n != 5 {
				t.Fatalf("ship ran %d times after the person's retry, want 5", n)
			}
			if p := s2.Pending(); len(p) != 1 || !p[0].Recovery {
				t.Fatalf("Pending after the person's retry = %+v, want a person again", p)
			}
		})
	}
}

// P1-3: skipping (denying) a recovery request does not tell the model
// "DENIED": the call may have run. The model reads an outcome-unknown
// result that says not to repeat it, with the decider's reason
// appended. Both backends.
func TestSkippedRecoveryReadsOutcomeUnknown(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			charge := &sideEffect{}
			id, st := crashTurn(t, b, func(cs *crashStore) *core.Agent {
				charge.crash = cs
				return core.New(wefttest.Script(charged("charge")), charge.def())
			}, nil)
			charge.crash = nil
			st = restart(t, b, st, id)
			s, err := thread.Open(bg, st, id, core.New(wefttest.Script(wefttest.Say("ok")), charge.def()))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close(bg) }()
			if _, err := s.RecoverTurn(bg); err != nil {
				t.Fatal(err)
			}
			rt, err := s.Decide(bg, bound(s, thread.Deny("call_1", "checked by hand")))
			if err != nil || rt == nil {
				t.Fatalf("Decide = %v, %v", rt, err)
			}
			res, err := rt.Wait()
			if err != nil {
				t.Fatal(err)
			}
			got := toolResult(res, "call_1")
			want := `OUTCOME_UNKNOWN: tool "charge" may have run; it was skipped, not retried: do not repeat it (skipped: checked by hand)`
			if got != want {
				t.Fatalf("the model read %q, want %q", got, want)
			}
			if len(charge.ran()) != 1 {
				t.Fatalf("charge ran %d times", len(charge.ran()))
			}
		})
	}
}
