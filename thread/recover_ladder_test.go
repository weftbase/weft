package thread_test

import (
	"context"
	"errors"
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
			var at []time.Time
			slow := func() *core.ToolDef {
				return core.Tool("ship", "Ship.", func(ctx context.Context, _ struct{}) (string, error) {
					mu.Lock()
					invoked++
					at = append(at, time.Now())
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
			deadline := time.Now().Add(30 * time.Second)
			for {
				_ = s.WaitIdle(bg)
				if p := s.Pending(); len(p) == 1 || time.Now().After(deadline) {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			mu.Lock()
			n := invoked
			gaps := make([]time.Duration, 0, 3)
			for i := 1; i < len(at); i++ {
				gaps = append(gaps, at[i].Sub(at[i-1]))
			}
			mu.Unlock()
			if n != 4 {
				t.Fatalf("ship ran %d times, want 4: the first run and 3 automatic retries", n)
			}
			// Every automatic retry backs off, doubling: base, 2·base,
			// 4·base (the package's tests run with a 10 ms base).
			for i, g := range gaps {
				if want := (10 * time.Millisecond) << i; g < want {
					t.Fatalf("retry %d started %v after the previous run, want ≥ %v (gaps %v)", i+1, g, want, gaps)
				}
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

// Review 3 P2-2: in a live process a keyed call whose handler the loop
// abandoned at its Timeout is not retried while that handler may still
// be running: the end of the turn waits for it to return (bounded),
// then retries under the same key — never two handlers of the call at
// once.
func TestKeyedRetryWaitsForTheAbandonedHandler(t *testing.T) {
	var mu sync.Mutex
	var keys []string
	active, most := 0, 0
	release := make(chan struct{})
	started := make(chan struct{}, 4)
	ship := core.Tool("ship", "Ship.", func(ctx context.Context, _ struct{}) (string, error) {
		c, _ := core.CallFromContext(ctx)
		mu.Lock()
		keys = append(keys, c.IdempotencyKey)
		first := len(keys) == 1
		active++
		most = max(most, active)
		mu.Unlock()
		defer func() {
			mu.Lock()
			active--
			mu.Unlock()
		}()
		started <- struct{}{}
		if first {
			<-release // ignores its deadline: abandoned, still running
		}
		return "shipped", nil
	}, core.Replay(core.ReplayKeyed), core.Timeout(10*time.Millisecond))
	s, err := thread.Create(bg, thread.Memory(), core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{}`}),
		wefttest.Say("done"),
	), ship))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(bg) }()
	turn, err := s.Send(bg, core.User("ship it"))
	if err != nil {
		t.Fatal(err)
	}
	<-started
	time.Sleep(200 * time.Millisecond) // the timeout passed long ago; the retry must wait
	mu.Lock()
	n := len(keys)
	mu.Unlock()
	if n != 1 {
		t.Fatalf("ship ran %d times while the abandoned handler still ran, want 1", n)
	}
	close(release)
	_, _ = turn.Wait()
	deadline := time.Now().Add(10 * time.Second)
	for {
		_ = s.WaitIdle(bg)
		mu.Lock()
		n = len(keys)
		mu.Unlock()
		if n == 2 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	_ = s.WaitIdle(bg)
	mu.Lock()
	defer mu.Unlock()
	if len(keys) != 2 || keys[0] != keys[1] || most != 1 {
		t.Fatalf("keys %v, at most %d at once: want one retry under the same key, after the first returned", keys, most)
	}
	if p := s.Pending(); len(p) != 0 {
		t.Fatalf("Pending = %+v", p)
	}
}

// A handler still running past the bound (the package's tests run with
// 2 s): the session does not retry it — the call goes to a person, the
// handler ran once.
func TestKeyedRetryOfAStillRunningHandlerAsksAPerson(t *testing.T) {
	var mu sync.Mutex
	invoked := 0
	release := make(chan struct{})
	defer close(release)
	ship := core.Tool("ship", "Ship.", func(ctx context.Context, _ struct{}) (string, error) {
		mu.Lock()
		invoked++
		mu.Unlock()
		<-release
		return "shipped", nil
	}, core.Replay(core.ReplayKeyed), core.Timeout(10*time.Millisecond))
	s, err := thread.Create(bg, thread.Memory(), core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{}`}),
	), ship))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(bg) }()
	turn, err := s.Send(bg, core.User("ship it"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = turn.Wait()
	_ = s.WaitIdle(bg)
	p := s.Pending()
	if len(p) != 1 || !p[0].Recovery || !strings.Contains(p[0].Reason, "still running") {
		t.Fatalf("Pending = %+v, want the call waiting for a person", p)
	}
	mu.Lock()
	defer mu.Unlock()
	if invoked != 1 {
		t.Fatalf("ship ran %d times, want 1", invoked)
	}
}

// abandonedTool is a tool whose first handler ignores its 10 ms
// Timeout and runs until release closes — abandoned by the loop, still
// running — and whose later handlers answer at once. It counts the
// handlers and the most that ever ran at once.
type abandonedTool struct {
	mu      sync.Mutex
	invoked int
	active  int
	most    int
	release chan struct{}
}

func (a *abandonedTool) def(policy core.ReplayPolicy) *core.ToolDef {
	return core.Tool("ship", "Ship.", func(ctx context.Context, _ struct{}) (string, error) {
		a.mu.Lock()
		a.invoked++
		first := a.invoked == 1
		a.active++
		a.most = max(a.most, a.active)
		a.mu.Unlock()
		defer func() {
			a.mu.Lock()
			a.active--
			a.mu.Unlock()
		}()
		if first {
			<-a.release
		}
		return "shipped", nil
	}, core.Replay(policy), core.Timeout(10*time.Millisecond))
}

func (a *abandonedTool) counts() (invoked, most int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.invoked, a.most
}

// P2-1 (final review): a ReplayNever call abandoned at its Timeout is
// not offered as a recovery request while its handler may still run —
// the end of the turn waits for it (bounded, as for a keyed retry) —
// so a person's fast approval never starts a second handler beside
// the first.
func TestNeverRetryWaitsForTheAbandonedHandler(t *testing.T) {
	ship := &abandonedTool{release: make(chan struct{})}
	s, err := thread.Create(bg, thread.Memory(), core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{}`}),
		wefttest.Say("done"),
	), ship.def(core.ReplayNever)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(bg) }()
	turn, err := s.Send(bg, core.User("ship it"))
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(300 * time.Millisecond)
		close(ship.release)
	}()
	_, _ = turn.Wait()
	p := s.Pending()
	if len(p) != 1 || !p[0].Recovery || strings.Contains(p[0].Reason, "still running") {
		t.Fatalf("Pending = %+v, want one recovery request, its handler returned", p)
	}
	rt, err := s.Decide(bg, thread.Approve("s1").Bind(p[0]))
	if err != nil || rt == nil {
		t.Fatalf("Decide = %v, %v", rt, err)
	}
	if res, err := rt.Wait(); err != nil || res.Text() != "done" {
		t.Fatalf("resume = %v, %v", res, err)
	}
	if invoked, most := ship.counts(); invoked != 2 || most != 1 {
		t.Fatalf("ship ran %d times, at most %d at once: want the retry after the first returned", invoked, most)
	}
}

// P2-1 (final review): past the bound, the request says the handler is
// still running, and an approval — the retry — is refused with
// ErrStillRunning until the handler returns; then it is taken, and the
// retry runs alone. ReplayNever and ReplayKeyed alike.
func TestApprovalWaitsForAStillRunningHandler(t *testing.T) {
	for _, policy := range []core.ReplayPolicy{core.ReplayNever, core.ReplayKeyed} {
		t.Run(string(policy), func(t *testing.T) {
			ship := &abandonedTool{release: make(chan struct{})}
			s, err := thread.Create(bg, thread.Memory(), core.New(wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{}`}),
				wefttest.Say("done"),
			), ship.def(policy)))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close(bg) }()
			turn, err := s.Send(bg, core.User("ship it"))
			if err != nil {
				t.Fatal(err)
			}
			_, _ = turn.Wait()
			p := s.Pending()
			if len(p) != 1 || !p[0].Recovery || !strings.Contains(p[0].Reason, "still running") {
				t.Fatalf("Pending = %+v, want a recovery request saying the handler still runs", p)
			}
			if rt, err := s.Decide(bg, thread.Approve("s1").Bind(p[0])); !errors.Is(err, thread.ErrStillRunning) || rt != nil {
				t.Fatalf("Decide while the handler runs = %v, %v; want ErrStillRunning", rt, err)
			}
			close(ship.release)
			var rt *thread.Turn
			deadline := time.Now().Add(5 * time.Second)
			for {
				rt, err = s.Decide(bg, thread.Approve("s1").Bind(p[0]))
				if !errors.Is(err, thread.ErrStillRunning) || time.Now().After(deadline) {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			if err != nil || rt == nil {
				t.Fatalf("Decide after the handler returned = %v, %v", rt, err)
			}
			if res, err := rt.Wait(); err != nil || res.Text() != "done" {
				t.Fatalf("resume = %v, %v", res, err)
			}
			if invoked, most := ship.counts(); invoked != 2 || most != 1 {
				t.Fatalf("ship ran %d times, at most %d at once", invoked, most)
			}
		})
	}
}

// P2-4 (final review): the end of a turn waits for its abandoned
// handlers in parallel, under one shared deadline — two still running
// past the bound (2 s in this package's tests) hold the turn about 2 s,
// not 2 s each.
func TestAbandonedHandlersShareOneDeadline(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	slow := func(name string) *core.ToolDef {
		return core.Tool(name, "Slow.", func(context.Context, struct{}) (string, error) {
			<-release
			return "ok", nil
		}, core.Replay(core.ReplayKeyed), core.Timeout(10*time.Millisecond))
	}
	s, err := thread.Create(bg, thread.Memory(), core.New(wefttest.Script(
		wefttest.ToolCalls(
			wefttest.Call{Name: "a", ID: "a1", Args: `{}`},
			wefttest.Call{Name: "b", ID: "b1", Args: `{}`},
			wefttest.Call{Name: "c", ID: "c1", Args: `{}`},
		),
	), slow("a"), slow("b"), slow("c")))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(bg) }()
	start := time.Now()
	turn, err := s.Send(bg, core.User("go"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = turn.Wait()
	if d := time.Since(start); d > 4*time.Second {
		t.Fatalf("the turn ended after %v: its abandoned handlers were waited for one after another", d)
	}
	if p := s.Pending(); len(p) != 3 {
		t.Fatalf("Pending = %+v, want the three calls for a person", p)
	}
}
