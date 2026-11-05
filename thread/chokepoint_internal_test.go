package thread

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
)

// The choke point itself (release-gate review): with a recovery
// request undecided, every internal arming function refuses —
// armResumeLocked, armSettledLocked's AutoResume arm, the runner's own
// mintResumeLocked hand-offs, armRecoveredLocked — and none registers
// a resume; the runner's start check (resumeRefusedLocked, run by
// runResume) refuses a resume armed before the request appeared. The
// request is ship's: a ReplayNever call whose handler lost its answer
// (core.ErrOutcomeUnknown — not a short Timeout, which can pass while
// the dispatch gate still records the call and then starts nothing).
func TestEveryArmingFunctionRefusesAnUndecidedRecoveryRequest(t *testing.T) {
	ctx := context.Background()
	ship := core.Tool("ship", "Ship.", func(context.Context, struct{}) (string, error) {
		return "", fmt.Errorf("%w: the carrier's answer was lost", core.ErrOutcomeUnknown)
	}, core.Replay(core.ReplayNever))
	s, err := Create(ctx, Memory(), core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{}`}),
		wefttest.Say("x"),
	), ship))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(ctx) }()
	turn, err := s.Send(ctx, core.User("go"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = turn.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	if p := s.pendingLocked(); len(p) != 1 || !p[0].Recovery {
		t.Fatalf("pending = %+v, want one recovery request", p)
	}
	refused := func(name string, rt *Turn, err error) {
		t.Helper()
		if !errors.Is(err, ErrRecoveryRequired) || rt != nil {
			t.Errorf("%s = %v, %v; want ErrRecoveryRequired", name, rt, err)
		}
		if s.await.resumed != nil {
			t.Fatalf("%s registered a resume", name)
		}
	}
	rt, err := s.armResumeLocked(ctx)
	refused("armResumeLocked", rt, err)
	rt, err = s.mintResumeLocked()
	refused("mintResumeLocked", rt, err)
	if err := s.resumeRefusedLocked(); !errors.Is(err, ErrRecoveryRequired) {
		t.Errorf("resumeRefusedLocked = %v", err)
	}
	// The paths that arm only a settled boundary arm nothing.
	if rt, err := s.armSettledLocked(ctx); rt != nil || err != nil {
		t.Errorf("armSettledLocked = %v, %v", rt, err)
	}
	if rt, err := s.armRecoveredLocked(ctx); rt != nil || err != nil {
		t.Errorf("armRecoveredLocked = %v, %v", rt, err)
	}
	if s.await.resumed != nil {
		t.Fatal("a resume was registered")
	}
}

// P2-2: the abandoned-handler registry holds only handlers still
// running — an entry is released when its handler returns, whatever
// became of the request (here: still pending, then denied).
//
// The handler must start before its Timeout passes, and a Timeout
// spans the dispatch gate: one that passes while the gate is still
// recording the call (a loaded machine) ends a call that never started
// — nothing abandoned. Another session tries again then, with twice
// the Timeout.
func TestAbandonedRegistryReleasesOnReturn(t *testing.T) {
	ctx := context.Background()
	release := make(chan struct{})
	var s *Session
	for timeout, attempt := 100*time.Millisecond, 1; ; timeout, attempt = 2*timeout, attempt+1 {
		var started atomic.Bool
		ship := core.Tool("ship", "Ship.", func(context.Context, struct{}) (string, error) {
			started.Store(true)
			<-release
			return "shipped", nil
		}, core.Replay(core.ReplayNever), core.Timeout(timeout))
		var err error
		s, err = Create(ctx, Memory(), core.New(wefttest.Script(
			wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{}`}),
			wefttest.Say("skipped"),
		), ship))
		if err != nil {
			t.Fatal(err)
		}
		turn, err := s.Send(ctx, core.User("go"))
		if err != nil {
			t.Fatal(err)
		}
		_, _ = turn.Wait()
		if started.Load() {
			break
		}
		_ = s.Close(ctx) // timed out in the gate: never started
		t.Logf("the call timed out in the dispatch gate under a Timeout of %v: again, with twice that", timeout)
		if attempt == 8 {
			t.Fatalf("the call timed out in the dispatch gate 8 times running (the last Timeout %v)", timeout)
		}
	}
	defer func() { _ = s.Close(ctx) }()
	p := s.Pending()
	if len(p) != 1 || p[0].Reason != reasonStillRunning {
		t.Fatalf("pending = %+v", p)
	}
	k := abandonKey{run: p[0].RunID, call: "s1"}
	held := func() int {
		abandoned.mu.Lock()
		defer abandoned.mu.Unlock()
		return len(abandoned.m[k])
	}
	if got := held(); got != 1 {
		t.Fatalf("registry holds %d for the call, want 1", got)
	}
	// Denied (skipped) while it runs: the entry stays — the handler
	// still runs — until it returns.
	rt, err := s.Decide(ctx, Deny("s1", "").Bind(p[0]))
	if err != nil || rt == nil {
		t.Fatalf("Decide = %v, %v", rt, err)
	}
	if _, err := rt.Wait(); err != nil {
		t.Fatal(err)
	}
	if got := held(); got != 1 {
		t.Fatalf("registry holds %d after the skip, want 1 (the handler still runs)", got)
	}
	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for held() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := held(); got != 0 {
		t.Fatalf("registry holds %d after the handler returned, want 0", got)
	}
}
