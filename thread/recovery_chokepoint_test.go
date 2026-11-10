package thread_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
	"github.com/weftbase/weft/thread"
)

// everyStore is the storages a choke-point test runs over: memory and
// the two file-backed stores.
func everyStore(t *testing.T) []durableBackend {
	t.Helper()
	out := []durableBackend{{name: "memory", open: func(*testing.T) thread.Storage { return thread.Memory() }}}
	return append(out, durableBackends(t)...)
}

// pathResult returns the tool result the session's active path holds
// for callID, if any.
func pathResult(t *testing.T, s *thread.Session, callID string) (string, bool) {
	t.Helper()
	path, err := s.Path(s.Leaf())
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range path {
		if me, ok := e.(thread.MessageEntry); ok {
			for _, part := range me.Message.Content {
				if r, ok := part.(core.ToolResultPart); ok && r.CallID == callID {
					return r.Content, true
				}
			}
		}
	}
	return "", false
}

// pendingRecovery returns the pending recovery request for callID.
func pendingRecovery(s *thread.Session, callID string) (thread.Request, bool) {
	for _, r := range s.Pending() {
		if r.CallID == callID && r.Recovery {
			return r, true
		}
	}
	return thread.Request{}, false
}

// P1-A (release-gate review): an Interrupt fells a resume while its
// approved call with effects runs. The felled call may have run: it
// parks as a recovery request, and the runner's re-arm for the
// interrupt's follow-up must not resume over it — the resume would
// settle it "DENIED: no decision" and the model would ask again. The
// boundary stays parked, the follow-up queued behind it, until a
// person decides; the skip then reads OUTCOME_UNKNOWN, never DENIED.
func TestInterruptedResumeLeavesItsRecoveryRequestForAPerson(t *testing.T) {
	for _, sc := range everyStore(t) {
		t.Run(sc.name, func(t *testing.T) {
			ctx := context.Background()
			inPay := make(chan struct{}, 4)
			var paid atomic.Int32
			pay := core.Tool("pay", "Pay.", func(ctx context.Context, _ struct{}) (string, error) {
				paid.Add(1)
				inPay <- struct{}{}
				<-ctx.Done()
				return "", ctx.Err()
			}, core.RequireApproval())
			model := wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "p1", Args: `{}`}),
				wefttest.Say("after pay"),
				wefttest.Say("the follow-up"),
			)
			s, err := thread.Create(ctx, sc.open(t), core.New(model, pay))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close(ctx) }()
			t1, err := s.Send(ctx, core.User("pay"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := t1.Wait(); err != nil {
				t.Fatal(err)
			}
			p := s.Pending()
			if len(p) != 1 {
				t.Fatalf("Pending = %+v", p)
			}
			rt, err := s.Decide(ctx, thread.Approve("p1").Bind(p[0]))
			if err != nil || rt == nil {
				t.Fatalf("Decide = %v, %v", rt, err)
			}
			<-inPay
			t2, err := s.Send(ctx, core.User("stop"), thread.As(thread.Interrupt))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := rt.Wait(); !errors.Is(err, thread.ErrRecoveryRequired) {
				t.Fatalf("felled resume Wait = %v, want ErrRecoveryRequired", err)
			}
			// Give a wrong re-arm every chance to run.
			time.Sleep(100 * time.Millisecond)
			req, ok := pendingRecovery(s, "p1")
			if !ok {
				t.Fatalf("Pending = %+v, want p1's recovery request", s.Pending())
			}
			if got, ok := pathResult(t, s, "p1"); ok {
				t.Fatalf("p1 settled as %q before a person decided", got)
			}
			select {
			case <-t2.Done():
				t.Fatal("the interrupt's follow-up ran past an undecided recovery request")
			default:
			}
			if rt, err := s.Resume(ctx); !errors.Is(err, thread.ErrRecoveryRequired) || rt != nil {
				t.Fatalf("Resume = %v, %v; want ErrRecoveryRequired", rt, err)
			}
			// A person skips it: the resume runs, then the follow-up.
			rt2, err := s.Decide(ctx, thread.Deny("p1", "not again").Bind(req))
			if err != nil || rt2 == nil {
				t.Fatalf("Decide(skip) = %v, %v", rt2, err)
			}
			if res, err := rt2.Wait(); err != nil || res.Text() != "after pay" {
				t.Fatalf("resume = %v, %v", res, err)
			}
			if res, err := t2.Wait(); err != nil || res.Text() != "the follow-up" {
				t.Fatalf("follow-up = %v, %v", res, err)
			}
			got, _ := pathResult(t, s, "p1")
			if strings.Contains(got, "DENIED") || !strings.Contains(got, core.CodeOutcomeUnknown) {
				t.Fatalf("p1's result = %q, want the skipped OUTCOME_UNKNOWN text", got)
			}
			if n := paid.Load(); n != 1 {
				t.Fatalf("pay ran %d times", n)
			}
		})
	}
}

// recoveryBoundary parks a turn on two calls: "ask", an ordinary
// approval request, and "ship", a ReplayNever call whose handler lost
// its answer (core.ErrOutcomeUnknown) — a recovery request (outcome
// unknown) for a person. It returns the session and how many times
// ship's handler ran.
//
// ship says it lost its answer rather than outliving a short Timeout:
// a Timeout spans the dispatch gate too, and one that passes while the
// gate is still recording the call (a loaded machine) ends a call that
// never started — answered as timed out, no recovery request
// (TestTimeoutInTheDispatchGateStartsNothing).
func recoveryBoundary(t *testing.T, st thread.Storage, opts ...thread.SessionOption) (*thread.Session, *atomic.Int32) {
	t.Helper()
	var shipped atomic.Int32
	ship := core.Tool("ship", "Ship.", func(context.Context, struct{}) (string, error) {
		shipped.Add(1)
		return "", fmt.Errorf("%w: the carrier's answer was lost", core.ErrOutcomeUnknown)
	}, core.Replay(core.ReplayNever))
	ask := core.Tool("ask", "Ask.", func(context.Context, struct{}) (string, error) {
		return "asked", nil
	}, core.RequireApproval())
	model := wefttest.Script(
		wefttest.ToolCalls(
			wefttest.Call{Name: "ask", ID: "a1", Args: `{}`},
			wefttest.Call{Name: "ship", ID: "s1", Args: `{}`},
		),
		wefttest.Say("resumed"),
		wefttest.Say("follow-up"),
	)
	s, err := thread.Create(bg, st, core.New(model, ship, ask), opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(bg) })
	turn, err := s.Send(bg, core.User("go"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = turn.Wait()
	if _, ok := pendingRecovery(s, "s1"); !ok {
		t.Fatalf("Pending = %+v, want s1's recovery request", s.Pending())
	}
	return s, &shipped
}

// approveAsk records the ordinary request's approval: what is left
// undecided is the recovery request alone. The decision must arm
// nothing.
func approveAsk(t *testing.T, s *thread.Session) {
	t.Helper()
	for _, r := range s.Pending() {
		if r.CallID == "a1" {
			rt, err := s.Decide(bg, thread.Approve("a1").Bind(r))
			if err != nil || rt != nil {
				t.Fatalf("Decide(a1) = %v, %v; want recorded, nothing armed", rt, err)
			}
			return
		}
	}
	t.Fatalf("no request for a1: %+v", s.Pending())
}

// assertStillParked checks that nothing settled the recovery request:
// it is pending, s1 has no result, nothing says "no decision", and
// ship ran once.
func assertStillParked(t *testing.T, s *thread.Session, shipped *atomic.Int32) {
	t.Helper()
	time.Sleep(50 * time.Millisecond) // a wrong arm's resume would have run by now
	if _, ok := pendingRecovery(s, "s1"); !ok {
		t.Fatalf("Pending = %+v: the recovery request was settled without a person", s.Pending())
	}
	if got, ok := pathResult(t, s, "s1"); ok {
		t.Fatalf("s1 settled as %q", got)
	}
	for _, e := range s.Entries() {
		if me, ok := e.(thread.MessageEntry); ok && strings.Contains(me.Message.Text(), "no decision") {
			t.Fatalf("an entry reads %q", me.Message.Text())
		}
	}
	if n := shipped.Load(); n != 1 {
		t.Fatalf("ship ran %d times", n)
	}
}

// The choke point (release-gate review): every path that arms a resume
// meets an undecided recovery request and refuses — the resume would
// settle it "DENIED: no decision" — and the boundary stays parked for
// a person. The interrupt re-arm has its own test
// (TestInterruptedResumeLeavesItsRecoveryRequestForAPerson), the pool's
// replay its own in thread/pool (TestPoolReplayNeverSettlesAnUndecidedRecoveryRequest).
func TestNoArmingPathSettlesAnUndecidedRecoveryRequest(t *testing.T) {
	for _, sc := range everyStore(t) {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			t.Run("Resume", func(t *testing.T) {
				s, shipped := recoveryBoundary(t, sc.open(t))
				approveAsk(t, s)
				if rt, err := s.Resume(bg); !errors.Is(err, thread.ErrRecoveryRequired) || rt != nil {
					t.Fatalf("Resume = %v, %v; want ErrRecoveryRequired", rt, err)
				}
				assertStillParked(t, s, shipped)
			})
			t.Run("Decide", func(t *testing.T) {
				s, shipped := recoveryBoundary(t, sc.open(t))
				approveAsk(t, s) // AutoResume's settled pickup: nothing to arm
				assertStillParked(t, s, shipped)
			})
			t.Run("Send", func(t *testing.T) {
				s, shipped := recoveryBoundary(t, sc.open(t))
				approveAsk(t, s)
				ft, err := s.Send(bg, core.User("more"))
				if err != nil || ft == nil {
					t.Fatalf("Send = %v, %v; want queued", ft, err)
				}
				assertStillParked(t, s, shipped)
				select {
				case <-ft.Done():
					t.Fatal("the queued send ran past the boundary")
				default:
				}
			})
			t.Run("Continue", func(t *testing.T) {
				s, shipped := recoveryBoundary(t, sc.open(t))
				approveAsk(t, s)
				if _, err := s.Continue(bg); err != nil {
					t.Fatalf("Continue = %v", err)
				}
				assertStillParked(t, s, shipped)
			})
			t.Run("ManualResume", func(t *testing.T) {
				// Without AutoResume a settled boundary is the caller's to
				// start; a Resume over the recovery request still refuses.
				s, shipped := recoveryBoundary(t, sc.open(t), thread.AutoResume(false))
				approveAsk(t, s)
				if rt, err := s.Resume(bg); !errors.Is(err, thread.ErrRecoveryRequired) || rt != nil {
					t.Fatalf("Resume = %v, %v; want ErrRecoveryRequired", rt, err)
				}
				assertStillParked(t, s, shipped)
			})
			t.Run("Reopen", func(t *testing.T) {
				// A reopened session's arming paths are the same paths.
				st := sc.open(t)
				s, shipped := recoveryBoundary(t, st)
				approveAsk(t, s)
				if err := s.Close(bg); err != nil {
					t.Fatal(err)
				}
				s2, err := thread.Open(bg, st, s.ID(), core.New(wefttest.Script(wefttest.Say("x")),
					core.Tool("ship", "Ship.", func(context.Context, struct{}) (string, error) { return "", nil }),
					core.Tool("ask", "Ask.", func(context.Context, struct{}) (string, error) { return "", nil }, core.RequireApproval())))
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = s2.Close(bg) }()
				if _, err := s2.Continue(bg); err != nil {
					t.Fatalf("Continue = %v", err)
				}
				if rt, err := s2.Resume(bg); !errors.Is(err, thread.ErrRecoveryRequired) || rt != nil {
					t.Fatalf("Resume = %v, %v; want ErrRecoveryRequired", rt, err)
				}
				assertStillParked(t, s2, shipped)
			})
			t.Run("ReplayDecisions", func(t *testing.T) {
				s, shipped := recoveryBoundary(t, sc.open(t), thread.WithLineage("s_parent", "c-wrapper"))
				var ask thread.Request
				for _, r := range s.Pending() {
					if r.CallID == "a1" {
						ask = r
					}
				}
				rt, err := s.ReplayDecisions(bg, thread.ApprovalDecisionEntry{
					CallID: "a1", Outcome: thread.OutcomeApprove, Who: "alice", ArgsSHA256: ask.ArgsSHA256,
				})
				if err != nil || rt != nil {
					t.Fatalf("ReplayDecisions = %v, %v; want recorded, nothing armed", rt, err)
				}
				assertStillParked(t, s, shipped)
			})
			t.Run("DecidedThenResumes", func(t *testing.T) {
				// Once a person decides it, the boundary resumes, the
				// skip reading OUTCOME_UNKNOWN.
				s, shipped := recoveryBoundary(t, sc.open(t))
				approveAsk(t, s)
				req, _ := pendingRecovery(s, "s1")
				rt, err := s.Decide(bg, thread.Deny("s1", "").Bind(req))
				if err != nil || rt == nil {
					t.Fatalf("Decide(skip) = %v, %v", rt, err)
				}
				if res, err := rt.Wait(); err != nil || res.Text() != "resumed" {
					t.Fatalf("resume = %v, %v", res, err)
				}
				if got, _ := pathResult(t, s, "s1"); !strings.Contains(got, core.CodeOutcomeUnknown) {
					t.Fatalf("s1's result = %q", got)
				}
				if n := shipped.Load(); n != 1 {
					t.Fatalf("ship ran %d times", n)
				}
			})
		})
	}
}
