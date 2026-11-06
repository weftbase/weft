package thread_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/thread/jsonl"
	"github.com/weftbase/weft/wefttest"
)

// restartRig is a session whose agent has an approval-gated tool
// "ship", a plain tool "wipe" and a plain tool "lookup", rebuilt per
// process over one jsonl directory: what each handler saw is counted
// across the restart.
type restartRig struct {
	st     thread.Storage
	wiped  atomic.Int32
	mu     sync.Mutex
	shipMD []map[string]string
}

func newRestartRig(t *testing.T) *restartRig {
	t.Helper()
	st, err := jsonl.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return &restartRig{st: st}
}

func (r *restartRig) agent(turns ...wefttest.Turn) *core.Agent {
	ship := core.Tool("ship", "Ship an order.",
		func(ctx context.Context, in refundInput) (string, error) {
			r.mu.Lock()
			r.shipMD = append(r.shipMD, core.MetadataFromContext(ctx))
			r.mu.Unlock()
			return "shipped " + in.OrderID, nil
		}, core.RequireApproval())
	wipe := core.Tool("wipe", "Wipe the account.",
		func(ctx context.Context, in refundInput) (string, error) {
			r.wiped.Add(1)
			return "wiped", nil
		})
	lookup := core.Tool("lookup", "Look an order up.",
		func(ctx context.Context, in refundInput) (string, error) { return "found", nil })
	return core.New(wefttest.Script(turns...), core.Name("restart-rig"), ship, wipe, lookup)
}

// park sends one turn under opts that parks call_1 to ship, then drops
// the process: the returned id is reopened by the caller.
func (r *restartRig) park(t *testing.T, ctx context.Context, opts ...core.RunOption) string {
	t.Helper()
	s, err := thread.Create(ctx, r.st, r.agent(wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "call_1", Args: `{"order_id":"1"}`})))
	if err != nil {
		t.Fatal(err)
	}
	turn, err := s.Send(ctx, core.User("ship it"), thread.RunOptions(opts...))
	if err != nil {
		t.Fatal(err)
	}
	res, err := turn.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Pending) != 1 {
		t.Fatalf("pending before restart: %+v", res.Pending)
	}
	abandon(t, r.st, s.ID())
	return s.ID()
}

// resume reopens id with a fresh agent scripted with turns and approves
// call_1 under ctx, returning the resume's outcome.
func (r *restartRig) resume(t *testing.T, ctx context.Context, id string, turns ...wefttest.Turn) (*core.RunResult, error) {
	t.Helper()
	s, err := thread.Open(ctx, r.st, id, r.agent(turns...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	rt, err := s.Decide(ctx, bound(s, thread.Approve("call_1")))
	if err != nil {
		t.Fatal(err)
	}
	return rt.Wait()
}

// The run options and context a parked send ran under keep binding
// its resume after a restart: OnlyTools still narrows (a tool narrowed
// away never runs), a lowered MaxSteps still bounds the resumed run,
// and the caller's metadata — a tenant the context carried, a pair the
// run options added — still tags it. Before the fix all three were
// process state, lost with the parked turn.
func TestRunSettingsSurviveRestart(t *testing.T) {
	ctx := context.Background()

	t.Run("only_tools", func(t *testing.T) {
		r := newRestartRig(t)
		id := r.park(t, ctx, core.OnlyTools("ship", "lookup"))
		_, err := r.resume(t, ctx, id,
			wefttest.ToolCalls(wefttest.Call{Name: "wipe", ID: "call_2", Args: `{"order_id":"1"}`}),
			wefttest.Say("done"))
		if err != nil {
			t.Fatal(err)
		}
		if n := r.wiped.Load(); n != 0 {
			t.Fatalf("wipe ran %d time(s) after a restart; the parked send narrowed it away", n)
		}
	})

	t.Run("max_steps", func(t *testing.T) {
		r := newRestartRig(t)
		id := r.park(t, ctx, core.MaxSteps(2))
		look := func(id string) wefttest.Turn {
			return wefttest.ToolCalls(wefttest.Call{Name: "lookup", ID: id, Args: `{"order_id":"1"}`})
		}
		_, err := r.resume(t, ctx, id, look("call_2"), look("call_3"), look("call_4"), wefttest.Say("done"))
		if !errors.Is(err, core.ErrMaxSteps) {
			t.Fatalf("resume err = %v, want ErrMaxSteps: the parked send lowered MaxSteps to 2", err)
		}
	})

	t.Run("metadata", func(t *testing.T) {
		r := newRestartRig(t)
		id := r.park(t, core.WithMetadata(ctx, map[string]string{"tenant": "acme"}),
			core.Metadata(map[string]string{"order": "7"}))
		if _, err := r.resume(t, ctx, id, wefttest.Say("done")); err != nil {
			t.Fatal(err)
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		if len(r.shipMD) != 1 {
			t.Fatalf("ship ran %d times, want 1", len(r.shipMD))
		}
		if md := r.shipMD[0]; md["tenant"] != "acme" || md["order"] != "7" {
			t.Fatalf("resumed call's metadata = %v, want tenant=acme and order=7", md)
		}
	})
}

// A send queued behind an open boundary keeps its run options across a
// restart: the reopened session restores it from its accepted receipt
// and runs it narrowed and parked as it was sent, never with the
// agent's full set and nothing parked.
func TestQueuedSendSettingsSurviveRestart(t *testing.T) {
	ctx := context.Background()
	r := newRestartRig(t)
	s, err := thread.Create(ctx, r.st, r.agent(wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "call_1", Args: `{"order_id":"1"}`})))
	if err != nil {
		t.Fatal(err)
	}
	turn, err := s.Send(ctx, core.User("ship it"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := turn.Wait(); err != nil {
		t.Fatal(err)
	}
	// Queued behind the boundary, under a default-deny rule.
	if _, err := s.Send(ctx, core.User("now tidy up"), thread.RunOptions(core.ParkAllExcept("lookup"))); err != nil {
		t.Fatal(err)
	}
	id := s.ID()
	abandon(t, r.st, id)

	s2, err := thread.Open(ctx, r.st, id, r.agent(
		wefttest.Say("shipped"),
		wefttest.ToolCalls(wefttest.Call{Name: "wipe", ID: "call_2", Args: `{"order_id":"1"}`}),
		wefttest.Say("done"),
	))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close(context.Background()) })
	rt, err := s2.Decide(ctx, bound(s2, thread.Approve("call_1")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := s2.WaitIdle(ctx); err != nil {
		t.Fatal(err)
	}
	if n := r.wiped.Load(); n != 0 {
		t.Fatalf("the restored queued send ran wipe %d time(s) unasked; it was sent under ParkAllExcept(lookup)", n)
	}
	if p := s2.Pending(); len(p) != 1 || p[0].Tool != "wipe" {
		t.Fatalf("pending = %+v, want wipe parked", p)
	}
}

// A decider's own context metadata never hides the parked turn's: an
// inbox handler that tags its request context (WithMetadata) and calls
// Decide must not strip the tenant the parked turn ran under from the
// resumed run — in the same process, with no restart. The decider's
// pairs merge under the parked turn's.
func TestDeciderMetadataDoesNotHideTheParkedTurns(t *testing.T) {
	ctx := context.Background()
	r := newRestartRig(t)
	s, err := thread.Create(ctx, r.st, r.agent(
		wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "call_1", Args: `{"order_id":"1"}`}),
		wefttest.Say("done")))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(ctx) }()
	turn, err := s.Send(core.WithMetadata(ctx, map[string]string{"tenant": "acme"}), core.User("ship it"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := turn.Wait(); err != nil {
		t.Fatal(err)
	}
	inbox := core.WithMetadata(ctx, map[string]string{"inbox.request": "r-9"})
	rt, err := s.Decide(inbox, bound(s, thread.Approve("call_1")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Wait(); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.shipMD) != 1 {
		t.Fatalf("ship ran %d times", len(r.shipMD))
	}
	if md := r.shipMD[0]; md["tenant"] != "acme" || md["inbox.request"] != "r-9" {
		t.Fatalf("resumed call's metadata = %v, want the parked turn's tenant and the decider's pair", md)
	}
}

// A steer deferred behind an open boundary becomes a follow-up under the
// aimed turn's settings — its context's metadata included — even when
// the steer's own context carries metadata of its own: the pairs merge,
// the aimed turn's are not hidden. The follow-up's accepted receipt
// records what it will run under.
func TestSteerFollowUpKeepsTheAimedTurnsMetadata(t *testing.T) {
	ctx := context.Background()
	r := newRestartRig(t)
	s, err := thread.Create(ctx, r.st, r.agent(
		wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "call_1", Args: `{"order_id":"1"}`})))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(ctx) }()
	turn, err := s.Send(core.WithMetadata(ctx, map[string]string{"tenant": "acme"}), core.User("ship it"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := turn.Wait(); err != nil {
		t.Fatal(err)
	}
	chat := core.WithMetadata(ctx, map[string]string{"src": "chat"})
	steer, err := s.Send(chat, core.User("also tidy up"), thread.As(thread.Steer))
	if err != nil {
		t.Fatal(err)
	}
	// The steer's Turn ends when the steer reaches its final state
	// (Policy Steer's godoc). The parked turn's Wait returns before its
	// runner's epilogue, so a steer accepted in that window waits in the
	// live queue and defers in the epilogue, not inside Send: the
	// deferral — the follow-up's accepted receipt — is only promised
	// once the steer's Turn is done.
	select {
	case <-steer.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the steer's Turn never reached a final state")
	}
	if o := steer.Outcome(); o != thread.TurnDeferred || steer.Next() == nil {
		t.Fatalf("steer outcome = %v, next = %v; want deferred with a follow-up", o, steer.Next())
	}
	var run *core.RunSettings
	found := false
	for _, e := range s.Entries() {
		if rc, ok := e.(thread.ReceiptEntry); ok && rc.Status == thread.ReceiptAccepted && rc.Turn == steer.Next().ID() {
			run, found = rc.Run, true
		}
	}
	if !found {
		t.Fatalf("no accepted receipt names the follow-up turn %s", steer.Next().ID())
	}
	if run == nil || run.Metadata["tenant"] != "acme" || run.Metadata["src"] != "chat" {
		t.Fatalf("follow-up settings = %+v, want tenant=acme beside src=chat", run)
	}
}
