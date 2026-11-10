package pool_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/thread/pool"
)

// crashImage is what a process that died now leaves behind: a fresh
// store holding exactly the entries of ids that landed, no lease of the
// dead writer's. The dead process's pool is closed when the test ends,
// its writes going to the old store only.
func crashImage(t *testing.T, st thread.Storage, dead *pool.Pool, ids ...string) thread.Storage {
	t.Helper()
	ctx := context.Background()
	image := thread.Memory()
	for _, id := range ids {
		h, entries, _, err := st.Load(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if err := image.Create(ctx, h); err != nil {
			t.Fatal(err)
		}
		if err := image.Append(ctx, id, entries...); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		_ = dead.Close(cctx)
	})
	return image
}

// keyedCharge is a charge tool that records each run's idempotency
// key; with in set, the handler signals in and holds until its context
// ends (the process dies inside it). opts set its replay class and
// gate.
type keyedCharge struct {
	mu   sync.Mutex
	keys []string
}

func (k *keyedCharge) def(in chan struct{}, opts ...core.ToolOption) *core.ToolDef {
	return core.Tool("charge", "Charge.", func(ctx context.Context, _ struct{}) (string, error) {
		call, _ := core.CallFromContext(ctx)
		k.mu.Lock()
		k.keys = append(k.keys, call.IdempotencyKey)
		k.mu.Unlock()
		if in == nil {
			return "charged", nil
		}
		select {
		case <-in:
		default:
			close(in)
		}
		<-ctx.Done()
		return "", ctx.Err()
	}, opts...)
}

func (k *keyedCharge) ran() []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]string(nil), k.keys...)
}

// P1-a: two crashes. The first cuts the child off in its model call;
// the next process recovers it — a continuation run, named only by the
// recovery's audit — and a second crash cuts the continuation off
// before its turn entry lands. The third process must read the
// continuation as cut off (recover it again), never the first cut-off
// turn's error as the child's failure: the delegation ends done with
// the child's answer.
func TestRecoverDoubleCrashContinuationEndsDone(t *testing.T) {
	ctx := context.Background()
	st := thread.Memory()
	parent, err := thread.Create(ctx, st, core.New(wefttest.Script()))
	if err != nil {
		t.Fatal(err)
	}
	in := make(chan struct{})
	p := pool.New(1)
	r, err := p.Submit(ctx, parent, core.New(blockModel{Model: wefttest.Script(), in: in}), "go")
	if err != nil {
		t.Fatal(err)
	}
	<-in
	st2 := crashImage(t, st, p, parent.ID(), r.Child) // crash 1, inside the model call

	parent2, err := thread.Open(ctx, st2, parent.ID(), core.New(wefttest.Script()))
	if err != nil {
		t.Fatal(err)
	}
	in2 := make(chan struct{})
	p2 := pool.New(1)
	if err := p2.Register(r.Child, core.New(blockModel{Model: wefttest.Script(), in: in2})); err != nil {
		t.Fatal(err)
	}
	if err := p2.Recover(ctx, parent2); err != nil {
		t.Fatalf("Recover = %v", err)
	}
	<-in2
	st3 := crashImage(t, st2, p2, parent.ID(), r.Child) // crash 2, inside the continuation's model call

	parent3, err := thread.Open(ctx, st3, parent.ID(), core.New(wefttest.Script()))
	if err != nil {
		t.Fatal(err)
	}
	p3 := pool.New(1)
	defer func() { _ = p3.Close(ctx) }()
	if err := p3.Register(r.Child, core.New(wefttest.Script(wefttest.Say("recovered")))); err != nil {
		t.Fatal(err)
	}
	if err := p3.Recover(ctx, parent3); err != nil {
		t.Fatalf("Recover = %v", err)
	}
	wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
	defer wcancel()
	got, err := p3.Wait(wctx, parent3, r.ID)
	if err != nil || got.State != pool.Done || got.Stop != "recovered" {
		t.Fatalf("receipt after a second crash = %+v (err %v), want done with the child's answer", got, err)
	}
}

// P1-b: a crash inside a ReplayKeyed call; the next process's recovery
// retries it (the session's own approval, a resume), and a second
// crash lands inside the retry. The resume started and never ended: the
// third process recovers the child again, never reads it as parked
// with nothing to decide. The call runs once more under its same key
// and the delegation ends done.
func TestRecoverDoubleCrashKeyedRetryRunsOnceMore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	charge := &keyedCharge{}
	parent, image, r := crashInKeyedChild(t, charge, 2)

	p3 := pool.New(1)
	defer func() { _ = p3.Close(ctx) }()
	if err := p3.Register(r.Child, core.New(wefttest.Script(wefttest.Say("ok")), charge.def(nil, core.Replay(core.ReplayKeyed)))); err != nil {
		t.Fatal(err)
	}
	parent3, err := thread.Open(ctx, image, parent, core.New(wefttest.Script()))
	if err != nil {
		t.Fatal(err)
	}
	if err := p3.Recover(ctx, parent3); err != nil {
		t.Fatalf("Recover = %v", err)
	}
	wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
	defer wcancel()
	got, err := p3.Wait(wctx, parent3, r.ID)
	if err != nil || got.State != pool.Done || got.Stop != "ok" {
		t.Fatalf("receipt = %+v (err %v; pending %+v), want done", got, err, parent3.Pending())
	}
	keys := charge.ran()
	if len(keys) != 3 {
		t.Fatalf("charge ran %d times (%v), want 3: twice cut off, once to its answer", len(keys), keys)
	}
	for _, k := range keys {
		if k == "" || k != keys[0] {
			t.Fatalf("keys %v: every run of the call carries its one idempotency key", keys)
		}
	}
}

// The bound (R12) across crashes: every retry the session made of the
// keyed call was cut off by a crash. The retries spent, the next
// process does not retry again: the call parks as a recovery request
// for a person, mirrored onto the parent — the delegation stays open,
// never failed.
func TestRecoverKeyedRetriesSpentAcrossCrashesAsksAPerson(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	charge := &keyedCharge{}
	parent, image, r := crashInKeyedChild(t, charge, 4) // the first run and three retries

	p5 := pool.New(1)
	defer func() { _ = p5.Close(ctx) }()
	if err := p5.Register(r.Child, core.New(wefttest.Script(wefttest.Say("ok")), charge.def(nil, core.Replay(core.ReplayKeyed)))); err != nil {
		t.Fatal(err)
	}
	parent5, err := thread.Open(ctx, image, parent, core.New(wefttest.Script()))
	if err != nil {
		t.Fatal(err)
	}
	if err := p5.Recover(ctx, parent5); err != nil {
		t.Fatalf("Recover = %v", err)
	}
	pend := waitPending(t, parent5)
	if len(pend) != 1 || !strings.Contains(pend[0].Reason, "retries are spent") || pend[0].Child != r.Child {
		t.Fatalf("pending = %+v, want the child's call mirrored as a recovery request (retries spent)", pend)
	}
	if n := len(charge.ran()); n != 4 {
		t.Fatalf("charge ran %d times, want 4 (no retry past the cap)", n)
	}
	if rc := receiptOf(parent5, r.ID); rc.Settled() {
		t.Fatalf("receipt = %+v, want open", rc)
	}
}

// crashInKeyedChild delegates a child whose ReplayKeyed charge blocks,
// and kills the process inside the charge crashes times: the first
// run, then each recovery's retry. It returns the parent's id, the
// image the last crash left and the receipt.
func crashInKeyedChild(t *testing.T, charge *keyedCharge, crashes int) (string, thread.Storage, *pool.Receipt) {
	t.Helper()
	ctx := context.Background()
	st := thread.Memory()
	parent, err := thread.Create(ctx, st, core.New(wefttest.Script()))
	if err != nil {
		t.Fatal(err)
	}
	in := make(chan struct{})
	p := pool.New(1)
	r, err := p.Submit(ctx, parent, core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "charge", ID: "c1", Args: `{}`}),
	), charge.def(in, core.Replay(core.ReplayKeyed))), "go")
	if err != nil {
		t.Fatal(err)
	}
	<-in
	image := crashImage(t, st, p, parent.ID(), r.Child)
	for i := 1; i < crashes; i++ {
		next, err := thread.Open(ctx, image, parent.ID(), core.New(wefttest.Script()))
		if err != nil {
			t.Fatal(err)
		}
		in := make(chan struct{})
		pi := pool.New(1)
		if err := pi.Register(r.Child, core.New(wefttest.Script(), charge.def(in, core.Replay(core.ReplayKeyed)))); err != nil {
			t.Fatal(err)
		}
		if err := pi.Recover(ctx, next); err != nil {
			t.Fatalf("Recover %d = %v", i, err)
		}
		select {
		case <-in:
		case <-time.After(20 * time.Second): // the retry backs off 1 s, 2 s, 4 s
			t.Fatalf("process %d: the keyed retry never ran (pending %+v)", i+1, next.Pending())
		}
		image = crashImage(t, image, pi, parent.ID(), r.Child)
	}
	return parent.ID(), image, r
}

// waitPending waits for s to offer a request.
func waitPending(t *testing.T, s *thread.Session) []thread.Request {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if pend := s.Pending(); len(pend) > 0 {
			return pend
		}
		time.Sleep(5 * time.Millisecond)
	}
	return nil
}

func receiptOf(parent *thread.Session, id string) pool.Receipt {
	for _, rc := range pool.Receipts(parent) {
		if rc.ID == id {
			return rc
		}
	}
	return pool.Receipt{}
}

// A person approves a child's mirrored request, the child resumes, and
// the process dies inside the approved handler. The next process's
// Recover finds the resume started and never ended: a ReplayNever call
// may have run, so it is mirrored onto the parent as a recovery request
// — never re-run blind, never left with nothing to decide; a
// ReplayKeyed one runs once more under its key and the delegation ends
// done.
func TestRecoverCrashInsideAnApprovedChildResume(t *testing.T) {
	for _, keyed := range []bool{false, true} {
		name := "never"
		if keyed {
			name = "keyed"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			st := thread.Memory()
			parent, err := thread.Create(ctx, st, core.New(wefttest.Script()))
			if err != nil {
				t.Fatal(err)
			}
			opts := []core.ToolOption{core.RequireApproval()}
			if keyed {
				opts = append(opts, core.Replay(core.ReplayKeyed))
			}
			in := make(chan struct{})
			charge := &keyedCharge{}
			gated := func(in chan struct{}) *core.ToolDef { return charge.def(in, opts...) }
			p := pool.New(1)
			r, err := p.Submit(ctx, parent, core.New(wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "charge", ID: "c1", Args: `{}`}),
			), gated(in)), "go")
			if err != nil {
				t.Fatal(err)
			}
			pend := waitPending(t, parent)
			if len(pend) != 1 {
				t.Fatalf("pending %+v", pend)
			}
			if err := p.Decide(ctx, parent, bound(parent, thread.Approve(pend[0].CallID))); err != nil {
				t.Fatal(err)
			}
			select {
			case <-in:
			case <-time.After(5 * time.Second):
				t.Fatal("the approved handler never ran")
			}
			image := crashImage(t, st, p, parent.ID(), r.Child) // inside the approved handler

			parent2, err := thread.Open(ctx, image, parent.ID(), core.New(wefttest.Script()))
			if err != nil {
				t.Fatal(err)
			}
			p2 := pool.New(1)
			defer func() { _ = p2.Close(ctx) }()
			if err := p2.Register(r.Child, core.New(wefttest.Script(wefttest.Say("ok")), gated(nil))); err != nil {
				t.Fatal(err)
			}
			if err := p2.Recover(ctx, parent2); err != nil {
				t.Fatalf("Recover = %v", err)
			}
			if keyed {
				wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
				defer wcancel()
				got, err := p2.Wait(wctx, parent2, r.ID)
				if err != nil || got.State != pool.Done {
					t.Fatalf("receipt = %+v (err %v), want done", got, err)
				}
				if keys := charge.ran(); len(keys) != 2 || keys[0] != keys[1] {
					t.Fatalf("keys %v, want the call run once more under its key", keys)
				}
				return
			}
			pend = waitPending(t, parent2)
			if len(pend) != 1 || !strings.Contains(pend[0].Reason, "may have run") {
				t.Fatalf("pending = %+v, want the call mirrored as a recovery request", pend)
			}
			if n := len(charge.ran()); n != 1 {
				t.Fatalf("charge ran %d times, want 1 (never re-run blind)", n)
			}
		})
	}
}

// delegatingChild is a child agent whose first step delegates to a
// grandchild through the wrap "level2" (call "d"); answer is its
// answer after that.
func delegatingChild(p *pool.Pool, grandchild *core.Agent, first bool) *core.Agent {
	var turns []wefttest.Turn
	if first {
		turns = append(turns, wefttest.ToolCalls(wefttest.Call{Name: "level2", ID: "d",
			Args: wefttest.Args(struct{ Prompt string }{"go"})}))
	}
	turns = append(turns, wefttest.Say("level1 done"))
	return core.New(wefttest.Script(turns...), p.MustWrap("level2", "the next level", grandchild))
}

// resultOf is the result the session holds for callID, "" when none.
func resultOf(t *testing.T, st thread.Storage, id, callID string) string {
	t.Helper()
	_, entries, _, err := st.Load(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	out := ""
	for _, e := range entries {
		me, ok := e.(thread.MessageEntry)
		if !ok {
			continue
		}
		for _, part := range me.Message.Content {
			if r, ok := part.(core.ToolResultPart); ok && r.CallID == callID {
				out = r.Content
			}
		}
	}
	return out
}

// P1-c: a child cut off while it was itself delegating is never
// settled failed — what its delegation did is not known, and a parent
// told "failed" may delegate the work again. Recovery is not built at
// depth: the child's own delegations are ended depth first, and its
// delegating call of unknown outcome goes to a person, mirrored onto
// the parent as a recovery request, whatever its tool's replay class.
// Deny skips it — the child's model reads OUTCOME_UNKNOWN ("may have
// run … do not repeat it") and goes on; approve runs the delegating
// call again (a new grandchild).
func TestRecoverCutOffDelegatingChildAsksAPerson(t *testing.T) {
	for _, approve := range []bool{false, true} {
		name := "deny"
		if approve {
			name = "approve"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			st := thread.Memory()
			parent, err := thread.Create(ctx, st, core.New(wefttest.Script()))
			if err != nil {
				t.Fatal(err)
			}
			in := make(chan struct{})
			p := pool.New(2)
			r, err := p.Submit(ctx, parent, delegatingChild(p, core.New(blockModel{Model: wefttest.Script(), in: in}), true), "go")
			if err != nil {
				t.Fatal(err)
			}
			<-in // the grandchild runs: the child is in its delegating call
			_, entries, _, err := st.Load(ctx, r.Child)
			if err != nil {
				t.Fatal(err)
			}
			grand := ""
			for _, e := range entries {
				if pr, ok := e.(thread.PoolReceiptEntry); ok && pr.Child != "" {
					grand = pr.Child
				}
			}
			image := crashImage(t, st, p, parent.ID(), r.Child, grand)

			parent2, err := thread.Open(ctx, image, parent.ID(), core.New(wefttest.Script()))
			if err != nil {
				t.Fatal(err)
			}
			p2 := pool.New(2)
			defer func() { _ = p2.Close(ctx) }()
			grandchild := core.New(wefttest.Script(wefttest.Say("level2 done")))
			if err := p2.Register(r.Child, delegatingChild(p2, grandchild, false)); err != nil {
				t.Fatal(err)
			}
			if err := p2.Recover(ctx, parent2); err != nil {
				t.Fatalf("Recover = %v", err)
			}
			if rc := receiptOf(parent2, r.ID); rc.Settled() {
				t.Fatalf("receipt = %+v: a child cut off while delegating is never settled", rc)
			}
			pend := waitPending(t, parent2)
			if len(pend) != 1 || pend[0].Child != r.Child || !pend[0].Recovery || pend[0].Tool != "level2" ||
				!strings.Contains(pend[0].Reason, "delegated work may have run") {
				t.Fatalf("parent Pending = %+v, want the child's delegating call as a recovery request", pend)
			}
			if rs := pool.Receipts(readOnly(t, image, r.Child)); len(rs) != 1 || !rs[0].Settled() {
				t.Fatalf("the child's receipts = %+v, want its delegation ended", rs)
			}
			d := thread.Deny(pend[0].CallID, "checked by hand")
			if approve {
				d = thread.Approve(pend[0].CallID)
			}
			if err := p2.Decide(ctx, parent2, bound(parent2, d)); err != nil {
				t.Fatal(err)
			}
			rc := waitFor(t, p2, parent2, r.ID, pool.Done)
			if rc.Stop != "level1 done" {
				t.Fatalf("receipt = %+v, want the child's answer", rc)
			}
			got := resultOf(t, image, r.Child, "d")
			if approve {
				if got != "level2 done" {
					t.Fatalf("the re-run delegation's result = %q", got)
				}
				return
			}
			if !strings.Contains(got, "OUTCOME_UNKNOWN") || !strings.Contains(got, "may have run") ||
				!strings.Contains(got, "do not repeat") || !strings.Contains(got, "checked by hand") {
				t.Fatalf("the skipped delegation reads %q", got)
			}
		})
	}
}
