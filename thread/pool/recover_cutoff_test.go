package pool_test

import (
	"context"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/thread/pool"
)

// P1-4: a child a shutdown cut off is not settled failed by the next
// process's Recover — the parent could delegate the work again over an
// effect the child had. The child is recovered on its own ladder
// (thread.Session.RecoverTurn): here its model call was cut off, so it
// continues, and the delegation ends with the child's answer.
func TestRecoverResumesACutOffChild(t *testing.T) {
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
	cctx, cancel := context.WithCancelCause(ctx)
	cancel(thread.ErrShutdown)
	_ = p.Close(cctx)
	wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
	_ = p.Close(wctx)
	wcancel()

	// The next process: a fresh store holding exactly what landed, no
	// lease of the dead writers'.
	time.Sleep(50 * time.Millisecond)
	st2 := thread.Memory()
	for _, id := range []string{parent.ID(), r.Child} {
		h, entries, _, err := st.Load(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if err := st2.Create(ctx, h); err != nil {
			t.Fatal(err)
		}
		if err := st2.Append(ctx, id, entries...); err != nil {
			t.Fatal(err)
		}
	}
	parent, err = thread.Open(ctx, st2, parent.ID(), core.New(wefttest.Script()))
	if err != nil {
		t.Fatal(err)
	}
	p2 := pool.New(1)
	defer func() { _ = p2.Close(ctx) }()
	if err := p2.Register(r.Child, core.New(wefttest.Script(wefttest.Say("recovered")))); err != nil {
		t.Fatal(err)
	}
	if err := p2.Recover(ctx, parent); err != nil {
		t.Fatalf("Recover = %v", err)
	}
	wctx, wcancel = context.WithTimeout(ctx, 5*time.Second)
	defer wcancel()
	got, err := p2.Wait(wctx, parent, r.ID)
	if err != nil {
		t.Fatalf("Wait = %v (receipt %+v)", err, got)
	}
	if got.State != pool.Done || got.Stop != "recovered" {
		t.Fatalf("receipt = %+v, want done with the child's answer", got)
	}
}

// A child cut off inside a call with effects: Recover recovers it, the
// call parks as a recovery request in the child, and the parent shows
// it pending (mirrored) — an unknown outcome, never a failure.
func TestRecoverParksACutOffChildsUnknownCall(t *testing.T) {
	ctx := context.Background()
	st := thread.Memory()
	parent, err := thread.Create(ctx, st, core.New(wefttest.Script()))
	if err != nil {
		t.Fatal(err)
	}
	in := make(chan struct{})
	charge := func() *core.ToolDef {
		return core.Tool("charge", "Charge.", func(ctx context.Context, _ struct{}) (string, error) {
			select {
			case <-in:
			default:
				close(in)
			}
			<-ctx.Done()
			return "", ctx.Err()
		})
	}
	p := pool.New(1)
	r, err := p.Submit(ctx, parent, core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "charge", ID: "c1", Args: `{}`}),
	), charge()), "go")
	if err != nil {
		t.Fatal(err)
	}
	<-in
	cctx, cancel := context.WithCancelCause(ctx)
	cancel(thread.ErrShutdown)
	_ = p.Close(cctx)
	wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
	_ = p.Close(wctx)
	wcancel()
	time.Sleep(50 * time.Millisecond)
	st2 := thread.Memory()
	for _, id := range []string{parent.ID(), r.Child} {
		h, entries, _, err := st.Load(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if err := st2.Create(ctx, h); err != nil {
			t.Fatal(err)
		}
		if err := st2.Append(ctx, id, entries...); err != nil {
			t.Fatal(err)
		}
	}
	parent, err = thread.Open(ctx, st2, parent.ID(), core.New(wefttest.Script()))
	if err != nil {
		t.Fatal(err)
	}
	p2 := pool.New(1)
	defer func() { _ = p2.Close(ctx) }()
	if err := p2.Register(r.Child, core.New(wefttest.Script(wefttest.Say("ok")), charge())); err != nil {
		t.Fatal(err)
	}
	if err := p2.Recover(ctx, parent); err != nil {
		t.Fatalf("Recover = %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(parent.Pending()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	pend := parent.Pending()
	if len(pend) != 1 || !pend[0].Recovery || pend[0].Child != r.Child {
		t.Fatalf("parent Pending = %+v, want the child's recovery request", pend)
	}
	for _, rc := range pool.Receipts(parent) {
		if rc.ID == r.ID && rc.Settled() {
			t.Fatalf("receipt settled %+v, want it open", rc)
		}
	}
}
