package pool_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/thread/pool"
)

// Two Pool values at different levels of one delegation chain — the
// lead delegates through pa, and the child pa runs delegates through
// pb — are refused at the second delegation, before pb creates any
// child (ErrMixedPools; ADR 0022 amendment 2026-11-06). Before 0.24.1
// pb ran the child, its park was mirrored up, and a decision at the top
// failed the top receipt with "nothing to resume" while pb ran the
// approved effect anyway: the effect happened, the lead read failed.
func TestMixedPoolsAcrossLevelsRefused(t *testing.T) {
	ctx := context.Background()
	st := thread.Memory()
	pa, pb := pool.New(2), pool.New(2)
	defer func() { _ = pb.Close(ctx); _ = pa.Close(ctx) }()
	payments, ran := gatedChild(
		wefttest.ToolCalls(wefttest.Call{Name: "refund", Args: `{"order_id":"B-7"}`}),
		wefttest.Say("payments: refunded B-7"),
	)
	finance := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "fp",
			Args: wefttest.Args(struct{ Prompt string }{"refund B-7"})}),
		wefttest.Say("finance: could not delegate"),
	), pb.MustWrap("pay", "the payments desk", payments))
	lead := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "finance", ID: "cf",
			Args: wefttest.Args(struct{ Prompt string }{"settle B-7"})}),
		wefttest.Say("lead: done"),
	), pa.MustWrap("finance", "the finance desk", finance))
	s, err := thread.Create(ctx, st, lead)
	if err != nil {
		t.Fatal(err)
	}
	t1, err := s.Send(ctx, core.User("settle B-7"))
	if err != nil {
		t.Fatal(err)
	}
	res, err := t1.Wait()
	if err != nil {
		t.Fatalf("lead turn: %v", err)
	}
	if len(res.Pending) != 0 || res.Text() != "lead: done" {
		t.Fatalf("lead ended %q with %d pending, want done with none", res.Text(), len(res.Pending))
	}
	rs := pool.Receipts(s)
	if len(rs) != 1 || rs[0].State != pool.Done {
		t.Fatalf("lead receipts = %+v, want finance done", rs)
	}
	fin, err := thread.Open(ctx, st, rs[0].Child, core.New(wefttest.Script()))
	if err != nil {
		t.Fatal(err)
	}
	if frs := pool.Receipts(fin); len(frs) != 0 {
		t.Errorf("pb receipted %+v in pa's child, want nothing: refused before any child", frs)
	}
	got := toolResultsOf(fin.Context())
	if len(got) != 1 || !strings.Contains(got[0], pool.ErrMixedPools.Error()) {
		t.Errorf("finance model read %q, want the ErrMixedPools refusal", got)
	}
	if n := len(ran.snapshot()); n != 0 {
		t.Errorf("the payments effect ran %d times, want never", n)
	}
	page, err := st.List(ctx, thread.Query{Limit: 50})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Sessions) != 2 {
		t.Errorf("%d sessions stored, want the lead and finance alone", len(page.Sessions))
	}
}

// Submit from a handler of another pool's child names that child as
// the parent: refused the same way, with errors.Is.
func TestMixedPoolsSubmitRefused(t *testing.T) {
	ctx := context.Background()
	pa, pb := pool.New(2), pool.New(2)
	defer func() { _ = pb.Close(ctx); _ = pa.Close(ctx) }()
	var subErr error
	kick := core.Tool("kick", "", func(ctx context.Context, _ struct{}) (string, error) {
		_, subErr = pb.Submit(ctx, thread.SessionFromContext(ctx), core.New(wefttest.Script(wefttest.Say("x"))), "go")
		return "kicked", nil
	})
	child := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "kick", Args: `{}`}),
		wefttest.Say("child done"),
	), kick)
	s, err := thread.Create(ctx, thread.Memory(), nestedParent(pa, child))
	if err != nil {
		t.Fatal(err)
	}
	t1, err := s.Send(ctx, core.User("go"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := t1.Wait(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(subErr, pool.ErrMixedPools) {
		t.Errorf("Submit through pb from pa's child = %v, want ErrMixedPools", subErr)
	}
}

// A wrapped tool called by a core.Subagent child running inside a
// session turn is refused with ErrNestedDelegation before any child
// exists: the call is the child run's, not the session's, and a receipt
// in the session would name a call id its transcript does not hold.
// Before 0.24.1 the delegation ran and receipted into the lead.
func TestSubagentChildDelegationRefused(t *testing.T) {
	ctx := context.Background()
	p := pool.New(2)
	defer func() { _ = p.Close(ctx) }()
	leaf, _ := gatedChild(wefttest.Say("leaf done"))
	var mu sync.Mutex
	var helperRead []string
	helper := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "research", ID: "hc",
			Args: wefttest.Args(struct{ Prompt string }{"go"})}),
		wefttest.Say("helper done"),
	), p.MustWrap("research", "", leaf), core.Tap(func(_ context.Context, ev core.Event) {
		if f, ok := ev.(core.ToolFinish); ok && f.Name == "research" {
			mu.Lock()
			helperRead = append(helperRead, f.Content)
			mu.Unlock()
		}
	}))
	lead := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "helper", ID: "lh", Args: `{"prompt":"help"}`}),
		wefttest.Say("lead done"),
	), core.Subagent("helper", "", helper))
	st := thread.Memory()
	s, err := thread.Create(ctx, st, lead)
	if err != nil {
		t.Fatal(err)
	}
	t1, err := s.Send(ctx, core.User("go"))
	if err != nil {
		t.Fatal(err)
	}
	res, err := t1.Wait()
	if err != nil || res.Text() != "lead done" {
		t.Fatalf("lead ended %q, %v", res.Text(), err)
	}
	if rs := pool.Receipts(s); len(rs) != 0 {
		t.Errorf("lead receipts = %+v, want none: the delegation is the child run's", rs)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(helperRead) != 1 || !strings.Contains(helperRead[0], pool.ErrNestedDelegation.Error()) {
		t.Errorf("helper read %q, want the ErrNestedDelegation refusal", helperRead)
	}
	page, err := st.List(ctx, thread.Query{Limit: 50})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Sessions) != 1 {
		t.Errorf("%d sessions stored, want the lead alone", len(page.Sessions))
	}
}

// The parent session's OnRequest fires for a mirrored child request,
// once, after it is durable: an inbox fed by OnRequest sees nested
// approvals. Before 0.24.1 it never fired for a mirror.
func TestOnRequestFiresForAMirroredRequest(t *testing.T) {
	ctx := context.Background()
	p := pool.New(2)
	defer func() { _ = p.Close(ctx) }()
	child, _ := gatedChild(
		wefttest.ToolCalls(wefttest.Call{Name: "refund", Args: `{"order_id":"1234"}`}),
		wefttest.Say("refunded"),
	)
	var mu sync.Mutex
	var got []thread.Request
	var s *thread.Session
	durable := true
	s, err := thread.Create(ctx, thread.Memory(), nestedParent(p, child), thread.OnRequest(func(r thread.Request) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, r)
		seen := false
		for _, q := range s.Pending() {
			seen = seen || q.CallID == r.CallID
		}
		durable = durable && seen
	}))
	if err != nil {
		t.Fatal(err)
	}
	t1, err := s.Send(ctx, core.User("go"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := t1.Wait(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	var mirrored []thread.Request
	for _, r := range got {
		if r.Child != "" {
			mirrored = append(mirrored, r)
		}
	}
	if len(mirrored) != 1 || mirrored[0].Tool != "refund" {
		t.Fatalf("OnRequest got %+v, want the child's refund request once", got)
	}
	if !durable {
		t.Error("OnRequest fired before the mirror was on Pending")
	}
}
